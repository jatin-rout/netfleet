"""
FailureTracker — per-execution device outcome counters.

Storage strategy:
  Redis HASH  : real-time atomic counters; TTL 24h
                Key: collector_counters:{execution_id}
                Uses HINCRBY so concurrent threads never corrupt counts.

  MongoDB     : permanent audit record written once when the execution
                reaches terminal state (successful + failed >= scheduled).
                Collection: collector_job_stats
                Survives Redis restarts and Redis TTL expiry.
"""
import os
from datetime import datetime
from typing import Optional

from shared.models.status import FailureReason
from shared.utils.logger import get_logger
from shared.utils.mongo_client import MongoDBClient
from shared.utils.redis_client import RedisClient

logger = get_logger("collector.failure_tracker")

_KEY_PREFIX = "collector_counters:"
_TTL_SECONDS = 86400          # 24 h — covers any reasonable job window
_MONGO_COLLECTION = "collector_job_stats"

_COUNTER_FIELDS = [
    "scheduled",
    "successful",
    "failed_total",
    "failed_auth",
    "failed_connection",
    "failed_timeout",
    "failed_rollback",
    "failed_dirty_state",
    "failed_unknown",
]

_REASON_TO_FIELD = {
    FailureReason.AUTH:        "failed_auth",
    FailureReason.CONNECTION:  "failed_connection",
    FailureReason.TIMEOUT:     "failed_timeout",
    FailureReason.ROLLBACK:    "failed_rollback",
    FailureReason.DIRTY_STATE: "failed_dirty_state",
    FailureReason.UNKNOWN:     "failed_unknown",
}


class FailureTracker:
    """
    Thread-safe per-execution outcome counters backed by Redis HASH + MongoDB.

    Redis HASH is used for atomic increments (HINCRBY — no read-modify-write
    race condition). A permanent snapshot is written to MongoDB once the
    execution reaches terminal state so dashboard history survives TTL expiry.
    """

    def __init__(self):
        self._redis = RedisClient()
        self._mongo = MongoDBClient()

    # ------------------------------------------------------------------ #
    # Seed                                                                 #
    # ------------------------------------------------------------------ #

    def seed(
        self,
        execution_id: str,
        job_id: str,
        job_name: str,
        operation: str,
    ) -> None:
        """
        Initialise the counter HASH for an execution on first message seen.
        Idempotent — safe to call multiple times for the same execution.

        Reads total_records (scheduled) from job_progress:{execution_id} so
        the dashboard can show a meaningful denominator immediately.
        """
        key = self._key(execution_id)
        client = self._redis._client

        # EXISTS is atomic: skip seeding if already initialised
        if client.exists(key):
            return

        # Pull scheduled count from Orchestrator's job_progress key
        progress = self._redis.get_cache(f"job_progress:{execution_id}") or {}
        scheduled = int(progress.get("total_records", 0))

        # Seed all fields to 0 via HSET (single round-trip)
        init_fields: dict = {f: "0" for f in _COUNTER_FIELDS}
        init_fields["scheduled"] = str(scheduled)
        init_fields["execution_id"] = execution_id
        init_fields["job_id"] = job_id
        init_fields["job_name"] = job_name
        init_fields["operation"] = operation
        init_fields["seeded_at"] = datetime.utcnow().isoformat()
        init_fields["last_updated"] = datetime.utcnow().isoformat()
        init_fields["flushed_to_mongo"] = "0"

        client.hset(key, mapping=init_fields)
        client.expire(key, _TTL_SECONDS)

        logger.info(
            f"[{execution_id}] Counter HASH seeded — "
            f"job={job_name} operation={operation} scheduled={scheduled}"
        )

    # ------------------------------------------------------------------ #
    # Record outcomes — atomic via HINCRBY                                #
    # ------------------------------------------------------------------ #

    def record_success(self, execution_id: str) -> None:
        self._hincrby(execution_id, "successful", 1)
        logger.debug(f"[{execution_id}] Counter +successful")
        self._check_terminal(execution_id)

    def record_failure(
        self, execution_id: str, reason: FailureReason
    ) -> None:
        self._hincrby(execution_id, "failed_total", 1)
        field = _REASON_TO_FIELD.get(reason, "failed_unknown")
        self._hincrby(execution_id, field, 1)
        logger.debug(
            f"[{execution_id}] Counter +failed_total +{field} reason={reason.value}"
        )
        self._check_terminal(execution_id)

    # ------------------------------------------------------------------ #
    # Read                                                                 #
    # ------------------------------------------------------------------ #

    def get_counters(self, execution_id: str) -> Optional[dict]:
        """Return current counter snapshot as a dict. Returns None if not found."""
        client = self._redis._client
        try:
            raw = client.hgetall(self._key(execution_id))
            if not raw:
                return None
            return self._decode_hash(raw)
        except Exception as exc:
            logger.error(
                f"[{execution_id}] Failed to read counter HASH: {exc}"
            )
            return None

    def list_active_executions(self) -> list:
        """Return counter snapshots for all live collector_counters:* keys."""
        client = self._redis._client
        results = []
        try:
            keys = client.keys(f"{_KEY_PREFIX}*")
            for key in keys:
                key_str = key if isinstance(key, str) else key.decode()
                raw = client.hgetall(key_str)
                if raw:
                    results.append(self._decode_hash(raw))
        except Exception as exc:
            logger.error(f"Failed to list active executions: {exc}")
        results.sort(key=lambda r: r.get("seeded_at", ""), reverse=True)
        return results

    # ------------------------------------------------------------------ #
    # MongoDB flush                                                        #
    # ------------------------------------------------------------------ #

    def flush_to_mongo(self, execution_id: str) -> None:
        """
        Write the current counter snapshot to MongoDB for permanent storage.
        Idempotent — marks flushed_to_mongo=1 in the HASH so it is not
        written twice.
        """
        key = self._key(execution_id)
        client = self._redis._client

        already = client.hget(key, "flushed_to_mongo")
        if already == "1":
            return

        snapshot = self.get_counters(execution_id)
        if not snapshot:
            logger.warning(
                f"[{execution_id}] flush_to_mongo: counter HASH not found, skipping"
            )
            return

        snapshot["flushed_at"] = datetime.utcnow().isoformat()

        try:
            self._mongo.update_one(
                _MONGO_COLLECTION,
                {"execution_id": execution_id},
                {"$set": snapshot},
                upsert=True,
            )
            client.hset(key, "flushed_to_mongo", "1")
            logger.info(
                f"[{execution_id}] Counter snapshot flushed to MongoDB — "
                f"successful={snapshot.get('successful')} "
                f"failed={snapshot.get('failed_total')}"
            )
        except Exception as exc:
            logger.error(
                f"[{execution_id}] MongoDB flush failed: {exc}"
            )

    # ------------------------------------------------------------------ #
    # Internal                                                             #
    # ------------------------------------------------------------------ #

    def _hincrby(self, execution_id: str, field: str, amount: int) -> None:
        """Atomic Redis HINCRBY — no read-modify-write race condition."""
        key = self._key(execution_id)
        client = self._redis._client
        try:
            client.hincrby(key, field, amount)
            client.hset(key, "last_updated", datetime.utcnow().isoformat())
            # Refresh TTL on every write so active jobs never expire mid-flight
            client.expire(key, _TTL_SECONDS)
        except Exception as exc:
            logger.error(
                f"[{execution_id}] HINCRBY failed for field={field}: {exc}"
            )

    def _check_terminal(self, execution_id: str) -> None:
        """Flush to MongoDB once successful + failed_total >= scheduled."""
        snapshot = self.get_counters(execution_id)
        if not snapshot:
            return
        scheduled = int(snapshot.get("scheduled", 0))
        if scheduled == 0:
            return
        done = int(snapshot.get("successful", 0)) + int(snapshot.get("failed_total", 0))
        if done >= scheduled:
            logger.info(
                f"[{execution_id}] All {scheduled} devices processed "
                f"({snapshot.get('successful')} ok / {snapshot.get('failed_total')} failed). "
                f"Flushing to MongoDB."
            )
            self.flush_to_mongo(execution_id)

    @staticmethod
    def _key(execution_id: str) -> str:
        return f"{_KEY_PREFIX}{execution_id}"

    @staticmethod
    def _decode_hash(raw: dict) -> dict:
        """Convert Redis HASH byte/string values to Python-typed dict."""
        result = {}
        int_fields = set(_COUNTER_FIELDS)
        for k, v in raw.items():
            key = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            if key in int_fields:
                result[key] = int(val)
            else:
                result[key] = val
        return result
