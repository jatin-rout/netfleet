"""
Dashboard API — live job execution counters.

Data sources:
  Redis HASH  collector_counters:{execution_id}  — real-time Collector counters
  Redis JSON  job_progress:{execution_id}         — Orchestrator scheduled count + DB Insert progress
  MongoDB     collector_job_stats                 — permanent history after Redis TTL

The endpoint merges both Redis keys so the dashboard shows a single
coherent row per execution without any client-side joining.
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional

from shared.utils.redis_client import RedisClient
from shared.utils.mongo_client import MongoDBClient
from shared.utils.logger import get_logger

logger = get_logger("api.dashboard")
router = APIRouter()

_COUNTER_PREFIX = "collector_counters:"
_JOB_PROGRESS_PREFIX = "job_progress:"
_MONGO_COLLECTION = "collector_job_stats"

_INT_FIELDS = {
    "scheduled", "successful", "failed_total",
    "failed_auth", "failed_connection", "failed_timeout",
    "failed_rollback", "failed_dirty_state", "failed_unknown",
}


def _get_redis() -> RedisClient:
    return RedisClient()


def _read_hash(redis: RedisClient, key: str) -> Optional[dict]:
    """Read a Redis HASH as a typed dict. Returns None if key does not exist."""
    try:
        raw = redis._client.hgetall(key)
        if not raw:
            return None
        result = {}
        for k, v in raw.items():
            field = k.decode() if isinstance(k, bytes) else k
            val = v.decode() if isinstance(v, bytes) else v
            result[field] = int(val) if field in _INT_FIELDS else val
        return result
    except Exception as exc:
        logger.error(f"Redis HGETALL failed for key={key}: {exc}")
        return None


@router.get("/dashboard/jobs")
async def list_job_dashboard(
    execution_id: Optional[str] = Query(
        None, description="Filter to a specific execution"
    ),
    include_history: bool = Query(
        False,
        description="Include completed executions from MongoDB history"
    ),
):
    """
    Returns live job execution counter rows for the dashboard.

    Each row includes:
      - execution_id, job_id, job_name, operation
      - scheduled        (total devices from Orchestrator)
      - successful       (devices collected OK by Collector)
      - failed_total     (total device failures)
      - failed_breakdown (auth / connection / timeout / rollback / dirty_state / unknown)
      - inserted_records (records written to MongoDB by DB Insert)
      - last_updated     (ISO timestamp of last counter change)
    """
    redis = _get_redis()

    if execution_id:
        row = _build_row(redis, execution_id)
        if not row and include_history:
            row = _mongo_row(execution_id)
        if not row:
            raise HTTPException(
                status_code=404,
                detail=f"No counters found for execution_id={execution_id}",
            )
        return [row]

    # List all live keys from Redis
    try:
        keys = redis._client.keys(f"{_COUNTER_PREFIX}*")
    except Exception as exc:
        logger.error(f"Redis key scan failed: {exc}")
        raise HTTPException(status_code=503, detail="Redis unavailable")

    results = []
    for key in keys:
        key_str = key.decode() if isinstance(key, bytes) else key
        exec_id = key_str[len(_COUNTER_PREFIX):]
        row = _build_row(redis, exec_id)
        if row:
            results.append(row)

    if include_history:
        # Append MongoDB records not already covered by live Redis keys
        live_ids = {r["execution_id"] for r in results}
        mongo = MongoDBClient()
        try:
            history = mongo.find_many(_MONGO_COLLECTION, {}, {"_id": 0})
            for doc in history:
                if doc.get("execution_id") not in live_ids:
                    results.append(_format_row(doc, {}))
        except Exception as exc:
            logger.warning(f"MongoDB history fetch failed: {exc}")

    results.sort(key=lambda r: r.get("seeded_at", ""), reverse=True)
    return results


@router.get("/dashboard/jobs/{execution_id}")
async def get_job_dashboard(execution_id: str):
    """Single-execution counter detail. Falls back to MongoDB if not in Redis."""
    redis = _get_redis()
    row = _build_row(redis, execution_id)
    if not row:
        row = _mongo_row(execution_id)
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"No counters found for execution_id={execution_id}",
        )
    return row


# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #

def _build_row(redis: RedisClient, execution_id: str) -> Optional[dict]:
    """Build dashboard row from Redis HASH + job_progress JSON."""
    counters = _read_hash(redis, f"{_COUNTER_PREFIX}{execution_id}")
    if not counters:
        return None
    progress = redis.get_cache(f"{_JOB_PROGRESS_PREFIX}{execution_id}") or {}
    return _format_row(counters, progress)


def _mongo_row(execution_id: str) -> Optional[dict]:
    """Read a counter snapshot from MongoDB permanent storage."""
    try:
        mongo = MongoDBClient()
        doc = mongo.find_one(
            _MONGO_COLLECTION, {"execution_id": execution_id}
        )
        if not doc:
            return None
        doc.pop("_id", None)
        return _format_row(doc, {})
    except Exception as exc:
        logger.error(f"MongoDB row fetch failed for {execution_id}: {exc}")
        return None


def _format_row(counters: dict, progress: dict) -> dict:
    """Merge collector counters and job_progress into one dashboard dict."""
    scheduled = int(
        progress.get("total_records")
        or counters.get("scheduled")
        or 0
    )
    return {
        "execution_id":   counters.get("execution_id"),
        "job_id":         counters.get("job_id"),
        "job_name":       counters.get("job_name"),
        "operation":      counters.get("operation"),
        "scheduled":      scheduled,
        "successful":     int(counters.get("successful", 0)),
        "failed_total":   int(counters.get("failed_total", 0)),
        "failed_breakdown": {
            "auth":        int(counters.get("failed_auth", 0)),
            "connection":  int(counters.get("failed_connection", 0)),
            "timeout":     int(counters.get("failed_timeout", 0)),
            "rollback":    int(counters.get("failed_rollback", 0)),
            "dirty_state": int(counters.get("failed_dirty_state", 0)),
            "unknown":     int(counters.get("failed_unknown", 0)),
        },
        "inserted_records": int(progress.get("inserted_records", 0)),
        "flushed_to_mongo": counters.get("flushed_to_mongo") == "1",
        "seeded_at":      counters.get("seeded_at"),
        "last_updated":   counters.get("last_updated"),
    }
