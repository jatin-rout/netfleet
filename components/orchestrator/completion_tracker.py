import threading
import time
from datetime import datetime
from typing import Optional

from shared.models.job import JobStatus
from shared.utils.logger import get_logger
from shared.utils.mongo_client import MongoDBClient
from shared.utils.redis_client import RedisClient

logger = get_logger("orchestrator.completion_tracker")

_POLL_INTERVAL_SECONDS = 10


class CompletionTracker:
    """
    Monitors all RUNNING job executions and transitions them to
    COMPLETE or TIMEOUT based on Redis progress cache.

    Redis key:  job_progress:{execution_id}
    Written by: DB Insert component after each batch
    Read by:    this class every _POLL_INTERVAL_SECONDS seconds

    Completion condition: inserted_records >= total_records > 0
    Timeout condition:    elapsed time > execution.timeout_minutes
    """

    def __init__(self):
        self._mongo = MongoDBClient()
        self._redis = RedisClient()
        self._running = False

    def start(self) -> None:
        self._running = True
        t = threading.Thread(
            target=self._monitor_loop,
            name="completion-tracker",
            daemon=True,
        )
        t.start()
        logger.info("CompletionTracker started")

    def stop(self) -> None:
        self._running = False

    def _monitor_loop(self) -> None:
        while self._running:
            try:
                self._scan()
            except Exception as exc:
                logger.error(f"CompletionTracker scan error: {exc}")
            time.sleep(_POLL_INTERVAL_SECONDS)

    def _scan(self) -> None:
        running = self._mongo.find_many(
            "job_executions",
            {"status": JobStatus.RUNNING},
        )
        for exe in running:
            self._evaluate(exe)

    def _evaluate(self, exe: dict) -> None:
        execution_id = exe["execution_id"]
        progress = self._redis.get_cache(
            f"job_progress:{execution_id}"
        )

        if progress:
            total = progress.get("total_records", 0)
            inserted = progress.get("inserted_records", 0)
            failed = progress.get("failed_records", 0)
            if total > 0 and inserted >= total:
                self._mark_complete(execution_id, inserted, failed)
                return

        if self._is_timed_out(exe):
            self._mark_timeout(execution_id, progress)

    def _is_timed_out(self, exe: dict) -> bool:
        triggered_at_str = exe.get("triggered_at")
        timeout_min = exe.get("timeout_minutes", 120)
        try:
            triggered_dt = datetime.fromisoformat(triggered_at_str)
        except (TypeError, ValueError):
            return False
        elapsed_min = (
            datetime.utcnow() - triggered_dt
        ).total_seconds() / 60
        return elapsed_min > timeout_min

    def _mark_complete(
        self,
        execution_id: str,
        inserted: int,
        failed: int,
    ) -> None:
        self._mongo.update_one(
            "job_executions",
            {"execution_id": execution_id},
            {
                "$set": {
                    "status": JobStatus.COMPLETE,
                    "processed_records": inserted,
                    "failed_records": failed,
                    "completed_at": datetime.utcnow().isoformat(),
                }
            },
        )
        self._redis.delete_cache(f"job_progress:{execution_id}")
        logger.info(
            f"Job COMPLETE — execution={execution_id} "
            f"inserted={inserted} failed={failed}"
        )

    def _mark_timeout(
        self,
        execution_id: str,
        progress: Optional[dict],
    ) -> None:
        inserted = progress.get("inserted_records", 0) if progress else 0
        failed = progress.get("failed_records", 0) if progress else 0
        self._mongo.update_one(
            "job_executions",
            {"execution_id": execution_id},
            {
                "$set": {
                    "status": JobStatus.TIMEOUT,
                    "processed_records": inserted,
                    "failed_records": failed,
                    "completed_at": datetime.utcnow().isoformat(),
                    "error_message": "Job exceeded timeout window",
                }
            },
        )
        self._redis.delete_cache(f"job_progress:{execution_id}")
        logger.warning(f"Job TIMEOUT — execution={execution_id}")
