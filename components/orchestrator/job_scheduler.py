import threading
import time
import uuid
from datetime import datetime, timedelta

from croniter import croniter

from components.orchestrator.completion_tracker import CompletionTracker
from components.orchestrator.priority_queue_manager import PriorityQueueManager
from shared.config.settings import JOBS, OrchestratorConfig
from shared.models.job import JobStatus
from shared.utils.logger import get_logger
from shared.utils.mongo_client import MongoDBClient
from shared.utils.redis_client import RedisClient

logger = get_logger("orchestrator.job_scheduler")

_ADHOC_QUEUE = "queue_job_trigger"


class JobScheduler:
    """
    Component 2 — Job Orchestrator.

    Two trigger paths run as background threads:

    Cron path  (PERIODIC jobs):
        Ticks every JOB_CHECK_INTERVAL seconds.
        Fires each PERIODIC job whose cron expression last fired
        within that window, provided no RUNNING execution exists.

    Adhoc path (ADHOC jobs):
        Blocks on queue_job_trigger.
        API pre-creates a PENDING execution and signals this queue.
        Orchestrator transitions PENDING → RUNNING and dispatches.

    On each trigger the orchestrator:
        1. Counts total active devices for the job's segments
           by querying the Inventory DB (devices collection).
        2. Creates/transitions the execution record to RUNNING,
           stores total_records.
        3. Iterates every matching active device via a MongoDB
           cursor and publishes one DeviceQueueMessage to the
           appropriate queue (higher or lower segment queue).
        4. Seeds job_progress:{execution_id} in Redis for the
           CompletionTracker.

    The orchestrator does not know or care who consumes the queues.
    """

    def __init__(self):
        self._mongo = MongoDBClient()
        self._redis = RedisClient()
        self._jobs = JOBS.get("jobs", [])
        self._running = False
        self._router = PriorityQueueManager()
        self._tracker = CompletionTracker()

    def start(self) -> None:
        self._running = True
        self._router.start()
        self._tracker.start()
        threads = [
            threading.Thread(
                target=self._cron_loop,
                name="orchestrator-cron",
                daemon=True,
            ),
            threading.Thread(
                target=self._adhoc_listener,
                name="orchestrator-adhoc",
                daemon=True,
            ),
        ]
        for t in threads:
            t.start()
        periodic = sum(
            1 for j in self._jobs
            if j.get("job_type", "PERIODIC") == "PERIODIC"
        )
        adhoc = len(self._jobs) - periodic
        logger.info(
            f"JobScheduler started — "
            f"{periodic} periodic, {adhoc} adhoc jobs loaded"
        )

    def stop(self) -> None:
        self._running = False
        self._router.stop()
        self._tracker.stop()
        logger.info("JobScheduler stopped")

    # ------------------------------------------------------------------ #
    # Cron path                                                            #
    # ------------------------------------------------------------------ #

    def _cron_loop(self) -> None:
        while self._running:
            now = datetime.utcnow()
            for job in self._jobs:
                if job.get("job_type", "PERIODIC") != "PERIODIC":
                    continue
                cron_expr = job.get("cron")
                if not cron_expr:
                    continue
                try:
                    if (
                        self._cron_fired(cron_expr, now)
                        and not self._has_running(job["name"])
                    ):
                        self._fire_periodic(job)
                except Exception as exc:
                    logger.error(
                        f"Cron check failed for {job.get('name')}: {exc}"
                    )
            time.sleep(OrchestratorConfig.JOB_CHECK_INTERVAL)

    def _cron_fired(self, expr: str, now: datetime) -> bool:
        """
        True if the cron expression last fired within JOB_CHECK_INTERVAL
        seconds of now. Tolerates minor scheduling drift.
        """
        try:
            # Add a microsecond so that when now is exactly on the cron boundary,
            # get_prev returns now rather than the prior occurrence.
            prev = croniter(expr, now + timedelta(microseconds=1)).get_prev(datetime)
            return (now - prev).total_seconds() <= OrchestratorConfig.JOB_CHECK_INTERVAL
        except Exception as exc:
            logger.error(f"Invalid cron '{expr}': {exc}")
            return False

    def _has_running(self, job_name: str) -> bool:
        return (
            self._mongo.count_documents(
                "job_executions",
                {"job_id": job_name, "status": JobStatus.RUNNING},
            )
            > 0
        )

    def _fire_periodic(self, job_config: dict) -> None:
        execution_id = str(uuid.uuid4())
        segments = job_config["segments"]
        total_records = self._count_devices(segments)
        now_iso = datetime.utcnow().isoformat()

        self._mongo.insert_many(
            "job_executions",
            [{
                "execution_id": execution_id,
                "job_id": job_config["name"],
                "job_name": job_config["name"],
                "operation": job_config["operation"],
                "segments": segments,
                "status": JobStatus.RUNNING,
                "total_records": total_records,
                "processed_records": 0,
                "failed_records": 0,
                "triggered_at": now_iso,
                "triggered_by": "scheduler",
                "completed_at": None,
                "error_message": None,
                "timeout_minutes": job_config.get("timeout_minutes", 120),
            }],
        )

        if total_records == 0:
            self._complete_immediately(execution_id, job_config["name"])
            return

        self._publish_devices(execution_id, job_config, total_records, now_iso)

    # ------------------------------------------------------------------ #
    # Adhoc path                                                           #
    # ------------------------------------------------------------------ #

    def _adhoc_listener(self) -> None:
        while self._running:
            try:
                msg = self._redis.pop_from_queue(_ADHOC_QUEUE, timeout=5)
                if msg:
                    self._fire_adhoc(msg)
            except Exception as exc:
                logger.error(f"Adhoc listener error: {exc}")

    def _fire_adhoc(self, msg: dict) -> None:
        """
        Handle an on-demand trigger from queue_job_trigger.
        API pre-inserts a PENDING execution; transition to RUNNING
        and dispatch device messages.
        """
        execution_id = msg.get("execution_id")
        job_config = msg.get("job_config")
        triggered_by = msg.get("triggered_by", "api")

        if not execution_id or not job_config:
            logger.error(f"Malformed adhoc trigger: {msg}")
            return

        segments = job_config["segments"]
        total_records = self._count_devices(segments)
        now_iso = datetime.utcnow().isoformat()

        updated = self._mongo.update_one(
            "job_executions",
            {"execution_id": execution_id, "status": JobStatus.PENDING},
            {
                "$set": {
                    "status": JobStatus.RUNNING,
                    "total_records": total_records,
                    "triggered_by": triggered_by,
                    "timeout_minutes": job_config.get("timeout_minutes", 120),
                }
            },
        )
        if updated == 0:
            logger.warning(
                f"Adhoc skip — execution {execution_id} not in PENDING state"
            )
            return

        if total_records == 0:
            self._complete_immediately(execution_id, job_config["name"])
            return

        self._publish_devices(execution_id, job_config, total_records, now_iso)

    # ------------------------------------------------------------------ #
    # Device publishing — core of the scheduler                           #
    # ------------------------------------------------------------------ #

    def _count_devices(self, segments: list[str]) -> int:
        """Count ACTIVE devices for given segments in the Inventory DB."""
        return self._mongo.count_documents(
            "devices",
            {"segment": {"$in": segments}, "status": "ACTIVE"},
        )

    def _publish_devices(
        self,
        execution_id: str,
        job_config: dict,
        total_records: int,
        now_iso: str,
    ) -> None:
        """
        Iterate the Inventory DB cursor for matching ACTIVE devices and
        publish one DeviceQueueMessage per device to the priority queue.
        Uses a cursor to avoid loading the full fleet into memory.
        """
        segments = job_config["segments"]
        collection = self._mongo.get_collection("devices")
        cursor = collection.find(
            {"segment": {"$in": segments}, "status": "ACTIVE"},
            {"_id": 0},
        )

        published = 0
        for device_doc in cursor:
            priority = PriorityQueueManager.segment_priority(
                device_doc.get("segment", "")
            )
            self._router.route({
                "execution_id": execution_id,
                "job_id": job_config["name"],
                "job_name": job_config["name"],
                "operation": job_config["operation"],
                "device": device_doc,
                "priority": priority,
                "job_protocol": job_config.get("protocol"),
                "params": job_config.get("params", {}),
                "queued_at": now_iso,
            })
            published += 1

        # Seed Redis progress cache for CompletionTracker
        ttl = job_config.get("timeout_minutes", 120) * 60 + 300
        self._redis.set_cache(
            f"job_progress:{execution_id}",
            {
                "execution_id": execution_id,
                "job_id": job_config["name"],
                "total_records": total_records,
                "inserted_records": 0,
                "failed_records": 0,
                "last_updated": now_iso,
            },
            ttl_seconds=ttl,
        )
        logger.info(
            f"Job RUNNING — {job_config['name']} "
            f"execution={execution_id} "
            f"published={published}/{total_records} devices"
        )

    def _complete_immediately(
        self, execution_id: str, job_name: str
    ) -> None:
        self._mongo.update_one(
            "job_executions",
            {"execution_id": execution_id},
            {
                "$set": {
                    "status": JobStatus.COMPLETE,
                    "completed_at": datetime.utcnow().isoformat(),
                    "error_message": "No active devices for segments",
                }
            },
        )
        logger.info(
            f"Job COMPLETE (no devices) — {job_name} "
            f"execution={execution_id}"
        )
