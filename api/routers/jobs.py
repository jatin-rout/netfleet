import uuid
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from shared.utils.mongo_client import MongoDBClient
from shared.utils.redis_client import RedisClient
from shared.models import JobStatus
from shared.utils.logger import get_logger

logger = get_logger("api.jobs")
router = APIRouter()


class JobTriggerRequest(BaseModel):
    job_name: str
    triggered_by: str = "api"


@router.post("/jobs/trigger")
async def trigger_job(request: JobTriggerRequest):
    """
    Manually trigger a configured job by name.
    Creates a PENDING execution record and pushes it to the
    scheduler via Redis so the scheduler picks it up immediately.
    """
    from shared.config.settings import JOBS

    job_config = next(
        (
            j
            for j in JOBS.get("jobs", [])
            if j["name"] == request.job_name
        ),
        None,
    )
    if not job_config:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Job {request.job_name!r} not found in jobs.yaml"
            ),
        )

    try:
        mongo = MongoDBClient()
        redis = RedisClient()

        execution_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()

        execution = {
            "execution_id": execution_id,
            "job_id": job_config["name"],
            "job_name": job_config["name"],
            "operation": job_config["operation"],
            "segments": job_config["segments"],
            "status": JobStatus.PENDING,
            "total_records": 0,
            "processed_records": 0,
            "failed_records": 0,
            "triggered_at": now,
            "triggered_by": request.triggered_by,
            "completed_at": None,
            "error_message": None,
        }
        mongo.insert_many("job_executions", [execution])

        # Signal the scheduler — it owns job lifecycle from here
        redis.push_to_queue(
            "queue_job_trigger",
            {
                "execution_id": execution_id,
                "job_config": job_config,
                "triggered_by": request.triggered_by,
            },
        )

        return {
            "execution_id": execution_id,
            "job_name": request.job_name,
            "status": "PENDING",
            "triggered_at": now,
        }
    except Exception as exc:
        logger.error(f"trigger_job failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/jobs/{execution_id}/status")
async def get_job_status(execution_id: str):
    """
    Get status of a job execution.
    Enriches with live Redis progress cache if the job is running.
    """
    try:
        mongo = MongoDBClient()
        execution = mongo.find_one(
            "job_executions",
            {"execution_id": execution_id},
        )
        if not execution:
            raise HTTPException(
                status_code=404,
                detail=f"Execution {execution_id!r} not found",
            )
        execution.pop("_id", None)

        redis = RedisClient()
        cache_key = f"job_progress:{execution_id}"
        live_progress = redis.get_cache(cache_key)
        if live_progress:
            execution["live_progress"] = live_progress

        return execution
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"get_job_status failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/jobs")
async def list_jobs(
    status: Optional[str] = Query(
        None,
        enum=["PENDING", "RUNNING", "COMPLETE", "FAILED", "TIMEOUT"],
    ),
    limit: int = Query(20, ge=1, le=200),
):
    """List recent job executions, newest first."""
    try:
        mongo = MongoDBClient()
        query: dict = {}
        if status:
            query["status"] = status

        collection = mongo.get_collection("job_executions")
        executions = list(
            collection.find(query, {"_id": 0})
            .sort("triggered_at", -1)
            .limit(limit)
        )

        return {
            "total": len(executions),
            "executions": executions,
        }
    except Exception as exc:
        logger.error(f"list_jobs failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/jobs/config/all")
async def list_configured_jobs():
    """All jobs defined in jobs.yaml."""
    from shared.config.settings import JOBS

    return {"jobs": JOBS.get("jobs", [])}
