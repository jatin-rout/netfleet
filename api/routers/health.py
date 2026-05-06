from fastapi import APIRouter
from shared.utils.mongo_client import MongoDBClient
from shared.utils.redis_client import RedisClient
from shared.utils.logger import get_logger

logger = get_logger("api.health")
router = APIRouter()


@router.get("/health")
async def health_check():
    """System health — MongoDB and Redis connectivity."""
    mongo = MongoDBClient()
    redis = RedisClient()

    mongo_ok = mongo.health_check()
    redis_ok = redis.health_check()

    overall = "HEALTHY" if (mongo_ok and redis_ok) else "DEGRADED"

    return {
        "status": overall,
        "components": {
            "mongodb": "HEALTHY" if mongo_ok else "FAILED",
            "redis": "HEALTHY" if redis_ok else "FAILED",
        },
    }


@router.get("/metrics")
async def component_metrics():
    """Live queue depths readable without any component running."""
    redis = RedisClient()

    queues = [
        "queue_higher_segments",
        "queue_lower_segments",
        "queue_raw_results",
        "queue_normalized_records",
        "queue_db_insert",
        "queue_rag_raw",
        "queue_discovery_trigger",
    ]

    depths = {}
    for q in queues:
        try:
            depths[q] = redis.get_queue_size(q)
        except Exception:
            depths[q] = -1

    return {"queue_depths": depths}
