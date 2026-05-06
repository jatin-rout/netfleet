from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from shared.utils.redis_client import RedisClient
from shared.utils.mongo_client import MongoDBClient
from shared.utils.logger import get_logger

logger = get_logger("api.discovery")
router = APIRouter()

TRIGGER_QUEUE = "queue_discovery_trigger"


class DiscoverySyncRequest(BaseModel):
    segments: list[str] = []
    regions: list[str] = []
    triggered_by: str = "api"


@router.post("/discovery/sync")
async def trigger_discovery_sync(
    request: DiscoverySyncRequest,
):
    """
    Trigger a manual discovery cycle.

    Pushes a message onto the discovery trigger queue consumed by
    the discovery component's trigger listener thread.
    Returns immediately — discovery runs asynchronously.
    """
    try:
        redis = RedisClient()
        msg = {
            "segments": request.segments,
            "regions": request.regions,
            "triggered_by": request.triggered_by,
            "triggered_at": datetime.utcnow().isoformat(),
        }
        redis.push_to_queue(TRIGGER_QUEUE, msg)

        return {
            "status": "queued",
            "message": "Discovery sync queued for processing",
            "triggered_at": msg["triggered_at"],
        }
    except Exception as exc:
        logger.error(f"Failed to trigger discovery: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/discovery/status")
async def discovery_status():
    """
    Discovery queue depth plus per-(region, segment) device counts
    from MongoDB.
    """
    try:
        redis = RedisClient()
        mongo = MongoDBClient()

        queue_depth = redis.get_queue_size(TRIGGER_QUEUE)

        collection = mongo.get_collection("devices")
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "region": "$region",
                        "segment": "$segment",
                    },
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id.region": 1, "_id.segment": 1}},
        ]
        breakdown = list(collection.aggregate(pipeline))

        region_map: dict[str, dict] = {}
        total_devices = 0
        for row in breakdown:
            region = row["_id"]["region"]
            segment = row["_id"]["segment"]
            count = row["count"]
            total_devices += count
            region_map.setdefault(region, {})[segment] = count

        return {
            "trigger_queue_depth": queue_depth,
            "trigger_status": "pending" if queue_depth > 0 else "idle",
            "total_devices": total_devices,
            "by_region": region_map,
        }
    except Exception as exc:
        logger.error(f"Failed to get discovery status: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
