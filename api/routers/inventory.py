from datetime import datetime
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from shared.utils.redis_client import RedisClient
from shared.utils.mongo_client import MongoDBClient
from shared.utils.logger import get_logger

logger = get_logger("api.inventory")
router = APIRouter()

TRIGGER_QUEUE = "queue_inventory_trigger"


class InventorySyncRequest(BaseModel):
    segments: list[str] = []
    regions: list[str] = []
    triggered_by: str = "api"


@router.post("/inventory/sync")
async def trigger_inventory_sync(
    request: InventorySyncRequest,
):
    """
    Trigger a manual inventory cycle.

    Pushes a message onto the inventory trigger queue consumed by
    the inventory component's trigger listener thread.
    Returns immediately — inventory runs asynchronously.
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
            "message": "Inventory sync queued for processing",
            "triggered_at": msg["triggered_at"],
        }
    except Exception as exc:
        logger.error(f"Failed to trigger inventory sync: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/inventory/status")
async def inventory_status():
    """
    Inventory queue depth plus per-(region, segment) device counts
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
        logger.error(f"Failed to get inventory status: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
