from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from shared.utils.mongo_client import MongoDBClient
from shared.utils.logger import get_logger

logger = get_logger("api.devices")
router = APIRouter()


@router.get("/devices")
async def list_devices(
    segment: Optional[str] = Query(None),
    region: Optional[str] = Query(None),
    vendor: Optional[str] = Query(None),
    status: Optional[str] = Query(
        None,
        enum=["ACTIVE", "INACTIVE", "UNREACHABLE"],
    ),
    limit: int = Query(50, ge=1, le=1000),
    skip: int = Query(0, ge=0),
):
    """List devices with optional filters. Supports pagination."""
    try:
        mongo = MongoDBClient()
        query: dict = {}
        if segment:
            query["segment"] = segment
        if region:
            query["region"] = region
        if vendor:
            query["vendor"] = vendor
        if status:
            query["status"] = status

        collection = mongo.get_collection("devices")
        total = collection.count_documents(query)
        devices = list(
            collection.find(query, {"_id": 0})
            .skip(skip)
            .limit(limit)
        )

        return {
            "total": total,
            "skip": skip,
            "limit": limit,
            "devices": devices,
        }
    except Exception as exc:
        logger.error(f"list_devices failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/devices/{device_id}")
async def get_device(device_id: str):
    """Get a single device by device_id."""
    try:
        mongo = MongoDBClient()
        device = mongo.find_one(
            "devices",
            {"device_id": device_id},
        )
        if not device:
            raise HTTPException(
                status_code=404,
                detail=f"Device {device_id!r} not found",
            )
        device.pop("_id", None)
        return device
    except HTTPException:
        raise
    except Exception as exc:
        logger.error(f"get_device failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))


@router.get("/devices/region/{region}/count")
async def get_region_device_count(
    region: str,
    segment: Optional[str] = Query(None),
):
    """
    Device count for a city (region).
    Without segment — returns total + per-segment breakdown.
    With segment    — returns count for that specific segment.
    """
    try:
        mongo = MongoDBClient()
        collection = mongo.get_collection("devices")

        if segment:
            count = collection.count_documents(
                {"region": region, "segment": segment}
            )
            return {"region": region, "segment": segment, "count": count}

        pipeline = [
            {"$match": {"region": region}},
            {
                "$group": {
                    "_id": "$segment",
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id": 1}},
        ]
        rows = list(collection.aggregate(pipeline))
        total = sum(r["count"] for r in rows)

        return {
            "region": region,
            "total": total,
            "by_segment": {r["_id"]: r["count"] for r in rows},
        }
    except Exception as exc:
        logger.error(f"get_region_device_count failed: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
