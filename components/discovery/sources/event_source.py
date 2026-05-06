from datetime import datetime
from shared.models import (
    Device,
    DeviceDiscoveryRecord,
    DeviceStatus,
    SegmentType,
    VendorType,
    Protocol,
    SegmentPriority,
    IdentityType,
)
from shared.config.settings import SegmentConfig, KafkaConfig
from shared.transport.factory import get_transport
from shared.utils.mongo_client import MongoDBClient
from shared.utils.logger import get_logger
from .base import BaseDiscoverySource

logger = get_logger("event_source")


class EventDiscoverySource(BaseDiscoverySource):
    """
    Event-driven discovery source for distributed (Kafka) mode.

    Processes incremental device events from the network controller:
        device_join      — upsert device as ACTIVE
        device_leave     — mark device INACTIVE
        device_ip_change — update ip_address

    fetch_all_regions() reads current DB state (used for delta
    validation on-demand, not for continuous processing).
    """

    def __init__(self):
        self.transport = get_transport()
        self.mongo = MongoDBClient()
        self.topic = KafkaConfig.TOPICS["discovery_events"]

    # ------------------------------------------------------------------ #
    # BaseDiscoverySource interface                                        #
    # ------------------------------------------------------------------ #

    def fetch_all_regions(
        self,
        segment: str
    ) -> list[DeviceDiscoveryRecord]:
        docs = self.mongo.find_many(
            "devices",
            {"segment": segment, "status": "ACTIVE"},
        )
        region_map: dict[str, list[Device]] = {}
        for doc in docs:
            doc.pop("_id", None)
            region = doc["region"]
            region_map.setdefault(region, [])
            region_map[region].append(Device(**doc))

        return [
            DeviceDiscoveryRecord(
                region=region,
                segment=SegmentType(segment),
                devices=devices,
                total_count=len(devices),
                source="event_driven",
            )
            for region, devices in region_map.items()
        ]

    def health_check(self) -> bool:
        return self.transport.health_check()

    # ------------------------------------------------------------------ #
    # Event processing                                                     #
    # ------------------------------------------------------------------ #

    def consume_events(self, batch_size: int = 100) -> int:
        """Consume and process a batch of device events. Returns count processed."""
        events = self.transport.consume_batch(
            self.topic,
            batch_size=batch_size,
        )
        processed = 0
        for event in events:
            if self._dispatch(event):
                processed += 1
        return processed

    def _dispatch(self, event: dict) -> bool:
        event_type = event.get("event_type")
        device_data = event.get("device", {})

        if not device_data.get("device_id"):
            logger.warning(f"Skipping event — no device_id: {event}")
            return False

        handlers = {
            "device_join": self._handle_join,
            "device_leave": self._handle_leave,
            "device_ip_change": self._handle_ip_change,
        }
        handler = handlers.get(event_type)
        if handler is None:
            logger.warning(f"Unknown event type: {event_type}")
            return False

        try:
            return handler(device_data)
        except Exception as exc:
            logger.error(
                f"Event handler failed for {event_type} "
                f"device={device_data.get('device_id')}: {exc}"
            )
            return False

    def _handle_join(self, data: dict) -> bool:
        segment = data.get("segment", "Edge")
        device = Device(
            device_id=data["device_id"],
            ip_address=data["ip_address"],
            mac_address=data.get("mac_address"),
            serial_number=data.get("serial_number"),
            vendor=VendorType(data["vendor"]),
            segment=SegmentType(segment),
            priority=SegmentPriority(
                SegmentConfig.PRIORITY_MAP.get(segment, "STANDARD")
            ),
            protocol=Protocol(data.get("protocol", "SSH")),
            identity_type=IdentityType(
                SegmentConfig.IDENTITY_MAP.get(segment, "mac_address")
            ),
            region=data["region"],
            status=DeviceStatus.ACTIVE,
        )
        self.mongo.update_one(
            "devices",
            {"device_id": device.device_id},
            {"$set": device.model_dump()},
            upsert=True,
        )
        logger.info(
            f"device_join — {device.device_id} "
            f"region={device.region} segment={segment}"
        )
        return True

    def _handle_leave(self, data: dict) -> bool:
        self.mongo.update_one(
            "devices",
            {"device_id": data["device_id"]},
            {"$set": {
                "status": "INACTIVE",
                "last_seen": datetime.utcnow().isoformat(),
            }},
        )
        logger.info(f"device_leave — {data['device_id']}")
        return True

    def _handle_ip_change(self, data: dict) -> bool:
        self.mongo.update_one(
            "devices",
            {"device_id": data["device_id"]},
            {"$set": {
                "ip_address": data["new_ip"],
                "last_ip_change": datetime.utcnow().isoformat(),
            }},
        )
        logger.info(
            f"device_ip_change — {data['device_id']} "
            f"→ {data['new_ip']}"
        )
        return True
