import os
from .base import BaseInventorySource
from shared.utils.logger import get_logger

logger = get_logger("inventory_source_factory")


def get_source() -> BaseInventorySource:
    """
    Returns the appropriate inventory source based on INVENTORY_MODE.
        cron  → CronInventorySource  (standalone, scheduled batch)
        event → EventInventorySource (distributed, real-time Kafka)
    """
    mode = os.getenv("INVENTORY_MODE", "cron").lower()

    if mode == "event":
        from .event_source import EventInventorySource
        logger.info("Inventory source: event-driven (Kafka)")
        return EventInventorySource()

    from .cron_source import CronInventorySource
    logger.info("Inventory source: cron batch")
    return CronInventorySource()
