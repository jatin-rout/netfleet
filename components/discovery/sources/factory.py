import os
from .base import BaseDiscoverySource
from shared.utils.logger import get_logger

logger = get_logger("discovery_source_factory")


def get_source() -> BaseDiscoverySource:
    """
    Returns the appropriate discovery source based on DISCOVERY_MODE.
        cron  → CronDiscoverySource  (standalone, scheduled batch)
        event → EventDiscoverySource (distributed, real-time Kafka)
    """
    mode = os.getenv("DISCOVERY_MODE", "cron").lower()

    if mode == "event":
        from .event_source import EventDiscoverySource
        logger.info("Discovery source: event-driven (Kafka)")
        return EventDiscoverySource()

    from .cron_source import CronDiscoverySource
    logger.info("Discovery source: cron batch")
    return CronDiscoverySource()
