import os
from shared.transport.base import EventTransport
from shared.utils.logger import get_logger

logger = get_logger("transport_factory")

_transport_instance = None


def get_transport() -> EventTransport:
    """
    Returns transport based on TRANSPORT_MODE env var.

    TRANSPORT_MODE=redis  → RedisTransport (standalone)
    TRANSPORT_MODE=kafka  → KafkaTransport (distributed)

    Singleton — same instance returned on every call.
    """
    global _transport_instance

    if _transport_instance is not None:
        return _transport_instance

    mode = os.getenv("TRANSPORT_MODE", "redis").lower()

    if mode == "kafka":
        from shared.transport.kafka_transport import (
            KafkaTransport
        )
        _transport_instance = KafkaTransport()
        logger.info("Transport mode: Kafka — distributed")

    elif mode == "redis":
        from shared.transport.redis_transport import (
            RedisTransport
        )
        _transport_instance = RedisTransport()
        logger.info("Transport mode: Redis — standalone")

    else:
        raise ValueError(
            f"Unknown TRANSPORT_MODE: {mode}. "
            f"Use 'redis' or 'kafka'."
        )

    return _transport_instance
