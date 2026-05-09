from shared.config.settings import SegmentConfig
from shared.transport.factory import get_transport
from shared.utils.logger import get_logger

logger = get_logger("orchestrator.priority_queue_manager")

_QUEUE_HIGHER = "queue_higher_segments"
_QUEUE_LOWER = "queue_lower_segments"


class PriorityQueueManager:
    """
    Routes per-device messages to the correct transport queue.

    HIGH  segments (Tier1, Tier2, Tier3) → queue_higher_segments
    STANDARD segments (Edge, Field)      → queue_lower_segments

    Selection is stateless and synchronous — the transport queues
    (Redis Lists or Kafka Topics) carry the messages; no in-process
    buffering is needed.
    """

    def __init__(self):
        self._transport = get_transport()

    def route(self, msg: dict) -> None:
        queue = (
            _QUEUE_HIGHER
            if msg.get("priority") == "HIGH"
            else _QUEUE_LOWER
        )
        self._transport.publish(topic=queue, message=msg)

    def start(self) -> None:
        logger.info("PriorityQueueManager ready")

    def stop(self) -> None:
        pass

    @staticmethod
    def segment_priority(segment: str) -> str:
        return (
            "HIGH"
            if segment in SegmentConfig.HIGHER_SEGMENTS
            else "STANDARD"
        )
