from abc import ABC, abstractmethod
from typing import Optional, Any


class EventTransport(ABC):
    """
    Abstract transport layer.
    Implementations: RedisTransport, KafkaTransport.
    Switch via TRANSPORT_MODE environment variable.
    """

    @abstractmethod
    def publish(
        self,
        topic: str,
        message: dict,
        priority: str = "STANDARD"
    ) -> bool:
        """Publish message to topic."""
        pass

    @abstractmethod
    def consume(
        self,
        topic: str,
        timeout: int = 5
    ) -> Optional[dict]:
        """Consume one message from topic."""
        pass

    @abstractmethod
    def consume_batch(
        self,
        topic: str,
        batch_size: int = 100,
        timeout: int = 5
    ) -> list[dict]:
        """Consume batch of messages from topic."""
        pass

    @abstractmethod
    def get_queue_depth(self, topic: str) -> int:
        """Get number of pending messages in topic."""
        pass

    @abstractmethod
    def health_check(self) -> bool:
        """Check transport connectivity."""
        pass
