import json
import redis
import os
from typing import Optional
from shared.transport.base import EventTransport
from shared.utils.logger import get_logger

logger = get_logger("redis_transport")


class RedisTransport(EventTransport):
    """
    Redis based transport for standalone deployment.
    Suitable for fleets up to 100K devices.
    Uses Redis Lists as queues — RPUSH/BLPOP pattern.
    """

    def __init__(self):
        self._client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("REDIS_DB", 0)),
            decode_responses=True,
            socket_connect_timeout=5,
            retry_on_timeout=True
        )
        logger.info("RedisTransport initialized")

    def publish(
        self,
        topic: str,
        message: dict,
        priority: str = "STANDARD"
    ) -> bool:
        try:
            payload = json.dumps(message)
            self._client.rpush(topic, payload)
            logger.debug(f"Published to {topic}")
            return True
        except Exception as e:
            logger.error(f"Redis publish failed: {e}")
            raise

    def consume(
        self,
        topic: str,
        timeout: int = 5
    ) -> Optional[dict]:
        try:
            result = self._client.blpop(
                topic,
                timeout=timeout
            )
            if result:
                _, payload = result
                return json.loads(payload)
            return None
        except Exception as e:
            logger.error(f"Redis consume failed: {e}")
            raise

    def consume_batch(
        self,
        topic: str,
        batch_size: int = 100,
        timeout: int = 5
    ) -> list[dict]:
        messages = []
        for _ in range(batch_size):
            result = self._client.lpop(topic)
            if result is None:
                break
            messages.append(json.loads(result))
        return messages

    def get_queue_depth(self, topic: str) -> int:
        try:
            return self._client.llen(topic)
        except Exception as e:
            logger.error(f"Redis queue depth failed: {e}")
            raise

    def health_check(self) -> bool:
        try:
            self._client.ping()
            return True
        except Exception:
            return False
