import redis
import json
import os
from typing import Optional, Any
from shared.utils.logger import get_logger

logger = get_logger("redis_client")


class RedisClient:
    _instance: Optional["RedisClient"] = None
    _client: Optional[redis.Redis] = None

    def __new__(cls) -> "RedisClient":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if self._client is None:
            self._client = redis.Redis(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                db=int(os.getenv("REDIS_DB", 0)),
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )
            logger.info("Redis client initialized")

    def push_to_queue(
        self,
        queue_name: str,
        data: dict
    ) -> int:
        try:
            payload = json.dumps(data)
            result = self._client.rpush(
                queue_name,
                payload
            )
            logger.debug(
                f"Pushed to {queue_name} — "
                f"queue size: {result}"
            )
            return result
        except Exception as e:
            logger.error(f"Redis push failed: {e}")
            raise

    def pop_from_queue(
        self,
        queue_name: str,
        timeout: int = 5
    ) -> Optional[dict]:
        try:
            result = self._client.blpop(
                queue_name,
                timeout=timeout
            )
            if result:
                _, payload = result
                return json.loads(payload)
            return None
        except Exception as e:
            logger.error(f"Redis pop failed: {e}")
            raise

    def pop_batch(
        self,
        queue_name: str,
        batch_size: int = 100
    ) -> list[dict]:
        messages = []
        try:
            for _ in range(batch_size):
                result = self._client.lpop(queue_name)
                if result is None:
                    break
                messages.append(json.loads(result))
            return messages
        except Exception as e:
            logger.error(f"Redis batch pop failed: {e}")
            raise

    def set_cache(
        self,
        key: str,
        value: Any,
        ttl_seconds: int = 1800
    ) -> bool:
        try:
            payload = json.dumps(value)
            self._client.setex(
                key,
                ttl_seconds,
                payload
            )
            return True
        except Exception as e:
            logger.error(f"Redis cache set failed: {e}")
            raise

    def get_cache(
        self,
        key: str
    ) -> Optional[Any]:
        try:
            result = self._client.get(key)
            if result:
                return json.loads(result)
            return None
        except Exception as e:
            logger.error(f"Redis cache get failed: {e}")
            raise

    def delete_cache(self, key: str) -> bool:
        try:
            self._client.delete(key)
            return True
        except Exception as e:
            logger.error(
                f"Redis cache delete failed: {e}"
            )
            raise

    def increment(
        self,
        key: str,
        amount: int = 1
    ) -> int:
        try:
            return self._client.incrby(key, amount)
        except Exception as e:
            logger.error(
                f"Redis increment failed: {e}"
            )
            raise

    def get_queue_size(
        self,
        queue_name: str
    ) -> int:
        try:
            return self._client.llen(queue_name)
        except Exception as e:
            logger.error(
                f"Redis queue size failed: {e}"
            )
            raise

    def set_hash(
        self,
        key: str,
        field: str,
        value: Any
    ) -> bool:
        try:
            self._client.hset(
                key,
                field,
                json.dumps(value)
            )
            return True
        except Exception as e:
            logger.error(f"Redis hset failed: {e}")
            raise

    def get_hash(
        self,
        key: str,
        field: str
    ) -> Optional[Any]:
        try:
            result = self._client.hget(key, field)
            if result:
                return json.loads(result)
            return None
        except Exception as e:
            logger.error(f"Redis hget failed: {e}")
            raise

    def health_check(self) -> bool:
        try:
            self._client.ping()
            return True
        except Exception:
            return False
