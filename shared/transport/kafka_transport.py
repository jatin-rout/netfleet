import json
import os
from typing import Optional
from confluent_kafka import Producer, Consumer, KafkaError
from shared.transport.base import EventTransport
from shared.utils.logger import get_logger

logger = get_logger("kafka_transport")


class KafkaTransport(EventTransport):
    """
    Kafka based transport for distributed deployment.
    Suitable for fleets of 100K to 10M+ devices.
    Provides persistence, replay and audit trail.
    """

    def __init__(self):
        bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092"
        )
        group_id = os.getenv(
            "KAFKA_CONSUMER_GROUP",
            "netfleet-workers"
        )

        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 1000
        })

        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True
        })

        logger.info(
            f"KafkaTransport initialized — "
            f"brokers: {bootstrap_servers}"
        )

    def publish(
        self,
        topic: str,
        message: dict,
        priority: str = "STANDARD"
    ) -> bool:
        try:
            payload = json.dumps(message).encode("utf-8")
            self._producer.produce(
                topic,
                value=payload,
                headers={"priority": priority}
            )
            self._producer.flush()
            logger.debug(f"Published to Kafka topic {topic}")
            return True
        except Exception as e:
            logger.error(f"Kafka publish failed: {e}")
            raise

    def consume(
        self,
        topic: str,
        timeout: int = 5
    ) -> Optional[dict]:
        try:
            self._consumer.subscribe([topic])
            msg = self._consumer.poll(timeout=timeout)
            if msg is None:
                return None
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    return None
                raise Exception(f"Kafka error: {msg.error()}")
            return json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            logger.error(f"Kafka consume failed: {e}")
            raise

    def consume_batch(
        self,
        topic: str,
        batch_size: int = 100,
        timeout: int = 5
    ) -> list[dict]:
        messages = []
        self._consumer.subscribe([topic])
        while len(messages) < batch_size:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                break
            messages.append(
                json.loads(msg.value().decode("utf-8"))
            )
        return messages

    def get_queue_depth(self, topic: str) -> int:
        try:
            metadata = self._producer.list_topics(topic)
            partitions = metadata.topics[topic].partitions
            return len(partitions)
        except Exception as e:
            logger.error(f"Kafka queue depth failed: {e}")
            return 0

    def health_check(self) -> bool:
        try:
            metadata = self._producer.list_topics(
                timeout=5
            )
            return metadata is not None
        except Exception:
            return False
