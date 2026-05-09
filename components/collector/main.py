import signal
import time

from components.collector.failure_tracker import FailureTracker
from components.collector.thread_pool_manager import (
    CircuitBreakerOpen,
    ThreadPoolManager,
)
from shared.config.settings import CollectorConfig, RedisConfig, TransportConfig
from shared.transport.factory import create_transport
from shared.utils.logger import get_logger
from shared.utils.redis_client import RedisClient

logger = get_logger("collector.main")

_HIGH_QUEUE = RedisConfig.QUEUES["higher"]
_LOW_QUEUE = RedisConfig.QUEUES["lower"]
_RAW_RESULTS_QUEUE = RedisConfig.QUEUES["raw_results"]
_POLL_TIMEOUT_S = 5
_QUEUE_DEPTH_LOG_INTERVAL = 60   # log queue depths every N seconds

_running = True


def _shutdown(sig, frame):
    global _running
    logger.info(f"Signal {sig} received — initiating graceful shutdown")
    _running = False


def _log_queue_depths(redis: RedisClient) -> None:
    try:
        high_depth = redis.get_queue_size(_HIGH_QUEUE)
        low_depth = redis.get_queue_size(_LOW_QUEUE)
        raw_depth = redis.get_queue_size(_RAW_RESULTS_QUEUE)
        logger.info(
            f"Queue depths — "
            f"high_priority={high_depth} "
            f"standard_priority={low_depth} "
            f"raw_results_out={raw_depth}"
        )
    except Exception as exc:
        logger.warning(f"Could not read queue depths: {exc}")


def main():
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    transport = create_transport()
    redis = RedisClient()
    failure_tracker = FailureTracker()

    def emit_raw_output(msg: dict) -> None:
        transport.publish(_RAW_RESULTS_QUEUE, msg)

    pool = ThreadPoolManager(emit_callback=emit_raw_output)

    logger.info(
        f"Collector started — "
        f"transport={TransportConfig.MODE} "
        f"pool_size={CollectorConfig.THREAD_POOL_SIZE} "
        f"error_threshold={CollectorConfig.ERROR_THRESHOLD} "
        f"auth_retry_count={CollectorConfig.AUTH_RETRY_COUNT} "
        f"device_timeout={CollectorConfig.DEVICE_TIMEOUT}s "
        f"high_queue={_HIGH_QUEUE} "
        f"low_queue={_LOW_QUEUE}"
    )

    # Alternate between HIGH (priority) and LOW queues.
    # HIGH is tried first every iteration; if empty, LOW is tried.
    # This gives HIGH messages constant priority over LOW.
    queues = [_HIGH_QUEUE, _LOW_QUEUE]
    queue_idx = 0
    last_depth_log = time.monotonic()
    messages_dispatched = 0

    while _running:
        # ---- Circuit breaker check -----------------------------------
        if pool.is_open:
            logger.critical(
                "Circuit breaker is OPEN. Collector stopping. "
                f"messages_dispatched={messages_dispatched}"
            )
            break

        # ---- Periodic queue depth logging ----------------------------
        now = time.monotonic()
        if now - last_depth_log >= _QUEUE_DEPTH_LOG_INTERVAL:
            _log_queue_depths(redis)
            last_depth_log = now

        # ---- Consume next message ------------------------------------
        queue = queues[queue_idx % len(queues)]
        queue_idx += 1

        try:
            message = transport.consume(queue, timeout=_POLL_TIMEOUT_S)
        except Exception as exc:
            logger.error(
                f"Transport consume error on queue={queue}: "
                f"{type(exc).__name__}: {exc}"
            )
            continue

        if not message:
            continue

        # ---- Extract message fields ----------------------------------
        execution_id = message.get("execution_id", "")
        job_id = message.get("job_id", "")
        job_name = message.get("job_name", "")
        operation = message.get("operation", "")
        device = message.get("device", {})
        device_id = device.get("device_id", "unknown")
        ip = device.get("ip_address", "unknown")
        vendor = device.get("vendor", "unknown")
        protocol = (
            message.get("job_protocol") or device.get("protocol", "SSH")
        ).upper()

        logger.info(
            f"[exec={execution_id}] Dequeued from {queue} — "
            f"device={device_id} ip={ip} vendor={vendor} "
            f"protocol={protocol} operation={operation}"
        )

        # ---- Seed counters for this execution (idempotent) ----------
        try:
            failure_tracker.seed(
                execution_id=execution_id,
                job_id=job_id,
                job_name=job_name,
                operation=operation,
            )
        except Exception as seed_exc:
            logger.warning(
                f"[exec={execution_id}] Counter seed failed (continuing): {seed_exc}"
            )

        # ---- Submit to thread pool -----------------------------------
        try:
            pool.submit(message)
            messages_dispatched += 1
        except CircuitBreakerOpen as exc:
            logger.critical(str(exc))
            break
        except Exception as exc:
            logger.error(
                f"[exec={execution_id}] Failed to submit "
                f"device={device_id} to pool: "
                f"{type(exc).__name__}: {exc}"
            )

    # ---- Graceful shutdown -------------------------------------------
    logger.info(
        f"Collector shutting down — "
        f"messages_dispatched={messages_dispatched}. "
        f"Waiting for in-flight tasks..."
    )
    pool.shutdown(wait=True)
    logger.info("Collector stopped cleanly")


if __name__ == "__main__":
    main()
