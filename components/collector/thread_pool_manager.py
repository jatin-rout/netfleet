import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Callable

from components.collector.device_executor import DeviceExecutor
from components.collector.failure_tracker import FailureTracker
from shared.config.settings import CollectorConfig
from shared.utils.logger import get_logger

logger = get_logger("collector.thread_pool_manager")


class CircuitBreakerOpen(Exception):
    """Raised when the circuit breaker has tripped."""


class ThreadPoolManager:
    """
    Wraps a ThreadPoolExecutor with a circuit breaker.

    Circuit breaker logic:
        - error_count incremented on each device failure
        - error_count reset to 0 on each device success
        - When error_count >= error_threshold: breaker opens, no new tasks accepted
        - Opening is permanent until process restart (by design —
          a healthy fleet should not hit the threshold under normal conditions)

    Lock:
        The error counter is updated from worker threads. A lock ensures
        the increment + threshold check is atomic (no TOCTOU race).
    """

    def __init__(self, emit_callback: Callable[[dict], None]):
        self._pool = ThreadPoolExecutor(
            max_workers=CollectorConfig.THREAD_POOL_SIZE,
            thread_name_prefix="collector-worker",
        )
        self._emit_callback = emit_callback
        self._failure_tracker = FailureTracker()
        self._executor = DeviceExecutor(self._failure_tracker)

        self._error_count = 0
        self._error_threshold = CollectorConfig.ERROR_THRESHOLD
        self._lock = threading.Lock()
        self._breaker_open = False
        self._total_submitted = 0
        self._total_success = 0
        self._total_failed = 0

        logger.info(
            f"ThreadPoolManager initialised — "
            f"pool_size={CollectorConfig.THREAD_POOL_SIZE} "
            f"error_threshold={CollectorConfig.ERROR_THRESHOLD}"
        )

    @property
    def is_open(self) -> bool:
        return self._breaker_open

    def submit(self, message: dict) -> Future:
        if self._breaker_open:
            raise CircuitBreakerOpen(
                f"Circuit breaker OPEN — "
                f"error_count={self._error_threshold} reached. "
                f"Restart collector to reset."
            )
        with self._lock:
            self._total_submitted += 1
        device_id = message.get("device", {}).get("device_id", "unknown")
        execution_id = message.get("execution_id", "")
        logger.debug(
            f"[exec={execution_id}] Submitting device={device_id} to pool "
            f"(submitted={self._total_submitted} "
            f"ok={self._total_success} "
            f"fail={self._total_failed} "
            f"err_count={self._error_count}/{self._error_threshold})"
        )
        return self._pool.submit(self._run_device, message)

    def shutdown(self, wait: bool = True) -> None:
        logger.info(
            f"ThreadPoolManager shutting down — "
            f"total_submitted={self._total_submitted} "
            f"total_success={self._total_success} "
            f"total_failed={self._total_failed}"
        )
        self._pool.shutdown(wait=wait)
        logger.info("ThreadPoolManager shut down complete")

    # ------------------------------------------------------------------ #
    # Per-device worker                                                    #
    # ------------------------------------------------------------------ #

    def _run_device(self, message: dict) -> None:
        device_id = message.get("device", {}).get("device_id", "unknown")
        execution_id = message.get("execution_id", "")
        operation = message.get("operation", "")

        result = self._executor.run(message, self._emit_callback)

        with self._lock:
            if result.success:
                self._total_success += 1
                prev_err = self._error_count
                self._error_count = 0
                if prev_err > 0:
                    logger.info(
                        f"[exec={execution_id}] Circuit breaker reset — "
                        f"device={device_id} success after {prev_err} consecutive errors"
                    )
                logger.debug(
                    f"[exec={execution_id}] device={device_id} op={operation} "
                    f"SUCCESS duration={result.duration_ms}ms "
                    f"(pool_ok={self._total_success} "
                    f"pool_fail={self._total_failed})"
                )
            else:
                self._total_failed += 1
                self._error_count += 1
                logger.warning(
                    f"[exec={execution_id}] device={device_id} op={operation} "
                    f"FAILED reason={result.failure_reason} "
                    f"duration={result.duration_ms}ms "
                    f"— consecutive_errors={self._error_count}/{self._error_threshold} "
                    f"(pool_ok={self._total_success} pool_fail={self._total_failed})"
                )
                if self._error_count >= self._error_threshold:
                    self._breaker_open = True
                    logger.critical(
                        f"CIRCUIT BREAKER OPEN — "
                        f"{self._error_count} consecutive failures reached threshold "
                        f"{self._error_threshold}. "
                        f"Collector will stop accepting new tasks. "
                        f"total_submitted={self._total_submitted} "
                        f"total_success={self._total_success} "
                        f"total_failed={self._total_failed}"
                    )
