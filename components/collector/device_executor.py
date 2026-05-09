"""
DeviceExecutor — orchestrates one device's complete operation lifecycle.

Retry policy (matches CollectorConfig):
  Auth failure   : retry AUTH_RETRY_COUNT times (transient AAA caches)
  Timeout        : retry once (transient congestion)
  Connection err : skip — device is unreachable, retrying wastes pool threads
  All other      : no retry

The executor never raises — all exceptions are caught and classified so
the ThreadPoolManager thread stays alive and the circuit breaker gets
accurate counts.
"""
import json
import socket
import time
from datetime import datetime
from typing import Optional

from netmiko.exceptions import (
    NetmikoAuthenticationException,
    NetmikoTimeoutException,
)
from paramiko.ssh_exception import SSHException

from components.collector.failure_tracker import FailureTracker
from components.collector.handlers.base import HandlerResult
from components.collector.handlers.rest_handler import RESTAuthenticationError
from components.collector.operation_registry import OperationRegistry
from components.collector.protocol_factory import get_handler
from components.collector.transaction_manager import (
    TransactionError,
    TransactionManager,
)
from shared.config.settings import CollectorConfig
from shared.models.status import FailureReason, RawDeviceOutput
from shared.utils.logger import get_logger

logger = get_logger("collector.device_executor")

_RETRY_BACKOFF_S = 2   # seconds to wait between retry attempts


def _classify_exception(exc: Exception) -> FailureReason:
    """Map any exception to a FailureReason without raising."""
    exc_type = type(exc).__name__.lower()
    exc_msg = str(exc).lower()

    # Netmiko-specific
    if isinstance(exc, NetmikoAuthenticationException):
        return FailureReason.AUTH
    if isinstance(exc, NetmikoTimeoutException):
        return FailureReason.TIMEOUT

    # Paramiko SSH layer
    if isinstance(exc, SSHException):
        if "authentication" in exc_msg or "auth" in exc_msg:
            return FailureReason.AUTH
        return FailureReason.CONNECTION

    # Socket / OS layer
    if isinstance(exc, (socket.timeout, TimeoutError)):
        return FailureReason.TIMEOUT
    if isinstance(exc, (socket.error, OSError, EOFError, ConnectionRefusedError)):
        return FailureReason.CONNECTION

    # Message-based classification for wrapped exceptions
    if "auth" in exc_msg or "authentication" in exc_msg or "password" in exc_msg:
        return FailureReason.AUTH
    if "timeout" in exc_msg or "timed out" in exc_msg:
        return FailureReason.TIMEOUT
    if (
        "connection refused" in exc_msg
        or "no route to host" in exc_msg
        or "unreachable" in exc_msg
        or "network is down" in exc_msg
        or "connecterror" in exc_type
        or "econnreset" in exc_msg
        or "broken pipe" in exc_msg
    ):
        return FailureReason.CONNECTION

    return FailureReason.UNKNOWN


def _is_retryable(reason: FailureReason) -> bool:
    """True for failures where a second attempt might succeed."""
    return reason in (FailureReason.AUTH, FailureReason.TIMEOUT)


class DeviceExecutor:
    """
    Runs a single device operation end-to-end.

    Flow:
        1. Resolve protocol (job_protocol > device.protocol > SSH)
        2. Resolve operation type (GET / SET)
        3. Connect with retry
        4a. GET  — run each command, aggregate raw output
        4b. SET  — run TransactionManager (backup→apply→verify→rollback)
        5. Disconnect (always in finally)
        6. Update FailureTracker
        7. Emit RawDeviceOutput to output queue
    """

    def __init__(self, failure_tracker: FailureTracker):
        self._tracker = failure_tracker
        self._tx_manager = TransactionManager()

    def run(self, message: dict, emit_callback) -> HandlerResult:
        execution_id = message.get("execution_id", "")
        job_id = message.get("job_id", "")
        job_name = message.get("job_name", "")
        operation = message.get("operation", "")
        device = message.get("device", {})
        params = message.get("params", {})

        device_id = device.get("device_id", "unknown")
        ip = device.get("ip_address", "unknown")
        vendor = device.get("vendor", "cisco_ios")
        segment = device.get("segment", "")
        region = device.get("region", "")
        protocol = (
            message.get("job_protocol") or device.get("protocol", "SSH")
        ).upper()

        ctx = (
            f"[exec={execution_id} device={device_id} "
            f"ip={ip} vendor={vendor} proto={protocol} op={operation}]"
        )

        start_ms = int(time.monotonic() * 1000)
        logger.info(f"{ctx} Starting device operation")

        # --- Validate operation ----------------------------------------
        try:
            op_type = OperationRegistry.get_type(operation)
        except ValueError as exc:
            logger.error(f"{ctx} Unknown operation: {exc}")
            self._tracker.record_failure(execution_id, FailureReason.UNKNOWN)
            return HandlerResult(
                success=False,
                error_message=str(exc),
                failure_reason=FailureReason.UNKNOWN,
                duration_ms=int(time.monotonic() * 1000) - start_ms,
            )

        # Reject SET over SNMP — meaningless and dangerous
        if op_type == "SET" and protocol == "SNMP":
            logger.error(
                f"{ctx} SET operation is not supported over SNMP"
            )
            self._tracker.record_failure(execution_id, FailureReason.UNKNOWN)
            return HandlerResult(
                success=False,
                error_message="SET operations cannot be executed over SNMP",
                failure_reason=FailureReason.UNKNOWN,
                duration_ms=int(time.monotonic() * 1000) - start_ms,
            )

        # --- Validate SET payload ---------------------------------------
        if op_type == "SET":
            payload_commands = params.get("commands", [])
            if not payload_commands:
                logger.error(
                    f"{ctx} SET operation missing 'commands' in params"
                )
                self._tracker.record_failure(execution_id, FailureReason.UNKNOWN)
                return HandlerResult(
                    success=False,
                    error_message="SET operation requires 'commands' in message params",
                    failure_reason=FailureReason.UNKNOWN,
                    duration_ms=int(time.monotonic() * 1000) - start_ms,
                )
        else:
            payload_commands = []

        # --- Connect with retry ----------------------------------------
        enriched_device = self._enrich_credentials(device, params)
        handler = get_handler(protocol)
        connect_result = self._connect_with_retry(handler, enriched_device, ctx)

        if connect_result is not None:
            # connect_result holds the FailureReason if connection failed
            duration_ms = int(time.monotonic() * 1000) - start_ms
            logger.warning(
                f"{ctx} Connection failed after retries — "
                f"reason={connect_result.value} duration={duration_ms}ms"
            )
            self._tracker.record_failure(execution_id, connect_result)
            return HandlerResult(
                success=False,
                failure_reason=connect_result,
                duration_ms=duration_ms,
            )

        # --- Execute operation ------------------------------------------
        raw_output: Optional[str] = None
        failure_reason: Optional[FailureReason] = None

        try:
            if op_type == "GET":
                logger.debug(f"{ctx} Executing GET operation")
                raw_output = self._execute_get(
                    handler, operation, vendor, protocol, ctx
                )
            else:
                logger.debug(
                    f"{ctx} Executing SET operation "
                    f"({len(payload_commands)} config lines)"
                )
                raw_output = self._execute_set(
                    handler, operation, vendor, payload_commands, ctx
                )

        except (NetmikoAuthenticationException, RESTAuthenticationError) as exc:
            failure_reason = FailureReason.AUTH
            logger.warning(f"{ctx} Auth failure during execution: {exc}")
        except NetmikoTimeoutException as exc:
            failure_reason = FailureReason.TIMEOUT
            logger.warning(f"{ctx} Timeout during execution: {exc}")
        except (socket.timeout, TimeoutError) as exc:
            failure_reason = FailureReason.TIMEOUT
            logger.warning(f"{ctx} Socket timeout during execution: {exc}")
        except TransactionError as exc:
            failure_reason = (
                FailureReason.DIRTY_STATE if exc.dirty else FailureReason.ROLLBACK
            )
            logger.error(
                f"{ctx} Transaction {'DIRTY' if exc.dirty else 'ROLLED BACK'}: {exc}"
            )
        except (socket.error, OSError, EOFError, ConnectionRefusedError) as exc:
            failure_reason = FailureReason.CONNECTION
            logger.warning(f"{ctx} Connection error during execution: {type(exc).__name__}: {exc}")
        except SSHException as exc:
            failure_reason = _classify_exception(exc)
            logger.warning(f"{ctx} SSH error during execution: {exc}")
        except Exception as exc:
            failure_reason = _classify_exception(exc)
            logger.error(
                f"{ctx} Unexpected error during execution "
                f"type={type(exc).__name__} reason={failure_reason.value}: {exc}"
            )
        finally:
            try:
                handler.disconnect()
                logger.debug(f"{ctx} Handler disconnected")
            except Exception as disc_exc:
                logger.warning(f"{ctx} Disconnect error (ignored): {disc_exc}")

        duration_ms = int(time.monotonic() * 1000) - start_ms

        if failure_reason is not None:
            self._tracker.record_failure(execution_id, failure_reason)
            logger.warning(
                f"{ctx} Device FAILED — "
                f"reason={failure_reason.value} duration={duration_ms}ms"
            )
            return HandlerResult(
                success=False,
                failure_reason=failure_reason,
                duration_ms=duration_ms,
            )

        # --- Emit success -----------------------------------------------
        self._tracker.record_success(execution_id)
        logger.info(
            f"{ctx} Device SUCCESS — "
            f"output_bytes={len(raw_output or '')} duration={duration_ms}ms"
        )

        output_msg = RawDeviceOutput(
            execution_id=execution_id,
            job_id=job_id,
            device_id=device_id,
            vendor=vendor,
            region=region,
            segment=segment,
            operation=operation,
            raw_output=raw_output or "",
            collected_at=datetime.utcnow(),
            success=True,
        )
        try:
            emit_callback(output_msg.model_dump())
            logger.debug(f"{ctx} Raw output emitted to queue")
        except Exception as emit_exc:
            # Don't fail the device result because the emit failed —
            # the output is still counted as successful collection.
            logger.error(
                f"{ctx} Failed to emit raw output to queue: {emit_exc}"
            )

        return HandlerResult(
            success=True,
            output=raw_output,
            duration_ms=duration_ms,
        )

    # ------------------------------------------------------------------ #
    # Connection with retry                                                #
    # ------------------------------------------------------------------ #

    def _connect_with_retry(
        self,
        handler,
        device: dict,
        ctx: str,
    ) -> Optional[FailureReason]:
        """
        Attempt to connect, retrying based on failure type:
          - TIMEOUT  : retry once (transient network congestion)
          - AUTH     : retry AUTH_RETRY_COUNT times (transient AAA cache)
          - CONNECTION : no retry (device is genuinely down)

        Returns None on success, FailureReason on permanent failure.
        """
        ip = device.get("ip_address", "unknown")
        max_auth_retries = CollectorConfig.AUTH_RETRY_COUNT
        attempt = 0

        while True:
            attempt += 1
            try:
                logger.info(
                    f"{ctx} Connecting (attempt {attempt}) to {ip}"
                )
                handler.connect(device)
                logger.info(
                    f"{ctx} Connected successfully on attempt {attempt}"
                )
                return None  # success

            except (NetmikoAuthenticationException, RESTAuthenticationError) as exc:
                logger.warning(
                    f"{ctx} Auth failure on attempt {attempt}/{max_auth_retries}: {exc}"
                )
                if attempt < max_auth_retries:
                    logger.info(
                        f"{ctx} Retrying auth in {_RETRY_BACKOFF_S}s..."
                    )
                    time.sleep(_RETRY_BACKOFF_S)
                    continue
                logger.error(
                    f"{ctx} Auth failed after {attempt} attempt(s) — giving up"
                )
                return FailureReason.AUTH

            except NetmikoTimeoutException as exc:
                logger.warning(
                    f"{ctx} Timeout on connect attempt {attempt}: {exc}"
                )
                if attempt == 1:
                    logger.info(
                        f"{ctx} Retrying after timeout in {_RETRY_BACKOFF_S}s..."
                    )
                    time.sleep(_RETRY_BACKOFF_S)
                    continue
                logger.error(
                    f"{ctx} Timeout on retry — giving up"
                )
                return FailureReason.TIMEOUT

            except (socket.timeout, TimeoutError) as exc:
                logger.warning(
                    f"{ctx} Socket timeout on connect attempt {attempt}: {exc}"
                )
                if attempt == 1:
                    logger.info(
                        f"{ctx} Retrying after socket timeout in {_RETRY_BACKOFF_S}s..."
                    )
                    time.sleep(_RETRY_BACKOFF_S)
                    continue
                return FailureReason.TIMEOUT

            except SSHException as exc:
                reason = _classify_exception(exc)
                logger.warning(
                    f"{ctx} SSH error on connect attempt {attempt}: {exc}"
                )
                if reason == FailureReason.AUTH and attempt < max_auth_retries:
                    time.sleep(_RETRY_BACKOFF_S)
                    continue
                return reason

            except (socket.error, OSError, EOFError, ConnectionRefusedError) as exc:
                # Hard connection failure — no retry, device is down or filtered
                logger.error(
                    f"{ctx} Connection error (no retry): "
                    f"type={type(exc).__name__} msg={exc}"
                )
                return FailureReason.CONNECTION

            except Exception as exc:
                reason = _classify_exception(exc)
                logger.error(
                    f"{ctx} Unexpected connect error on attempt {attempt}: "
                    f"type={type(exc).__name__} reason={reason.value}: {exc}"
                )
                if _is_retryable(reason) and attempt == 1:
                    logger.info(
                        f"{ctx} Retrying in {_RETRY_BACKOFF_S}s..."
                    )
                    time.sleep(_RETRY_BACKOFF_S)
                    continue
                return reason

    # ------------------------------------------------------------------ #
    # GET execution                                                        #
    # ------------------------------------------------------------------ #

    def _execute_get(
        self,
        handler,
        operation: str,
        vendor: str,
        protocol: str,
        ctx: str,
    ) -> str:
        if protocol == "SNMP":
            oids = OperationRegistry.get_snmp_oids(operation, vendor)
            logger.debug(
                f"{ctx} SNMP GET — {len(oids)} OIDs for vendor={vendor}"
            )
            command = json.dumps(oids)
            output = handler.execute(
                command, timeout=CollectorConfig.DEVICE_TIMEOUT
            )
            logger.debug(
                f"{ctx} SNMP GET complete — "
                f"response_bytes={len(output)}"
            )
            return output
        else:
            commands = OperationRegistry.get_commands(operation, vendor)
            logger.debug(
                f"{ctx} CLI GET — {len(commands)} commands for vendor={vendor}"
            )
            outputs = []
            for i, cmd_spec in enumerate(commands, 1):
                cmd = cmd_spec["cmd"]
                timeout = cmd_spec.get("timeout", CollectorConfig.DEVICE_TIMEOUT)
                logger.debug(
                    f"{ctx} Executing command {i}/{len(commands)}: '{cmd}'"
                )
                out = handler.execute(cmd, timeout=timeout)
                logger.debug(
                    f"{ctx} Command {i} response — {len(out)} chars"
                )
                outputs.append(f"--- {cmd} ---\n{out}")
            return "\n".join(outputs)

    # ------------------------------------------------------------------ #
    # SET execution                                                        #
    # ------------------------------------------------------------------ #

    def _execute_set(
        self,
        handler,
        operation: str,
        vendor: str,
        payload_commands: list,
        ctx: str,
    ) -> str:
        logger.info(
            f"{ctx} SET transaction start — "
            f"operation={operation} vendor={vendor} "
            f"payload_lines={len(payload_commands)}"
        )
        steps = OperationRegistry.get_set_steps(operation, vendor)
        result = self._tx_manager.execute_set(
            handler, steps, vendor, payload_commands, ctx
        )
        logger.info(f"{ctx} SET transaction complete")
        return result

    # ------------------------------------------------------------------ #
    # Credential enrichment                                                #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _enrich_credentials(device: dict, params: dict) -> dict:
        """
        Merge runtime credentials from job params into the device dict.
        Params may carry: username, password, enable_secret, snmp_community,
        snmp_version, snmp_user, snmp_auth_key, snmp_priv_key, api_token,
        connect_timeout, session_timeout.
        """
        merged = dict(device)
        credential_keys = [
            "username", "password", "enable_secret",
            "snmp_community", "snmp_version", "snmp_user",
            "snmp_auth_key", "snmp_priv_key", "api_token",
            "connect_timeout", "session_timeout",
        ]
        for key in credential_keys:
            if key in params:
                merged[key] = params[key]
        return merged
