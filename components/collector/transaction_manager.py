"""
TransactionManager — three-phase SET operation lifecycle.

    Phase 1 — Backup  : read current device state, store as rollback baseline
    Phase 2 — Apply   : enter config mode, push payload, commit if needed
    Phase 3 — Verify  : re-read and confirm change is present in running config
    Rollback          : triggered if apply or verify fails; restores baseline

A TransactionError is raised on any failure:
    dirty=False — rollback succeeded, device is in original state
    dirty=True  — rollback ALSO failed, device may be partially configured

The ctx string (execution/device context) is passed in from DeviceExecutor
so every log line carries full device identity.
"""
import time

from components.collector.handlers.base import BaseProtocolHandler
from shared.utils.logger import get_logger

logger = get_logger("collector.transaction_manager")

_ROLLBACK_PLACEHOLDER = "__rollback_config__"


class TransactionError(Exception):
    """Raised when a SET transaction cannot complete or cannot be rolled back."""

    def __init__(self, message: str, dirty: bool = False):
        super().__init__(message)
        self.dirty = dirty


class TransactionManager:

    def execute_set(
        self,
        handler: BaseProtocolHandler,
        steps: dict,
        vendor: str,
        payload_commands: list,
        ctx: str = "",
    ) -> str:
        """
        Run backup → apply → verify → (rollback on failure).
        Returns the apply output on success.
        Raises TransactionError on failure.
        """
        # ---- Phase 1: Backup -----------------------------------------
        t0 = time.monotonic()
        backup_output = self._backup(handler, steps["backup"], ctx)
        backup_ms = int((time.monotonic() - t0) * 1000)
        logger.info(
            f"{ctx} TX BACKUP complete — "
            f"captured {len(backup_output)} chars in {backup_ms}ms"
        )

        # ---- Phase 2 & 3: Apply + Verify (with rollback on failure) ---
        try:
            t0 = time.monotonic()
            apply_output = self._apply(
                handler, steps["apply"], payload_commands, ctx
            )
            apply_ms = int((time.monotonic() - t0) * 1000)
            logger.info(
                f"{ctx} TX APPLY complete — "
                f"{len(payload_commands)} lines pushed in {apply_ms}ms"
            )

            t0 = time.monotonic()
            verify_output = self._verify(handler, steps["apply"], ctx)
            verify_ms = int((time.monotonic() - t0) * 1000)

            if not self._verify_ok(verify_output, payload_commands, ctx):
                raise ValueError(
                    "Post-apply verification failed: "
                    "expected config lines not found in device output"
                )
            logger.info(
                f"{ctx} TX VERIFY passed — "
                f"checked in {verify_ms}ms "
                f"(verify_output={len(verify_output)} chars)"
            )
            return apply_output

        except Exception as apply_exc:
            logger.error(
                f"{ctx} TX FAILED during apply/verify — "
                f"initiating rollback. Error: {apply_exc}"
            )
            try:
                t0 = time.monotonic()
                self._rollback(handler, steps["rollback"], backup_output, ctx)
                rollback_ms = int((time.monotonic() - t0) * 1000)
                logger.warning(
                    f"{ctx} TX ROLLBACK succeeded in {rollback_ms}ms — "
                    f"device restored to pre-change state"
                )
            except Exception as rollback_exc:
                logger.critical(
                    f"{ctx} TX ROLLBACK FAILED — "
                    f"device may be in DIRTY STATE. "
                    f"Rollback error: {rollback_exc}. "
                    f"Original error: {apply_exc}"
                )
                raise TransactionError(
                    f"Apply failed ({apply_exc}) AND rollback failed ({rollback_exc})",
                    dirty=True,
                ) from rollback_exc

            raise TransactionError(
                f"SET apply failed (rolled back cleanly): {apply_exc}",
                dirty=False,
            ) from apply_exc

    # ------------------------------------------------------------------ #
    # Phases                                                               #
    # ------------------------------------------------------------------ #

    def _backup(
        self,
        handler: BaseProtocolHandler,
        backup_cmds: list,
        ctx: str,
    ) -> str:
        logger.debug(
            f"{ctx} TX BACKUP — running {len(backup_cmds)} read command(s)"
        )
        outputs = []
        for i, spec in enumerate(backup_cmds, 1):
            cmd = spec["cmd"]
            timeout = spec.get("timeout", 60)
            logger.debug(f"{ctx} TX BACKUP cmd {i}/{len(backup_cmds)}: '{cmd}'")
            out = handler.execute(cmd, timeout=timeout)
            logger.debug(
                f"{ctx} TX BACKUP cmd {i} response: {len(out)} chars"
            )
            outputs.append(out)
        return "\n".join(outputs)

    def _apply(
        self,
        handler: BaseProtocolHandler,
        apply_spec: dict,
        payload_commands: list,
        ctx: str,
    ) -> str:
        logger.debug(
            f"{ctx} TX APPLY — entering config mode, "
            f"pushing {len(payload_commands)} line(s)"
        )
        handler.enter_config_mode()
        output = ""
        try:
            if hasattr(handler, "send_config_set"):
                output = handler.send_config_set(payload_commands, timeout=60)
                logger.debug(
                    f"{ctx} TX APPLY — send_config_set returned {len(output)} chars"
                )
            else:
                lines = []
                for i, cmd in enumerate(payload_commands, 1):
                    logger.debug(
                        f"{ctx} TX APPLY line {i}/{len(payload_commands)}: '{cmd}'"
                    )
                    line_out = handler.execute(cmd, timeout=60)
                    lines.append(line_out)
                output = "\n".join(lines)

            commit_cmd = apply_spec.get("commit_command")
            if commit_cmd:
                logger.debug(f"{ctx} TX APPLY — committing: '{commit_cmd}'")
                handler.execute(commit_cmd, timeout=30)
                logger.debug(f"{ctx} TX APPLY — commit sent")

        except Exception:
            logger.error(f"{ctx} TX APPLY error in config mode — exiting config mode")
            raise
        finally:
            handler.exit_config_mode()
            logger.debug(f"{ctx} TX APPLY — exited config mode")

        return output

    def _verify(
        self,
        handler: BaseProtocolHandler,
        apply_spec: dict,
        ctx: str,
    ) -> str:
        verify_cmd = apply_spec.get("verify_command")
        if not verify_cmd:
            logger.debug(f"{ctx} TX VERIFY — no verify command configured, skipping")
            return ""
        logger.debug(f"{ctx} TX VERIFY — running: '{verify_cmd}'")
        out = handler.execute(verify_cmd, timeout=60)
        logger.debug(f"{ctx} TX VERIFY — got {len(out)} chars")
        return out

    def _verify_ok(
        self,
        verify_output: str,
        payload_commands: list,
        ctx: str,
    ) -> bool:
        """
        Heuristic: every non-trivial payload line should appear somewhere in
        the post-apply running-config output.
        Mode transition lines (configure terminal, end, quit, commit, system-view)
        are skipped — they are not config stanzas.
        """
        skip_keywords = {
            "configure terminal", "config terminal", "system-view",
            "end", "quit", "exit", "commit",
        }
        meaningful = [
            cmd.strip().lower()
            for cmd in payload_commands
            if cmd.strip().lower() not in skip_keywords
        ]
        if not meaningful:
            logger.debug(
                f"{ctx} TX VERIFY — no meaningful lines to check, passing"
            )
            return True

        output_lower = verify_output.lower()
        failed = []
        for line in meaningful:
            words = line.split()
            found = any(word in output_lower for word in words)
            if not found:
                failed.append(line)

        if failed:
            logger.warning(
                f"{ctx} TX VERIFY — {len(failed)}/{len(meaningful)} lines NOT found "
                f"in device output: {failed[:5]}"  # log first 5 missing lines
            )
            return False

        logger.debug(
            f"{ctx} TX VERIFY — all {len(meaningful)} meaningful lines confirmed"
        )
        return True

    def _rollback(
        self,
        handler: BaseProtocolHandler,
        rollback_cmds: list,
        backup_output: str,
        ctx: str,
    ) -> None:
        """
        Build the rollback command list by substituting __rollback_config__
        with the previously captured backup lines, then push them.
        """
        expanded = []
        for spec in rollback_cmds:
            cmd = spec["cmd"]
            if cmd == _ROLLBACK_PLACEHOLDER:
                backup_lines = [
                    line for line in backup_output.splitlines()
                    if line.strip() and not line.strip().startswith("!")
                ]
                logger.debug(
                    f"{ctx} TX ROLLBACK — expanding placeholder "
                    f"to {len(backup_lines)} backup lines"
                )
                expanded.extend(backup_lines)
            else:
                expanded.append(cmd)

        logger.debug(
            f"{ctx} TX ROLLBACK — pushing {len(expanded)} lines"
        )
        handler.enter_config_mode()
        try:
            if hasattr(handler, "send_config_set"):
                handler.send_config_set(expanded, timeout=60)
            else:
                for cmd in expanded:
                    handler.execute(cmd, timeout=60)
        except Exception:
            logger.error(f"{ctx} TX ROLLBACK failed inside config mode")
            raise
        finally:
            handler.exit_config_mode()
            logger.debug(f"{ctx} TX ROLLBACK — exited config mode")
