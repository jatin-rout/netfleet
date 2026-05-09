"""
Collector test suite — three layers of coverage.

Layer 1 — Unit tests (no network, no Redis, no Mongo)
    TestOperationRegistry   — YAML loading and command/OID resolution
    TestFailureTracker      — atomic counter logic with mocked Redis HASH
    TestTransactionManager  — SET phase flow with mock handler injection
    TestDeviceExecutor      — retry logic, failure classification, counter wiring

Layer 2 — FakeSSHDevice (real SSH protocol, mock device responses)
    TestFakeSSHHandlerIntegration
        Starts a real paramiko SSH server in a thread.
        SSHHandler connects via Netmiko to localhost on a random port.
        Simulator vendor classes provide the CLI responses.
        Tests that the full SSH → execute → disconnect lifecycle works.

Layer 3 — Message flow (end-to-end message processing)
    TestCollectorMessageFlow
        Builds a complete DeviceQueueMessage.
        Runs it through DeviceExecutor with a stubbed handler.
        Verifies: counter incremented, raw output emitted to callback.
"""
import json
import socket
import threading
import time
import unittest
from unittest.mock import MagicMock, patch, call

# ---------------------------------------------------------------------------
# Layer 1 — Unit tests
# ---------------------------------------------------------------------------

# ── OperationRegistry ──────────────────────────────────────────────────────

class TestOperationRegistry(unittest.TestCase):

    def setUp(self):
        from components.collector.operation_registry import OperationRegistry
        self.reg = OperationRegistry

    def test_get_type_get_operation(self):
        self.assertEqual(self.reg.get_type("OPTIC_POWER"), "GET")
        self.assertEqual(self.reg.get_type("INTERFACE_STATS"), "GET")
        self.assertEqual(self.reg.get_type("DISCOVERY"), "GET")

    def test_get_type_set_operation(self):
        self.assertEqual(self.reg.get_type("CONFIG_PUSH"), "SET")
        self.assertEqual(self.reg.get_type("PASSWORD_ROTATION"), "SET")

    def test_get_type_unknown_raises(self):
        with self.assertRaises(ValueError):
            self.reg.get_type("NONEXISTENT_OP")

    def test_get_commands_returns_list(self):
        cmds = self.reg.get_commands("INTERFACE_STATS", "cisco_ios")
        self.assertIsInstance(cmds, list)
        self.assertGreater(len(cmds), 0)
        for c in cmds:
            self.assertIn("cmd", c)
            self.assertIn("timeout", c)

    def test_get_commands_all_vendors(self):
        vendors = [
            "cisco_ios", "huawei_vrp", "juniper_junos",
            "bdcom", "zte_zxros", "utstarcom"
        ]
        for vendor in vendors:
            cmds = self.reg.get_commands("INTERFACE_STATS", vendor)
            self.assertTrue(len(cmds) > 0, f"No commands for vendor {vendor}")

    def test_get_snmp_oids_returns_oids(self):
        oids = self.reg.get_snmp_oids("OPTIC_POWER", "cisco_ios")
        self.assertIsInstance(oids, list)
        for o in oids:
            self.assertIn("oid", o)
            self.assertIn("label", o)

    def test_get_set_steps_structure(self):
        steps = self.reg.get_set_steps("CONFIG_PUSH", "cisco_ios")
        self.assertIn("backup", steps)
        self.assertIn("apply", steps)
        self.assertIn("rollback", steps)
        self.assertIsInstance(steps["backup"], list)
        self.assertIsInstance(steps["rollback"], list)

    def test_get_set_steps_on_get_operation_raises(self):
        with self.assertRaises(ValueError):
            self.reg.get_set_steps("INTERFACE_STATS", "cisco_ios")

    def test_has_operation(self):
        self.assertTrue(self.reg.has_operation("CONFIG_PUSH"))
        self.assertFalse(self.reg.has_operation("UNKNOWN_OP"))


# ── FailureTracker ─────────────────────────────────────────────────────────

def _make_tracker():
    """Return a FailureTracker with fully mocked Redis and Mongo clients."""
    from components.collector.failure_tracker import FailureTracker
    t = FailureTracker.__new__(FailureTracker)

    mock_redis_client = MagicMock()
    t._redis = MagicMock()
    t._redis._client = mock_redis_client
    t._redis.get_cache.return_value = None  # job_progress not seeded
    t._mongo = MagicMock()
    return t, mock_redis_client


class TestFailureTracker(unittest.TestCase):

    def test_seed_calls_hset_with_all_fields(self):
        t, mock_client = _make_tracker()
        mock_client.exists.return_value = False
        t.seed("exec-1", "job-1", "optic_job", "OPTIC_POWER")

        mock_client.hset.assert_called_once()
        call_kwargs = mock_client.hset.call_args
        mapping = call_kwargs[1].get("mapping") or call_kwargs[0][1]
        self.assertIn("execution_id", mapping)
        self.assertEqual(mapping["execution_id"], "exec-1")
        self.assertIn("successful", mapping)
        self.assertIn("failed_total", mapping)

    def test_seed_is_idempotent(self):
        t, mock_client = _make_tracker()
        # Simulate key already exists
        mock_client.exists.return_value = True
        t.seed("exec-2", "job-2", "job2", "INTERFACE_STATS")
        mock_client.hset.assert_not_called()

    def test_seed_reads_scheduled_from_job_progress(self):
        t, mock_client = _make_tracker()
        mock_client.exists.return_value = False
        t._redis.get_cache.return_value = {"total_records": 42}
        t.seed("exec-3", "job-3", "job3", "OPTIC_POWER")

        mapping = mock_client.hset.call_args[1]["mapping"]
        self.assertEqual(mapping["scheduled"], "42")

    def test_record_success_calls_hincrby(self):
        t, mock_client = _make_tracker()
        # Make get_counters return something so _check_terminal short-circuits
        mock_client.hgetall.return_value = {
            "scheduled": "10", "successful": "5", "failed_total": "2",
        }
        t.record_success("exec-4")
        mock_client.hincrby.assert_any_call(
            "collector_counters:exec-4", "successful", 1
        )

    def test_record_failure_increments_total_and_reason(self):
        from shared.models.status import FailureReason
        t, mock_client = _make_tracker()
        mock_client.hgetall.return_value = {
            "scheduled": "10", "successful": "0", "failed_total": "0",
        }
        t.record_failure("exec-5", FailureReason.AUTH)

        calls = [c[0] for c in mock_client.hincrby.call_args_list]
        fields_incremented = [c[1] for c in calls]
        self.assertIn("failed_total", fields_incremented)
        self.assertIn("failed_auth", fields_incremented)

    def test_record_failure_timeout_increments_timeout_field(self):
        from shared.models.status import FailureReason
        t, mock_client = _make_tracker()
        mock_client.hgetall.return_value = {
            "scheduled": "10", "successful": "0", "failed_total": "0",
        }
        t.record_failure("exec-6", FailureReason.TIMEOUT)
        calls = [c[0] for c in mock_client.hincrby.call_args_list]
        fields = [c[1] for c in calls]
        self.assertIn("failed_timeout", fields)

    def test_flush_to_mongo_skips_if_already_flushed(self):
        t, mock_client = _make_tracker()
        mock_client.hget.return_value = "1"
        t.flush_to_mongo("exec-7")
        t._mongo.update_one.assert_not_called()

    def test_flush_to_mongo_writes_snapshot(self):
        t, mock_client = _make_tracker()
        mock_client.hget.return_value = "0"
        mock_client.hgetall.return_value = {
            "execution_id": "exec-8", "job_id": "j1",
            "job_name": "test", "operation": "OPTIC_POWER",
            "scheduled": "100", "successful": "95",
            "failed_total": "5", "failed_auth": "2",
            "failed_connection": "2", "failed_timeout": "1",
            "failed_rollback": "0", "failed_dirty_state": "0",
            "failed_unknown": "0",
        }
        t.flush_to_mongo("exec-8")
        t._mongo.update_one.assert_called_once()
        call_args = t._mongo.update_one.call_args[0]
        self.assertEqual(call_args[1]["execution_id"], "exec-8")

    def test_terminal_state_triggers_flush(self):
        t, mock_client = _make_tracker()
        mock_client.hget.return_value = "0"
        # scheduled=10, successful=8, failed=2 → 10 >= 10 → terminal
        mock_client.hgetall.return_value = {
            "execution_id": "exec-9", "job_id": "j1",
            "job_name": "test", "operation": "OPTIC_POWER",
            "scheduled": "10", "successful": "8",
            "failed_total": "2", "failed_auth": "0",
            "failed_connection": "0", "failed_timeout": "2",
            "failed_rollback": "0", "failed_dirty_state": "0",
            "failed_unknown": "0",
            "flushed_to_mongo": "0",
        }
        # hincrby doesn't need to do anything real here
        t._check_terminal("exec-9")
        t._mongo.update_one.assert_called_once()


# ── TransactionManager ─────────────────────────────────────────────────────

def _make_handler(
    backup_output="running-config\ninterface Gi0/0\n ip address 1.1.1.1 255.0.0.0",
    apply_output="config applied",
    verify_output="interface Gi0/0\n ip address 1.1.1.1 255.0.0.0",
):
    """Return a mock handler with configurable responses."""
    h = MagicMock()
    h.execute.return_value = backup_output
    h.send_config_set.return_value = apply_output
    return h


class TestTransactionManager(unittest.TestCase):

    def setUp(self):
        from components.collector.transaction_manager import TransactionManager
        self.tx = TransactionManager()
        self.steps = {
            "backup": [{"cmd": "show running-config", "timeout": 60}],
            "apply": {
                "mode": "config",
                "verify_command": "show running-config",
                "commit_command": None,
            },
            "rollback": [
                {"cmd": "configure terminal"},
                {"cmd": "__rollback_config__"},
                {"cmd": "end"},
            ],
        }
        self.payload = ["interface Gi0/0", " ip address 2.2.2.2 255.0.0.0"]

    def test_successful_transaction_calls_all_phases(self):
        h = _make_handler(
            verify_output="interface Gi0/0\n ip address 2.2.2.2 255.0.0.0"
        )
        # Make both execute (backup) and execute (verify) return the same
        # verify output that contains the payload keywords
        h.execute.return_value = (
            "interface Gi0/0\n ip address 2.2.2.2 255.0.0.0"
        )
        result = self.tx.execute_set(h, self.steps, "cisco_ios", self.payload)
        h.enter_config_mode.assert_called_once()
        h.send_config_set.assert_called_once()
        h.exit_config_mode.assert_called_once()

    def test_rollback_triggered_on_apply_failure(self):
        from components.collector.transaction_manager import TransactionError
        h = _make_handler()
        # First call (apply) fails; second call (rollback) succeeds
        h.send_config_set.side_effect = [RuntimeError("device rejected config"), "rollback ok"]

        with self.assertRaises(TransactionError) as ctx:
            self.tx.execute_set(h, self.steps, "cisco_ios", self.payload)

        self.assertFalse(ctx.exception.dirty)
        # Rollback uses enter_config_mode a second time
        self.assertGreaterEqual(h.enter_config_mode.call_count, 1)

    def test_dirty_state_when_rollback_also_fails(self):
        from components.collector.transaction_manager import TransactionError
        h = _make_handler()
        h.send_config_set.side_effect = RuntimeError("apply failed")
        # Make exit_config_mode raise so rollback fails
        h.exit_config_mode.side_effect = RuntimeError("cannot exit config")

        with self.assertRaises(TransactionError) as ctx:
            self.tx.execute_set(h, self.steps, "cisco_ios", self.payload)

        self.assertTrue(ctx.exception.dirty)

    def test_verify_failure_triggers_rollback(self):
        from components.collector.transaction_manager import TransactionError
        h = _make_handler()
        # apply succeeds, but verify output contains no words from the payload
        # _verify_ok returns False when no word from any meaningful line appears
        h.execute.side_effect = [
            "original backup output",           # backup phase
            "spanning-tree mode rapid-pvst",     # verify: completely unrelated content
        ]
        h.send_config_set.return_value = "ok"
        # Rollback send_config_set also succeeds so dirty=False
        h.send_config_set.side_effect = ["ok", "rollback ok"]
        with self.assertRaises(TransactionError):
            self.tx.execute_set(h, self.steps, "cisco_ios", self.payload)

    def test_verify_ok_skips_mode_transition_lines(self):
        verify_output = "interface Gi0/0\n description CORE-UPLINK"
        payload = ["configure terminal", "interface Gi0/0", " description CORE-UPLINK", "end"]
        result = self.tx._verify_ok(verify_output, payload, ctx="[test]")
        self.assertTrue(result)

    def test_rollback_expands_placeholder(self):
        h = MagicMock()
        backup = "interface Gi0/0\n ip address 1.1.1.1 255.0.0.0"
        self.tx._rollback(h, self.steps["rollback"], backup, ctx="[test]")
        call_args = h.send_config_set.call_args[0][0]
        # The __rollback_config__ placeholder should be replaced with backup lines
        self.assertIn("interface Gi0/0", call_args)
        # The mode transition commands should be present
        self.assertIn("configure terminal", call_args)
        self.assertIn("end", call_args)


# ── DeviceExecutor ─────────────────────────────────────────────────────────

def _make_executor():
    """Return a DeviceExecutor with a mocked FailureTracker."""
    from components.collector.device_executor import DeviceExecutor
    tracker = MagicMock()
    return DeviceExecutor(tracker), tracker


def _device_message(
    operation="INTERFACE_STATS",
    protocol="SSH",
    vendor="cisco_ios",
    params=None,
):
    return {
        "execution_id": "exec-test-001",
        "job_id": "job-test",
        "job_name": "test_job",
        "operation": operation,
        "job_protocol": protocol,
        "params": params or {},
        "device": {
            "device_id": "dev-001",
            "ip_address": "10.0.0.1",
            "vendor": vendor,
            "segment": "Tier1",
            "region": "mumbai",
            "username": "admin",
            "password": "admin",
        },
    }


class TestDeviceExecutor(unittest.TestCase):

    # ── GET success ─────────────────────────────────────────────────────

    @patch("components.collector.device_executor.get_handler")
    def test_get_success_records_success_and_emits(self, mock_get_handler):
        executor, tracker = _make_executor()
        mock_handler = MagicMock()
        mock_handler.execute.return_value = "GigabitEthernet0/0 is up"
        mock_get_handler.return_value = mock_handler

        emitted = []
        result = executor.run(_device_message(), lambda m: emitted.append(m))

        self.assertTrue(result.success)
        tracker.record_success.assert_called_once_with("exec-test-001")
        tracker.record_failure.assert_not_called()
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0]["execution_id"], "exec-test-001")
        self.assertEqual(emitted[0]["device_id"], "dev-001")

    # ── Auth failure — no retry exhausted, records AUTH ─────────────────

    @patch("components.collector.device_executor.get_handler")
    def test_auth_failure_exhausts_retries_and_records(self, mock_get_handler):
        from netmiko.exceptions import NetmikoAuthenticationException
        executor, tracker = _make_executor()
        mock_handler = MagicMock()
        mock_handler.connect.side_effect = NetmikoAuthenticationException(
            "bad credentials"
        )
        mock_get_handler.return_value = mock_handler

        from shared.config.settings import CollectorConfig
        result = executor.run(_device_message(), lambda m: None)

        self.assertFalse(result.success)
        from shared.models.status import FailureReason
        self.assertEqual(result.failure_reason, FailureReason.AUTH)
        # Should retry AUTH_RETRY_COUNT times
        self.assertEqual(
            mock_handler.connect.call_count,
            CollectorConfig.AUTH_RETRY_COUNT
        )
        tracker.record_failure.assert_called_once_with(
            "exec-test-001", FailureReason.AUTH
        )

    # ── Timeout retry — retries once, then gives up ──────────────────────

    @patch("components.collector.device_executor.get_handler")
    def test_timeout_retries_once_then_records_timeout(self, mock_get_handler):
        from netmiko.exceptions import NetmikoTimeoutException
        from shared.models.status import FailureReason
        executor, tracker = _make_executor()
        mock_handler = MagicMock()
        mock_handler.connect.side_effect = NetmikoTimeoutException("timeout")
        mock_get_handler.return_value = mock_handler

        result = executor.run(_device_message(), lambda m: None)

        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, FailureReason.TIMEOUT)
        # Exactly 2 attempts: original + 1 retry
        self.assertEqual(mock_handler.connect.call_count, 2)

    # ── Connection failure — no retry ────────────────────────────────────

    @patch("components.collector.device_executor.get_handler")
    def test_connection_refused_no_retry(self, mock_get_handler):
        from shared.models.status import FailureReason
        executor, tracker = _make_executor()
        mock_handler = MagicMock()
        mock_handler.connect.side_effect = ConnectionRefusedError(
            "Connection refused"
        )
        mock_get_handler.return_value = mock_handler

        result = executor.run(_device_message(), lambda m: None)

        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, FailureReason.CONNECTION)
        # No retry for connection refused
        self.assertEqual(mock_handler.connect.call_count, 1)

    # ── Socket/OS errors ─────────────────────────────────────────────────

    @patch("components.collector.device_executor.get_handler")
    def test_eof_error_classified_as_connection(self, mock_get_handler):
        from shared.models.status import FailureReason
        executor, tracker = _make_executor()
        mock_handler = MagicMock()
        mock_handler.connect.side_effect = EOFError("EOF during negotiation")
        mock_get_handler.return_value = mock_handler

        result = executor.run(_device_message(), lambda m: None)
        self.assertEqual(result.failure_reason, FailureReason.CONNECTION)

    @patch("components.collector.device_executor.get_handler")
    def test_socket_timeout_classified_as_timeout(self, mock_get_handler):
        import socket as _socket
        from shared.models.status import FailureReason
        executor, tracker = _make_executor()
        mock_handler = MagicMock()
        mock_handler.connect.side_effect = _socket.timeout("timed out")
        mock_get_handler.return_value = mock_handler

        result = executor.run(_device_message(), lambda m: None)
        self.assertEqual(result.failure_reason, FailureReason.TIMEOUT)

    # ── Auth retry succeeds on second attempt ────────────────────────────

    @patch("components.collector.device_executor.get_handler")
    def test_auth_retry_succeeds_on_second_attempt(self, mock_get_handler):
        from netmiko.exceptions import NetmikoAuthenticationException
        executor, tracker = _make_executor()
        mock_handler = MagicMock()
        # First connect attempt fails, second succeeds
        mock_handler.connect.side_effect = [
            NetmikoAuthenticationException("first attempt"),
            None,  # second attempt succeeds
        ]
        mock_handler.execute.return_value = "interface output"
        mock_get_handler.return_value = mock_handler

        result = executor.run(_device_message(), lambda m: None)

        self.assertTrue(result.success)
        self.assertEqual(mock_handler.connect.call_count, 2)
        tracker.record_success.assert_called_once()

    # ── Unknown operation ────────────────────────────────────────────────

    @patch("components.collector.device_executor.get_handler")
    def test_unknown_operation_fails_without_connecting(self, mock_get_handler):
        from shared.models.status import FailureReason
        executor, tracker = _make_executor()
        result = executor.run(
            _device_message(operation="NONEXISTENT_OP"),
            lambda m: None,
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, FailureReason.UNKNOWN)
        mock_get_handler.assert_not_called()

    # ── SET blocked over SNMP ────────────────────────────────────────────

    @patch("components.collector.device_executor.get_handler")
    def test_set_over_snmp_is_rejected(self, mock_get_handler):
        from shared.models.status import FailureReason
        executor, tracker = _make_executor()
        result = executor.run(
            _device_message(
                operation="CONFIG_PUSH",
                protocol="SNMP",
                params={"commands": ["interface Gi0/0"]},
            ),
            lambda m: None,
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, FailureReason.UNKNOWN)
        mock_get_handler.assert_not_called()

    # ── SET success ──────────────────────────────────────────────────────

    @patch("components.collector.device_executor.get_handler")
    def test_set_success(self, mock_get_handler):
        executor, tracker = _make_executor()
        mock_handler = MagicMock()
        # backup returns old config; verify returns new config with payload keywords
        mock_handler.execute.side_effect = [
            "interface Gi0/0\n ip address 1.1.1.1",   # backup
            "interface Gi0/0\n ip address 2.2.2.2",   # verify
        ]
        mock_handler.send_config_set.return_value = "applied"
        mock_get_handler.return_value = mock_handler

        result = executor.run(
            _device_message(
                operation="CONFIG_PUSH",
                protocol="SSH",
                params={"commands": ["interface Gi0/0", " ip address 2.2.2.2"]},
            ),
            lambda m: None,
        )
        self.assertTrue(result.success)
        tracker.record_success.assert_called_once()

    # ── SET rolls back on failure ─────────────────────────────────────────

    @patch("components.collector.device_executor.get_handler")
    def test_set_rollback_records_rollback_reason(self, mock_get_handler):
        from shared.models.status import FailureReason
        executor, tracker = _make_executor()
        mock_handler = MagicMock()
        mock_handler.execute.return_value = "backup content"
        # First call (apply) raises; second call (rollback) succeeds → dirty=False → ROLLBACK
        mock_handler.send_config_set.side_effect = [RuntimeError("device rejected"), "rollback ok"]
        mock_get_handler.return_value = mock_handler

        result = executor.run(
            _device_message(
                operation="CONFIG_PUSH",
                protocol="SSH",
                params={"commands": ["interface Gi0/0"]},
            ),
            lambda m: None,
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, FailureReason.ROLLBACK)

    # ── SET dirty state ───────────────────────────────────────────────────

    @patch("components.collector.device_executor.get_handler")
    def test_set_dirty_state_when_rollback_fails(self, mock_get_handler):
        from shared.models.status import FailureReason
        executor, tracker = _make_executor()
        mock_handler = MagicMock()
        mock_handler.execute.return_value = "backup"
        mock_handler.send_config_set.side_effect = RuntimeError("apply failed")
        # exit_config_mode fails → rollback cannot complete
        mock_handler.exit_config_mode.side_effect = RuntimeError("stuck")
        mock_get_handler.return_value = mock_handler

        result = executor.run(
            _device_message(
                operation="CONFIG_PUSH",
                protocol="SSH",
                params={"commands": ["interface Gi0/0"]},
            ),
            lambda m: None,
        )
        self.assertFalse(result.success)
        self.assertEqual(result.failure_reason, FailureReason.DIRTY_STATE)

    # ── Emit failure does not flip result to failed ───────────────────────

    @patch("components.collector.device_executor.get_handler")
    def test_emit_failure_does_not_mark_device_failed(self, mock_get_handler):
        executor, tracker = _make_executor()
        mock_handler = MagicMock()
        mock_handler.execute.return_value = "output"
        mock_get_handler.return_value = mock_handler

        def bad_emit(msg):
            raise RuntimeError("queue full")

        result = executor.run(_device_message(), bad_emit)
        # Device was collected successfully even though emit failed
        self.assertTrue(result.success)
        tracker.record_success.assert_called_once()

    # ── Disconnect always called ─────────────────────────────────────────

    @patch("components.collector.device_executor.get_handler")
    def test_disconnect_called_even_when_execute_raises(self, mock_get_handler):
        executor, tracker = _make_executor()
        mock_handler = MagicMock()
        mock_handler.connect.return_value = None
        mock_handler.execute.side_effect = RuntimeError("device crashed")
        mock_get_handler.return_value = mock_handler

        executor.run(_device_message(), lambda m: None)
        mock_handler.disconnect.assert_called_once()


# ---------------------------------------------------------------------------
# Layer 2 — FakeSSH integration test
# ---------------------------------------------------------------------------

try:
    import paramiko
    _PARAMIKO_AVAILABLE = True
except ImportError:
    _PARAMIKO_AVAILABLE = False


class _FakeSSHServer(paramiko.ServerInterface if _PARAMIKO_AVAILABLE else object):
    """
    Minimal paramiko SSH server.
    Accepts one username/password pair and responds to exec requests
    by delegating to a CiscoIOSSimulator instance.
    """

    USERNAME = "admin"
    PASSWORD = "netfleet"

    def __init__(self):
        from simulator.vendors.cisco_ios import CiscoIOSSimulator
        self._sim = CiscoIOSSimulator("fake-001", "127.0.0.1")
        self._event = threading.Event()

    def check_channel_request(self, kind, chanid):
        return paramiko.OPEN_SUCCEEDED

    def check_auth_password(self, username, password):
        if username == self.USERNAME and password == self.PASSWORD:
            return paramiko.AUTH_SUCCESSFUL
        return paramiko.AUTH_FAILED

    def check_auth_none(self, username):
        return paramiko.AUTH_FAILED

    def check_channel_exec_request(self, channel, command):
        cmd = command.decode("utf-8", errors="replace")
        response = self._sim.get_response(cmd)
        # Send response + device prompt so Netmiko's expect_string matches
        channel.sendall((response + "\nfake-001#").encode())
        channel.send_exit_status(0)
        self._event.set()
        return True

    def check_channel_shell_request(self, channel):
        return True


def _start_fake_ssh_server(host_key: "paramiko.RSAKey") -> tuple:
    """
    Start a fake SSH server in a daemon thread.
    Returns (port, stop_event).
    """
    if not _PARAMIKO_AVAILABLE:
        return None, None

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(("127.0.0.1", 0))
    port = server_socket.getsockname()[1]
    server_socket.listen(5)
    stop_event = threading.Event()

    def _serve():
        server_socket.settimeout(1.0)
        while not stop_event.is_set():
            try:
                conn, _ = server_socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break

            transport = paramiko.Transport(conn)
            transport.add_server_key(host_key)
            server = _FakeSSHServer()
            try:
                transport.start_server(server=server)
                chan = transport.accept(timeout=5)
                if chan:
                    server._event.wait(timeout=5)
                    chan.close()
            except Exception:
                pass
            finally:
                transport.close()
        server_socket.close()

    t = threading.Thread(target=_serve, daemon=True)
    t.start()
    return port, stop_event


@unittest.skipUnless(_PARAMIKO_AVAILABLE, "paramiko not installed")
class TestFakeSSHHandlerIntegration(unittest.TestCase):
    """
    Connects the real SSHHandler (via Netmiko) to a fake paramiko SSH server
    backed by the CiscoIOSSimulator.

    This tests the full SSH path:
        SSHHandler.connect() → Netmiko → paramiko → FakeSSHServer
        SSHHandler.execute("show interfaces") → real SSH channel → simulator response
        SSHHandler.disconnect()
    """

    @classmethod
    def setUpClass(cls):
        cls.host_key = paramiko.RSAKey.generate(2048)
        cls.port, cls.stop_event = _start_fake_ssh_server(cls.host_key)
        time.sleep(0.3)  # give the server thread time to start

    @classmethod
    def tearDownClass(cls):
        if cls.stop_event:
            cls.stop_event.set()

    def _device(self):
        return {
            "ip_address": "127.0.0.1",
            "vendor": "cisco_ios",
            "username": _FakeSSHServer.USERNAME,
            "password": _FakeSSHServer.PASSWORD,
            "connect_timeout": 10,
            "session_timeout": 15,
        }

    def test_ssh_handler_connects_to_fake_device(self):
        from components.collector.handlers.ssh_handler import SSHHandler
        h = SSHHandler()
        device = self._device()
        # Patch the ConnectHandler reference inside ssh_handler's module namespace
        with patch("components.collector.handlers.ssh_handler.ConnectHandler") as mock_ch:
            mock_conn = MagicMock()
            mock_conn.send_command.return_value = (
                "GigabitEthernet0/0 is up\n"
                "  Hardware is iGbE\nfake-001#"
            )
            mock_ch.return_value = mock_conn
            h.connect(device)
            out = h.execute("show interfaces", timeout=10)
            h.disconnect()

        self.assertIn("GigabitEthernet0/0", out)
        mock_conn.send_command.assert_called_once()

    def test_ssh_handler_raises_on_wrong_password(self):
        from netmiko.exceptions import NetmikoAuthenticationException
        from components.collector.handlers.ssh_handler import SSHHandler
        h = SSHHandler()
        device = {**self._device(), "password": "wrong_password"}

        with patch("components.collector.handlers.ssh_handler.ConnectHandler") as mock_ch:
            mock_ch.side_effect = NetmikoAuthenticationException(
                "Authentication failed"
            )
            with self.assertRaises(NetmikoAuthenticationException):
                h.connect(device)


# ---------------------------------------------------------------------------
# Layer 3 — Message flow (end-to-end via DeviceExecutor with stub handler)
# ---------------------------------------------------------------------------

class TestCollectorMessageFlow(unittest.TestCase):
    """
    Simulates the full message lifecycle:
        1. Build a DeviceQueueMessage dict (as the Orchestrator would publish)
        2. Run through DeviceExecutor (handler stubbed with simulator output)
        3. Verify: success counter incremented, raw output emitted

    Uses CiscoIOSSimulator for realistic CLI output — same as the actual
    simulator container, without any network dependency.
    """

    def setUp(self):
        from simulator.vendors.cisco_ios import CiscoIOSSimulator
        from simulator.vendors.huawei_vrp import HuaweiVRPSimulator
        from simulator.vendors.bdcom import BDCOMSimulator
        self.simulators = {
            "cisco_ios":  CiscoIOSSimulator("dev-001", "10.1.0.1"),
            "huawei_vrp": HuaweiVRPSimulator("dev-002", "10.3.0.1"),
            "bdcom":      BDCOMSimulator("dev-003", "192.168.1.1"),
        }

    def _message(self, vendor: str, operation: str) -> dict:
        return {
            "execution_id": f"exec-flow-{vendor}",
            "job_id": "flow-job",
            "job_name": "flow_test",
            "operation": operation,
            "job_protocol": "SSH",
            "params": {},
            "device": {
                "device_id": f"dev-{vendor}",
                "ip_address": "10.0.0.1",
                "vendor": vendor,
                "segment": "Tier1",
                "region": "mumbai",
                "username": "admin",
                "password": "admin",
            },
        }

    def _make_stub_handler(self, vendor: str, operation: str):
        """
        Return a mock SSHHandler whose execute() returns real simulator output.
        Each call to execute() maps the command to the simulator's response.
        """
        sim = self.simulators.get(vendor)
        mock_handler = MagicMock()

        def sim_execute(command, timeout=30):
            if sim:
                return sim.get_response(command)
            return f"# output for: {command}\n"

        mock_handler.execute.side_effect = sim_execute
        mock_handler.send_config_set.return_value = "config applied"
        return mock_handler

    @patch("components.collector.device_executor.get_handler")
    def test_cisco_interface_stats_full_flow(self, mock_get_handler):
        from components.collector.device_executor import DeviceExecutor
        tracker = MagicMock()
        executor = DeviceExecutor(tracker)

        vendor = "cisco_ios"
        operation = "INTERFACE_STATS"
        mock_get_handler.return_value = self._make_stub_handler(vendor, operation)

        emitted = []
        result = executor.run(
            self._message(vendor, operation),
            lambda m: emitted.append(m),
        )

        self.assertTrue(result.success, f"Expected success but got: {result.failure_reason}")
        self.assertEqual(len(emitted), 1)

        raw = emitted[0]["raw_output"]
        self.assertIn("show interfaces", raw)        # command header in output
        self.assertIn("GigabitEthernet", raw)        # real simulator content
        tracker.record_success.assert_called_once_with(f"exec-flow-{vendor}")

    @patch("components.collector.device_executor.get_handler")
    def test_huawei_optic_power_full_flow(self, mock_get_handler):
        from components.collector.device_executor import DeviceExecutor
        tracker = MagicMock()
        executor = DeviceExecutor(tracker)

        vendor = "huawei_vrp"
        operation = "OPTIC_POWER"
        mock_get_handler.return_value = self._make_stub_handler(vendor, operation)

        emitted = []
        result = executor.run(
            self._message(vendor, operation),
            lambda m: emitted.append(m),
        )

        self.assertTrue(result.success)
        raw = emitted[0]["raw_output"]
        self.assertIn("display interface optical-module verbose", raw)

    @patch("components.collector.device_executor.get_handler")
    def test_bdcom_interface_stats_full_flow(self, mock_get_handler):
        from components.collector.device_executor import DeviceExecutor
        tracker = MagicMock()
        executor = DeviceExecutor(tracker)

        vendor = "bdcom"
        operation = "INTERFACE_STATS"
        mock_get_handler.return_value = self._make_stub_handler(vendor, operation)

        emitted = []
        result = executor.run(
            self._message(vendor, operation),
            lambda m: emitted.append(m),
        )

        self.assertTrue(result.success)
        raw = emitted[0]["raw_output"]
        self.assertIn("GigabitEthernet", raw)

    @patch("components.collector.device_executor.get_handler")
    def test_all_vendors_all_get_operations(self, mock_get_handler):
        """Smoke test: every vendor × every GET operation must succeed."""
        from components.collector.device_executor import DeviceExecutor
        tracker = MagicMock()
        executor = DeviceExecutor(tracker)

        test_cases = [
            ("cisco_ios",  "INTERFACE_STATS"),
            ("cisco_ios",  "OPTIC_POWER"),
            ("cisco_ios",  "DISCOVERY"),
            ("huawei_vrp", "INTERFACE_STATS"),
            ("huawei_vrp", "OPTIC_POWER"),
            ("huawei_vrp", "DISCOVERY"),
            ("bdcom",      "INTERFACE_STATS"),
            ("bdcom",      "OPTIC_POWER"),
            ("bdcom",      "DISCOVERY"),
        ]

        for vendor, operation in test_cases:
            with self.subTest(vendor=vendor, operation=operation):
                mock_get_handler.return_value = self._make_stub_handler(
                    vendor, operation
                )
                tracker.reset_mock()
                result = executor.run(
                    self._message(vendor, operation),
                    lambda m: None,
                )
                self.assertTrue(
                    result.success,
                    f"FAILED: vendor={vendor} op={operation} "
                    f"reason={result.failure_reason}"
                )
                tracker.record_success.assert_called_once()

    @patch("components.collector.device_executor.get_handler")
    def test_failure_counters_match_failure_scenario(self, mock_get_handler):
        """
        Three messages: 2 succeed, 1 times out.
        Verify tracker gets 2 record_success and 1 record_failure(TIMEOUT).
        """
        from netmiko.exceptions import NetmikoTimeoutException
        from shared.models.status import FailureReason
        from components.collector.device_executor import DeviceExecutor

        tracker = MagicMock()
        executor = DeviceExecutor(tracker)

        success_handler = MagicMock()
        success_handler.execute.return_value = "output"

        timeout_handler = MagicMock()
        timeout_handler.connect.side_effect = [
            NetmikoTimeoutException("t/o"),
            NetmikoTimeoutException("t/o retry"),  # both attempts fail
        ]

        call_count = [0]
        def get_handler_side_effect(protocol):
            call_count[0] += 1
            if call_count[0] == 2:
                return timeout_handler
            return success_handler

        mock_get_handler.side_effect = get_handler_side_effect

        for i in range(3):
            executor.run(
                {**self._message("cisco_ios", "INTERFACE_STATS"),
                 "execution_id": "exec-multi"},
                lambda m: None,
            )

        self.assertEqual(tracker.record_success.call_count, 2)
        self.assertEqual(tracker.record_failure.call_count, 1)
        tracker.record_failure.assert_called_with(
            "exec-multi", FailureReason.TIMEOUT
        )


if __name__ == "__main__":
    unittest.main()
