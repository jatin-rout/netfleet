import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch, call

from shared.models.job import JobStatus

# ------------------------------------------------------------------ #
# Fixtures                                                            #
# ------------------------------------------------------------------ #

_PERIODIC_JOB = {
    "name": "optic_power_collection",
    "job_type": "PERIODIC",
    "operation": "OPTIC_POWER",
    "segments": ["Tier1", "Tier2", "Tier3"],
    "cron": "0 2 * * *",
    "protocol": "SNMP",
    "timeout_minutes": 120,
    "error_threshold": 100,
}

_ADHOC_JOB = {
    "name": "config_push_tier1",
    "job_type": "ADHOC",
    "operation": "CONFIG_PUSH",
    "segments": ["Tier1"],
    "cron": None,
    "protocol": "SSH",
    "timeout_minutes": 90,
    "error_threshold": 50,
}

_STANDARD_JOB = {
    "name": "config_push_edge",
    "job_type": "ADHOC",
    "operation": "CONFIG_PUSH",
    "segments": ["Edge", "Field"],
    "cron": None,
    "protocol": "TELNET",
    "timeout_minutes": 120,
    "error_threshold": 100,
}

_SAMPLE_DEVICE = {
    "device_id": "dev-001",
    "ip_address": "192.168.1.1",
    "vendor": "cisco_ios",
    "segment": "Tier1",
    "region": "mumbai",
    "protocol": "SSH",
    "status": "ACTIVE",
    "priority": "HIGH",
}


def _make_scheduler(devices=None):
    """Return a JobScheduler with mocked infrastructure."""
    from components.scheduler.job_scheduler import JobScheduler

    s = JobScheduler.__new__(JobScheduler)
    s._mongo = MagicMock()
    s._redis = MagicMock()
    s._jobs = [_PERIODIC_JOB, _ADHOC_JOB]
    s._running = False
    s._router = MagicMock()
    s._tracker = MagicMock()

    # mock count_documents for device count
    s._mongo.count_documents.return_value = (
        len(devices) if devices is not None else 3
    )
    # mock get_collection().find() cursor
    mock_collection = MagicMock()
    mock_collection.find.return_value = iter(
        devices if devices is not None else [_SAMPLE_DEVICE]
    )
    s._mongo.get_collection.return_value = mock_collection
    return s


def _make_tracker():
    """Return a CompletionTracker with mocked infrastructure."""
    from components.scheduler.completion_tracker import CompletionTracker

    t = CompletionTracker.__new__(CompletionTracker)
    t._mongo = MagicMock()
    t._redis = MagicMock()
    t._running = False
    return t


# ------------------------------------------------------------------ #
# Cron matching                                                        #
# ------------------------------------------------------------------ #

class TestCronFired(unittest.TestCase):
    def setUp(self):
        self.s = _make_scheduler()

    def test_fires_within_check_interval(self):
        # cron is 02:00 daily; now is 02:00:15 — within 30s window
        now = datetime(2026, 5, 7, 2, 0, 15)
        self.assertTrue(self.s._cron_fired("0 2 * * *", now))

    def test_does_not_fire_outside_window(self):
        now = datetime(2026, 5, 7, 2, 1, 2)
        self.assertFalse(self.s._cron_fired("0 2 * * *", now))

    def test_fires_at_exact_second(self):
        now = datetime(2026, 5, 7, 2, 0, 0)
        self.assertTrue(self.s._cron_fired("0 2 * * *", now))

    def test_invalid_expression_returns_false(self):
        self.assertFalse(
            self.s._cron_fired("not-a-valid-cron", datetime.utcnow())
        )


# ------------------------------------------------------------------ #
# Periodic job firing                                                  #
# ------------------------------------------------------------------ #

class TestFirePeriodic(unittest.TestCase):
    def setUp(self):
        self.devices = [
            {**_SAMPLE_DEVICE, "device_id": f"dev-{i:03}"}
            for i in range(3)
        ]
        self.s = _make_scheduler(devices=self.devices)
        self.s._mongo.insert_many.return_value = 1

    def test_inserts_execution_as_running(self):
        self.s._fire_periodic(_PERIODIC_JOB)

        col, docs = self.s._mongo.insert_many.call_args[0]
        self.assertEqual(col, "job_executions")
        self.assertEqual(docs[0]["status"], JobStatus.RUNNING)
        self.assertEqual(docs[0]["triggered_by"], "scheduler")

    def test_stores_correct_total_records(self):
        self.s._fire_periodic(_PERIODIC_JOB)

        _, docs = self.s._mongo.insert_many.call_args[0]
        self.assertEqual(docs[0]["total_records"], 3)

    def test_queries_discovery_db_with_correct_filter(self):
        self.s._fire_periodic(_PERIODIC_JOB)

        collection = self.s._mongo.get_collection.return_value
        query = collection.find.call_args[0][0]
        self.assertEqual(query["status"], "ACTIVE")
        self.assertEqual(
            query["segment"]["$in"],
            ["Tier1", "Tier2", "Tier3"],
        )

    def test_publishes_one_message_per_device(self):
        self.s._fire_periodic(_PERIODIC_JOB)
        self.assertEqual(self.s._router.route.call_count, 3)

    def test_published_message_contains_required_fields(self):
        self.s._fire_periodic(_PERIODIC_JOB)

        msg = self.s._router.route.call_args_list[0][0][0]
        for field in ("execution_id", "job_id", "job_name", "operation", "device", "priority"):
            self.assertIn(field, msg)
        self.assertEqual(msg["operation"], "OPTIC_POWER")

    def test_seeds_redis_progress_cache(self):
        self.s._fire_periodic(_PERIODIC_JOB)
        self.s._redis.set_cache.assert_called_once()
        key = self.s._redis.set_cache.call_args[0][0]
        self.assertIn("job_progress:", key)
        payload = self.s._redis.set_cache.call_args[0][1]
        self.assertEqual(payload["total_records"], 3)
        self.assertEqual(payload["inserted_records"], 0)

    def test_completes_immediately_when_no_devices(self):
        self.s._mongo.count_documents.return_value = 0
        self.s._mongo.get_collection.return_value.find.return_value = iter([])
        self.s._fire_periodic(_PERIODIC_JOB)

        # Nothing routed to queues
        self.s._router.route.assert_not_called()
        # No Redis seed
        self.s._redis.set_cache.assert_not_called()
        # Marked COMPLETE immediately
        update = self.s._mongo.update_one.call_args[0][2]
        self.assertEqual(update["$set"]["status"], JobStatus.COMPLETE)

    def test_does_not_fire_when_already_running(self):
        self.s._mongo.count_documents.side_effect = [1, 3]
        self.assertTrue(self.s._has_running(_PERIODIC_JOB["name"]))


# ------------------------------------------------------------------ #
# Adhoc job firing                                                     #
# ------------------------------------------------------------------ #

class TestFireAdhoc(unittest.TestCase):
    def setUp(self):
        self.devices = [_SAMPLE_DEVICE]
        self.s = _make_scheduler(devices=self.devices)
        self.s._mongo.update_one.return_value = 1

    def test_transitions_pending_to_running(self):
        msg = {
            "execution_id": "exec-001",
            "job_config": _ADHOC_JOB,
            "triggered_by": "api",
        }
        self.s._fire_adhoc(msg)

        query = self.s._mongo.update_one.call_args[0][1]
        update = self.s._mongo.update_one.call_args[0][2]
        self.assertEqual(query["status"], JobStatus.PENDING)
        self.assertEqual(update["$set"]["status"], JobStatus.RUNNING)
        self.assertEqual(update["$set"]["total_records"], 1)

    def test_publishes_device_messages_to_queues(self):
        msg = {
            "execution_id": "exec-002",
            "job_config": _ADHOC_JOB,
            "triggered_by": "api",
        }
        self.s._fire_adhoc(msg)
        self.s._router.route.assert_called_once()
        published = self.s._router.route.call_args[0][0]
        self.assertEqual(published["execution_id"], "exec-002")
        self.assertEqual(published["operation"], "CONFIG_PUSH")

    def test_skips_when_execution_not_pending(self):
        self.s._mongo.update_one.return_value = 0
        msg = {
            "execution_id": "exec-003",
            "job_config": _ADHOC_JOB,
            "triggered_by": "api",
        }
        self.s._fire_adhoc(msg)
        self.s._router.route.assert_not_called()

    def test_skips_malformed_message_missing_job_config(self):
        self.s._fire_adhoc({"execution_id": "exec-x"})
        self.s._router.route.assert_not_called()
        self.s._mongo.update_one.assert_not_called()

    def test_completes_immediately_when_no_devices(self):
        self.s._mongo.count_documents.return_value = 0
        self.s._mongo.get_collection.return_value.find.return_value = iter([])
        msg = {
            "execution_id": "exec-004",
            "job_config": _ADHOC_JOB,
            "triggered_by": "api",
        }
        self.s._fire_adhoc(msg)

        self.s._router.route.assert_not_called()
        # last update_one call marks COMPLETE
        last_update = self.s._mongo.update_one.call_args_list[-1][0][2]
        self.assertEqual(last_update["$set"]["status"], JobStatus.COMPLETE)


# ------------------------------------------------------------------ #
# Device routing                                                       #
# ------------------------------------------------------------------ #

class TestDeviceRouting(unittest.TestCase):
    """Verify Scheduler puts HIGH and STANDARD devices into correct queues."""

    def setUp(self):
        high_device = {**_SAMPLE_DEVICE, "segment": "Tier1"}
        low_device = {**_SAMPLE_DEVICE, "segment": "Edge", "priority": "STANDARD"}
        self.s = _make_scheduler(devices=[high_device, low_device])
        self.s._mongo.count_documents.return_value = 2
        self.s._mongo.insert_many.return_value = 1

    def test_high_segment_devices_get_high_priority(self):
        from components.scheduler.priority_queue_manager import PriorityQueueManager

        self.assertEqual(
            PriorityQueueManager.segment_priority("Tier1"), "HIGH"
        )
        self.assertEqual(
            PriorityQueueManager.segment_priority("Tier2"), "HIGH"
        )
        self.assertEqual(
            PriorityQueueManager.segment_priority("Tier3"), "HIGH"
        )

    def test_lower_segment_devices_get_standard_priority(self):
        from components.scheduler.priority_queue_manager import PriorityQueueManager

        self.assertEqual(
            PriorityQueueManager.segment_priority("Edge"), "STANDARD"
        )
        self.assertEqual(
            PriorityQueueManager.segment_priority("Field"), "STANDARD"
        )

    def test_fire_periodic_routes_all_devices(self):
        self.s._fire_periodic(_PERIODIC_JOB)
        self.assertEqual(self.s._router.route.call_count, 2)


# ------------------------------------------------------------------ #
# Completion tracker                                                   #
# ------------------------------------------------------------------ #

class TestCompletionTracker(unittest.TestCase):
    def setUp(self):
        self.t = _make_tracker()

    def _exe(self, extra=None):
        base = {
            "execution_id": "exec-100",
            "triggered_at": datetime.utcnow().isoformat(),
            "timeout_minutes": 120,
        }
        if extra:
            base.update(extra)
        return base

    def test_marks_complete_when_inserted_equals_total(self):
        self.t._redis.get_cache.return_value = {
            "total_records": 100,
            "inserted_records": 100,
            "failed_records": 3,
        }
        self.t._evaluate(self._exe())

        status = self.t._mongo.update_one.call_args[0][2]["$set"]["status"]
        self.assertEqual(status, JobStatus.COMPLETE)

    def test_marks_complete_when_inserted_exceeds_total(self):
        # Batch writes can slightly overshoot the total
        self.t._redis.get_cache.return_value = {
            "total_records": 98,
            "inserted_records": 100,
            "failed_records": 0,
        }
        self.t._evaluate(self._exe())
        status = self.t._mongo.update_one.call_args[0][2]["$set"]["status"]
        self.assertEqual(status, JobStatus.COMPLETE)

    def test_no_action_when_in_progress_within_timeout(self):
        self.t._redis.get_cache.return_value = {
            "total_records": 100,
            "inserted_records": 50,
            "failed_records": 0,
        }
        self.t._evaluate(self._exe())
        self.t._mongo.update_one.assert_not_called()

    def test_marks_timeout_when_window_exceeded(self):
        past = (datetime.utcnow() - timedelta(minutes=130)).isoformat()
        self.t._redis.get_cache.return_value = {
            "total_records": 100,
            "inserted_records": 30,
            "failed_records": 2,
        }
        self.t._evaluate(self._exe({"triggered_at": past}))

        status = self.t._mongo.update_one.call_args[0][2]["$set"]["status"]
        self.assertEqual(status, JobStatus.TIMEOUT)

    def test_marks_timeout_when_no_redis_progress(self):
        past = (datetime.utcnow() - timedelta(minutes=200)).isoformat()
        self.t._redis.get_cache.return_value = None
        self.t._evaluate(self._exe({"triggered_at": past}))

        status = self.t._mongo.update_one.call_args[0][2]["$set"]["status"]
        self.assertEqual(status, JobStatus.TIMEOUT)

    def test_cleans_redis_on_complete(self):
        self.t._redis.get_cache.return_value = {
            "total_records": 50,
            "inserted_records": 50,
            "failed_records": 0,
        }
        self.t._evaluate(self._exe())
        self.t._redis.delete_cache.assert_called_once_with(
            "job_progress:exec-100"
        )

    def test_cleans_redis_on_timeout(self):
        past = (datetime.utcnow() - timedelta(minutes=130)).isoformat()
        self.t._redis.get_cache.return_value = {
            "total_records": 100,
            "inserted_records": 10,
            "failed_records": 0,
        }
        self.t._evaluate(self._exe({"triggered_at": past}))
        self.t._redis.delete_cache.assert_called_once_with(
            "job_progress:exec-100"
        )

    def test_does_not_complete_when_total_is_zero(self):
        # Zero total_records: job is already COMPLETE via _complete_immediately
        # CompletionTracker should not re-mark it
        self.t._redis.get_cache.return_value = {
            "total_records": 0,
            "inserted_records": 0,
            "failed_records": 0,
        }
        self.t._evaluate(self._exe())
        self.t._mongo.update_one.assert_not_called()


if __name__ == "__main__":
    unittest.main()
