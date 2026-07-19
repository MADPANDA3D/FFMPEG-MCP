import fnmatch
import threading
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import redis

from madpanda_ffmpeg_mcp import brand_kits, jobs, metrics, redis_store, server, tenant

OWNER_A = "a" * 64
OWNER_B = "b" * 64
REPO_ROOT = Path(__file__).resolve().parents[1]


class AtomicFakeRedis:
    """Small atomic model for the Lua contracts used by this security slice."""

    def __init__(self):
        self.values: dict[str, str] = {}
        self.sets: dict[str, set[str]] = {}
        self.sorted_sets: dict[str, dict[str, int]] = {}
        self.ttls: dict[str, int] = {}
        self.lock = threading.RLock()
        self.fail_eval = False

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ex=None):
        self.values[key] = value
        if ex is not None:
            self.ttls[key] = int(ex)

    def delete(self, key):
        self.values.pop(key, None)
        self.sets.pop(key, None)
        self.sorted_sets.pop(key, None)

    def zadd(self, key, mapping):
        self.sorted_sets.setdefault(key, {}).update(mapping)

    def zrem(self, key, member):
        self.sorted_sets.setdefault(key, {}).pop(member, None)

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def scan_iter(self, match):
        return iter(key for key in self.values if fnmatch.fnmatch(key, match))

    def pipeline(self, transaction=True):
        if not transaction:
            raise AssertionError("metrics updates must be transactional")
        return _MetricsPipeline(self)

    def eval(self, script, numkeys, *values):
        del numkeys
        if self.fail_eval:
            raise RuntimeError("simulated Redis outage with internal details")
        with self.lock:
            if script == redis_store._JOB_ADMISSION_SCRIPT:
                return self._admit(values)
            if script == redis_store._JOB_ADMISSION_RELEASE_SCRIPT:
                return self._release(values, rollback_rate=False)
            if script == redis_store._JOB_ADMISSION_REFRESH_SCRIPT:
                return self._refresh(values)
            if script == redis_store._JOB_ADMISSION_ROLLBACK_SCRIPT:
                return self._release(values, rollback_rate=True)
            if script == redis_store._BRAND_KIT_SAVE_SCRIPT:
                return self._save_brand_kit(values)
            if script == redis_store._BRAND_KIT_DELETE_SCRIPT:
                return self._delete_brand_kit(values)
        raise AssertionError("unexpected Lua script")

    def _admit(self, values):
        (
            global_key,
            owner_key,
            rate_key,
            job_id,
            now_ms,
            expires_ms,
            owner_max,
            global_max,
            owner_rpm,
            reservation_ttl,
        ) = values
        now_ms = int(now_ms)
        for key, cutoff in (
            (global_key, now_ms),
            (owner_key, now_ms),
            (rate_key, now_ms - 60_000),
        ):
            retained = {
                member: score
                for member, score in self.sorted_sets.get(key, {}).items()
                if int(score) > cutoff
            }
            self.sorted_sets[key] = retained
        if len(self.sorted_sets[owner_key]) >= int(owner_max):
            return [0, 1]
        if len(self.sorted_sets[global_key]) >= int(global_max):
            return [0, 2]
        if len(self.sorted_sets[rate_key]) >= int(owner_rpm):
            return [0, 3]
        self.sorted_sets[global_key][job_id] = int(expires_ms)
        self.sorted_sets[owner_key][job_id] = int(expires_ms)
        self.sorted_sets[rate_key][job_id] = now_ms
        self.ttls[global_key] = int(reservation_ttl)
        self.ttls[owner_key] = int(reservation_ttl)
        self.ttls[rate_key] = 61
        return [1, 0]

    def _release(self, values, *, rollback_rate):
        key_count = 3 if rollback_rate else 2
        keys = values[:key_count]
        job_id = values[key_count]
        removed = 0
        for key in keys:
            removed += int(job_id in self.sorted_sets.get(key, {}))
            self.sorted_sets.setdefault(key, {}).pop(job_id, None)
        return removed

    def _refresh(self, values):
        global_key, owner_key, job_id, expires_ms, execution_ttl = values
        if job_id not in self.sorted_sets.get(global_key, {}) or job_id not in self.sorted_sets.get(
            owner_key, {}
        ):
            return 0
        self.sorted_sets[global_key][job_id] = int(expires_ms)
        self.sorted_sets[owner_key][job_id] = int(expires_ms)
        self.ttls[global_key] = max(self.ttls.get(global_key, 0), int(execution_ttl))
        self.ttls[owner_key] = max(self.ttls.get(owner_key, 0), int(execution_ttl))
        return 1

    def _save_brand_kit(self, values):
        key, index_key, brand_kit_id, payload, maximum = values
        if brand_kit_id not in self.sets.get(index_key, set()) and len(
            self.sets.get(index_key, set())
        ) >= int(maximum):
            return 0
        self.values[key] = payload
        self.sets.setdefault(index_key, set()).add(brand_kit_id)
        return 1

    def _delete_brand_kit(self, values):
        key, index_key, brand_kit_id = values
        deleted = int(key in self.values)
        self.values.pop(key, None)
        self.sets.setdefault(index_key, set()).discard(brand_kit_id)
        return deleted


class _MetricsPipeline:
    def __init__(self, client):
        self.client = client
        self.actions: list[tuple] = []

    def incrby(self, key, amount):
        self.actions.append(("increment", key, int(amount)))
        return self

    def incrbyfloat(self, key, amount):
        self.actions.append(("increment", key, float(amount)))
        return self

    def expire(self, key, ttl):
        self.actions.append(("expire", key, int(ttl)))
        return self

    def execute(self):
        with self.client.lock:
            for action, key, value in self.actions:
                if action == "increment":
                    current = float(self.client.values.get(key, "0"))
                    result = current + value
                    self.client.values[key] = (
                        str(int(result)) if result.is_integer() else str(result)
                    )
                else:
                    self.client.ttls[key] = value
        return []


def _configured(**overrides):
    values = {
        "mcp_mode": "portal",
        "portal_grant_token": "p" * 40,
        "principal_hash_secret": "h" * 40,
        "allowed_hosts": ["localhost"],
        "download_signing_secret": "d" * 40,
        "ingest_allow_http": False,
        "ingest_allow_any_public_domain": False,
    }
    values.update(overrides)
    return replace(server.settings, **values)


class JobAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.redis = AtomicFakeRedis()

    def _patch_settings(self, **overrides):
        configured = _configured(**overrides)
        return patch.multiple(redis_store, settings=configured, _redis_client=self.redis)

    def test_nth_owner_rejection_leaves_other_tenant_capacity(self):
        with self._patch_settings(
            job_admission_owner_max_active=1,
            job_admission_global_max_active=3,
            job_admission_owner_rpm=10,
        ):
            redis_store.reserve_job_admission("a1", owner_hash=OWNER_A)
            with self.assertRaisesRegex(
                redis_store.JobAdmissionError, "tenant active job limit reached"
            ):
                redis_store.reserve_job_admission("a2", owner_hash=OWNER_A)
            redis_store.reserve_job_admission("b1", owner_hash=OWNER_B)

    def test_nth_global_and_rate_rejections_are_distinct(self):
        with self._patch_settings(
            job_admission_owner_max_active=4,
            job_admission_global_max_active=2,
            job_admission_owner_rpm=10,
        ):
            redis_store.reserve_job_admission("a1", owner_hash=OWNER_A)
            redis_store.reserve_job_admission("b1", owner_hash=OWNER_B)
            with self.assertRaisesRegex(
                redis_store.JobAdmissionError, "service active job limit reached"
            ):
                redis_store.reserve_job_admission("a2", owner_hash=OWNER_A)

        self.redis = AtomicFakeRedis()
        with (
            self._patch_settings(
                job_admission_owner_max_active=4,
                job_admission_global_max_active=8,
                job_admission_owner_rpm=2,
            ),
            patch.object(redis_store.time, "time", return_value=1000.0),
        ):
            for job_id in ("one", "two"):
                redis_store.reserve_job_admission(job_id, owner_hash=OWNER_A)
                redis_store.release_job_admission(job_id, owner_hash=OWNER_A)
            with self.assertRaisesRegex(
                redis_store.JobAdmissionError, "tenant enqueue rate limit reached"
            ):
                redis_store.reserve_job_admission("three", owner_hash=OWNER_A)

    def test_expired_crash_reservation_is_purged_atomically(self):
        configured = _configured(
            job_ttl_hours=1,
            job_admission_execution_buffer_seconds=3_720,
            job_admission_owner_max_active=1,
            job_admission_global_max_active=1,
            job_admission_owner_rpm=10,
        )
        with patch.multiple(redis_store, settings=configured, _redis_client=self.redis):
            with patch.object(redis_store.time, "time", return_value=1000.0):
                redis_store.reserve_job_admission("crashed", owner_hash=OWNER_A)
            after_expiry = 1000.0 + configured.job_admission_reservation_ttl_seconds() + 1
            with patch.object(redis_store.time, "time", return_value=after_expiry):
                redis_store.reserve_job_admission("replacement", owner_hash=OWNER_A)

    def test_worker_start_refresh_shortens_crash_lease_to_execution_window(self):
        configured = _configured(
            job_ttl_hours=24,
            job_admission_execution_buffer_seconds=3_720,
            job_admission_owner_max_active=1,
            job_admission_global_max_active=1,
            job_admission_owner_rpm=10,
        )
        with patch.multiple(redis_store, settings=configured, _redis_client=self.redis):
            with patch.object(redis_store.time, "time", return_value=1000.0):
                redis_store.reserve_job_admission("running", owner_hash=OWNER_A)
            with patch.object(redis_store.time, "time", return_value=1001.0):
                redis_store.refresh_job_admission("running", 1020, owner_hash=OWNER_A)
        expected_expiry = int((1001.0 + 1020) * 1000)
        self.assertEqual(
            self.redis.sorted_sets[redis_store.JOB_ADMISSION_GLOBAL_KEY]["running"],
            expected_expiry,
        )
        self.assertEqual(
            self.redis.sorted_sets[redis_store._job_admission_owner_key(OWNER_A)]["running"],
            expected_expiry,
        )
        self.assertIn(
            "running", self.redis.sorted_sets[redis_store._job_admission_rate_key(OWNER_A)]
        )

    def test_enqueue_failure_removes_job_and_rolls_back_active_and_rate_slots(self):
        configured = _configured(
            job_admission_owner_max_active=1,
            job_admission_global_max_active=1,
            job_admission_owner_rpm=1,
        )
        queue = SimpleNamespace(
            connection=self.redis, enqueue=Mock(side_effect=RuntimeError("boom"))
        )
        with (
            tenant.tenant_context(OWNER_A),
            patch.multiple(redis_store, settings=configured, _redis_client=self.redis),
            patch.object(server, "settings", configured),
            patch.object(server, "get_queue", return_value=queue),
            patch.object(server, "_rq_enqueue_commit_state", return_value="absent"),
            patch.object(server, "record_cache_miss"),
            patch.object(server, "log_event"),
            self.assertRaisesRegex(
                redis_store.JobAdmissionError, "job queue temporarily unavailable"
            ),
        ):
            server._enqueue_job("test", lambda: None, ())
        self.assertFalse(any(key.startswith("job:") for key in self.redis.values))
        self.assertFalse(any(self.redis.sorted_sets.values()))

    def test_lost_enqueue_ack_with_committed_job_returns_tracking_id(self):
        configured = _configured(
            job_admission_owner_max_active=1,
            job_admission_global_max_active=1,
            job_admission_owner_rpm=1,
        )
        queue = SimpleNamespace(
            connection=self.redis,
            enqueue=Mock(side_effect=RuntimeError("lost acknowledgement")),
        )
        with (
            tenant.tenant_context(OWNER_A),
            patch.multiple(redis_store, settings=configured, _redis_client=self.redis),
            patch.object(server, "settings", configured),
            patch.object(server, "get_queue", return_value=queue),
            patch.object(server, "_rq_enqueue_commit_state", return_value="committed"),
            patch.object(server, "record_cache_miss"),
            patch.object(server, "log_event"),
        ):
            job_id = server._enqueue_job("test", lambda: None, ())
        self.assertIn(f"job:{job_id}", self.redis.values)
        self.assertIn(job_id, self.redis.sorted_sets[redis_store.JOB_ADMISSION_GLOBAL_KEY])

    def test_ambiguous_enqueue_retains_job_and_lease_for_safe_reconciliation(self):
        configured = _configured(
            job_admission_owner_max_active=1,
            job_admission_global_max_active=1,
            job_admission_owner_rpm=1,
        )
        queue = SimpleNamespace(
            connection=self.redis,
            enqueue=Mock(side_effect=RuntimeError("connection lost")),
        )
        fixed_job_id = "1" * 32
        with (
            tenant.tenant_context(OWNER_A),
            patch.multiple(redis_store, settings=configured, _redis_client=self.redis),
            patch.object(server, "settings", configured),
            patch.object(server, "get_queue", return_value=queue),
            patch.object(server, "_rq_enqueue_commit_state", return_value="unknown"),
            patch.object(server.uuid, "uuid4", return_value=SimpleNamespace(hex=fixed_job_id)),
            patch.object(server, "log_event"),
            self.assertRaisesRegex(
                redis_store.JobAdmissionError, "job queue temporarily unavailable"
            ),
        ):
            server._enqueue_job("test", lambda: None, ())
        self.assertIn(f"job:{fixed_job_id}", self.redis.values)
        self.assertIn(fixed_job_id, self.redis.sorted_sets[redis_store.JOB_ADMISSION_GLOBAL_KEY])

    def test_commit_reconciliation_requires_queued_or_intermediate_membership(self):
        pipeline = Mock()
        pipeline.lpos.return_value = pipeline
        connection = Mock()
        connection.pipeline.return_value = pipeline
        queue = SimpleNamespace(
            connection=connection,
            name="av-jobs",
            key="rq:queue:av-jobs",
            intermediate_queue_key="rq:queue:av-jobs:intermediate",
        )
        rq_job = SimpleNamespace(
            meta={"owner_hash": OWNER_A},
            origin="av-jobs",
            get_status=Mock(return_value=server.JobStatus.QUEUED),
            refresh=Mock(),
        )
        with patch.object(server.Job, "fetch", return_value=rq_job):
            pipeline.execute.return_value = [0, None]
            self.assertEqual(server._rq_enqueue_commit_state(queue, "job-id", OWNER_A), "committed")
            pipeline.execute.return_value = [None, 0]
            self.assertEqual(server._rq_enqueue_commit_state(queue, "job-id", OWNER_A), "committed")
            pipeline.execute.return_value = [None, None]
            self.assertEqual(server._rq_enqueue_commit_state(queue, "job-id", OWNER_A), "unknown")
            rq_job.get_status.return_value = server.JobStatus.STARTED
            self.assertEqual(server._rq_enqueue_commit_state(queue, "job-id", OWNER_A), "committed")

    def test_save_failure_rolls_back_reservation_before_enqueue(self):
        configured = _configured(
            job_admission_owner_max_active=1,
            job_admission_global_max_active=1,
            job_admission_owner_rpm=1,
        )
        queue = SimpleNamespace(connection=self.redis, enqueue=Mock())
        with (
            tenant.tenant_context(OWNER_A),
            patch.multiple(redis_store, settings=configured, _redis_client=self.redis),
            patch.object(server, "settings", configured),
            patch.object(server, "get_queue", return_value=queue),
            patch.object(server, "save_job", side_effect=RuntimeError("write failed")),
            self.assertRaisesRegex(
                redis_store.JobAdmissionError, "job queue temporarily unavailable"
            ),
        ):
            server._enqueue_job("test", lambda: None, ())
        queue.enqueue.assert_not_called()
        self.assertFalse(any(self.redis.sorted_sets.values()))

    def test_metric_failure_does_not_roll_back_an_enqueued_job(self):
        configured = _configured(
            job_admission_owner_max_active=1,
            job_admission_global_max_active=1,
            job_admission_owner_rpm=1,
        )
        queue = SimpleNamespace(connection=self.redis, enqueue=Mock())
        with (
            tenant.tenant_context(OWNER_A),
            patch.multiple(redis_store, settings=configured, _redis_client=self.redis),
            patch.object(server, "settings", configured),
            patch.object(server, "get_queue", return_value=queue),
            patch.object(
                server,
                "record_cache_miss",
                side_effect=RuntimeError("metrics temporarily unavailable"),
            ),
            patch.object(server, "log_event"),
        ):
            job_id = server._enqueue_job("test", lambda: None, ())
        queue.enqueue.assert_called_once()
        enqueue_kwargs = queue.enqueue.call_args.kwargs
        self.assertIs(enqueue_kwargs["on_failure"], jobs.release_tenant_job_admission_callback)
        self.assertIs(enqueue_kwargs["on_stopped"], jobs.release_tenant_job_admission_callback)
        self.assertTrue(job_id)
        self.assertTrue(any(key.startswith("job:") for key in self.redis.values))
        self.assertTrue(any(self.redis.sorted_sets.values()))

    def test_release_restores_active_capacity_but_preserves_rate_history(self):
        with self._patch_settings(
            job_admission_owner_max_active=1,
            job_admission_global_max_active=1,
            job_admission_owner_rpm=5,
        ):
            redis_store.reserve_job_admission("one", owner_hash=OWNER_A)
            self.assertTrue(redis_store.release_job_admission("one", owner_hash=OWNER_A))
            redis_store.reserve_job_admission("two", owner_hash=OWNER_A)
        rate_key = redis_store._job_admission_rate_key(OWNER_A)
        self.assertEqual(set(self.redis.sorted_sets[rate_key]), {"one", "two"})

    def test_cached_job_records_use_rate_admission_without_holding_active_capacity(self):
        configured = _configured(
            job_admission_owner_max_active=1,
            job_admission_global_max_active=1,
            job_admission_owner_rpm=1,
        )
        with (
            tenant.tenant_context(OWNER_A),
            patch.multiple(redis_store, settings=configured, _redis_client=self.redis),
            patch.object(server, "settings", configured),
            patch.object(server, "record_cache_hit"),
            patch.object(server, "log_event"),
        ):
            job_id = server._record_cached_job("transcode", "asset", ["output"], "cache-key")
            with self.assertRaisesRegex(
                redis_store.JobAdmissionError, "tenant enqueue rate limit reached"
            ):
                server._record_cached_job("transcode", "asset", ["output"], "cache-key")
        self.assertIn(f"job:{job_id}", self.redis.values)
        self.assertNotIn(job_id, self.redis.sorted_sets[redis_store.JOB_ADMISSION_GLOBAL_KEY])
        self.assertIn(job_id, self.redis.sorted_sets[redis_store._job_admission_rate_key(OWNER_A)])

    def test_worker_releases_reservation_in_finally(self):
        rq_job = SimpleNamespace(id="job-id", meta={"owner_hash": OWNER_A}, timeout=960)
        release = Mock()
        refresh = Mock()
        with (
            tenant.tenant_context(OWNER_A),
            patch.object(jobs, "get_current_job", return_value=rq_job),
            patch.object(jobs, "_finish_job"),
            patch.object(jobs, "refresh_job_admission", refresh),
            patch.object(jobs, "release_job_admission", release),
        ):
            result = jobs.execute_tenant_job(lambda: (_ for _ in ()).throw(ValueError("bad")), ())
        self.assertEqual(result["ok"], False)
        refresh.assert_called_once_with("job-id", 1020, owner_hash=OWNER_A)
        release.assert_called_once_with("job-id", owner_hash=OWNER_A)

    def test_terminal_rq_callback_releases_admission_idempotently(self):
        rq_job = SimpleNamespace(id="job-id", meta={"owner_hash": OWNER_A})
        release = Mock()
        with patch.object(jobs, "release_job_admission", release):
            jobs.release_tenant_job_admission_callback(
                rq_job, self.redis, RuntimeError, RuntimeError("failed"), None
            )
        release.assert_called_once_with("job-id", owner_hash=OWNER_A)

    def test_worker_does_not_execute_when_lease_refresh_is_unavailable(self):
        rq_job = SimpleNamespace(id="job-id", meta={"owner_hash": OWNER_A}, timeout=960)
        function = Mock()
        release = Mock()
        with (
            tenant.tenant_context(OWNER_A),
            patch.object(jobs, "get_current_job", return_value=rq_job),
            patch.object(jobs, "_finish_job"),
            patch.object(
                jobs,
                "refresh_job_admission",
                side_effect=redis_store.JobAdmissionError("queue unavailable"),
            ),
            patch.object(jobs, "release_job_admission", release),
        ):
            result = jobs.execute_tenant_job(function, ())
        self.assertEqual(result, {"ok": False, "error_code": "queue_unavailable"})
        function.assert_not_called()
        release.assert_not_called()

    def test_worker_metric_failure_cannot_change_job_outcome(self):
        with (
            patch.object(jobs, "job_timer", return_value=12.5),
            patch.object(jobs, "record_job_duration", side_effect=RuntimeError("metrics down")),
            patch.object(jobs, "log_event") as log_event,
        ):
            jobs._record_job_metrics("transcode", 10.0, "success", "job-id")
        log_event.assert_called_once()

    def test_redis_failure_is_fail_closed_and_non_secret(self):
        self.redis.fail_eval = True
        with self._patch_settings(), self.assertRaises(redis_store.JobAdmissionError) as raised:
            redis_store.reserve_job_admission("job", owner_hash=OWNER_A)
        self.assertEqual(str(raised.exception), "job queue temporarily unavailable")
        self.assertNotIn("internal details", str(raised.exception))


class BrandKitBoundTests(unittest.TestCase):
    def setUp(self):
        self.redis = AtomicFakeRedis()

    def test_identifier_reserved_and_string_bounds(self):
        for invalid in ("all", "ALL", "../escape", "x" * 65, "has space"):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "brand_kit_id"):
                brand_kits.sanitize_brand_kit({"brand_kit_id": invalid})
        configured = _configured(brand_kit_max_string_chars=16)
        with (
            patch("madpanda_ffmpeg_mcp.brand_kit_policy.settings", configured),
            self.assertRaisesRegex(ValueError, "string exceeds configured limit"),
        ):
            brand_kits.sanitize_brand_kit({"brand_kit_id": "valid", "name": "n" * 17})

    def test_atomic_count_allows_update_at_limit_and_rejects_concurrent_create(self):
        configured = _configured(brand_kit_max_count=1)
        with (
            patch.multiple(redis_store, settings=configured, _redis_client=self.redis),
            patch("madpanda_ffmpeg_mcp.brand_kit_policy.settings", configured),
            tenant.tenant_context(OWNER_A),
        ):
            redis_store.save_brand_kit({"brand_kit_id": "existing", "name": "one"})
            redis_store.save_brand_kit({"brand_kit_id": "existing", "name": "updated"})
            self.assertEqual(redis_store.get_brand_kit("existing")["name"], "updated")

        self.redis = AtomicFakeRedis()
        barrier = threading.Barrier(2)
        outcomes: list[str] = []

        def create(brand_kit_id):
            with tenant.tenant_context(OWNER_A):
                barrier.wait()
                try:
                    redis_store.save_brand_kit({"brand_kit_id": brand_kit_id})
                    outcomes.append("saved")
                except redis_store.BrandKitLimitError:
                    outcomes.append("limited")

        with (
            patch.multiple(redis_store, settings=configured, _redis_client=self.redis),
            patch("madpanda_ffmpeg_mcp.brand_kit_policy.settings", configured),
        ):
            threads = [threading.Thread(target=create, args=(value,)) for value in ("one", "two")]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=2)
        self.assertEqual(sorted(outcomes), ["limited", "saved"])

    def test_serialized_payload_bound_is_enforced_before_redis_write(self):
        configured = _configured(brand_kit_max_serialized_bytes=80)
        with (
            patch.multiple(redis_store, settings=configured, _redis_client=self.redis),
            patch("madpanda_ffmpeg_mcp.brand_kit_policy.settings", configured),
            tenant.tenant_context(OWNER_A),
            self.assertRaisesRegex(ValueError, "payload exceeds configured limit"),
        ):
            redis_store.save_brand_kit({"brand_kit_id": "valid", "name": "bounded"})
        self.assertEqual(self.redis.values, {})

    def test_orphan_record_cannot_bypass_logical_index_quota(self):
        configured = _configured(brand_kit_max_count=1)
        index_key = redis_store._tenant_brand_kit_set(OWNER_A)
        orphan_key = redis_store._tenant_brand_kit_key(OWNER_A, "orphan")
        self.redis.sets[index_key] = {"indexed"}
        self.redis.values[orphan_key] = "{}"
        with (
            patch.multiple(redis_store, settings=configured, _redis_client=self.redis),
            patch("madpanda_ffmpeg_mcp.brand_kit_policy.settings", configured),
            tenant.tenant_context(OWNER_A),
            self.assertRaisesRegex(redis_store.BrandKitLimitError, "brand kit limit reached"),
        ):
            redis_store.save_brand_kit({"brand_kit_id": "orphan"})
        self.assertEqual(self.redis.sets[index_key], {"indexed"})


class TenantMetricTests(unittest.IsolatedAsyncioTestCase):
    async def test_metric_oom_is_best_effort_and_warning_is_rate_limited(self):
        pipeline = Mock()
        pipeline.incrby.return_value = pipeline
        pipeline.expire.return_value = pipeline
        pipeline.execute.side_effect = redis.exceptions.OutOfMemoryError("OOM internal")
        client = Mock()
        client.pipeline.return_value = pipeline
        with (
            tenant.tenant_context(OWNER_A),
            patch.object(metrics, "get_redis", return_value=client),
            patch.object(metrics, "_last_metric_warning", 0.0),
            patch.object(metrics.time, "monotonic", side_effect=(100.0, 101.0)),
            patch.object(metrics.logger, "warning") as warning,
        ):
            metrics.record_cache_hit("transcode")
            metrics.record_cache_hit("transcode")
        warning.assert_called_once_with("tenant metrics temporarily unavailable")

    async def test_metrics_are_owner_scoped_and_portal_hides_global_queue_depth(self):
        client = AtomicFakeRedis()
        configured = _configured(metrics_ttl_seconds=600)
        with (
            patch.multiple(metrics, settings=configured, get_redis=Mock(return_value=client)),
            tenant.tenant_context(OWNER_A),
        ):
            metrics.record_cache_hit("transcode")
            snapshot_a = metrics.collect_metrics_snapshot()
        with (
            patch.multiple(metrics, settings=configured, get_redis=Mock(return_value=client)),
            tenant.tenant_context(OWNER_B),
        ):
            metrics.record_cache_miss("transcode")
            snapshot_b = metrics.collect_metrics_snapshot()

        self.assertEqual(snapshot_a["cache_hits"], {"transcode": 1})
        self.assertEqual(snapshot_a["cache_misses"], {})
        self.assertEqual(snapshot_b["cache_hits"], {})
        self.assertEqual(snapshot_b["cache_misses"], {"transcode": 1})
        self.assertTrue(all(key.startswith("metrics:tenant:") for key in client.values))
        self.assertEqual(set(client.ttls.values()), {600})

        with (
            tenant.tenant_context(OWNER_A),
            patch.object(server, "settings", configured),
            patch.object(server, "collect_metrics_snapshot", return_value={"cache_hits": {}}),
            patch.object(server, "Queue") as queue,
        ):
            portal_snapshot = await server.tool_metrics_snapshot()
        self.assertEqual(portal_snapshot["queue_depth"], {})
        queue.assert_not_called()


class ComposeCapacityTests(unittest.TestCase):
    def test_capacity_variables_are_wired_through_config_env_generator_and_manifests(self):
        variables = (
            "REDIS_MAXMEMORY_BYTES",
            "JOB_ADMISSION_OWNER_MAX_ACTIVE",
            "JOB_ADMISSION_GLOBAL_MAX_ACTIVE",
            "JOB_ADMISSION_OWNER_RPM",
            "JOB_ADMISSION_EXECUTION_BUFFER_SECONDS",
            "METRICS_TTL_SECONDS",
            "BRAND_KIT_MAX_COUNT",
            "BRAND_KIT_MAX_SERIALIZED_BYTES",
            "BRAND_KIT_MAX_STRING_CHARS",
            "STORAGE_STAGING_MAX_AGE_SECONDS",
            "INGEST_STAGING_OWNER_MAX_ACTIVE",
            "INGEST_STAGING_GLOBAL_MAX_ACTIVE",
            "INGEST_STAGING_OWNER_MAX_BYTES",
            "INGEST_STAGING_GLOBAL_MAX_BYTES",
            "INGEST_STAGING_LEASE_SECONDS",
            "INGEST_STAGING_HEARTBEAT_SECONDS",
            "S3_CONNECT_TIMEOUT_SECONDS",
            "S3_READ_TIMEOUT_SECONDS",
            "STORAGE_ASGI_MAX_CONCURRENCY",
            "STORAGE_ASGI_ADMISSION_TIMEOUT_SECONDS",
            "STORAGE_ASGI_OPERATION_TIMEOUT_SECONDS",
            "ASSET_QUOTA_OWNER_MAX_COUNT",
            "ASSET_QUOTA_OWNER_MAX_BYTES",
            "ASSET_QUOTA_GLOBAL_MAX_COUNT",
            "ASSET_QUOTA_GLOBAL_MAX_BYTES",
            "ASSET_RESERVATION_LEASE_SECONDS",
            "ASSET_RESERVATION_HEARTBEAT_SECONDS",
            "ASSET_DELETE_LEASE_SECONDS",
            "ASSET_DELETE_RETRY_BASE_SECONDS",
            "ASSET_DELETE_RETRY_MAX_SECONDS",
            "JOB_STORAGE_MAX_OUTPUT_COUNT",
            "JOB_STORAGE_MAX_OUTPUT_BYTES",
            "JOB_STORAGE_MAX_MATERIALIZE_BYTES",
        )
        common_files = (
            ".env.example",
            "scripts/init_runtime_env.py",
            "src/madpanda_ffmpeg_mcp/config.py",
        )
        for filename in common_files:
            content = (REPO_ROOT / filename).read_text(encoding="utf-8")
            for variable in variables:
                self.assertIn(variable, content, (filename, variable))
        for filename in (
            "docker-compose.yml",
            "docker-compose.portal.yml",
            "docker-compose.release.yml",
            "docker-compose.portal.release.yml",
        ):
            content = (REPO_ROOT / filename).read_text(encoding="utf-8")
            for variable in variables:
                self.assertIn(variable, content, (filename, variable))

    def test_new_capacity_settings_fail_closed_outside_bounds(self):
        invalid = (
            ({"redis_maxmemory_bytes": 192 * 1024 * 1024 + 1}, "REDIS_MAXMEMORY_BYTES"),
            ({"job_admission_owner_max_active": 0}, "JOB_ADMISSION_OWNER_MAX_ACTIVE"),
            (
                {
                    "job_admission_owner_max_active": 5,
                    "job_admission_global_max_active": 4,
                },
                "must not exceed",
            ),
            ({"job_admission_owner_rpm": 0}, "JOB_ADMISSION_OWNER_RPM"),
            ({"metrics_ttl_seconds": 59}, "METRICS_TTL_SECONDS"),
            ({"brand_kit_max_count": 0}, "BRAND_KIT_MAX_COUNT"),
            ({"brand_kit_max_serialized_bytes": 1_023}, "BRAND_KIT_MAX_SERIALIZED_BYTES"),
            ({"brand_kit_max_string_chars": 15}, "BRAND_KIT_MAX_STRING_CHARS"),
            (
                {"job_admission_execution_buffer_seconds": 959},
                "must cover the maximum configured job timeout",
            ),
        )
        for overrides, expected in invalid:
            with self.subTest(overrides=overrides):
                errors = _configured(**overrides).runtime_errors()
                self.assertTrue(any(expected in error for error in errors), errors)

    def test_all_compose_variants_pin_noeviction_with_aof_headroom(self):
        for filename in (
            "docker-compose.yml",
            "docker-compose.portal.yml",
            "docker-compose.release.yml",
            "docker-compose.portal.release.yml",
        ):
            with self.subTest(filename=filename):
                content = (REPO_ROOT / filename).read_text(encoding="utf-8")
                self.assertIn("mem_limit: 512m", content)
                self.assertIn("REDIS_MAXMEMORY_BYTES:-201326592", content)
                self.assertLessEqual(201_326_592, 192 * 1024 * 1024)
                self.assertIn('--maxmemory "$$REDIS_MAXMEMORY_BYTES"', content)
                self.assertIn("--maxmemory-policy noeviction", content)
                self.assertIn("--appendonly yes", content)
                self.assertIn('--requirepass "$$REDIS_PASSWORD"', content)
                self.assertIn('user: "999:1000"', content)


if __name__ == "__main__":
    unittest.main()
