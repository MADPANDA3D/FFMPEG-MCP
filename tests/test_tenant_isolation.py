import json
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from madpanda_ffmpeg_mcp import jobs, redis_store, server, tenant

OWNER_A = "a" * 64
OWNER_B = "b" * 64
ASSET_A = "1" * 32
ASSET_B = "2" * 32


def _asset(asset_id: str) -> dict:
    return {
        "asset_id": asset_id,
        "expires_at": int(time.time()) + 60,
        "size_bytes": 5,
        "storage_key": f"{asset_id[:2]}/{asset_id[2:4]}/{asset_id}.mp4",
    }


class FakeRedis:
    def __init__(self):
        self.values: dict[str, str] = {}
        self.sets: dict[str, set[str]] = {}
        self.sorted_sets: dict[str, dict[str, int]] = {}
        self.hashes: dict[str, dict[str, int]] = {}

    def set(self, key, value, ex=None):
        del ex
        self.values[key] = value

    def get(self, key):
        return self.values.get(key)

    def delete(self, key):
        self.values.pop(key, None)

    def zadd(self, key, mapping):
        self.sorted_sets.setdefault(key, {}).update(mapping)

    def zrem(self, key, member):
        self.sorted_sets.setdefault(key, {}).pop(member, None)

    def zrangebyscore(self, key, minimum, maximum):
        return [
            member
            for member, score in self.sorted_sets.get(key, {}).items()
            if float(minimum) <= score <= float(maximum)
        ]

    def sadd(self, key, member):
        self.sets.setdefault(key, set()).add(member)

    def srem(self, key, member):
        self.sets.setdefault(key, set()).discard(member)

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def eval(self, script, numkeys, *values):
        del numkeys
        if script == redis_store._ASSET_RESERVE_SCRIPT:
            (
                asset_key,
                owner_quota_key,
                global_quota_key,
                expiry_key,
                payload,
                reservation_token,
                owner_hash,
                size_bytes,
                owner_max_count,
                owner_max_bytes,
                global_max_count,
                global_max_bytes,
                lease_until,
                asset_id,
                storage_key,
                expires_at,
            ) = values
            existing = self.values.get(asset_key)
            if existing:
                record = json.loads(existing)
                if (
                    record.get("state") == "reserved"
                    and record.get("reservation_token") == reservation_token
                    and record.get("owner_hash") == owner_hash
                    and int(record.get("size_bytes", -1)) == int(size_bytes)
                    and record.get("storage_key") == storage_key
                    and int(record.get("expires_at", -1)) == int(expires_at)
                ):
                    return [1, 0]
                return [0, 4]
            owner_quota = self.hashes.setdefault(owner_quota_key, {})
            global_quota = self.hashes.setdefault(global_quota_key, {})
            if owner_quota.get("count", 0) + 1 > int(owner_max_count) or owner_quota.get(
                "bytes", 0
            ) + int(size_bytes) > int(owner_max_bytes):
                return [0, 1]
            if global_quota.get("count", 0) + 1 > int(global_max_count) or global_quota.get(
                "bytes", 0
            ) + int(size_bytes) > int(global_max_bytes):
                return [0, 2]
            owner_quota["count"] = owner_quota.get("count", 0) + 1
            owner_quota["bytes"] = owner_quota.get("bytes", 0) + int(size_bytes)
            global_quota["count"] = global_quota.get("count", 0) + 1
            global_quota["bytes"] = global_quota.get("bytes", 0) + int(size_bytes)
            self.values[asset_key] = payload
            self.sorted_sets.setdefault(expiry_key, {})[asset_id] = int(lease_until)
            return [1, 0]
        if script == redis_store._ASSET_COMMIT_SCRIPT:
            asset_key, expiry_key, asset_id, reservation_token, payload, expires_at = values
            existing = json.loads(self.values.get(asset_key, "{}"))
            if (
                existing.get("state") == "active"
                and existing.get("reservation_token") == reservation_token
            ):
                return 1
            if (
                existing.get("state") != "reserved"
                or existing.get("reservation_token") != reservation_token
            ):
                return 0
            self.values[asset_key] = payload
            self.sorted_sets.setdefault(expiry_key, {})[asset_id] = int(expires_at)
            return 1
        if script == redis_store._ASSET_UPDATE_SCRIPT:
            asset_key, _asset_id, owner_hash, now, payload = values
            existing = json.loads(self.values.get(asset_key, "{}"))
            if (
                existing.get("state") != "active"
                or existing.get("owner_hash") != owner_hash
                or int(existing.get("expires_at", 0)) <= int(now)
            ):
                return 0
            self.values[asset_key] = payload
            return 1
        if script == redis_store._ASSET_CLAIM_DELETE_SCRIPT:
            (
                asset_key,
                expiry_key,
                asset_id,
                now,
                lease_until,
                delete_token,
                force,
                owner_hash,
            ) = values
            existing = self.values.get(asset_key)
            if not existing:
                self.sorted_sets.setdefault(expiry_key, {}).pop(asset_id, None)
                return ""
            record = json.loads(existing)
            eligible = False
            if record.get("state") == "active":
                eligible = int(record.get("expires_at", 0)) <= int(now) or (
                    force == "1" and record.get("owner_hash") == owner_hash
                )
            elif record.get("state") == "reserved":
                eligible = int(record.get("lease_until", 0)) <= int(now)
            elif record.get("state") == "delete_pending":
                eligible = int(record.get("retry_at", 0)) <= int(now)
            elif record.get("state") == "deleting":
                eligible = int(record.get("lease_until", 0)) <= int(now)
            if not eligible:
                return ""
            record.update(
                state="deleting",
                delete_token=delete_token,
                lease_until=int(lease_until),
            )
            record.pop("retry_at", None)
            payload = json.dumps(record, separators=(",", ":"))
            self.values[asset_key] = payload
            self.sorted_sets.setdefault(expiry_key, {})[asset_id] = int(lease_until)
            return payload
        if script == redis_store._ASSET_FINALIZE_DELETE_SCRIPT:
            (
                asset_key,
                owner_quota_key,
                global_quota_key,
                expiry_key,
                asset_id,
                delete_token,
                owner_hash,
                size_bytes,
            ) = values
            existing = self.values.get(asset_key)
            if not existing:
                self.sorted_sets.setdefault(expiry_key, {}).pop(asset_id, None)
                return 1
            record = json.loads(existing)
            if (
                record.get("state") != "deleting"
                or record.get("delete_token") != delete_token
                or record.get("owner_hash") != owner_hash
                or int(record.get("size_bytes", -1)) != int(size_bytes)
            ):
                return 0
            for quota_key in (owner_quota_key, global_quota_key):
                quota = self.hashes.get(quota_key, {})
                if quota.get("count", 0) < 1 or quota.get("bytes", 0) < int(size_bytes):
                    return -2
            for quota_key in (owner_quota_key, global_quota_key):
                quota = self.hashes[quota_key]
                quota["count"] -= 1
                quota["bytes"] -= int(size_bytes)
                if quota == {"count": 0, "bytes": 0}:
                    self.hashes.pop(quota_key)
            self.values.pop(asset_key, None)
            self.sorted_sets.setdefault(expiry_key, {}).pop(asset_id, None)
            return 1
        if script == redis_store._BRAND_KIT_SAVE_SCRIPT:
            key, index_key, brand_kit_id, payload, maximum = values
            if brand_kit_id not in self.sets.get(index_key, set()) and len(
                self.sets.get(index_key, set())
            ) >= int(maximum):
                return 0
            self.values[key] = payload
            self.sets.setdefault(index_key, set()).add(brand_kit_id)
            return 1
        if script == redis_store._BRAND_KIT_DELETE_SCRIPT:
            key, index_key, brand_kit_id = values
            deleted = int(key in self.values)
            self.values.pop(key, None)
            self.sets.setdefault(index_key, set()).discard(brand_kit_id)
            return deleted
        if script == redis_store._JOB_ADMISSION_SCRIPT:
            global_key, owner_key, rate_key, job_id, now_ms, expires_ms, *_ = values
            self.sorted_sets.setdefault(global_key, {})[job_id] = int(expires_ms)
            self.sorted_sets.setdefault(owner_key, {})[job_id] = int(expires_ms)
            self.sorted_sets.setdefault(rate_key, {})[job_id] = int(now_ms)
            return [1, 0]
        if script in {
            redis_store._JOB_ADMISSION_RELEASE_SCRIPT,
            redis_store._JOB_ADMISSION_ROLLBACK_SCRIPT,
        }:
            keys = values[:2] if script == redis_store._JOB_ADMISSION_RELEASE_SCRIPT else values[:3]
            job_id = values[len(keys)]
            removed = 0
            for key in keys:
                removed += int(job_id in self.sorted_sets.get(key, {}))
                self.sorted_sets.setdefault(key, {}).pop(job_id, None)
            return removed
        raise AssertionError("unexpected Lua script")


class TenantIsolationTests(unittest.TestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.redis_patch = patch.object(redis_store, "_redis_client", self.redis)
        self.redis_patch.start()

    def tearDown(self):
        self.redis_patch.stop()

    def test_hmac_is_namespaced_and_never_contains_principal(self):
        principal = "customer@example.test"
        first = tenant.hash_principal(principal, "s" * 32, namespace="portal")
        repeat = tenant.hash_principal(principal, "s" * 32, namespace="portal")
        other = tenant.hash_principal(principal, "s" * 32, namespace="standalone")
        self.assertEqual(first, repeat)
        self.assertRegex(first, r"^[a-f0-9]{64}$")
        self.assertNotEqual(first, other)
        self.assertNotIn(principal, first)

    def test_assets_jobs_and_cache_are_tenant_isolated(self):
        asset = _asset(ASSET_A)
        reservation_token = "3" * 32
        with tenant.tenant_context(OWNER_A):
            self.assertEqual(redis_store.reserve_asset(asset, reservation_token), OWNER_A)
            self.assertIsNone(redis_store.get_asset(ASSET_A))
            redis_store.commit_asset(asset, reservation_token)
            redis_store.save_job({"job_id": "job", "expires_at": 100}, 60)
            cache_key = redis_store.build_cache_key("test", {"asset_id": ASSET_A})
            redis_store.set_cached_result(cache_key, {"ok": True}, 60)
            self.assertNotIn("owner_hash", redis_store.get_asset(ASSET_A))

        stored = json.loads(self.redis.values[f"asset:{ASSET_A}"])
        self.assertEqual(stored["owner_hash"], OWNER_A)
        self.assertEqual(stored["state"], "active")
        with tenant.tenant_context(OWNER_B):
            self.assertIsNone(redis_store.get_asset(ASSET_A))
            self.assertIsNone(redis_store.get_job("job"))
            self.assertIsNone(redis_store.get_cached_result(cache_key))
            self.assertIsNone(redis_store.update_asset(ASSET_A, {"mime_type": "evil"}))
            self.assertIsNone(redis_store.claim_asset_deletion(ASSET_A, "4" * 32, force=True))
            redis_store.delete_cached_result(cache_key)
            with self.assertRaises(PermissionError):
                redis_store.reserve_asset(
                    {**asset, "owner_hash": OWNER_A},
                    "5" * 32,
                )

        with tenant.tenant_context(OWNER_A):
            self.assertIsNotNone(redis_store.get_asset(ASSET_A))
            self.assertIsNotNone(redis_store.get_cached_result(cache_key))

    def test_brand_kits_with_same_id_are_independent(self):
        with tenant.tenant_context(OWNER_A):
            redis_store.save_brand_kit({"brand_kit_id": "shared", "name": "A"})
        with tenant.tenant_context(OWNER_B):
            redis_store.save_brand_kit({"brand_kit_id": "shared", "name": "B"})
            self.assertEqual(redis_store.get_brand_kit("shared")["name"], "B")
            redis_store.delete_brand_kit("shared")
        with tenant.tenant_context(OWNER_A):
            self.assertEqual(redis_store.get_brand_kit("shared")["name"], "A")

    def test_no_context_cleanup_uses_explicit_claim_and_finalize_lifecycle(self):
        asset = _asset(ASSET_B)
        with tenant.tenant_context(OWNER_A):
            redis_store.reserve_asset(asset, "6" * 32)
            redis_store.commit_asset(asset, "6" * 32)

        self.assertIsNone(redis_store.get_asset(ASSET_B))
        self.assertEqual(redis_store.get_asset_control(ASSET_B)["owner_hash"], OWNER_A)
        self.assertIsNone(redis_store.claim_asset_deletion(ASSET_B, "7" * 32))
        claimed = redis_store.claim_asset_deletion(
            ASSET_B,
            "8" * 32,
            force=True,
            owner_hash=OWNER_A,
        )
        self.assertEqual(claimed["state"], "deleting")
        self.assertTrue(redis_store.finalize_asset_deletion(claimed))
        self.assertIsNone(redis_store.get_asset_control(ASSET_B))

    def test_queue_carries_only_owner_hash_and_safe_description(self):
        captured: dict = {}

        class Queue:
            def enqueue(self, func, **kwargs):
                captured["func"] = func
                captured.update(kwargs)

        with (
            tenant.tenant_context(OWNER_A),
            patch.object(server, "get_queue", return_value=Queue()),
            patch.object(server, "save_job"),
            patch.object(server, "record_cache_miss"),
            patch.object(server, "log_event"),
        ):
            job_id = server._enqueue_job("transcode", lambda: None, ("asset",))

        self.assertIs(captured["func"], jobs.execute_tenant_job)
        self.assertEqual(captured["meta"], {"owner_hash": OWNER_A})
        self.assertEqual(captured["description"], f"transcode:{job_id}")
        self.assertNotIn("asset", captured["description"])

    def test_rq_metadata_inherits_tenant_without_raw_subject(self):
        rq_job = SimpleNamespace(meta={"owner_hash": OWNER_A})
        with patch("rq.get_current_job", return_value=rq_job):
            self.assertEqual(tenant.current_owner_hash(), OWNER_A)

    def test_error_and_log_sanitization_is_bounded(self):
        raw = {
            "error": "Traceback token=secret https://evil.test /data/private/file.mp4",
            "logs_short": "authorization=secret\n/path/to/private/file.mp4 https://evil.test " * 30,
            "traceback": "never persist",
            "args": ["raw"],
            "subject": "person@example.test",
        }
        sanitized = jobs._sanitize_job_updates(raw)
        serialized = repr(sanitized).lower()
        for forbidden in ("traceback", "evil.test", "/data/private", "person@example"):
            self.assertNotIn(forbidden, serialized)
        self.assertLessEqual(len(sanitized["error"]), jobs.settings.job_error_max_chars)
        self.assertLessEqual(len(sanitized["logs_short"]), jobs.settings.job_log_max_chars)


if __name__ == "__main__":
    unittest.main()
