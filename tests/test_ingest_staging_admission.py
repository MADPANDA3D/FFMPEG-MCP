from __future__ import annotations

import asyncio
import json
import os
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

from madpanda_ffmpeg_mcp import ingest, redis_store

OWNER_A = "a" * 64
OWNER_B = "b" * 64
OWNER_C = "c" * 64


class AtomicStagingRedis:
    """Atomic in-memory model of the dedicated staging-lease Lua contract."""

    def __init__(self) -> None:
        self.records: dict[str, dict[str, Any]] = {}
        self.expiry: dict[str, int] = {}
        self.lock = threading.RLock()
        self.fail_eval = False

    def eval(self, script: str, numkeys: int, *values: Any) -> object:
        del numkeys
        if self.fail_eval:
            raise RuntimeError("simulated Redis outage with internal details")
        with self.lock:
            if script == redis_store._INGEST_STAGING_RESERVE_SCRIPT:
                return self._reserve(values)
            if script == redis_store._INGEST_STAGING_REFRESH_SCRIPT:
                return self._refresh(values)
            if script == redis_store._INGEST_STAGING_RELEASE_SCRIPT:
                return self._release(values)
        raise AssertionError("unexpected Lua script")

    def _reserve(self, values: tuple[Any, ...]) -> list[int]:
        (
            _records_key,
            _expiry_key,
            reservation_id,
            token,
            owner,
            reserved_bytes,
            serialized,
            now,
            owner_max_active,
            owner_max_bytes,
            global_max_active,
            global_max_bytes,
            lease_until,
            scan_limit,
        ) = values
        if len(self.records) != len(self.expiry) or len(self.records) > int(scan_limit):
            return [0, 6]
        for existing_id in list(self.expiry):
            record = self.records.get(str(existing_id))
            if record is None:
                return [0, 6]
            if int(record["lease_until"]) <= int(now):
                self.records.pop(str(existing_id), None)
                self.expiry.pop(str(existing_id), None)

        existing = self.records.get(str(reservation_id))
        if existing is not None:
            if (
                existing["owner_hash"] == owner
                and existing["token"] == token
                and int(existing["reserved_bytes"]) == int(reserved_bytes)
            ):
                return [1, 0]
            return [0, 4]

        active = list(self.records.values())
        owner_active = [record for record in active if record["owner_hash"] == owner]
        owner_bytes = sum(int(record["reserved_bytes"]) for record in owner_active)
        global_bytes = sum(int(record["reserved_bytes"]) for record in active)
        if len(owner_active) + 1 > int(owner_max_active) or owner_bytes + int(reserved_bytes) > int(
            owner_max_bytes
        ):
            return [0, 1]
        if len(active) + 1 > int(global_max_active) or global_bytes + int(reserved_bytes) > int(
            global_max_bytes
        ):
            return [0, 2]

        record = json.loads(str(serialized))
        self.records[str(reservation_id)] = record
        self.expiry[str(reservation_id)] = int(lease_until)
        return [1, 0]

    def _refresh(self, values: tuple[Any, ...]) -> int:
        (
            _records_key,
            _expiry_key,
            reservation_id,
            token,
            owner,
            now,
            lease_until,
        ) = values
        record = self.records.get(str(reservation_id))
        if (
            record is None
            or record["token"] != token
            or record["owner_hash"] != owner
            or int(record["lease_until"]) <= int(now)
            or int(lease_until) <= int(now)
        ):
            return 0
        record["lease_until"] = int(lease_until)
        self.expiry[str(reservation_id)] = int(lease_until)
        return int(lease_until)

    def _release(self, values: tuple[Any, ...]) -> int:
        _records_key, _expiry_key, reservation_id, token, owner = values
        record = self.records.get(str(reservation_id))
        if record is None:
            self.expiry.pop(str(reservation_id), None)
            return 1
        if record["token"] != token or record["owner_hash"] != owner:
            return 0
        self.records.pop(str(reservation_id), None)
        self.expiry.pop(str(reservation_id), None)
        return 1


def _settings(**overrides: Any):
    values: dict[str, Any] = {
        "max_ingest_bytes": 100,
        "ingest_staging_owner_max_active": 2,
        "ingest_staging_global_max_active": 3,
        "ingest_staging_owner_max_bytes": 200,
        "ingest_staging_global_max_bytes": 300,
        "ingest_staging_lease_seconds": 60,
        "ingest_staging_heartbeat_seconds": 5,
    }
    values.update(overrides)
    return replace(redis_store.settings, **values)


class IngestStagingAdmissionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.redis = AtomicStagingRedis()

    def test_atomic_concurrency_enforces_owner_and_global_limits(self) -> None:
        configured = _settings()

        def attempt(owner: str):
            try:
                return redis_store.reserve_ingest_staging(owner_hash=owner)
            except redis_store.IngestStagingAdmissionError as exc:
                return str(exc)

        with (
            patch.object(redis_store, "settings", configured),
            patch.object(redis_store, "_redis_client", self.redis),
            ThreadPoolExecutor(max_workers=12) as executor,
        ):
            results = list(executor.map(attempt, [OWNER_A] * 8 + [OWNER_B] * 8))

        admitted = [
            result for result in results if isinstance(result, redis_store.IngestStagingReservation)
        ]
        self.assertEqual(len(admitted), 3)
        self.assertLessEqual(sum(item.owner_hash == OWNER_A for item in admitted), 2)
        self.assertLessEqual(sum(item.owner_hash == OWNER_B for item in admitted), 2)
        self.assertTrue(
            set(result for result in results if isinstance(result, str))
            <= {
                "tenant remote-ingest staging limit reached",
                "service remote-ingest staging limit reached",
            }
        )

    def test_original_byte_charge_and_lowered_quota_still_reap_expired_leases(self) -> None:
        initial = _settings(
            ingest_staging_owner_max_active=2,
            ingest_staging_global_max_active=2,
            ingest_staging_global_max_bytes=200,
        )
        with (
            patch.object(redis_store, "settings", initial),
            patch.object(redis_store, "_redis_client", self.redis),
            patch.object(redis_store, "_now_ts", return_value=1_000),
        ):
            first = redis_store.reserve_ingest_staging(owner_hash=OWNER_A)
            second = redis_store.reserve_ingest_staging(owner_hash=OWNER_B)

        changed_size = replace(
            initial,
            max_ingest_bytes=25,
            ingest_staging_owner_max_bytes=100,
            ingest_staging_global_max_active=3,
            ingest_staging_global_max_bytes=224,
        )
        with (
            patch.object(redis_store, "settings", changed_size),
            patch.object(redis_store, "_redis_client", self.redis),
            patch.object(redis_store, "_now_ts", return_value=1_001),
            self.assertRaisesRegex(
                redis_store.IngestStagingAdmissionError,
                "service remote-ingest staging limit reached",
            ),
        ):
            redis_store.reserve_ingest_staging(owner_hash=OWNER_C)

        lowered = replace(
            changed_size,
            ingest_staging_owner_max_active=1,
            ingest_staging_global_max_active=1,
            ingest_staging_owner_max_bytes=25,
            ingest_staging_global_max_bytes=25,
        )
        with (
            patch.object(redis_store, "settings", lowered),
            patch.object(redis_store, "_redis_client", self.redis),
            patch.object(
                redis_store,
                "_now_ts",
                return_value=max(first.lease_until, second.lease_until),
            ),
        ):
            replacement = redis_store.reserve_ingest_staging(owner_hash=OWNER_C)

        self.assertEqual(list(self.redis.records), [replacement.reservation_id])
        self.assertEqual(self.redis.records[replacement.reservation_id]["reserved_bytes"], 25)

    def test_refresh_and_release_are_token_fenced_and_expiry_is_not_revived(self) -> None:
        configured = _settings(
            ingest_staging_owner_max_active=1,
            ingest_staging_global_max_active=1,
            ingest_staging_owner_max_bytes=100,
            ingest_staging_global_max_bytes=100,
        )
        with (
            patch.object(redis_store, "settings", configured),
            patch.object(redis_store, "_redis_client", self.redis),
            patch.object(redis_store, "_now_ts", return_value=1_000),
        ):
            reservation = redis_store.reserve_ingest_staging(owner_hash=OWNER_A)

        wrong_token = replace(reservation, token="f" * 32)
        with (
            patch.object(redis_store, "settings", configured),
            patch.object(redis_store, "_redis_client", self.redis),
            patch.object(redis_store, "_now_ts", return_value=1_010),
        ):
            self.assertIsNone(redis_store.refresh_ingest_staging(wrong_token))
            self.assertFalse(redis_store.release_ingest_staging(wrong_token))
            self.assertEqual(redis_store.refresh_ingest_staging(reservation), 1_070)

        with (
            patch.object(redis_store, "settings", configured),
            patch.object(redis_store, "_redis_client", self.redis),
            patch.object(redis_store, "_now_ts", return_value=1_070),
        ):
            self.assertIsNone(redis_store.refresh_ingest_staging(reservation))
            replacement = redis_store.reserve_ingest_staging(owner_hash=OWNER_B)
            self.assertTrue(redis_store.release_ingest_staging(reservation))
            self.assertTrue(redis_store.release_ingest_staging(replacement))
            self.assertTrue(redis_store.release_ingest_staging(replacement))

    def test_redis_outage_fails_closed_without_secret_details(self) -> None:
        self.redis.fail_eval = True
        configured = _settings()
        reservation = redis_store.IngestStagingReservation(
            reservation_id="1" * 32,
            token="2" * 32,
            owner_hash=OWNER_A,
            reserved_bytes=100,
            lease_until=1_060,
        )
        with (
            patch.object(redis_store, "settings", configured),
            patch.object(redis_store, "_redis_client", self.redis),
            self.assertRaisesRegex(
                redis_store.IngestStagingAdmissionError,
                "^remote-ingest staging admission unavailable$",
            ),
        ):
            redis_store.reserve_ingest_staging(owner_hash=OWNER_A)
        with (
            patch.object(redis_store, "settings", configured),
            patch.object(redis_store, "_redis_client", self.redis),
        ):
            self.assertIsNone(redis_store.refresh_ingest_staging(reservation))
            self.assertFalse(redis_store.release_ingest_staging(reservation))


class RemoteIngestStagingContextTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancellation_during_acquisition_reconciles_then_re_raises(self) -> None:
        reservation = redis_store.IngestStagingReservation(
            reservation_id="a" * 32,
            token="b" * 32,
            owner_hash=OWNER_A,
            reserved_bytes=100,
            lease_until=int(time.time()) + 60,
        )
        acquisition_started = threading.Event()
        allow_acquisition = threading.Event()
        release_started = threading.Event()
        allow_release = threading.Event()
        release_finished = threading.Event()

        def reserve() -> redis_store.IngestStagingReservation:
            acquisition_started.set()
            if not allow_acquisition.wait(1):
                raise AssertionError("test did not settle staging acquisition")
            return reservation

        def release(candidate: redis_store.IngestStagingReservation) -> bool:
            self.assertEqual(candidate, reservation)
            release_started.set()
            if not allow_release.wait(1):
                raise AssertionError("test did not settle staging release")
            release_finished.set()
            return True

        async def request() -> None:
            async with ingest._remote_ingest_staging_admission():
                self.fail("cancelled acquisition yielded control")

        with (
            patch.object(ingest, "reserve_ingest_staging", side_effect=reserve),
            patch.object(ingest, "release_ingest_staging", side_effect=release),
        ):
            task = asyncio.create_task(request())
            self.assertTrue(await asyncio.to_thread(acquisition_started.wait, 1))
            task.cancel()
            task.cancel()
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            allow_acquisition.set()
            self.assertTrue(await asyncio.to_thread(release_started.wait, 1))
            self.assertFalse(task.done())
            allow_release.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
        self.assertTrue(release_finished.is_set())

    async def test_cancelled_failed_acquisition_preserves_cancel_without_release(self) -> None:
        acquisition_started = threading.Event()
        allow_failure = threading.Event()
        release = Mock(return_value=True)

        def reserve() -> redis_store.IngestStagingReservation:
            acquisition_started.set()
            if not allow_failure.wait(1):
                raise AssertionError("test did not settle staging acquisition")
            raise redis_store.IngestStagingAdmissionError(
                "remote-ingest staging admission unavailable"
            )

        async def request() -> None:
            async with ingest._remote_ingest_staging_admission():
                self.fail("failed acquisition yielded control")

        with (
            patch.object(ingest, "reserve_ingest_staging", side_effect=reserve),
            patch.object(ingest, "release_ingest_staging", side_effect=release),
        ):
            task = asyncio.create_task(request())
            self.assertTrue(await asyncio.to_thread(acquisition_started.wait, 1))
            task.cancel()
            allow_failure.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 1)
        release.assert_not_called()

    async def test_cancelled_ffprobe_retains_lease_for_unlinked_live_inode(self) -> None:
        reservation = redis_store.IngestStagingReservation(
            reservation_id="c" * 32,
            token="d" * 32,
            owner_hash=OWNER_A,
            reserved_bytes=100,
            lease_until=int(time.time()) + 60,
        )
        configured = SimpleNamespace(ingest_staging_heartbeat_seconds=30)
        probe_started = threading.Event()
        allow_probe = threading.Event()
        probe_finished = threading.Event()
        release_finished = threading.Event()

        def probe(path: str) -> dict[str, object]:
            with open(path, "rb") as handle:
                probe_started.set()
                if not allow_probe.wait(1):
                    raise AssertionError("test did not settle ffprobe")
                self.assertEqual(handle.read(), b"staged-media")
            probe_finished.set()
            return {"duration_sec": 1.0}

        def release(candidate: redis_store.IngestStagingReservation) -> bool:
            self.assertEqual(candidate, reservation)
            release_finished.set()
            return True

        async def request(path: str) -> None:
            async with ingest._remote_ingest_staging_admission() as lease:
                prior = asyncio.Event()
                prior.set()
                lease.retain_until_settled(prior)
                await ingest._run_ingest_ffprobe(path, lease)

        descriptor, path = tempfile.mkstemp()
        try:
            os.write(descriptor, b"staged-media")
        finally:
            os.close(descriptor)
        try:
            with (
                patch.object(ingest, "settings", configured),
                patch.object(ingest, "reserve_ingest_staging", return_value=reservation),
                patch.object(ingest, "release_ingest_staging", side_effect=release),
                patch.object(ingest, "run_ffprobe", side_effect=probe),
            ):
                task = asyncio.create_task(request(path))
                self.assertTrue(await asyncio.to_thread(probe_started.wait, 1))
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 0.2)
                self.assertEqual(len(ingest._STAGING_OPERATIONS), 1)
                self.assertEqual(len(ingest._STAGING_FINALIZERS), 1)
                self.assertFalse(release_finished.is_set())
                os.unlink(path)
                self.assertFalse(os.path.exists(path))
                self.assertFalse(release_finished.is_set())
                allow_probe.set()
                self.assertTrue(await asyncio.to_thread(probe_finished.wait, 1))
                self.assertTrue(await asyncio.to_thread(release_finished.wait, 1))
                await asyncio.wait_for(
                    asyncio.gather(*tuple(ingest._STAGING_FINALIZERS)),
                    1,
                )
                await asyncio.sleep(0)
            self.assertFalse(ingest._STAGING_OPERATIONS)
            self.assertFalse(ingest._STAGING_FINALIZERS)
        finally:
            allow_probe.set()
            if os.path.exists(path):
                os.unlink(path)

    async def test_normal_ffprobe_settlement_can_transfer_to_persistence(self) -> None:
        reservation = redis_store.IngestStagingReservation(
            reservation_id="e" * 32,
            token="f" * 32,
            owner_hash=OWNER_A,
            reserved_bytes=100,
            lease_until=int(time.time()) + 60,
        )
        lease = ingest._RemoteIngestStagingLease(
            reservation=reservation,
            confirmed_lease_until=reservation.lease_until,
        )
        with patch.object(ingest, "run_ffprobe", return_value={"duration_sec": 1.0}):
            result = await ingest._run_ingest_ffprobe("unused", lease)
        self.assertEqual(result, {"duration_sec": 1.0})
        persistence_settled = asyncio.Event()
        lease.retain_until_settled(persistence_settled)
        self.assertIs(lease.unsettled_retention(), persistence_settled)
        await asyncio.sleep(0)
        self.assertFalse(ingest._STAGING_OPERATIONS)

    async def test_storage_timeout_returns_while_retained_lease_finishes_late(self) -> None:
        reservation = redis_store.IngestStagingReservation(
            reservation_id="7" * 32,
            token="8" * 32,
            owner_hash=OWNER_A,
            reserved_bytes=100,
            lease_until=int(time.time()) + 60,
        )
        configured = SimpleNamespace(ingest_staging_heartbeat_seconds=30)
        settled = asyncio.Event()
        release_finished = threading.Event()

        def release(candidate: redis_store.IngestStagingReservation) -> bool:
            self.assertEqual(candidate, reservation)
            release_finished.set()
            return True

        async def request() -> None:
            async with ingest._remote_ingest_staging_admission() as lease:
                lease.retain_until_settled(settled)
                raise ingest.IngestError("Storage operation timed out")

        with (
            patch.object(ingest, "settings", configured),
            patch.object(ingest, "reserve_ingest_staging", return_value=reservation),
            patch.object(ingest, "release_ingest_staging", side_effect=release),
        ):
            with self.assertRaisesRegex(ingest.IngestError, "Storage operation timed out"):
                await asyncio.wait_for(request(), 0.2)
            self.assertFalse(release_finished.is_set())
            self.assertEqual(len(ingest._STAGING_FINALIZERS), 1)
            settled.set()
            self.assertTrue(await asyncio.to_thread(release_finished.wait, 1))
            await asyncio.wait_for(
                asyncio.gather(*tuple(ingest._STAGING_FINALIZERS)),
                1,
            )
            await asyncio.sleep(0)
        self.assertFalse(ingest._STAGING_FINALIZERS)

    async def test_storage_cancellation_returns_while_retained_lease_finishes_late(self) -> None:
        reservation = redis_store.IngestStagingReservation(
            reservation_id="9" * 32,
            token="0" * 32,
            owner_hash=OWNER_A,
            reserved_bytes=100,
            lease_until=int(time.time()) + 60,
        )
        configured = SimpleNamespace(ingest_staging_heartbeat_seconds=30)
        settled = asyncio.Event()
        body_started = asyncio.Event()
        release_finished = threading.Event()

        def release(candidate: redis_store.IngestStagingReservation) -> bool:
            self.assertEqual(candidate, reservation)
            release_finished.set()
            return True

        async def request() -> None:
            async with ingest._remote_ingest_staging_admission() as lease:
                lease.retain_until_settled(settled)
                body_started.set()
                await asyncio.Event().wait()

        with (
            patch.object(ingest, "settings", configured),
            patch.object(ingest, "reserve_ingest_staging", return_value=reservation),
            patch.object(ingest, "release_ingest_staging", side_effect=release),
        ):
            task = asyncio.create_task(request())
            await asyncio.wait_for(body_started.wait(), 1)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 0.2)
            self.assertFalse(release_finished.is_set())
            self.assertEqual(len(ingest._STAGING_FINALIZERS), 1)
            settled.set()
            self.assertTrue(await asyncio.to_thread(release_finished.wait, 1))
            await asyncio.wait_for(
                asyncio.gather(*tuple(ingest._STAGING_FINALIZERS)),
                1,
            )
            await asyncio.sleep(0)
        self.assertFalse(ingest._STAGING_FINALIZERS)

    async def test_cancellation_waits_for_token_fenced_release(self) -> None:
        reservation = redis_store.IngestStagingReservation(
            reservation_id="5" * 32,
            token="6" * 32,
            owner_hash=OWNER_A,
            reserved_bytes=100,
            lease_until=int(time.time()) + 60,
        )
        configured = SimpleNamespace(ingest_staging_heartbeat_seconds=30)
        body_started = asyncio.Event()
        release_started = threading.Event()
        allow_release = threading.Event()
        release_finished = threading.Event()

        def release(candidate: redis_store.IngestStagingReservation) -> bool:
            self.assertEqual(candidate, reservation)
            release_started.set()
            if not allow_release.wait(1):
                raise AssertionError("test did not allow staging release")
            release_finished.set()
            return True

        async def request() -> None:
            async with ingest._remote_ingest_staging_admission():
                body_started.set()
                await asyncio.Event().wait()

        with (
            patch.object(ingest, "settings", configured),
            patch.object(ingest, "reserve_ingest_staging", return_value=reservation),
            patch.object(ingest, "release_ingest_staging", side_effect=release),
        ):
            task = asyncio.create_task(request())
            await asyncio.wait_for(body_started.wait(), 1)
            task.cancel()
            self.assertTrue(await asyncio.to_thread(release_started.wait, 1))
            self.assertFalse(task.done())
            allow_release.set()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertTrue(release_finished.is_set())

    async def test_failure_and_expired_entry_both_release_the_reservation(self) -> None:
        reservation = redis_store.IngestStagingReservation(
            reservation_id="3" * 32,
            token="4" * 32,
            owner_hash=OWNER_A,
            reserved_bytes=100,
            lease_until=int(time.time()) + 60,
        )
        configured = SimpleNamespace(ingest_staging_heartbeat_seconds=30)
        release = Mock(return_value=True)
        with (
            patch.object(ingest, "settings", configured),
            patch.object(ingest, "reserve_ingest_staging", return_value=reservation),
            patch.object(ingest, "release_ingest_staging", side_effect=release),
            self.assertRaisesRegex(RuntimeError, "body failed"),
        ):
            async with ingest._remote_ingest_staging_admission():
                raise RuntimeError("body failed")
        self.assertEqual(release.call_count, 1)

        expired = replace(reservation, lease_until=int(time.time()))
        release.reset_mock()
        with (
            patch.object(ingest, "settings", configured),
            patch.object(ingest, "reserve_ingest_staging", return_value=expired),
            patch.object(ingest, "release_ingest_staging", side_effect=release),
            self.assertRaisesRegex(ingest.IngestError, "staging admission expired"),
        ):
            async with ingest._remote_ingest_staging_admission():
                self.fail("expired staging admission yielded control")
        self.assertEqual(release.call_count, 1)

    async def test_admission_rejection_happens_before_http_client_creation(self) -> None:
        configured = replace(
            ingest.settings,
            allowed_domains=["media.example.com"],
            asset_ttl_hours=24,
            max_asset_ttl_hours=168,
        )
        http_client = Mock()
        with (
            patch.object(ingest, "settings", configured),
            patch.object(
                ingest,
                "reserve_ingest_staging",
                side_effect=redis_store.IngestStagingAdmissionError(
                    "service remote-ingest staging limit reached"
                ),
            ),
            patch.object(ingest.httpx, "AsyncClient", http_client),
            self.assertRaisesRegex(
                ingest.IngestError,
                "service remote-ingest staging limit reached",
            ),
        ):
            await ingest.ingest_from_url(
                "https://media.example.com/video.mp4",
                None,
                None,
            )
        http_client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
