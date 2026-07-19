from __future__ import annotations

import asyncio
import os
import tempfile
import threading
import time
import unittest
import weakref
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

from madpanda_ffmpeg_mcp import cleanup, config, jobs, storage
from madpanda_ffmpeg_mcp.tenant import require_owner_hash, tenant_context

OWNER = "a" * 64


def _asgi_settings(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "storage_backend": "local",
        "storage_asgi_max_concurrency": 1,
        "storage_asgi_admission_timeout_seconds": 0.03,
        "storage_asgi_operation_timeout_seconds": 0.04,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _runtime_settings(**overrides: object) -> config.Settings:
    values: dict[str, object] = {
        "mcp_mode": "standalone",
        "mcp_access_token": "a" * 40,
        "principal_hash_secret": "h" * 40,
        "download_signing_secret": "d" * 40,
    }
    values.update(overrides)
    return replace(config.settings, **values)


async def _wait_for_event(event: threading.Event, timeout: float = 1.0) -> None:
    if not await asyncio.wait_for(asyncio.to_thread(event.wait, timeout), timeout + 0.1):
        raise AssertionError("thread event was not signaled")


class AsgiStorageLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_tenant_context_reaches_storage_executor(self) -> None:
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            with (
                patch.object(storage, "settings", _asgi_settings()),
                patch.object(storage, "_storage_executor", executor),
                patch.object(storage, "_storage_executor_workers", 1),
                patch.object(storage, "_asgi_semaphores", weakref.WeakKeyDictionary()),
                tenant_context(OWNER),
            ):
                self.assertEqual(await storage.run_asgi_storage_call(require_owner_hash), OWNER)
        finally:
            executor.shutdown(wait=True, cancel_futures=True)

    async def _assert_late_result_holds_capacity(self, *, cancel: bool) -> None:
        executor = ThreadPoolExecutor(max_workers=1)
        started = threading.Event()
        release_operation = threading.Event()
        cleanup_started = threading.Event()
        release_cleanup = threading.Event()
        cleanup_finished = threading.Event()
        settled = threading.Event()
        settled_calls: list[None] = []
        cleanup_calls: list[str] = []

        def slow_operation() -> str:
            started.set()
            if not release_operation.wait(1):
                raise AssertionError("test did not release storage operation")
            return "late-result"

        def cleanup(value: str) -> None:
            cleanup_calls.append(value)
            cleanup_started.set()
            try:
                if not release_cleanup.wait(1):
                    raise AssertionError("test did not release late cleanup")
            finally:
                cleanup_finished.set()

        def mark_settled() -> None:
            settled_calls.append(None)
            settled.set()

        configured = _asgi_settings(
            storage_asgi_operation_timeout_seconds=1.0 if cancel else 0.04,
        )
        try:
            with (
                patch.object(storage, "settings", configured),
                patch.object(storage, "_storage_executor", executor),
                patch.object(storage, "_storage_executor_workers", 1),
                patch.object(storage, "_asgi_semaphores", weakref.WeakKeyDictionary()),
            ):
                task = asyncio.create_task(
                    storage.run_asgi_storage_call(
                        slow_operation,
                        late_result_cleanup=cleanup,
                        on_settled=mark_settled,
                    )
                )
                await _wait_for_event(started)
                if cancel:
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                else:
                    with self.assertRaisesRegex(storage.StorageError, "timed out"):
                        await task
                self.assertFalse(settled.is_set())

                with self.assertRaisesRegex(storage.StorageError, "busy"):
                    await storage.run_asgi_storage_call(lambda: "must-not-run")

                release_operation.set()
                await _wait_for_event(cleanup_started)
                self.assertFalse(settled.is_set())
                with self.assertRaisesRegex(storage.StorageError, "busy"):
                    await storage.run_asgi_storage_call(lambda: "still-must-not-run")

                release_cleanup.set()
                await _wait_for_event(cleanup_finished)
                await _wait_for_event(settled)
                self.assertEqual(settled_calls, [None])
                self.assertEqual(
                    await storage.run_asgi_storage_call(lambda: "available"),
                    "available",
                )
                self.assertEqual(cleanup_calls, ["late-result"])
        finally:
            release_operation.set()
            release_cleanup.set()
            executor.shutdown(wait=True, cancel_futures=True)

    async def test_timeout_holds_capacity_through_one_late_cleanup(self) -> None:
        await self._assert_late_result_holds_capacity(cancel=False)

    async def test_cancellation_holds_capacity_through_one_late_cleanup(self) -> None:
        await self._assert_late_result_holds_capacity(cancel=True)

    async def test_heartbeat_keeps_timed_out_put_out_of_cleanup_claim(self) -> None:
        executor = ThreadPoolExecutor(max_workers=1)
        put_started = threading.Event()
        release_put = threading.Event()
        finalized = threading.Event()
        state_lock = threading.Lock()
        lease_window = 0.03
        state: dict[str, object] = {
            "phase": "new",
            "lease_until": 0.0,
            "refreshes": 0,
            "forced_claims": 0,
        }
        asset_id = "c" * 32
        reservation: dict[str, object] = {}

        def reserve(asset: dict[str, object], token: str) -> str:
            self.assertEqual(require_owner_hash(), OWNER)
            with state_lock:
                state["phase"] = "reserved"
                state["lease_until"] = time.monotonic() + lease_window
                reservation.update(asset)
                reservation["reservation_token"] = token
            return OWNER

        def refresh(
            _asset_id: str,
            _token: str,
            *,
            owner_hash: str | None = None,
        ) -> bool:
            self.assertEqual(owner_hash, OWNER)
            with state_lock:
                if state["phase"] != "reserved":
                    return False
                state["lease_until"] = time.monotonic() + lease_window
                state["refreshes"] = int(state["refreshes"]) + 1
            return True

        def slow_put(_path: str, _key: str, _size: int) -> None:
            put_started.set()
            if not release_put.wait(1):
                raise AssertionError("test did not release storage put")

        def commit(asset: dict[str, object], _token: str) -> None:
            with state_lock:
                state["phase"] = "active"
                reservation.update(asset)

        def claim(
            _asset_id: str,
            delete_token: str,
            *,
            force: bool = False,
            owner_hash: str | None = None,
        ) -> dict[str, object] | None:
            with state_lock:
                phase = state["phase"]
                if phase == "reserved":
                    if time.monotonic() < float(state["lease_until"]):
                        return None
                    state["phase"] = "deleting"
                elif phase == "active" and force and owner_hash == OWNER:
                    state["phase"] = "deleting"
                    state["forced_claims"] = int(state["forced_claims"]) + 1
                else:
                    return None
                return {
                    **reservation,
                    "asset_id": asset_id,
                    "owner_hash": OWNER,
                    "storage_key": f"cc/cc/{asset_id}.mp4",
                    "size_bytes": 5,
                    "delete_token": delete_token,
                    "state": "deleting",
                }

        def finalize(_asset: dict[str, object]) -> bool:
            finalized.set()
            return True

        with tempfile.TemporaryDirectory() as temp_dir:
            source = os.path.join(temp_dir, "source.mp4")
            with open(source, "wb") as handle:
                handle.write(b"media")
            configured = _asgi_settings(
                storage_local_dir=temp_dir,
                storage_temp_dir=temp_dir,
                storage_asgi_operation_timeout_seconds=0.02,
                asset_reservation_heartbeat_seconds=0.005,
            )
            try:
                with (
                    patch.object(storage, "settings", configured),
                    patch.object(storage, "_storage_executor", executor),
                    patch.object(storage, "_storage_executor_workers", 1),
                    patch.object(storage, "_asgi_semaphores", weakref.WeakKeyDictionary()),
                    patch.object(storage, "reserve_asset", side_effect=reserve),
                    patch.object(storage, "refresh_asset_reservation", side_effect=refresh),
                    patch.object(storage, "_put_reserved_file", side_effect=slow_put),
                    patch.object(storage, "commit_asset", side_effect=commit),
                    patch.object(storage, "claim_asset_deletion", side_effect=claim),
                    patch.object(storage, "delete_file"),
                    patch.object(storage, "finalize_asset_deletion", side_effect=finalize),
                    tenant_context(OWNER),
                ):
                    with self.assertRaisesRegex(storage.StorageError, "timed out"):
                        await storage.persist_asset_async(
                            source,
                            {"asset_id": asset_id},
                            "mp4",
                        )
                    await _wait_for_event(put_started)
                    await asyncio.sleep(lease_window * 2)
                    self.assertFalse(storage.delete_managed_asset(asset_id))
                    with state_lock:
                        self.assertGreater(int(state["refreshes"]), 2)
                        self.assertEqual(state["phase"], "reserved")

                    release_put.set()
                    await _wait_for_event(finalized)
                    with state_lock:
                        self.assertEqual(state["forced_claims"], 1)
            finally:
                release_put.set()
                executor.shutdown(wait=True, cancel_futures=True)


class JobStorageLifecycleTests(unittest.TestCase):
    def test_output_and_materialization_budgets_are_cumulative_and_scoped(self) -> None:
        configured = SimpleNamespace(
            job_storage_max_output_count=2,
            job_storage_max_output_bytes=5,
            job_storage_max_materialize_bytes=4,
        )
        with patch.object(storage, "settings", configured):
            with storage.job_storage_budget():
                storage._consume_job_output(2)
                storage._consume_job_output(3)
                with self.assertRaisesRegex(storage.StorageError, "count"):
                    storage._consume_job_output(0)
                storage._consume_job_materialization(2)
                storage._consume_job_materialization(2)
                with self.assertRaisesRegex(storage.StorageError, "downloads"):
                    storage._consume_job_materialization(1)

            with storage.job_storage_budget():
                storage._consume_job_output(3)
                with self.assertRaisesRegex(storage.StorageError, "bytes"):
                    storage._consume_job_output(3)

            storage._consume_job_output(10_000)
            storage._consume_job_materialization(10_000)

    def test_failed_job_purges_every_committed_output_before_budget_reset(self) -> None:
        rq_job = SimpleNamespace(id="job-id", meta={"owner_hash": OWNER}, timeout=60)
        asset_ids = ("1" * 32, "2" * 32)
        observed: list[tuple[str, tuple[str, ...]]] = []

        def fail_after_outputs() -> None:
            for asset_id in asset_ids:
                storage._record_committed_job_output(asset_id)
            raise storage.StorageError("Job output bytes exceed the configured limit")

        def observe_purge(owner_hash: str) -> bool:
            observed.append((owner_hash, storage.current_job_output_asset_ids()))
            return True

        with (
            tenant_context(OWNER),
            patch.object(jobs, "get_current_job", return_value=rq_job),
            patch.object(jobs, "refresh_job_admission"),
            patch.object(jobs, "release_job_admission") as release,
            patch.object(jobs, "_finish_job") as finish,
            patch.object(jobs, "purge_current_job_outputs", side_effect=observe_purge) as purge,
        ):
            result = jobs.execute_tenant_job(fail_after_outputs, ())

        self.assertEqual(result, {"ok": False, "error_code": "limit_exceeded"})
        self.assertEqual(observed, [(OWNER, asset_ids)])
        purge.assert_called_once_with(OWNER)
        finish.assert_called_once()
        release.assert_called_once_with("job-id", owner_hash=OWNER)
        self.assertEqual(storage.current_job_output_asset_ids(), ())

    def test_backend_delete_failure_schedules_retry_without_finalizing_control(self) -> None:
        asset = {
            "asset_id": "d" * 32,
            "delete_token": "e" * 32,
            "storage_key": "dd/dd/media.mp4",
        }
        with (
            patch.object(storage, "delete_file", side_effect=OSError("backend unavailable")),
            patch.object(storage, "schedule_asset_delete_retry") as retry,
            patch.object(storage, "finalize_asset_deletion") as finalize,
        ):
            self.assertFalse(storage._delete_claimed_asset(asset))

        retry.assert_called_once_with(asset["asset_id"], asset["delete_token"])
        finalize.assert_not_called()


class StagingLifecycleConfigurationTests(unittest.TestCase):
    def test_staging_age_default_and_bounds(self) -> None:
        self.assertEqual(config.Settings().storage_staging_max_age_seconds, 7_200)
        for value in (59, 86_401):
            with self.subTest(value=value):
                errors = _runtime_settings(storage_staging_max_age_seconds=value).runtime_errors()
                self.assertTrue(
                    any("STORAGE_STAGING_MAX_AGE_SECONDS" in error for error in errors),
                    errors,
                )

    def test_staging_age_must_exceed_ingest_and_resolved_job_timeouts(self) -> None:
        cases = (
            {
                "storage_staging_max_age_seconds": 300,
                "ingest_timeout_seconds": 300,
                "ffmpeg_timeout_seconds": 60,
            },
            {
                "storage_staging_max_age_seconds": 3_600,
                "ffmpeg_batch_timeout_seconds": 3_600,
            },
        )
        for overrides in cases:
            with self.subTest(overrides=overrides):
                errors = _runtime_settings(**overrides).runtime_errors()
                self.assertIn(
                    "STORAGE_STAGING_MAX_AGE_SECONDS must exceed the longest configured "
                    "ingest or job operation timeout",
                    errors,
                )

        errors = _runtime_settings(
            storage_staging_max_age_seconds=3_601,
            ffmpeg_timeout_seconds=3_600,
        ).runtime_errors()
        self.assertFalse(
            any("STORAGE_STAGING_MAX_AGE_SECONDS" in error for error in errors),
            errors,
        )

    def test_local_storage_roots_must_be_absolute_distinct_and_non_nested(self) -> None:
        invalid_roots = (
            ("/data/assets", "/data/assets"),
            ("/data/assets", "/data/assets/staging"),
            ("/data/staging/assets", "/data/staging"),
            ("/data/assets", "/data/assets/../assets"),
            ("relative/assets", "/data/staging"),
        )
        for local_dir, temp_dir in invalid_roots:
            with self.subTest(local_dir=local_dir, temp_dir=temp_dir):
                errors = _runtime_settings(
                    storage_local_dir=local_dir,
                    storage_temp_dir=temp_dir,
                ).runtime_errors()
                self.assertTrue(
                    any("STORAGE_LOCAL_DIR and STORAGE_TEMP_DIR" in error for error in errors),
                    errors,
                )

        errors = _runtime_settings(
            storage_local_dir="/data/assets",
            storage_temp_dir="/data/staging",
        ).runtime_errors()
        self.assertFalse(
            any("STORAGE_LOCAL_DIR and STORAGE_TEMP_DIR" in error for error in errors),
            errors,
        )


class StagingCleanupTests(unittest.TestCase):
    def test_cleanup_removes_only_stale_top_level_regular_files(self) -> None:
        now = 10_000.0
        with tempfile.TemporaryDirectory() as root:
            staging = os.path.join(root, "staging")
            outside = os.path.join(root, "outside")
            nested = os.path.join(staging, "nested")
            os.makedirs(nested)
            os.makedirs(outside)

            stale = os.path.join(staging, "stale.tmp")
            fresh = os.path.join(staging, "fresh.tmp")
            nested_stale = os.path.join(nested, "nested-stale.tmp")
            outside_target = os.path.join(outside, "target.tmp")
            symlink = os.path.join(staging, "linked.tmp")
            for path in (stale, fresh, nested_stale, outside_target):
                with open(path, "wb") as handle:
                    handle.write(b"staging")
            os.symlink(outside_target, symlink)
            os.utime(stale, (now - 101, now - 101))
            os.utime(fresh, (now - 99, now - 99))
            os.utime(nested_stale, (now - 101, now - 101))
            os.utime(outside_target, (now - 101, now - 101))

            configured = SimpleNamespace(
                storage_temp_dir=staging,
                storage_staging_max_age_seconds=100,
            )
            with patch.object(cleanup, "settings", configured):
                self.assertEqual(cleanup.cleanup_stale_staging_files(now=now), 1)

            self.assertFalse(os.path.exists(stale))
            self.assertTrue(os.path.exists(fresh))
            self.assertTrue(os.path.exists(nested_stale))
            self.assertTrue(os.path.lexists(symlink))
            self.assertTrue(os.path.exists(outside_target))

    def test_cleanup_refuses_a_symlink_staging_root(self) -> None:
        now = 10_000.0
        with tempfile.TemporaryDirectory() as root:
            actual = os.path.join(root, "actual")
            staging_link = os.path.join(root, "staging-link")
            os.makedirs(actual)
            stale = os.path.join(actual, "stale.tmp")
            with open(stale, "wb") as handle:
                handle.write(b"staging")
            os.utime(stale, (now - 101, now - 101))
            os.symlink(actual, staging_link, target_is_directory=True)

            configured = SimpleNamespace(
                storage_temp_dir=staging_link,
                storage_staging_max_age_seconds=100,
            )
            with (
                patch.object(cleanup, "settings", configured),
                patch.object(cleanup.logger, "exception") as logged,
            ):
                self.assertEqual(cleanup.cleanup_stale_staging_files(now=now), 0)

            self.assertTrue(os.path.exists(stale))
            logged.assert_called_once_with("staging_cleanup_root_unavailable")

    def test_cleanup_isolates_each_file_deletion_failure(self) -> None:
        now = 10_000.0
        with tempfile.TemporaryDirectory() as staging:
            blocked = os.path.join(staging, "blocked.tmp")
            removable = os.path.join(staging, "removable.tmp")
            for path in (blocked, removable):
                with open(path, "wb") as handle:
                    handle.write(b"staging")
                os.utime(path, (now - 101, now - 101))

            real_unlink = os.unlink

            def selective_unlink(path: str, *, dir_fd: int | None = None) -> None:
                if path == "blocked.tmp":
                    raise PermissionError("blocked for test")
                real_unlink(path, dir_fd=dir_fd)

            configured = SimpleNamespace(
                storage_temp_dir=staging,
                storage_staging_max_age_seconds=100,
            )
            with (
                patch.object(cleanup, "settings", configured),
                patch.object(cleanup.os, "unlink", side_effect=selective_unlink),
                patch.object(cleanup.logger, "exception") as logged,
            ):
                self.assertEqual(cleanup.cleanup_stale_staging_files(now=now), 1)

            self.assertTrue(os.path.exists(blocked))
            self.assertFalse(os.path.exists(removable))
            logged.assert_called_once_with("staging_cleanup_file_failed")

    def test_cleanup_once_runs_staging_cleanup(self) -> None:
        with (
            patch.object(cleanup, "cleanup_stale_staging_files") as staging_cleanup,
            patch.object(cleanup, "list_expired_assets", return_value=[]),
            patch.object(cleanup, "list_expired_jobs", return_value=[]),
        ):
            cleanup.cleanup_once()

        staging_cleanup.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
