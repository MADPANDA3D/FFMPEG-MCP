import asyncio
import base64
import contextvars
import functools
import hashlib
import hmac
import os
import re
import stat
import threading
import time
import uuid
import weakref
from collections.abc import Callable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from typing import Any, cast
from urllib.parse import urlencode

import boto3
from botocore.config import Config

from .config import settings
from .redis_store import (
    abort_asset_reservation,
    claim_asset_deletion,
    commit_asset,
    finalize_asset_deletion,
    refresh_asset_reservation,
    reserve_asset,
    schedule_asset_delete_retry,
)
from .tenant import require_owner_hash


class StorageError(RuntimeError):
    pass


_storage_executor: ThreadPoolExecutor | None = None
_storage_executor_workers: int | None = None
_storage_executor_lock = threading.Lock()
_asgi_semaphore_lock = threading.Lock()
_asgi_semaphores: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.BoundedSemaphore] = (
    weakref.WeakKeyDictionary()
)


@dataclass
class _JobStorageBudget:
    output_count: int = 0
    output_bytes: int = 0
    materialized_bytes: int = 0
    committed_asset_ids: list[str] = field(default_factory=list)


_JOB_STORAGE_BUDGET: contextvars.ContextVar[_JobStorageBudget | None] = contextvars.ContextVar(
    "job_storage_budget", default=None
)


def _storage_backend() -> str:
    backend = settings.storage_backend
    if backend not in {"local", "s3"}:
        raise StorageError("STORAGE_BACKEND must be exactly local or s3")
    return backend


@contextmanager
def job_storage_budget() -> Iterator[None]:
    token = _JOB_STORAGE_BUDGET.set(_JobStorageBudget())
    try:
        yield
    finally:
        _JOB_STORAGE_BUDGET.reset(token)


def _consume_job_output(size_bytes: int) -> None:
    budget = _JOB_STORAGE_BUDGET.get()
    if budget is None:
        return
    if budget.output_count + 1 > settings.job_storage_max_output_count:
        raise StorageError("Job output count exceeds the configured limit")
    if budget.output_bytes + size_bytes > settings.job_storage_max_output_bytes:
        raise StorageError("Job output bytes exceed the configured limit")
    budget.output_count += 1
    budget.output_bytes += size_bytes


def _consume_job_materialization(size_bytes: int) -> None:
    budget = _JOB_STORAGE_BUDGET.get()
    if budget is None:
        return
    if budget.materialized_bytes + size_bytes > settings.job_storage_max_materialize_bytes:
        raise StorageError("Job storage downloads exceed the configured limit")
    budget.materialized_bytes += size_bytes


def _record_committed_job_output(asset_id: str) -> None:
    budget = _JOB_STORAGE_BUDGET.get()
    if budget is not None:
        budget.committed_asset_ids.append(asset_id)


def current_job_output_asset_ids() -> tuple[str, ...]:
    budget = _JOB_STORAGE_BUDGET.get()
    return tuple(budget.committed_asset_ids) if budget is not None else ()


def _get_storage_executor() -> ThreadPoolExecutor:
    global _storage_executor, _storage_executor_workers
    workers = settings.storage_asgi_max_concurrency
    with _storage_executor_lock:
        if _storage_executor is None:
            _storage_executor = ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="ffmpeg-storage",
            )
            _storage_executor_workers = workers
        elif _storage_executor_workers != workers:
            raise StorageError("storage executor configuration changed after startup")
        return _storage_executor


def _get_asgi_semaphore() -> asyncio.BoundedSemaphore:
    loop = asyncio.get_running_loop()
    with _asgi_semaphore_lock:
        semaphore = _asgi_semaphores.get(loop)
        if semaphore is None:
            semaphore = asyncio.BoundedSemaphore(settings.storage_asgi_max_concurrency)
            _asgi_semaphores[loop] = semaphore
        return semaphore


async def run_asgi_storage_call[T](
    function: Callable[..., T],
    /,
    *args: Any,
    late_result_cleanup: Callable[[T], Any] | None = None,
    on_settled: Callable[[], None] | None = None,
    **kwargs: Any,
) -> T:
    """Run storage I/O and signal only after its capacity and late cleanup settle."""

    settled = False

    def settle() -> None:
        nonlocal settled
        if settled:
            return
        settled = True
        if on_settled is not None:
            with suppress(Exception):
                on_settled()

    try:
        _storage_backend()
        semaphore = _get_asgi_semaphore()
        await asyncio.wait_for(
            semaphore.acquire(),
            timeout=settings.storage_asgi_admission_timeout_seconds,
        )
    except TimeoutError as exc:
        settle()
        raise StorageError("Storage service is busy") from exc
    except BaseException:
        settle()
        raise

    loop = asyncio.get_running_loop()
    outcome: asyncio.Future[T] = loop.create_future()
    state = {"waiting": True, "released": False, "cleanup_started": False}
    context = contextvars.copy_context()
    call = functools.partial(function, *args, **kwargs)
    try:
        concurrent = _get_storage_executor().submit(context.run, call)
    except Exception:
        semaphore.release()
        settle()
        raise StorageError("Storage service is unavailable") from None

    def release() -> None:
        if not state["released"]:
            state["released"] = True
            semaphore.release()
            settle()

    def finish_late(value: T | None, error: BaseException | None) -> None:
        if state["cleanup_started"]:
            return
        state["cleanup_started"] = True
        if error is not None or late_result_cleanup is None:
            release()
            return
        try:
            cleanup_future = _get_storage_executor().submit(
                late_result_cleanup,
                cast(T, value),
            )
        except Exception:
            release()
            return

        def release_after_cleanup(future: Future[Any]) -> None:
            with suppress(BaseException):
                future.result()
            with suppress(RuntimeError):
                loop.call_soon_threadsafe(release)

        cleanup_future.add_done_callback(release_after_cleanup)

    def completed(future: Future[T]) -> None:
        try:
            value = future.result()
            error: BaseException | None = None
        except BaseException as exc:  # consume all late executor failures
            value = None  # type: ignore[assignment]
            error = exc

        def deliver() -> None:
            if not state["waiting"] or outcome.done():
                finish_late(value, error)
                return
            if error is None:
                outcome.set_result(cast(T, value))
            else:
                outcome.set_exception(error)

        loop.call_soon_threadsafe(deliver)

    concurrent.add_done_callback(completed)
    try:
        result = await asyncio.wait_for(
            asyncio.shield(outcome),
            timeout=settings.storage_asgi_operation_timeout_seconds,
        )
    except TimeoutError as exc:
        state["waiting"] = False
        if outcome.done():
            try:
                value = outcome.result()
                error = None
            except BaseException as late_error:
                value = None
                error = late_error
            finish_late(value, error)
        raise StorageError("Storage operation timed out") from exc
    except asyncio.CancelledError:
        state["waiting"] = False
        if outcome.done():
            try:
                value = outcome.result()
                error = None
            except BaseException as late_error:
                value = None
                error = late_error
            finish_late(value, error)
        raise
    except BaseException:
        state["waiting"] = False
        release()
        raise
    else:
        state["waiting"] = False
        release()
        return result


def _valid_signing_secret() -> bool:
    secret = settings.download_signing_secret
    return (
        len(secret) >= 32
        and secret == secret.strip()
        and not any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in secret
        )
    )


def _normalize_storage_key(storage_key: str) -> str:
    if not isinstance(storage_key, str) or not storage_key:
        raise StorageError("Storage key is required")
    if "\\" in storage_key or any(ord(char) < 32 or ord(char) == 127 for char in storage_key):
        raise StorageError("Storage key contains invalid characters")
    if os.path.isabs(storage_key):
        raise StorageError("Absolute storage keys are not allowed")
    parts = storage_key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise StorageError("Storage key must be a normalized relative path")
    return "/".join(parts)


def _assert_no_symlink_components(root: str, candidate: str) -> None:
    relative = os.path.relpath(candidate, root)
    current = root
    for part in relative.split(os.sep):
        current = os.path.join(current, part)
        try:
            mode = os.lstat(current).st_mode
        except FileNotFoundError:
            break
        except OSError as exc:
            raise StorageError("Unable to validate local storage path") from exc
        if stat.S_ISLNK(mode):
            raise StorageError("Symlinks are not allowed in local storage paths")


def _ensure_local_dirs() -> None:
    os.makedirs(settings.storage_local_dir, exist_ok=True)
    os.makedirs(settings.storage_temp_dir, exist_ok=True)


def build_storage_key(asset_id: str, ext: str) -> str:
    if not isinstance(asset_id, str) or not re.fullmatch(r"[a-f0-9]{32}", asset_id):
        raise StorageError("Asset id is invalid")
    normalized_ext = ext.removeprefix(".")
    if normalized_ext and not re.fullmatch(r"[A-Za-z0-9]{1,10}", normalized_ext):
        raise StorageError("Asset extension is invalid")
    clean_ext = f".{normalized_ext.lower()}" if normalized_ext else ""
    prefix = os.path.join(asset_id[:2], asset_id[2:4])
    filename = f"{asset_id}{clean_ext}"
    return os.path.join(prefix, filename)


def local_path_from_key(storage_key: str) -> str:
    normalized_key = _normalize_storage_key(storage_key)
    root = os.path.realpath(settings.storage_local_dir)
    candidate = os.path.abspath(os.path.join(root, *normalized_key.split("/")))
    try:
        if os.path.commonpath([root, candidate]) != root:
            raise StorageError("Storage key escapes the local storage root")
    except ValueError as exc:
        raise StorageError("Storage key escapes the local storage root") from exc
    _assert_no_symlink_components(root, candidate)
    resolved_candidate = os.path.realpath(candidate)
    try:
        if os.path.commonpath([root, resolved_candidate]) != root:
            raise StorageError("Storage path resolves outside the local storage root")
    except ValueError as exc:
        raise StorageError("Storage path resolves outside the local storage root") from exc
    return candidate


def _ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _build_local_signed_url(asset_id: str, expires_at: int) -> str:
    if not settings.public_base_url:
        raise StorageError("PUBLIC_BASE_URL is required for local download URLs")
    if not _valid_signing_secret():
        raise StorageError(
            "DOWNLOAD_SIGNING_SECRET must contain at least 32 characters for local download URLs"
        )
    payload = f"{asset_id}:{expires_at}".encode()
    signature = hmac.new(
        settings.download_signing_secret.encode("utf-8"), payload, hashlib.sha256
    ).digest()
    sig = base64.urlsafe_b64encode(signature).decode("utf-8").rstrip("=")
    query = urlencode({"exp": str(expires_at), "sig": sig})
    return f"{settings.public_base_url.rstrip('/')}/download/{asset_id}?{query}"


def verify_local_signature(asset_id: str, expires_at: int, signature: str) -> bool:
    if not _valid_signing_secret():
        return False
    payload = f"{asset_id}:{expires_at}".encode()
    expected = hmac.new(
        settings.download_signing_secret.encode("utf-8"), payload, hashlib.sha256
    ).digest()
    expected_sig = base64.urlsafe_b64encode(expected).decode("utf-8").rstrip("=")
    return hmac.compare_digest(expected_sig, signature)


@functools.lru_cache(maxsize=1)
def get_storage_client() -> Any | None:
    if _storage_backend() != "s3":
        return None
    if not settings.s3_bucket:
        raise StorageError("S3_BUCKET is required for S3 storage")
    session = boto3.session.Session(
        aws_access_key_id=settings.s3_access_key or None,
        aws_secret_access_key=settings.s3_secret_key or None,
        region_name=settings.s3_region or None,
    )
    return session.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url or None,
        config=Config(
            connect_timeout=settings.s3_connect_timeout_seconds,
            read_timeout=settings.s3_read_timeout_seconds,
            max_pool_connections=settings.storage_asgi_max_concurrency,
            retries={"mode": "standard", "total_max_attempts": 1},
        ),
    )


class _ReservationHeartbeat:
    def __init__(self, asset_id: str, reservation_token: str, owner_hash: str):
        self.asset_id = asset_id
        self.reservation_token = reservation_token
        self.owner_hash = owner_hash
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _refresh(self) -> bool:
        return refresh_asset_reservation(
            self.asset_id,
            self.reservation_token,
            owner_hash=self.owner_hash,
        )

    def __enter__(self) -> "_ReservationHeartbeat":
        if not self._refresh():
            raise StorageError("Asset reservation is unavailable")

        def heartbeat() -> None:
            while not self._stop.wait(settings.asset_reservation_heartbeat_seconds):
                self._refresh()

        self._thread = threading.Thread(
            target=heartbeat,
            name=f"asset-heartbeat-{self.asset_id[:8]}",
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=settings.asset_reservation_heartbeat_seconds + 1)
        if not self._refresh() and exc_type is None:
            raise StorageError("Asset reservation is unavailable")


class _DeadlineReader:
    def __init__(self, handle: Any, deadline: float):
        self._handle = handle
        self._deadline = deadline

    def read(self, size: int = -1) -> bytes:
        if time.monotonic() >= self._deadline:
            raise StorageError("Storage operation timed out")
        return self._handle.read(size)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handle, name)


def _put_reserved_file(
    temp_path: str,
    storage_key: str,
    expected_size: int,
) -> None:
    _ensure_local_dirs()
    if not os.path.isfile(temp_path) or os.path.islink(temp_path):
        raise StorageError("Source must be a regular non-symlink file")
    if os.path.getsize(temp_path) != expected_size:
        raise StorageError("Source size changed before storage")
    if _storage_backend() == "s3":
        storage_key = _normalize_storage_key(storage_key)
        client = get_storage_client()
        if client is None:
            raise StorageError("S3 client not available")
        with open(temp_path, "rb") as handle:
            client.put_object(
                Bucket=settings.s3_bucket,
                Key=storage_key,
                Body=_DeadlineReader(
                    handle,
                    time.monotonic() + settings.storage_asgi_operation_timeout_seconds,
                ),
                ContentLength=expected_size,
            )
        os.remove(temp_path)
        return

    dest_path = local_path_from_key(storage_key)
    _ensure_parent_dir(dest_path)
    dest_path = local_path_from_key(storage_key)
    os.replace(temp_path, dest_path)
    if os.path.getsize(dest_path) != expected_size:
        raise StorageError("Stored object size does not match its reservation")


def persist_asset(
    temp_path: str,
    asset: dict[str, Any],
    extension: str,
    *,
    count_as_job_output: bool = False,
) -> dict[str, Any]:
    """Reserve quota, write the object, then atomically publish its metadata."""

    if not os.path.isfile(temp_path) or os.path.islink(temp_path):
        raise StorageError("Source must be a regular non-symlink file")
    size_bytes = os.path.getsize(temp_path)
    if count_as_job_output:
        _consume_job_output(size_bytes)
    asset_id = asset.get("asset_id")
    if not isinstance(asset_id, str):
        raise StorageError("Asset id is required")
    storage_key = build_storage_key(asset_id, extension)
    backend = _storage_backend()
    storage_uri = (
        f"s3://{settings.s3_bucket}/{storage_key}" if backend == "s3" else f"local://{storage_key}"
    )
    complete_asset = {
        **asset,
        "size_bytes": size_bytes,
        "storage_key": storage_key,
        "storage_uri": storage_uri,
    }
    reservation_token = uuid.uuid4().hex
    owner = reserve_asset(complete_asset, reservation_token)
    try:
        with _ReservationHeartbeat(asset_id, reservation_token, owner):
            _put_reserved_file(temp_path, storage_key, size_bytes)
    except Exception:
        with suppress(Exception):
            delete_file(storage_key)
        delete_token = uuid.uuid4().hex
        claimed = abort_asset_reservation(
            asset_id,
            reservation_token,
            delete_token,
            owner_hash=owner,
        )
        if claimed is not None:
            _delete_claimed_asset(claimed)
        raise
    try:
        commit_asset(complete_asset, reservation_token)
    except Exception:
        delete_token = uuid.uuid4().hex
        claimed = abort_asset_reservation(
            asset_id,
            reservation_token,
            delete_token,
            owner_hash=owner,
        )
        if claimed is not None:
            _delete_claimed_asset(claimed)
        raise
    if count_as_job_output:
        _record_committed_job_output(asset_id)
    return complete_asset


async def persist_asset_async(
    temp_path: str,
    asset: dict[str, Any],
    extension: str,
    *,
    on_settled: Callable[[], None] | None = None,
) -> dict[str, Any]:
    owner = require_owner_hash()

    def purge_late_result(result: dict[str, Any]) -> None:
        asset_id = result.get("asset_id")
        if isinstance(asset_id, str):
            delete_managed_asset(asset_id, force=True, owner_hash=owner)

    return await run_asgi_storage_call(
        persist_asset,
        temp_path,
        asset,
        extension,
        late_result_cleanup=purge_late_result,
        on_settled=on_settled,
    )


def _materialization_limit(per_object_limit: int) -> int:
    budget = _JOB_STORAGE_BUDGET.get()
    if budget is None:
        return per_object_limit
    remaining = settings.job_storage_max_materialize_bytes - budget.materialized_bytes
    if remaining <= 0:
        raise StorageError("Job storage downloads exceed the configured limit")
    return min(per_object_limit, remaining)


def download_to_temp(storage_key: str) -> str:
    storage_key = _normalize_storage_key(storage_key)
    if _storage_backend() != "s3":
        return local_path_from_key(storage_key)
    client = get_storage_client()
    if client is None:
        raise StorageError("S3 client not available")
    _ensure_local_dirs()
    import tempfile

    max_bytes = _materialization_limit(max(settings.max_ingest_bytes, settings.max_output_bytes))
    response = client.get_object(Bucket=settings.s3_bucket, Key=storage_key)
    body = response.get("Body")
    if body is None:
        raise StorageError("S3 download returned no body")
    try:
        content_length = int(response.get("ContentLength", 0))
    except (TypeError, ValueError):
        content_length = 0
    if content_length > max_bytes:
        body.close()
        raise StorageError("S3 object exceeds the configured size limit")

    descriptor = -1
    temp_path = ""
    written = 0
    deadline = time.monotonic() + settings.storage_asgi_operation_timeout_seconds
    try:
        descriptor, temp_path = tempfile.mkstemp(
            dir=settings.storage_temp_dir,
            prefix="s3_",
        )
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            while True:
                if time.monotonic() >= deadline:
                    raise StorageError("Storage operation timed out")
                chunk = body.read(64 * 1024)
                if not chunk:
                    break
                written += len(chunk)
                if written > max_bytes:
                    raise StorageError("S3 object exceeds the configured size limit")
                handle.write(chunk)
        if content_length > 0 and written != content_length:
            raise StorageError("S3 object length did not match its response metadata")
        _consume_job_materialization(written)
        return temp_path
    except Exception as exc:
        if descriptor >= 0:
            with suppress(OSError):
                os.close(descriptor)
        if temp_path:
            with suppress(FileNotFoundError):
                os.unlink(temp_path)
        if isinstance(exc, StorageError):
            raise
        raise StorageError("S3 download failed") from exc
    finally:
        body.close()


def _unlink_temp_result(path: str) -> None:
    with suppress(FileNotFoundError):
        os.unlink(path)


async def download_to_temp_async(storage_key: str) -> str:
    cleanup = _unlink_temp_result if _storage_backend() == "s3" else None
    return await run_asgi_storage_call(
        download_to_temp,
        storage_key,
        late_result_cleanup=cleanup,
    )


def delete_file(storage_key: str) -> None:
    storage_key = _normalize_storage_key(storage_key)
    if _storage_backend() == "s3":
        client = get_storage_client()
        if client is None:
            raise StorageError("S3 client not available")
        client.delete_object(Bucket=settings.s3_bucket, Key=storage_key)
        return
    path = local_path_from_key(storage_key)
    if os.path.lexists(path):
        if os.path.islink(path):
            raise StorageError("Refusing to delete a symlinked storage path")
        if not os.path.isfile(path):
            raise StorageError("Refusing to delete a non-file storage path")
        os.remove(path)


def _delete_claimed_asset(asset: dict[str, Any]) -> bool:
    asset_id = asset.get("asset_id")
    delete_token = asset.get("delete_token")
    storage_key = asset.get("storage_key")
    if (
        not isinstance(asset_id, str)
        or not isinstance(delete_token, str)
        or not isinstance(storage_key, str)
    ):
        return False
    try:
        delete_file(storage_key)
    except Exception:
        schedule_asset_delete_retry(asset_id, delete_token)
        return False
    return finalize_asset_deletion(asset)


def delete_managed_asset(
    asset_id: str,
    *,
    force: bool = False,
    owner_hash: str | None = None,
) -> bool:
    delete_token = uuid.uuid4().hex
    claimed = claim_asset_deletion(
        asset_id,
        delete_token,
        force=force,
        owner_hash=owner_hash,
    )
    return claimed is not None and _delete_claimed_asset(claimed)


def purge_current_job_outputs(owner_hash: str) -> bool:
    success = True
    for asset_id in reversed(dict.fromkeys(current_job_output_asset_ids())):
        try:
            deleted = delete_managed_asset(asset_id, force=True, owner_hash=owner_hash)
        except Exception:
            deleted = False
        success = deleted and success
    return success


def generate_download_url(
    asset_id: str,
    storage_key: str,
    asset_expires_at: int,
) -> tuple[str, int]:
    if not isinstance(asset_id, str) or not re.fullmatch(r"[a-f0-9]{32}", asset_id):
        raise StorageError("Asset id is invalid")
    storage_key = _normalize_storage_key(storage_key)
    if isinstance(asset_expires_at, bool) or not isinstance(asset_expires_at, int):
        raise StorageError("Asset expiry is invalid")
    now = int(time.time())
    ttl_seconds = min(settings.download_url_ttl_seconds, asset_expires_at - now)
    if ttl_seconds <= 0:
        raise StorageError("Asset has expired")
    expires_at = now + ttl_seconds
    if _storage_backend() == "s3":
        client = get_storage_client()
        if client is None:
            raise StorageError("S3 client not available")
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": settings.s3_bucket, "Key": storage_key},
            ExpiresIn=ttl_seconds,
        )
        return url, expires_at
    url = _build_local_signed_url(asset_id, expires_at)
    return url, expires_at


async def generate_download_url_async(
    asset_id: str,
    storage_key: str,
    asset_expires_at: int,
) -> tuple[str, int]:
    return await run_asgi_storage_call(
        generate_download_url,
        asset_id,
        storage_key,
        asset_expires_at,
    )
