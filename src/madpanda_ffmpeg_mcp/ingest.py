import asyncio
import hashlib
import ipaddress
import logging
import math
import os
import re
import socket
import tempfile
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import filetype
import httpx

from .config import settings
from .ffprobe_utils import run_ffprobe
from .media_limits import MediaLimitError, kind_from_mime_type, validate_media_probe
from .redis_store import (
    AssetQuotaError,
    AssetStateError,
    IngestStagingAdmissionError,
    IngestStagingReservation,
    refresh_ingest_staging,
    release_ingest_staging,
    reserve_ingest_staging,
)
from .storage import StorageError, persist_asset_async
from .utils import sanitize_filename, utc_now_iso, utc_now_ts


class IngestError(RuntimeError):
    pass


logger = logging.getLogger(__name__)
_REDIRECT_STATUSES = {301, 302, 303, 307, 308}
_MAX_REDIRECT_LIMIT = 10
_MAX_INGEST_TIMEOUT_SECONDS = 3600
_MAX_DNS_TIMEOUT_SECONDS = 10


@dataclass
class _RemoteIngestStagingLease:
    reservation: IngestStagingReservation
    confirmed_lease_until: int
    _retained_until: asyncio.Event | None = field(default=None, init=False, repr=False)

    def confirm(self, lease_until: int) -> None:
        self.confirmed_lease_until = lease_until

    def ensure_current(self) -> None:
        if int(time.time()) >= self.confirmed_lease_until:
            raise IngestError("Remote-ingest staging admission expired")

    def retain_until_settled(self, settled: asyncio.Event) -> None:
        if (
            self._retained_until is not None
            and not self._retained_until.is_set()
            and self._retained_until is not settled
        ):
            raise RuntimeError("remote-ingest staging settlement is already registered")
        self._retained_until = settled

    def unsettled_retention(self) -> asyncio.Event | None:
        if self._retained_until is None or self._retained_until.is_set():
            return None
        return self._retained_until


_STAGING_FINALIZERS: set[asyncio.Task[None]] = set()
_STAGING_OPERATIONS: set[asyncio.Task[Any]] = set()


async def _await_task_uninterruptibly[T](task: asyncio.Task[T]) -> T:
    """Finish one shielded reconciliation while preserving caller cancellation."""

    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return task.result()
            continue


def _track_staging_operation(task: asyncio.Task[Any]) -> asyncio.Event:
    """Keep a detached staging reader alive until its thread fully settles."""

    settled = asyncio.Event()
    _STAGING_OPERATIONS.add(task)

    def completed(completed_task: asyncio.Task[Any]) -> None:
        _STAGING_OPERATIONS.discard(completed_task)
        if not completed_task.cancelled():
            with suppress(Exception):
                completed_task.exception()
        settled.set()

    task.add_done_callback(completed)
    return settled


async def _release_staging_lease(reservation: IngestStagingReservation) -> None:
    release_task = asyncio.create_task(asyncio.to_thread(release_ingest_staging, reservation))
    try:
        released = await asyncio.shield(release_task)
    except asyncio.CancelledError:
        released = await _await_task_uninterruptibly(release_task)
        if not released:
            logger.warning("Remote-ingest staging lease release was not confirmed")
        raise
    except Exception:
        logger.warning("Remote-ingest staging lease release failed", exc_info=True)
        return
    if not released:
        logger.warning("Remote-ingest staging lease release was not confirmed")


async def _reconcile_staging_release_after_cancellation(
    reservation: IngestStagingReservation,
) -> None:
    """Finish an ambiguous acquisition release, then let the original cancel win."""

    release_task = asyncio.create_task(asyncio.to_thread(release_ingest_staging, reservation))
    try:
        released = await _await_task_uninterruptibly(release_task)
    except BaseException:
        logger.warning("Remote-ingest staging lease release failed", exc_info=True)
        return
    if not released:
        logger.warning("Remote-ingest staging lease release was not confirmed")


async def _stop_heartbeat_and_release(
    reservation: IngestStagingReservation,
    stop_heartbeat: asyncio.Event,
    heartbeat_task: asyncio.Task[None],
) -> None:
    stop_heartbeat.set()
    try:
        await heartbeat_task
    except Exception:
        logger.warning("Remote-ingest staging heartbeat failed", exc_info=True)
    await _release_staging_lease(reservation)


async def _finalize_retained_staging_lease(
    reservation: IngestStagingReservation,
    settled: asyncio.Event,
    stop_heartbeat: asyncio.Event,
    heartbeat_task: asyncio.Task[None],
) -> None:
    try:
        await settled.wait()
    finally:
        await _stop_heartbeat_and_release(
            reservation,
            stop_heartbeat,
            heartbeat_task,
        )


def _track_staging_finalizer(task: asyncio.Task[None]) -> None:
    _STAGING_FINALIZERS.add(task)

    def completed(completed_task: asyncio.Task[None]) -> None:
        _STAGING_FINALIZERS.discard(completed_task)
        if completed_task.cancelled():
            return
        try:
            error = completed_task.exception()
        except asyncio.CancelledError:
            return
        if error is not None:
            logger.warning("Retained remote-ingest staging finalizer failed", exc_info=error)

    task.add_done_callback(completed)


@asynccontextmanager
async def _remote_ingest_staging_admission() -> AsyncIterator[_RemoteIngestStagingLease]:
    acquisition_task = asyncio.create_task(
        asyncio.to_thread(reserve_ingest_staging),
        name="ingest-staging-acquisition",
    )
    try:
        reservation = await asyncio.shield(acquisition_task)
    except asyncio.CancelledError as cancellation:
        try:
            reservation = await _await_task_uninterruptibly(acquisition_task)
        except BaseException:
            raise cancellation from None
        await _reconcile_staging_release_after_cancellation(reservation)
        raise cancellation
    except IngestStagingAdmissionError as exc:
        raise IngestError(str(exc)) from exc
    except RuntimeError:
        raise IngestError("remote-ingest staging admission unavailable") from None

    lease = _RemoteIngestStagingLease(
        reservation=reservation,
        confirmed_lease_until=reservation.lease_until,
    )
    stop_heartbeat = asyncio.Event()

    async def heartbeat() -> None:
        while True:
            try:
                await asyncio.wait_for(
                    stop_heartbeat.wait(),
                    timeout=settings.ingest_staging_heartbeat_seconds,
                )
                return
            except TimeoutError:
                try:
                    lease_until = await asyncio.to_thread(
                        refresh_ingest_staging,
                        reservation,
                    )
                except Exception:
                    logger.warning("Remote-ingest staging lease refresh failed", exc_info=True)
                    continue
                if lease_until is None:
                    logger.warning("Remote-ingest staging lease refresh was not confirmed")
                    continue
                lease.confirm(lease_until)

    heartbeat_task = asyncio.create_task(
        heartbeat(),
        name=f"ingest-staging-heartbeat-{reservation.reservation_id}",
    )
    try:
        lease.ensure_current()
        yield lease
        lease.ensure_current()
    finally:
        retained_until = lease.unsettled_retention()
        if retained_until is None:
            await _stop_heartbeat_and_release(
                reservation,
                stop_heartbeat,
                heartbeat_task,
            )
        else:
            finalizer = asyncio.create_task(
                _finalize_retained_staging_lease(
                    reservation,
                    retained_until,
                    stop_heartbeat,
                    heartbeat_task,
                ),
                name=f"ingest-staging-finalizer-{reservation.reservation_id}",
            )
            _track_staging_finalizer(finalizer)


def _normalized_host(host: str) -> str:
    try:
        return host.strip().rstrip(".").encode("idna").decode("ascii").lower()
    except (UnicodeError, AttributeError):
        return ""


def _is_allowed_domain(host: str) -> bool:
    host = _normalized_host(host)
    if not host:
        return False
    allowed = {_normalized_host(domain) for domain in settings.allowed_domains}
    allowed.discard("")
    return host in allowed


def _is_allowed_content_type(content_type: str) -> bool:
    if not settings.allowed_content_types:
        return True
    if not content_type:
        return True
    mime = content_type.split(";", 1)[0].strip().lower()
    for allowed in settings.allowed_content_types:
        allowed = allowed.lower().strip()
        if allowed.endswith("/*"):
            if mime.startswith(allowed[:-1]):
                return True
        elif allowed.endswith("/"):
            if mime.startswith(allowed):
                return True
        elif mime == allowed:
            return True
    return False


def _allowed_domains_message() -> str:
    if not settings.allowed_domains:
        return "none configured"
    return ", ".join(settings.allowed_domains)


def _allowed_content_types_message() -> str:
    if not settings.allowed_content_types:
        return "any"
    return ", ".join(settings.allowed_content_types)


def _extract_drive_id(url: str) -> str | None:
    parsed = urlparse(url)
    hostname = _normalized_host(parsed.hostname or "")
    if hostname not in {"drive.google.com", "docs.google.com"}:
        return None
    if parsed.path.startswith("/file/d/"):
        parts = parsed.path.split("/")
        if len(parts) >= 4:
            return parts[3]
    query = parse_qs(parsed.query)
    if "id" in query:
        return query["id"][0]
    return None


def normalize_ingest_url(url: str) -> tuple[str, str]:
    if not isinstance(url, str) or not url.strip():
        raise IngestError("URL is required")
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme != "https":
        raise IngestError("HTTPS is required for media ingest")
    if parsed.username is not None or parsed.password is not None:
        raise IngestError("URLs containing credentials are not allowed")
    host = _normalized_host(parsed.hostname or "")
    if not host:
        raise IngestError("URL host is required")
    try:
        _ = parsed.port
    except ValueError as exc:
        raise IngestError("URL port is invalid") from exc
    if not _is_allowed_domain(host):
        allowed = _allowed_domains_message()
        raise IngestError(f"URL host is not allowed (host: {host}). Allowed hosts: {allowed}")
    drive_id = _extract_drive_id(url)
    if drive_id:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", drive_id):
            raise IngestError("Google Drive file id is invalid")
        direct = f"https://drive.google.com/uc?{urlencode({'export': 'download', 'id': drive_id})}"
        return direct, "drive"
    return url, "url"


def _ingest_timeout_seconds() -> int:
    return min(
        max(int(settings.ingest_timeout_seconds), 1),
        _MAX_INGEST_TIMEOUT_SECONDS,
    )


def _remaining_seconds(start_ts: float) -> float:
    remaining = _ingest_timeout_seconds() - (time.monotonic() - start_ts)
    if remaining <= 0:
        raise IngestError("Ingest timed out")
    return remaining


def _max_redirects() -> int:
    return min(max(int(settings.ingest_max_redirects), 0), _MAX_REDIRECT_LIMIT)


def _validate_public_address(address: str) -> None:
    try:
        ip = ipaddress.ip_address(address.split("%", 1)[0])
    except ValueError as exc:
        raise IngestError("URL host resolved to an invalid address") from exc
    if (
        not ip.is_global
        or ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    ):
        raise IngestError("URL host resolves to a non-public address")


async def _resolve_host_addresses(host: str, port: int, timeout: float) -> set[str]:
    try:
        records = await asyncio.wait_for(
            asyncio.to_thread(
                socket.getaddrinfo,
                host,
                port,
                type=socket.SOCK_STREAM,
            ),
            timeout=max(min(timeout, _MAX_DNS_TIMEOUT_SECONDS), 0.1),
        )
    except (TimeoutError, OSError, socket.gaierror) as exc:
        raise IngestError("URL host could not be resolved safely") from exc
    addresses = {
        address
        for record in records
        if record and record[4] and isinstance((address := record[4][0]), str)
    }
    if not addresses:
        raise IngestError("URL host did not resolve to an address")
    return addresses


async def _validate_request_target(url: str, start_ts: float) -> None:
    normalized, _ = normalize_ingest_url(url)
    parsed = urlparse(normalized)
    host = _normalized_host(parsed.hostname or "")
    port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
    try:
        _validate_public_address(host)
        addresses = {host}
    except IngestError:
        try:
            ipaddress.ip_address(host.split("%", 1)[0])
        except ValueError:
            addresses = await _resolve_host_addresses(
                host,
                port,
                _remaining_seconds(start_ts),
            )
        else:
            raise
    for address in addresses:
        _validate_public_address(address)


async def _request_with_safe_redirects(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    start_ts: float,
    *,
    headers: dict[str, str] | None = None,
    stream: bool = True,
    staging_lease: _RemoteIngestStagingLease | None = None,
) -> httpx.Response:
    current_url = url
    redirects_followed = 0
    while True:
        if staging_lease is not None:
            staging_lease.ensure_current()
        await _validate_request_target(current_url, start_ts)
        if staging_lease is not None:
            staging_lease.ensure_current()
        request = client.build_request(method, current_url, headers=headers)
        try:
            response = await asyncio.wait_for(
                client.send(request, stream=stream, follow_redirects=False),
                timeout=_remaining_seconds(start_ts),
            )
        except TimeoutError as exc:
            raise IngestError("Ingest timed out") from exc
        if staging_lease is not None:
            staging_lease.ensure_current()
        if response.status_code not in _REDIRECT_STATUSES:
            return response

        location = response.headers.get("location")
        await response.aclose()
        if not location:
            raise IngestError("Redirect response is missing a location")
        if redirects_followed >= _max_redirects():
            raise IngestError("Media URL exceeded the redirect limit")
        current_url = urljoin(str(response.url), location)
        redirects_followed += 1


def _filename_from_headers(headers: httpx.Headers) -> str | None:
    content_disp = headers.get("content-disposition")
    if not content_disp:
        return None
    match = re.search(r"filename\*=UTF-8''(?P<name>[^;]+)", content_disp)
    if match:
        return match.group("name")
    match = re.search(r"filename=\"?(?P<name>[^\";]+)\"?", content_disp)
    if match:
        return match.group("name")
    return None


def _validate_magic(path: str) -> tuple[str, str]:
    kind = filetype.guess(path)
    if not kind:
        raise IngestError("Unable to detect file type")
    mime = kind.mime
    if not (
        mime.startswith("video/")
        or mime.startswith("audio/")
        or (settings.allow_image_ingest and mime.startswith("image/"))
    ):
        raise IngestError("Unsupported media type")
    return mime, kind.extension


async def _run_ingest_ffprobe(
    path: str,
    staging_lease: _RemoteIngestStagingLease,
) -> dict[str, Any]:
    """Probe without letting request cancellation orphan live staging usage."""

    probe_task = asyncio.create_task(
        asyncio.to_thread(run_ffprobe, path),
        name=f"ingest-ffprobe-{staging_lease.reservation.reservation_id}",
    )
    probe_settled = _track_staging_operation(probe_task)
    staging_lease.retain_until_settled(probe_settled)
    try:
        result = await asyncio.shield(probe_task)
    except Exception as exc:
        raise IngestError("Unable to probe media safely") from exc
    finally:
        if probe_task.done():
            probe_settled.set()
    return result


def _validate_media_duration(mime_type: str, probe_data: dict[str, Any]) -> None:
    if mime_type.startswith("image/"):
        return
    value = probe_data.get("duration_sec")
    if value is None or isinstance(value, bool):
        raise IngestError("Media duration is unavailable or invalid")
    try:
        duration = float(value)
    except (TypeError, ValueError):
        raise IngestError("Media duration is unavailable or invalid") from None
    if not math.isfinite(duration) or duration <= 0:
        raise IngestError("Media duration is unavailable or invalid")
    if duration > settings.max_duration_seconds:
        raise IngestError("Media exceeds max duration")


def _check_timeout(start_ts: float) -> None:
    _remaining_seconds(start_ts)


def _parse_content_length(headers: httpx.Headers | None) -> int | None:
    if not headers:
        return None
    value = headers.get("content-length")
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _ensure_allowed_content_type(headers: httpx.Headers | None) -> None:
    if not headers:
        return
    content_type = headers.get("content-type", "")
    if not _is_allowed_content_type(content_type):
        allowed = _allowed_content_types_message()
        raise IngestError(f"Content-Type is not allowed ({content_type}). Allowed types: {allowed}")


async def _download_streaming(
    client: httpx.AsyncClient,
    url: str,
    temp_dir: str,
    start_ts: float,
    hasher: "hashlib._Hash",
    staging_lease: _RemoteIngestStagingLease,
) -> tuple[str, int, str | None, bytes]:
    temp_path = None
    size_bytes = 0
    header_filename = None
    first_bytes = b""
    magic_checked = False

    staging_lease.ensure_current()
    with tempfile.NamedTemporaryFile(dir=temp_dir, delete=False) as handle:
        temp_path = handle.name

    try:
        response = await _request_with_safe_redirects(
            client,
            "GET",
            url,
            start_ts,
            stream=True,
            staging_lease=staging_lease,
        )
        try:
            response.raise_for_status()
            _ensure_allowed_content_type(response.headers)
            content_length = _parse_content_length(response.headers)
            if content_length is not None and content_length > settings.max_ingest_bytes:
                raise IngestError("File exceeds max ingest size")
            header_filename = _filename_from_headers(response.headers)
            with open(temp_path, "ab") as handle:
                chunk_size = min(max(int(settings.ingest_stream_chunk_bytes), 1), 1024 * 1024)
                async for chunk in response.aiter_bytes(chunk_size):
                    staging_lease.ensure_current()
                    _check_timeout(start_ts)
                    if not chunk:
                        continue
                    size_bytes += len(chunk)
                    if size_bytes > settings.max_ingest_bytes:
                        raise IngestError("File exceeds max ingest size")
                    if not magic_checked:
                        first_bytes += chunk
                        if len(first_bytes) >= 2048:
                            kind = filetype.guess(first_bytes)
                            if kind and not (
                                kind.mime.startswith("video/")
                                or kind.mime.startswith("audio/")
                                or (settings.allow_image_ingest and kind.mime.startswith("image/"))
                            ):
                                raise IngestError("Unsupported media type")
                            if kind or len(first_bytes) >= 65536:
                                magic_checked = True
                    hasher.update(chunk)
                    handle.write(chunk)
                    staging_lease.ensure_current()
        finally:
            await response.aclose()
    except Exception:
        if temp_path and os.path.exists(temp_path):
            with suppress(OSError):
                os.remove(temp_path)
        raise

    return temp_path, size_bytes, header_filename, first_bytes


async def ingest_from_url(
    url: str,
    filename_hint: str | None,
    ttl_hours: int | None,
) -> dict[str, Any]:
    start_ts = time.monotonic()
    normalized_url, source = normalize_ingest_url(url)
    requested_ttl = settings.asset_ttl_hours if ttl_hours is None else ttl_hours
    if isinstance(requested_ttl, bool) or not isinstance(requested_ttl, int):
        raise IngestError("ttl_hours must be an integer")
    if requested_ttl <= 0 or requested_ttl > settings.max_asset_ttl_hours:
        raise IngestError(f"ttl_hours must be between 1 and {settings.max_asset_ttl_hours}")
    ttl_seconds = requested_ttl * 3600
    async with _remote_ingest_staging_admission() as staging_lease:
        temp_dir = settings.storage_temp_dir
        os.makedirs(temp_dir, exist_ok=True)
        temp_path = None
        hasher = hashlib.sha256()
        size_bytes = 0
        filename = None

        try:
            staging_lease.ensure_current()
            async with httpx.AsyncClient(
                follow_redirects=False,
                timeout=httpx.Timeout(
                    _ingest_timeout_seconds(),
                    connect=min(_ingest_timeout_seconds(), _MAX_DNS_TIMEOUT_SECONDS),
                ),
                trust_env=False,
            ) as client:
                head_headers = None
                try:
                    head_response = await _request_with_safe_redirects(
                        client,
                        "HEAD",
                        normalized_url,
                        start_ts,
                        stream=True,
                        staging_lease=staging_lease,
                    )
                    try:
                        normalized_url = str(head_response.url)
                        if head_response.status_code < 400:
                            _ensure_allowed_content_type(head_response.headers)
                            head_headers = head_response.headers
                    finally:
                        await head_response.aclose()
                except httpx.HTTPError:
                    head_headers = None

                content_length = _parse_content_length(head_headers)
                if content_length and content_length > settings.max_ingest_bytes:
                    raise IngestError("File exceeds max ingest size")

                header_filename = _filename_from_headers(head_headers) if head_headers else None
                temp_path, size_bytes, stream_filename, _ = await _download_streaming(
                    client,
                    normalized_url,
                    temp_dir,
                    start_ts,
                    hasher,
                    staging_lease,
                )
                header_filename = header_filename or stream_filename

                filename = sanitize_filename(
                    filename_hint or header_filename or os.path.basename(urlparse(url).path)
                )

            staging_lease.ensure_current()
            _check_timeout(start_ts)
            mime_type, extension = _validate_magic(temp_path)
            probe_data = await _run_ingest_ffprobe(temp_path, staging_lease)
            staging_lease.ensure_current()
            _check_timeout(start_ts)

            _validate_media_duration(mime_type, probe_data)
            try:
                validate_media_probe(probe_data, expected_kind=kind_from_mime_type(mime_type))
            except MediaLimitError as exc:
                raise IngestError("Media exceeds the configured safety policy") from exc
            asset_id = uuid.uuid4().hex
            created_at = utc_now_iso()
            expires_at = utc_now_ts() + ttl_seconds

            asset = {
                "asset_id": asset_id,
                "source": source,
                "original_filename": filename,
                "mime_type": mime_type,
                "size_bytes": size_bytes,
                "sha256": hasher.hexdigest(),
                "created_at": created_at,
                "expires_at": expires_at,
            }
            if probe_data:
                asset.update(probe_data)

            staging_lease.ensure_current()
            persistence_settled = asyncio.Event()
            staging_lease.retain_until_settled(persistence_settled)
            try:
                persisted = await persist_asset_async(
                    temp_path,
                    asset,
                    extension,
                    on_settled=persistence_settled.set,
                )
            except (AssetQuotaError, AssetStateError, StorageError) as exc:
                raise IngestError(str(exc)) from exc
            staging_lease.ensure_current()
            return persisted
        finally:
            if temp_path and os.path.exists(temp_path):
                with suppress(OSError):
                    os.remove(temp_path)
