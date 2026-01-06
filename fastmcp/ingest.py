import asyncio
import hashlib
import os
import re
import tempfile
import time
import uuid
from typing import Any
from urllib.parse import parse_qs, urlparse

import filetype
import httpx

from config import settings
from ffprobe_utils import run_ffprobe
from redis_store import save_asset
from storage import put_file
from utils import sanitize_filename, utc_now_iso, utc_now_ts


class IngestError(RuntimeError):
    pass


def _is_allowed_domain(host: str) -> bool:
    if not settings.allowed_domains:
        return True
    host = host.lower()
    for domain in settings.allowed_domains:
        if host == domain or host.endswith(f".{domain}"):
            return True
    return False


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
        return "any"
    return ", ".join(settings.allowed_domains)


def _allowed_content_types_message() -> str:
    if not settings.allowed_content_types:
        return "any"
    return ", ".join(settings.allowed_content_types)


def _extract_drive_id(url: str) -> str | None:
    parsed = urlparse(url)
    hostname = parsed.hostname or ""
    if "drive.google.com" not in hostname and "docs.google.com" not in hostname:
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
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise IngestError("Only http/https URLs are allowed")
    host = parsed.hostname or ""
    if not _is_allowed_domain(host):
        allowed = _allowed_domains_message()
        raise IngestError(
            f"URL host is not allowed (host: {host}). Allowed hosts: {allowed}"
        )
    drive_id = _extract_drive_id(url)
    if drive_id:
        direct = f"https://drive.google.com/uc?export=download&id={drive_id}"
        return direct, "drive"
    return url, "url"


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


def _check_timeout(start_ts: float) -> None:
    if time.monotonic() - start_ts > settings.ingest_timeout_seconds:
        raise IngestError("Ingest timed out")


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


def _supports_range(headers: httpx.Headers | None) -> bool:
    if not headers:
        return False
    return "bytes" in headers.get("accept-ranges", "").lower()


def _validate_response_hosts(response: httpx.Response) -> None:
    hosts = [response.url.host]
    for item in response.history:
        hosts.append(item.url.host)
    for host in hosts:
        if host and not _is_allowed_domain(host):
            allowed = _allowed_domains_message()
            raise IngestError(
                f"Redirected host is not allowed (host: {host}). Allowed hosts: {allowed}"
            )


def _ensure_allowed_content_type(headers: httpx.Headers | None) -> None:
    if not headers:
        return
    content_type = headers.get("content-type", "")
    if not _is_allowed_content_type(content_type):
        allowed = _allowed_content_types_message()
        raise IngestError(
            f"Content-Type is not allowed ({content_type}). Allowed types: {allowed}"
        )


async def _download_streaming(
    client: httpx.AsyncClient,
    url: str,
    temp_dir: str,
    start_ts: float,
    hasher: "hashlib._Hash",
) -> tuple[str, int, str | None, bytes]:
    temp_path = None
    size_bytes = 0
    header_filename = None
    first_bytes = b""
    magic_checked = False

    with tempfile.NamedTemporaryFile(dir=temp_dir, delete=False) as handle:
        temp_path = handle.name

    async with client.stream("GET", url) as response:
        _validate_response_hosts(response)
        response.raise_for_status()
        _ensure_allowed_content_type(response.headers)
        header_filename = _filename_from_headers(response.headers)
        with open(temp_path, "ab") as handle:
            async for chunk in response.aiter_bytes(settings.ingest_stream_chunk_bytes):
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

    return temp_path, size_bytes, header_filename, first_bytes


async def _download_ranges(
    client: httpx.AsyncClient,
    url: str,
    temp_dir: str,
    start_ts: float,
    hasher: "hashlib._Hash",
    content_length: int,
) -> tuple[str, int, str | None, bytes]:
    temp_path = None
    size_bytes = 0
    header_filename = None
    first_bytes = b""
    magic_checked = False

    with tempfile.NamedTemporaryFile(dir=temp_dir, delete=False) as handle:
        temp_path = handle.name

    start = 0
    with open(temp_path, "ab") as handle:
        while start < content_length:
            _check_timeout(start_ts)
            end = min(start + settings.ingest_range_chunk_bytes - 1, content_length - 1)
            headers = {"Range": f"bytes={start}-{end}"}
            response = await client.get(url, headers=headers)
            _validate_response_hosts(response)
            response.raise_for_status()
            _ensure_allowed_content_type(response.headers)
            if header_filename is None:
                header_filename = _filename_from_headers(response.headers)
            data = await response.aread()
            if not data:
                raise IngestError("Empty response while downloading")
            if response.status_code == 200 and start == 0:
                # Server ignored range; treat as full download and stop.
                content_length = len(data)
            size_bytes += len(data)
            if size_bytes > settings.max_ingest_bytes:
                raise IngestError("File exceeds max ingest size")
            if not magic_checked:
                first_bytes += data
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
            hasher.update(data)
            handle.write(data)
            start += len(data)
            if response.status_code == 200:
                break

    return temp_path, size_bytes, header_filename, first_bytes


async def ingest_from_url(url: str, filename_hint: str | None, ttl_hours: int | None) -> dict[str, Any]:
    normalized_url, source = normalize_ingest_url(url)
    ttl_seconds = (ttl_hours or settings.asset_ttl_hours) * 3600
    temp_dir = settings.storage_temp_dir
    os.makedirs(temp_dir, exist_ok=True)
    temp_path = None
    start_ts = time.monotonic()
    hasher = hashlib.sha256()
    size_bytes = 0
    filename = None

    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(settings.ingest_timeout_seconds),
        ) as client:
            head_headers = None
            try:
                head_response = await client.head(normalized_url)
                _validate_response_hosts(head_response)
                if head_response.status_code < 400:
                    _ensure_allowed_content_type(head_response.headers)
                    head_headers = head_response.headers
                    normalized_url = str(head_response.url)
            except httpx.HTTPError:
                head_headers = None

            content_length = _parse_content_length(head_headers)
            if content_length and content_length > settings.max_ingest_bytes:
                raise IngestError("File exceeds max ingest size")

            header_filename = _filename_from_headers(head_headers) if head_headers else None
            if content_length and _supports_range(head_headers):
                temp_path, size_bytes, range_filename, _ = await _download_ranges(
                    client, normalized_url, temp_dir, start_ts, hasher, content_length
                )
                header_filename = header_filename or range_filename
            else:
                temp_path, size_bytes, stream_filename, _ = await _download_streaming(
                    client, normalized_url, temp_dir, start_ts, hasher
                )
                header_filename = header_filename or stream_filename

            filename = sanitize_filename(
                filename_hint or header_filename or os.path.basename(urlparse(url).path)
            )

        _check_timeout(start_ts)
        probe_data = None
        try:
            probe_data = await asyncio.to_thread(run_ffprobe, temp_path)
        except Exception:
            probe_data = None

        if probe_data and probe_data.get("duration_sec"):
            duration = probe_data["duration_sec"]
            if duration and duration > settings.max_duration_seconds:
                raise IngestError("Media exceeds max duration")

        mime_type, extension = _validate_magic(temp_path)
        asset_id = uuid.uuid4().hex
        storage_key, storage_uri, final_size = put_file(temp_path, asset_id=asset_id, ext=extension)
        if final_size != size_bytes:
            size_bytes = final_size

        created_at = utc_now_iso()
        expires_at = utc_now_ts() + ttl_seconds

        asset = {
            "asset_id": asset_id,
            "source": source,
            "original_filename": filename,
            "mime_type": mime_type,
            "size_bytes": size_bytes,
            "sha256": hasher.hexdigest(),
            "storage_uri": storage_uri,
            "storage_key": storage_key,
            "created_at": created_at,
            "expires_at": expires_at,
        }
        if probe_data:
            asset.update(probe_data)

        save_asset(asset, ttl_seconds)
        return asset
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError:
                pass
