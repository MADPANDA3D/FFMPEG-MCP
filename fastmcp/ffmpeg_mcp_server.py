import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qs

import aiofiles
from mcp.server.fastmcp import FastMCP
from rq.job import Job
import uvicorn
from starlette.middleware.trustedhost import TrustedHostMiddleware

from cleanup import cleanup_loop
from config import settings
from discord_export import DiscordExportError, send_file
from drive_utils import DriveError, upload_file
from ffprobe_utils import run_ffprobe
from ingest import IngestError, ingest_from_url
from jobs import extract_audio_job, thumbnail_job, transcode_job, trim_job
from presets import describe_preset, get_preset, list_presets
from task_queue import get_queue
from redis_store import (
    build_cache_key,
    delete_cached_result,
    get_asset,
    get_cached_result,
    get_job,
    save_job,
    update_asset,
    update_job,
)
from storage import download_to_temp, generate_download_url, local_path_from_key, verify_local_signature
from utils import utc_now_iso, utc_now_ts


if settings.log_requests:
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

mcp = FastMCP(
    name="av-suite-mcp",
    stateless_http=True,
    json_response=True,
    host="0.0.0.0",
)

TOOL_MODE = settings.tool_mode if settings.tool_mode in {"individual", "router"} else "individual"

_cleanup_started = False


def _start_cleanup_thread() -> None:
    global _cleanup_started
    if _cleanup_started:
        return
    _cleanup_started = True

    def launch():
        asyncio.run(cleanup_loop())

    import threading

    thread = threading.Thread(target=launch, daemon=True)
    thread.start()


async def _download_handler(scope, receive, send) -> None:
    if scope.get("method") not in {"GET", "HEAD"}:
        await send(
            {
                "type": "http.response.start",
                "status": 405,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"Method not allowed"})
        return

    if settings.storage_backend != "local":
        await send(
            {
                "type": "http.response.start",
                "status": 404,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"Not found"})
        return

    path = scope.get("path", "")
    parts = path.strip("/").split("/")
    if len(parts) != 2:
        await send(
            {
                "type": "http.response.start",
                "status": 404,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"Not found"})
        return
    asset_id = parts[1]

    query = parse_qs((scope.get("query_string") or b"").decode("utf-8"))
    exp_raw = query.get("exp", [""])[0]
    sig = query.get("sig", [""])[0]
    try:
        exp = int(exp_raw)
    except (TypeError, ValueError):
        exp = 0

    if exp <= int(time.time()):
        await send(
            {
                "type": "http.response.start",
                "status": 403,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"Expired"})
        return

    if not verify_local_signature(asset_id, exp, sig):
        await send(
            {
                "type": "http.response.start",
                "status": 403,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"Forbidden"})
        return

    asset = get_asset(asset_id)
    if not asset:
        await send(
            {
                "type": "http.response.start",
                "status": 404,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"Not found"})
        return

    storage_key = asset.get("storage_key")
    if not storage_key:
        await send(
            {
                "type": "http.response.start",
                "status": 404,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"Not found"})
        return

    if asset.get("expires_at") and int(asset["expires_at"]) <= int(time.time()):
        await send(
            {
                "type": "http.response.start",
                "status": 410,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"Gone"})
        return

    file_path = local_path_from_key(storage_key)
    if not os.path.exists(file_path):
        await send(
            {
                "type": "http.response.start",
                "status": 404,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"Not found"})
        return

    mime_type = asset.get("mime_type", "application/octet-stream")
    headers = [
        (b"content-type", mime_type.encode("utf-8")),
        (b"content-length", str(os.path.getsize(file_path)).encode("utf-8")),
    ]
    filename = asset.get("original_filename")
    if filename:
        disposition = f'attachment; filename="{filename}"'
        headers.append((b"content-disposition", disposition.encode("utf-8")))

    await send({"type": "http.response.start", "status": 200, "headers": headers})
    if scope.get("method") == "HEAD":
        await send({"type": "http.response.body", "body": b""})
        return

    async with aiofiles.open(file_path, "rb") as handle:
        while True:
            chunk = await handle.read(1024 * 256)
            if not chunk:
                break
            await send({"type": "http.response.body", "body": chunk, "more_body": True})
    await send({"type": "http.response.body", "body": b"", "more_body": False})


def _map_rq_status(status: str) -> str:
    return {
        "queued": "queued",
        "started": "running",
        "deferred": "queued",
        "finished": "success",
        "failed": "error",
    }.get(status, status)


def _derive_error_code(error: str | None) -> str | None:
    if not error:
        return None
    lowered = error.lower()
    if "timeout" in lowered:
        return "timeout"
    if "exceeds max" in lowered or "max " in lowered and "exceed" in lowered:
        return "limit_exceeded"
    if "duration" in lowered and "exceed" in lowered:
        return "duration_limit"
    if "not found" in lowered:
        return "not_found"
    if "allowlist" in lowered or "host is not allowed" in lowered:
        return "allowlist"
    if "content-type" in lowered:
        return "content_type"
    return "processing_error"


def _last_log_line(logs: str | None) -> str | None:
    if not logs:
        return None
    lines = [line.strip() for line in logs.splitlines() if line.strip()]
    if not lines:
        return None
    return lines[-1]


def _build_cache_key(job_type: str, payload: dict) -> str:
    return build_cache_key(f"ffmpeg:{job_type}", payload)


def _resolve_cached_outputs(cache_key: str) -> list[str] | None:
    cached = get_cached_result(cache_key)
    if not cached:
        return None
    output_ids = cached.get("output_asset_ids")
    if not output_ids:
        delete_cached_result(cache_key)
        return None
    for asset_id in output_ids:
        asset = get_asset(asset_id)
        if not asset:
            delete_cached_result(cache_key)
            return None
        expires_at = asset.get("expires_at")
        if expires_at and int(expires_at) <= utc_now_ts():
            delete_cached_result(cache_key)
            return None
    return list(output_ids)


def _record_cached_job(job_type: str, input_asset_id: str, output_asset_ids: list[str], cache_key: str) -> str:
    job_id = uuid.uuid4().hex
    now = utc_now_iso()
    job = {
        "job_id": job_id,
        "type": job_type,
        "status": "success",
        "progress": 100,
        "input_asset_id": input_asset_id,
        "output_asset_ids": output_asset_ids,
        "error": None,
        "logs_short": "cache hit",
        "cache_hit": True,
        "cache_key": cache_key,
        "created_at": now,
        "updated_at": now,
        "started_at": now,
        "finished_at": now,
    }
    save_job(job, settings.job_ttl_seconds())
    return job_id


def _enqueue_job(job_type: str, func, args: tuple, cache_key: str | None = None) -> str:
    queue = get_queue()
    job_id = uuid.uuid4().hex
    now = utc_now_iso()
    job = {
        "job_id": job_id,
        "type": job_type,
        "status": "queued",
        "progress": 0,
        "input_asset_id": args[0] if args else "",
        "output_asset_ids": [],
        "error": None,
        "logs_short": None,
        "cache_hit": False,
        "cache_key": cache_key,
        "created_at": now,
        "updated_at": now,
    }
    save_job(job, settings.job_ttl_seconds())
    queue.enqueue(
        func,
        job_id=job_id,
        args=args,
        job_timeout=settings.ffmpeg_timeout_seconds + 60,
        result_ttl=settings.job_ttl_seconds(),
    )
    return job_id


def _sync_job_status(job_id: str, job_record: dict) -> dict:
    updated = dict(job_record)
    try:
        rq_job = Job.fetch(job_id, connection=get_queue().connection)
    except Exception:
        rq_job = None

    if rq_job is None:
        if updated.get("status") in {"queued", "running"}:
            updates = {
                "status": "error",
                "progress": 100,
                "error": "job missing or worker crashed",
                "updated_at": utc_now_iso(),
            }
            update_job(job_id, updates)
            updated.update(updates)
        return updated

    rq_status = _map_rq_status(rq_job.get_status())
    updates = {}
    if rq_status and rq_status != updated.get("status"):
        updates["status"] = rq_status
    if rq_status == "queued":
        updates.setdefault("progress", 0)
    elif rq_status == "running":
        updates.setdefault("progress", updated.get("progress") or 10)
        if not updated.get("started_at"):
            updates["started_at"] = utc_now_iso()
    elif rq_status == "success":
        updates.setdefault("progress", 100)
        if not updated.get("finished_at"):
            updates["finished_at"] = utc_now_iso()
    elif rq_status == "error":
        updates.setdefault("progress", 100)
        if rq_job.exc_info and not updated.get("error"):
            updates["error"] = rq_job.exc_info

    if rq_status == "running":
        stale_seconds = settings.stale_job_seconds()
        heartbeat = rq_job.last_heartbeat
        if heartbeat and isinstance(heartbeat, datetime):
            heartbeat_ts = int(heartbeat.replace(tzinfo=timezone.utc).timestamp())
            if utc_now_ts() - heartbeat_ts > stale_seconds:
                updates.update(
                    {
                        "status": "error",
                        "progress": 100,
                        "error": "worker heartbeat stale",
                        "finished_at": utc_now_iso(),
                    }
                )

    if updates:
        updates["updated_at"] = utc_now_iso()
        update_job(job_id, updates)
        updated.update(updates)

    return updated


async def tool_ingest_from_url(url: str, filename_hint: str | None = None, ttl_hours: int | None = None) -> dict:
    try:
        asset = await ingest_from_url(url, filename_hint, ttl_hours)
    except IngestError as exc:
        raise ValueError(str(exc))
    return {
        "asset_id": asset["asset_id"],
        "mime_type": asset.get("mime_type"),
        "size_bytes": asset.get("size_bytes"),
        "sha256": asset.get("sha256"),
        "original_filename": asset.get("original_filename"),
        "expires_at": asset.get("expires_at"),
    }


async def tool_ingest_from_drive(drive_file_id: str, ttl_hours: int | None = None) -> dict:
    if not drive_file_id:
        raise ValueError("drive_file_id is required")
    url = f"https://drive.google.com/uc?export=download&id={drive_file_id}"
    return await tool_ingest_from_url(url, None, ttl_hours)


async def tool_probe(asset_id: str) -> dict:
    asset = get_asset(asset_id)
    if not asset:
        raise ValueError("asset_id not found")
    storage_key = asset.get("storage_key")
    if not storage_key:
        raise ValueError("asset storage missing")

    path = local_path_from_key(storage_key)
    if settings.storage_backend == "s3":
        from storage import download_to_temp

        path = download_to_temp(storage_key)
    try:
        probe = await asyncio.to_thread(run_ffprobe, path)
    finally:
        if settings.storage_backend == "s3" and os.path.exists(path):
            os.remove(path)

    update_asset(asset_id, probe)
    return probe


async def tool_transcode(asset_id: str, preset: str) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    get_preset(preset)
    cache_key = _build_cache_key("transcode", {"asset_id": asset_id, "preset": preset})
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("transcode", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job("transcode", transcode_job, (asset_id, preset, cache_key), cache_key=cache_key)
    return {"job_id": job_id, "cache_hit": False}


async def tool_thumbnail(asset_id: str, time_sec: float = 3, width: int | None = None) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    cache_key = _build_cache_key(
        "thumbnail",
        {"asset_id": asset_id, "time_sec": float(time_sec), "width": width},
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("thumbnail", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "thumbnail", thumbnail_job, (asset_id, float(time_sec), width, cache_key), cache_key=cache_key
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_extract_audio(asset_id: str, format: str, bitrate: str | None = None) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    cache_key = _build_cache_key(
        "extract_audio",
        {"asset_id": asset_id, "format": format.lower(), "bitrate": bitrate},
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("extract_audio", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "extract_audio", extract_audio_job, (asset_id, format, bitrate, cache_key), cache_key=cache_key
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_trim(asset_id: str, start_sec: float, end_sec: float, reencode: bool = True) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    cache_key = _build_cache_key(
        "trim",
        {
            "asset_id": asset_id,
            "start_sec": float(start_sec),
            "end_sec": float(end_sec),
            "reencode": bool(reencode),
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("trim", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "trim",
        trim_job,
        (asset_id, float(start_sec), float(end_sec), bool(reencode), cache_key),
        cache_key=cache_key,
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_list_presets() -> dict:
    return {"presets": list_presets()}


async def tool_describe_preset(name: str) -> dict:
    if not name:
        raise ValueError("preset name is required")
    return {"preset": describe_preset(name)}


async def tool_capabilities() -> dict:
    presets = list_presets()
    output_containers = sorted(
        {preset.get("output_container") for preset in presets if preset.get("output_container")}
    )
    tool_names = ["FFMPEG_MCP"] if TOOL_MODE == "router" else sorted(TOOL_REGISTRY.keys())
    return {
        "tool_mode": TOOL_MODE,
        "tool_names": tool_names,
        "limits": {
            "max_ingest_bytes": settings.max_ingest_bytes,
            "max_output_bytes": settings.max_output_bytes,
            "max_duration_seconds": settings.max_duration_seconds,
            "ingest_timeout_seconds": settings.ingest_timeout_seconds,
            "ffmpeg_timeout_seconds": settings.ffmpeg_timeout_seconds,
            "download_url_ttl_seconds": settings.download_url_ttl_seconds,
            "discord_max_upload_bytes": settings.discord_max_upload_bytes,
        },
        "allowlist": {
            "domains": settings.allowed_domains,
            "content_types": settings.allowed_content_types,
        },
        "storage": {
            "backend": settings.storage_backend,
        },
        "queue": {
            "queue_name": settings.queue_name,
            "job_timeout_seconds": settings.ffmpeg_timeout_seconds + 60,
            "worker_concurrency": 1,
        },
        "cache": {
            "enabled": True,
            "default_ttl_seconds": settings.asset_ttl_seconds(),
            "strategy": "completed-job reuse",
        },
        "supported_inputs": ["video/*", "audio/*"],
        "output_containers": output_containers,
        "presets": presets,
    }


async def tool_job_status(job_id: str) -> dict:
    job_record = get_job(job_id) or {}
    if not job_record:
        return {
            "status": "unknown",
            "state": "unknown",
            "progress": None,
            "progress_pct": None,
            "output_asset_ids": None,
            "error": None,
            "logs_short": None,
            "last_log_line": None,
            "error_code": None,
            "started_at": None,
            "finished_at": None,
            "cache_hit": None,
        }

    synced = _sync_job_status(job_id, job_record)
    status = synced.get("status") or "unknown"
    state = status if status in {"queued", "running", "success", "error"} else "unknown"
    progress = synced.get("progress")
    if progress is None:
        progress = 0 if status == "queued" else 50 if status == "running" else 100
    error = synced.get("error")
    logs_short = synced.get("logs_short")
    return {
        "status": status,
        "state": state,
        "progress": progress,
        "progress_pct": progress,
        "output_asset_ids": synced.get("output_asset_ids"),
        "error": error,
        "logs_short": logs_short,
        "last_log_line": _last_log_line(logs_short),
        "error_code": _derive_error_code(error),
        "started_at": synced.get("started_at"),
        "finished_at": synced.get("finished_at"),
        "cache_hit": synced.get("cache_hit"),
    }


async def tool_get_download_url(asset_id: str) -> dict:
    asset = get_asset(asset_id)
    if not asset:
        raise ValueError("asset_id not found")
    storage_key = asset.get("storage_key")
    if not storage_key:
        raise ValueError("asset storage missing")
    expires_at = asset.get("expires_at")
    if expires_at and int(expires_at) <= utc_now_ts():
        raise ValueError("asset expired")
    url, exp = generate_download_url(asset_id, storage_key)
    return {"url": url, "expires_at": exp}


async def tool_export_to_drive(asset_id: str, folder_id: str | None = None) -> dict:
    asset = get_asset(asset_id)
    if not asset:
        raise ValueError("asset_id not found")
    storage_key = asset.get("storage_key")
    if not storage_key:
        raise ValueError("asset storage missing")

    if settings.storage_backend == "s3":
        path = download_to_temp(storage_key)
        cleanup = True
    else:
        path = local_path_from_key(storage_key)
        cleanup = False

    if not os.path.exists(path):
        raise ValueError("asset file missing")

    filename = asset.get("original_filename") or f"{asset_id}"
    folder = folder_id or settings.google_drive_folder_default or None
    try:
        drive_file_id = await asyncio.to_thread(
            upload_file, path, filename, asset.get("mime_type", ""), folder
        )
    except DriveError as exc:
        raise ValueError(str(exc))
    finally:
        if cleanup and os.path.exists(path):
            os.remove(path)

    return {"drive_file_id": drive_file_id}


async def tool_export_to_discord(
    asset_id: str,
    channel_id: str,
    message: str | None = None,
    filename: str | None = None,
) -> dict:
    asset = get_asset(asset_id)
    if not asset:
        raise ValueError("asset_id not found")
    if not channel_id:
        raise ValueError("channel_id is required")
    storage_key = asset.get("storage_key")
    if not storage_key:
        raise ValueError("asset storage missing")
    size_bytes = asset.get("size_bytes") or 0
    if size_bytes > settings.discord_max_upload_bytes:
        raise ValueError("asset exceeds Discord upload limit")

    if settings.storage_backend == "s3":
        path = download_to_temp(storage_key)
        cleanup = True
    else:
        path = local_path_from_key(storage_key)
        cleanup = False

    if not os.path.exists(path):
        raise ValueError("asset file missing")

    send_name = filename or asset.get("original_filename") or f"{asset_id}"
    try:
        message_id = await send_file(
            channel_id=channel_id,
            file_path=path,
            filename=send_name,
            message=message,
            mime_type=asset.get("mime_type"),
        )
    except DiscordExportError as exc:
        raise ValueError(str(exc))
    finally:
        if cleanup and os.path.exists(path):
            os.remove(path)

    return {"message_id": message_id}


TOOL_REGISTRY = {
    "media_ingest_from_url": tool_ingest_from_url,
    "media_ingest_from_drive": tool_ingest_from_drive,
    "media_probe": tool_probe,
    "ffmpeg_transcode": tool_transcode,
    "ffmpeg_thumbnail": tool_thumbnail,
    "ffmpeg_extract_audio": tool_extract_audio,
    "ffmpeg_trim": tool_trim,
    "ffmpeg_list_presets": tool_list_presets,
    "ffmpeg_describe_preset": tool_describe_preset,
    "ffmpeg_capabilities": tool_capabilities,
    "job_status": tool_job_status,
    "media_get_download_url": tool_get_download_url,
    "media_export_to_drive": tool_export_to_drive,
    "media_export_to_discord": tool_export_to_discord,
}


async def tool_router(tool: str, arguments: dict | None = None) -> dict:
    if not tool:
        raise ValueError("tool is required")
    func = TOOL_REGISTRY.get(tool)
    if not func:
        available = ", ".join(sorted(TOOL_REGISTRY.keys()))
        raise ValueError(f"Unknown tool: {tool}. Available tools: {available}")
    if arguments is None:
        arguments = {}
    if not isinstance(arguments, dict):
        raise ValueError("arguments must be an object")
    return await func(**arguments)


def register_tools() -> None:
    if TOOL_MODE == "router":
        mcp.tool(name="FFMPEG_MCP")(tool_router)
        return
    for name, func in TOOL_REGISTRY.items():
        mcp.tool(name=name)(func)


if __name__ == "__main__":
    register_tools()
    os.environ.setdefault("HOST", settings.mcp_bind_address)
    os.environ.setdefault("PORT", str(settings.mcp_http_port))
    app_factory = mcp.streamable_http_app

    def build_app():
        app = app_factory() if callable(app_factory) else app_factory
        try:
            app.add_middleware(TrustedHostMiddleware, allowed_hosts=["*"])
        except Exception:
            pass

        async def router(scope, receive, send):
            if scope.get("type") == "http" and scope.get("path", "").startswith("/download/"):
                return await _download_handler(scope, receive, send)
            return await app(scope, receive, send)

        async def host_override(scope, receive, send):
            if scope.get("type") == "http":
                headers = []
                for key, value in scope.get("headers", []):
                    if key.lower() == b"host":
                        continue
                    headers.append((key, value))
                headers.append((b"host", b"localhost"))
                scope = {**scope, "headers": headers}
            await router(scope, receive, send)

        return host_override

    _start_cleanup_thread()

    uvicorn.run(
        build_app,
        host=settings.mcp_bind_address,
        port=settings.mcp_http_port,
        factory=True,
    )
