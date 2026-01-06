import asyncio
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qs

import aiofiles
from mcp.server.fastmcp import FastMCP
from rq import Queue
from rq.job import Job
import uvicorn
from starlette.middleware.trustedhost import TrustedHostMiddleware

from brand_kits import sanitize_brand_kit
from cleanup import cleanup_loop
from config import settings
from discord_export import DiscordExportError, send_file
from drive_utils import DriveError, upload_file
from ffprobe_utils import run_ffprobe
from ingest import IngestError, ingest_from_url
from jobs import (
    asset_compare_job,
    audio_duck_job,
    audio_fade_job,
    audio_mix_job,
    audio_mix_with_background_job,
    audio_normalize_job,
    audio_trim_silence_job,
    batch_export_job,
    brand_kit_apply_job,
    captions_burn_in_job,
    campaign_process_job,
    extract_audio_job,
    image_to_video_job,
    images_to_slideshow_job,
    images_to_slideshow_ken_burns_job,
    render_offer_card_job,
    render_iterate_job,
    render_social_ad_job,
    render_testimonial_clip_job,
    template_apply_job,
    thumbnail_job,
    transcode_job,
    trim_job,
    video_concat_job,
    video_add_logo_job,
    video_add_text_job,
    video_analyze_job,
    workflow_job,
)
from metrics import collect_metrics_snapshot, log_event, record_cache_hit, record_cache_miss
from overlay_utils import (
    DEFAULT_BOX_BORDER_WIDTH,
    DEFAULT_BOX_COLOR,
    DEFAULT_FONT_COLOR,
    DEFAULT_FONT_SIZE,
    DEFAULT_LOGO_OPACITY,
    DEFAULT_LOGO_POSITION,
    DEFAULT_LOGO_SCALE_PCT,
    DEFAULT_TEXT_POSITION,
    LOGO_POSITIONS,
    TEXT_POSITIONS,
    sanitize_box_border,
    sanitize_color,
    sanitize_font_size,
    sanitize_opacity,
    sanitize_position,
    sanitize_scale_pct,
    sanitize_text,
)
from presets import describe_preset, get_preset, list_presets
from rubrics import describe_rubric, list_rubrics
from templates import describe_template, list_templates
from task_queue import get_queue
from redis_store import (
    build_cache_key,
    delete_cached_result,
    get_asset,
    get_brand_kit,
    get_cached_result,
    get_job,
    get_redis,
    list_brand_kits,
    save_brand_kit,
    delete_brand_kit,
    save_job,
    update_asset,
    update_job,
)
from storage import download_to_temp, generate_download_url, local_path_from_key, verify_local_signature
from utils import utc_now_iso, utc_now_ts


if settings.log_requests or settings.log_structured:
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


def _resolve_cached_payload(cache_key: str) -> dict | None:
    cached = get_cached_result(cache_key)
    if not cached:
        return None
    output_ids = cached.get("output_asset_ids") or []
    for asset_id in output_ids:
        asset = get_asset(asset_id)
        if not asset:
            delete_cached_result(cache_key)
            return None
        expires_at = asset.get("expires_at")
        if expires_at and int(expires_at) <= utc_now_ts():
            delete_cached_result(cache_key)
            return None
    return cached


def _resolve_cached_outputs(cache_key: str) -> list[str] | None:
    cached = _resolve_cached_payload(cache_key)
    if not cached:
        return None
    output_ids = cached.get("output_asset_ids")
    if not output_ids:
        delete_cached_result(cache_key)
        return None
    return list(output_ids)


def _record_cached_job(
    job_type: str,
    input_asset_id: str,
    output_asset_ids: list[str],
    cache_key: str,
    extra: dict | None = None,
) -> str:
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
    if extra:
        job.update(extra)
    save_job(job, settings.job_ttl_seconds())
    record_cache_hit(job_type)
    log_event(
        "cache_hit",
        {"job_type": job_type, "job_id": job_id, "input_asset_id": input_asset_id},
    )
    return job_id


def _enqueue_job(
    job_type: str,
    func,
    args: tuple,
    cache_key: str | None = None,
    priority: str | None = None,
    job_timeout: int | None = None,
) -> str:
    queue = get_queue(priority=priority)
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
        job_timeout=(job_timeout or settings.ffmpeg_timeout_seconds) + 60,
        result_ttl=settings.job_ttl_seconds(),
    )
    record_cache_miss(job_type)
    log_event(
        "job_enqueued",
        {"job_type": job_type, "job_id": job_id, "priority": priority or "default"},
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


async def tool_transcode(asset_id: str, preset: str, priority: str | None = None) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    get_preset(preset)
    cache_key = _build_cache_key("transcode", {"asset_id": asset_id, "preset": preset})
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("transcode", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "transcode",
        transcode_job,
        (asset_id, preset, cache_key),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.ffmpeg_timeout_seconds,
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_thumbnail(
    asset_id: str, time_sec: float = 3, width: int | None = None, priority: str | None = None
) -> dict:
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
        "thumbnail",
        thumbnail_job,
        (asset_id, float(time_sec), width, cache_key),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.ffmpeg_timeout_seconds,
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_extract_audio(
    asset_id: str, format: str, bitrate: str | None = None, priority: str | None = None
) -> dict:
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
        "extract_audio",
        extract_audio_job,
        (asset_id, format, bitrate, cache_key),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.audio_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_trim(
    asset_id: str,
    start_sec: float,
    end_sec: float,
    reencode: bool = True,
    priority: str | None = None,
) -> dict:
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
        priority=priority,
        job_timeout=settings.ffmpeg_timeout_seconds,
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_video_add_text(
    asset_id: str,
    text: str,
    position: str | None = None,
    font_size: int | None = None,
    font_color: str | None = None,
    background_box: bool | None = None,
    box_color: str | None = None,
    box_border_width: int | None = None,
    font_name: str | None = None,
    font_asset_id: str | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    cleaned_text = sanitize_text(text)
    resolved_position = sanitize_position(position or DEFAULT_TEXT_POSITION, TEXT_POSITIONS)
    resolved_font_size = sanitize_font_size(font_size, DEFAULT_FONT_SIZE)
    resolved_font_color = sanitize_color(font_color, DEFAULT_FONT_COLOR)
    resolved_box_color = sanitize_color(box_color, DEFAULT_BOX_COLOR)
    resolved_box_border = sanitize_box_border(box_border_width, DEFAULT_BOX_BORDER_WIDTH)
    resolved_box = True if background_box is None else bool(background_box)

    cache_key = _build_cache_key(
        "video_add_text",
        {
            "asset_id": asset_id,
            "text": cleaned_text,
            "position": resolved_position,
            "font_size": resolved_font_size,
            "font_color": resolved_font_color,
            "background_box": resolved_box,
            "box_color": resolved_box_color,
            "box_border_width": resolved_box_border,
            "font_name": font_name,
            "font_asset_id": font_asset_id,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("video_add_text", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "video_add_text",
        video_add_text_job,
        (
            asset_id,
            cleaned_text,
            resolved_position,
            resolved_font_size,
            resolved_font_color,
            resolved_box,
            resolved_box_color,
            resolved_box_border,
            font_name,
            font_asset_id,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.text_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_video_add_logo(
    asset_id: str,
    logo_asset_id: str | None = None,
    logo_key: str | None = None,
    position: str | None = None,
    scale_pct: int | None = None,
    opacity: float | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    if not logo_asset_id and not logo_key:
        raise ValueError("logo_asset_id or logo_key is required")
    resolved_position = sanitize_position(position or DEFAULT_LOGO_POSITION, LOGO_POSITIONS)
    resolved_scale = sanitize_scale_pct(scale_pct, DEFAULT_LOGO_SCALE_PCT)
    resolved_opacity = sanitize_opacity(opacity, DEFAULT_LOGO_OPACITY)

    cache_key = _build_cache_key(
        "video_add_logo",
        {
            "asset_id": asset_id,
            "logo_asset_id": logo_asset_id,
            "logo_key": logo_key,
            "position": resolved_position,
            "scale_pct": resolved_scale,
            "opacity": resolved_opacity,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("video_add_logo", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "video_add_logo",
        video_add_logo_job,
        (
            asset_id,
            logo_asset_id,
            logo_key,
            resolved_position,
            resolved_scale,
            resolved_opacity,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.logo_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_captions_burn_in(
    asset_id: str,
    captions_srt: str | None = None,
    captions_vtt: str | None = None,
    words_json: list[dict] | None = None,
    brand_kit_id: str | None = None,
    highlight_mode: str | None = None,
    position: str | None = None,
    font_size: int | None = None,
    font_color: str | None = None,
    box_color: str | None = None,
    box_opacity: float | None = None,
    highlight_color: str | None = None,
    padding_px: int | None = None,
    max_chars: int | None = None,
    max_lines: int | None = None,
    max_words: int | None = None,
    safe_zone_bottom_px: int | None = None,
    safe_zone_top_px: int | None = None,
    font_name: str | None = None,
    font_asset_id: str | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    if sum(bool(value) for value in [captions_srt, captions_vtt, words_json]) != 1:
        raise ValueError("Provide exactly one of captions_srt, captions_vtt, words_json")
    if words_json is not None and not isinstance(words_json, list):
        raise ValueError("words_json must be a list")
    if brand_kit_id and not get_brand_kit(brand_kit_id):
        raise ValueError("brand_kit_id not found")
    if highlight_mode:
        highlight_mode = highlight_mode.strip().lower()

    cache_key = _build_cache_key(
        "captions_burn_in",
        {
            "asset_id": asset_id,
            "captions_srt": captions_srt,
            "captions_vtt": captions_vtt,
            "words_json": words_json,
            "brand_kit_id": brand_kit_id,
            "highlight_mode": highlight_mode,
            "position": position,
            "font_size": font_size,
            "font_color": font_color,
            "box_color": box_color,
            "box_opacity": box_opacity,
            "highlight_color": highlight_color,
            "padding_px": padding_px,
            "max_chars": max_chars,
            "max_lines": max_lines,
            "max_words": max_words,
            "safe_zone_bottom_px": safe_zone_bottom_px,
            "safe_zone_top_px": safe_zone_top_px,
            "font_name": font_name,
            "font_asset_id": font_asset_id,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("captions_burn_in", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "captions_burn_in",
        captions_burn_in_job,
        (
            asset_id,
            captions_srt,
            captions_vtt,
            words_json,
            brand_kit_id,
            highlight_mode,
            position,
            font_size,
            font_color,
            box_color,
            box_opacity,
            highlight_color,
            padding_px,
            max_chars,
            max_lines,
            max_words,
            safe_zone_bottom_px,
            safe_zone_top_px,
            font_name,
            font_asset_id,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.text_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_video_analyze(
    asset_id: str,
    rubric_name: str | None = None,
    target_preset: str | None = None,
    captions_srt: str | None = None,
    captions_vtt: str | None = None,
    words_json: list[dict] | None = None,
    brand_kit_id: str | None = None,
    position: str | None = None,
    font_size: int | None = None,
    padding_px: int | None = None,
    max_chars: int | None = None,
    max_lines: int | None = None,
    max_words: int | None = None,
    safe_zone_bottom_px: int | None = None,
    safe_zone_top_px: int | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    if sum(bool(value) for value in [captions_srt, captions_vtt, words_json]) > 1:
        raise ValueError("Provide only one of captions_srt, captions_vtt, words_json")
    if words_json is not None and not isinstance(words_json, list):
        raise ValueError("words_json must be a list")
    if brand_kit_id and not get_brand_kit(brand_kit_id):
        raise ValueError("brand_kit_id not found")
    if target_preset:
        get_preset(target_preset)
    if rubric_name:
        describe_rubric(rubric_name)

    cache_key = _build_cache_key(
        "video_analyze",
        {
            "asset_id": asset_id,
            "rubric_name": rubric_name,
            "target_preset": target_preset,
            "captions_srt": captions_srt,
            "captions_vtt": captions_vtt,
            "words_json": words_json,
            "brand_kit_id": brand_kit_id,
            "position": position,
            "font_size": font_size,
            "padding_px": padding_px,
            "max_chars": max_chars,
            "max_lines": max_lines,
            "max_words": max_words,
            "safe_zone_bottom_px": safe_zone_bottom_px,
            "safe_zone_top_px": safe_zone_top_px,
        },
    )
    cached_payload = _resolve_cached_payload(cache_key)
    if cached_payload and cached_payload.get("report"):
        report = cached_payload.get("report")
        output_ids = cached_payload.get("output_asset_ids") or [asset_id]
        job_id = _record_cached_job(
            "video_analyze",
            asset_id,
            list(output_ids),
            cache_key,
            extra={"report": report},
        )
        return {"job_id": job_id, "cache_hit": True, "report": report}
    job_id = _enqueue_job(
        "video_analyze",
        video_analyze_job,
        (
            asset_id,
            rubric_name,
            target_preset,
            captions_srt,
            captions_vtt,
            words_json,
            brand_kit_id,
            position,
            font_size,
            padding_px,
            max_chars,
            max_lines,
            max_words,
            safe_zone_bottom_px,
            safe_zone_top_px,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.ffmpeg_timeout_seconds,
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_asset_compare(
    asset_ids: list[str],
    rubric_name: str,
    target_preset: str | None = None,
    priority: str | None = None,
) -> dict:
    if not asset_ids:
        raise ValueError("asset_ids is required")
    if len(asset_ids) > settings.max_batch_assets:
        raise ValueError("Too many assets to compare")
    for asset_id in asset_ids:
        if not get_asset(asset_id):
            raise ValueError("asset_id not found")
    if not rubric_name:
        raise ValueError("rubric_name is required")
    describe_rubric(rubric_name)
    if target_preset:
        get_preset(target_preset)

    cache_key = _build_cache_key(
        "asset_compare",
        {
            "asset_ids": asset_ids,
            "rubric_name": rubric_name,
            "target_preset": target_preset,
        },
    )
    cached_payload = _resolve_cached_payload(cache_key)
    if cached_payload and cached_payload.get("ranking"):
        ranking = cached_payload.get("ranking")
        output_ids = cached_payload.get("output_asset_ids") or []
        job_id = _record_cached_job(
            "asset_compare",
            asset_ids[0],
            list(output_ids),
            cache_key,
            extra={"ranking": ranking},
        )
        return {"job_id": job_id, "cache_hit": True, "ranking": ranking}
    job_id = _enqueue_job(
        "asset_compare",
        asset_compare_job,
        (asset_ids, rubric_name, target_preset, cache_key),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.batch_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_video_concat(
    asset_ids: list[str],
    transition: str | None = None,
    transition_duration: float | None = None,
    target_width: int | None = None,
    target_height: int | None = None,
    include_audio: bool | None = None,
    priority: str | None = None,
) -> dict:
    if not asset_ids or len(asset_ids) < 2:
        raise ValueError("asset_ids must contain at least two items")
    if len(asset_ids) > settings.max_concat_clips:
        raise ValueError("Too many clips for concat")
    for asset_id in asset_ids:
        if not get_asset(asset_id):
            raise ValueError("asset_id not found")

    cache_key = _build_cache_key(
        "video_concat",
        {
            "asset_ids": asset_ids,
            "transition": transition,
            "transition_duration": transition_duration,
            "target_width": target_width,
            "target_height": target_height,
            "include_audio": include_audio,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("video_concat", asset_ids[0], cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "video_concat",
        video_concat_job,
        (
            asset_ids,
            transition,
            transition_duration,
            target_width,
            target_height,
            include_audio,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.concat_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_image_to_video(
    asset_id: str,
    duration_sec: float | None = None,
    width: int | None = None,
    height: int | None = None,
    fps: int | None = None,
    background_color: str | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    duration_sec = duration_sec if duration_sec is not None else settings.default_image_duration_sec
    cache_key = _build_cache_key(
        "image_to_video",
        {
            "asset_id": asset_id,
            "duration_sec": float(duration_sec),
            "width": width,
            "height": height,
            "fps": fps,
            "background_color": background_color,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("image_to_video", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "image_to_video",
        image_to_video_job,
        (
            asset_id,
            float(duration_sec),
            width,
            height,
            fps,
            background_color,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.image_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_images_to_slideshow(
    asset_ids: list[str],
    duration_per_image: float | None = None,
    durations: list[float] | None = None,
    width: int | None = None,
    height: int | None = None,
    fps: int | None = None,
    background_color: str | None = None,
    priority: str | None = None,
) -> dict:
    if not asset_ids:
        raise ValueError("asset_ids is required")
    if len(asset_ids) > settings.max_slideshow_images:
        raise ValueError("Too many images for slideshow")
    for asset_id in asset_ids:
        if not get_asset(asset_id):
            raise ValueError("asset_id not found")

    cache_key = _build_cache_key(
        "images_to_slideshow",
        {
            "asset_ids": asset_ids,
            "duration_per_image": duration_per_image,
            "durations": durations,
            "width": width,
            "height": height,
            "fps": fps,
            "background_color": background_color,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("images_to_slideshow", asset_ids[0], cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "images_to_slideshow",
        images_to_slideshow_job,
        (
            asset_ids,
            duration_per_image,
            durations,
            width,
            height,
            fps,
            background_color,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.slideshow_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_images_to_slideshow_ken_burns(
    asset_ids: list[str],
    duration_per_image: float | None = None,
    durations: list[float] | None = None,
    width: int | None = None,
    height: int | None = None,
    fps: int | None = None,
    background_color: str | None = None,
    priority: str | None = None,
) -> dict:
    if not asset_ids:
        raise ValueError("asset_ids is required")
    if len(asset_ids) > settings.max_slideshow_images:
        raise ValueError("Too many images for slideshow")
    for asset_id in asset_ids:
        if not get_asset(asset_id):
            raise ValueError("asset_id not found")

    cache_key = _build_cache_key(
        "images_to_slideshow_ken_burns",
        {
            "asset_ids": asset_ids,
            "duration_per_image": duration_per_image,
            "durations": durations,
            "width": width,
            "height": height,
            "fps": fps,
            "background_color": background_color,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job(
            "images_to_slideshow_ken_burns", asset_ids[0], cached_outputs, cache_key
        )
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "images_to_slideshow_ken_burns",
        images_to_slideshow_ken_burns_job,
        (
            asset_ids,
            duration_per_image,
            durations,
            width,
            height,
            fps,
            background_color,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.slideshow_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_audio_normalize(
    asset_id: str,
    output_format: str = "m4a",
    target_lufs: float | None = None,
    lra: float | None = None,
    true_peak: float | None = None,
    bitrate: str | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    cache_key = _build_cache_key(
        "audio_normalize",
        {
            "asset_id": asset_id,
            "output_format": output_format,
            "target_lufs": target_lufs,
            "lra": lra,
            "true_peak": true_peak,
            "bitrate": bitrate,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("audio_normalize", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "audio_normalize",
        audio_normalize_job,
        (
            asset_id,
            output_format,
            target_lufs,
            lra,
            true_peak,
            bitrate,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.audio_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_audio_mix(
    asset_ids: list[str],
    output_format: str = "m4a",
    volumes: list[float] | None = None,
    normalize: bool | None = None,
    duration_mode: str | None = None,
    bitrate: str | None = None,
    priority: str | None = None,
) -> dict:
    if not asset_ids:
        raise ValueError("asset_ids is required")
    if len(asset_ids) > settings.max_audio_tracks:
        raise ValueError("Too many audio tracks")
    for asset_id in asset_ids:
        if not get_asset(asset_id):
            raise ValueError("asset_id not found")
    cache_key = _build_cache_key(
        "audio_mix",
        {
            "asset_ids": asset_ids,
            "output_format": output_format,
            "volumes": volumes,
            "normalize": normalize,
            "duration_mode": duration_mode,
            "bitrate": bitrate,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("audio_mix", asset_ids[0], cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "audio_mix",
        audio_mix_job,
        (
            asset_ids,
            output_format,
            volumes,
            normalize,
            duration_mode,
            bitrate,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.audio_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_audio_duck(
    voice_asset_id: str,
    music_asset_id: str,
    output_format: str = "m4a",
    ratio: float | None = None,
    threshold: float | None = None,
    attack_ms: int | None = None,
    release_ms: int | None = None,
    music_gain: float | None = None,
    bitrate: str | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(voice_asset_id):
        raise ValueError("voice_asset_id not found")
    if not get_asset(music_asset_id):
        raise ValueError("music_asset_id not found")
    cache_key = _build_cache_key(
        "audio_duck",
        {
            "voice_asset_id": voice_asset_id,
            "music_asset_id": music_asset_id,
            "output_format": output_format,
            "ratio": ratio,
            "threshold": threshold,
            "attack_ms": attack_ms,
            "release_ms": release_ms,
            "music_gain": music_gain,
            "bitrate": bitrate,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("audio_duck", voice_asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "audio_duck",
        audio_duck_job,
        (
            voice_asset_id,
            music_asset_id,
            output_format,
            ratio,
            threshold,
            attack_ms,
            release_ms,
            music_gain,
            bitrate,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.audio_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_audio_mix_with_background(
    voice_asset_id: str,
    music_asset_id: str,
    output_format: str = "m4a",
    ducking: bool | None = None,
    ratio: float | None = None,
    threshold: float | None = None,
    attack_ms: int | None = None,
    release_ms: int | None = None,
    music_gain: float | None = None,
    voice_gain: float | None = None,
    bitrate: str | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(voice_asset_id):
        raise ValueError("voice_asset_id not found")
    if not get_asset(music_asset_id):
        raise ValueError("music_asset_id not found")
    cache_key = _build_cache_key(
        "audio_mix_with_background",
        {
            "voice_asset_id": voice_asset_id,
            "music_asset_id": music_asset_id,
            "output_format": output_format,
            "ducking": ducking,
            "ratio": ratio,
            "threshold": threshold,
            "attack_ms": attack_ms,
            "release_ms": release_ms,
            "music_gain": music_gain,
            "voice_gain": voice_gain,
            "bitrate": bitrate,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("audio_mix_with_background", voice_asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "audio_mix_with_background",
        audio_mix_with_background_job,
        (
            voice_asset_id,
            music_asset_id,
            output_format,
            ducking,
            ratio,
            threshold,
            attack_ms,
            release_ms,
            music_gain,
            voice_gain,
            bitrate,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.audio_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_audio_fade(
    asset_id: str,
    output_format: str = "m4a",
    fade_in_sec: float | None = None,
    fade_out_sec: float | None = None,
    fade_out_start: float | None = None,
    bitrate: str | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    cache_key = _build_cache_key(
        "audio_fade",
        {
            "asset_id": asset_id,
            "output_format": output_format,
            "fade_in_sec": fade_in_sec,
            "fade_out_sec": fade_out_sec,
            "fade_out_start": fade_out_start,
            "bitrate": bitrate,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("audio_fade", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "audio_fade",
        audio_fade_job,
        (
            asset_id,
            output_format,
            fade_in_sec,
            fade_out_sec,
            fade_out_start,
            bitrate,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.audio_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_audio_trim_silence(
    asset_id: str,
    output_format: str = "m4a",
    min_silence_sec: float | None = None,
    threshold_db: float | None = None,
    trim_leading: bool | None = None,
    trim_trailing: bool | None = None,
    bitrate: str | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    cache_key = _build_cache_key(
        "audio_trim_silence",
        {
            "asset_id": asset_id,
            "output_format": output_format,
            "min_silence_sec": min_silence_sec,
            "threshold_db": threshold_db,
            "trim_leading": trim_leading,
            "trim_trailing": trim_trailing,
            "bitrate": bitrate,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("audio_trim_silence", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "audio_trim_silence",
        audio_trim_silence_job,
        (
            asset_id,
            output_format,
            min_silence_sec,
            threshold_db,
            trim_leading,
            trim_trailing,
            bitrate,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.audio_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_template_list() -> dict:
    return {"templates": list_templates()}


async def tool_template_describe(name: str) -> dict:
    if not name:
        raise ValueError("template name is required")
    return {"template": describe_template(name)}


async def tool_template_apply(
    asset_id: str,
    template_name: str,
    variables: dict | None = None,
    brand_kit_id: str | None = None,
    quality: str | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    if not template_name:
        raise ValueError("template_name is required")
    if variables is not None and not isinstance(variables, dict):
        raise ValueError("variables must be an object")
    if brand_kit_id:
        if not get_brand_kit(brand_kit_id):
            raise ValueError("brand_kit_id not found")
    if quality:
        quality = quality.strip().lower()
        if quality not in {"final", "draft"}:
            raise ValueError("quality must be 'final' or 'draft'")

    cache_key = _build_cache_key(
        "template_apply",
        {
            "asset_id": asset_id,
            "template_name": template_name,
            "variables": variables or {},
            "brand_kit_id": brand_kit_id,
            "quality": quality,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("template_apply", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "template_apply",
        template_apply_job,
        (asset_id, template_name, variables or {}, brand_kit_id, quality, cache_key),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.template_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_brand_kit_upsert(brand_kit: dict) -> dict:
    kit = sanitize_brand_kit(brand_kit)
    existing = get_brand_kit(kit["brand_kit_id"])
    if existing and existing.get("created_at"):
        kit["created_at"] = existing["created_at"]
    else:
        kit["created_at"] = utc_now_iso()
    kit["updated_at"] = utc_now_iso()
    save_brand_kit(kit)
    return {"brand_kit": kit}


async def tool_brand_kit_get(brand_kit_id: str) -> dict:
    if not brand_kit_id:
        raise ValueError("brand_kit_id is required")
    kit = get_brand_kit(brand_kit_id)
    if not kit:
        raise ValueError("brand_kit_id not found")
    return {"brand_kit": kit}


async def tool_brand_kit_list() -> dict:
    ids = list_brand_kits()
    return {"brand_kit_ids": sorted(ids)}


async def tool_brand_kit_delete(brand_kit_id: str) -> dict:
    if not brand_kit_id:
        raise ValueError("brand_kit_id is required")
    delete_brand_kit(brand_kit_id)
    return {"deleted": True}


async def tool_brand_kit_apply(
    asset_id: str,
    brand_kit_id: str,
    text: str | None = None,
    position: str | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    if not get_brand_kit(brand_kit_id):
        raise ValueError("brand_kit_id not found")
    cache_key = _build_cache_key(
        "brand_kit_apply",
        {"asset_id": asset_id, "brand_kit_id": brand_kit_id, "text": text, "position": position},
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("brand_kit_apply", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "brand_kit_apply",
        brand_kit_apply_job,
        (asset_id, brand_kit_id, text, position, cache_key),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.template_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_batch_export_formats(
    asset_id: str,
    presets: list[str],
    priority: str | None = None,
) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    if not presets:
        raise ValueError("presets is required")
    if len(presets) > settings.max_batch_presets:
        raise ValueError("Too many presets")
    cache_key = _build_cache_key("batch_export", {"asset_id": asset_id, "presets": presets})
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("batch_export", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "batch_export",
        batch_export_job,
        (asset_id, presets, cache_key),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.batch_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_batch_export_social_formats(
    asset_id: str,
    presets: list[str] | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    presets = presets or settings.social_presets
    if not presets:
        raise ValueError("No social presets configured")
    cache_key = _build_cache_key(
        "batch_export_social",
        {"asset_id": asset_id, "presets": presets},
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("batch_export_social", asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "batch_export_social",
        batch_export_job,
        (asset_id, presets, cache_key),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.batch_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_campaign_process(
    asset_ids: list[str],
    presets: list[str] | None = None,
    template_name: str | None = None,
    variables: dict | None = None,
    brand_kit_id: str | None = None,
    quality: str | None = None,
    priority: str | None = None,
) -> dict:
    if not asset_ids:
        raise ValueError("asset_ids is required")
    if len(asset_ids) > settings.max_batch_assets:
        raise ValueError("Too many assets for campaign")
    for asset_id in asset_ids:
        if not get_asset(asset_id):
            raise ValueError("asset_id not found")
    if presets and len(presets) > settings.max_batch_presets:
        raise ValueError("Too many presets")
    if variables is not None and not isinstance(variables, dict):
        raise ValueError("variables must be an object")
    if brand_kit_id and not get_brand_kit(brand_kit_id):
        raise ValueError("brand_kit_id not found")

    if quality:
        quality = quality.strip().lower()
        if quality not in {"final", "draft"}:
            raise ValueError("quality must be 'final' or 'draft'")

    cache_key = _build_cache_key(
        "campaign_process",
        {
            "asset_ids": asset_ids,
            "presets": presets,
            "template_name": template_name,
            "variables": variables or {},
            "brand_kit_id": brand_kit_id,
            "quality": quality,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("campaign_process", asset_ids[0], cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "campaign_process",
        campaign_process_job,
        (asset_ids, presets, template_name, variables or {}, brand_kit_id, quality, cache_key),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.batch_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_render_social_ad(
    primary_asset_id: str,
    hook: str | None = None,
    headline: str | None = None,
    cta: str | None = None,
    price: str | None = None,
    brand_kit_id: str | None = None,
    broll_asset_ids: list[str] | None = None,
    voice_asset_id: str | None = None,
    music_asset_id: str | None = None,
    captions_srt: str | None = None,
    captions_vtt: str | None = None,
    words_json: list[dict] | None = None,
    highlight_mode: str | None = None,
    include_16_9: bool | None = None,
    quality: str | None = None,
    framing_mode: str | None = None,
    caption_position: str | None = None,
    caption_font_size: int | None = None,
    caption_font_color: str | None = None,
    caption_box_color: str | None = None,
    caption_box_opacity: float | None = None,
    caption_highlight_color: str | None = None,
    caption_padding_px: int | None = None,
    caption_max_chars: int | None = None,
    caption_max_lines: int | None = None,
    caption_max_words: int | None = None,
    caption_safe_zone_bottom_px: int | None = None,
    caption_safe_zone_top_px: int | None = None,
    caption_font_name: str | None = None,
    caption_font_asset_id: str | None = None,
    audio_target_lufs: float | None = None,
    audio_lra: float | None = None,
    audio_true_peak: float | None = None,
    ducking_ratio: float | None = None,
    ducking_threshold: float | None = None,
    ducking_attack_ms: int | None = None,
    ducking_release_ms: int | None = None,
    music_gain: float | None = None,
    voice_gain: float | None = None,
    trim_silence: bool | None = None,
    trim_silence_min_sec: float | None = None,
    trim_silence_threshold_db: float | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(primary_asset_id):
        raise ValueError("primary_asset_id not found")
    if broll_asset_ids is not None and not isinstance(broll_asset_ids, list):
        raise ValueError("broll_asset_ids must be a list")
    if broll_asset_ids and len(broll_asset_ids) + 1 > settings.max_concat_clips:
        raise ValueError("Too many clips for concat")
    if broll_asset_ids:
        for asset_id in broll_asset_ids:
            if not get_asset(asset_id):
                raise ValueError("broll_asset_id not found")
    if voice_asset_id and not get_asset(voice_asset_id):
        raise ValueError("voice_asset_id not found")
    if music_asset_id and not get_asset(music_asset_id):
        raise ValueError("music_asset_id not found")
    if sum(bool(value) for value in [captions_srt, captions_vtt, words_json]) > 1:
        raise ValueError("Provide only one of captions_srt, captions_vtt, words_json")
    if words_json is not None and not isinstance(words_json, list):
        raise ValueError("words_json must be a list")
    if brand_kit_id and not get_brand_kit(brand_kit_id):
        raise ValueError("brand_kit_id not found")
    if quality:
        quality = quality.strip().lower()
        if quality not in {"final", "draft"}:
            raise ValueError("quality must be 'final' or 'draft'")
    if highlight_mode:
        highlight_mode = highlight_mode.strip().lower()
    if framing_mode:
        framing_mode = framing_mode.strip().lower()
        if framing_mode not in {"safe_pad", "crop"}:
            raise ValueError("framing_mode must be 'safe_pad' or 'crop'")

    cache_key = _build_cache_key(
        "render_social_ad",
        {
            "primary_asset_id": primary_asset_id,
            "hook": hook,
            "headline": headline,
            "cta": cta,
            "price": price,
            "brand_kit_id": brand_kit_id,
            "broll_asset_ids": broll_asset_ids or [],
            "voice_asset_id": voice_asset_id,
            "music_asset_id": music_asset_id,
            "captions_srt": captions_srt,
            "captions_vtt": captions_vtt,
            "words_json": words_json,
            "highlight_mode": highlight_mode,
            "include_16_9": include_16_9,
            "quality": quality,
            "framing_mode": framing_mode,
            "caption_position": caption_position,
            "caption_font_size": caption_font_size,
            "caption_font_color": caption_font_color,
            "caption_box_color": caption_box_color,
            "caption_box_opacity": caption_box_opacity,
            "caption_highlight_color": caption_highlight_color,
            "caption_padding_px": caption_padding_px,
            "caption_max_chars": caption_max_chars,
            "caption_max_lines": caption_max_lines,
            "caption_max_words": caption_max_words,
            "caption_safe_zone_bottom_px": caption_safe_zone_bottom_px,
            "caption_safe_zone_top_px": caption_safe_zone_top_px,
            "caption_font_name": caption_font_name,
            "caption_font_asset_id": caption_font_asset_id,
            "audio_target_lufs": audio_target_lufs,
            "audio_lra": audio_lra,
            "audio_true_peak": audio_true_peak,
            "ducking_ratio": ducking_ratio,
            "ducking_threshold": ducking_threshold,
            "ducking_attack_ms": ducking_attack_ms,
            "ducking_release_ms": ducking_release_ms,
            "music_gain": music_gain,
            "voice_gain": voice_gain,
            "trim_silence": trim_silence,
            "trim_silence_min_sec": trim_silence_min_sec,
            "trim_silence_threshold_db": trim_silence_threshold_db,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("render_social_ad", primary_asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "render_social_ad",
        render_social_ad_job,
        (
            primary_asset_id,
            hook,
            headline,
            cta,
            price,
            brand_kit_id,
            broll_asset_ids or [],
            voice_asset_id,
            music_asset_id,
            captions_srt,
            captions_vtt,
            words_json,
            highlight_mode,
            include_16_9,
            quality,
            framing_mode,
            caption_position,
            caption_font_size,
            caption_font_color,
            caption_box_color,
            caption_box_opacity,
            caption_highlight_color,
            caption_padding_px,
            caption_max_chars,
            caption_max_lines,
            caption_max_words,
            caption_safe_zone_bottom_px,
            caption_safe_zone_top_px,
            caption_font_name,
            caption_font_asset_id,
            audio_target_lufs,
            audio_lra,
            audio_true_peak,
            ducking_ratio,
            ducking_threshold,
            ducking_attack_ms,
            ducking_release_ms,
            music_gain,
            voice_gain,
            trim_silence,
            trim_silence_min_sec,
            trim_silence_threshold_db,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.batch_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_render_testimonial_clip(
    primary_asset_id: str,
    quote: str | None = None,
    author: str | None = None,
    brand_kit_id: str | None = None,
    broll_asset_ids: list[str] | None = None,
    voice_asset_id: str | None = None,
    music_asset_id: str | None = None,
    captions_srt: str | None = None,
    captions_vtt: str | None = None,
    words_json: list[dict] | None = None,
    highlight_mode: str | None = None,
    include_16_9: bool | None = None,
    quality: str | None = None,
    framing_mode: str | None = None,
    caption_position: str | None = None,
    caption_font_size: int | None = None,
    caption_font_color: str | None = None,
    caption_box_color: str | None = None,
    caption_box_opacity: float | None = None,
    caption_highlight_color: str | None = None,
    caption_padding_px: int | None = None,
    caption_max_chars: int | None = None,
    caption_max_lines: int | None = None,
    caption_max_words: int | None = None,
    caption_safe_zone_bottom_px: int | None = None,
    caption_safe_zone_top_px: int | None = None,
    caption_font_name: str | None = None,
    caption_font_asset_id: str | None = None,
    audio_target_lufs: float | None = None,
    audio_lra: float | None = None,
    audio_true_peak: float | None = None,
    ducking_ratio: float | None = None,
    ducking_threshold: float | None = None,
    ducking_attack_ms: int | None = None,
    ducking_release_ms: int | None = None,
    music_gain: float | None = None,
    voice_gain: float | None = None,
    trim_silence: bool | None = None,
    trim_silence_min_sec: float | None = None,
    trim_silence_threshold_db: float | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(primary_asset_id):
        raise ValueError("primary_asset_id not found")
    if broll_asset_ids is not None and not isinstance(broll_asset_ids, list):
        raise ValueError("broll_asset_ids must be a list")
    if broll_asset_ids and len(broll_asset_ids) + 1 > settings.max_concat_clips:
        raise ValueError("Too many clips for concat")
    if broll_asset_ids:
        for asset_id in broll_asset_ids:
            if not get_asset(asset_id):
                raise ValueError("broll_asset_id not found")
    if voice_asset_id and not get_asset(voice_asset_id):
        raise ValueError("voice_asset_id not found")
    if music_asset_id and not get_asset(music_asset_id):
        raise ValueError("music_asset_id not found")
    if sum(bool(value) for value in [captions_srt, captions_vtt, words_json]) > 1:
        raise ValueError("Provide only one of captions_srt, captions_vtt, words_json")
    if words_json is not None and not isinstance(words_json, list):
        raise ValueError("words_json must be a list")
    if brand_kit_id and not get_brand_kit(brand_kit_id):
        raise ValueError("brand_kit_id not found")
    if quality:
        quality = quality.strip().lower()
        if quality not in {"final", "draft"}:
            raise ValueError("quality must be 'final' or 'draft'")
    if highlight_mode:
        highlight_mode = highlight_mode.strip().lower()
    if framing_mode:
        framing_mode = framing_mode.strip().lower()
        if framing_mode not in {"safe_pad", "crop"}:
            raise ValueError("framing_mode must be 'safe_pad' or 'crop'")

    cache_key = _build_cache_key(
        "render_testimonial_clip",
        {
            "primary_asset_id": primary_asset_id,
            "quote": quote,
            "author": author,
            "brand_kit_id": brand_kit_id,
            "broll_asset_ids": broll_asset_ids or [],
            "voice_asset_id": voice_asset_id,
            "music_asset_id": music_asset_id,
            "captions_srt": captions_srt,
            "captions_vtt": captions_vtt,
            "words_json": words_json,
            "highlight_mode": highlight_mode,
            "include_16_9": include_16_9,
            "quality": quality,
            "framing_mode": framing_mode,
            "caption_position": caption_position,
            "caption_font_size": caption_font_size,
            "caption_font_color": caption_font_color,
            "caption_box_color": caption_box_color,
            "caption_box_opacity": caption_box_opacity,
            "caption_highlight_color": caption_highlight_color,
            "caption_padding_px": caption_padding_px,
            "caption_max_chars": caption_max_chars,
            "caption_max_lines": caption_max_lines,
            "caption_max_words": caption_max_words,
            "caption_safe_zone_bottom_px": caption_safe_zone_bottom_px,
            "caption_safe_zone_top_px": caption_safe_zone_top_px,
            "caption_font_name": caption_font_name,
            "caption_font_asset_id": caption_font_asset_id,
            "audio_target_lufs": audio_target_lufs,
            "audio_lra": audio_lra,
            "audio_true_peak": audio_true_peak,
            "ducking_ratio": ducking_ratio,
            "ducking_threshold": ducking_threshold,
            "ducking_attack_ms": ducking_attack_ms,
            "ducking_release_ms": ducking_release_ms,
            "music_gain": music_gain,
            "voice_gain": voice_gain,
            "trim_silence": trim_silence,
            "trim_silence_min_sec": trim_silence_min_sec,
            "trim_silence_threshold_db": trim_silence_threshold_db,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("render_testimonial_clip", primary_asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "render_testimonial_clip",
        render_testimonial_clip_job,
        (
            primary_asset_id,
            quote,
            author,
            brand_kit_id,
            broll_asset_ids or [],
            voice_asset_id,
            music_asset_id,
            captions_srt,
            captions_vtt,
            words_json,
            highlight_mode,
            include_16_9,
            quality,
            framing_mode,
            caption_position,
            caption_font_size,
            caption_font_color,
            caption_box_color,
            caption_box_opacity,
            caption_highlight_color,
            caption_padding_px,
            caption_max_chars,
            caption_max_lines,
            caption_max_words,
            caption_safe_zone_bottom_px,
            caption_safe_zone_top_px,
            caption_font_name,
            caption_font_asset_id,
            audio_target_lufs,
            audio_lra,
            audio_true_peak,
            ducking_ratio,
            ducking_threshold,
            ducking_attack_ms,
            ducking_release_ms,
            music_gain,
            voice_gain,
            trim_silence,
            trim_silence_min_sec,
            trim_silence_threshold_db,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.batch_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_render_offer_card(
    primary_asset_id: str,
    headline: str | None = None,
    price: str | None = None,
    cta: str | None = None,
    brand_kit_id: str | None = None,
    broll_asset_ids: list[str] | None = None,
    voice_asset_id: str | None = None,
    music_asset_id: str | None = None,
    captions_srt: str | None = None,
    captions_vtt: str | None = None,
    words_json: list[dict] | None = None,
    highlight_mode: str | None = None,
    include_16_9: bool | None = None,
    quality: str | None = None,
    framing_mode: str | None = None,
    caption_position: str | None = None,
    caption_font_size: int | None = None,
    caption_font_color: str | None = None,
    caption_box_color: str | None = None,
    caption_box_opacity: float | None = None,
    caption_highlight_color: str | None = None,
    caption_padding_px: int | None = None,
    caption_max_chars: int | None = None,
    caption_max_lines: int | None = None,
    caption_max_words: int | None = None,
    caption_safe_zone_bottom_px: int | None = None,
    caption_safe_zone_top_px: int | None = None,
    caption_font_name: str | None = None,
    caption_font_asset_id: str | None = None,
    audio_target_lufs: float | None = None,
    audio_lra: float | None = None,
    audio_true_peak: float | None = None,
    ducking_ratio: float | None = None,
    ducking_threshold: float | None = None,
    ducking_attack_ms: int | None = None,
    ducking_release_ms: int | None = None,
    music_gain: float | None = None,
    voice_gain: float | None = None,
    trim_silence: bool | None = None,
    trim_silence_min_sec: float | None = None,
    trim_silence_threshold_db: float | None = None,
    priority: str | None = None,
) -> dict:
    if not get_asset(primary_asset_id):
        raise ValueError("primary_asset_id not found")
    if broll_asset_ids is not None and not isinstance(broll_asset_ids, list):
        raise ValueError("broll_asset_ids must be a list")
    if broll_asset_ids and len(broll_asset_ids) + 1 > settings.max_concat_clips:
        raise ValueError("Too many clips for concat")
    if broll_asset_ids:
        for asset_id in broll_asset_ids:
            if not get_asset(asset_id):
                raise ValueError("broll_asset_id not found")
    if voice_asset_id and not get_asset(voice_asset_id):
        raise ValueError("voice_asset_id not found")
    if music_asset_id and not get_asset(music_asset_id):
        raise ValueError("music_asset_id not found")
    if sum(bool(value) for value in [captions_srt, captions_vtt, words_json]) > 1:
        raise ValueError("Provide only one of captions_srt, captions_vtt, words_json")
    if words_json is not None and not isinstance(words_json, list):
        raise ValueError("words_json must be a list")
    if brand_kit_id and not get_brand_kit(brand_kit_id):
        raise ValueError("brand_kit_id not found")
    if quality:
        quality = quality.strip().lower()
        if quality not in {"final", "draft"}:
            raise ValueError("quality must be 'final' or 'draft'")
    if highlight_mode:
        highlight_mode = highlight_mode.strip().lower()
    if framing_mode:
        framing_mode = framing_mode.strip().lower()
        if framing_mode not in {"safe_pad", "crop"}:
            raise ValueError("framing_mode must be 'safe_pad' or 'crop'")

    cache_key = _build_cache_key(
        "render_offer_card",
        {
            "primary_asset_id": primary_asset_id,
            "headline": headline,
            "price": price,
            "cta": cta,
            "brand_kit_id": brand_kit_id,
            "broll_asset_ids": broll_asset_ids or [],
            "voice_asset_id": voice_asset_id,
            "music_asset_id": music_asset_id,
            "captions_srt": captions_srt,
            "captions_vtt": captions_vtt,
            "words_json": words_json,
            "highlight_mode": highlight_mode,
            "include_16_9": include_16_9,
            "quality": quality,
            "framing_mode": framing_mode,
            "caption_position": caption_position,
            "caption_font_size": caption_font_size,
            "caption_font_color": caption_font_color,
            "caption_box_color": caption_box_color,
            "caption_box_opacity": caption_box_opacity,
            "caption_highlight_color": caption_highlight_color,
            "caption_padding_px": caption_padding_px,
            "caption_max_chars": caption_max_chars,
            "caption_max_lines": caption_max_lines,
            "caption_max_words": caption_max_words,
            "caption_safe_zone_bottom_px": caption_safe_zone_bottom_px,
            "caption_safe_zone_top_px": caption_safe_zone_top_px,
            "caption_font_name": caption_font_name,
            "caption_font_asset_id": caption_font_asset_id,
            "audio_target_lufs": audio_target_lufs,
            "audio_lra": audio_lra,
            "audio_true_peak": audio_true_peak,
            "ducking_ratio": ducking_ratio,
            "ducking_threshold": ducking_threshold,
            "ducking_attack_ms": ducking_attack_ms,
            "ducking_release_ms": ducking_release_ms,
            "music_gain": music_gain,
            "voice_gain": voice_gain,
            "trim_silence": trim_silence,
            "trim_silence_min_sec": trim_silence_min_sec,
            "trim_silence_threshold_db": trim_silence_threshold_db,
        },
    )
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("render_offer_card", primary_asset_id, cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "render_offer_card",
        render_offer_card_job,
        (
            primary_asset_id,
            headline,
            price,
            cta,
            brand_kit_id,
            broll_asset_ids or [],
            voice_asset_id,
            music_asset_id,
            captions_srt,
            captions_vtt,
            words_json,
            highlight_mode,
            include_16_9,
            quality,
            framing_mode,
            caption_position,
            caption_font_size,
            caption_font_color,
            caption_box_color,
            caption_box_opacity,
            caption_highlight_color,
            caption_padding_px,
            caption_max_chars,
            caption_max_lines,
            caption_max_words,
            caption_safe_zone_bottom_px,
            caption_safe_zone_top_px,
            caption_font_name,
            caption_font_asset_id,
            audio_target_lufs,
            audio_lra,
            audio_true_peak,
            ducking_ratio,
            ducking_threshold,
            ducking_attack_ms,
            ducking_release_ms,
            music_gain,
            voice_gain,
            trim_silence,
            trim_silence_min_sec,
            trim_silence_threshold_db,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.batch_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_render_iterate(
    render_type: str,
    primary_asset_id: str,
    hook: str | None = None,
    headline: str | None = None,
    cta: str | None = None,
    price: str | None = None,
    quote: str | None = None,
    author: str | None = None,
    brand_kit_id: str | None = None,
    broll_asset_ids: list[str] | None = None,
    voice_asset_id: str | None = None,
    music_asset_id: str | None = None,
    captions_srt: str | None = None,
    captions_vtt: str | None = None,
    words_json: list[dict] | None = None,
    highlight_mode: str | None = None,
    include_16_9: bool | None = None,
    quality: str | None = None,
    framing_mode: str | None = None,
    caption_position: str | None = None,
    caption_font_size: int | None = None,
    caption_font_color: str | None = None,
    caption_box_color: str | None = None,
    caption_box_opacity: float | None = None,
    caption_highlight_color: str | None = None,
    caption_padding_px: int | None = None,
    caption_max_chars: int | None = None,
    caption_max_lines: int | None = None,
    caption_max_words: int | None = None,
    caption_safe_zone_bottom_px: int | None = None,
    caption_safe_zone_top_px: int | None = None,
    caption_font_name: str | None = None,
    caption_font_asset_id: str | None = None,
    audio_target_lufs: float | None = None,
    audio_lra: float | None = None,
    audio_true_peak: float | None = None,
    ducking_ratio: float | None = None,
    ducking_threshold: float | None = None,
    ducking_attack_ms: int | None = None,
    ducking_release_ms: int | None = None,
    music_gain: float | None = None,
    voice_gain: float | None = None,
    trim_silence: bool | None = None,
    trim_silence_min_sec: float | None = None,
    trim_silence_threshold_db: float | None = None,
    rubric_name: str | None = None,
    pass_threshold: float | None = None,
    max_iterations: int | None = None,
    priority: str | None = None,
) -> dict:
    if not render_type:
        raise ValueError("render_type is required")
    render_type = render_type.strip().lower()
    if render_type not in {"social_ad", "testimonial_clip", "offer_card"}:
        raise ValueError("render_type must be social_ad, testimonial_clip, or offer_card")
    if not get_asset(primary_asset_id):
        raise ValueError("primary_asset_id not found")
    if broll_asset_ids is not None and not isinstance(broll_asset_ids, list):
        raise ValueError("broll_asset_ids must be a list")
    if broll_asset_ids and len(broll_asset_ids) + 1 > settings.max_concat_clips:
        raise ValueError("Too many clips for concat")
    if broll_asset_ids:
        for asset_id in broll_asset_ids:
            if not get_asset(asset_id):
                raise ValueError("broll_asset_id not found")
    if voice_asset_id and not get_asset(voice_asset_id):
        raise ValueError("voice_asset_id not found")
    if music_asset_id and not get_asset(music_asset_id):
        raise ValueError("music_asset_id not found")
    if sum(bool(value) for value in [captions_srt, captions_vtt, words_json]) > 1:
        raise ValueError("Provide only one of captions_srt, captions_vtt, words_json")
    if words_json is not None and not isinstance(words_json, list):
        raise ValueError("words_json must be a list")
    if brand_kit_id and not get_brand_kit(brand_kit_id):
        raise ValueError("brand_kit_id not found")
    if quality:
        quality = quality.strip().lower()
        if quality not in {"final", "draft"}:
            raise ValueError("quality must be 'final' or 'draft'")
    if highlight_mode:
        highlight_mode = highlight_mode.strip().lower()
    if framing_mode:
        framing_mode = framing_mode.strip().lower()
        if framing_mode not in {"safe_pad", "crop"}:
            raise ValueError("framing_mode must be 'safe_pad' or 'crop'")
    if rubric_name:
        describe_rubric(rubric_name)
    if max_iterations is not None and int(max_iterations) <= 0:
        raise ValueError("max_iterations must be > 0")

    cache_key = _build_cache_key(
        "render_iterate",
        {
            "render_type": render_type,
            "primary_asset_id": primary_asset_id,
            "hook": hook,
            "headline": headline,
            "cta": cta,
            "price": price,
            "quote": quote,
            "author": author,
            "brand_kit_id": brand_kit_id,
            "broll_asset_ids": broll_asset_ids or [],
            "voice_asset_id": voice_asset_id,
            "music_asset_id": music_asset_id,
            "captions_srt": captions_srt,
            "captions_vtt": captions_vtt,
            "words_json": words_json,
            "highlight_mode": highlight_mode,
            "include_16_9": include_16_9,
            "quality": quality,
            "framing_mode": framing_mode,
            "caption_position": caption_position,
            "caption_font_size": caption_font_size,
            "caption_font_color": caption_font_color,
            "caption_box_color": caption_box_color,
            "caption_box_opacity": caption_box_opacity,
            "caption_highlight_color": caption_highlight_color,
            "caption_padding_px": caption_padding_px,
            "caption_max_chars": caption_max_chars,
            "caption_max_lines": caption_max_lines,
            "caption_max_words": caption_max_words,
            "caption_safe_zone_bottom_px": caption_safe_zone_bottom_px,
            "caption_safe_zone_top_px": caption_safe_zone_top_px,
            "caption_font_name": caption_font_name,
            "caption_font_asset_id": caption_font_asset_id,
            "audio_target_lufs": audio_target_lufs,
            "audio_lra": audio_lra,
            "audio_true_peak": audio_true_peak,
            "ducking_ratio": ducking_ratio,
            "ducking_threshold": ducking_threshold,
            "ducking_attack_ms": ducking_attack_ms,
            "ducking_release_ms": ducking_release_ms,
            "music_gain": music_gain,
            "voice_gain": voice_gain,
            "trim_silence": trim_silence,
            "trim_silence_min_sec": trim_silence_min_sec,
            "trim_silence_threshold_db": trim_silence_threshold_db,
            "rubric_name": rubric_name,
            "pass_threshold": pass_threshold,
            "max_iterations": max_iterations,
        },
    )
    cached_payload = _resolve_cached_payload(cache_key)
    if cached_payload and cached_payload.get("result"):
        result = cached_payload.get("result")
        output_ids = cached_payload.get("output_asset_ids") or []
        job_id = _record_cached_job(
            "render_iterate",
            primary_asset_id,
            list(output_ids),
            cache_key,
            extra={"result": result},
        )
        return {"job_id": job_id, "cache_hit": True, "result": result}
    job_id = _enqueue_job(
        "render_iterate",
        render_iterate_job,
        (
            render_type,
            primary_asset_id,
            hook,
            headline,
            cta,
            price,
            quote,
            author,
            brand_kit_id,
            broll_asset_ids or [],
            voice_asset_id,
            music_asset_id,
            captions_srt,
            captions_vtt,
            words_json,
            highlight_mode,
            include_16_9,
            quality,
            framing_mode,
            caption_position,
            caption_font_size,
            caption_font_color,
            caption_box_color,
            caption_box_opacity,
            caption_highlight_color,
            caption_padding_px,
            caption_max_chars,
            caption_max_lines,
            caption_max_words,
            caption_safe_zone_bottom_px,
            caption_safe_zone_top_px,
            caption_font_name,
            caption_font_asset_id,
            audio_target_lufs,
            audio_lra,
            audio_true_peak,
            ducking_ratio,
            ducking_threshold,
            ducking_attack_ms,
            ducking_release_ms,
            music_gain,
            voice_gain,
            trim_silence,
            trim_silence_min_sec,
            trim_silence_threshold_db,
            rubric_name,
            pass_threshold,
            max_iterations,
            cache_key,
        ),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.batch_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_workflow_run(workflow: dict, priority: str | None = None) -> dict:
    if not isinstance(workflow, dict):
        raise ValueError("workflow must be an object")
    cache_key = _build_cache_key("workflow_run", workflow)
    cached_outputs = _resolve_cached_outputs(cache_key)
    if cached_outputs:
        job_id = _record_cached_job("workflow_run", "", cached_outputs, cache_key)
        return {"job_id": job_id, "cache_hit": True, "output_asset_ids": cached_outputs}
    job_id = _enqueue_job(
        "workflow_run",
        workflow_job,
        (workflow, cache_key),
        cache_key=cache_key,
        priority=priority,
        job_timeout=settings.workflow_timeout_seconds(),
    )
    return {"job_id": job_id, "cache_hit": False}


async def tool_list_presets() -> dict:
    return {"presets": list_presets()}


async def tool_describe_preset(name: str) -> dict:
    if not name:
        raise ValueError("preset name is required")
    return {"preset": describe_preset(name)}


async def tool_rubric_list() -> dict:
    return {"rubrics": list_rubrics()}


async def tool_rubric_describe(name: str) -> dict:
    if not name:
        raise ValueError("rubric name is required")
    return {"rubric": describe_rubric(name)}


async def tool_capabilities() -> dict:
    presets = list_presets()
    templates = list_templates()
    rubrics = list_rubrics()
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
            "ffmpeg_text_timeout_seconds": settings.text_timeout_seconds(),
            "ffmpeg_logo_timeout_seconds": settings.logo_timeout_seconds(),
            "ffmpeg_concat_timeout_seconds": settings.concat_timeout_seconds(),
            "ffmpeg_image_timeout_seconds": settings.image_timeout_seconds(),
            "ffmpeg_slideshow_timeout_seconds": settings.slideshow_timeout_seconds(),
            "ffmpeg_audio_timeout_seconds": settings.audio_timeout_seconds(),
            "ffmpeg_template_timeout_seconds": settings.template_timeout_seconds(),
            "ffmpeg_workflow_timeout_seconds": settings.workflow_timeout_seconds(),
            "ffmpeg_batch_timeout_seconds": settings.batch_timeout_seconds(),
            "download_url_ttl_seconds": settings.download_url_ttl_seconds,
            "discord_max_upload_bytes": settings.discord_max_upload_bytes,
            "max_text_chars": settings.max_text_chars,
            "min_font_size": settings.min_font_size,
            "max_font_size": settings.max_font_size,
            "max_box_border_width": settings.max_box_border_width,
            "overlay_margin_px": settings.overlay_margin_px,
            "logo_min_scale_pct": settings.logo_min_scale_pct,
            "logo_max_scale_pct": settings.logo_max_scale_pct,
            "logo_max_opacity": settings.logo_max_opacity,
            "default_video_fps": settings.default_video_fps,
            "default_image_duration_sec": settings.default_image_duration_sec,
            "default_image_width": settings.default_image_width,
            "default_image_height": settings.default_image_height,
            "max_concat_clips": settings.max_concat_clips,
            "max_slideshow_images": settings.max_slideshow_images,
            "max_audio_tracks": settings.max_audio_tracks,
            "max_template_layers": settings.max_template_layers,
            "max_template_text_layers": settings.max_template_text_layers,
            "max_workflow_nodes": settings.max_workflow_nodes,
            "max_batch_assets": settings.max_batch_assets,
            "max_batch_presets": settings.max_batch_presets,
            "max_caption_segments": settings.max_caption_segments,
            "caption_max_chars": settings.caption_max_chars,
            "caption_max_lines": settings.caption_max_lines,
            "caption_max_words": settings.caption_max_words,
            "caption_line_spacing": settings.caption_line_spacing,
            "caption_font_size": settings.caption_font_size,
            "caption_position": settings.caption_position,
            "caption_text_color": settings.caption_text_color,
            "caption_box_color": settings.caption_box_color,
            "caption_box_opacity": settings.caption_box_opacity,
            "caption_highlight_color": settings.caption_highlight_color,
            "caption_padding_px": settings.caption_padding_px,
            "caption_safe_zone_bottom_px": settings.caption_safe_zone_bottom_px,
            "caption_safe_zone_top_px": settings.caption_safe_zone_top_px,
            "draft_max_dimension": settings.draft_max_dimension,
            "draft_crf": settings.draft_crf,
            "draft_preset": settings.draft_preset,
            "draft_audio_bitrate": settings.draft_audio_bitrate,
            "draft_watermark_enabled": settings.draft_watermark_enabled,
            "draft_watermark_text": settings.draft_watermark_text,
            "draft_watermark_opacity": settings.draft_watermark_opacity,
            "draft_watermark_font_size": settings.draft_watermark_font_size,
            "audio_norm_i": settings.audio_norm_i,
            "audio_norm_lra": settings.audio_norm_lra,
            "audio_norm_tp": settings.audio_norm_tp,
            "audio_sample_rate": settings.audio_sample_rate,
            "audio_min_silence_sec": settings.audio_min_silence_sec,
            "audio_silence_db": settings.audio_silence_db,
            "audio_fade_default_sec": settings.audio_fade_default_sec,
            "audio_ducking_ratio": settings.ducking_ratio,
            "audio_ducking_threshold": settings.ducking_threshold,
            "audio_ducking_attack_ms": settings.ducking_attack_ms,
            "audio_ducking_release_ms": settings.ducking_release_ms,
            "audio_ducking_music_gain": settings.ducking_music_gain,
        },
        "allowlist": {
            "domains": settings.allowed_domains,
            "content_types": settings.allowed_content_types,
            "allow_image_ingest": settings.allow_image_ingest,
        },
        "storage": {
            "backend": settings.storage_backend,
        },
        "queue": {
            "queue_name": settings.queue_name,
            "queue_names": settings.queue_names(),
            "job_timeout_seconds": settings.ffmpeg_timeout_seconds + 60,
            "worker_concurrency": 1,
        },
        "cache": {
            "enabled": True,
            "default_ttl_seconds": settings.asset_ttl_seconds(),
            "strategy": "completed-job reuse + layer caching",
        },
        "supported_inputs": ["video/*", "audio/*", "image/*"] if settings.allow_image_ingest else ["video/*", "audio/*"],
        "output_containers": output_containers,
        "presets": presets,
        "templates": templates,
        "rubrics": rubrics,
        "social_presets": settings.social_presets,
    }


async def tool_job_progress(job_id: str) -> dict:
    if not job_id:
        raise ValueError("job_id is required")
    job_record = get_job(job_id) or {}
    if not job_record:
        return {
            "job_id": job_id,
            "status": "unknown",
            "phase": "unknown",
            "progress_pct": None,
        }

    synced = _sync_job_status(job_id, job_record)
    status = synced.get("status") or "unknown"
    progress = synced.get("progress")
    if progress is None:
        progress = 0 if status == "queued" else 50 if status == "running" else 100
    return {
        "job_id": job_id,
        "status": status,
        "phase": status,
        "progress_pct": progress,
    }


async def tool_job_logs(job_id: str) -> dict:
    if not job_id:
        raise ValueError("job_id is required")
    job_record = get_job(job_id) or {}
    if not job_record:
        return {
            "job_id": job_id,
            "status": "unknown",
            "logs_short": None,
            "last_log_line": None,
            "error": None,
        }

    synced = _sync_job_status(job_id, job_record)
    logs_short = synced.get("logs_short")
    return {
        "job_id": job_id,
        "status": synced.get("status"),
        "logs_short": logs_short,
        "last_log_line": _last_log_line(logs_short),
        "error": synced.get("error"),
    }


async def tool_metrics_snapshot() -> dict:
    queue_depth: dict[str, int] = {}
    for name in settings.queue_names():
        queue = Queue(name, connection=get_redis())
        depth = queue.count
        if callable(depth):
            depth = depth()
        queue_depth[name] = int(depth)
    snapshot = collect_metrics_snapshot()
    snapshot["queue_depth"] = queue_depth
    return snapshot


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
        "report": synced.get("report"),
        "ranking": synced.get("ranking"),
        "result": synced.get("result"),
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
    "video_add_text": tool_video_add_text,
    "video_add_logo": tool_video_add_logo,
    "captions_burn_in": tool_captions_burn_in,
    "video_analyze": tool_video_analyze,
    "asset_compare": tool_asset_compare,
    "video_concat": tool_video_concat,
    "image_to_video": tool_image_to_video,
    "images_to_slideshow": tool_images_to_slideshow,
    "images_to_slideshow_ken_burns": tool_images_to_slideshow_ken_burns,
    "audio_normalize": tool_audio_normalize,
    "audio_mix": tool_audio_mix,
    "audio_duck": tool_audio_duck,
    "audio_mix_with_background": tool_audio_mix_with_background,
    "audio_fade": tool_audio_fade,
    "audio_trim_silence": tool_audio_trim_silence,
    "template_list": tool_template_list,
    "template_describe": tool_template_describe,
    "template_apply": tool_template_apply,
    "brand_kit_upsert": tool_brand_kit_upsert,
    "brand_kit_get": tool_brand_kit_get,
    "brand_kit_list": tool_brand_kit_list,
    "brand_kit_delete": tool_brand_kit_delete,
    "brand_kit_apply": tool_brand_kit_apply,
    "batch_export_formats": tool_batch_export_formats,
    "batch_export_social_formats": tool_batch_export_social_formats,
    "campaign_process": tool_campaign_process,
    "render_social_ad": tool_render_social_ad,
    "render_testimonial_clip": tool_render_testimonial_clip,
    "render_offer_card": tool_render_offer_card,
    "render_iterate": tool_render_iterate,
    "workflow_run": tool_workflow_run,
    "ffmpeg_list_presets": tool_list_presets,
    "ffmpeg_describe_preset": tool_describe_preset,
    "rubric_list": tool_rubric_list,
    "rubric_describe": tool_rubric_describe,
    "ffmpeg_capabilities": tool_capabilities,
    "job_status": tool_job_status,
    "job_progress": tool_job_progress,
    "job_logs": tool_job_logs,
    "metrics_snapshot": tool_metrics_snapshot,
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
