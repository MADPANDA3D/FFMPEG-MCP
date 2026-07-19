import asyncio
import contextvars
import functools
import hmac
import inspect
import json
import logging
import math
import os
import re
import shutil
import time
import uuid
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any, Literal, overload
from urllib.parse import parse_qsl, urlsplit

import aiofiles
import uvicorn
from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from rq import Queue
from rq.exceptions import NoSuchJobError
from rq.job import Job, JobStatus

from . import __version__
from .brand_kits import sanitize_brand_kit
from .captions import SAFE_ZONE_PROFILES
from .cleanup import cleanup_loop
from .config import settings
from .discord_export import DiscordExportError, send_file
from .drive_utils import DriveError, upload_file
from .ffprobe_utils import run_ffprobe
from .ingest import IngestError, ingest_from_url
from .jobs import (
    ITERATE_STRATEGIES,
    asset_compare_job,
    audio_duck_job,
    audio_fade_job,
    audio_mix_job,
    audio_mix_with_background_job,
    audio_normalize_job,
    audio_trim_silence_job,
    batch_export_job,
    brand_kit_apply_job,
    campaign_process_job,
    captions_burn_in_job,
    execute_tenant_job,
    extract_audio_job,
    image_to_video_job,
    images_to_slideshow_job,
    images_to_slideshow_ken_burns_job,
    release_tenant_job_admission_callback,
    render_iterate_job,
    render_offer_card_job,
    render_social_ad_job,
    render_testimonial_clip_job,
    template_apply_job,
    thumbnail_job,
    transcode_job,
    trim_job,
    video_add_logo_job,
    video_add_text_job,
    video_analyze_job,
    video_concat_job,
    workflow_job,
)
from .media_limits import kind_from_mime_type, validate_media_probe
from .metrics import collect_metrics_snapshot, log_event, record_cache_hit, record_cache_miss
from .overlay_utils import (
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
from .presets import describe_preset, get_preset, list_presets
from .public_limits import (
    finite_number,
    validate_analysis_plan,
    validate_audio_controls,
    validate_batch_operations,
    validate_brand_kit_request,
    validate_campaign_plan,
    validate_caption_sources,
    validate_concat_asset_plan,
    validate_dimensions,
    validate_image_video_plan,
    validate_preset_list,
    validate_preset_request,
    validate_render_request,
    validate_slideshow_plan,
    validate_template_asset_plan,
    validate_thumbnail_request,
    validate_trim_request,
    validate_workflow,
)
from .redis_store import (
    JobAdmissionError,
    build_cache_key,
    delete_brand_kit,
    delete_cached_result,
    delete_job,
    get_asset,
    get_brand_kit,
    get_cached_result,
    get_job,
    get_redis,
    get_rq_redis,
    get_signed_download_asset,
    list_brand_kits,
    release_job_admission,
    reserve_job_admission,
    rollback_job_admission,
    save_brand_kit,
    save_job,
    update_asset,
    update_job,
)
from .rubrics import describe_rubric, get_rubric, list_rubrics, qa_from_report
from .storage import (
    download_to_temp_async,
    generate_download_url_async,
    local_path_from_key,
    run_asgi_storage_call,
    verify_local_signature,
)
from .task_queue import get_queue
from .templates import describe_template, list_templates
from .tenant import hash_principal, require_owner_hash, tenant_context
from .tool_manifest import (
    DOCUMENTATION_URL as MANIFEST_DOCUMENTATION_URL,
)
from .tool_manifest import (
    build_tool_manifest,
    resolve_tool_descriptor,
    search_tool_manifest,
)
from .utils import utc_now_iso, utc_now_ts

if settings.log_requests or settings.log_structured:
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

mcp = FastMCP(
    name="ffmpeg-mcp",
    stateless_http=True,
    json_response=True,
    host="0.0.0.0",
)
# FastMCP does not expose the low-level Server version in its constructor.
mcp._mcp_server.version = __version__

TOOL_MODE = "individual"

REQUEST_CONTEXT: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "request_context", default=None
)

SYNC_TOOL_NAMES = {
    "brand_kit_delete",
    "brand_kit_get",
    "brand_kit_list",
    "brand_kit_upsert",
    "ffmpeg_capabilities",
    "ffmpeg_describe_preset",
    "ffmpeg_list_presets",
    "find_tools",
    "get_endpoint_coverage",
    "get_tool_usage",
    "job_logs",
    "job_progress",
    "job_status",
    "media_export_to_discord",
    "media_export_to_drive",
    "media_get_download_url",
    "media_ingest_from_drive",
    "media_ingest_from_url",
    "media_probe",
    "metrics_snapshot",
    "check_configuration",
    "list_capabilities",
    "rubric_describe",
    "rubric_list",
    "template_describe",
    "template_list",
}


def _safe_parse_json(raw: bytes) -> dict | None:
    if not raw:
        return {}

    def reject_nonfinite(_value: str) -> None:
        raise ValueError("non-finite JSON number")

    try:
        payload = json.loads(raw, parse_constant=reject_nonfinite)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _extract_jsonrpc_metadata(payload: dict | None) -> tuple[str | None, str | None, str | int]:
    request_id: str | int = "server-error"
    method_name: str | None = None
    tool_name: str | None = None
    if isinstance(payload, dict):
        raw_id = payload.get("id", "server-error")
        if isinstance(raw_id, (str, int)):
            request_id = raw_id
        method_raw = payload.get("method")
        if isinstance(method_raw, str):
            method_name = method_raw
            if method_raw == "tools/call":
                params = payload.get("params")
                if isinstance(params, dict):
                    name = params.get("name")
                    if isinstance(name, str):
                        tool_name = name
    return method_name, tool_name, request_id


def _replay_receive(body: bytes):
    sent = False

    async def _inner():
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.request", "body": b"", "more_body": False}

    return _inner


async def _send_json(
    send, status: int, payload: dict, extra_headers: list[tuple[bytes, bytes]] | None = None
) -> int:
    body = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
    ]
    if extra_headers:
        headers.extend(extra_headers)
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})
    return len(body)


async def _send_jsonrpc_error(
    send,
    *,
    status: int,
    code: int,
    message: str,
    request_id: str | int,
    data: dict | None = None,
    retry_after: int | None = None,
) -> int:
    error_payload = {"code": code, "message": message}
    if data is not None:
        error_payload["data"] = data
    extra_headers = []
    if retry_after is not None and retry_after > 0:
        extra_headers.append((b"retry-after", str(retry_after).encode("ascii")))
    return await _send_json(
        send,
        status,
        {"jsonrpc": "2.0", "id": request_id, "error": error_payload},
        extra_headers=extra_headers,
    )


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
    if len(parts) != 2 or not re.fullmatch(r"[a-f0-9]{32}", parts[1]):
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

    try:
        query_pairs = parse_qsl(
            (scope.get("query_string") or b"").decode("ascii"),
            keep_blank_values=True,
            strict_parsing=True,
        )
    except (UnicodeDecodeError, ValueError):
        query_pairs = []
    exp_values = [value for key, value in query_pairs if key == "exp"]
    sig_values = [value for key, value in query_pairs if key == "sig"]
    if (
        len(query_pairs) != 2
        or len(exp_values) != 1
        or len(sig_values) != 1
        or not re.fullmatch(r"[A-Za-z0-9_-]{43}", sig_values[0])
    ):
        await send(
            {
                "type": "http.response.start",
                "status": 403,
                "headers": [(b"content-type", b"text/plain")],
            }
        )
        await send({"type": "http.response.body", "body": b"Forbidden"})
        return
    exp_raw = exp_values[0]
    sig = sig_values[0]
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

    asset = await run_asgi_storage_call(get_signed_download_asset, asset_id)
    asset_expires_at = asset.get("expires_at") if asset else None
    if (
        not asset
        or isinstance(asset_expires_at, bool)
        or not isinstance(asset_expires_at, int)
        or exp > asset_expires_at
    ):
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


def _derive_qa(job_record: dict) -> dict[str, Any]:
    qa = job_record.get("qa")
    if isinstance(qa, dict):
        if "failed_checks_codes" not in qa:
            qa = {**qa, "failed_checks_codes": []}
        return qa

    report = job_record.get("report")
    if isinstance(report, dict):
        rubric_name = report.get("rubric", {}).get("name")
        if rubric_name:
            try:
                rubric = get_rubric(rubric_name)
            except ValueError:
                rubric = None
            if rubric:
                return qa_from_report(report, rubric, report.get("target_preset"))

    ranking = job_record.get("ranking")
    if isinstance(ranking, list) and ranking:
        top_report = ranking[0].get("report")
        rubric_name = None
        if isinstance(top_report, dict):
            rubric_name = top_report.get("rubric", {}).get("name")
        if rubric_name:
            try:
                rubric = get_rubric(rubric_name)
            except ValueError:
                rubric = None
            if rubric:
                return qa_from_report(top_report, rubric, top_report.get("target_preset"))

    result = job_record.get("result")
    if isinstance(result, dict):
        best = result.get("best")
        if isinstance(best, dict):
            analysis = best.get("analysis")
            if isinstance(analysis, dict):
                rubric_name = analysis.get("rubric", {}).get("name")
                if rubric_name:
                    try:
                        rubric = get_rubric(rubric_name)
                    except ValueError:
                        rubric = None
                    if rubric:
                        return qa_from_report(analysis, rubric, analysis.get("target_preset"))

    return {
        "pass": None,
        "score": None,
        "failed_checks": [],
        "failed_checks_codes": [],
        "recommended_fix": None,
        "fingerprint": None,
    }


def _build_cache_key(job_type: str, payload: dict) -> str:
    return build_cache_key(f"ffmpeg:{job_type}", payload)


def _normalize_safe_zone_profile(profile: str | None) -> str | None:
    if not profile:
        return None
    value = profile.strip().lower()
    if value not in SAFE_ZONE_PROFILES:
        raise ValueError(
            f"safe_zone_profile must be one of: {', '.join(sorted(SAFE_ZONE_PROFILES.keys()))}"
        )
    return value


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
    owner_hash = require_owner_hash()
    now = utc_now_iso()
    job: dict[str, Any] = {
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
    reserve_job_admission(job_id, owner_hash=owner_hash)
    try:
        save_job(job, settings.job_ttl_seconds())
    except Exception:
        with suppress(Exception):
            delete_job(job_id)
        rollback_job_admission(job_id, owner_hash=owner_hash)
        raise JobAdmissionError("job queue temporarily unavailable") from None
    release_job_admission(job_id, owner_hash=owner_hash)
    with suppress(Exception):
        record_cache_hit(job_type)
    log_event(
        "cache_hit",
        {"job_type": job_type, "job_id": job_id, "input_asset_id": input_asset_id},
    )
    return job_id


def _rq_enqueue_commit_state(
    queue: Any,
    job_id: str,
    owner_hash: str,
) -> Literal["committed", "absent", "unknown"]:
    """Reconcile an enqueue acknowledgement without mutating possible work."""

    connection = getattr(queue, "connection", None)
    if connection is None:
        return "unknown"
    try:
        rq_job = Job.fetch(job_id, connection=connection)
    except NoSuchJobError:
        return "absent"
    except Exception:
        return "unknown"
    meta = getattr(rq_job, "meta", None)
    if not isinstance(meta, dict) or meta.get("owner_hash") != owner_hash:
        return "unknown"
    queue_name = getattr(queue, "name", None)
    if isinstance(queue_name, str) and getattr(rq_job, "origin", None) != queue_name:
        return "unknown"
    try:
        status = rq_job.get_status(refresh=False)
    except Exception:
        return "unknown"
    if status != JobStatus.QUEUED:
        return "committed"
    queue_key = getattr(queue, "key", None)
    intermediate_key = getattr(queue, "intermediate_queue_key", None)
    if not isinstance(queue_key, str) or not isinstance(intermediate_key, str):
        return "unknown"
    try:
        pipeline = connection.pipeline(transaction=False)
        pipeline.lpos(queue_key, job_id)
        pipeline.lpos(intermediate_key, job_id)
        queue_position, intermediate_position = pipeline.execute()
    except Exception:
        return "unknown"
    if queue_position is not None or intermediate_position is not None:
        return "committed"
    try:
        rq_job.refresh()
        if rq_job.get_status(refresh=False) != JobStatus.QUEUED:
            return "committed"
    except Exception:
        return "unknown"
    return "unknown"


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
    owner_hash = require_owner_hash()
    job: dict[str, Any] = {
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
    reserve_job_admission(job_id, owner_hash=owner_hash)
    try:
        save_job(job, settings.job_ttl_seconds())
    except Exception:
        with suppress(Exception):
            delete_job(job_id)
        rollback_job_admission(job_id, owner_hash=owner_hash)
        raise JobAdmissionError("job queue temporarily unavailable") from None
    try:
        queue.enqueue(
            execute_tenant_job,
            job_id=job_id,
            args=(func, args),
            meta={"owner_hash": owner_hash},
            description=f"{job_type}:{job_id}",
            job_timeout=(job_timeout or settings.ffmpeg_timeout_seconds) + 60,
            ttl=settings.job_ttl_seconds(),
            result_ttl=settings.job_ttl_seconds(),
            failure_ttl=settings.job_ttl_seconds(),
            on_failure=release_tenant_job_admission_callback,
            on_stopped=release_tenant_job_admission_callback,
        )
    except Exception:
        commit_state = _rq_enqueue_commit_state(queue, job_id, owner_hash)
        if commit_state == "absent":
            with suppress(Exception):
                delete_job(job_id)
            rollback_job_admission(job_id, owner_hash=owner_hash)
            raise JobAdmissionError("job queue temporarily unavailable") from None
        if commit_state == "unknown":
            log_event("job_enqueue_outcome_unknown", {"job_id": job_id, "job_type": job_type})
            raise JobAdmissionError("job queue temporarily unavailable") from None
    with suppress(Exception):
        record_cache_miss(job_type)
    log_event(
        "job_enqueued",
        {"job_type": job_type, "job_id": job_id, "priority": priority or "default"},
    )
    return job_id


def _sync_job_status(job_id: str, job_record: dict) -> dict[str, Any]:
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
    if updated.get("status") in {"success", "error"}:
        return updated
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
        if isinstance(rq_job.result, dict) and rq_job.result.get("ok") is False:
            updates["status"] = "error"
            updates["error"] = "Media processing failed"
            updates["error_code"] = str(rq_job.result.get("error_code") or "processing_error")[:64]
        updates.setdefault("progress", 100)
        if not updated.get("finished_at"):
            updates["finished_at"] = utc_now_iso()
    elif rq_status == "error":
        updates.setdefault("progress", 100)
        if not updated.get("error"):
            updates["error"] = "Media processing failed"
            updates["error_code"] = "processing_error"

    if rq_status == "running":
        stale_seconds = settings.stale_job_seconds()
        heartbeat = rq_job.last_heartbeat
        if heartbeat and isinstance(heartbeat, datetime):
            heartbeat_ts = int(heartbeat.replace(tzinfo=UTC).timestamp())
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


_DRIVE_INGEST_HOSTS = {
    "drive.google.com",
    "docs.google.com",
    "drive.usercontent.google.com",
}
_DRIVE_FILE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{10,200}$")


def _drive_file_id_from_url(url: str) -> str | None:
    try:
        parsed = urlsplit(url)
    except ValueError:
        return None
    if (parsed.hostname or "").lower() not in _DRIVE_INGEST_HOSTS:
        return None
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    candidate = query.get("id")
    if not candidate:
        parts = [part for part in parsed.path.split("/") if part]
        if "d" in parts and parts.index("d") + 1 < len(parts):
            candidate = parts[parts.index("d") + 1]
    if candidate and _DRIVE_FILE_ID_PATTERN.fullmatch(candidate):
        return candidate
    raise ValueError("Google Drive URL must contain a valid file ID")


def _require_drive_ingest_file(drive_file_id: str) -> None:
    if not settings.google_drive_ingest_enabled:
        raise ValueError("Google Drive ingest is disabled")
    if not isinstance(drive_file_id, str) or not _DRIVE_FILE_ID_PATTERN.fullmatch(drive_file_id):
        raise ValueError("drive_file_id is invalid")
    allowed = set(settings.google_drive_allowed_file_ids)
    if not allowed or drive_file_id not in allowed:
        raise ValueError("drive_file_id is not allowlisted")


async def tool_ingest_from_url(
    url: str, filename_hint: str | None = None, ttl_hours: int | None = None
) -> dict[str, Any]:
    finite_number(
        ttl_hours,
        "ttl_hours",
        minimum=1,
        maximum=settings.max_asset_ttl_hours,
        integer=True,
    )
    drive_file_id = _drive_file_id_from_url(url)
    if drive_file_id is not None:
        _require_drive_ingest_file(drive_file_id)
    try:
        asset = await ingest_from_url(url, filename_hint, ttl_hours)
    except IngestError as exc:
        raise ValueError("media ingest failed") from exc
    return {
        "asset_id": asset["asset_id"],
        "mime_type": asset.get("mime_type"),
        "size_bytes": asset.get("size_bytes"),
        "sha256": asset.get("sha256"),
        "original_filename": asset.get("original_filename"),
        "expires_at": asset.get("expires_at"),
    }


async def tool_ingest_from_drive(
    drive_file_id: str, ttl_hours: int | None = None
) -> dict[str, Any]:
    _require_drive_ingest_file(drive_file_id)
    url = f"https://drive.google.com/uc?export=download&id={drive_file_id}"
    return await tool_ingest_from_url(url, None, ttl_hours)


async def tool_probe(asset_id: str) -> dict[str, Any]:
    asset = get_asset(asset_id)
    if not asset:
        raise ValueError("asset_id not found")
    storage_key = asset.get("storage_key")
    if not storage_key:
        raise ValueError("asset storage missing")

    path = local_path_from_key(storage_key)
    if settings.storage_backend == "s3":
        path = await download_to_temp_async(storage_key)
    try:
        probe = await asyncio.to_thread(run_ffprobe, path)
        validate_media_probe(
            probe,
            expected_kind=kind_from_mime_type(asset.get("mime_type")),
        )
    finally:
        if settings.storage_backend == "s3" and os.path.exists(path):
            os.remove(path)

    update_asset(asset_id, probe)
    return probe


async def tool_transcode(asset_id: str, preset: str, priority: str | None = None) -> dict[str, Any]:
    asset = get_asset(asset_id)
    if not asset:
        raise ValueError("asset_id not found")
    validate_preset_request(asset, preset)
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
) -> dict[str, Any]:
    asset = get_asset(asset_id)
    if not asset:
        raise ValueError("asset_id not found")
    validate_thumbnail_request(asset, time_sec, width)
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
) -> dict[str, Any]:
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
) -> dict[str, Any]:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    validate_trim_request(start_sec, end_sec)
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
) -> dict[str, Any]:
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
) -> dict[str, Any]:
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
    safe_zone_profile: str | None = None,
    safe_zone_bottom_px: int | None = None,
    safe_zone_top_px: int | None = None,
    font_name: str | None = None,
    font_asset_id: str | None = None,
    priority: str | None = None,
) -> dict[str, Any]:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    if sum(bool(value) for value in [captions_srt, captions_vtt, words_json]) != 1:
        raise ValueError("Provide exactly one of captions_srt, captions_vtt, words_json")
    validate_caption_sources(locals())
    if brand_kit_id and not get_brand_kit(brand_kit_id):
        raise ValueError("brand_kit_id not found")
    if highlight_mode:
        highlight_mode = highlight_mode.strip().lower()
    safe_zone_profile = _normalize_safe_zone_profile(safe_zone_profile)

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
            "safe_zone_profile": safe_zone_profile,
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
            safe_zone_profile,
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
    reference_asset_id: str | None = None,
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
    safe_zone_profile: str | None = None,
    priority: str | None = None,
) -> dict[str, Any]:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    if sum(bool(value) for value in [captions_srt, captions_vtt, words_json]) > 1:
        raise ValueError("Provide only one of captions_srt, captions_vtt, words_json")
    validate_caption_sources(locals())
    if brand_kit_id and not get_brand_kit(brand_kit_id):
        raise ValueError("brand_kit_id not found")
    if target_preset:
        get_preset(target_preset)
    if rubric_name:
        describe_rubric(rubric_name)
    if reference_asset_id and not get_asset(reference_asset_id):
        raise ValueError("reference_asset_id not found")
    validate_analysis_plan(1 + int(reference_asset_id is not None))
    safe_zone_profile = _normalize_safe_zone_profile(safe_zone_profile)

    cache_key = _build_cache_key(
        "video_analyze",
        {
            "asset_id": asset_id,
            "rubric_name": rubric_name,
            "target_preset": target_preset,
            "reference_asset_id": reference_asset_id,
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
            "safe_zone_profile": safe_zone_profile,
        },
    )
    cached_payload = _resolve_cached_payload(cache_key)
    if cached_payload and cached_payload.get("report"):
        report = cached_payload.get("report")
        output_ids = cached_payload.get("output_asset_ids") or [asset_id]
        extra = {"report": report}
        qa = cached_payload.get("qa")
        if not qa and isinstance(report, dict):
            qa = report.get("qa")
        if qa:
            extra["qa"] = qa
        job_id = _record_cached_job(
            "video_analyze",
            asset_id,
            list(output_ids),
            cache_key,
            extra=extra,
        )
        return {"job_id": job_id, "cache_hit": True, "report": report}
    job_id = _enqueue_job(
        "video_analyze",
        video_analyze_job,
        (
            asset_id,
            rubric_name,
            target_preset,
            reference_asset_id,
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
            safe_zone_profile,
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
) -> dict[str, Any]:
    if not asset_ids:
        raise ValueError("asset_ids is required")
    if len(asset_ids) > settings.max_batch_assets:
        raise ValueError("Too many assets to compare")
    validate_analysis_plan(len(asset_ids))
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
        extra = {"ranking": ranking}
        if cached_payload.get("qa"):
            extra["qa"] = cached_payload.get("qa")
        job_id = _record_cached_job(
            "asset_compare",
            asset_ids[0],
            list(output_ids),
            cache_key,
            extra=extra,
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
) -> dict[str, Any]:
    if not asset_ids or len(asset_ids) < 2:
        raise ValueError("asset_ids must contain at least two items")
    if len(asset_ids) > settings.max_concat_clips:
        raise ValueError("Too many clips for concat")
    validate_batch_operations(len(asset_ids), "concat")
    validate_dimensions(target_width, target_height, field="concat")
    finite_number(
        transition_duration,
        "transition_duration",
        minimum=0,
        maximum=settings.max_duration_seconds,
    )
    assets = []
    for asset_id in asset_ids:
        asset = get_asset(asset_id)
        if not asset:
            raise ValueError("asset_id not found")
        assets.append(asset)
    validate_concat_asset_plan(
        assets,
        target_width=target_width,
        target_height=target_height,
        transition=transition,
        transition_duration=transition_duration,
    )

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
) -> dict[str, Any]:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    duration_sec = duration_sec if duration_sec is not None else settings.default_image_duration_sec
    validate_image_video_plan(
        duration=duration_sec,
        width=width,
        height=height,
        fps=fps,
    )
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
) -> dict[str, Any]:
    if not asset_ids:
        raise ValueError("asset_ids is required")
    if len(asset_ids) > settings.max_slideshow_images:
        raise ValueError("Too many images for slideshow")
    validate_slideshow_plan(len(asset_ids), duration_per_image, durations, width, height, fps)
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
) -> dict[str, Any]:
    if not asset_ids:
        raise ValueError("asset_ids is required")
    if len(asset_ids) > settings.max_slideshow_images:
        raise ValueError("Too many images for slideshow")
    validate_slideshow_plan(len(asset_ids), duration_per_image, durations, width, height, fps)
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
) -> dict[str, Any]:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    validate_audio_controls(locals())
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
) -> dict[str, Any]:
    if not asset_ids:
        raise ValueError("asset_ids is required")
    if len(asset_ids) > settings.max_audio_tracks:
        raise ValueError("Too many audio tracks")
    validate_audio_controls(locals())
    validate_batch_operations(len(asset_ids), "audio mix")
    if volumes is not None and len(volumes) != len(asset_ids):
        raise ValueError("volumes length must match asset_ids length")
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
) -> dict[str, Any]:
    if not get_asset(voice_asset_id):
        raise ValueError("voice_asset_id not found")
    if not get_asset(music_asset_id):
        raise ValueError("music_asset_id not found")
    validate_audio_controls(locals())
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
) -> dict[str, Any]:
    if not get_asset(voice_asset_id):
        raise ValueError("voice_asset_id not found")
    if not get_asset(music_asset_id):
        raise ValueError("music_asset_id not found")
    validate_audio_controls(locals())
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
        job_id = _record_cached_job(
            "audio_mix_with_background", voice_asset_id, cached_outputs, cache_key
        )
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
) -> dict[str, Any]:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    validate_audio_controls(locals())
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
) -> dict[str, Any]:
    if not get_asset(asset_id):
        raise ValueError("asset_id not found")
    validate_audio_controls(locals())
    if trim_leading is False and trim_trailing is False:
        raise ValueError("trim_leading or trim_trailing must be true")
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


async def tool_template_list() -> dict[str, Any]:
    return {"templates": list_templates()}


async def tool_template_describe(name: str) -> dict[str, Any]:
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
) -> dict[str, Any]:
    asset = get_asset(asset_id)
    if not asset:
        raise ValueError("asset_id not found")
    if not template_name:
        raise ValueError("template_name is required")
    if variables is not None and not isinstance(variables, dict):
        raise ValueError("variables must be an object")
    validate_template_asset_plan(asset, template_name, variables)
    if brand_kit_id and not get_brand_kit(brand_kit_id):
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


async def tool_brand_kit_upsert(brand_kit: dict) -> dict[str, Any]:
    validate_brand_kit_request(brand_kit)
    kit = sanitize_brand_kit(brand_kit)
    existing = get_brand_kit(kit["brand_kit_id"])
    if existing and existing.get("created_at"):
        kit["created_at"] = existing["created_at"]
    else:
        kit["created_at"] = utc_now_iso()
    kit["updated_at"] = utc_now_iso()
    save_brand_kit(kit)
    return {"brand_kit": kit}


async def tool_brand_kit_get(brand_kit_id: str) -> dict[str, Any]:
    if not brand_kit_id:
        raise ValueError("brand_kit_id is required")
    kit = get_brand_kit(brand_kit_id)
    if not kit:
        raise ValueError("brand_kit_id not found")
    return {"brand_kit": kit}


async def tool_brand_kit_list() -> dict[str, Any]:
    ids = list_brand_kits()
    return {"brand_kit_ids": sorted(ids)}


async def tool_brand_kit_delete(brand_kit_id: str, confirmation: str) -> dict[str, Any]:
    if not brand_kit_id:
        raise ValueError("brand_kit_id is required")
    if confirmation != "DELETE BRAND KIT":
        raise ValueError("confirmation must exactly match DELETE BRAND KIT")
    delete_brand_kit(brand_kit_id)
    return {"deleted": True}


async def tool_brand_kit_apply(
    asset_id: str,
    brand_kit_id: str,
    text: str | None = None,
    position: str | None = None,
    priority: str | None = None,
) -> dict[str, Any]:
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
) -> dict[str, Any]:
    asset = get_asset(asset_id)
    if not asset:
        raise ValueError("asset_id not found")
    presets = validate_preset_list(presets)
    for preset in presets:
        validate_preset_request(asset, preset)
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
) -> dict[str, Any]:
    asset = get_asset(asset_id)
    if not asset:
        raise ValueError("asset_id not found")
    presets = presets or settings.social_presets
    presets = validate_preset_list(presets, field="social presets")
    for preset in presets:
        validate_preset_request(asset, preset)
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
) -> dict[str, Any]:
    if not asset_ids:
        raise ValueError("asset_ids is required")
    if len(asset_ids) > settings.max_batch_assets:
        raise ValueError("Too many assets for campaign")
    assets: list[dict[str, Any]] = []
    for asset_id in asset_ids:
        asset = get_asset(asset_id)
        if not asset:
            raise ValueError("asset_id not found")
        assets.append(asset)
    if presets is not None:
        presets = validate_preset_list(presets)
    if variables is not None and not isinstance(variables, dict):
        raise ValueError("variables must be an object")
    if brand_kit_id and not get_brand_kit(brand_kit_id):
        raise ValueError("brand_kit_id not found")
    validate_campaign_plan(len(asset_ids), len(presets or []), template_name)
    for asset in assets:
        for preset in presets or []:
            validate_preset_request(asset, preset)
        if template_name:
            validate_template_asset_plan(asset, template_name, variables)

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


def _validate_render_concat_plan(
    primary_asset_id: str,
    broll_asset_ids: list[str] | None,
) -> None:
    if not broll_asset_ids:
        return
    asset_ids = [primary_asset_id, *broll_asset_ids]
    assets = []
    for asset_id in asset_ids:
        asset = get_asset(asset_id)
        if not asset:
            raise ValueError("render concat asset not found")
        assets.append(asset)
    validate_concat_asset_plan(
        assets,
        target_width=None,
        target_height=None,
        transition=None,
        transition_duration=None,
        allow_renderable_images=True,
    )


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
    safe_zone_profile: str | None = None,
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
) -> dict[str, Any]:
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
    _validate_render_concat_plan(primary_asset_id, broll_asset_ids)
    if voice_asset_id and not get_asset(voice_asset_id):
        raise ValueError("voice_asset_id not found")
    if music_asset_id and not get_asset(music_asset_id):
        raise ValueError("music_asset_id not found")
    if sum(bool(value) for value in [captions_srt, captions_vtt, words_json]) > 1:
        raise ValueError("Provide only one of captions_srt, captions_vtt, words_json")
    validate_caption_sources(locals())
    validate_render_request(locals(), iterative=False, template_name="social_ad_basic")
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
    safe_zone_profile = _normalize_safe_zone_profile(safe_zone_profile)

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
            "safe_zone_profile": safe_zone_profile,
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
            safe_zone_profile,
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
    safe_zone_profile: str | None = None,
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
) -> dict[str, Any]:
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
    _validate_render_concat_plan(primary_asset_id, broll_asset_ids)
    if voice_asset_id and not get_asset(voice_asset_id):
        raise ValueError("voice_asset_id not found")
    if music_asset_id and not get_asset(music_asset_id):
        raise ValueError("music_asset_id not found")
    if sum(bool(value) for value in [captions_srt, captions_vtt, words_json]) > 1:
        raise ValueError("Provide only one of captions_srt, captions_vtt, words_json")
    validate_caption_sources(locals())
    validate_render_request(locals(), iterative=False, template_name="testimonial_clip_basic")
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
    safe_zone_profile = _normalize_safe_zone_profile(safe_zone_profile)

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
            "safe_zone_profile": safe_zone_profile,
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
        job_id = _record_cached_job(
            "render_testimonial_clip", primary_asset_id, cached_outputs, cache_key
        )
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
            safe_zone_profile,
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
    safe_zone_profile: str | None = None,
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
) -> dict[str, Any]:
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
    _validate_render_concat_plan(primary_asset_id, broll_asset_ids)
    if voice_asset_id and not get_asset(voice_asset_id):
        raise ValueError("voice_asset_id not found")
    if music_asset_id and not get_asset(music_asset_id):
        raise ValueError("music_asset_id not found")
    if sum(bool(value) for value in [captions_srt, captions_vtt, words_json]) > 1:
        raise ValueError("Provide only one of captions_srt, captions_vtt, words_json")
    validate_caption_sources(locals())
    validate_render_request(locals(), iterative=False, template_name="offer_card_basic")
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
    safe_zone_profile = _normalize_safe_zone_profile(safe_zone_profile)

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
            "safe_zone_profile": safe_zone_profile,
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
        job_id = _record_cached_job(
            "render_offer_card", primary_asset_id, cached_outputs, cache_key
        )
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
            safe_zone_profile,
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
    safe_zone_profile: str | None = None,
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
    strategy: str | None = None,
    caption_font_size_min: int | None = None,
    caption_font_size_max: int | None = None,
    caption_box_opacity_min: float | None = None,
    caption_box_opacity_max: float | None = None,
    music_gain_min: float | None = None,
    music_gain_max: float | None = None,
    max_crop_pct: float | None = None,
    min_duration_sec: float | None = None,
    fail_fast: bool | None = None,
    lock_framing: bool | None = None,
    lock_captions: bool | None = None,
    lock_audio: bool | None = None,
    allow_trim_silence: bool | None = None,
    rubric_name: str | None = None,
    pass_threshold: float | None = None,
    max_iterations: int | None = None,
    priority: str | None = None,
) -> dict[str, Any]:
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
    _validate_render_concat_plan(primary_asset_id, broll_asset_ids)
    if voice_asset_id and not get_asset(voice_asset_id):
        raise ValueError("voice_asset_id not found")
    if music_asset_id and not get_asset(music_asset_id):
        raise ValueError("music_asset_id not found")
    if sum(bool(value) for value in [captions_srt, captions_vtt, words_json]) > 1:
        raise ValueError("Provide only one of captions_srt, captions_vtt, words_json")
    validate_caption_sources(locals())
    render_templates = {
        "social_ad": "social_ad_basic",
        "testimonial_clip": "testimonial_clip_basic",
        "offer_card": "offer_card_basic",
    }
    validate_render_request(locals(), iterative=True, template_name=render_templates[render_type])
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
    safe_zone_profile = _normalize_safe_zone_profile(safe_zone_profile)
    if rubric_name:
        describe_rubric(rubric_name)
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
            "safe_zone_profile": safe_zone_profile,
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
            "strategy": strategy,
            "caption_font_size_min": caption_font_size_min,
            "caption_font_size_max": caption_font_size_max,
            "caption_box_opacity_min": caption_box_opacity_min,
            "caption_box_opacity_max": caption_box_opacity_max,
            "music_gain_min": music_gain_min,
            "music_gain_max": music_gain_max,
            "max_crop_pct": max_crop_pct,
            "min_duration_sec": min_duration_sec,
            "fail_fast": fail_fast,
            "lock_framing": lock_framing,
            "lock_captions": lock_captions,
            "lock_audio": lock_audio,
            "allow_trim_silence": allow_trim_silence,
            "rubric_name": rubric_name,
            "pass_threshold": pass_threshold,
            "max_iterations": max_iterations,
        },
    )
    cached_payload = _resolve_cached_payload(cache_key)
    if cached_payload and cached_payload.get("result"):
        result = cached_payload.get("result")
        output_ids = cached_payload.get("output_asset_ids") or []
        extra = {"result": result}
        qa = cached_payload.get("qa")
        if not qa and isinstance(result, dict):
            qa = result.get("qa")
        if qa:
            extra["qa"] = qa
        job_id = _record_cached_job(
            "render_iterate",
            primary_asset_id,
            list(output_ids),
            cache_key,
            extra=extra,
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
            safe_zone_profile,
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
            strategy,
            caption_font_size_min,
            caption_font_size_max,
            caption_box_opacity_min,
            caption_box_opacity_max,
            music_gain_min,
            music_gain_max,
            max_crop_pct,
            min_duration_sec,
            fail_fast,
            lock_framing,
            lock_captions,
            lock_audio,
            allow_trim_silence,
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


async def tool_workflow_run(workflow: dict, priority: str | None = None) -> dict[str, Any]:
    validate_workflow(workflow, asset_resolver=get_asset)
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


async def tool_list_presets() -> dict[str, Any]:
    return {"presets": list_presets()}


async def tool_describe_preset(name: str) -> dict[str, Any]:
    if not name:
        raise ValueError("preset name is required")
    return {"preset": describe_preset(name)}


async def tool_rubric_list() -> dict[str, Any]:
    return {"rubrics": list_rubrics()}


async def tool_rubric_describe(name: str) -> dict[str, Any]:
    if not name:
        raise ValueError("rubric name is required")
    return {"rubric": describe_rubric(name)}


async def tool_capabilities() -> dict[str, Any]:
    presets = list_presets()
    templates = list_templates()
    rubrics = list_rubrics()
    output_containers = sorted(
        {
            output_container
            for preset in presets
            if isinstance((output_container := preset.get("output_container")), str)
            and output_container
        }
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
            "ingest_staging_owner_max_active": settings.ingest_staging_owner_max_active,
            "ingest_staging_global_max_active": settings.ingest_staging_global_max_active,
            "ingest_staging_owner_max_bytes": settings.ingest_staging_owner_max_bytes,
            "ingest_staging_global_max_bytes": settings.ingest_staging_global_max_bytes,
            "ingest_staging_lease_seconds": settings.ingest_staging_lease_seconds,
            "ingest_staging_heartbeat_seconds": settings.ingest_staging_heartbeat_seconds,
            "ffmpeg_timeout_seconds": settings.ffmpeg_timeout_seconds,
            "ffprobe_timeout_seconds": settings.ffprobe_timeout_seconds,
            "ffmpeg_rlimit_as_bytes": settings.ffmpeg_rlimit_as_bytes,
            "ffprobe_rlimit_as_bytes": settings.ffprobe_rlimit_as_bytes,
            "ffmpeg_rlimit_cpu_seconds": settings.ffmpeg_rlimit_cpu_seconds,
            "ffprobe_rlimit_cpu_seconds": settings.ffprobe_rlimit_cpu_seconds,
            "media_rlimit_nofile": settings.media_rlimit_nofile,
            "ffmpeg_threads": settings.ffmpeg_threads,
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
            "asset_quota_owner_max_count": settings.asset_quota_owner_max_count,
            "asset_quota_owner_max_bytes": settings.asset_quota_owner_max_bytes,
            "asset_quota_global_max_count": settings.asset_quota_global_max_count,
            "asset_quota_global_max_bytes": settings.asset_quota_global_max_bytes,
            "job_storage_max_output_count": settings.job_storage_max_output_count,
            "job_storage_max_output_bytes": settings.job_storage_max_output_bytes,
            "job_storage_max_materialize_bytes": settings.job_storage_max_materialize_bytes,
            "storage_asgi_max_concurrency": settings.storage_asgi_max_concurrency,
            "storage_asgi_admission_timeout_seconds": (
                settings.storage_asgi_admission_timeout_seconds
            ),
            "storage_asgi_operation_timeout_seconds": (
                settings.storage_asgi_operation_timeout_seconds
            ),
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
            "max_frame_width": settings.max_frame_width,
            "max_frame_height": settings.max_frame_height,
            "max_frame_pixels": settings.max_frame_pixels,
            "max_media_streams": settings.max_media_streams,
            "max_video_fps": settings.max_video_fps,
            "max_audio_channels": settings.max_audio_channels,
            "max_audio_sample_rate": settings.max_audio_sample_rate,
            "max_decoded_video_pixel_frames": settings.max_decoded_video_pixel_frames,
            "max_decoded_audio_sample_channels": settings.max_decoded_audio_sample_channels,
            "max_concat_clips": settings.max_concat_clips,
            "max_slideshow_images": settings.max_slideshow_images,
            "max_audio_tracks": settings.max_audio_tracks,
            "max_template_layers": settings.max_template_layers,
            "max_template_text_layers": settings.max_template_text_layers,
            "max_workflow_nodes": settings.max_workflow_nodes,
            "max_batch_assets": settings.max_batch_assets,
            "max_batch_presets": settings.max_batch_presets,
            "max_batch_operations": settings.max_batch_operations,
            "max_render_iterations": settings.max_render_iterations,
            "max_caption_segments": settings.max_caption_segments,
            "max_caption_word_timings": settings.max_caption_word_timings,
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
            "caption_safe_zone_profiles": sorted(SAFE_ZONE_PROFILES.keys()),
            "caption_positions": ["bottom_safe", "mid", "top"],
            "text_positions": sorted(TEXT_POSITIONS),
            "logo_positions": sorted(LOGO_POSITIONS),
            "iterate_strategies": list(ITERATE_STRATEGIES),
            "auto_caption_font_size_min": settings.auto_caption_font_size_min,
            "auto_caption_font_size_max": settings.auto_caption_font_size_max,
            "auto_caption_box_opacity_min": settings.auto_caption_box_opacity_min,
            "auto_caption_box_opacity_max": settings.auto_caption_box_opacity_max,
            "auto_music_gain_min": settings.auto_music_gain_min,
            "auto_music_gain_max": settings.auto_music_gain_max,
            "auto_max_crop_pct": settings.auto_max_crop_pct,
            "auto_min_duration_sec": settings.auto_min_duration_sec,
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
        "supported_inputs": ["video/*", "audio/*", "image/*"]
        if settings.allow_image_ingest
        else ["video/*", "audio/*"],
        "output_containers": output_containers,
        "presets": presets,
        "templates": templates,
        "rubrics": rubrics,
        "social_presets": settings.social_presets,
    }


async def tool_job_progress(job_id: str) -> dict[str, Any]:
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


async def tool_job_logs(job_id: str) -> dict[str, Any]:
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


async def tool_metrics_snapshot() -> dict[str, Any]:
    queue_depth: dict[str, int] = {}
    if settings.mcp_mode == "standalone":
        for name in settings.queue_names():
            queue = Queue(name, connection=get_rq_redis())
            depth = queue.count
            if callable(depth):
                depth = depth()
            queue_depth[name] = int(depth)
    snapshot = collect_metrics_snapshot()
    snapshot["queue_depth"] = queue_depth
    return snapshot


async def tool_job_status(job_id: str) -> dict[str, Any]:
    job_record = get_job(job_id) or {}
    if not job_record:
        return {
            "status": "unknown",
            "state": "unknown",
            "progress": None,
            "progress_pct": None,
            "output_asset_ids": None,
            "qa": {
                "pass": None,
                "score": None,
                "failed_checks": [],
                "failed_checks_codes": [],
                "recommended_fix": None,
                "fingerprint": None,
            },
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
    qa = _derive_qa(synced)
    return {
        "status": status,
        "state": state,
        "progress": progress,
        "progress_pct": progress,
        "output_asset_ids": synced.get("output_asset_ids"),
        "report": synced.get("report"),
        "ranking": synced.get("ranking"),
        "result": synced.get("result"),
        "qa": qa,
        "error": error,
        "logs_short": logs_short,
        "last_log_line": _last_log_line(logs_short),
        "error_code": _derive_error_code(error),
        "started_at": synced.get("started_at"),
        "finished_at": synced.get("finished_at"),
        "cache_hit": synced.get("cache_hit"),
    }


async def tool_get_download_url(asset_id: str) -> dict[str, Any]:
    asset = get_asset(asset_id)
    if not asset:
        raise ValueError("asset_id not found")
    storage_key = asset.get("storage_key")
    if not storage_key:
        raise ValueError("asset storage missing")
    expires_at = asset.get("expires_at")
    if expires_at and int(expires_at) <= utc_now_ts():
        raise ValueError("asset expired")
    if isinstance(expires_at, bool) or not isinstance(expires_at, int):
        raise ValueError("asset expiry is invalid")
    url, exp = await generate_download_url_async(asset_id, storage_key, expires_at)
    return {"url": url, "expires_at": exp}


async def tool_export_to_drive(
    asset_id: str,
    confirmation: str,
    folder_id: str | None = None,
) -> dict[str, Any]:
    if not settings.google_drive_export_enabled:
        raise ValueError("Google Drive export is disabled")
    if confirmation != "EXPORT TO GOOGLE DRIVE":
        raise ValueError("confirmation must exactly match EXPORT TO GOOGLE DRIVE")
    asset = get_asset(asset_id)
    if not asset:
        raise ValueError("asset_id not found")
    storage_key = asset.get("storage_key")
    if not storage_key:
        raise ValueError("asset storage missing")

    if settings.storage_backend == "s3":
        path = await download_to_temp_async(storage_key)
        cleanup = True
    else:
        path = local_path_from_key(storage_key)
        cleanup = False

    try:
        if not os.path.exists(path):
            raise ValueError("asset file missing")
        filename = asset.get("original_filename") or f"{asset_id}"
        folder = folder_id or settings.google_drive_folder_default or None
        drive_file_id = await asyncio.to_thread(
            upload_file, path, filename, asset.get("mime_type", ""), folder
        )
    except DriveError as exc:
        raise ValueError("Google Drive export failed") from exc
    finally:
        if cleanup and os.path.exists(path):
            os.remove(path)

    return {"drive_file_id": drive_file_id}


async def tool_export_to_discord(
    asset_id: str,
    channel_id: str,
    confirmation: str,
    message: str | None = None,
    filename: str | None = None,
) -> dict[str, Any]:
    if not settings.discord_export_enabled:
        raise ValueError("Discord export is disabled")
    if confirmation != "EXPORT TO DISCORD":
        raise ValueError("confirmation must exactly match EXPORT TO DISCORD")
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
        path = await download_to_temp_async(storage_key)
        cleanup = True
    else:
        path = local_path_from_key(storage_key)
        cleanup = False

    try:
        if not os.path.exists(path):
            raise ValueError("asset file missing")
        send_name = filename or asset.get("original_filename") or f"{asset_id}"
        message_id = await send_file(
            channel_id=channel_id,
            file_path=path,
            filename=send_name,
            message=message,
            mime_type=asset.get("mime_type"),
        )
    except DiscordExportError as exc:
        raise ValueError("Discord export failed") from exc
    finally:
        if cleanup and os.path.exists(path):
            os.remove(path)

    return {"message_id": message_id}


def _current_tool_manifest() -> dict[str, Any]:
    return build_tool_manifest(TOOL_REGISTRY)


def _configuration_status() -> dict[str, Any]:
    auth_configured = (
        len(settings.mcp_access_token) >= 32
        if settings.mcp_mode == "standalone"
        else len(settings.portal_grant_token) >= 32
        if settings.mcp_mode == "portal"
        else False
    )
    try:
        redis_reachable = bool(get_redis().ping())
    except Exception:
        redis_reachable = False
    required = {
        "runtime_configuration_valid": not settings.runtime_errors(),
        "access_mode_configured": settings.mcp_mode in {"standalone", "portal"},
        "active_access_secret_configured": auth_configured,
        "principal_hash_secret_configured": len(settings.principal_hash_secret) >= 32,
        "portal_subject_header_configured": bool(settings.portal_subject_header),
        "redis_configured": bool(settings.redis_url),
        "redis_reachable": redis_reachable,
        "storage_configured": bool(
            settings.storage_local_dir
            if settings.storage_backend == "local"
            else settings.s3_bucket and settings.s3_access_key and settings.s3_secret_key
        ),
        "download_signing_configured": bool(
            settings.storage_backend == "s3"
            or (settings.public_base_url and len(settings.download_signing_secret) >= 32)
        ),
        "ffmpeg_available": shutil.which(settings.ffmpeg_bin) is not None,
        "ffprobe_available": shutil.which(settings.ffprobe_bin) is not None,
    }
    optional = {
        "google_drive_ingest_enabled": settings.google_drive_ingest_enabled,
        "google_drive_export_configured": bool(
            settings.google_drive_export_enabled
            and settings.google_drive_credentials_path
            and settings.google_drive_allowed_folder_ids
        ),
        "discord_export_configured": bool(
            settings.discord_export_enabled
            and settings.discord_bot_token
            and settings.discord_allowed_channel_ids
        ),
        "s3_storage_active": settings.storage_backend == "s3",
    }
    missing = [name for name, configured in required.items() if not configured]
    return {
        "ok": not missing,
        "serviceId": "ffmpeg",
        "required": required,
        "optional": optional,
        "missing": missing,
    }


def _health_payload() -> dict[str, Any]:
    manifest = _current_tool_manifest()
    counts = manifest["counts"]
    tool_count = len(TOOL_REGISTRY)
    configuration = _configuration_status()
    return {
        "ok": True,
        "status": "healthy" if configuration["ok"] else "degraded",
        "service": "ffmpeg-mcp",
        "version": __version__,
        "server_version": __version__,
        "build_sha": manifest["buildSha"],
        "source_fingerprint": settings.source_fingerprint,
        "image_reference": settings.image_reference,
        "catalog_version": manifest["catalogVersion"],
        "descriptor_hash": manifest["descriptorHash"],
        "tool_count": tool_count,
        "raw_tool_count": counts["raw"],
        "exposed_tool_count": tool_count,
        "agent_ready_tool_count": counts["agentReady"],
        "documented_tool_count": counts["raw"],
        "tools": {
            "total": tool_count,
            "raw": counts["raw"],
            "exposed": tool_count,
            "agent_ready": counts["agentReady"],
            "legacy": counts["legacy"],
            "hidden": counts["hidden"],
            "documented": counts["raw"],
        },
        "configuration_ready": configuration["ok"],
        "configuration": {
            "ready": configuration["ok"],
            "required": configuration["required"],
            "optional": configuration["optional"],
            "missing": configuration["missing"],
        },
    }


async def tool_check_configuration() -> dict[str, Any]:
    return _configuration_status()


async def tool_list_capabilities(
    include_descriptors: bool = False,
) -> dict[str, Any]:
    manifest = _current_tool_manifest()
    result = {
        "schemaVersion": manifest["schemaVersion"],
        "serviceId": manifest["serviceId"],
        "catalogVersion": manifest["catalogVersion"],
        "buildSha": manifest["buildSha"],
        "descriptorHash": manifest["descriptorHash"],
        "counts": manifest["counts"],
        "tools": manifest["tools"] if include_descriptors else [],
    }
    return result


async def tool_get_endpoint_coverage(
    category: str | None = None,
    tool_name: str | None = None,
) -> dict[str, Any]:
    manifest = _current_tool_manifest()
    descriptors = manifest["tools"]
    if category:
        descriptors = [item for item in descriptors if item["category"] == category]
    if tool_name:
        selected = resolve_tool_descriptor(manifest, tool_name)
        descriptors = [
            item for item in descriptors if item["nativeToolName"] == selected["nativeToolName"]
        ]
    entries = [
        {
            "capability": descriptor["category"],
            "nativeToolName": descriptor["nativeToolName"],
            "title": descriptor["title"],
            "tier": descriptor["tier"],
            "status": "covered",
            "providerInterface": "ffmpeg-cli",
            "documentationUrl": descriptor["documentationUrl"],
        }
        for descriptor in descriptors
    ]
    return {
        "serviceId": "ffmpeg",
        "providerKind": "local-cli",
        "source": "https://ffmpeg.org/documentation.html",
        "catalogDocumentation": MANIFEST_DOCUMENTATION_URL,
        "entries": entries,
    }


async def tool_get_tool_usage(tool_name: str) -> dict[str, Any]:
    manifest = _current_tool_manifest()
    return {"tool": resolve_tool_descriptor(manifest, tool_name)}


async def tool_find_tools(
    query: str,
    category: str | None = None,
    risk: str | None = None,
    tier: str | None = "agent_ready",
    limit: int = 8,
) -> dict[str, Any]:
    manifest = _current_tool_manifest()
    results = search_tool_manifest(
        manifest,
        query,
        category=category,
        risk=risk,
        tier=tier,
        limit=limit,
    )
    return {
        "query": query,
        "count": len(results),
        "results": results,
    }


TOOL_REGISTRY: dict[str, Callable[..., Awaitable[dict]]] = {
    "check_configuration": tool_check_configuration,
    "list_capabilities": tool_list_capabilities,
    "get_endpoint_coverage": tool_get_endpoint_coverage,
    "get_tool_usage": tool_get_tool_usage,
    "find_tools": tool_find_tools,
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


def _reject_nonfinite_input(value: Any) -> None:
    stack: list[tuple[Any, int]] = [(value, 0)]
    visited = 0
    while stack:
        current, depth = stack.pop()
        visited += 1
        if visited > 10_000 or depth > 16:
            raise ValueError("tool input exceeded validation limits")
        if isinstance(current, float) and not math.isfinite(current):
            raise ValueError("tool input contained a non-finite number")
        if isinstance(current, dict):
            stack.extend((item, depth + 1) for item in current.values())
        elif isinstance(current, (list, tuple)):
            stack.extend((item, depth + 1) for item in current)


def _validated_tool_function(
    func,
    output_schema: dict[str, Any],
    input_schema: dict[str, Any] | None = None,
    tool_name: str | None = None,
):
    Draft202012Validator.check_schema(output_schema)
    output_validator = Draft202012Validator(output_schema)
    input_validator = None
    signature = inspect.signature(func)
    if input_schema is not None:
        Draft202012Validator.check_schema(input_schema)
        input_validator = Draft202012Validator(input_schema)

    @functools.wraps(func)
    async def validated(*args, **kwargs):
        if input_validator is not None:
            try:
                bound = signature.bind(*args, **kwargs)
                bound.apply_defaults()
                payload = dict(bound.arguments)
                _reject_nonfinite_input(payload)
                input_validator.validate(payload)
            except (JsonSchemaValidationError, TypeError, ValueError) as exc:
                label = tool_name or "tool"
                raise ValueError(f"{label} input violated its declared schema") from exc
        result = await func(*args, **kwargs)
        try:
            output_validator.validate(result)
        except JsonSchemaValidationError as exc:
            raise RuntimeError("tool output violated its declared schema") from exc
        return result

    return validated


def register_tools() -> None:
    manifest_by_name = {
        descriptor["nativeToolName"]: descriptor for descriptor in _current_tool_manifest()["tools"]
    }
    for name, func in TOOL_REGISTRY.items():
        if mcp._tool_manager.get_tool(name) is not None:
            continue
        descriptor = manifest_by_name[name]
        validated_func = _validated_tool_function(
            func,
            descriptor["outputSchema"],
            descriptor["inputSchema"],
            name,
        )
        mcp.tool(
            name=name,
            title=descriptor["title"],
            description=descriptor["description"],
            annotations=ToolAnnotations(**descriptor["annotations"]),
            meta={
                "com.madpanda/catalogVersion": descriptor["catalogVersion"],
                "com.madpanda/descriptorHash": descriptor["descriptorHash"],
                "com.madpanda/tier": descriptor["tier"],
            },
            structured_output=True,
        )(validated_func)
        registered = mcp._tool_manager.get_tool(name)
        if registered is None:
            raise RuntimeError(f"failed to register tool: {name}")
        registered.parameters = descriptor["inputSchema"]
        registered.fn_metadata.output_schema = descriptor["outputSchema"]
        registered.fn_metadata.arg_model.model_config["extra"] = "forbid"
        registered.fn_metadata.arg_model.model_rebuild(force=True)
        registered.__dict__.pop("output_schema", None)


class _BodyTooLarge(Exception):
    pass


class _ResponseTooLarge(Exception):
    pass


def _header_values(scope: dict, name: str) -> list[str]:
    target = name.lower().encode("ascii")
    values: list[str] = []
    for key, value in scope.get("headers", []):
        if key.lower() != target:
            continue
        try:
            values.append(value.decode("latin-1"))
        except UnicodeDecodeError:
            values.append("")
    return values


@overload
def _single_header(scope: dict, name: str, *, required: Literal[True]) -> str: ...


@overload
def _single_header(scope: dict, name: str, *, required: Literal[False] = False) -> str | None: ...


def _single_header(scope: dict, name: str, *, required: bool = False) -> str | None:
    values = _header_values(scope, name)
    if len(values) > 1 or (required and len(values) != 1):
        raise ValueError("duplicate or missing protected header")
    return values[0] if values else None


def _safe_request_id(scope: dict) -> str:
    values = _header_values(scope, "x-request-id")
    if len(values) == 1 and re.fullmatch(r"[A-Za-z0-9._-]{1,128}", values[0]):
        return values[0]
    return uuid.uuid4().hex


def _authenticate(scope: dict) -> str:
    authorization = _single_header(scope, "authorization")
    grant = _single_header(scope, settings.portal_grant_header)
    subject = _single_header(scope, settings.portal_subject_header)
    if settings.mcp_mode == "standalone":
        if grant is not None or subject is not None or authorization is None:
            raise PermissionError("unauthorized")
        bearer = re.fullmatch(r"(?i:Bearer) ([^\s\x00-\x1f\x7f]+)", authorization)
        if bearer is None or not hmac.compare_digest(bearer.group(1), settings.mcp_access_token):
            raise PermissionError("unauthorized")
        return hash_principal(
            "standalone-owner",
            settings.principal_hash_secret,
            namespace="standalone",
        )
    if settings.mcp_mode == "portal":
        if authorization is not None or grant is None or subject is None:
            raise PermissionError("unauthorized")
        if not hmac.compare_digest(grant, settings.portal_grant_token):
            raise PermissionError("unauthorized")
        if subject != subject.strip() or not 1 <= len(subject) <= 512:
            raise PermissionError("unauthorized")
        if any(ord(character) < 32 or ord(character) == 127 for character in subject):
            raise PermissionError("unauthorized")
        return hash_principal(
            subject,
            settings.principal_hash_secret,
            namespace="portal-subject",
        )
    raise PermissionError("unauthorized")


def _register_principal_hit(owner_hash: str) -> tuple[int, int]:
    window = settings.rate_limit_window_seconds
    now = int(time.time())
    bucket = now // window
    retry_after = max(1, window - (now % window))
    key = f"mcp:ratelimit:principal:{owner_hash}:{bucket}"
    client = get_redis()
    pipe = client.pipeline()
    pipe.incr(key, 1)
    pipe.expire(key, window + 5)
    result = pipe.execute()
    return int(result[0]), retry_after


def _normalized_hostname(value: str) -> str | None:
    if not value or value != value.strip() or any(ord(char) < 33 for char in value):
        return None
    try:
        parsed = urlsplit(f"//{value}")
        _ = parsed.port
    except ValueError:
        return None
    if (
        parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or not parsed.hostname
    ):
        return None
    return parsed.hostname.lower()


async def _read_bounded_body(receive, limit: int, timeout_seconds: float) -> bytes:
    async with asyncio.timeout(timeout_seconds):
        chunks: list[bytes] = []
        size = 0
        while True:
            message = await receive()
            if message.get("type") == "http.disconnect":
                raise ValueError("request disconnected")
            if message.get("type") != "http.request":
                continue
            chunk = message.get("body") or b""
            size += len(chunk)
            if size > limit:
                raise _BodyTooLarge
            if chunk:
                chunks.append(chunk)
            if not message.get("more_body", False):
                return b"".join(chunks)


async def _call_with_bounded_response(app, scope, receive, limit: int) -> tuple[int, list[dict]]:
    messages: list[dict] = []
    body_size = 0
    status = 500

    async def buffered_send(message: dict) -> None:
        nonlocal body_size, status
        if message.get("type") == "http.response.start":
            status = int(message.get("status", 500))
        elif message.get("type") == "http.response.body":
            body_size += len(message.get("body") or b"")
            if body_size > limit:
                raise _ResponseTooLarge
        messages.append(message)

    await app(scope, receive, buffered_send)
    return status, messages


async def _flush_messages(send, messages: list[dict]) -> int:
    size = 0
    for message in messages:
        if message.get("type") == "http.response.body":
            size += len(message.get("body") or b"")
        await send(message)
    return size


def _audit_safe(
    *,
    request_id: str,
    owner_hash: str | None,
    tool: str | None,
    status_code: int,
    duration_ms: int,
    bytes_in: int,
    bytes_out: int,
) -> None:
    log_event(
        "mcp_request",
        {
            "request_id": request_id,
            "owner_hash": owner_hash,
            "tool": tool,
            "status": "ok" if status_code < 400 else "error",
            "http_status": status_code,
            "duration_ms": duration_ms,
            "bytes_in": bytes_in,
            "bytes_out": bytes_out,
        },
    )


def build_app():
    settings.validate_runtime()
    register_tools()
    inner_factory = mcp.streamable_http_app
    inner_app = inner_factory() if callable(inner_factory) else inner_factory

    async def guarded(scope, receive, send):
        if scope.get("type") != "http":
            await inner_app(scope, receive, send)
            return

        try:
            host = _single_header(scope, "host", required=True)
            origin = _single_header(scope, "origin")
        except ValueError:
            await _send_json(send, 400, {"error": "invalid headers"})
            return
        allowed_hostnames = {
            normalized
            for item in settings.allowed_hosts
            if (normalized := _normalized_hostname(item)) is not None
        }
        if host is None or _normalized_hostname(host) not in allowed_hostnames:
            await _send_json(send, 400, {"error": "invalid host"})
            return
        if origin is not None and origin not in settings.allowed_origins:
            await _send_json(send, 403, {"error": "origin not allowed"})
            return

        path = str(scope.get("path") or "")
        method = str(scope.get("method") or "").upper()
        if path == "/health":
            if method != "GET":
                await _send_json(send, 405, {"error": "method not allowed"})
                return
            await _send_json(send, 200, _health_payload())
            return
        if path.startswith("/download/"):
            await _download_handler(scope, receive, send)
            return
        if path != "/mcp":
            await _send_json(send, 404, {"error": "not found"})
            return
        if method != "POST":
            await _send_jsonrpc_error(
                send,
                status=405,
                code=-32600,
                message="Method not allowed",
                request_id="server-error",
            )
            return

        request_id = _safe_request_id(scope)
        started_at = time.perf_counter()
        owner_hash: str | None = None
        tool_name: str | None = None
        jsonrpc_id: str | int = "server-error"
        bytes_in = 0
        bytes_out = 0
        status_code = 500
        try:
            try:
                owner_hash = _authenticate(scope)
            except (PermissionError, ValueError):
                status_code = 401
                bytes_out = await _send_jsonrpc_error(
                    send,
                    status=status_code,
                    code=-32001,
                    message="Unauthorized",
                    request_id=jsonrpc_id,
                )
                return

            try:
                hits, retry_after = _register_principal_hit(owner_hash)
            except Exception:
                status_code = 503
                bytes_out = await _send_jsonrpc_error(
                    send,
                    status=status_code,
                    code=-32003,
                    message="Service temporarily unavailable",
                    request_id=jsonrpc_id,
                )
                return
            if hits > settings.rate_limit_principal_rpm:
                status_code = 429
                bytes_out = await _send_jsonrpc_error(
                    send,
                    status=status_code,
                    code=-32002,
                    message="Rate limit exceeded",
                    request_id=jsonrpc_id,
                    retry_after=retry_after,
                )
                return

            try:
                content_length = _single_header(scope, "content-length")
                content_type = _single_header(scope, "content-type", required=True)
                accept = _single_header(scope, "accept", required=True)
            except ValueError:
                status_code = 400
                bytes_out = await _send_jsonrpc_error(
                    send,
                    status=status_code,
                    code=-32600,
                    message="Invalid request headers",
                    request_id=jsonrpc_id,
                )
                return
            if content_length is not None:
                try:
                    declared_length = int(content_length)
                except ValueError:
                    declared_length = -1
                if declared_length < 0:
                    status_code = 400
                    bytes_out = await _send_jsonrpc_error(
                        send,
                        status=status_code,
                        code=-32600,
                        message="Invalid Content-Length",
                        request_id=jsonrpc_id,
                    )
                    return
                if declared_length > settings.request_body_max_bytes:
                    status_code = 413
                    bytes_out = await _send_jsonrpc_error(
                        send,
                        status=status_code,
                        code=-32600,
                        message="Request body too large",
                        request_id=jsonrpc_id,
                    )
                    return
            media_type = (content_type or "").split(";", 1)[0].strip().lower()
            if media_type != "application/json":
                status_code = 415
                bytes_out = await _send_jsonrpc_error(
                    send,
                    status=status_code,
                    code=-32600,
                    message="Content-Type must be application/json",
                    request_id=jsonrpc_id,
                )
                return
            accepted = {part.split(";", 1)[0].strip().lower() for part in (accept or "").split(",")}
            if not accepted.intersection({"application/json", "*/*"}):
                status_code = 406
                bytes_out = await _send_jsonrpc_error(
                    send,
                    status=status_code,
                    code=-32600,
                    message="Not acceptable",
                    request_id=jsonrpc_id,
                )
                return
            try:
                body = await _read_bounded_body(
                    receive,
                    settings.request_body_max_bytes,
                    settings.request_body_timeout_seconds,
                )
            except _BodyTooLarge:
                status_code = 413
                bytes_out = await _send_jsonrpc_error(
                    send,
                    status=status_code,
                    code=-32600,
                    message="Request body too large",
                    request_id=jsonrpc_id,
                )
                return
            except TimeoutError:
                status_code = 408
                bytes_out = await _send_jsonrpc_error(
                    send,
                    status=status_code,
                    code=-32600,
                    message="Request body timed out",
                    request_id=jsonrpc_id,
                )
                return
            bytes_in = len(body)
            payload = _safe_parse_json(body)
            if payload is None:
                status_code = 400
                bytes_out = await _send_jsonrpc_error(
                    send,
                    status=status_code,
                    code=-32600,
                    message="Body must be a JSON-RPC object",
                    request_id=jsonrpc_id,
                )
                return
            method_name, tool_name, jsonrpc_id = _extract_jsonrpc_metadata(payload)
            safe_headers = [
                (b"host", b"localhost"),
                (b"content-type", content_type.encode("latin-1")),
                (b"content-length", str(len(body)).encode("ascii")),
                (b"accept", accept.encode("latin-1")),
                (b"x-request-id", request_id.encode("ascii")),
            ]
            for forwarded_name in ("mcp-protocol-version", "mcp-session-id"):
                forwarded_values = _header_values(scope, forwarded_name)
                if len(forwarded_values) > 1:
                    status_code = 400
                    bytes_out = await _send_jsonrpc_error(
                        send,
                        status=status_code,
                        code=-32600,
                        message="Invalid request headers",
                        request_id=jsonrpc_id,
                    )
                    return
                if forwarded_values:
                    safe_headers.append(
                        (
                            forwarded_name.encode("ascii"),
                            forwarded_values[0].encode("latin-1"),
                        )
                    )
            inner_scope = {**scope, "headers": safe_headers}
            request_context_token = REQUEST_CONTEXT.set(
                {"request_id": request_id, "owner_hash": owner_hash}
            )
            try:
                with tenant_context(owner_hash):
                    try:
                        status_code, messages = await _call_with_bounded_response(
                            inner_app,
                            inner_scope,
                            _replay_receive(body),
                            settings.response_body_max_bytes,
                        )
                    except _ResponseTooLarge:
                        status_code = 502
                        bytes_out = await _send_jsonrpc_error(
                            send,
                            status=status_code,
                            code=-32603,
                            message="Response exceeded server limit",
                            request_id=jsonrpc_id,
                        )
                        return
                    except Exception:
                        status_code = 500
                        bytes_out = await _send_jsonrpc_error(
                            send,
                            status=status_code,
                            code=-32603,
                            message="Internal server error",
                            request_id=jsonrpc_id,
                        )
                        return
                    bytes_out = await _flush_messages(send, messages)
            finally:
                REQUEST_CONTEXT.reset(request_context_token)
        finally:
            _audit_safe(
                request_id=request_id,
                owner_hash=owner_hash,
                tool=tool_name,
                status_code=status_code,
                duration_ms=int((time.perf_counter() - started_at) * 1000),
                bytes_in=bytes_in,
                bytes_out=bytes_out,
            )

    return guarded


def main() -> None:
    settings.validate_runtime()
    os.environ.setdefault("HOST", settings.mcp_bind_address)
    os.environ.setdefault("PORT", str(settings.mcp_http_port))
    _start_cleanup_thread()
    uvicorn.run(
        build_app,
        host=settings.mcp_bind_address,
        port=settings.mcp_http_port,
        factory=True,
    )


if __name__ == "__main__":
    main()
