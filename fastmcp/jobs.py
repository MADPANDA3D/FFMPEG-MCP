import hashlib
import os
import string
import tempfile
import uuid
from typing import Any

from rq import get_current_job

from config import settings
from ffmpeg_utils import FfmpegError, run_ffmpeg
from ffprobe_utils import run_ffprobe
from metrics import job_timer, log_event, record_job_duration
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
    escape_drawtext_value,
    resolve_font_path,
    resolve_logo_path,
    sanitize_box_border,
    sanitize_color,
    sanitize_font_size,
    sanitize_opacity,
    sanitize_position,
    sanitize_scale_pct,
    sanitize_text,
)
from templates import get_template
from presets import get_preset
from redis_store import (
    build_cache_key,
    delete_cached_result,
    get_asset,
    get_brand_kit,
    get_cached_result,
    save_asset,
    set_cached_result,
    update_job,
)
from storage import download_to_temp, put_file
from utils import utc_now_iso, utc_now_ts


class JobError(RuntimeError):
    pass


def _hash_file(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 64)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _ensure_temp_dir() -> None:
    os.makedirs(settings.storage_temp_dir, exist_ok=True)


def _create_output_asset(
    path: str,
    mime_type: str,
    extension: str,
    parent_asset_id: str,
    ttl_seconds: int,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    asset_id = uuid.uuid4().hex
    sha256 = _hash_file(path)
    storage_key, storage_uri, size_bytes = put_file(path, asset_id=asset_id, ext=extension)
    created_at = utc_now_iso()
    expires_at = utc_now_ts() + ttl_seconds

    asset = {
        "asset_id": asset_id,
        "source": "process",
        "original_filename": os.path.basename(storage_key),
        "mime_type": mime_type,
        "size_bytes": size_bytes,
        "sha256": sha256,
        "storage_uri": storage_uri,
        "storage_key": storage_key,
        "created_at": created_at,
        "expires_at": expires_at,
        "parent_asset_id": parent_asset_id,
    }
    if metadata:
        asset.update(metadata)
    save_asset(asset, ttl_seconds)
    return asset


def _enforce_output_size(path: str) -> None:
    size_bytes = os.path.getsize(path)
    if size_bytes > settings.max_output_bytes:
        raise JobError("Output exceeds max size")


def _probe_optional(path: str) -> dict[str, Any] | None:
    try:
        return run_ffprobe(path)
    except Exception:
        return None


def _resolve_input_path(asset: dict[str, Any]) -> tuple[str, bool]:
    storage_key = asset.get("storage_key")
    if not storage_key:
        raise JobError("Asset storage key missing")
    temp_path = download_to_temp(storage_key)
    return temp_path, settings.storage_backend == "s3"


def _finish_job(job_id: str, status: str, updates: dict[str, Any]) -> None:
    if not job_id:
        return
    if status in {"success", "error"}:
        updates.setdefault("progress", 100)
    updates = {**updates, "status": status, "updated_at": utc_now_iso()}
    update_job(job_id, updates)


def _log_job_started(job_type: str, job_id: str, asset_id: str | None = None) -> None:
    log_event(
        "job_started",
        {"job_type": job_type, "job_id": job_id, "asset_id": asset_id},
    )


def _record_job_metrics(job_type: str, start_ts: float, status: str, job_id: str) -> None:
    duration_ms = int((job_timer() - start_ts) * 1000)
    record_job_duration(job_type, duration_ms, status)
    log_event(
        "job_finished",
        {"job_type": job_type, "job_id": job_id, "status": status, "duration_ms": duration_ms},
    )


def _get_asset_or_error(asset_id: str) -> dict[str, Any]:
    asset = get_asset(asset_id)
    if not asset:
        raise JobError("Asset not found")
    return asset


def _asset_mime(asset: dict[str, Any]) -> str:
    return (asset.get("mime_type") or "").lower()


def _ensure_video_asset(asset: dict[str, Any]) -> None:
    if not _asset_mime(asset).startswith("video/"):
        raise JobError("Input asset must be a video")


def _ensure_image_asset(asset: dict[str, Any]) -> None:
    if not _asset_mime(asset).startswith("image/"):
        raise JobError("Input asset must be an image")


def _probe_or_error(path: str) -> dict[str, Any]:
    try:
        return run_ffprobe(path)
    except Exception as exc:
        raise JobError("Unable to probe media") from exc


def _has_audio_stream(probe: dict[str, Any]) -> bool:
    streams = probe.get("streams") or []
    for stream in streams:
        if stream.get("codec_type") == "audio":
            return True
    return False


def _has_video_stream(probe: dict[str, Any]) -> bool:
    streams = probe.get("streams") or []
    for stream in streams:
        if stream.get("codec_type") == "video":
            return True
    return False


def _text_overlay_xy(position: str) -> tuple[str, str]:
    margin = max(settings.overlay_margin_px, 0)
    x_expr = "(w-text_w)/2"
    if position == "top":
        y_expr = str(margin)
    elif position == "center":
        y_expr = "(h-text_h)/2"
    else:
        y_expr = f"h-text_h-{margin}"
    return x_expr, y_expr


def _logo_overlay_xy(position: str) -> tuple[str, str]:
    margin = max(settings.overlay_margin_px, 0)
    if position == "top-left":
        return str(margin), str(margin)
    if position == "top-right":
        return f"W-w-{margin}", str(margin)
    if position == "bottom-left":
        return str(margin), f"H-h-{margin}"
    return f"W-w-{margin}", f"H-h-{margin}"


def _audio_output_config(fmt: str, bitrate: str | None) -> tuple[str, list[str], str]:
    fmt = (fmt or "").lower()
    if fmt not in {"mp3", "wav", "m4a"}:
        raise JobError("Unsupported audio format")
    if fmt == "mp3":
        args = ["-c:a", "libmp3lame"]
        mime_type = "audio/mpeg"
        ext = ".mp3"
    elif fmt == "wav":
        args = ["-c:a", "pcm_s16le"]
        mime_type = "audio/wav"
        ext = ".wav"
    else:
        args = ["-c:a", "aac"]
        mime_type = "audio/mp4"
        ext = ".m4a"
    if bitrate:
        args += ["-b:a", str(bitrate)]
    return mime_type, args, ext


def _cleanup_inputs(inputs: list[tuple[str, bool]]) -> None:
    for path, cleanup in inputs:
        if cleanup and os.path.exists(path):
            os.remove(path)


def _resolve_cached_output(cache_key: str) -> str | None:
    cached = get_cached_result(cache_key)
    if not cached:
        return None
    output_ids = cached.get("output_asset_ids")
    if not output_ids:
        delete_cached_result(cache_key)
        return None
    output_id = output_ids[0]
    asset = get_asset(output_id)
    if not asset:
        delete_cached_result(cache_key)
        return None
    expires_at = asset.get("expires_at")
    if expires_at and int(expires_at) <= utc_now_ts():
        delete_cached_result(cache_key)
        return None
    return output_id


def _resolve_cached_outputs_list(cache_key: str) -> list[str] | None:
    cached = get_cached_result(cache_key)
    if not cached:
        return None
    output_ids = cached.get("output_asset_ids")
    if not output_ids:
        delete_cached_result(cache_key)
        return None
    resolved: list[str] = []
    for asset_id in output_ids:
        asset = get_asset(asset_id)
        if not asset:
            delete_cached_result(cache_key)
            return None
        expires_at = asset.get("expires_at")
        if expires_at and int(expires_at) <= utc_now_ts():
            delete_cached_result(cache_key)
            return None
        resolved.append(asset_id)
    return resolved


def _extract_placeholders(value: str) -> list[str]:
    formatter = string.Formatter()
    names = []
    for _, field_name, _, _ in formatter.parse(value):
        if field_name:
            names.append(field_name)
    return names


def transcode_job(
    asset_id: str,
    preset: str,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("transcode", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    asset = get_asset(asset_id)
    if not asset:
        raise JobError("Asset not found")

    _ensure_temp_dir()
    input_path, cleanup = _resolve_input_path(asset)
    preset_def = get_preset(preset)

    output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}{preset_def['output_ext']}")
    args = ["-i", input_path] + preset_def["ffmpeg_args"] + [output_path]
    logs = ""
    try:
        logs = run_ffmpeg(args)
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        probe = _probe_optional(output_path)
        if probe and probe.get("duration_sec") and probe["duration_sec"] > settings.max_duration_seconds:
            raise JobError("Output exceeds max duration")
        ttl_seconds = settings.asset_ttl_seconds()
        output_asset = _create_output_asset(
            output_path,
            preset_def["mime_type"],
            preset_def["output_ext"],
            asset_id,
            ttl_seconds,
            probe,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("transcode", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "transcode",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": logs, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("transcode", start_ts, "error", job_id)
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if os.path.exists(output_path):
            os.remove(output_path)


def thumbnail_job(
    asset_id: str,
    time_sec: float,
    width: int | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("thumbnail", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    asset = get_asset(asset_id)
    if not asset:
        raise JobError("Asset not found")

    _ensure_temp_dir()
    input_path, cleanup = _resolve_input_path(asset)
    output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}.jpg")

    vf = []
    if width and width > 0:
        vf = ["-vf", f"scale={width}:-2"]

    args = ["-ss", str(time_sec), "-i", input_path] + vf + ["-vframes", "1", output_path]
    logs = ""
    try:
        logs = run_ffmpeg(args)
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        ttl_seconds = settings.asset_ttl_seconds()
        output_asset = _create_output_asset(
            output_path,
            "image/jpeg",
            ".jpg",
            asset_id,
            ttl_seconds,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("thumbnail", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "thumbnail",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": logs, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("thumbnail", start_ts, "error", job_id)
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if os.path.exists(output_path):
            os.remove(output_path)


def extract_audio_job(
    asset_id: str,
    fmt: str,
    bitrate: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("extract_audio", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    asset = get_asset(asset_id)
    if not asset:
        raise JobError("Asset not found")

    _ensure_temp_dir()
    fmt = fmt.lower()
    if fmt not in {"mp3", "wav", "m4a"}:
        raise JobError("Unsupported audio format")

    input_path, cleanup = _resolve_input_path(asset)
    output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}.{fmt}")

    args = ["-i", input_path, "-vn"]
    if fmt == "mp3":
        args += ["-c:a", "libmp3lame"]
        if bitrate:
            args += ["-b:a", bitrate]
    elif fmt == "wav":
        args += ["-c:a", "pcm_s16le"]
    elif fmt == "m4a":
        args += ["-c:a", "aac"]
        if bitrate:
            args += ["-b:a", bitrate]
    args.append(output_path)

    logs = ""
    try:
        logs = run_ffmpeg(args)
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        ttl_seconds = settings.asset_ttl_seconds()
        mime_type = "audio/mpeg" if fmt == "mp3" else "audio/wav" if fmt == "wav" else "audio/mp4"
        probe = _probe_optional(output_path)
        if probe and probe.get("duration_sec") and probe["duration_sec"] > settings.max_duration_seconds:
            raise JobError("Output exceeds max duration")
        output_asset = _create_output_asset(
            output_path,
            mime_type,
            f".{fmt}",
            asset_id,
            ttl_seconds,
            probe,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("extract_audio", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "extract_audio",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": logs, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("extract_audio", start_ts, "error", job_id)
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if os.path.exists(output_path):
            os.remove(output_path)


def trim_job(
    asset_id: str,
    start_sec: float,
    end_sec: float,
    reencode: bool,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("trim", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    asset = get_asset(asset_id)
    if not asset:
        raise JobError("Asset not found")

    if end_sec <= start_sec:
        raise JobError("end_sec must be greater than start_sec")

    _ensure_temp_dir()
    input_path, cleanup = _resolve_input_path(asset)
    output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}.mp4")

    args = ["-ss", str(start_sec), "-to", str(end_sec), "-i", input_path]
    if reencode:
        args += ["-c:v", "libx264", "-preset", "veryfast", "-crf", "23", "-c:a", "aac"]
    else:
        args += ["-c", "copy", "-avoid_negative_ts", "make_zero", "-reset_timestamps", "1"]
    args += ["-movflags", "+faststart", output_path]

    logs = ""
    try:
        logs = run_ffmpeg(args)
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        ttl_seconds = settings.asset_ttl_seconds()
        probe = _probe_optional(output_path)
        if probe and probe.get("duration_sec") and probe["duration_sec"] > settings.max_duration_seconds:
            raise JobError("Output exceeds max duration")
        output_asset = _create_output_asset(
            output_path,
            "video/mp4",
            ".mp4",
            asset_id,
            ttl_seconds,
            probe,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("trim", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "trim",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": logs, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("trim", start_ts, "error", job_id)
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if os.path.exists(output_path):
            os.remove(output_path)


def video_add_text_job(
    asset_id: str,
    text: str,
    position: str | None,
    font_size: int | None,
    font_color: str | None,
    background_box: bool | None,
    box_color: str | None,
    box_border_width: int | None,
    font_name: str | None,
    font_asset_id: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("video_add_text", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    asset = get_asset(asset_id)
    if not asset:
        raise JobError("Asset not found")
    mime_type = (asset.get("mime_type") or "").lower()
    if not mime_type.startswith("video/"):
        raise JobError("Input asset must be a video")

    _ensure_temp_dir()
    input_path, cleanup = _resolve_input_path(asset)
    output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}.mp4")

    logs = ""
    font_cleanup = False
    font_path = None
    text_path = None
    try:
        cleaned_text = sanitize_text(text)
        resolved_position = sanitize_position(
            position or DEFAULT_TEXT_POSITION,
            TEXT_POSITIONS,
        )
        resolved_font_size = sanitize_font_size(font_size, DEFAULT_FONT_SIZE)
        resolved_font_color = sanitize_color(font_color, DEFAULT_FONT_COLOR)
        resolved_box_color = sanitize_color(box_color, DEFAULT_BOX_COLOR)
        resolved_box_border = sanitize_box_border(box_border_width, DEFAULT_BOX_BORDER_WIDTH)
        font_path, font_cleanup = resolve_font_path(font_name, font_asset_id)

        with tempfile.NamedTemporaryFile(
            dir=settings.storage_temp_dir, prefix="drawtext_", suffix=".txt", delete=False
        ) as handle:
            handle.write(cleaned_text.encode("utf-8"))
            text_path = handle.name

        x_expr, y_expr = _text_overlay_xy(resolved_position)
        drawtext_parts = [
            f"fontfile={escape_drawtext_value(font_path)}",
            f"textfile={escape_drawtext_value(text_path)}",
            f"fontcolor={resolved_font_color}",
            f"fontsize={resolved_font_size}",
            f"x={x_expr}",
            f"y={y_expr}",
            "expansion=none",
        ]
        if background_box:
            drawtext_parts.extend(
                [
                    "box=1",
                    f"boxcolor={resolved_box_color}",
                    f"boxborderw={resolved_box_border}",
                ]
            )
        drawtext_filter = "drawtext=" + ":".join(drawtext_parts)

        args = [
            "-i",
            input_path,
            "-vf",
            drawtext_filter,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-c:a",
            "aac",
            "-b:a",
            "160k",
            "-movflags",
            "+faststart",
            output_path,
        ]
        logs = run_ffmpeg(args, timeout=settings.text_timeout_seconds())
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        probe = _probe_optional(output_path)
        if probe and probe.get("duration_sec") and probe["duration_sec"] > settings.max_duration_seconds:
            raise JobError("Output exceeds max duration")
        ttl_seconds = settings.asset_ttl_seconds()
        output_asset = _create_output_asset(
            output_path,
            "video/mp4",
            ".mp4",
            asset_id,
            ttl_seconds,
            probe,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("video_add_text", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "video_add_text",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": logs, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("video_add_text", start_ts, "error", job_id)
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if font_cleanup and font_path and os.path.exists(font_path):
            os.remove(font_path)
        if text_path and os.path.exists(text_path):
            os.remove(text_path)
        if os.path.exists(output_path):
            os.remove(output_path)


def video_add_logo_job(
    asset_id: str,
    logo_asset_id: str | None,
    logo_key: str | None,
    position: str | None,
    scale_pct: int | None,
    opacity: float | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("video_add_logo", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    asset = get_asset(asset_id)
    if not asset:
        raise JobError("Asset not found")
    mime_type = (asset.get("mime_type") or "").lower()
    if not mime_type.startswith("video/"):
        raise JobError("Input asset must be a video")

    _ensure_temp_dir()
    input_path, cleanup = _resolve_input_path(asset)
    output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}.mp4")
    logo_cleanup = False
    logo_path = None
    logs = ""
    try:
        resolved_position = sanitize_position(
            position or DEFAULT_LOGO_POSITION,
            LOGO_POSITIONS,
        )
        resolved_scale = sanitize_scale_pct(scale_pct, DEFAULT_LOGO_SCALE_PCT)
        resolved_opacity = sanitize_opacity(opacity, DEFAULT_LOGO_OPACITY)
        logo_path, logo_cleanup = resolve_logo_path(logo_asset_id, logo_key)

        try:
            probe = run_ffprobe(input_path)
        except Exception as exc:
            raise JobError("Unable to probe video") from exc
        width = probe.get("width")
        if not width:
            raise JobError("Unable to determine video width")
        logo_width = max(1, int(width * resolved_scale / 100))

        x_expr, y_expr = _logo_overlay_xy(resolved_position)
        logo_chain = f"[1:v]scale={logo_width}:-1,format=rgba"
        if resolved_opacity < 1:
            logo_chain += f",colorchannelmixer=aa={resolved_opacity}"
        logo_chain += "[logo]"
        overlay_chain = f"[0:v][logo]overlay={x_expr}:{y_expr}[v]"
        filter_complex = f"{logo_chain};{overlay_chain}"

        args = [
            "-i",
            input_path,
            "-loop",
            "1",
            "-i",
            logo_path,
            "-filter_complex",
            filter_complex,
            "-map",
            "[v]",
            "-map",
            "0:a?",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-c:a",
            "aac",
            "-b:a",
            "160k",
            "-movflags",
            "+faststart",
            "-shortest",
            output_path,
        ]
        logs = run_ffmpeg(args, timeout=settings.logo_timeout_seconds())
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        probe_out = _probe_optional(output_path)
        if probe_out and probe_out.get("duration_sec") and probe_out["duration_sec"] > settings.max_duration_seconds:
            raise JobError("Output exceeds max duration")
        ttl_seconds = settings.asset_ttl_seconds()
        output_asset = _create_output_asset(
            output_path,
            "video/mp4",
            ".mp4",
            asset_id,
            ttl_seconds,
            probe_out,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("video_add_logo", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "video_add_logo",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": logs, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("video_add_logo", start_ts, "error", job_id)
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if logo_cleanup and logo_path and os.path.exists(logo_path):
            os.remove(logo_path)
        if os.path.exists(output_path):
            os.remove(output_path)


def video_concat_job(
    asset_ids: list[str],
    transition: str | None,
    transition_duration: float | None,
    target_width: int | None,
    target_height: int | None,
    include_audio: bool | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("video_concat", job_id, asset_ids[0] if asset_ids else None)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    if not asset_ids or len(asset_ids) < 2:
        raise JobError("At least two asset_ids are required")
    if len(asset_ids) > settings.max_concat_clips:
        raise JobError("Too many clips for concat")
    transition = (transition or "none").lower()
    if transition not in {"none", "crossfade"}:
        raise JobError("Unsupported transition")
    transition_duration = float(transition_duration or 0.0)
    if transition == "crossfade" and transition_duration <= 0:
        raise JobError("transition_duration must be > 0 for crossfade")
    include_audio = True if include_audio is None else bool(include_audio)

    _ensure_temp_dir()
    input_paths: list[tuple[str, bool]] = []
    probes: list[dict[str, Any]] = []
    durations: list[float] = []
    try:
        for asset_id in asset_ids:
            asset = _get_asset_or_error(asset_id)
            _ensure_video_asset(asset)
            path, cleanup = _resolve_input_path(asset)
            input_paths.append((path, cleanup))
            probe = _probe_or_error(path)
            if not _has_video_stream(probe):
                raise JobError("Video stream missing")
            duration = probe.get("duration_sec")
            if not duration:
                raise JobError("Duration missing for concat input")
            durations.append(float(duration))
            probes.append(probe)

        if transition == "crossfade":
            for duration in durations:
                if duration <= transition_duration:
                    raise JobError("Clip shorter than transition duration")

        width = int(target_width) if target_width else int(probes[0].get("width") or 0)
        height = int(target_height) if target_height else int(probes[0].get("height") or 0)
        if width <= 0 or height <= 0:
            raise JobError("Unable to determine output dimensions")

        args: list[str] = []
        for path, _ in input_paths:
            args += ["-i", path]

        audio_indexes: list[int] = []
        next_input_index = len(input_paths)
        for idx, probe in enumerate(probes):
            if include_audio and _has_audio_stream(probe):
                audio_indexes.append(idx)
                continue
            if include_audio:
                args += [
                    "-f",
                    "lavfi",
                    "-t",
                    str(durations[idx]),
                    "-i",
                    f"anullsrc=channel_layout=stereo:sample_rate={settings.audio_sample_rate}",
                ]
                audio_indexes.append(next_input_index)
                next_input_index += 1
            else:
                audio_indexes.append(-1)

        filter_parts: list[str] = []
        for idx, duration in enumerate(durations):
            v_label = f"v{idx}"
            v_chain = (
                f"[{idx}:v]scale={width}:{height}:force_original_aspect_ratio=decrease,"
                f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color=black,"
                f"trim=duration={duration},setpts=PTS-STARTPTS,setsar=1[{v_label}]"
            )
            filter_parts.append(v_chain)
            if include_audio:
                a_idx = audio_indexes[idx]
                a_label = f"a{idx}"
                a_chain = (
                    f"[{a_idx}:a]atrim=duration={duration},asetpts=PTS-STARTPTS,"
                    f"aresample={settings.audio_sample_rate},"
                    "aformat=sample_fmts=fltp:channel_layouts=stereo"
                    f"[{a_label}]"
                )
                filter_parts.append(a_chain)

        if transition == "none":
            concat_inputs = "".join([f"[v{idx}]" for idx in range(len(asset_ids))])
            if include_audio:
                concat_inputs = "".join(
                    [f"[v{idx}][a{idx}]" for idx in range(len(asset_ids))]
                )
                filter_parts.append(
                    f"{concat_inputs}concat=n={len(asset_ids)}:v=1:a=1[vout][aout]"
                )
            else:
                filter_parts.append(
                    f"{concat_inputs}concat=n={len(asset_ids)}:v=1:a=0[vout]"
                )
        else:
            cumulative = durations[0]
            v_current = "v0"
            a_current = "a0"
            for idx in range(1, len(asset_ids)):
                offset = cumulative - transition_duration
                v_next = f"v{idx}"
                v_out = f"vxf{idx}"
                filter_parts.append(
                    f"[{v_current}][{v_next}]"
                    f"xfade=transition=fade:duration={transition_duration}:offset={offset}[{v_out}]"
                )
                v_current = v_out
                if include_audio:
                    a_next = f"a{idx}"
                    a_out = f"axf{idx}"
                    filter_parts.append(
                        f"[{a_current}][{a_next}]acrossfade=d={transition_duration}:c1=tri:c2=tri[{a_out}]"
                    )
                    a_current = a_out
                cumulative = cumulative + durations[idx] - transition_duration

            if include_audio:
                filter_parts.append(f"[{v_current}]null[vout]")
                filter_parts.append(f"[{a_current}]anull[aout]")
            else:
                filter_parts.append(f"[{v_current}]null[vout]")

        filter_complex = ";".join(filter_parts)
        output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}.mp4")
        ffmpeg_args = args + [
            "-filter_complex",
            filter_complex,
            "-map",
            "[vout]",
        ]
        if include_audio:
            ffmpeg_args += ["-map", "[aout]"]
        else:
            ffmpeg_args += ["-an"]
        ffmpeg_args += [
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-c:a",
            "aac",
            "-b:a",
            "160k",
            "-movflags",
            "+faststart",
            output_path,
        ]
        logs = run_ffmpeg(ffmpeg_args, timeout=settings.concat_timeout_seconds())
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        probe_out = _probe_optional(output_path)
        if probe_out and probe_out.get("duration_sec") and probe_out["duration_sec"] > settings.max_duration_seconds:
            raise JobError("Output exceeds max duration")
        ttl_seconds = settings.asset_ttl_seconds()
        output_asset = _create_output_asset(
            output_path,
            "video/mp4",
            ".mp4",
            asset_ids[0],
            ttl_seconds,
            probe_out,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("video_concat", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "video_concat",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": None, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("video_concat", start_ts, "error", job_id)
        raise
    finally:
        _cleanup_inputs(input_paths)
        if "output_path" in locals() and os.path.exists(output_path):
            os.remove(output_path)


def image_to_video_job(
    asset_id: str,
    duration_sec: float,
    width: int | None,
    height: int | None,
    fps: int | None,
    background_color: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("image_to_video", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    asset = _get_asset_or_error(asset_id)
    _ensure_image_asset(asset)
    duration_sec = float(duration_sec)
    if duration_sec <= 0:
        raise JobError("duration_sec must be > 0")
    if duration_sec > settings.max_duration_seconds:
        raise JobError("Output exceeds max duration")
    width = int(width) if width else settings.default_image_width
    height = int(height) if height else settings.default_image_height
    if width <= 0 or height <= 0:
        raise JobError("Invalid output dimensions")
    fps = int(fps) if fps else settings.default_video_fps
    if fps <= 0:
        raise JobError("Invalid fps")
    pad_color = sanitize_color(background_color, "black")

    _ensure_temp_dir()
    input_path, cleanup = _resolve_input_path(asset)
    output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}.mp4")
    logs = ""
    try:
        vf = (
            f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
            f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color={pad_color},"
            f"fps={fps},format=yuv420p"
        )
        args = [
            "-loop",
            "1",
            "-i",
            input_path,
            "-t",
            str(duration_sec),
            "-vf",
            vf,
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            output_path,
        ]
        logs = run_ffmpeg(args, timeout=settings.image_timeout_seconds())
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        probe_out = _probe_optional(output_path)
        ttl_seconds = settings.asset_ttl_seconds()
        output_asset = _create_output_asset(
            output_path,
            "video/mp4",
            ".mp4",
            asset_id,
            ttl_seconds,
            probe_out,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("image_to_video", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "image_to_video",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": logs, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("image_to_video", start_ts, "error", job_id)
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if os.path.exists(output_path):
            os.remove(output_path)


def images_to_slideshow_job(
    asset_ids: list[str],
    duration_per_image: float | None,
    durations: list[float] | None,
    width: int | None,
    height: int | None,
    fps: int | None,
    background_color: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("images_to_slideshow", job_id, asset_ids[0] if asset_ids else None)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    if not asset_ids:
        raise JobError("asset_ids is required")
    if len(asset_ids) > settings.max_slideshow_images:
        raise JobError("Too many images for slideshow")

    if durations is not None and len(durations) != len(asset_ids):
        raise JobError("durations length must match asset_ids length")
    duration_per_image = float(duration_per_image or settings.default_image_duration_sec)
    if duration_per_image <= 0:
        raise JobError("duration_per_image must be > 0")
    resolved_durations = (
        [float(value) for value in durations] if durations is not None else [duration_per_image] * len(asset_ids)
    )
    total_duration = sum(resolved_durations)
    if total_duration > settings.max_duration_seconds:
        raise JobError("Output exceeds max duration")

    width = int(width) if width else settings.default_image_width
    height = int(height) if height else settings.default_image_height
    if width <= 0 or height <= 0:
        raise JobError("Invalid output dimensions")
    fps = int(fps) if fps else settings.default_video_fps
    if fps <= 0:
        raise JobError("Invalid fps")
    pad_color = sanitize_color(background_color, "black")

    _ensure_temp_dir()
    input_paths: list[tuple[str, bool]] = []
    try:
        for asset_id in asset_ids:
            asset = _get_asset_or_error(asset_id)
            _ensure_image_asset(asset)
            path, cleanup = _resolve_input_path(asset)
            input_paths.append((path, cleanup))

        args: list[str] = []
        for idx, (path, _) in enumerate(input_paths):
            args += ["-loop", "1", "-t", str(resolved_durations[idx]), "-i", path]

        filter_parts: list[str] = []
        for idx, duration in enumerate(resolved_durations):
            v_label = f"v{idx}"
            v_chain = (
                f"[{idx}:v]scale={width}:{height}:force_original_aspect_ratio=decrease,"
                f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color={pad_color},"
                f"fps={fps},trim=duration={duration},setpts=PTS-STARTPTS,setsar=1,format=yuv420p[{v_label}]"
            )
            filter_parts.append(v_chain)

        concat_inputs = "".join([f"[v{idx}]" for idx in range(len(asset_ids))])
        filter_parts.append(f"{concat_inputs}concat=n={len(asset_ids)}:v=1:a=0[vout]")
        filter_complex = ";".join(filter_parts)

        output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}.mp4")
        ffmpeg_args = args + [
            "-filter_complex",
            filter_complex,
            "-map",
            "[vout]",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            output_path,
        ]
        logs = run_ffmpeg(ffmpeg_args, timeout=settings.slideshow_timeout_seconds())
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        probe_out = _probe_optional(output_path)
        ttl_seconds = settings.asset_ttl_seconds()
        output_asset = _create_output_asset(
            output_path,
            "video/mp4",
            ".mp4",
            asset_ids[0],
            ttl_seconds,
            probe_out,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("images_to_slideshow", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "images_to_slideshow",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": None, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("images_to_slideshow", start_ts, "error", job_id)
        raise
    finally:
        _cleanup_inputs(input_paths)
        if "output_path" in locals() and os.path.exists(output_path):
            os.remove(output_path)


def images_to_slideshow_ken_burns_job(
    asset_ids: list[str],
    duration_per_image: float | None,
    durations: list[float] | None,
    width: int | None,
    height: int | None,
    fps: int | None,
    background_color: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started(
        "images_to_slideshow_ken_burns", job_id, asset_ids[0] if asset_ids else None
    )
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    if not asset_ids:
        raise JobError("asset_ids is required")
    if len(asset_ids) > settings.max_slideshow_images:
        raise JobError("Too many images for slideshow")

    if durations is not None and len(durations) != len(asset_ids):
        raise JobError("durations length must match asset_ids length")
    duration_per_image = float(duration_per_image or settings.default_image_duration_sec)
    if duration_per_image <= 0:
        raise JobError("duration_per_image must be > 0")
    resolved_durations = (
        [float(value) for value in durations] if durations is not None else [duration_per_image] * len(asset_ids)
    )
    total_duration = sum(resolved_durations)
    if total_duration > settings.max_duration_seconds:
        raise JobError("Output exceeds max duration")

    width = int(width) if width else settings.default_image_width
    height = int(height) if height else settings.default_image_height
    if width <= 0 or height <= 0:
        raise JobError("Invalid output dimensions")
    fps = int(fps) if fps else settings.default_video_fps
    if fps <= 0:
        raise JobError("Invalid fps")
    pad_color = sanitize_color(background_color, "black")

    _ensure_temp_dir()
    input_paths: list[tuple[str, bool]] = []
    try:
        for asset_id in asset_ids:
            asset = _get_asset_or_error(asset_id)
            _ensure_image_asset(asset)
            path, cleanup = _resolve_input_path(asset)
            input_paths.append((path, cleanup))

        args: list[str] = []
        for idx, (path, _) in enumerate(input_paths):
            args += ["-loop", "1", "-t", str(resolved_durations[idx]), "-i", path]

        filter_parts: list[str] = []
        for idx, duration in enumerate(resolved_durations):
            frames = max(1, int(duration * fps))
            v_label = f"v{idx}"
            zoompan = (
                "zoompan=z='min(zoom+0.0015,1.1)':"
                "x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':"
                f"d={frames}:s={width}x{height}:fps={fps}"
            )
            v_chain = (
                f"[{idx}:v]scale={width}:{height}:force_original_aspect_ratio=increase,"
                f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:color={pad_color},"
                f"{zoompan},format=yuv420p[{v_label}]"
            )
            filter_parts.append(v_chain)

        concat_inputs = "".join([f"[v{idx}]" for idx in range(len(asset_ids))])
        filter_parts.append(f"{concat_inputs}concat=n={len(asset_ids)}:v=1:a=0[vout]")
        filter_complex = ";".join(filter_parts)

        output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}.mp4")
        ffmpeg_args = args + [
            "-filter_complex",
            filter_complex,
            "-map",
            "[vout]",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "23",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            output_path,
        ]
        logs = run_ffmpeg(ffmpeg_args, timeout=settings.slideshow_timeout_seconds())
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        probe_out = _probe_optional(output_path)
        ttl_seconds = settings.asset_ttl_seconds()
        output_asset = _create_output_asset(
            output_path,
            "video/mp4",
            ".mp4",
            asset_ids[0],
            ttl_seconds,
            probe_out,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("images_to_slideshow_ken_burns", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "images_to_slideshow_ken_burns",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": None, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("images_to_slideshow_ken_burns", start_ts, "error", job_id)
        raise
    finally:
        _cleanup_inputs(input_paths)
        if "output_path" in locals() and os.path.exists(output_path):
            os.remove(output_path)


def audio_normalize_job(
    asset_id: str,
    output_format: str,
    target_lufs: float | None,
    lra: float | None,
    true_peak: float | None,
    bitrate: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("audio_normalize", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    asset = _get_asset_or_error(asset_id)
    _ensure_temp_dir()
    input_path, cleanup = _resolve_input_path(asset)
    logs = ""
    output_path = None
    try:
        probe = _probe_or_error(input_path)
        if not _has_audio_stream(probe):
            raise JobError("Input has no audio stream")

        target_lufs = float(target_lufs if target_lufs is not None else settings.audio_norm_i)
        lra = float(lra if lra is not None else settings.audio_norm_lra)
        true_peak = float(true_peak if true_peak is not None else settings.audio_norm_tp)
        if target_lufs > -5 or target_lufs < -30:
            raise JobError("target_lufs out of range")
        if lra <= 0 or lra > 20:
            raise JobError("lra out of range")
        if true_peak > 0 or true_peak < -10:
            raise JobError("true_peak out of range")

        mime_type, codec_args, ext = _audio_output_config(output_format, bitrate)
        output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}{ext}")
        loudnorm = f"loudnorm=I={target_lufs}:LRA={lra}:TP={true_peak}"
        args = ["-i", input_path, "-vn", "-af", loudnorm] + codec_args + [output_path]
        logs = run_ffmpeg(args, timeout=settings.audio_timeout_seconds())
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        probe_out = _probe_optional(output_path)
        ttl_seconds = settings.asset_ttl_seconds()
        output_asset = _create_output_asset(
            output_path,
            mime_type,
            ext,
            asset_id,
            ttl_seconds,
            probe_out,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("audio_normalize", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "audio_normalize",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": logs, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("audio_normalize", start_ts, "error", job_id)
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if output_path and os.path.exists(output_path):
            os.remove(output_path)


def audio_mix_job(
    asset_ids: list[str],
    output_format: str,
    volumes: list[float] | None,
    normalize: bool | None,
    duration_mode: str | None,
    bitrate: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("audio_mix", job_id, asset_ids[0] if asset_ids else None)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    if not asset_ids:
        raise JobError("asset_ids is required")
    if len(asset_ids) > settings.max_audio_tracks:
        raise JobError("Too many audio tracks")
    if volumes is not None and len(volumes) != len(asset_ids):
        raise JobError("volumes length must match asset_ids length")
    duration_mode = (duration_mode or "longest").lower()
    if duration_mode not in {"longest", "shortest", "first"}:
        raise JobError("duration_mode must be longest, shortest, or first")
    normalize = bool(normalize) if normalize is not None else False

    _ensure_temp_dir()
    input_paths: list[tuple[str, bool]] = []
    logs = ""
    output_path = None
    try:
        for asset_id in asset_ids:
            asset = _get_asset_or_error(asset_id)
            path, cleanup = _resolve_input_path(asset)
            probe = _probe_or_error(path)
            if not _has_audio_stream(probe):
                raise JobError("Input has no audio stream")
            input_paths.append((path, cleanup))

        args: list[str] = []
        for path, _ in input_paths:
            args += ["-i", path]

        filter_parts: list[str] = []
        for idx in range(len(asset_ids)):
            label = f"a{idx}"
            chain = (
                f"[{idx}:a]aresample={settings.audio_sample_rate},"
                "aformat=sample_fmts=fltp:channel_layouts=stereo"
            )
            if volumes and volumes[idx] is not None:
                vol = float(volumes[idx])
                if vol < 0 or vol > 4:
                    raise JobError("volume out of range")
                chain += f",volume={vol}"
            chain += f"[{label}]"
            filter_parts.append(chain)

        mix_inputs = "".join([f"[a{idx}]" for idx in range(len(asset_ids))])
        mix_filter = (
            f"{mix_inputs}amix=inputs={len(asset_ids)}:duration={duration_mode}:"
            f"dropout_transition=0.5:normalize={'1' if normalize else '0'}[aout]"
        )
        filter_parts.append(mix_filter)
        filter_complex = ";".join(filter_parts)

        mime_type, codec_args, ext = _audio_output_config(output_format, bitrate)
        output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}{ext}")
        ffmpeg_args = args + [
            "-filter_complex",
            filter_complex,
            "-map",
            "[aout]",
        ] + codec_args + [output_path]
        logs = run_ffmpeg(ffmpeg_args, timeout=settings.audio_timeout_seconds())
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        probe_out = _probe_optional(output_path)
        ttl_seconds = settings.asset_ttl_seconds()
        output_asset = _create_output_asset(
            output_path,
            mime_type,
            ext,
            asset_ids[0],
            ttl_seconds,
            probe_out,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("audio_mix", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "audio_mix",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": logs, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("audio_mix", start_ts, "error", job_id)
        raise
    finally:
        _cleanup_inputs(input_paths)
        if output_path and os.path.exists(output_path):
            os.remove(output_path)


def audio_duck_job(
    voice_asset_id: str,
    music_asset_id: str,
    output_format: str,
    ratio: float | None,
    threshold: float | None,
    attack_ms: int | None,
    release_ms: int | None,
    music_gain: float | None,
    bitrate: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("audio_duck", job_id, voice_asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    _ensure_temp_dir()
    inputs: list[tuple[str, bool]] = []
    logs = ""
    output_path = None
    try:
        voice_asset = _get_asset_or_error(voice_asset_id)
        music_asset = _get_asset_or_error(music_asset_id)
        voice_path, voice_cleanup = _resolve_input_path(voice_asset)
        music_path, music_cleanup = _resolve_input_path(music_asset)
        inputs.extend([(music_path, music_cleanup), (voice_path, voice_cleanup)])
        voice_probe = _probe_or_error(voice_path)
        music_probe = _probe_or_error(music_path)
        if not _has_audio_stream(voice_probe):
            raise JobError("voice_asset_id has no audio stream")
        if not _has_audio_stream(music_probe):
            raise JobError("music_asset_id has no audio stream")

        ratio = float(ratio if ratio is not None else settings.ducking_ratio)
        threshold = float(threshold if threshold is not None else settings.ducking_threshold)
        attack_ms = int(attack_ms if attack_ms is not None else settings.ducking_attack_ms)
        release_ms = int(release_ms if release_ms is not None else settings.ducking_release_ms)
        music_gain = float(music_gain if music_gain is not None else settings.ducking_music_gain)
        if ratio < 1 or ratio > 20:
            raise JobError("ratio out of range")
        if threshold <= 0 or threshold > 1:
            raise JobError("threshold out of range")
        if attack_ms < 1 or attack_ms > 2000:
            raise JobError("attack_ms out of range")
        if release_ms < 1 or release_ms > 5000:
            raise JobError("release_ms out of range")
        if music_gain <= 0 or music_gain > 4:
            raise JobError("music_gain out of range")

        filter_complex = (
            f"[0:a]aresample={settings.audio_sample_rate},"
            "aformat=sample_fmts=fltp:channel_layouts=stereo,"
            f"volume={music_gain}[music];"
            f"[1:a]aresample={settings.audio_sample_rate},"
            "aformat=sample_fmts=fltp:channel_layouts=stereo[voice];"
            f"[music][voice]sidechaincompress=threshold={threshold}:"
            f"ratio={ratio}:attack={attack_ms}:release={release_ms}[ducked];"
            "[ducked][voice]amix=inputs=2:normalize=0:duration=longest[aout]"
        )

        mime_type, codec_args, ext = _audio_output_config(output_format, bitrate)
        output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}{ext}")
        ffmpeg_args = [
            "-i",
            inputs[0][0],
            "-i",
            inputs[1][0],
            "-filter_complex",
            filter_complex,
            "-map",
            "[aout]",
        ] + codec_args + [output_path]
        logs = run_ffmpeg(ffmpeg_args, timeout=settings.audio_timeout_seconds())
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        probe_out = _probe_optional(output_path)
        ttl_seconds = settings.asset_ttl_seconds()
        output_asset = _create_output_asset(
            output_path,
            mime_type,
            ext,
            voice_asset_id,
            ttl_seconds,
            probe_out,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("audio_duck", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "audio_duck",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": logs, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("audio_duck", start_ts, "error", job_id)
        raise
    finally:
        _cleanup_inputs(inputs)
        if output_path and os.path.exists(output_path):
            os.remove(output_path)


def audio_mix_with_background_job(
    voice_asset_id: str,
    music_asset_id: str,
    output_format: str,
    ducking: bool | None,
    ratio: float | None,
    threshold: float | None,
    attack_ms: int | None,
    release_ms: int | None,
    music_gain: float | None,
    voice_gain: float | None,
    bitrate: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("audio_mix_with_background", job_id, voice_asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    ducking = True if ducking is None else bool(ducking)
    voice_gain = float(voice_gain) if voice_gain is not None else 1.0
    if voice_gain <= 0 or voice_gain > 4:
        raise JobError("voice_gain out of range")

    try:
        if ducking:
            output_asset = audio_duck_job(
                voice_asset_id,
                music_asset_id,
                output_format,
                ratio,
                threshold,
                attack_ms,
                release_ms,
                music_gain,
                bitrate,
                cache_key=cache_key,
                job_id_override="",
            )
        else:
            output_asset = audio_mix_job(
                [music_asset_id, voice_asset_id],
                output_format,
                [music_gain or settings.ducking_music_gain, voice_gain],
                False,
                "longest",
                bitrate,
                cache_key=cache_key,
                job_id_override="",
            )

        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": "audio mix complete",
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("audio_mix_with_background", start_ts, "success", job_id)
        return output_asset
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": None, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("audio_mix_with_background", start_ts, "error", job_id)
        raise


def audio_fade_job(
    asset_id: str,
    output_format: str,
    fade_in_sec: float | None,
    fade_out_sec: float | None,
    fade_out_start: float | None,
    bitrate: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("audio_fade", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    asset = _get_asset_or_error(asset_id)
    _ensure_temp_dir()
    input_path, cleanup = _resolve_input_path(asset)
    output_path = None
    logs = ""
    try:
        probe = _probe_or_error(input_path)
        if not _has_audio_stream(probe):
            raise JobError("Input has no audio stream")
        duration = probe.get("duration_sec")
        if not duration:
            raise JobError("Duration missing for audio")
        duration = float(duration)

        fade_in_sec = float(fade_in_sec) if fade_in_sec is not None else settings.audio_fade_default_sec
        fade_out_sec = float(fade_out_sec) if fade_out_sec is not None else settings.audio_fade_default_sec
        if fade_in_sec < 0 or fade_out_sec < 0:
            raise JobError("fade durations must be >= 0")
        if fade_out_sec > 0:
            if fade_out_start is None:
                fade_out_start = max(0.0, duration - fade_out_sec)
            if fade_out_start < 0 or fade_out_start >= duration:
                raise JobError("fade_out_start out of range")

        filters: list[str] = []
        if fade_in_sec > 0:
            filters.append(f"afade=t=in:st=0:d={fade_in_sec}")
        if fade_out_sec > 0:
            filters.append(f"afade=t=out:st={fade_out_start}:d={fade_out_sec}")
        if not filters:
            raise JobError("No fade requested")

        mime_type, codec_args, ext = _audio_output_config(output_format, bitrate)
        output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}{ext}")
        args = ["-i", input_path, "-vn", "-af", ",".join(filters)] + codec_args + [output_path]
        logs = run_ffmpeg(args, timeout=settings.audio_timeout_seconds())
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        probe_out = _probe_optional(output_path)
        ttl_seconds = settings.asset_ttl_seconds()
        output_asset = _create_output_asset(
            output_path,
            mime_type,
            ext,
            asset_id,
            ttl_seconds,
            probe_out,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("audio_fade", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "audio_fade",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": logs, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("audio_fade", start_ts, "error", job_id)
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if output_path and os.path.exists(output_path):
            os.remove(output_path)


def audio_trim_silence_job(
    asset_id: str,
    output_format: str,
    min_silence_sec: float | None,
    threshold_db: float | None,
    trim_leading: bool | None,
    trim_trailing: bool | None,
    bitrate: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("audio_trim_silence", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    asset = _get_asset_or_error(asset_id)
    _ensure_temp_dir()
    input_path, cleanup = _resolve_input_path(asset)
    output_path = None
    logs = ""
    try:
        probe = _probe_or_error(input_path)
        if not _has_audio_stream(probe):
            raise JobError("Input has no audio stream")

        min_silence_sec = float(
            min_silence_sec if min_silence_sec is not None else settings.audio_min_silence_sec
        )
        threshold_db = float(
            threshold_db if threshold_db is not None else settings.audio_silence_db
        )
        trim_leading = True if trim_leading is None else bool(trim_leading)
        trim_trailing = True if trim_trailing is None else bool(trim_trailing)
        if min_silence_sec <= 0:
            raise JobError("min_silence_sec must be > 0")
        if not trim_leading and not trim_trailing:
            raise JobError("trim_leading or trim_trailing must be true")

        start_periods = 1 if trim_leading else 0
        stop_periods = 1 if trim_trailing else 0
        silenceremove = (
            f"silenceremove=start_periods={start_periods}:"
            f"start_duration={min_silence_sec}:start_threshold={threshold_db}dB:"
            f"stop_periods={stop_periods}:stop_duration={min_silence_sec}:"
            f"stop_threshold={threshold_db}dB"
        )

        mime_type, codec_args, ext = _audio_output_config(output_format, bitrate)
        output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}{ext}")
        args = ["-i", input_path, "-vn", "-af", silenceremove] + codec_args + [output_path]
        logs = run_ffmpeg(args, timeout=settings.audio_timeout_seconds())
        update_job(job_id, {"progress": 80, "updated_at": utc_now_iso()})
        _enforce_output_size(output_path)
        probe_out = _probe_optional(output_path)
        ttl_seconds = settings.asset_ttl_seconds()
        output_asset = _create_output_asset(
            output_path,
            mime_type,
            ext,
            asset_id,
            ttl_seconds,
            probe_out,
        )
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [output_asset["asset_id"]],
                "logs_short": logs,
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("audio_trim_silence", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "audio_trim_silence",
                },
                settings.asset_ttl_seconds(),
            )
        return output_asset
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": logs, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("audio_trim_silence", start_ts, "error", job_id)
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if output_path and os.path.exists(output_path):
            os.remove(output_path)


def template_apply_job(
    asset_id: str,
    template_name: str,
    variables: dict[str, Any] | None,
    brand_kit_id: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("template_apply", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 5, "cache_key": cache_key},
    )

    asset = _get_asset_or_error(asset_id)
    template = get_template(template_name)
    layers = list(template.get("layers", []))
    if len(layers) > settings.max_template_layers:
        raise JobError("Template exceeds max layers")

    vars_in = variables or {}
    if not isinstance(vars_in, dict):
        raise JobError("variables must be an object")
    merged_vars = dict(template.get("defaults", {}))
    merged_vars.update(vars_in)

    brand_kit = None
    if brand_kit_id:
        brand_kit = get_brand_kit(brand_kit_id)
        if not brand_kit:
            raise JobError("brand_kit_id not found")

    include_brand_logo = bool(template.get("include_brand_logo"))
    has_logo_layer = any(layer.get("type") == "logo" for layer in layers)
    if brand_kit and include_brand_logo and not has_logo_layer:
        layers.append({"type": "logo", "optional": True})

    current_asset_id = asset_id
    text_layer_count = 0
    applied_layers = 0

    try:
        for idx, layer in enumerate(layers):
            layer_type = (layer.get("type") or "").strip().lower()
            if not layer_type:
                raise JobError("Layer type is required")
            if layer_type == "text":
                text_layer_count += 1
                if text_layer_count > settings.max_template_text_layers:
                    raise JobError("Template exceeds max text layers")

            optional_layer = bool(layer.get("optional"))
            params = {k: v for k, v in layer.items() if k not in {"type", "optional"}}

            if layer_type == "transcode" and brand_kit:
                params.setdefault("preset", brand_kit.get("default_preset"))
            if layer_type == "text" and brand_kit:
                params.setdefault("font_name", brand_kit.get("font_name"))
                params.setdefault("font_asset_id", brand_kit.get("font_asset_id"))
                params.setdefault("font_color", brand_kit.get("font_color"))
                params.setdefault("box_color", brand_kit.get("box_color"))
                if "background_box" not in params and brand_kit.get("background_box") is not None:
                    params["background_box"] = brand_kit.get("background_box")
                params.setdefault("position", brand_kit.get("text_position"))
            if layer_type == "logo" and brand_kit:
                params.setdefault("logo_asset_id", brand_kit.get("logo_asset_id"))
                params.setdefault("logo_key", brand_kit.get("logo_key"))
                params.setdefault("position", brand_kit.get("logo_position"))
                params.setdefault("scale_pct", brand_kit.get("logo_scale_pct"))
                params.setdefault("opacity", brand_kit.get("logo_opacity"))

            missing_vars: list[str] = []
            for key, value in list(params.items()):
                if isinstance(value, str):
                    placeholders = _extract_placeholders(value)
                    for name in placeholders:
                        if name not in merged_vars:
                            missing_vars.append(name)
                    if missing_vars:
                        continue
                    params[key] = value.format(**merged_vars)

            if missing_vars:
                if optional_layer:
                    continue
                raise JobError(f"Missing template variables: {', '.join(sorted(set(missing_vars)))}")

            if layer_type == "text":
                text_value = str(params.get("text") or "").strip()
                if not text_value:
                    if optional_layer:
                        continue
                    raise JobError("Text layer requires text")
            if layer_type == "logo":
                if not params.get("logo_asset_id") and not params.get("logo_key"):
                    if optional_layer:
                        continue
                    raise JobError("Logo layer requires logo_asset_id or logo_key")

            layer_cache_key = build_cache_key(
                f"layer:{layer_type}",
                {
                    "input_asset_id": current_asset_id,
                    "params": params,
                    "brand_kit_id": brand_kit_id,
                    "template": template_name,
                },
            )
            cached_output_id = _resolve_cached_output(layer_cache_key)
            if cached_output_id:
                current_asset_id = cached_output_id
                applied_layers += 1
                update_job(
                    job_id,
                    {
                        "progress": 5 + int(85 * (applied_layers / max(len(layers), 1))),
                        "updated_at": utc_now_iso(),
                    },
                )
                continue

            output_asset: dict[str, Any]
            if layer_type == "transcode":
                preset = params.get("preset")
                if not preset:
                    raise JobError("transcode layer requires preset")
                output_asset = transcode_job(
                    current_asset_id, preset, cache_key=layer_cache_key, job_id_override=""
                )
            elif layer_type == "text":
                output_asset = video_add_text_job(
                    current_asset_id,
                    params.get("text"),
                    params.get("position"),
                    params.get("font_size"),
                    params.get("font_color"),
                    params.get("background_box"),
                    params.get("box_color"),
                    params.get("box_border_width"),
                    params.get("font_name"),
                    params.get("font_asset_id"),
                    cache_key=layer_cache_key,
                    job_id_override="",
                )
            elif layer_type == "logo":
                output_asset = video_add_logo_job(
                    current_asset_id,
                    params.get("logo_asset_id"),
                    params.get("logo_key"),
                    params.get("position"),
                    params.get("scale_pct"),
                    params.get("opacity"),
                    cache_key=layer_cache_key,
                    job_id_override="",
                )
            elif layer_type == "trim":
                output_asset = trim_job(
                    current_asset_id,
                    float(params.get("start_sec", 0)),
                    float(params.get("end_sec", 0)),
                    bool(params.get("reencode", True)),
                    cache_key=layer_cache_key,
                    job_id_override="",
                )
            elif layer_type == "audio_normalize":
                output_asset = audio_normalize_job(
                    current_asset_id,
                    params.get("output_format", "m4a"),
                    params.get("target_lufs"),
                    params.get("lra"),
                    params.get("true_peak"),
                    params.get("bitrate"),
                    cache_key=layer_cache_key,
                    job_id_override="",
                )
            elif layer_type == "audio_fade":
                output_asset = audio_fade_job(
                    current_asset_id,
                    params.get("output_format", "m4a"),
                    params.get("fade_in_sec"),
                    params.get("fade_out_sec"),
                    params.get("fade_out_start"),
                    params.get("bitrate"),
                    cache_key=layer_cache_key,
                    job_id_override="",
                )
            elif layer_type == "audio_trim_silence":
                output_asset = audio_trim_silence_job(
                    current_asset_id,
                    params.get("output_format", "m4a"),
                    params.get("min_silence_sec"),
                    params.get("threshold_db"),
                    params.get("trim_leading"),
                    params.get("trim_trailing"),
                    params.get("bitrate"),
                    cache_key=layer_cache_key,
                    job_id_override="",
                )
            else:
                raise JobError(f"Unsupported template layer type: {layer_type}")

            current_asset_id = output_asset["asset_id"]
            applied_layers += 1
            update_job(
                job_id,
                {
                    "progress": 5 + int(85 * (applied_layers / max(len(layers), 1))),
                    "updated_at": utc_now_iso(),
                },
            )

        if applied_layers == 0:
            current_asset_id = asset_id

        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [current_asset_id],
                "logs_short": "template applied",
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("template_apply", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [current_asset_id],
                    "created_at": utc_now_iso(),
                    "job_type": "template_apply",
                },
                settings.asset_ttl_seconds(),
            )
        return get_asset(current_asset_id) or asset
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": None, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("template_apply", start_ts, "error", job_id)
        raise


def brand_kit_apply_job(
    asset_id: str,
    brand_kit_id: str,
    text: str | None,
    position: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("brand_kit_apply", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    asset = _get_asset_or_error(asset_id)
    brand_kit = get_brand_kit(brand_kit_id)
    if not brand_kit:
        raise JobError("brand_kit_id not found")

    current_asset_id = asset_id
    applied_layers = 0
    try:
        if brand_kit.get("logo_asset_id") or brand_kit.get("logo_key"):
            output_asset = video_add_logo_job(
                current_asset_id,
                brand_kit.get("logo_asset_id"),
                brand_kit.get("logo_key"),
                brand_kit.get("logo_position"),
                brand_kit.get("logo_scale_pct"),
                brand_kit.get("logo_opacity"),
                cache_key=None,
                job_id_override="",
            )
            current_asset_id = output_asset["asset_id"]
            applied_layers += 1
            update_job(job_id, {"progress": 55, "updated_at": utc_now_iso()})

        if text:
            output_asset = video_add_text_job(
                current_asset_id,
                text,
                position or brand_kit.get("text_position"),
                None,
                brand_kit.get("font_color"),
                brand_kit.get("background_box"),
                brand_kit.get("box_color"),
                None,
                brand_kit.get("font_name"),
                brand_kit.get("font_asset_id"),
                cache_key=None,
                job_id_override="",
            )
            current_asset_id = output_asset["asset_id"]
            applied_layers += 1
            update_job(job_id, {"progress": 85, "updated_at": utc_now_iso()})

        if applied_layers == 0:
            raise JobError("Brand kit has no logo and no text provided")

        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [current_asset_id],
                "logs_short": "brand kit applied",
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("brand_kit_apply", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [current_asset_id],
                    "created_at": utc_now_iso(),
                    "job_type": "brand_kit_apply",
                },
                settings.asset_ttl_seconds(),
            )
        return get_asset(current_asset_id) or asset
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": None, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("brand_kit_apply", start_ts, "error", job_id)
        raise


def batch_export_job(
    asset_id: str,
    presets: list[str],
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("batch_export", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    if not presets:
        raise JobError("presets list is required")
    if len(presets) > settings.max_batch_presets:
        raise JobError("Too many presets for batch export")
    _get_asset_or_error(asset_id)

    output_ids: list[str] = []
    try:
        for idx, preset in enumerate(presets):
            transcode_cache_key = build_cache_key(
                "ffmpeg:transcode",
                {"asset_id": asset_id, "preset": preset},
            )
            cached_output = _resolve_cached_output(transcode_cache_key)
            if cached_output:
                output_ids.append(cached_output)
            else:
                output_asset = transcode_job(
                    asset_id, preset, cache_key=transcode_cache_key, job_id_override=""
                )
                output_ids.append(output_asset["asset_id"])

            update_job(
                job_id,
                {
                    "progress": 10 + int(80 * ((idx + 1) / len(presets))),
                    "updated_at": utc_now_iso(),
                },
            )

        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": output_ids,
                "logs_short": "batch export complete",
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("batch_export", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": output_ids,
                    "created_at": utc_now_iso(),
                    "job_type": "batch_export",
                },
                settings.asset_ttl_seconds(),
            )
        return {"output_asset_ids": output_ids}
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": None, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("batch_export", start_ts, "error", job_id)
        raise


def campaign_process_job(
    asset_ids: list[str],
    presets: list[str] | None,
    template_name: str | None,
    variables: dict[str, Any] | None,
    brand_kit_id: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("campaign_process", job_id, asset_ids[0] if asset_ids else None)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 5, "cache_key": cache_key},
    )

    if not asset_ids:
        raise JobError("asset_ids is required")
    if len(asset_ids) > settings.max_batch_assets:
        raise JobError("Too many assets for campaign")

    outputs: dict[str, list[str]] = {}
    try:
        for idx, asset_id in enumerate(asset_ids):
            _get_asset_or_error(asset_id)
            base_asset_id = asset_id

            if template_name:
                template_cache_key = build_cache_key(
                    "ffmpeg:template_apply",
                    {
                        "asset_id": asset_id,
                        "template_name": template_name,
                        "variables": variables or {},
                        "brand_kit_id": brand_kit_id,
                    },
                )
                cached_output = _resolve_cached_output(template_cache_key)
                if cached_output:
                    base_asset_id = cached_output
                else:
                    output_asset = template_apply_job(
                        asset_id,
                        template_name,
                        variables or {},
                        brand_kit_id,
                        cache_key=template_cache_key,
                        job_id_override="",
                    )
                    base_asset_id = output_asset["asset_id"]

            if presets:
                batch_cache_key = build_cache_key(
                    "ffmpeg:batch_export",
                    {"asset_id": base_asset_id, "presets": presets},
                )
                cached_ids = _resolve_cached_outputs_list(batch_cache_key)
                if cached_ids:
                    outputs[asset_id] = list(cached_ids)
                else:
                    batch_result = batch_export_job(
                        base_asset_id, presets, cache_key=batch_cache_key, job_id_override=""
                    )
                    outputs[asset_id] = list(batch_result.get("output_asset_ids") or [])
            else:
                outputs[asset_id] = [base_asset_id]

            update_job(
                job_id,
                {
                    "progress": 5 + int(90 * ((idx + 1) / len(asset_ids))),
                    "updated_at": utc_now_iso(),
                },
            )

        flattened = []
        for items in outputs.values():
            flattened.extend(items)

        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": flattened,
                "logs_short": "campaign complete",
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("campaign_process", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": flattened,
                    "created_at": utc_now_iso(),
                    "job_type": "campaign_process",
                    "outputs_by_asset": outputs,
                },
                settings.asset_ttl_seconds(),
            )
        return {"outputs_by_asset": outputs, "output_asset_ids": flattened}
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": None, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("campaign_process", start_ts, "error", job_id)
        raise


def workflow_job(
    workflow: dict[str, Any],
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("workflow_run", job_id, None)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 5, "cache_key": cache_key},
    )

    if not isinstance(workflow, dict):
        raise JobError("workflow must be an object")
    nodes = workflow.get("nodes") or []
    if not isinstance(nodes, list) or not nodes:
        raise JobError("workflow.nodes must be a non-empty list")
    if len(nodes) > settings.max_workflow_nodes:
        raise JobError("workflow exceeds max nodes")

    outputs: dict[str, str] = {}
    remaining = {node.get("id"): node for node in nodes if node.get("id")}
    if len(remaining) != len(nodes):
        raise JobError("Each workflow node requires an id")

    try:
        while remaining:
            progressed = False
            for node_id, node in list(remaining.items()):
                node_type = (node.get("type") or "").strip().lower()
                params = node.get("params") or {}
                if not isinstance(params, dict):
                    raise JobError("workflow node params must be an object")

                input_ref = node.get("input")
                inputs_ref = node.get("inputs")
                resolved_input = None
                resolved_inputs = None
                deps: list[str] = []
                if input_ref is not None:
                    deps = [input_ref] if isinstance(input_ref, str) else []
                if inputs_ref is not None:
                    if not isinstance(inputs_ref, list):
                        raise JobError("inputs must be a list")
                    deps = [item for item in inputs_ref if isinstance(item, str)]

                unmet = [dep for dep in deps if dep in remaining]
                if unmet:
                    continue

                if input_ref is not None:
                    if isinstance(input_ref, str) and input_ref in outputs:
                        resolved_input = outputs[input_ref]
                    else:
                        resolved_input = input_ref
                if inputs_ref is not None:
                    resolved_inputs = []
                    for ref in inputs_ref:
                        if isinstance(ref, str) and ref in outputs:
                            resolved_inputs.append(outputs[ref])
                        else:
                            resolved_inputs.append(ref)

                if node_type in {"transcode", "trim", "video_add_text", "video_add_logo"} and not resolved_input:
                    raise JobError(f"{node_type} requires input")
                if node_type in {"video_concat", "audio_mix", "images_to_slideshow", "images_to_slideshow_ken_burns"} and not resolved_inputs:
                    raise JobError(f"{node_type} requires inputs")

                cache_payload = {
                    "node_id": node_id,
                    "type": node_type,
                    "input": resolved_input,
                    "inputs": resolved_inputs,
                    "params": params,
                }
                node_cache_key = build_cache_key(f"workflow:{node_type}", cache_payload)
                cached_output = _resolve_cached_output(node_cache_key)
                if cached_output:
                    outputs[node_id] = cached_output
                else:
                    if node_type == "transcode":
                        output_asset = transcode_job(
                            resolved_input,
                            params.get("preset"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "trim":
                        output_asset = trim_job(
                            resolved_input,
                            float(params.get("start_sec", 0)),
                            float(params.get("end_sec", 0)),
                            bool(params.get("reencode", True)),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "video_add_text":
                        output_asset = video_add_text_job(
                            resolved_input,
                            params.get("text"),
                            params.get("position"),
                            params.get("font_size"),
                            params.get("font_color"),
                            params.get("background_box"),
                            params.get("box_color"),
                            params.get("box_border_width"),
                            params.get("font_name"),
                            params.get("font_asset_id"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "video_add_logo":
                        output_asset = video_add_logo_job(
                            resolved_input,
                            params.get("logo_asset_id"),
                            params.get("logo_key"),
                            params.get("position"),
                            params.get("scale_pct"),
                            params.get("opacity"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "video_concat":
                        output_asset = video_concat_job(
                            resolved_inputs,
                            params.get("transition"),
                            params.get("transition_duration"),
                            params.get("target_width"),
                            params.get("target_height"),
                            params.get("include_audio"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "image_to_video":
                        output_asset = image_to_video_job(
                            resolved_input,
                            float(params.get("duration_sec", settings.default_image_duration_sec)),
                            params.get("width"),
                            params.get("height"),
                            params.get("fps"),
                            params.get("background_color"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "images_to_slideshow":
                        output_asset = images_to_slideshow_job(
                            resolved_inputs,
                            params.get("duration_per_image"),
                            params.get("durations"),
                            params.get("width"),
                            params.get("height"),
                            params.get("fps"),
                            params.get("background_color"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "images_to_slideshow_ken_burns":
                        output_asset = images_to_slideshow_ken_burns_job(
                            resolved_inputs,
                            params.get("duration_per_image"),
                            params.get("durations"),
                            params.get("width"),
                            params.get("height"),
                            params.get("fps"),
                            params.get("background_color"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "audio_normalize":
                        output_asset = audio_normalize_job(
                            resolved_input,
                            params.get("output_format", "m4a"),
                            params.get("target_lufs"),
                            params.get("lra"),
                            params.get("true_peak"),
                            params.get("bitrate"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "audio_mix":
                        output_asset = audio_mix_job(
                            resolved_inputs,
                            params.get("output_format", "m4a"),
                            params.get("volumes"),
                            params.get("normalize"),
                            params.get("duration_mode"),
                            params.get("bitrate"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "audio_duck":
                        voice_ref = params.get("voice_asset_id")
                        music_ref = params.get("music_asset_id")
                        if isinstance(voice_ref, str) and voice_ref in outputs:
                            voice_ref = outputs[voice_ref]
                        if isinstance(music_ref, str) and music_ref in outputs:
                            music_ref = outputs[music_ref]
                        output_asset = audio_duck_job(
                            voice_ref,
                            music_ref,
                            params.get("output_format", "m4a"),
                            params.get("ratio"),
                            params.get("threshold"),
                            params.get("attack_ms"),
                            params.get("release_ms"),
                            params.get("music_gain"),
                            params.get("bitrate"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "audio_mix_with_background":
                        voice_ref = params.get("voice_asset_id")
                        music_ref = params.get("music_asset_id")
                        if isinstance(voice_ref, str) and voice_ref in outputs:
                            voice_ref = outputs[voice_ref]
                        if isinstance(music_ref, str) and music_ref in outputs:
                            music_ref = outputs[music_ref]
                        output_asset = audio_mix_with_background_job(
                            voice_ref,
                            music_ref,
                            params.get("output_format", "m4a"),
                            params.get("ducking"),
                            params.get("ratio"),
                            params.get("threshold"),
                            params.get("attack_ms"),
                            params.get("release_ms"),
                            params.get("music_gain"),
                            params.get("voice_gain"),
                            params.get("bitrate"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "audio_fade":
                        output_asset = audio_fade_job(
                            resolved_input,
                            params.get("output_format", "m4a"),
                            params.get("fade_in_sec"),
                            params.get("fade_out_sec"),
                            params.get("fade_out_start"),
                            params.get("bitrate"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "audio_trim_silence":
                        output_asset = audio_trim_silence_job(
                            resolved_input,
                            params.get("output_format", "m4a"),
                            params.get("min_silence_sec"),
                            params.get("threshold_db"),
                            params.get("trim_leading"),
                            params.get("trim_trailing"),
                            params.get("bitrate"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "template_apply":
                        output_asset = template_apply_job(
                            resolved_input,
                            params.get("template_name"),
                            params.get("variables") or {},
                            params.get("brand_kit_id"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    elif node_type == "brand_kit_apply":
                        output_asset = brand_kit_apply_job(
                            resolved_input,
                            params.get("brand_kit_id"),
                            params.get("text"),
                            params.get("position"),
                            cache_key=node_cache_key,
                            job_id_override="",
                        )
                    else:
                        raise JobError(f"Unsupported workflow node type: {node_type}")

                    outputs[node_id] = output_asset["asset_id"]

                progressed = True
                remaining.pop(node_id, None)
                update_job(
                    job_id,
                    {
                        "progress": 5 + int(90 * ((len(outputs)) / len(nodes))),
                        "updated_at": utc_now_iso(),
                    },
                )
            if not progressed:
                raise JobError("Workflow has unresolved dependencies")

        output_nodes = workflow.get("outputs") or []
        output_ids: list[str] = []
        if output_nodes:
            for node_id in output_nodes:
                if node_id in outputs:
                    output_ids.append(outputs[node_id])
        else:
            if outputs:
                output_ids.append(next(reversed(outputs.values())))

        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": output_ids,
                "logs_short": "workflow complete",
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("workflow_run", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": output_ids,
                    "created_at": utc_now_iso(),
                    "job_type": "workflow_run",
                },
                settings.asset_ttl_seconds(),
            )
        return {"output_asset_ids": output_ids, "node_outputs": outputs}
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": None, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("workflow_run", start_ts, "error", job_id)
        raise
