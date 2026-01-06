import hashlib
import json
import os
import re
import string
import tempfile
import uuid
from typing import Any

from rq import get_current_job

from captions import parse_captions_input, resolve_safe_zone_profile
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
from templates import get_template, validate_template_variables
from presets import draft_preset_for, get_preset, map_presets_for_quality
from rubrics import get_rubric, qa_from_report, score_report
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


ITERATE_STRATEGIES = ("balanced", "captions_first", "audio_first", "framing_first")

FAIL_FAST_ERRORS = {
    "no_video_track": {
        "code": "ERR_NO_VIDEO_TRACK",
        "reason": "missing video track",
        "fix": "use a valid video source",
    },
    "no_audio_track": {
        "code": "ERR_NO_AUDIO_TRACK",
        "reason": "missing audio track",
        "fix": "check audio inputs",
    },
    "duration_too_short": {
        "code": "ERR_DURATION_TOO_SHORT",
        "reason": "duration below minimum",
        "fix": "use a longer source clip",
    },
    "resolution_too_low": {
        "code": "ERR_RES_TOO_LOW",
        "reason": "resolution below target",
        "fix": "use higher-resolution media",
    },
    "captions_empty": {
        "code": "ERR_CAPTIONS_EMPTY",
        "reason": "captions empty or unparsable",
        "fix": "check caption input format",
    },
}


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


def _coerce_video_asset_id(asset_id: str) -> str:
    asset = _get_asset_or_error(asset_id)
    mime = _asset_mime(asset)
    if mime.startswith("video/"):
        return asset_id
    if mime.startswith("image/"):
        if not settings.allow_image_ingest:
            raise JobError("Image ingest is disabled")
        cache_key = build_cache_key(
            "ffmpeg:image_to_video",
            {
                "asset_id": asset_id,
                "duration_sec": None,
                "width": None,
                "height": None,
                "fps": None,
                "background_color": None,
            },
        )
        cached = _resolve_cached_output(cache_key)
        if cached:
            return cached
        output_asset = image_to_video_job(
            asset_id,
            None,
            None,
            None,
            None,
            None,
            cache_key=cache_key,
            job_id_override="",
        )
        return output_asset["asset_id"]
    raise JobError("Input asset must be a video or image")


def _coerce_audio_asset_id(asset_id: str) -> str:
    asset = _get_asset_or_error(asset_id)
    mime = _asset_mime(asset)
    if mime.startswith("audio/"):
        return asset_id
    if mime.startswith("video/"):
        cache_key = build_cache_key(
            "ffmpeg:extract_audio",
            {"asset_id": asset_id, "output_format": "m4a"},
        )
        cached = _resolve_cached_output(cache_key)
        if cached:
            return cached
        output_asset = extract_audio_job(
            asset_id,
            "m4a",
            cache_key=cache_key,
            job_id_override="",
        )
        return output_asset["asset_id"]
    raise JobError("Input asset must be audio or video with audio")


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


def _ensure_audio_asset(asset: dict[str, Any]) -> None:
    if not _asset_mime(asset).startswith("audio/"):
        raise JobError("Input asset must be an audio file")


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


CAPTION_POSITIONS = {"bottom_safe", "mid", "top"}


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


def _caption_overlay_xy(position: str, safe_bottom_px: int, safe_top_px: int) -> tuple[str, str]:
    x_expr = "(w-text_w)/2"
    if position == "top":
        y_expr = str(max(safe_top_px, 0))
    elif position == "mid":
        y_expr = "(h-text_h)/2"
    else:
        y_expr = f"h-text_h-{max(safe_bottom_px, 0)}"
    return x_expr, y_expr


def _apply_opacity(color: str, opacity: float) -> str:
    if "@" in color:
        return color
    opacity = min(max(opacity, 0.0), 1.0)
    opacity_str = f"{opacity:.3f}".rstrip("0").rstrip(".")
    return f"{color}@{opacity_str}"


def _parse_loudnorm_json(logs: str) -> dict[str, Any] | None:
    if not logs:
        return None
    candidates = re.findall(r"\{[^{}]*\}", logs, flags=re.DOTALL)
    for raw in reversed(candidates):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        keys = set(payload.keys())
        if {"input_i", "input_tp", "input_lra"} & keys or {"measured_I", "measured_TP", "measured_LRA"} & keys:
            return payload
    return None


def _parse_silencedetect(logs: str, total_duration: float | None) -> float | None:
    if not logs or not total_duration:
        return None
    silence_total = 0.0
    open_start: float | None = None
    for line in logs.splitlines():
        if "silence_start" in line:
            match = re.search(r"silence_start:\\s*([-\\d\\.]+)", line)
            if match:
                try:
                    open_start = float(match.group(1))
                except ValueError:
                    open_start = None
        if "silence_end" in line:
            match = re.search(
                r"silence_end:\\s*([-\\d\\.]+)\\s*\\|\\s*silence_duration:\\s*([-\\d\\.]+)",
                line,
            )
            if match:
                try:
                    duration = float(match.group(2))
                except ValueError:
                    duration = 0.0
                silence_total += max(duration, 0.0)
                open_start = None
    if open_start is not None and total_duration > open_start:
        silence_total += total_duration - open_start
    if total_duration <= 0:
        return None
    return round((silence_total / total_duration) * 100.0, 3)


def _parse_blackdetect(logs: str, total_duration: float | None) -> float | None:
    if not logs or not total_duration:
        return None
    black_total = 0.0
    for line in logs.splitlines():
        if "black_duration" not in line:
            continue
        match = re.search(r"black_duration:\\s*([-\\d\\.]+)", line)
        if match:
            try:
                black_total += float(match.group(1))
            except ValueError:
                continue
    if total_duration <= 0:
        return None
    return round((black_total / total_duration) * 100.0, 3)


def _parse_astats_clipping(logs: str) -> float | None:
    if not logs:
        return None
    samples = 0
    clipped = 0
    for line in logs.splitlines():
        match_samples = re.search(r"Number of samples:\\s*(\\d+)", line)
        if match_samples:
            try:
                samples += int(match_samples.group(1))
            except ValueError:
                continue
        match_clipped = re.search(r"Number of samples clipped:\\s*(\\d+)", line)
        if match_clipped:
            try:
                clipped += int(match_clipped.group(1))
            except ValueError:
                continue
    if samples <= 0:
        return None
    return round((clipped / samples) * 100.0, 5)


def _expected_dims_from_preset(preset_name: str | None) -> tuple[int | None, int | None]:
    if not preset_name:
        return None, None
    try:
        preset = get_preset(preset_name)
    except ValueError:
        return None, None
    profile = preset.get("profile") or {}
    scale = profile.get("scale") or ""
    match = re.search(r"(\\d+)\\s*:\\s*(\\d+)", str(scale))
    if match:
        return int(match.group(1)), int(match.group(2))
    for arg in preset.get("ffmpeg_args", []):
        match = re.search(r"(?:scale|pad|crop)=(\\d+):(\\d+)", str(arg))
        if match:
            return int(match.group(1)), int(match.group(2))
    return None, None


def _caption_metrics(
    segments: list[dict[str, Any]],
    max_chars: int,
    max_lines: int,
    max_words: int,
    position: str,
    safe_bottom_px: int,
    safe_top_px: int,
    padding_px: int,
    font_size: int,
    speed_wpm_max: float,
) -> dict[str, Any]:
    if not segments:
        return {
            "caption_readability_score": None,
            "caption_speed_score": None,
            "caption_speed_wpm": None,
            "safe_zone_violations": None,
        }

    readability_scores: list[float] = []
    speed_values: list[float] = []
    violation_count = 0
    for seg in segments:
        text = str(seg.get("text") or "")
        lines = [line for line in text.split("\\n") if line.strip()]
        if not lines:
            continue
        longest_line = max(len(line) for line in lines)
        line_usage = longest_line / max_chars if max_chars > 0 else 0
        lines_usage = len(lines) / max_lines if max_lines > 0 else 0
        words = len(text.replace("\\n", " ").split())
        words_usage = words / max_words if max_words > 0 else 0

        score = 100.0
        if line_usage > 1:
            score -= (line_usage - 1) * 60
        if lines_usage > 1:
            score -= (lines_usage - 1) * 60
        if words_usage > 1:
            score -= (words_usage - 1) * 40
        readability_scores.append(max(0.0, min(100.0, score)))

        duration = float(seg.get("end", 0)) - float(seg.get("start", 0))
        if duration > 0:
            wpm = (words / duration) * 60.0
            speed_values.append(wpm)

        safe_margin_needed = padding_px + int(font_size * 0.6)
        if position == "bottom_safe" and safe_bottom_px < safe_margin_needed:
            violation_count += 1
        if position == "top" and safe_top_px < safe_margin_needed:
            violation_count += 1

    avg_readability = sum(readability_scores) / len(readability_scores) if readability_scores else None
    avg_wpm = sum(speed_values) / len(speed_values) if speed_values else None
    speed_score = None
    if avg_wpm is not None:
        if avg_wpm <= speed_wpm_max:
            speed_score = 100.0
        else:
            speed_score = max(0.0, 100.0 - (avg_wpm - speed_wpm_max) * 0.6)

    return {
        "caption_readability_score": round(avg_readability, 2) if avg_readability is not None else None,
        "caption_speed_score": round(speed_score, 2) if speed_score is not None else None,
        "caption_speed_wpm": round(avg_wpm, 2) if avg_wpm is not None else None,
        "safe_zone_violations": violation_count,
    }


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


def captions_burn_in_job(
    asset_id: str,
    captions_srt: str | None,
    captions_vtt: str | None,
    words_json: list[dict[str, Any]] | None,
    brand_kit_id: str | None,
    highlight_mode: str | None,
    position: str | None,
    font_size: int | None,
    font_color: str | None,
    box_color: str | None,
    box_opacity: float | None,
    highlight_color: str | None,
    padding_px: int | None,
    max_chars: int | None,
    max_lines: int | None,
    max_words: int | None,
    safe_zone_profile: str | None,
    safe_zone_bottom_px: int | None,
    safe_zone_top_px: int | None,
    font_name: str | None,
    font_asset_id: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("captions_burn_in", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    if sum(bool(value) for value in [captions_srt, captions_vtt, words_json]) != 1:
        raise JobError("Provide exactly one of captions_srt, captions_vtt, words_json")
    if captions_srt is not None and not isinstance(captions_srt, str):
        raise JobError("captions_srt must be a string")
    if captions_vtt is not None and not isinstance(captions_vtt, str):
        raise JobError("captions_vtt must be a string")
    if words_json is not None and not isinstance(words_json, list):
        raise JobError("words_json must be a list")

    if highlight_mode:
        highlight_mode = highlight_mode.strip().lower()
        if highlight_mode not in {"word"}:
            raise JobError("highlight_mode must be 'word' or omitted")

    asset = _get_asset_or_error(asset_id)
    _ensure_video_asset(asset)

    brand_kit = get_brand_kit(brand_kit_id) if brand_kit_id else None
    if brand_kit_id and not brand_kit:
        raise JobError("brand_kit_id not found")

    resolved_max_chars = max_chars if max_chars is not None else (
        brand_kit.get("caption_max_chars") if brand_kit else None
    )
    resolved_max_lines = max_lines if max_lines is not None else (
        brand_kit.get("caption_max_lines") if brand_kit else None
    )
    resolved_max_words = max_words if max_words is not None else (
        brand_kit.get("caption_max_words") if brand_kit else None
    )
    resolved_max_chars = int(resolved_max_chars or settings.caption_max_chars)
    resolved_max_lines = int(resolved_max_lines or settings.caption_max_lines)
    resolved_max_words = int(resolved_max_words or settings.caption_max_words)
    if resolved_max_chars <= 0 or resolved_max_lines <= 0 or resolved_max_words <= 0:
        raise JobError("caption max values must be > 0")

    resolved_position = position if position else (
        brand_kit.get("caption_position") if brand_kit else None
    )
    if not resolved_position:
        resolved_position = settings.caption_position
    resolved_position = sanitize_position(resolved_position, CAPTION_POSITIONS)

    resolved_font_size = font_size if font_size is not None else (
        brand_kit.get("caption_font_size") if brand_kit else None
    )
    resolved_font_size = sanitize_font_size(resolved_font_size, settings.caption_font_size)

    resolved_font_color = font_color if font_color else (
        brand_kit.get("caption_text_color") if brand_kit else None
    )
    resolved_font_color = sanitize_color(resolved_font_color, settings.caption_text_color)

    resolved_box_color = box_color if box_color else (
        brand_kit.get("caption_box_color") if brand_kit else None
    )
    resolved_box_color = sanitize_color(resolved_box_color, settings.caption_box_color)

    resolved_box_opacity = box_opacity if box_opacity is not None else (
        brand_kit.get("caption_box_opacity") if brand_kit else None
    )
    resolved_box_opacity = (
        float(resolved_box_opacity) if resolved_box_opacity is not None else settings.caption_box_opacity
    )
    if resolved_box_opacity < 0 or resolved_box_opacity > 1:
        raise JobError("caption box opacity must be between 0 and 1")

    resolved_highlight = highlight_color if highlight_color else (
        brand_kit.get("caption_highlight_color") if brand_kit else None
    )
    resolved_highlight = sanitize_color(resolved_highlight, settings.caption_highlight_color)

    resolved_padding = padding_px if padding_px is not None else (
        brand_kit.get("caption_padding_px") if brand_kit else None
    )
    resolved_padding = int(resolved_padding or settings.caption_padding_px)
    if resolved_padding < 0:
        raise JobError("caption_padding_px must be >= 0")

    profile_bottom, profile_top = resolve_safe_zone_profile(safe_zone_profile)
    resolved_safe_bottom = safe_zone_bottom_px if safe_zone_bottom_px is not None else (
        profile_bottom if profile_bottom is not None else (brand_kit.get("caption_safe_zone_bottom_px") if brand_kit else None)
    )
    resolved_safe_top = safe_zone_top_px if safe_zone_top_px is not None else (
        profile_top if profile_top is not None else (brand_kit.get("caption_safe_zone_top_px") if brand_kit else None)
    )
    resolved_safe_bottom = int(resolved_safe_bottom or settings.caption_safe_zone_bottom_px)
    resolved_safe_top = int(resolved_safe_top or settings.caption_safe_zone_top_px)
    if resolved_safe_bottom < 0 or resolved_safe_top < 0:
        raise JobError("caption safe zones must be >= 0")

    resolved_font_name = font_name if font_name else (
        brand_kit.get("caption_font_name") if brand_kit else None
    )
    resolved_font_asset_id = font_asset_id if font_asset_id else (
        brand_kit.get("caption_font_asset_id") if brand_kit else None
    )
    if brand_kit and not resolved_font_name and not resolved_font_asset_id:
        resolved_font_name = brand_kit.get("font_name")
        resolved_font_asset_id = brand_kit.get("font_asset_id")

    segments = parse_captions_input(
        captions_srt,
        captions_vtt,
        words_json,
        resolved_max_chars,
        resolved_max_lines,
        resolved_max_words,
        highlight_mode,
    )
    if not segments:
        raise JobError("No captions to render")
    if len(segments) > settings.max_caption_segments:
        raise JobError("Too many caption segments")

    _ensure_temp_dir()
    input_path, cleanup = _resolve_input_path(asset)
    output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}.mp4")
    font_cleanup = False
    font_path = None
    text_paths: list[str] = []
    logs = ""
    try:
        font_path, font_cleanup = resolve_font_path(resolved_font_name, resolved_font_asset_id)
        x_expr, y_expr = _caption_overlay_xy(
            resolved_position,
            resolved_safe_bottom,
            resolved_safe_top,
        )
        line_spacing = settings.caption_line_spacing
        box_color_value = _apply_opacity(resolved_box_color, resolved_box_opacity)
        color_value = resolved_highlight if highlight_mode == "word" else resolved_font_color

        filters: list[str] = []
        for seg in segments:
            start = float(seg["start"])
            end = float(seg["end"])
            if end <= start:
                continue
            text = seg.get("text") or ""
            with tempfile.NamedTemporaryFile(
                dir=settings.storage_temp_dir, prefix="caption_", suffix=".txt", delete=False
            ) as handle:
                handle.write(text.encode("utf-8"))
                text_paths.append(handle.name)

            drawtext_parts = [
                f"fontfile={escape_drawtext_value(font_path)}",
                f"textfile={escape_drawtext_value(text_paths[-1])}",
                f"fontcolor={color_value}",
                f"fontsize={resolved_font_size}",
                f"x={x_expr}",
                f"y={y_expr}",
                f"line_spacing={line_spacing}",
                "box=1",
                f"boxcolor={box_color_value}",
                f"boxborderw={resolved_padding}",
                f"enable='between(t,{start:.3f},{end:.3f})'",
                "expansion=none",
            ]
            filters.append("drawtext=" + ":".join(drawtext_parts))

        if not filters:
            raise JobError("No caption segments to render")

        filter_chain = ",".join(filters)
        args = [
            "-i",
            input_path,
            "-vf",
            filter_chain,
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
        _record_job_metrics("captions_burn_in", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "captions_burn_in",
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
        _record_job_metrics("captions_burn_in", start_ts, "error", job_id)
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if font_cleanup and font_path and os.path.exists(font_path):
            os.remove(font_path)
        for path in text_paths:
            if os.path.exists(path):
                os.remove(path)
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


def video_replace_audio_job(
    video_asset_id: str,
    audio_asset_id: str,
    audio_bitrate: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("video_replace_audio", job_id, video_asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 10, "cache_key": cache_key},
    )

    video_asset = _get_asset_or_error(video_asset_id)
    audio_asset = _get_asset_or_error(audio_asset_id)
    _ensure_video_asset(video_asset)
    _ensure_audio_asset(audio_asset)

    _ensure_temp_dir()
    video_path, video_cleanup = _resolve_input_path(video_asset)
    audio_path, audio_cleanup = _resolve_input_path(audio_asset)
    output_path = os.path.join(settings.storage_temp_dir, f"{uuid.uuid4().hex}.mp4")
    logs = ""
    try:
        video_probe = _probe_optional(video_path)
        audio_probe = _probe_optional(audio_path)
        use_shortest = True
        if video_probe and audio_probe:
            video_duration = video_probe.get("duration_sec")
            audio_duration = audio_probe.get("duration_sec")
            if video_duration and audio_duration:
                use_shortest = audio_duration >= video_duration

        args = [
            "-i",
            video_path,
            "-i",
            audio_path,
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-c:v",
            "copy",
            "-c:a",
            "aac",
        ]
        if audio_bitrate:
            args += ["-b:a", str(audio_bitrate)]
        if use_shortest:
            args.append("-shortest")
        args += [
            "-movflags",
            "+faststart",
            output_path,
        ]
        logs = run_ffmpeg(args, timeout=settings.ffmpeg_timeout_seconds)
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
            video_asset_id,
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
        _record_job_metrics("video_replace_audio", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [output_asset["asset_id"]],
                    "created_at": utc_now_iso(),
                    "job_type": "video_replace_audio",
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
        _record_job_metrics("video_replace_audio", start_ts, "error", job_id)
        raise
    finally:
        if video_cleanup and os.path.exists(video_path):
            os.remove(video_path)
        if audio_cleanup and os.path.exists(audio_path):
            os.remove(audio_path)
        if os.path.exists(output_path):
            os.remove(output_path)


def video_analyze_job(
    asset_id: str,
    rubric_name: str | None,
    target_preset: str | None,
    reference_asset_id: str | None,
    captions_srt: str | None,
    captions_vtt: str | None,
    words_json: list[dict[str, Any]] | None,
    brand_kit_id: str | None,
    position: str | None,
    font_size: int | None,
    padding_px: int | None,
    max_chars: int | None,
    max_lines: int | None,
    max_words: int | None,
    safe_zone_bottom_px: int | None,
    safe_zone_top_px: int | None,
    safe_zone_profile: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("video_analyze", job_id, asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 5, "cache_key": cache_key},
    )

    asset = _get_asset_or_error(asset_id)
    _ensure_video_asset(asset)

    rubric = get_rubric(rubric_name) if rubric_name else None
    targets = rubric.get("targets", {}) if rubric else {}

    brand_kit = get_brand_kit(brand_kit_id) if brand_kit_id else None
    if brand_kit_id and not brand_kit:
        raise JobError("brand_kit_id not found")

    _ensure_temp_dir()
    input_path, cleanup = _resolve_input_path(asset)
    reference_asset = None
    reference_path = None
    reference_cleanup = False
    if reference_asset_id:
        reference_asset = _get_asset_or_error(reference_asset_id)
        _ensure_video_asset(reference_asset)
        reference_path, reference_cleanup = _resolve_input_path(reference_asset)
    try:
        def collect_metrics(path: str, size_bytes: int | None, include_clipping: bool) -> dict[str, Any]:
            probe = _probe_or_error(path)
            duration = probe.get("duration_sec")
            width = probe.get("width")
            height = probe.get("height")
            bitrate_kbps = None
            if duration and size_bytes:
                bitrate_kbps = round((float(size_bytes) * 8.0) / float(duration) / 1000.0, 2)

            has_audio = _has_audio_stream(probe)
            has_video = _has_video_stream(probe)

            loudness_lufs = None
            true_peak_db = None
            lra = None
            silence_pct = None
            clipping_pct = None
            if has_audio:
                try:
                    loudnorm_logs = run_ffmpeg(
                        [
                            "-i",
                            path,
                            "-vn",
                            "-af",
                            f"loudnorm=I={settings.audio_norm_i}:TP={settings.audio_norm_tp}:"
                            f"LRA={settings.audio_norm_lra}:print_format=json",
                            "-f",
                            "null",
                            "-",
                        ],
                        timeout=settings.audio_timeout_seconds(),
                    )
                    payload = _parse_loudnorm_json(loudnorm_logs) or {}
                    for key, fallback in [
                        ("input_i", "measured_I"),
                        ("input_tp", "measured_TP"),
                        ("input_lra", "measured_LRA"),
                    ]:
                        if key in payload:
                            value = payload.get(key)
                        else:
                            value = payload.get(fallback)
                        try:
                            value = float(value)
                        except (TypeError, ValueError):
                            value = None
                        if key == "input_i":
                            loudness_lufs = value
                        elif key == "input_tp":
                            true_peak_db = value
                        elif key == "input_lra":
                            lra = value
                except FfmpegError:
                    pass

                try:
                    silence_logs = run_ffmpeg(
                        [
                            "-i",
                            path,
                            "-vn",
                            "-af",
                            f"silencedetect=noise={settings.audio_silence_db}dB:"
                            f"d={settings.audio_min_silence_sec}",
                            "-f",
                            "null",
                            "-",
                        ],
                        timeout=settings.audio_timeout_seconds(),
                    )
                    silence_pct = _parse_silencedetect(silence_logs, duration)
                except FfmpegError:
                    pass

                if include_clipping:
                    try:
                        clipping_logs = run_ffmpeg(
                            [
                                "-i",
                                path,
                                "-vn",
                                "-af",
                                "astats=metadata=1:reset=1",
                                "-f",
                                "null",
                                "-",
                            ],
                            timeout=settings.audio_timeout_seconds(),
                        )
                        clipping_pct = _parse_astats_clipping(clipping_logs)
                    except FfmpegError:
                        pass

            black_frames_pct = None
            if has_video:
                try:
                    black_logs = run_ffmpeg(
                        [
                            "-i",
                            path,
                            "-an",
                            "-vf",
                            "blackdetect=d=0.1:pic_th=0.98",
                            "-f",
                            "null",
                            "-",
                        ],
                        timeout=settings.ffmpeg_timeout_seconds,
                    )
                    black_frames_pct = _parse_blackdetect(black_logs, duration)
                except FfmpegError:
                    pass

            return {
                "probe": probe,
                "duration_sec": duration,
                "width": width,
                "height": height,
                "bitrate_kbps": bitrate_kbps,
                "audio": {
                    "has_audio": has_audio,
                    "loudness_lufs": loudness_lufs,
                    "true_peak_db": true_peak_db,
                    "lra": lra,
                    "silence_pct": silence_pct,
                    "clipping_pct": clipping_pct,
                },
                "video": {
                    "has_video": has_video,
                    "black_frames_pct": black_frames_pct,
                },
            }

        include_clipping = bool(rubric and rubric.get("weights", {}).get("audio.clipping_pct", 0) > 0)
        main_metrics = collect_metrics(input_path, asset.get("size_bytes"), include_clipping)
        probe = main_metrics["probe"]
        duration = main_metrics["duration_sec"]
        width = main_metrics["width"]
        height = main_metrics["height"]
        bitrate_kbps = main_metrics["bitrate_kbps"]

        expected_w, expected_h = _expected_dims_from_preset(target_preset)
        resolution_ok = None
        if width and height:
            if expected_w and expected_h:
                resolution_ok = int(width) == expected_w and int(height) == expected_h
            elif targets.get("min_width") and targets.get("min_height"):
                resolution_ok = int(width) >= int(targets["min_width"]) and int(height) >= int(
                    targets["min_height"]
                )

        file_size_ok = None
        size_bytes = asset.get("size_bytes")
        if size_bytes:
            file_size_ok = int(size_bytes) <= settings.max_output_bytes

        bitrate_ok = None
        if bitrate_kbps is not None and targets.get("bitrate_kbps_max") is not None:
            bitrate_ok = float(bitrate_kbps) <= float(targets["bitrate_kbps_max"])

        audio_metrics = main_metrics["audio"]
        video_metrics = main_metrics["video"]
        loudness_lufs = audio_metrics.get("loudness_lufs")
        true_peak_db = audio_metrics.get("true_peak_db")
        lra = audio_metrics.get("lra")
        silence_pct = audio_metrics.get("silence_pct")
        clipping_pct = audio_metrics.get("clipping_pct")
        black_frames_pct = video_metrics.get("black_frames_pct")

        resolved_max_chars = None
        resolved_max_lines = None
        resolved_max_words = None
        resolved_position = None
        resolved_padding = None
        resolved_safe_bottom = None
        resolved_safe_top = None
        resolved_font_size = None

        captions_metrics = {
            "caption_readability_score": None,
            "caption_speed_score": None,
            "caption_speed_wpm": None,
            "safe_zone_violations": None,
            "segment_count": None,
            "empty": None,
        }
        if captions_srt or captions_vtt or words_json:
            resolved_max_chars = max_chars if max_chars is not None else (
                brand_kit.get("caption_max_chars") if brand_kit else None
            )
            resolved_max_lines = max_lines if max_lines is not None else (
                brand_kit.get("caption_max_lines") if brand_kit else None
            )
            resolved_max_words = max_words if max_words is not None else (
                brand_kit.get("caption_max_words") if brand_kit else None
            )
            resolved_max_chars = int(resolved_max_chars or settings.caption_max_chars)
            resolved_max_lines = int(resolved_max_lines or settings.caption_max_lines)
            resolved_max_words = int(resolved_max_words or settings.caption_max_words)

            resolved_position = position if position else (
                brand_kit.get("caption_position") if brand_kit else None
            )
            if not resolved_position:
                resolved_position = settings.caption_position
            resolved_position = sanitize_position(resolved_position, CAPTION_POSITIONS)

            resolved_padding = padding_px if padding_px is not None else (
                brand_kit.get("caption_padding_px") if brand_kit else None
            )
            resolved_padding = int(resolved_padding or settings.caption_padding_px)

            profile_bottom, profile_top = resolve_safe_zone_profile(safe_zone_profile)
            resolved_safe_bottom = safe_zone_bottom_px if safe_zone_bottom_px is not None else (
                profile_bottom if profile_bottom is not None else (brand_kit.get("caption_safe_zone_bottom_px") if brand_kit else None)
            )
            resolved_safe_top = safe_zone_top_px if safe_zone_top_px is not None else (
                profile_top if profile_top is not None else (brand_kit.get("caption_safe_zone_top_px") if brand_kit else None)
            )
            resolved_safe_bottom = int(resolved_safe_bottom or settings.caption_safe_zone_bottom_px)
            resolved_safe_top = int(resolved_safe_top or settings.caption_safe_zone_top_px)

            resolved_font_size = font_size if font_size is not None else (
                brand_kit.get("caption_font_size") if brand_kit else None
            )
            resolved_font_size = sanitize_font_size(resolved_font_size, settings.caption_font_size)

            segments = parse_captions_input(
                captions_srt,
                captions_vtt,
                words_json,
                resolved_max_chars,
                resolved_max_lines,
                resolved_max_words,
                None,
            )
            captions_metrics["segment_count"] = len(segments)
            captions_metrics["empty"] = len(segments) == 0
            speed_target = float(targets.get("caption_speed_wpm_max", 180.0))
            captions_metrics = _caption_metrics(
                segments,
                resolved_max_chars,
                resolved_max_lines,
                resolved_max_words,
                resolved_position,
                resolved_safe_bottom,
                resolved_safe_top,
                resolved_padding,
                resolved_font_size,
                speed_target,
            )
            captions_metrics["segment_count"] = len(segments)
            captions_metrics["empty"] = len(segments) == 0
            if safe_zone_profile:
                captions_metrics["safe_zone_profile"] = safe_zone_profile

        qa_overrides: dict[str, Any] = {"target_preset": target_preset}
        if captions_srt or captions_vtt or words_json:
            qa_overrides.update(
                {
                    "caption_position": resolved_position,
                    "caption_font_size": resolved_font_size,
                    "caption_padding_px": resolved_padding,
                    "caption_max_chars": resolved_max_chars,
                    "caption_max_lines": resolved_max_lines,
                    "caption_max_words": resolved_max_words,
                    "caption_safe_zone_profile": safe_zone_profile,
                    "caption_safe_zone_bottom_px": resolved_safe_bottom,
                    "caption_safe_zone_top_px": resolved_safe_top,
                }
            )

        report = {
            "asset_id": asset_id,
            "duration_sec": duration,
            "video": {
                "width": width,
                "height": height,
                "bitrate_kbps": bitrate_kbps,
                "file_size_bytes": size_bytes,
                "has_video": video_metrics.get("has_video"),
                "resolution_ok": resolution_ok,
                "bitrate_ok": bitrate_ok,
                "file_size_ok": file_size_ok,
                "black_frames_pct": black_frames_pct,
            },
            "audio": {
                "has_audio": audio_metrics.get("has_audio"),
                "loudness_lufs": loudness_lufs,
                "true_peak_db": true_peak_db,
                "lra": lra,
                "silence_pct": silence_pct,
                "clipping_pct": clipping_pct,
            },
            "captions": captions_metrics,
        }

        if rubric:
            report["rubric"] = {
                "name": rubric_name,
                **score_report(report, rubric, target_preset),
            }
        if target_preset:
            report["target_preset"] = target_preset

        reference_report = None
        deltas: dict[str, Any] = {}
        if reference_asset_id and reference_path and reference_asset:
            reference_metrics = collect_metrics(
                reference_path,
                reference_asset.get("size_bytes"),
                include_clipping,
            )
            reference_report = {
                "asset_id": reference_asset_id,
                "duration_sec": reference_metrics.get("duration_sec"),
                "video": {
                    "width": reference_metrics.get("width"),
                    "height": reference_metrics.get("height"),
                    "bitrate_kbps": reference_metrics.get("bitrate_kbps"),
                    "has_video": reference_metrics.get("video", {}).get("has_video"),
                    "black_frames_pct": reference_metrics.get("video", {}).get("black_frames_pct"),
                },
                "audio": reference_metrics.get("audio", {}),
            }

            ref_audio = reference_report.get("audio", {})
            ref_video = reference_report.get("video", {})
            if loudness_lufs is not None and ref_audio.get("loudness_lufs") is not None:
                deltas["audio_loudness_lufs_delta"] = round(
                    float(loudness_lufs) - float(ref_audio["loudness_lufs"]), 3
                )
            if true_peak_db is not None and ref_audio.get("true_peak_db") is not None:
                deltas["audio_true_peak_db_delta"] = round(
                    float(true_peak_db) - float(ref_audio["true_peak_db"]), 3
                )
            if lra is not None and ref_audio.get("lra") is not None:
                deltas["audio_lra_delta"] = round(float(lra) - float(ref_audio["lra"]), 3)
            if silence_pct is not None and ref_audio.get("silence_pct") is not None:
                deltas["audio_silence_pct_delta"] = round(
                    float(silence_pct) - float(ref_audio["silence_pct"]), 3
                )
            if black_frames_pct is not None and ref_video.get("black_frames_pct") is not None:
                deltas["black_frames_pct_delta"] = round(
                    float(black_frames_pct) - float(ref_video["black_frames_pct"]), 3
                )
            if duration and reference_report.get("duration_sec"):
                deltas["duration_delta_sec"] = round(
                    float(duration) - float(reference_report["duration_sec"]), 3
                )
            if bitrate_kbps is not None and ref_video.get("bitrate_kbps") is not None:
                deltas["bitrate_kbps_delta"] = round(
                    float(bitrate_kbps) - float(ref_video["bitrate_kbps"]), 3
                )

        if reference_report:
            report["reference"] = reference_report
        if deltas:
            report["reference_deltas"] = deltas

        if rubric:
            report["qa"] = qa_from_report(report, rubric, target_preset, qa_overrides)

        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [asset_id],
                "report": report,
                "qa": report.get("qa"),
                "logs_short": "analysis complete",
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("video_analyze", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [asset_id],
                    "created_at": utc_now_iso(),
                    "job_type": "video_analyze",
                    "report": report,
                    "qa": report.get("qa"),
                },
                settings.asset_ttl_seconds(),
            )
        return report
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": None, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("video_analyze", start_ts, "error", job_id)
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)


def asset_compare_job(
    asset_ids: list[str],
    rubric_name: str,
    target_preset: str | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("asset_compare", job_id, asset_ids[0] if asset_ids else None)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 5, "cache_key": cache_key},
    )

    if not asset_ids:
        raise JobError("asset_ids is required")
    if len(asset_ids) > settings.max_batch_assets:
        raise JobError("Too many assets to compare")

    results: list[dict[str, Any]] = []
    try:
        for idx, asset_id in enumerate(asset_ids):
            _get_asset_or_error(asset_id)
            report = video_analyze_job(
                asset_id,
                rubric_name,
                target_preset,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                cache_key=None,
                job_id_override="",
            )
            score = None
            rubric_report = report.get("rubric") if isinstance(report, dict) else None
            if rubric_report:
                score = rubric_report.get("score")
            results.append(
                {
                    "asset_id": asset_id,
                    "score": score,
                    "passed": rubric_report.get("passed") if rubric_report else None,
                    "report": report,
                }
            )
            update_job(
                job_id,
                {
                    "progress": 5 + int(90 * ((idx + 1) / len(asset_ids))),
                    "updated_at": utc_now_iso(),
                },
            )

        ranked = sorted(
            results,
            key=lambda item: (item.get("score") is not None, item.get("score", 0)),
            reverse=True,
        )
        qa = None
        if ranked:
            top_report = ranked[0].get("report")
            if isinstance(top_report, dict) and isinstance(top_report.get("qa"), dict):
                qa = top_report.get("qa")
            else:
                rubric = get_rubric(rubric_name)
                if isinstance(top_report, dict):
                    qa = qa_from_report(top_report, rubric, target_preset)
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [item["asset_id"] for item in ranked],
                "ranking": ranked,
                "qa": qa,
                "logs_short": "compare complete",
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("asset_compare", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [item["asset_id"] for item in ranked],
                    "created_at": utc_now_iso(),
                    "job_type": "asset_compare",
                    "ranking": ranked,
                    "qa": qa,
                },
                settings.asset_ttl_seconds(),
            )
        return {"ranking": ranked, "qa": qa}
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": None, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("asset_compare", start_ts, "error", job_id)
        raise


def template_apply_job(
    asset_id: str,
    template_name: str,
    variables: dict[str, Any] | None,
    brand_kit_id: str | None,
    quality: str | None,
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

    quality_value = None
    if quality:
        quality_value = quality.strip().lower()
        if quality_value not in {"final", "draft"}:
            raise JobError("quality must be 'final' or 'draft'")

    asset = _get_asset_or_error(asset_id)
    template = get_template(template_name)
    layers = list(template.get("layers", []))
    if len(layers) > settings.max_template_layers:
        raise JobError("Template exceeds max layers")

    try:
        merged_vars = validate_template_variables(template, variables)
    except ValueError as exc:
        raise JobError(str(exc)) from None

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
                if quality_value == "draft":
                    preset = draft_preset_for(str(preset))
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
    quality: str | None,
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

    quality_value = None
    if quality:
        quality_value = quality.strip().lower()
        if quality_value not in {"final", "draft"}:
            raise JobError("quality must be 'final' or 'draft'")

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
                        "quality": quality_value,
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
                        quality_value,
                        cache_key=template_cache_key,
                        job_id_override="",
                    )
                    base_asset_id = output_asset["asset_id"]

            if presets:
                resolved_presets = map_presets_for_quality(presets, quality_value)
                batch_cache_key = build_cache_key(
                    "ffmpeg:batch_export",
                    {"asset_id": base_asset_id, "presets": resolved_presets},
                )
                cached_ids = _resolve_cached_outputs_list(batch_cache_key)
                if cached_ids:
                    outputs[asset_id] = list(cached_ids)
                else:
                    batch_result = batch_export_job(
                        base_asset_id,
                        resolved_presets,
                        cache_key=batch_cache_key,
                        job_id_override="",
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
                            params.get("quality"),
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


def _render_marketing_job(
    job_type: str,
    template_name: str,
    primary_asset_id: str,
    variables: dict[str, Any],
    brand_kit_id: str | None,
    broll_asset_ids: list[str] | None,
    voice_asset_id: str | None,
    music_asset_id: str | None,
    captions_srt: str | None,
    captions_vtt: str | None,
    words_json: list[dict[str, Any]] | None,
    highlight_mode: str | None,
    include_16_9: bool | None,
    quality: str | None,
    framing_mode: str | None,
    caption_position: str | None,
    caption_font_size: int | None,
    caption_font_color: str | None,
    caption_box_color: str | None,
    caption_box_opacity: float | None,
    caption_highlight_color: str | None,
    caption_padding_px: int | None,
    caption_max_chars: int | None,
    caption_max_lines: int | None,
    caption_max_words: int | None,
    caption_safe_zone_profile: str | None,
    caption_safe_zone_bottom_px: int | None,
    caption_safe_zone_top_px: int | None,
    caption_font_name: str | None,
    caption_font_asset_id: str | None,
    audio_target_lufs: float | None,
    audio_lra: float | None,
    audio_true_peak: float | None,
    ducking_ratio: float | None,
    ducking_threshold: float | None,
    ducking_attack_ms: int | None,
    ducking_release_ms: int | None,
    music_gain: float | None,
    voice_gain: float | None,
    trim_silence: bool | None,
    trim_silence_min_sec: float | None,
    trim_silence_threshold_db: float | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started(job_type, job_id, primary_asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 5, "cache_key": cache_key},
    )

    try:
        if not primary_asset_id:
            raise JobError("primary_asset_id is required")
        if broll_asset_ids is not None and not isinstance(broll_asset_ids, list):
            raise JobError("broll_asset_ids must be a list")

        template = get_template(template_name)
        try:
            template_vars = validate_template_variables(template, variables)
        except ValueError as exc:
            raise JobError(str(exc)) from None

        include_16_9 = bool(include_16_9) if include_16_9 is not None else False
        quality = (quality or "final").strip().lower()
        if quality not in {"final", "draft"}:
            raise JobError("quality must be 'final' or 'draft'")

        framing_mode = (framing_mode or "safe_pad").strip().lower()
        if framing_mode not in {"safe_pad", "crop"}:
            raise JobError("framing_mode must be 'safe_pad' or 'crop'")

        base_asset_id = _coerce_video_asset_id(primary_asset_id)
        if broll_asset_ids:
            video_ids = [base_asset_id]
            for asset_id in broll_asset_ids:
                if not asset_id:
                    continue
                video_ids.append(_coerce_video_asset_id(asset_id))
            if len(video_ids) > 1:
                concat_cache_key = build_cache_key(
                    "ffmpeg:video_concat",
                    {
                        "asset_ids": video_ids,
                        "transition": None,
                        "transition_duration": None,
                        "target_width": None,
                        "target_height": None,
                        "include_audio": True,
                    },
                )
                cached_concat = _resolve_cached_output(concat_cache_key)
                if cached_concat:
                    base_asset_id = cached_concat
                else:
                    concat_asset = video_concat_job(
                        video_ids,
                        None,
                        None,
                        None,
                        None,
                        True,
                        cache_key=concat_cache_key,
                        job_id_override="",
                    )
                    base_asset_id = concat_asset["asset_id"]

        mixed_audio_id = None
        voice_audio_id = _coerce_audio_asset_id(voice_asset_id) if voice_asset_id else None
        music_audio_id = _coerce_audio_asset_id(music_asset_id) if music_asset_id else None
        if voice_audio_id and trim_silence:
            trim_cache_key = build_cache_key(
                "ffmpeg:audio_trim_silence",
                {
                    "asset_id": voice_audio_id,
                    "output_format": "m4a",
                    "min_silence_sec": trim_silence_min_sec,
                    "threshold_db": trim_silence_threshold_db,
                    "trim_leading": True,
                    "trim_trailing": True,
                    "bitrate": None,
                },
            )
            cached_trim = _resolve_cached_output(trim_cache_key)
            if cached_trim:
                voice_audio_id = cached_trim
            else:
                trimmed = audio_trim_silence_job(
                    voice_audio_id,
                    "m4a",
                    trim_silence_min_sec,
                    trim_silence_threshold_db,
                    True,
                    True,
                    None,
                    cache_key=trim_cache_key,
                    job_id_override="",
                )
                voice_audio_id = trimmed["asset_id"]

        if voice_audio_id or music_audio_id:
            if voice_audio_id and music_audio_id:
                mix_cache_key = build_cache_key(
                    "ffmpeg:audio_mix_with_background",
                    {
                        "voice_asset_id": voice_audio_id,
                        "music_asset_id": music_audio_id,
                        "output_format": "m4a",
                        "ducking": None,
                        "ratio": ducking_ratio,
                        "threshold": ducking_threshold,
                        "attack_ms": ducking_attack_ms,
                        "release_ms": ducking_release_ms,
                        "music_gain": music_gain,
                        "voice_gain": voice_gain,
                        "bitrate": None,
                    },
                )
                cached_mix = _resolve_cached_output(mix_cache_key)
                if cached_mix:
                    mixed_audio_id = cached_mix
                else:
                    mix_asset = audio_mix_with_background_job(
                        voice_audio_id,
                        music_audio_id,
                        "m4a",
                        None,
                        ducking_ratio,
                        ducking_threshold,
                        ducking_attack_ms,
                        ducking_release_ms,
                        music_gain,
                        voice_gain,
                        None,
                        cache_key=mix_cache_key,
                        job_id_override="",
                    )
                    mixed_audio_id = mix_asset["asset_id"]
            elif voice_audio_id:
                norm_cache_key = build_cache_key(
                    "ffmpeg:audio_normalize",
                    {
                        "asset_id": voice_audio_id,
                        "output_format": "m4a",
                        "target_lufs": audio_target_lufs,
                        "lra": audio_lra,
                        "true_peak": audio_true_peak,
                        "bitrate": None,
                    },
                )
                cached_norm = _resolve_cached_output(norm_cache_key)
                if cached_norm:
                    mixed_audio_id = cached_norm
                else:
                    norm_asset = audio_normalize_job(
                        voice_audio_id,
                        "m4a",
                        audio_target_lufs,
                        audio_lra,
                        audio_true_peak,
                        None,
                        cache_key=norm_cache_key,
                        job_id_override="",
                    )
                    mixed_audio_id = norm_asset["asset_id"]
            elif music_audio_id:
                norm_cache_key = build_cache_key(
                    "ffmpeg:audio_normalize",
                    {
                        "asset_id": music_audio_id,
                        "output_format": "m4a",
                        "target_lufs": audio_target_lufs,
                        "lra": audio_lra,
                        "true_peak": audio_true_peak,
                        "bitrate": None,
                    },
                )
                cached_norm = _resolve_cached_output(norm_cache_key)
                if cached_norm:
                    mixed_audio_id = cached_norm
                else:
                    norm_asset = audio_normalize_job(
                        music_audio_id,
                        "m4a",
                        audio_target_lufs,
                        audio_lra,
                        audio_true_peak,
                        None,
                        cache_key=norm_cache_key,
                        job_id_override="",
                    )
                    mixed_audio_id = norm_asset["asset_id"]

        preset_maps = {
            "safe_pad": {
                "final": {
                    "9x16": "mp4_social_vertical_1080x1920_safe_pad",
                    "1x1": "mp4_social_square_1080x1080_safe_pad",
                    "4x5": "mp4_social_portrait_1080x1350_safe_pad",
                    "16x9": "mp4_youtube_1920x1080",
                },
                "draft": {
                    "9x16": "mp4_social_vertical_720x1280_safe_pad",
                    "1x1": "mp4_social_square_720x720_safe_pad",
                    "4x5": "mp4_social_portrait_720x900_safe_pad",
                    "16x9": "mp4_youtube_1280x720",
                },
            },
            "crop": {
                "final": {
                    "9x16": "mp4_social_vertical_1080x1920",
                    "1x1": "mp4_social_square_1080x1080",
                    "4x5": "mp4_social_portrait_1080x1350",
                    "16x9": "mp4_youtube_1920x1080",
                },
                "draft": {
                    "9x16": "mp4_social_vertical_720x1280",
                    "1x1": "mp4_social_square_720x720",
                    "4x5": "mp4_social_portrait_720x900",
                    "16x9": "mp4_youtube_1280x720",
                },
            },
        }
        variants = ["9x16", "1x1", "4x5"]
        if include_16_9:
            variants.append("16x9")
        preset_map = preset_maps[framing_mode]["draft" if quality == "draft" else "final"]

        outputs: dict[str, dict[str, str | None]] = {}
        output_ids: list[str] = []
        total_variants = max(len(variants), 1)
        for idx, variant in enumerate(variants):
            preset_name = preset_map[variant]
            layer_vars = dict(template_vars)
            layer_vars["preset"] = preset_name

            template_cache_key = build_cache_key(
                "ffmpeg:template_apply",
                {
                    "asset_id": base_asset_id,
                    "template_name": template_name,
                    "variables": layer_vars,
                    "brand_kit_id": brand_kit_id,
                    "quality": quality,
                },
            )
            cached_output = _resolve_cached_output(template_cache_key)
            if cached_output:
                current_asset_id = cached_output
            else:
                output_asset = template_apply_job(
                    base_asset_id,
                    template_name,
                    layer_vars,
                    brand_kit_id,
                    quality,
                    cache_key=template_cache_key,
                    job_id_override="",
                )
                current_asset_id = output_asset["asset_id"]

            if mixed_audio_id:
                audio_bitrate = settings.draft_audio_bitrate if quality == "draft" else "160k"
                replace_cache_key = build_cache_key(
                    "ffmpeg:video_replace_audio",
                    {
                        "video_asset_id": current_asset_id,
                        "audio_asset_id": mixed_audio_id,
                        "audio_bitrate": audio_bitrate,
                    },
                )
                cached_replace = _resolve_cached_output(replace_cache_key)
                if cached_replace:
                    current_asset_id = cached_replace
                else:
                    replaced = video_replace_audio_job(
                        current_asset_id,
                        mixed_audio_id,
                        audio_bitrate,
                        cache_key=replace_cache_key,
                        job_id_override="",
                    )
                    current_asset_id = replaced["asset_id"]

            if quality == "draft" and settings.draft_watermark_enabled:
                opacity_str = f"{settings.draft_watermark_opacity:.3f}".rstrip("0").rstrip(".")
                watermark_color = f"white@{opacity_str}"
                watermark_cache_key = build_cache_key(
                    "ffmpeg:video_add_text",
                    {
                        "asset_id": current_asset_id,
                        "text": settings.draft_watermark_text,
                        "position": "top",
                        "font_size": settings.draft_watermark_font_size,
                        "font_color": watermark_color,
                        "background_box": False,
                        "box_color": None,
                        "box_border_width": None,
                        "font_name": None,
                        "font_asset_id": None,
                    },
                )
                cached_watermark = _resolve_cached_output(watermark_cache_key)
                if cached_watermark:
                    current_asset_id = cached_watermark
                else:
                    watermarked = video_add_text_job(
                        current_asset_id,
                        settings.draft_watermark_text,
                        "top",
                        settings.draft_watermark_font_size,
                        watermark_color,
                        False,
                        None,
                        None,
                        None,
                        None,
                        cache_key=watermark_cache_key,
                        job_id_override="",
                    )
                    current_asset_id = watermarked["asset_id"]

            non_captioned_id = current_asset_id
            captioned_id = None
            if captions_srt or captions_vtt or words_json:
                captions_cache_key = build_cache_key(
                    "ffmpeg:captions_burn_in",
                    {
                        "asset_id": current_asset_id,
                        "captions_srt": captions_srt,
                        "captions_vtt": captions_vtt,
                        "words_json": words_json,
                        "brand_kit_id": brand_kit_id,
                        "highlight_mode": highlight_mode,
                        "position": caption_position,
                        "font_size": caption_font_size,
                        "font_color": caption_font_color,
                        "box_color": caption_box_color,
                        "box_opacity": caption_box_opacity,
                        "highlight_color": caption_highlight_color,
                        "padding_px": caption_padding_px,
                        "max_chars": caption_max_chars,
                        "max_lines": caption_max_lines,
                        "max_words": caption_max_words,
                        "safe_zone_profile": caption_safe_zone_profile,
                        "safe_zone_bottom_px": caption_safe_zone_bottom_px,
                        "safe_zone_top_px": caption_safe_zone_top_px,
                        "font_name": caption_font_name,
                        "font_asset_id": caption_font_asset_id,
                    },
                )
                cached_captioned = _resolve_cached_output(captions_cache_key)
                if cached_captioned:
                    captioned_id = cached_captioned
                else:
                    captioned = captions_burn_in_job(
                        current_asset_id,
                        captions_srt,
                        captions_vtt,
                        words_json,
                        brand_kit_id,
                        highlight_mode,
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
                        caption_safe_zone_profile,
                        caption_safe_zone_bottom_px,
                        caption_safe_zone_top_px,
                        caption_font_name,
                        caption_font_asset_id,
                        cache_key=captions_cache_key,
                        job_id_override="",
                    )
                    captioned_id = captioned["asset_id"]

            outputs[variant] = {"non_captioned": non_captioned_id, "captioned": captioned_id}
            output_ids.append(non_captioned_id)
            if captioned_id:
                output_ids.append(captioned_id)

            update_job(
                job_id,
                {
                    "progress": 10 + int(85 * ((idx + 1) / total_variants)),
                    "updated_at": utc_now_iso(),
                },
            )

        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": output_ids,
                "outputs": outputs,
                "logs_short": "render complete",
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics(job_type, start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": output_ids,
                    "created_at": utc_now_iso(),
                    "job_type": job_type,
                    "outputs": outputs,
                },
                settings.asset_ttl_seconds(),
            )
        return {"output_asset_ids": output_ids, "outputs": outputs}
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": None, "finished_at": utc_now_iso()},
        )
        _record_job_metrics(job_type, start_ts, "error", job_id)
        raise


def render_social_ad_job(
    primary_asset_id: str,
    hook: str | None,
    headline: str | None,
    cta: str | None,
    price: str | None,
    brand_kit_id: str | None,
    broll_asset_ids: list[str] | None,
    voice_asset_id: str | None,
    music_asset_id: str | None,
    captions_srt: str | None,
    captions_vtt: str | None,
    words_json: list[dict[str, Any]] | None,
    highlight_mode: str | None,
    include_16_9: bool | None,
    quality: str | None,
    framing_mode: str | None,
    caption_position: str | None,
    caption_font_size: int | None,
    caption_font_color: str | None,
    caption_box_color: str | None,
    caption_box_opacity: float | None,
    caption_highlight_color: str | None,
    caption_padding_px: int | None,
    caption_max_chars: int | None,
    caption_max_lines: int | None,
    caption_max_words: int | None,
    caption_safe_zone_profile: str | None,
    caption_safe_zone_bottom_px: int | None,
    caption_safe_zone_top_px: int | None,
    caption_font_name: str | None,
    caption_font_asset_id: str | None,
    audio_target_lufs: float | None,
    audio_lra: float | None,
    audio_true_peak: float | None,
    ducking_ratio: float | None,
    ducking_threshold: float | None,
    ducking_attack_ms: int | None,
    ducking_release_ms: int | None,
    music_gain: float | None,
    voice_gain: float | None,
    trim_silence: bool | None,
    trim_silence_min_sec: float | None,
    trim_silence_threshold_db: float | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    variables = {
        "hook": hook,
        "headline": headline,
        "cta": cta,
        "price": price,
    }
    return _render_marketing_job(
        job_type="render_social_ad",
        template_name="social_ad_basic",
        primary_asset_id=primary_asset_id,
        variables=variables,
        brand_kit_id=brand_kit_id,
        broll_asset_ids=broll_asset_ids,
        voice_asset_id=voice_asset_id,
        music_asset_id=music_asset_id,
        captions_srt=captions_srt,
        captions_vtt=captions_vtt,
        words_json=words_json,
        highlight_mode=highlight_mode,
        include_16_9=include_16_9,
        quality=quality,
        framing_mode=framing_mode,
        caption_position=caption_position,
        caption_font_size=caption_font_size,
        caption_font_color=caption_font_color,
        caption_box_color=caption_box_color,
        caption_box_opacity=caption_box_opacity,
        caption_highlight_color=caption_highlight_color,
        caption_padding_px=caption_padding_px,
        caption_max_chars=caption_max_chars,
        caption_max_lines=caption_max_lines,
        caption_max_words=caption_max_words,
        caption_safe_zone_profile=caption_safe_zone_profile,
        caption_safe_zone_bottom_px=caption_safe_zone_bottom_px,
        caption_safe_zone_top_px=caption_safe_zone_top_px,
        caption_font_name=caption_font_name,
        caption_font_asset_id=caption_font_asset_id,
        audio_target_lufs=audio_target_lufs,
        audio_lra=audio_lra,
        audio_true_peak=audio_true_peak,
        ducking_ratio=ducking_ratio,
        ducking_threshold=ducking_threshold,
        ducking_attack_ms=ducking_attack_ms,
        ducking_release_ms=ducking_release_ms,
        music_gain=music_gain,
        voice_gain=voice_gain,
        trim_silence=trim_silence,
        trim_silence_min_sec=trim_silence_min_sec,
        trim_silence_threshold_db=trim_silence_threshold_db,
        cache_key=cache_key,
        job_id_override=job_id_override,
    )


def render_testimonial_clip_job(
    primary_asset_id: str,
    quote: str | None,
    author: str | None,
    brand_kit_id: str | None,
    broll_asset_ids: list[str] | None,
    voice_asset_id: str | None,
    music_asset_id: str | None,
    captions_srt: str | None,
    captions_vtt: str | None,
    words_json: list[dict[str, Any]] | None,
    highlight_mode: str | None,
    include_16_9: bool | None,
    quality: str | None,
    framing_mode: str | None,
    caption_position: str | None,
    caption_font_size: int | None,
    caption_font_color: str | None,
    caption_box_color: str | None,
    caption_box_opacity: float | None,
    caption_highlight_color: str | None,
    caption_padding_px: int | None,
    caption_max_chars: int | None,
    caption_max_lines: int | None,
    caption_max_words: int | None,
    caption_safe_zone_profile: str | None,
    caption_safe_zone_bottom_px: int | None,
    caption_safe_zone_top_px: int | None,
    caption_font_name: str | None,
    caption_font_asset_id: str | None,
    audio_target_lufs: float | None,
    audio_lra: float | None,
    audio_true_peak: float | None,
    ducking_ratio: float | None,
    ducking_threshold: float | None,
    ducking_attack_ms: int | None,
    ducking_release_ms: int | None,
    music_gain: float | None,
    voice_gain: float | None,
    trim_silence: bool | None,
    trim_silence_min_sec: float | None,
    trim_silence_threshold_db: float | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    variables = {"quote": quote, "author": author}
    return _render_marketing_job(
        job_type="render_testimonial_clip",
        template_name="testimonial_clip_basic",
        primary_asset_id=primary_asset_id,
        variables=variables,
        brand_kit_id=brand_kit_id,
        broll_asset_ids=broll_asset_ids,
        voice_asset_id=voice_asset_id,
        music_asset_id=music_asset_id,
        captions_srt=captions_srt,
        captions_vtt=captions_vtt,
        words_json=words_json,
        highlight_mode=highlight_mode,
        include_16_9=include_16_9,
        quality=quality,
        framing_mode=framing_mode,
        caption_position=caption_position,
        caption_font_size=caption_font_size,
        caption_font_color=caption_font_color,
        caption_box_color=caption_box_color,
        caption_box_opacity=caption_box_opacity,
        caption_highlight_color=caption_highlight_color,
        caption_padding_px=caption_padding_px,
        caption_max_chars=caption_max_chars,
        caption_max_lines=caption_max_lines,
        caption_max_words=caption_max_words,
        caption_safe_zone_profile=caption_safe_zone_profile,
        caption_safe_zone_bottom_px=caption_safe_zone_bottom_px,
        caption_safe_zone_top_px=caption_safe_zone_top_px,
        caption_font_name=caption_font_name,
        caption_font_asset_id=caption_font_asset_id,
        audio_target_lufs=audio_target_lufs,
        audio_lra=audio_lra,
        audio_true_peak=audio_true_peak,
        ducking_ratio=ducking_ratio,
        ducking_threshold=ducking_threshold,
        ducking_attack_ms=ducking_attack_ms,
        ducking_release_ms=ducking_release_ms,
        music_gain=music_gain,
        voice_gain=voice_gain,
        trim_silence=trim_silence,
        trim_silence_min_sec=trim_silence_min_sec,
        trim_silence_threshold_db=trim_silence_threshold_db,
        cache_key=cache_key,
        job_id_override=job_id_override,
    )


def render_offer_card_job(
    primary_asset_id: str,
    headline: str | None,
    price: str | None,
    cta: str | None,
    brand_kit_id: str | None,
    broll_asset_ids: list[str] | None,
    voice_asset_id: str | None,
    music_asset_id: str | None,
    captions_srt: str | None,
    captions_vtt: str | None,
    words_json: list[dict[str, Any]] | None,
    highlight_mode: str | None,
    include_16_9: bool | None,
    quality: str | None,
    framing_mode: str | None,
    caption_position: str | None,
    caption_font_size: int | None,
    caption_font_color: str | None,
    caption_box_color: str | None,
    caption_box_opacity: float | None,
    caption_highlight_color: str | None,
    caption_padding_px: int | None,
    caption_max_chars: int | None,
    caption_max_lines: int | None,
    caption_max_words: int | None,
    caption_safe_zone_profile: str | None,
    caption_safe_zone_bottom_px: int | None,
    caption_safe_zone_top_px: int | None,
    caption_font_name: str | None,
    caption_font_asset_id: str | None,
    audio_target_lufs: float | None,
    audio_lra: float | None,
    audio_true_peak: float | None,
    ducking_ratio: float | None,
    ducking_threshold: float | None,
    ducking_attack_ms: int | None,
    ducking_release_ms: int | None,
    music_gain: float | None,
    voice_gain: float | None,
    trim_silence: bool | None,
    trim_silence_min_sec: float | None,
    trim_silence_threshold_db: float | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    variables = {"headline": headline, "price": price, "cta": cta}
    return _render_marketing_job(
        job_type="render_offer_card",
        template_name="offer_card_basic",
        primary_asset_id=primary_asset_id,
        variables=variables,
        brand_kit_id=brand_kit_id,
        broll_asset_ids=broll_asset_ids,
        voice_asset_id=voice_asset_id,
        music_asset_id=music_asset_id,
        captions_srt=captions_srt,
        captions_vtt=captions_vtt,
        words_json=words_json,
        highlight_mode=highlight_mode,
        include_16_9=include_16_9,
        quality=quality,
        framing_mode=framing_mode,
        caption_position=caption_position,
        caption_font_size=caption_font_size,
        caption_font_color=caption_font_color,
        caption_box_color=caption_box_color,
        caption_box_opacity=caption_box_opacity,
        caption_highlight_color=caption_highlight_color,
        caption_padding_px=caption_padding_px,
        caption_max_chars=caption_max_chars,
        caption_max_lines=caption_max_lines,
        caption_max_words=caption_max_words,
        caption_safe_zone_profile=caption_safe_zone_profile,
        caption_safe_zone_bottom_px=caption_safe_zone_bottom_px,
        caption_safe_zone_top_px=caption_safe_zone_top_px,
        caption_font_name=caption_font_name,
        caption_font_asset_id=caption_font_asset_id,
        audio_target_lufs=audio_target_lufs,
        audio_lra=audio_lra,
        audio_true_peak=audio_true_peak,
        ducking_ratio=ducking_ratio,
        ducking_threshold=ducking_threshold,
        ducking_attack_ms=ducking_attack_ms,
        ducking_release_ms=ducking_release_ms,
        music_gain=music_gain,
        voice_gain=voice_gain,
        trim_silence=trim_silence,
        trim_silence_min_sec=trim_silence_min_sec,
        trim_silence_threshold_db=trim_silence_threshold_db,
        cache_key=cache_key,
        job_id_override=job_id_override,
    )



def _default_rubric_name(render_type: str) -> str:
    if render_type == "testimonial_clip":
        return "testimonial_v1"
    return "social_reel_v1"


def _primary_variant_preset(quality: str, framing_mode: str) -> str:
    if framing_mode == "crop":
        return "mp4_social_vertical_720x1280" if quality == "draft" else "mp4_social_vertical_1080x1920"
    return "mp4_social_vertical_720x1280_safe_pad" if quality == "draft" else "mp4_social_vertical_1080x1920_safe_pad"


def _estimate_crop_pct(width: int, height: int, target_ratio: float) -> float | None:
    if width <= 0 or height <= 0 or target_ratio <= 0:
        return None
    input_ratio = float(width) / float(height)
    if abs(input_ratio - target_ratio) < 0.0001:
        return 0.0
    if input_ratio > target_ratio:
        new_width = float(height) * target_ratio
        crop_ratio = 1.0 - (new_width / float(width))
    else:
        new_height = float(width) / target_ratio
        crop_ratio = 1.0 - (new_height / float(height))
    return max(0.0, crop_ratio * 100.0)


def render_iterate_job(
    render_type: str,
    primary_asset_id: str,
    hook: str | None,
    headline: str | None,
    cta: str | None,
    price: str | None,
    quote: str | None,
    author: str | None,
    brand_kit_id: str | None,
    broll_asset_ids: list[str] | None,
    voice_asset_id: str | None,
    music_asset_id: str | None,
    captions_srt: str | None,
    captions_vtt: str | None,
    words_json: list[dict[str, Any]] | None,
    highlight_mode: str | None,
    include_16_9: bool | None,
    quality: str | None,
    framing_mode: str | None,
    caption_position: str | None,
    caption_font_size: int | None,
    caption_font_color: str | None,
    caption_box_color: str | None,
    caption_box_opacity: float | None,
    caption_highlight_color: str | None,
    caption_padding_px: int | None,
    caption_max_chars: int | None,
    caption_max_lines: int | None,
    caption_max_words: int | None,
    caption_safe_zone_profile: str | None,
    caption_safe_zone_bottom_px: int | None,
    caption_safe_zone_top_px: int | None,
    caption_font_name: str | None,
    caption_font_asset_id: str | None,
    audio_target_lufs: float | None,
    audio_lra: float | None,
    audio_true_peak: float | None,
    ducking_ratio: float | None,
    ducking_threshold: float | None,
    ducking_attack_ms: int | None,
    ducking_release_ms: int | None,
    music_gain: float | None,
    voice_gain: float | None,
    trim_silence: bool | None,
    trim_silence_min_sec: float | None,
    trim_silence_threshold_db: float | None,
    strategy: str | None,
    caption_font_size_min: int | None,
    caption_font_size_max: int | None,
    caption_box_opacity_min: float | None,
    caption_box_opacity_max: float | None,
    music_gain_min: float | None,
    music_gain_max: float | None,
    max_crop_pct: float | None,
    min_duration_sec: float | None,
    fail_fast: bool | None,
    lock_framing: bool | None,
    lock_captions: bool | None,
    lock_audio: bool | None,
    allow_trim_silence: bool | None,
    rubric_name: str | None,
    pass_threshold: float | None,
    max_iterations: int | None,
    cache_key: str | None = None,
    job_id_override: str | None = None,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job_id_override if job_id_override is not None else (job.id if job else "")
    start_ts = job_timer()
    _log_job_started("render_iterate", job_id, primary_asset_id)
    _finish_job(
        job_id,
        "running",
        {"started_at": utc_now_iso(), "progress": 5, "cache_key": cache_key},
    )

    try:
        render_type = (render_type or "").strip().lower()
        if render_type not in {"social_ad", "testimonial_clip", "offer_card"}:
            raise JobError("render_type must be social_ad, testimonial_clip, or offer_card")
        quality = (quality or "final").strip().lower()
        if quality not in {"final", "draft"}:
            raise JobError("quality must be 'final' or 'draft'")

        brand_kit = get_brand_kit(brand_kit_id) if brand_kit_id else None
        if brand_kit_id and not brand_kit:
            raise JobError("brand_kit_id not found")

        caption_position = caption_position or (brand_kit.get("caption_position") if brand_kit else None)
        caption_font_size = caption_font_size if caption_font_size is not None else (
            brand_kit.get("caption_font_size") if brand_kit else settings.caption_font_size
        )
        caption_padding_px = caption_padding_px if caption_padding_px is not None else (
            brand_kit.get("caption_padding_px") if brand_kit else settings.caption_padding_px
        )
        caption_max_chars = caption_max_chars if caption_max_chars is not None else (
            brand_kit.get("caption_max_chars") if brand_kit else settings.caption_max_chars
        )
        caption_max_lines = caption_max_lines if caption_max_lines is not None else (
            brand_kit.get("caption_max_lines") if brand_kit else settings.caption_max_lines
        )
        caption_max_words = caption_max_words if caption_max_words is not None else (
            brand_kit.get("caption_max_words") if brand_kit else settings.caption_max_words
        )
        profile_bottom, profile_top = resolve_safe_zone_profile(caption_safe_zone_profile)
        caption_safe_zone_bottom_px = caption_safe_zone_bottom_px if caption_safe_zone_bottom_px is not None else (
            profile_bottom if profile_bottom is not None else (
                brand_kit.get("caption_safe_zone_bottom_px") if brand_kit else settings.caption_safe_zone_bottom_px
            )
        )
        caption_safe_zone_top_px = caption_safe_zone_top_px if caption_safe_zone_top_px is not None else (
            profile_top if profile_top is not None else (
                brand_kit.get("caption_safe_zone_top_px") if brand_kit else settings.caption_safe_zone_top_px
            )
        )

        caption_box_opacity = caption_box_opacity if caption_box_opacity is not None else (
            brand_kit.get("caption_box_opacity") if brand_kit else settings.caption_box_opacity
        )

        audio_target_lufs = audio_target_lufs if audio_target_lufs is not None else settings.audio_norm_i
        audio_lra = audio_lra if audio_lra is not None else settings.audio_norm_lra
        audio_true_peak = audio_true_peak if audio_true_peak is not None else settings.audio_norm_tp
        ducking_ratio = ducking_ratio if ducking_ratio is not None else settings.ducking_ratio
        ducking_threshold = ducking_threshold if ducking_threshold is not None else settings.ducking_threshold
        ducking_attack_ms = ducking_attack_ms if ducking_attack_ms is not None else settings.ducking_attack_ms
        ducking_release_ms = ducking_release_ms if ducking_release_ms is not None else settings.ducking_release_ms
        music_gain = music_gain if music_gain is not None else settings.ducking_music_gain

        trim_silence = bool(trim_silence) if trim_silence is not None else False
        trim_silence_min_sec = (
            float(trim_silence_min_sec)
            if trim_silence_min_sec is not None
            else settings.audio_min_silence_sec
        )
        trim_silence_threshold_db = (
            float(trim_silence_threshold_db)
            if trim_silence_threshold_db is not None
            else settings.audio_silence_db
        )

        strategy = (strategy or "balanced").strip().lower()
        if strategy not in ITERATE_STRATEGIES:
            allowed = ", ".join(ITERATE_STRATEGIES)
            raise JobError(f"strategy must be {allowed}")

        caption_font_size_min = int(caption_font_size_min) if caption_font_size_min is not None else settings.auto_caption_font_size_min
        caption_font_size_max = int(caption_font_size_max) if caption_font_size_max is not None else settings.auto_caption_font_size_max
        caption_box_opacity_min = float(caption_box_opacity_min) if caption_box_opacity_min is not None else settings.auto_caption_box_opacity_min
        caption_box_opacity_max = float(caption_box_opacity_max) if caption_box_opacity_max is not None else settings.auto_caption_box_opacity_max
        music_gain_min = float(music_gain_min) if music_gain_min is not None else settings.auto_music_gain_min
        music_gain_max = float(music_gain_max) if music_gain_max is not None else settings.auto_music_gain_max
        max_crop_pct = float(max_crop_pct) if max_crop_pct is not None else settings.auto_max_crop_pct
        min_duration_sec = float(min_duration_sec) if min_duration_sec is not None else settings.auto_min_duration_sec
        fail_fast = bool(fail_fast) if fail_fast is not None else False

        lock_framing = bool(lock_framing) if lock_framing is not None else False
        lock_captions = bool(lock_captions) if lock_captions is not None else False
        lock_audio = bool(lock_audio) if lock_audio is not None else False
        allow_trim_silence = True if allow_trim_silence is None else bool(allow_trim_silence)

        if caption_font_size_min > caption_font_size_max:
            raise JobError("caption_font_size_min exceeds max")
        if caption_box_opacity_min > caption_box_opacity_max:
            raise JobError("caption_box_opacity_min exceeds max")
        if music_gain_min > music_gain_max:
            raise JobError("music_gain_min exceeds max")

        caption_font_size = int(min(max(caption_font_size, caption_font_size_min), caption_font_size_max))
        caption_box_opacity = float(min(max(caption_box_opacity, caption_box_opacity_min), caption_box_opacity_max))
        music_gain = float(min(max(music_gain, music_gain_min), music_gain_max))

        rubric_name = rubric_name or _default_rubric_name(render_type)
        rubric = get_rubric(rubric_name)
        threshold = float(pass_threshold) if pass_threshold is not None else float(rubric.get("pass_threshold", 85))
        targets = rubric.get("targets", {})

        max_iterations = int(max_iterations) if max_iterations is not None else 2
        if max_iterations <= 0:
            raise JobError("max_iterations must be > 0")

        crop_allowed = True
        if max_crop_pct is not None:
            max_crop_pct = float(max_crop_pct)
            if max_crop_pct <= 0:
                crop_allowed = False
            else:
                primary_asset = _get_asset_or_error(primary_asset_id)
                _ensure_temp_dir()
                primary_path, primary_cleanup = _resolve_input_path(primary_asset)
                try:
                    probe = _probe_optional(primary_path)
                    if probe and probe.get("width") and probe.get("height"):
                        crop_pct = _estimate_crop_pct(int(probe["width"]), int(probe["height"]), 9.0 / 16.0)
                        if crop_pct is not None and crop_pct > max_crop_pct:
                            crop_allowed = False
                finally:
                    if primary_cleanup and os.path.exists(primary_path):
                        os.remove(primary_path)

        iterations: list[dict[str, Any]] = []
        best_result: dict[str, Any] | None = None
        prev_settings: dict[str, Any] | None = None

        for idx in range(max_iterations):
            variables = {}
            if render_type == "social_ad":
                variables = {"hook": hook, "headline": headline, "cta": cta, "price": price}
            elif render_type == "testimonial_clip":
                variables = {"quote": quote, "author": author}
            elif render_type == "offer_card":
                variables = {"headline": headline, "price": price, "cta": cta}

            current_settings = {
                "framing_mode": framing_mode,
                "caption_max_chars": caption_max_chars,
                "caption_max_lines": caption_max_lines,
                "caption_max_words": caption_max_words,
                "caption_font_size": caption_font_size,
                "caption_box_opacity": caption_box_opacity,
                "caption_safe_zone_bottom_px": caption_safe_zone_bottom_px,
                "caption_safe_zone_top_px": caption_safe_zone_top_px,
                "audio_target_lufs": audio_target_lufs,
                "audio_true_peak": audio_true_peak,
                "ducking_ratio": ducking_ratio,
                "music_gain": music_gain,
                "trim_silence": trim_silence,
                "trim_silence_min_sec": trim_silence_min_sec,
                "trim_silence_threshold_db": trim_silence_threshold_db,
            }
            changes: list[str] = []
            if prev_settings:
                for key, value in current_settings.items():
                    prev_value = prev_settings.get(key)
                    if value != prev_value:
                        changes.append(f"{key} {prev_value} -> {value}")

            render_result = _render_marketing_job(
                job_type=f"render_iterate:{render_type}",
                template_name={
                    "social_ad": "social_ad_basic",
                    "testimonial_clip": "testimonial_clip_basic",
                    "offer_card": "offer_card_basic",
                }[render_type],
                primary_asset_id=primary_asset_id,
                variables=variables,
                brand_kit_id=brand_kit_id,
                broll_asset_ids=broll_asset_ids,
                voice_asset_id=voice_asset_id,
                music_asset_id=music_asset_id,
                captions_srt=captions_srt,
                captions_vtt=captions_vtt,
                words_json=words_json,
                highlight_mode=highlight_mode,
                include_16_9=include_16_9,
                quality=quality,
                framing_mode=framing_mode,
                caption_position=caption_position,
                caption_font_size=caption_font_size,
                caption_font_color=caption_font_color,
                caption_box_color=caption_box_color,
                caption_box_opacity=caption_box_opacity,
                caption_highlight_color=caption_highlight_color,
                caption_padding_px=caption_padding_px,
                caption_max_chars=caption_max_chars,
                caption_max_lines=caption_max_lines,
                caption_max_words=caption_max_words,
                caption_safe_zone_profile=caption_safe_zone_profile,
                caption_safe_zone_bottom_px=caption_safe_zone_bottom_px,
                caption_safe_zone_top_px=caption_safe_zone_top_px,
                caption_font_name=caption_font_name,
                caption_font_asset_id=caption_font_asset_id,
                audio_target_lufs=audio_target_lufs,
                audio_lra=audio_lra,
                audio_true_peak=audio_true_peak,
                ducking_ratio=ducking_ratio,
                ducking_threshold=ducking_threshold,
                ducking_attack_ms=ducking_attack_ms,
                ducking_release_ms=ducking_release_ms,
                music_gain=music_gain,
                voice_gain=voice_gain,
                trim_silence=trim_silence,
                trim_silence_min_sec=trim_silence_min_sec,
                trim_silence_threshold_db=trim_silence_threshold_db,
                cache_key=None,
                job_id_override="",
            )

            outputs = render_result.get("outputs", {}) if isinstance(render_result, dict) else {}
            primary_variant = outputs.get("9x16") or next(iter(outputs.values()), {})
            eval_asset_id = None
            if captions_srt or captions_vtt or words_json:
                eval_asset_id = primary_variant.get("captioned")
            if not eval_asset_id:
                eval_asset_id = primary_variant.get("non_captioned")
            if not eval_asset_id:
                eval_asset_id = (render_result.get("output_asset_ids") or [None])[0]

            target_preset = _primary_variant_preset(quality, framing_mode or "safe_pad")
            analysis = video_analyze_job(
                eval_asset_id,
                rubric_name,
                target_preset,
                None,
                captions_srt,
                captions_vtt,
                words_json,
                brand_kit_id,
                caption_position,
                caption_font_size,
                caption_padding_px,
                caption_max_chars,
                caption_max_lines,
                caption_max_words,
                caption_safe_zone_bottom_px,
                caption_safe_zone_top_px,
                caption_safe_zone_profile,
                cache_key=None,
                job_id_override="",
            )
            rubric_report = analysis.get("rubric", {}) if isinstance(analysis, dict) else {}
            score = rubric_report.get("score")

            iterations.append(
                {
                    "iteration": idx + 1,
                    "score": score,
                    "outputs": outputs,
                    "analysis": analysis,
                    "changes": changes,
                    "settings": {
                        "framing_mode": framing_mode,
                        "caption_max_chars": caption_max_chars,
                        "caption_max_lines": caption_max_lines,
                        "caption_max_words": caption_max_words,
                        "caption_font_size": caption_font_size,
                        "caption_box_opacity": caption_box_opacity,
                        "caption_safe_zone_bottom_px": caption_safe_zone_bottom_px,
                        "caption_safe_zone_top_px": caption_safe_zone_top_px,
                        "audio_target_lufs": audio_target_lufs,
                        "audio_true_peak": audio_true_peak,
                        "ducking_ratio": ducking_ratio,
                        "music_gain": music_gain,
                        "trim_silence": trim_silence,
                        "trim_silence_min_sec": trim_silence_min_sec,
                        "trim_silence_threshold_db": trim_silence_threshold_db,
                    },
                }
            )
            prev_settings = current_settings

            video_metrics = analysis.get("video", {}) if isinstance(analysis, dict) else {}
            audio_metrics = analysis.get("audio", {}) if isinstance(analysis, dict) else {}
            caption_metrics = analysis.get("captions", {}) if isinstance(analysis, dict) else {}

            fatal_checks: list[dict[str, str]] = []
            if fail_fast:
                has_audio = audio_metrics.get("has_audio")
                has_video = video_metrics.get("has_video")
                if has_video is False:
                    fatal_checks.append(dict(FAIL_FAST_ERRORS["no_video_track"]))
                if (voice_asset_id or music_asset_id) and has_audio is False:
                    fatal_checks.append(dict(FAIL_FAST_ERRORS["no_audio_track"]))
                duration_sec = analysis.get("duration_sec") if isinstance(analysis, dict) else None
                if duration_sec is not None and min_duration_sec > 0 and float(duration_sec) < min_duration_sec:
                    fatal_checks.append(dict(FAIL_FAST_ERRORS["duration_too_short"]))
                if video_metrics.get("resolution_ok") is False:
                    fatal_checks.append(dict(FAIL_FAST_ERRORS["resolution_too_low"]))
                if (captions_srt or captions_vtt or words_json) and caption_metrics.get("segment_count") in {0, None}:
                    fatal_checks.append(dict(FAIL_FAST_ERRORS["captions_empty"]))
            if fatal_checks:
                qa = analysis.get("qa") if isinstance(analysis, dict) else None
                if not isinstance(qa, dict):
                    qa = qa_from_report(analysis, rubric, target_preset)
                qa["pass"] = False
                qa["failed_checks"] = [item["reason"] for item in fatal_checks[:3]]
                qa["failed_checks_codes"] = [item["code"] for item in fatal_checks[:3]]
                qa["recommended_fix"] = fatal_checks[0]["fix"]
                analysis["qa"] = qa
                iterations[-1]["analysis"] = analysis
                best_result = iterations[-1]
                break

            if score is not None and score >= threshold:
                best_result = iterations[-1]
                break

            readability = caption_metrics.get("caption_readability_score")
            speed_wpm = caption_metrics.get("caption_speed_wpm")
            loudness = audio_metrics.get("loudness_lufs")
            true_peak = audio_metrics.get("true_peak_db")
            silence_pct = audio_metrics.get("silence_pct")

            caption_needs = False
            if not lock_captions:
                if caption_metrics.get("safe_zone_violations"):
                    caption_needs = True
                if readability is not None and readability < targets.get("caption_readability_min", 70.0):
                    caption_needs = True
                if speed_wpm and speed_wpm > targets.get("caption_speed_wpm_max", 180.0):
                    caption_needs = True

            audio_needs = False
            if not lock_audio:
                target_lufs = float(targets.get("loudness_lufs", -16.0))
                tolerance = float(targets.get("loudness_tolerance", 2.5))
                if loudness is not None and (loudness < target_lufs - tolerance or loudness > target_lufs + tolerance):
                    audio_needs = True
                if true_peak is not None and true_peak > targets.get("true_peak_max_db", -1.5):
                    audio_needs = True
                if (
                    silence_pct is not None
                    and silence_pct > targets.get("silence_pct_max", 5.0)
                    and voice_asset_id
                    and allow_trim_silence
                ):
                    audio_needs = True

            framing_needs = False
            if not lock_framing and crop_allowed:
                if (
                    video_metrics.get("black_frames_pct") is not None
                    and video_metrics.get("black_frames_pct") > targets.get("black_frames_pct_max", 1.0)
                    and framing_mode == "safe_pad"
                ):
                    framing_needs = True

            def apply_caption_adjustments() -> None:
                nonlocal caption_safe_zone_bottom_px, caption_max_chars, caption_max_words, caption_max_lines
                nonlocal caption_font_size, caption_box_opacity
                if caption_metrics.get("safe_zone_violations"):
                    caption_safe_zone_bottom_px = int(caption_safe_zone_bottom_px or settings.caption_safe_zone_bottom_px) + 16
                if readability is not None and readability < targets.get("caption_readability_min", 70.0):
                    caption_max_chars = max(40, int(caption_max_chars) - 6)
                    caption_max_words = max(6, int(caption_max_words) - 1)
                    caption_max_lines = min(3, int(caption_max_lines) + 1)
                    caption_font_size = max(caption_font_size_min, int(caption_font_size) - 2)
                    if caption_box_opacity < caption_box_opacity_max:
                        caption_box_opacity = min(
                            caption_box_opacity_max,
                            float(caption_box_opacity) + 0.05,
                        )
                if speed_wpm and speed_wpm > targets.get("caption_speed_wpm_max", 180.0):
                    caption_max_words = max(5, int(caption_max_words) - 1)

            def apply_audio_adjustments() -> None:
                nonlocal audio_target_lufs, audio_true_peak, music_gain, trim_silence
                nonlocal trim_silence_min_sec, trim_silence_threshold_db
                target_lufs = float(targets.get("loudness_lufs", -16.0))
                tolerance = float(targets.get("loudness_tolerance", 2.5))
                if loudness is not None:
                    if loudness < target_lufs - tolerance:
                        audio_target_lufs = min(-12.0, float(audio_target_lufs) + 1.0)
                    elif loudness > target_lufs + tolerance:
                        audio_target_lufs = max(-23.0, float(audio_target_lufs) - 1.0)
                if true_peak is not None and true_peak > targets.get("true_peak_max_db", -1.5):
                    audio_true_peak = min(float(audio_true_peak), targets.get("true_peak_max_db", -1.5))
                    music_gain = max(music_gain_min, float(music_gain) - 0.05)
                if (
                    silence_pct is not None
                    and silence_pct > targets.get("silence_pct_max", 5.0)
                    and voice_asset_id
                    and allow_trim_silence
                ):
                    trim_silence = True
                    trim_silence_min_sec = max(0.2, float(trim_silence_min_sec) - 0.1)
                    trim_silence_threshold_db = min(-20.0, float(trim_silence_threshold_db) + 2.0)
                music_gain = float(min(max(music_gain, music_gain_min), music_gain_max))

            def apply_framing_adjustments() -> None:
                nonlocal framing_mode
                if framing_needs:
                    framing_mode = "crop"

            if strategy == "balanced":
                if framing_needs:
                    apply_framing_adjustments()
                if caption_needs:
                    apply_caption_adjustments()
                if audio_needs:
                    apply_audio_adjustments()
            else:
                strategy_order = {
                    "captions_first": ["captions", "audio", "framing"],
                    "audio_first": ["audio", "captions", "framing"],
                    "framing_first": ["framing", "captions", "audio"],
                }[strategy]
                for item in strategy_order:
                    if item == "captions" and caption_needs:
                        apply_caption_adjustments()
                        break
                    if item == "audio" and audio_needs:
                        apply_audio_adjustments()
                        break
                    if item == "framing" and framing_needs:
                        apply_framing_adjustments()
                        break

            update_job(
                job_id,
                {
                    "progress": 5 + int(90 * ((idx + 1) / max_iterations)),
                    "updated_at": utc_now_iso(),
                },
            )

        if best_result is None and iterations:
            best_result = max(iterations, key=lambda item: item.get("score") or 0)

        qa = None
        if best_result and isinstance(best_result.get("analysis"), dict):
            analysis_report = best_result.get("analysis", {})
            qa = analysis_report.get("qa")
            if not isinstance(qa, dict):
                qa = qa_from_report(
                    analysis_report,
                    rubric,
                    analysis_report.get("target_preset"),
                )

        result_payload = {
            "rubric_name": rubric_name,
            "pass_threshold": threshold,
            "iterations": iterations,
            "best": best_result,
            "qa": qa,
        }
        _finish_job(
            job_id,
            "success",
            {
                "output_asset_ids": [best_result.get("analysis", {}).get("asset_id")] if best_result else [],
                "result": result_payload,
                "qa": qa,
                "logs_short": "iteration complete",
                "finished_at": utc_now_iso(),
            },
        )
        _record_job_metrics("render_iterate", start_ts, "success", job_id)
        if cache_key:
            set_cached_result(
                cache_key,
                {
                    "output_asset_ids": [best_result.get("analysis", {}).get("asset_id")] if best_result else [],
                    "created_at": utc_now_iso(),
                    "job_type": "render_iterate",
                    "result": result_payload,
                    "qa": qa,
                },
                settings.asset_ttl_seconds(),
            )
        return result_payload
    except (FfmpegError, JobError, ValueError) as exc:
        _finish_job(
            job_id,
            "error",
            {"error": str(exc), "logs_short": None, "finished_at": utc_now_iso()},
        )
        _record_job_metrics("render_iterate", start_ts, "error", job_id)
        raise
