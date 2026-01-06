import hashlib
import os
import uuid
from typing import Any

from rq import get_current_job

from config import settings
from ffmpeg_utils import FfmpegError, run_ffmpeg
from ffprobe_utils import run_ffprobe
from presets import get_preset
from redis_store import get_asset, save_asset, set_cached_result, update_job
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
    if status in {"success", "error"}:
        updates.setdefault("progress", 100)
    updates = {**updates, "status": status, "updated_at": utc_now_iso()}
    update_job(job_id, updates)


def transcode_job(asset_id: str, preset: str, cache_key: str | None = None) -> dict[str, Any]:
    job = get_current_job()
    job_id = job.id if job else ""
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
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if os.path.exists(output_path):
            os.remove(output_path)


def thumbnail_job(asset_id: str, time_sec: float, width: int | None, cache_key: str | None = None) -> dict[str, Any]:
    job = get_current_job()
    job_id = job.id if job else ""
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
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if os.path.exists(output_path):
            os.remove(output_path)


def extract_audio_job(asset_id: str, fmt: str, bitrate: str | None, cache_key: str | None = None) -> dict[str, Any]:
    job = get_current_job()
    job_id = job.id if job else ""
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
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if os.path.exists(output_path):
            os.remove(output_path)


def trim_job(asset_id: str, start_sec: float, end_sec: float, reencode: bool, cache_key: str | None = None) -> dict[str, Any]:
    job = get_current_job()
    job_id = job.id if job else ""
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
        raise
    finally:
        if cleanup and os.path.exists(input_path):
            os.remove(input_path)
        if os.path.exists(output_path):
            os.remove(output_path)
