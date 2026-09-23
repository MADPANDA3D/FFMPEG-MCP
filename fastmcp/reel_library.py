"""Durable reusable-Reel metadata and upload contracts.

Binary uploads are written to an isolated temporary path by the HTTP transport;
completion verifies the caller-declared digest before promoting the bytes into
the existing content-addressed storage backend.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
import uuid
from typing import Any

from config import settings
from redis_store import get_redis
from storage import put_file
from utils import utc_now_iso


UPLOAD_PREFIX = "reel:upload:"
CLIP_PREFIX = "reel:clip:"
CLIP_SET = "reel:clips"
CLIP_SHA_PREFIX = "reel:clip:sha256:"
TEMPLATE_PREFIX = "reel:template:"
TEMPLATE_SET = "reel:templates"
MAX_UPLOAD_BYTES = 2 * 1024 * 1024 * 1024
SAFE_ID = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$")


def _load(key: str) -> dict[str, Any] | None:
    raw = get_redis().get(key)
    return json.loads(raw) if raw else None


def _save(key: str, value: dict[str, Any], *, expiry: int | None = None) -> None:
    get_redis().set(key, json.dumps(value, ensure_ascii=True), ex=expiry)


def begin_upload(filename: str, mime_type: str, size_bytes: int, sha256: str) -> dict[str, Any]:
    safe_name = os.path.basename(filename.strip())
    digest = sha256.lower().strip()
    if not safe_name or safe_name != filename or len(safe_name) > 255:
        raise ValueError("filename must be a safe basename")
    if not mime_type.startswith(("video/", "audio/", "image/")):
        raise ValueError("mime_type must be video, audio, or image")
    if not 0 < size_bytes <= MAX_UPLOAD_BYTES:
        raise ValueError("size_bytes is outside the supported upload range")
    if not re.fullmatch(r"[a-f0-9]{64}", digest):
        raise ValueError("sha256 must be 64 lowercase hexadecimal characters")
    upload_id = uuid.uuid4().hex
    expires_at = int(time.time()) + 900
    path = os.path.join(settings.storage_temp_dir, f"direct_{upload_id}.part")
    session = {
        "upload_id": upload_id,
        "filename": safe_name,
        "mime_type": mime_type,
        "size_bytes": size_bytes,
        "sha256": digest,
        "temp_path": path,
        "expires_at": expires_at,
    }
    _save(f"{UPLOAD_PREFIX}{upload_id}", session, expiry=900)
    base = settings.public_base_url.rstrip("/")
    return {
        "upload_id": upload_id,
        "upload_url": f"{base}/upload/{upload_id}",
        "method": "PUT",
        "required_headers": {"Content-Type": mime_type, "X-Content-SHA256": digest},
        "expires_at": expires_at,
        "max_size_bytes": MAX_UPLOAD_BYTES,
    }


def get_upload(upload_id: str) -> dict[str, Any] | None:
    return _load(f"{UPLOAD_PREFIX}{upload_id}")


def complete_upload(upload_id: str, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    session = get_upload(upload_id)
    if not session:
        raise ValueError("upload_id not found or expired")
    path = session["temp_path"]
    if not os.path.isfile(path):
        raise ValueError("upload bytes have not been received")
    size = os.path.getsize(path)
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    actual = digest.hexdigest()
    if size != session["size_bytes"] or actual != session["sha256"]:
        os.remove(path)
        get_redis().delete(f"{UPLOAD_PREFIX}{upload_id}")
        raise ValueError("uploaded bytes do not match declared size and sha256")
    duplicate_id = get_redis().get(f"{CLIP_SHA_PREFIX}{actual}")
    if duplicate_id:
        os.remove(path)
        get_redis().delete(f"{UPLOAD_PREFIX}{upload_id}")
        return {"clip": get_clip(duplicate_id), "deduplicated": True}
    clip_id = f"clip_{actual[:24]}"
    ext = os.path.splitext(session["filename"])[1]
    storage_key, storage_uri, stored_size = put_file(path, clip_id, ext)
    now = utc_now_iso()
    fields = sanitize_clip_metadata(metadata or {})
    clip = {
        "clip_id": clip_id,
        "sha256": actual,
        "mime_type": session["mime_type"],
        "size_bytes": stored_size,
        "original_filename": session["filename"],
        "storage_key": storage_key,
        "storage_uri": storage_uri,
        "source_type": "direct_upload",
        "archived": False,
        "created_at": now,
        "updated_at": now,
        **fields,
    }
    _save(f"{CLIP_PREFIX}{clip_id}", clip)
    client = get_redis()
    client.sadd(CLIP_SET, clip_id)
    client.set(f"{CLIP_SHA_PREFIX}{actual}", clip_id)
    client.delete(f"{UPLOAD_PREFIX}{upload_id}")
    return {"clip": privacy_safe_clip(clip), "deduplicated": False}


def sanitize_clip_metadata(value: dict[str, Any]) -> dict[str, Any]:
    allowed = {"topic", "visual_role", "orientation", "duration_sec", "approval_status", "tags", "provenance"}
    result = {key: value[key] for key in allowed if key in value}
    tags = result.get("tags", [])
    if not isinstance(tags, list) or len(tags) > 50 or any(not isinstance(tag, str) or len(tag) > 64 for tag in tags):
        raise ValueError("tags must contain at most 50 strings of 64 characters")
    for key in ("topic", "visual_role", "orientation", "approval_status", "provenance"):
        if key in result and (not isinstance(result[key], str) or len(result[key]) > 256):
            raise ValueError(f"{key} must be a string of at most 256 characters")
    if "duration_sec" in result and not 0 <= float(result["duration_sec"]) <= 86400:
        raise ValueError("duration_sec is outside the supported range")
    return result


def privacy_safe_clip(clip: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in clip.items() if key not in {"storage_key", "storage_uri", "temp_path"}}


def get_clip(clip_id: str) -> dict[str, Any]:
    clip = _load(f"{CLIP_PREFIX}{clip_id}")
    if not clip:
        raise ValueError("clip_id not found")
    return privacy_safe_clip(clip)


def list_clips(query: str | None = None, tags: list[str] | None = None, include_archived: bool = False) -> list[dict[str, Any]]:
    needle = (query or "").strip().lower()
    required_tags = {tag.lower() for tag in tags or []}
    results = []
    for clip_id in sorted(get_redis().smembers(CLIP_SET)):
        clip = _load(f"{CLIP_PREFIX}{clip_id}")
        if not clip or (clip.get("archived") and not include_archived):
            continue
        haystack = " ".join(str(clip.get(key, "")) for key in ("topic", "visual_role", "original_filename", "provenance")).lower()
        clip_tags = {str(tag).lower() for tag in clip.get("tags", [])}
        if needle and needle not in haystack and needle not in " ".join(clip_tags):
            continue
        if not required_tags.issubset(clip_tags):
            continue
        results.append(privacy_safe_clip(clip))
    return results


def update_clip(clip_id: str, metadata: dict[str, Any]) -> dict[str, Any]:
    clip = _load(f"{CLIP_PREFIX}{clip_id}")
    if not clip:
        raise ValueError("clip_id not found")
    clip.update(sanitize_clip_metadata(metadata))
    clip["updated_at"] = utc_now_iso()
    _save(f"{CLIP_PREFIX}{clip_id}", clip)
    return privacy_safe_clip(clip)


def archive_clip(clip_id: str) -> dict[str, Any]:
    clip = _load(f"{CLIP_PREFIX}{clip_id}")
    if not clip:
        raise ValueError("clip_id not found")
    clip.update({"archived": True, "updated_at": utc_now_iso()})
    _save(f"{CLIP_PREFIX}{clip_id}", clip)
    return privacy_safe_clip(clip)


def save_template(template_id: str, definition: dict[str, Any], change_note: str) -> dict[str, Any]:
    if not SAFE_ID.fullmatch(template_id):
        raise ValueError("template_id must be a safe identifier")
    if not isinstance(definition, dict) or not definition:
        raise ValueError("definition must be a non-empty object")
    versions_key = f"{TEMPLATE_PREFIX}{template_id}:versions"
    version = int(get_redis().llen(versions_key)) + 1
    record = {"template_id": template_id, "version": version, "definition": definition, "change_note": change_note[:500], "created_at": utc_now_iso()}
    get_redis().rpush(versions_key, json.dumps(record, ensure_ascii=True))
    get_redis().sadd(TEMPLATE_SET, template_id)
    return record


def get_template(template_id: str, version: int | None = None) -> dict[str, Any]:
    values = get_redis().lrange(f"{TEMPLATE_PREFIX}{template_id}:versions", 0, -1)
    if not values:
        raise ValueError("template_id not found")
    records = [json.loads(value) for value in values]
    if version is None:
        return records[-1]
    for record in records:
        if record["version"] == version:
            return record
    raise ValueError("template version not found")


def list_versioned_templates() -> list[dict[str, Any]]:
    return [get_template(template_id) for template_id in sorted(get_redis().smembers(TEMPLATE_SET))]


MADPANDA_REEL_CONTRACT = {
    "template_id": "madpanda_vertical_reel",
    "canvas": {"width": 1080, "height": 1920, "fps": 24},
    "duration_seconds": {"minimum": 30, "maximum": 45},
    "video": {"codec": "h264", "pixel_format": "yuv420p", "faststart": True},
    "audio": {"codec": "aac", "channels": 2, "sample_rate": 48000, "target_lufs": -14.0, "true_peak_dbtp": -1.5, "lra": 10.0, "voice_gain": 0.72, "highpass_hz": 70, "eq": [{"hz": 180, "q": 1.1, "gain_db": 1.2}, {"hz": 3000, "q": 1.2, "gain_db": 1.5}], "compressor": {"threshold": 0.125, "ratio": 2.0, "attack_ms": 15, "release_ms": 180, "makeup": 1.25}, "music_phases": [0.42, 0.30, 0.43], "sidechain": {"threshold": 0.025, "ratio": 3.0, "attack_ms": 10, "release_ms": 250}, "limiter": 0.95},
    "outro": {"versioned": True, "default_duration_sec": 5.5},
}


def validate_reel_plan(plan: dict[str, Any]) -> dict[str, Any]:
    duration = float(plan.get("duration_sec", 0))
    errors: list[str] = []
    if not 30 <= duration <= 45:
        errors.append("duration_sec must be between 30 and 45 inclusive")
    shots = plan.get("shots")
    if not isinstance(shots, list) or not shots:
        errors.append("shots must be a non-empty ordered array")
    elif any(not isinstance(shot, dict) or not shot.get("clip_id") for shot in shots):
        errors.append("every shot requires clip_id and may include source trims")
    for required in ("narration_clip_id", "music_clip_id", "outro_clip_id"):
        if not plan.get(required):
            errors.append(f"{required} is required")
    return {"ok": not errors, "errors": errors, "contract": MADPANDA_REEL_CONTRACT, "input_fingerprint": hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}
