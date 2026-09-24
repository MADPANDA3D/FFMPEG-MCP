"""Bounded MADPANDA3D vertical Reel rendering and delivery QA."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
import uuid
from typing import Any

from rq import get_current_job

from config import settings
from ffmpeg_utils import FfmpegError, run_ffmpeg
from ffprobe_utils import run_ffprobe
from overlay_utils import (
    DEFAULT_BOX_BORDER_WIDTH,
    DEFAULT_BOX_COLOR,
    DEFAULT_FONT_COLOR,
    DEFAULT_FONT_SIZE,
    escape_drawtext_value,
    resolve_font_path,
    sanitize_box_border,
    sanitize_color,
    sanitize_font_size,
    sanitize_text,
)
from redis_store import get_asset, save_asset, update_job
from reel_library import MADPANDA_REEL_CONTRACT, get_template
from storage import download_to_temp, generate_download_url, local_path_from_key, put_file
from utils import utc_now_iso, utc_now_ts


class ReelRenderError(RuntimeError):
    pass


def _sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _parse_loudnorm(logs: str) -> dict[str, float | None]:
    payload: dict[str, Any] = {}
    for raw in reversed(re.findall(r"\{[^{}]*\}", logs or "", flags=re.DOTALL)):
        try:
            candidate = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict) and {"input_i", "input_tp", "input_lra"} & set(candidate):
            payload = candidate
            break

    def number(key: str) -> float | None:
        try:
            return float(payload[key])
        except (KeyError, TypeError, ValueError):
            return None

    return {
        "integrated_lufs": number("input_i"),
        "true_peak_dbtp": number("input_tp"),
        "lra": number("input_lra"),
        "threshold": number("input_thresh"),
        "target_offset": number("target_offset"),
    }


def _has_faststart(path: str) -> bool:
    with open(path, "rb") as handle:
        head = handle.read(4 * 1024 * 1024)
    moov = head.find(b"moov")
    mdat = head.find(b"mdat")
    return moov >= 0 and (mdat < 0 or moov < mdat)


def _finish(job_id: str, status: str, **updates: Any) -> None:
    if job_id:
        update_job(job_id, {"status": status, "updated_at": utc_now_iso(), **updates})


def _store_asset(
    path: str,
    *,
    mime_type: str,
    extension: str,
    parent_asset_id: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    asset_id = uuid.uuid4().hex
    sha256 = _sha256_file(path)
    storage_key, storage_uri, size_bytes = put_file(path, asset_id, extension)
    expires_at = utc_now_ts() + settings.asset_ttl_seconds()
    asset = {
        "asset_id": asset_id,
        "source": "madpanda_reel_render",
        "original_filename": os.path.basename(storage_key),
        "mime_type": mime_type,
        "size_bytes": size_bytes,
        "sha256": sha256,
        "storage_key": storage_key,
        "storage_uri": storage_uri,
        "created_at": utc_now_iso(),
        "expires_at": expires_at,
        "parent_asset_id": parent_asset_id,
        **metadata,
    }
    save_asset(asset, settings.asset_ttl_seconds())
    return asset


def _resolve_asset(asset_id: str, temporary_paths: set[str]) -> tuple[dict[str, Any], str]:
    asset = get_asset(asset_id)
    if not asset or not asset.get("storage_key"):
        raise ReelRenderError(f"asset not found: {asset_id}")
    if settings.storage_backend == "s3":
        path = download_to_temp(asset["storage_key"])
        temporary_paths.add(path)
    else:
        path = local_path_from_key(asset["storage_key"])
    if not os.path.isfile(path):
        raise ReelRenderError(f"asset bytes not found: {asset_id}")
    return asset, path


def _video_filter(index: int, duration: float) -> str:
    return (
        f"[{index}:v:0]"
        "scale=1080:1920:force_original_aspect_ratio=decrease,"
        "pad=1080:1920:(ow-iw)/2:(oh-ih)/2:color=black,"
        f"fps=24,setsar=1,trim=duration={duration:.3f},setpts=PTS-STARTPTS[v{index}]"
    )


def _overlay_position(position: str) -> tuple[str, str]:
    return {
        "top": ("(w-text_w)/2", "h*0.10"),
        "center": ("(w-text_w)/2", "(h-text_h)/2"),
        "bottom": ("(w-text_w)/2", "h*0.82"),
    }[position]


def reel_render_job(
    input_asset_id: str,
    plan: dict[str, Any],
    input_fingerprint: str,
) -> dict[str, Any]:
    job = get_current_job()
    job_id = job.id if job else ""
    _finish(job_id, "running", started_at=utc_now_iso(), progress=5)

    temporary_paths: set[str] = set()
    text_paths: list[str] = []
    premaster_path = os.path.join(settings.storage_temp_dir, f"reel_{uuid.uuid4().hex}.mkv")
    output_path = os.path.join(settings.storage_temp_dir, f"reel_{uuid.uuid4().hex}.mp4")
    contact_path = os.path.join(settings.storage_temp_dir, f"reel_{uuid.uuid4().hex}.jpg")
    font_path = None
    font_cleanup = False
    try:
        template = get_template(plan["template_id"], plan.get("template_version"))
        if template.get("definition", {}).get("kind") != "madpanda_vertical_reel":
            raise ReelRenderError("template kind must be madpanda_vertical_reel")

        duration = float(plan["duration_sec"])
        outro_duration = float(plan.get("outro_duration_sec", 5.5))
        shots = list(plan["shots"])
        visual_inputs = shots + [
            {
                "clip_id": plan["outro_clip_id"],
                "start_sec": 0,
                "duration_sec": outro_duration,
            }
        ]

        resolved: dict[str, tuple[dict[str, Any], str]] = {}
        requested_ids = [item["clip_id"] for item in visual_inputs] + [
            plan["narration_clip_id"],
            plan["music_clip_id"],
        ]
        for asset_id in dict.fromkeys(requested_ids):
            resolved[asset_id] = _resolve_asset(asset_id, temporary_paths)
        input_hashes = {asset_id: _sha256_file(path) for asset_id, (_, path) in resolved.items()}

        args: list[str] = []
        filters: list[str] = []
        for index, item in enumerate(visual_inputs):
            asset, path = resolved[item["clip_id"]]
            item_duration = float(item["duration_sec"])
            start = float(item.get("start_sec", 0))
            probe = run_ffprobe(path)
            mime_type = str(asset.get("mime_type") or "")
            if mime_type.startswith("image/"):
                args.extend(["-loop", "1", "-t", f"{item_duration:.3f}", "-i", path])
            elif mime_type.startswith("video/"):
                source_duration = probe.get("duration_sec")
                if source_duration and start + item_duration > float(source_duration) + 0.05:
                    raise ReelRenderError(f"source trim exceeds duration: {item['clip_id']}")
                args.extend(["-ss", f"{start:.3f}", "-t", f"{item_duration:.3f}", "-i", path])
            else:
                raise ReelRenderError(f"visual asset must be video or image: {item['clip_id']}")
            filters.append(_video_filter(index, item_duration))

        narration_index = len(visual_inputs)
        music_index = narration_index + 1
        narration_asset, narration_path = resolved[plan["narration_clip_id"]]
        music_asset, music_path = resolved[plan["music_clip_id"]]
        if not str(narration_asset.get("mime_type") or "").startswith(("audio/", "video/")):
            raise ReelRenderError("narration asset must contain audio")
        if not str(music_asset.get("mime_type") or "").startswith(("audio/", "video/")):
            raise ReelRenderError("music asset must contain audio")
        if not any(stream.get("codec_type") == "audio" for stream in run_ffprobe(narration_path)["streams"]):
            raise ReelRenderError("narration asset has no audio stream")
        if not any(stream.get("codec_type") == "audio" for stream in run_ffprobe(music_path)["streams"]):
            raise ReelRenderError("music asset has no audio stream")
        args.extend(["-i", narration_path, "-stream_loop", "-1", "-i", music_path])

        concat_inputs = "".join(f"[v{index}]" for index in range(len(visual_inputs)))
        filters.append(f"{concat_inputs}concat=n={len(visual_inputs)}:v=1:a=0[vbase]")

        current_video = "vbase"
        overlays = list(plan.get("overlays") or [])
        if overlays:
            font_path, font_cleanup = resolve_font_path(None, None)
        for index, overlay in enumerate(overlays):
            cleaned_text = sanitize_text(overlay["text"])
            with tempfile.NamedTemporaryFile(
                dir=settings.storage_temp_dir,
                prefix="reel_text_",
                suffix=".txt",
                delete=False,
            ) as handle:
                handle.write(cleaned_text.encode("utf-8"))
                text_paths.append(handle.name)
            position = str(overlay.get("position", "bottom")).lower()
            if position not in {"top", "center", "bottom"}:
                raise ReelRenderError("overlay position must be top, center, or bottom")
            x_expr, y_expr = _overlay_position(position)
            start = float(overlay["start_sec"])
            end = float(overlay["end_sec"])
            next_video = f"vo{index}"
            drawtext = ":".join(
                [
                    f"fontfile={escape_drawtext_value(font_path or '')}",
                    f"textfile={escape_drawtext_value(text_paths[-1])}",
                    f"fontcolor={sanitize_color(overlay.get('font_color'), DEFAULT_FONT_COLOR)}",
                    f"fontsize={sanitize_font_size(overlay.get('font_size'), DEFAULT_FONT_SIZE)}",
                    f"x={x_expr}",
                    f"y={y_expr}",
                    "expansion=none",
                    "box=1",
                    f"boxcolor={sanitize_color(overlay.get('box_color'), DEFAULT_BOX_COLOR)}",
                    f"boxborderw={sanitize_box_border(overlay.get('box_border_width'), DEFAULT_BOX_BORDER_WIDTH)}",
                    f"enable=between(t\\,{start:.3f}\\,{end:.3f})",
                ]
            )
            filters.append(f"[{current_video}]drawtext={drawtext}[{next_video}]")
            current_video = next_video

        body_duration = duration - outro_duration
        phase_one = duration * 0.25
        phase_two = duration * 0.72
        filters.extend(
            [
                f"[{narration_index}:a:0]highpass=f=70,"
                "equalizer=f=180:t=q:w=1.1:g=1.2,"
                "equalizer=f=3000:t=q:w=1.2:g=1.5,"
                "acompressor=threshold=0.125:ratio=2:attack=15:release=180:makeup=1.25,"
                f"volume=0.72,apad=whole_dur={duration:.3f},atrim=0:{duration:.3f},asetpts=PTS-STARTPTS[voice]",
                "[voice]asplit=2[voice_sc][voice_mix]",
                f"[{music_index}:a:0]aformat=sample_rates=48000:channel_layouts=stereo,"
                f"atrim=0:{duration:.3f},asetpts=PTS-STARTPTS,"
                f"volume='if(lt(t,{phase_one:.3f}),0.42,if(lt(t,{phase_two:.3f}),0.30,0.43))':eval=frame[music]",
                "[music][voice_sc]sidechaincompress=threshold=0.025:ratio=3:attack=10:release=250[ducked]",
                "[voice_mix][ducked]amix=inputs=2:duration=longest:normalize=0[premix]",
            ]
        )

        outro_index = len(visual_inputs) - 1
        outro_probe = run_ffprobe(resolved[plan["outro_clip_id"]][1])
        if any(stream.get("codec_type") == "audio" for stream in outro_probe["streams"]):
            filters.extend(
                [
                    f"[{outro_index}:a:0]atrim=0:{outro_duration:.3f},asetpts=PTS-STARTPTS,"
                    f"adelay={int(round(body_duration * 1000))}:all=1[sting]",
                    "[premix][sting]amix=inputs=2:duration=longest:normalize=0[premaster]",
                ]
            )
        else:
            filters.append("[premix]anull[premaster]")
        filters.append(
            f"[premaster]aformat=sample_rates=48000:channel_layouts=stereo,"
            f"atrim=0:{duration:.3f}[aout]"
        )

        quality = str(plan.get("quality") or "high").lower()
        preset, crf = ("slow", "18") if quality == "high" else ("medium", "21")
        args.extend(
            [
                "-filter_complex",
                ";".join(filters),
                "-map",
                f"[{current_video}]",
                "-map",
                "[aout]",
                "-t",
                f"{duration:.3f}",
                "-c:v",
                "libx264",
                "-preset",
                preset,
                "-crf",
                crf,
                "-pix_fmt",
                "yuv420p",
                "-r",
                "24",
                "-c:a",
                "pcm_s24le",
                "-ar",
                "48000",
                "-ac",
                "2",
                premaster_path,
            ]
        )
        os.makedirs(settings.storage_temp_dir, exist_ok=True)
        render_logs = run_ffmpeg(args, timeout=settings.workflow_timeout_seconds())
        first_pass_logs = run_ffmpeg(
            [
                "-i",
                premaster_path,
                "-vn",
                "-af",
                "loudnorm=I=-14:LRA=10:TP=-1.5:print_format=json",
                "-f",
                "null",
                "-",
            ],
            timeout=settings.audio_timeout_seconds(),
        )
        first_pass = _parse_loudnorm(first_pass_logs)
        if any(first_pass[key] is None for key in ("integrated_lufs", "true_peak_dbtp", "lra", "threshold", "target_offset")):
            raise ReelRenderError("could not measure premaster loudness")
        normalize = (
            "loudnorm=I=-14:LRA=10:TP=-1.5:"
            f"measured_I={first_pass['integrated_lufs']}:"
            f"measured_TP={first_pass['true_peak_dbtp']}:"
            f"measured_LRA={first_pass['lra']}:"
            f"measured_thresh={first_pass['threshold']}:"
            f"offset={first_pass['target_offset']}:linear=true,"
            "alimiter=limit=0.95:level=false,"
            "aformat=sample_rates=48000:channel_layouts=stereo"
        )
        render_logs += run_ffmpeg(
            [
                "-i",
                premaster_path,
                "-map",
                "0:v:0",
                "-map",
                "0:a:0",
                "-c:v",
                "copy",
                "-af",
                normalize,
                "-c:a",
                "aac",
                "-b:a",
                "192k",
                "-ar",
                "48000",
                "-ac",
                "2",
                "-movflags",
                "+faststart",
                output_path,
            ],
            timeout=settings.workflow_timeout_seconds(),
        )
        _finish(job_id, "running", progress=70)

        run_ffmpeg(["-v", "error", "-i", output_path, "-f", "null", "-"], timeout=settings.workflow_timeout_seconds())
        probe = run_ffprobe(output_path)
        loudness_logs = run_ffmpeg(
            [
                "-i",
                output_path,
                "-vn",
                "-af",
                "loudnorm=I=-14:LRA=10:TP=-1.5:print_format=json",
                "-f",
                "null",
                "-",
            ],
            timeout=settings.audio_timeout_seconds(),
        )
        loudness = _parse_loudnorm(loudness_logs)
        audio_stream = next(
            (stream for stream in probe["streams"] if stream.get("codec_type") == "audio"),
            {},
        )
        checks = {
            "full_decode": True,
            "duration_30_45": 30 <= float(probe.get("duration_sec") or 0) <= 45,
            "requested_duration": abs(float(probe.get("duration_sec") or 0) - duration) <= 0.15,
            "canvas_1080x1920": probe.get("width") == 1080 and probe.get("height") == 1920,
            "fps_24": abs(float(probe.get("fps") or 0) - 24) <= 0.02,
            "h264_yuv420p": probe.get("video_codec") == "h264" and probe.get("pixel_format") == "yuv420p",
            "aac_stereo_48khz": (
                probe.get("audio_codec") == "aac"
                and int(audio_stream.get("channels") or 0) == 2
                and int(audio_stream.get("sample_rate") or 0) == 48000
            ),
            "faststart": _has_faststart(output_path),
            "loudness_target": loudness["integrated_lufs"] is not None and -15 <= loudness["integrated_lufs"] <= -13,
            "true_peak_target": loudness["true_peak_dbtp"] is not None and loudness["true_peak_dbtp"] <= -1.0,
            "lra_target": loudness["lra"] is not None and loudness["lra"] <= 10.5,
            "inputs_unchanged": all(
                _sha256_file(resolved[asset_id][1]) == digest
                for asset_id, digest in input_hashes.items()
            ),
        }
        failed = [name for name, passed in checks.items() if not passed]
        if failed:
            raise ReelRenderError(
                "delivery QA failed: "
                + ", ".join(failed)
                + f"; loudness={json.dumps(loudness, sort_keys=True)}"
            )

        run_ffmpeg(
            [
                "-i",
                output_path,
                "-vf",
                "fps=1/6,scale=270:480,tile=3x2:nb_frames=6",
                "-frames:v",
                "1",
                contact_path,
            ],
            timeout=settings.ffmpeg_timeout_seconds,
        )
        master_sha256 = _sha256_file(output_path)
        contact_sha256 = _sha256_file(contact_path)
        metadata = {
            "input_fingerprint": input_fingerprint,
            "template_id": template["template_id"],
            "template_version": template["version"],
            "qa_passed": True,
        }
        master = _store_asset(
            output_path,
            mime_type="video/mp4",
            extension=".mp4",
            parent_asset_id=input_asset_id,
            metadata={**metadata, **probe},
        )
        contact = _store_asset(
            contact_path,
            mime_type="image/jpeg",
            extension=".jpg",
            parent_asset_id=master["asset_id"],
            metadata={**metadata, "artifact_type": "contact_sheet"},
        )
        master_url, master_expires_at = generate_download_url(master["asset_id"], master["storage_key"])
        contact_url, contact_expires_at = generate_download_url(contact["asset_id"], contact["storage_key"])
        qa = {
            "pass": True,
            "checks": checks,
            "duration_sec": probe.get("duration_sec"),
            "width": probe.get("width"),
            "height": probe.get("height"),
            "fps": probe.get("fps"),
            "video_codec": probe.get("video_codec"),
            "pixel_format": probe.get("pixel_format"),
            "audio_codec": probe.get("audio_codec"),
            **loudness,
            "master_sha256": master_sha256,
            "contact_sheet_sha256": contact_sha256,
            "input_sha256": input_hashes,
            "failed_checks": [],
            "failed_checks_codes": [],
        }
        result = {
            "input_fingerprint": input_fingerprint,
            "template_id": template["template_id"],
            "template_version": template["version"],
            "master": {
                "asset_id": master["asset_id"],
                "sha256": master_sha256,
                "download_url": master_url,
                "expires_at": master_expires_at,
            },
            "contact_sheet": {
                "asset_id": contact["asset_id"],
                "sha256": contact_sha256,
                "download_url": contact_url,
                "expires_at": contact_expires_at,
            },
            "qa": qa,
        }
        _finish(
            job_id,
            "success",
            progress=100,
            output_asset_ids=[master["asset_id"], contact["asset_id"]],
            result=result,
            qa=qa,
            logs_short=render_logs,
            finished_at=utc_now_iso(),
        )
        return result
    except (FfmpegError, ReelRenderError, ValueError, RuntimeError) as exc:
        _finish(
            job_id,
            "error",
            error=str(exc),
            logs_short=None,
            finished_at=utc_now_iso(),
        )
        raise
    finally:
        for path in temporary_paths:
            if os.path.exists(path):
                os.remove(path)
        for path in text_paths:
            if os.path.exists(path):
                os.remove(path)
        if font_cleanup and font_path and os.path.exists(font_path):
            os.remove(font_path)
        for path in (premaster_path, output_path, contact_path):
            if os.path.exists(path):
                os.remove(path)
