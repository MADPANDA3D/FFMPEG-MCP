import json
import math
import os
import re
from typing import Any

from .config import settings
from .media_process import (
    CapturedOutputLimitError,
    ProcessSafetyError,
    run_bounded_process,
    sanitized_media_environment,
)

_LOCAL_PROTOCOLS = "file"
_MAX_FFPROBE_TIMEOUT_SECONDS = 300
_MAX_FFPROBE_JSON_BYTES = 1024 * 1024
_STDERR_TAIL_BYTES = 2048
_URI_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


def _parse_fps(value: Any) -> float | None:
    if not value:
        return None
    if not isinstance(value, str):
        raise RuntimeError("ffprobe returned invalid media metadata")
    if value in {"0/0", "N/A"}:
        return None
    if "/" in value:
        num, den = value.split("/", 1)
        try:
            result = float(num) / float(den)
        except (ValueError, ZeroDivisionError):
            return None
    else:
        try:
            result = float(value)
        except ValueError:
            return None
    if not math.isfinite(result) or result <= 0:
        raise RuntimeError("ffprobe returned invalid media metadata")
    return result


def _parse_optional_number(value: Any, field: str, *, integer: bool) -> int | float | None:
    if value is None or value == "" or value == "N/A":
        return None
    if isinstance(value, bool):
        raise RuntimeError("ffprobe returned invalid media metadata")
    try:
        result = int(value) if integer else float(value)
    except (TypeError, ValueError):
        raise RuntimeError("ffprobe returned invalid media metadata") from None
    if integer and not isinstance(value, int):
        normalized = str(value).strip()
        if normalized not in {str(result), f"+{result}"}:
            raise RuntimeError("ffprobe returned invalid media metadata")
    if not math.isfinite(float(result)) or result < 0:
        raise RuntimeError("ffprobe returned invalid media metadata")
    del field
    return result


def _safe_metadata_string(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", value):
        raise RuntimeError("ffprobe returned invalid media metadata")
    return value


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def run_ffprobe(path: str) -> dict[str, Any]:
    if (
        not isinstance(path, str)
        or not path
        or "\x00" in path
        or path.startswith("-")
        or _URI_SCHEME_RE.match(path)
        or os.path.islink(path)
        or not os.path.isfile(path)
    ):
        raise RuntimeError("ffprobe input must be a local regular non-symlink file")
    cmd = [
        settings.ffprobe_bin,
        "-hide_banner",
        "-protocol_whitelist",
        _LOCAL_PROTOCOLS,
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_entries",
        (
            "format=duration:"
            "stream=index,codec_type,codec_name,width,height,duration,bit_rate,"
            "avg_frame_rate,r_frame_rate,nb_frames,sample_rate,channels:"
            "stream_disposition=attached_pic"
        ),
        path,
    ]
    timeout = min(
        max(int(settings.ffprobe_timeout_seconds), 1),
        _MAX_FFPROBE_TIMEOUT_SECONDS,
    )
    try:
        returncode, stdout, _, _ = run_bounded_process(
            cmd,
            timeout_seconds=timeout,
            capture_stdout=True,
            stdout_limit_bytes=_MAX_FFPROBE_JSON_BYTES,
            stderr_tail_bytes=_STDERR_TAIL_BYTES,
            address_space_limit_bytes=int(
                getattr(settings, "ffprobe_rlimit_as_bytes", 536_870_912)
            ),
            cpu_limit_seconds=int(getattr(settings, "ffprobe_rlimit_cpu_seconds", 60)),
            nofile_limit=int(getattr(settings, "media_rlimit_nofile", 256)),
            disable_core_dumps=True,
            environment=sanitized_media_environment(
                getattr(settings, "storage_temp_dir", os.path.dirname(path) or "/tmp")
            ),
        )
    except TimeoutError as exc:
        raise RuntimeError("ffprobe timed out") from exc
    except CapturedOutputLimitError as exc:
        raise RuntimeError("ffprobe output exceeded the safety limit") from exc
    except (OSError, ProcessSafetyError) as exc:
        raise RuntimeError("ffprobe could not start safely") from exc
    if returncode != 0:
        raise RuntimeError("ffprobe failed")
    try:
        data = json.loads(
            stdout.decode("utf-8"),
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise RuntimeError("ffprobe returned invalid output") from exc
    if not isinstance(data, dict):
        raise RuntimeError("ffprobe returned invalid output")
    streams = data.get("streams", []) or []
    fmt = data.get("format", {}) or {}
    if (
        not isinstance(streams, list)
        or not all(isinstance(stream, dict) for stream in streams)
        or not isinstance(fmt, dict)
    ):
        raise RuntimeError("ffprobe returned invalid output")

    duration_sec = None
    raw_duration = fmt.get("duration")
    if raw_duration is not None and raw_duration != "N/A":
        try:
            duration_sec = float(raw_duration)
        except (TypeError, ValueError):
            raise RuntimeError("ffprobe returned invalid media metadata") from None
        if not math.isfinite(duration_sec) or duration_sec < 0:
            raise RuntimeError("ffprobe returned invalid media metadata")
        if duration_sec == 0:
            duration_sec = None

    normalized_streams: list[dict[str, Any]] = []
    for stream in streams:
        stream_duration = _parse_optional_number(stream.get("duration"), "duration", integer=False)
        if stream_duration == 0:
            stream_duration = None
        avg_fps = _parse_fps(stream.get("avg_frame_rate"))
        real_fps = _parse_fps(stream.get("r_frame_rate"))
        normalized_streams.append(
            {
                "index": _parse_optional_number(stream.get("index"), "index", integer=True),
                "codec_type": _safe_metadata_string(stream.get("codec_type")),
                "codec_name": _safe_metadata_string(stream.get("codec_name")),
                "width": _parse_optional_number(stream.get("width"), "width", integer=True),
                "height": _parse_optional_number(stream.get("height"), "height", integer=True),
                "duration": stream_duration,
                "bit_rate": _parse_optional_number(
                    stream.get("bit_rate"), "bit_rate", integer=True
                ),
                "avg_frame_rate": avg_fps,
                "r_frame_rate": real_fps,
                "nb_frames": _parse_optional_number(
                    stream.get("nb_frames"), "nb_frames", integer=True
                ),
                "sample_rate": _parse_optional_number(
                    stream.get("sample_rate"), "sample_rate", integer=True
                ),
                "channels": _parse_optional_number(
                    stream.get("channels"), "channels", integer=True
                ),
                "disposition": {
                    "attached_pic": int(
                        stream.get("disposition", {}).get("attached_pic", 0) in {1, "1"}
                        if isinstance(stream.get("disposition"), dict)
                        else False
                    )
                },
            }
        )

    video_stream = next(
        (
            s
            for s in normalized_streams
            if s.get("codec_type") == "video"
            and not bool((s.get("disposition") or {}).get("attached_pic"))
        ),
        None,
    )
    audio_stream = next((s for s in normalized_streams if s.get("codec_type") == "audio"), None)

    width = None
    height = None
    fps = None
    video_codec = None
    if video_stream:
        width = video_stream.get("width")
        height = video_stream.get("height")
        fps_values = [
            value
            for value in (
                video_stream.get("avg_frame_rate"),
                video_stream.get("r_frame_rate"),
            )
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        ]
        nb_frames_value = video_stream.get("nb_frames")
        stream_duration_value = video_stream.get("duration")
        stream_duration = (
            float(stream_duration_value)
            if isinstance(stream_duration_value, (int, float))
            and not isinstance(stream_duration_value, bool)
            and stream_duration_value > 0
            else duration_sec
        )
        if (
            isinstance(nb_frames_value, (int, float))
            and not isinstance(nb_frames_value, bool)
            and nb_frames_value > 0
            and stream_duration
        ):
            fps_values.append(float(nb_frames_value) / stream_duration)
        fps = max(fps_values) if fps_values else None
        video_codec = video_stream.get("codec_name")

    audio_codec = None
    if audio_stream:
        audio_codec = audio_stream.get("codec_name")

    return {
        "duration_sec": duration_sec,
        "width": width,
        "height": height,
        "fps": fps,
        "video_stream_index": video_stream.get("index") if video_stream else None,
        "audio_stream_index": audio_stream.get("index") if audio_stream else None,
        "video_codec": video_codec,
        "audio_codec": audio_codec,
        "streams": normalized_streams,
    }
