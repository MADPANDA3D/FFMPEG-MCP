import json
import subprocess
from typing import Any

from config import settings


def _parse_fps(value: str | None) -> float | None:
    if not value or value == "0/0":
        return None
    if "/" in value:
        num, den = value.split("/", 1)
        try:
            return float(num) / float(den)
        except (ValueError, ZeroDivisionError):
            return None
    try:
        return float(value)
    except ValueError:
        return None


def run_ffprobe(path: str) -> dict[str, Any]:
    cmd = [
        settings.ffprobe_bin,
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "ffprobe failed")
    data = json.loads(result.stdout)
    streams = data.get("streams", []) or []
    fmt = data.get("format", {}) or {}

    duration_sec = None
    if "duration" in fmt:
        try:
            duration_sec = float(fmt["duration"])
        except (TypeError, ValueError):
            duration_sec = None

    video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
    audio_stream = next((s for s in streams if s.get("codec_type") == "audio"), None)

    width = None
    height = None
    fps = None
    video_codec = None
    if video_stream:
        width = video_stream.get("width")
        height = video_stream.get("height")
        fps = _parse_fps(video_stream.get("avg_frame_rate"))
        video_codec = video_stream.get("codec_name")

    audio_codec = None
    if audio_stream:
        audio_codec = audio_stream.get("codec_name")

    normalized_streams = []
    for stream in streams:
        normalized_streams.append(
            {
                "index": stream.get("index"),
                "codec_type": stream.get("codec_type"),
                "codec_name": stream.get("codec_name"),
                "width": stream.get("width"),
                "height": stream.get("height"),
                "duration": stream.get("duration"),
                "bit_rate": stream.get("bit_rate"),
                "avg_frame_rate": stream.get("avg_frame_rate"),
                "sample_rate": stream.get("sample_rate"),
                "channels": stream.get("channels"),
            }
        )

    return {
        "duration_sec": duration_sec,
        "width": width,
        "height": height,
        "fps": fps,
        "video_codec": video_codec,
        "audio_codec": audio_codec,
        "streams": normalized_streams,
    }
