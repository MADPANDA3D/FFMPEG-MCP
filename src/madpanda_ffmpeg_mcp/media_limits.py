"""Central media-complexity policy for untrusted FFmpeg inputs and outputs."""

import math
import os
import re
import stat
from typing import Any, Literal

from .config import settings


class MediaLimitError(ValueError):
    """Raised when media exceeds a configured structural or decoded-work limit."""


MediaKind = Literal["audio", "image", "video"]

_SCALE_RE = re.compile(r"(?:^|,)scale=(?P<width>-?\d+):(?P<height>-?\d+)(?::|,|$)")
_FILTER_FPS_RE = re.compile(r"(?:^|,)fps=(?P<fps>\d+(?:\.\d+)?)(?::|,|$)")


def _optional_positive_float(value: Any, field: str) -> float | None:
    if value is None or value == "" or value == "N/A" or value == "0/0":
        return None
    if isinstance(value, bool):
        raise MediaLimitError(f"{field} is invalid")
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise MediaLimitError(f"{field} is invalid") from None
    if not math.isfinite(number) or number < 0:
        raise MediaLimitError(f"{field} is invalid")
    if number == 0:
        return None
    return number


def _optional_positive_int(
    value: Any,
    field: str,
    *,
    zero_is_none: bool = False,
) -> int | None:
    if value is None or value == "" or value == "N/A":
        return None
    if isinstance(value, bool):
        raise MediaLimitError(f"{field} is invalid")
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise MediaLimitError(f"{field} is invalid") from None
    if str(value).strip() not in {str(number), f"+{number}"} and not isinstance(value, int):
        raise MediaLimitError(f"{field} is invalid")
    if number == 0 and zero_is_none:
        return None
    if number <= 0:
        raise MediaLimitError(f"{field} is invalid")
    return number


def _parse_rate(value: Any) -> float | None:
    if value is None or value == "" or value == "N/A" or value == "0/0":
        return None
    if isinstance(value, bool):
        raise MediaLimitError("video frame rate is invalid")
    if isinstance(value, (int, float)):
        fps = float(value)
    elif isinstance(value, str) and "/" in value:
        numerator, denominator = value.split("/", 1)
        try:
            fps = float(numerator) / float(denominator)
        except (ValueError, ZeroDivisionError):
            raise MediaLimitError("video frame rate is invalid") from None
    elif isinstance(value, str):
        try:
            fps = float(value)
        except ValueError:
            raise MediaLimitError("video frame rate is invalid") from None
    else:
        raise MediaLimitError("video frame rate is invalid")
    if not math.isfinite(fps) or fps <= 0:
        raise MediaLimitError("video frame rate is invalid")
    return fps


def _stream_fps(
    stream: dict[str, Any],
    *,
    duration: float | None,
    frame_count: int | None,
) -> float | None:
    rates: list[float] = []
    for field in ("avg_frame_rate", "r_frame_rate"):
        rate = _parse_rate(stream.get(field))
        if rate is not None:
            rates.append(rate)
    if duration is not None and frame_count is not None:
        rates.append(frame_count / duration)
    if rates:
        return max(rates)
    if duration is not None:
        return float(settings.max_video_fps)
    return None


def _is_attached_picture(stream: dict[str, Any]) -> bool:
    disposition = stream.get("disposition")
    if not isinstance(disposition, dict):
        return False
    value = disposition.get("attached_pic")
    return value in {1, "1"}


def selected_video_stream(probe: dict[str, Any]) -> dict[str, Any]:
    """Return the exact non-cover-art video stream selected by worker policy."""

    streams = probe.get("streams") if isinstance(probe, dict) else None
    if not isinstance(streams, list):
        raise MediaLimitError("media stream metadata is invalid")
    for stream in streams:
        if (
            isinstance(stream, dict)
            and stream.get("codec_type") == "video"
            and not _is_attached_picture(stream)
        ):
            return stream
    raise MediaLimitError("media does not contain a selectable video stream")


def selected_video_stream_index(probe: dict[str, Any]) -> int:
    stream = selected_video_stream(probe)
    index = stream.get("index")
    if isinstance(index, bool) or not isinstance(index, int) or index < 0:
        raise MediaLimitError("selected video stream index is invalid")
    return index


def selected_audio_stream_index(probe: dict[str, Any]) -> int:
    streams = probe.get("streams") if isinstance(probe, dict) else None
    if not isinstance(streams, list):
        raise MediaLimitError("media stream metadata is invalid")
    for stream in streams:
        if isinstance(stream, dict) and stream.get("codec_type") == "audio":
            index = stream.get("index")
            if isinstance(index, bool) or not isinstance(index, int) or index < 0:
                raise MediaLimitError("selected audio stream index is invalid")
            return index
    raise MediaLimitError("media does not contain a selectable audio stream")


def selected_video_duration(probe: dict[str, Any]) -> float:
    stream = selected_video_stream(probe)
    duration = _optional_positive_float(stream.get("duration"), "video stream duration")
    if duration is None:
        duration = _optional_positive_float(probe.get("duration_sec"), "media duration")
    if duration is None:
        raise MediaLimitError("selected video stream duration is unavailable")
    return duration


def validate_concat_probe_plan(
    probes: list[dict[str, Any]],
    *,
    target_width: Any | None,
    target_height: Any | None,
    transition: str,
    transition_duration: float,
) -> tuple[int, int, float]:
    """Bound the exact selected streams and aggregate concat output work."""

    if len(probes) < 2 or len(probes) > settings.max_concat_clips:
        raise MediaLimitError("concat input count exceeds the configured limit")
    streams = [selected_video_stream(probe) for probe in probes]
    durations = [selected_video_duration(probe) for probe in probes]
    source_duration = sum(durations)
    if source_duration > settings.max_duration_seconds:
        raise MediaLimitError("concat aggregate source duration exceeds the configured limit")
    if transition == "crossfade":
        if transition_duration <= 0 or any(
            duration <= transition_duration for duration in durations
        ):
            raise MediaLimitError("concat transition duration is invalid")
        output_duration = sum(durations) - transition_duration * (len(durations) - 1)
    elif transition == "none":
        output_duration = sum(durations)
    else:
        raise MediaLimitError("concat transition is unsupported")
    if output_duration > settings.max_duration_seconds:
        raise MediaLimitError("concat output duration exceeds the configured limit")

    width, height = validate_geometry(
        target_width if target_width is not None else streams[0].get("width"),
        target_height if target_height is not None else streams[0].get("height"),
        field="concat output",
    )
    fps_values: list[float] = []
    aggregate_scaled_frames = 0
    aggregate_source_work = 0
    for probe, stream, duration in zip(probes, streams, durations, strict=True):
        fps = _stream_fps(
            stream,
            duration=duration,
            frame_count=_optional_positive_int(
                stream.get("nb_frames"), "video frame count", zero_is_none=True
            ),
        )
        if fps is None:
            fps = _optional_positive_float(probe.get("fps"), "video frame rate")
        if fps is not None:
            fps_values.append(fps)
        resolved_fps = fps if fps is not None else float(settings.max_video_fps)
        frame_count = max(1, math.ceil(duration * resolved_fps))
        aggregate_scaled_frames += frame_count
        source_width, source_height = validate_geometry(
            stream.get("width"), stream.get("height"), field="concat source"
        )
        aggregate_source_work += source_width * source_height * frame_count
    if aggregate_source_work > settings.max_decoded_video_pixel_frames:
        raise MediaLimitError("concat aggregate source decode work exceeds the configured limit")
    if width * height * aggregate_scaled_frames > settings.max_decoded_video_pixel_frames:
        raise MediaLimitError("concat aggregate scaled work exceeds the configured limit")
    validate_planned_video_work(
        width,
        height,
        fps=max(fps_values) if fps_values else settings.max_video_fps,
        duration=output_duration,
        field="concat output",
    )
    return width, height, output_duration


def validate_geometry(width: Any, height: Any, *, field: str = "frame") -> tuple[int, int]:
    resolved_width = _optional_positive_int(width, f"{field} width")
    resolved_height = _optional_positive_int(height, f"{field} height")
    if resolved_width is None or resolved_height is None:
        raise MediaLimitError(f"{field} geometry is unavailable")
    if resolved_width > settings.max_frame_width:
        raise MediaLimitError(f"{field} width exceeds the configured limit")
    if resolved_height > settings.max_frame_height:
        raise MediaLimitError(f"{field} height exceeds the configured limit")
    if resolved_width * resolved_height > settings.max_frame_pixels:
        raise MediaLimitError(f"{field} pixel count exceeds the configured limit")
    return resolved_width, resolved_height


def validate_media_probe(
    probe: dict[str, Any],
    *,
    expected_kind: MediaKind | None = None,
) -> dict[str, Any]:
    """Validate every stream plus aggregate decoded work without trusting metadata."""

    if not isinstance(probe, dict):
        raise MediaLimitError("media metadata is invalid")
    streams = probe.get("streams")
    if not isinstance(streams, list) or not streams:
        raise MediaLimitError("media contains no supported streams")
    if len(streams) > settings.max_media_streams:
        raise MediaLimitError("media stream count exceeds the configured limit")
    if not all(isinstance(stream, dict) for stream in streams):
        raise MediaLimitError("media stream metadata is invalid")

    duration = _optional_positive_float(probe.get("duration_sec"), "media duration")
    if duration is not None and duration > settings.max_duration_seconds:
        raise MediaLimitError("media duration exceeds the configured limit")
    stream_durations: list[float | None] = []
    for stream in streams:
        stream_duration = _optional_positive_float(stream.get("duration"), "stream duration")
        if stream_duration is not None and stream_duration > settings.max_duration_seconds:
            raise MediaLimitError("stream duration exceeds the configured limit")
        stream_durations.append(stream_duration)
    known_durations = [value for value in [duration, *stream_durations] if value is not None]
    resolved_duration = max(known_durations) if known_durations else None

    video_streams = 0
    audio_streams = 0
    decoded_video_work = 0.0
    decoded_audio_work = 0.0
    stream_indexes: set[int] = set()
    for index, stream in enumerate(streams):
        stream_index = stream.get("index")
        if (
            isinstance(stream_index, bool)
            or not isinstance(stream_index, int)
            or stream_index < 0
            or stream_index in stream_indexes
        ):
            raise MediaLimitError("media stream index is invalid")
        stream_indexes.add(stream_index)
        stream_type = stream.get("codec_type")
        stream_duration = stream_durations[index]
        effective_duration = stream_duration or resolved_duration

        if stream_type == "video":
            width, height = validate_geometry(
                stream.get("width"), stream.get("height"), field="video frame"
            )
            if _is_attached_picture(stream):
                decoded_video_work += width * height
                continue
            frame_count = _optional_positive_int(
                stream.get("nb_frames"), "video frame count", zero_is_none=True
            )
            fps = _stream_fps(
                stream,
                duration=effective_duration,
                frame_count=frame_count,
            )
            if fps is not None and fps > settings.max_video_fps:
                raise MediaLimitError("video frame rate exceeds the configured limit")
            video_streams += 1
            if frame_count is None:
                if effective_duration is None:
                    frame_count = 1 if expected_kind == "image" else None
                elif fps is not None:
                    frame_count = max(1, math.ceil(effective_duration * fps))
            if frame_count is None:
                raise MediaLimitError("video decoded-work estimate is unavailable")
            decoded_video_work += width * height * frame_count

        elif stream_type == "audio":
            audio_streams += 1
            channels = _optional_positive_int(stream.get("channels"), "audio channel count")
            sample_rate = _optional_positive_int(stream.get("sample_rate"), "audio sample rate")
            if channels is None or sample_rate is None:
                raise MediaLimitError("audio stream metadata is unavailable")
            if channels > settings.max_audio_channels:
                raise MediaLimitError("audio channel count exceeds the configured limit")
            if sample_rate > settings.max_audio_sample_rate:
                raise MediaLimitError("audio sample rate exceeds the configured limit")
            if effective_duration is None:
                raise MediaLimitError("audio decoded-work estimate is unavailable")
            decoded_audio_work += sample_rate * channels * effective_duration

    if expected_kind == "video" and video_streams == 0:
        raise MediaLimitError("media does not contain a video stream")
    if expected_kind == "audio" and audio_streams == 0:
        raise MediaLimitError("media does not contain an audio stream")
    if expected_kind == "image" and video_streams == 0:
        raise MediaLimitError("media does not contain an image stream")
    if video_streams == 0 and audio_streams == 0:
        raise MediaLimitError("media contains no supported streams")
    if expected_kind in {"audio", "video"} and resolved_duration is None:
        raise MediaLimitError("media duration is unavailable")
    if decoded_video_work > settings.max_decoded_video_pixel_frames:
        raise MediaLimitError("video decoded work exceeds the configured limit")
    if decoded_audio_work > settings.max_decoded_audio_sample_channels:
        raise MediaLimitError("audio decoded work exceeds the configured limit")
    return probe


def kind_from_mime_type(mime_type: str | None) -> MediaKind | None:
    normalized = (mime_type or "").lower()
    if normalized.startswith("video/"):
        return "video"
    if normalized.startswith("audio/"):
        return "audio"
    if normalized.startswith("image/"):
        return "image"
    return None


def validate_media_file(
    path: str,
    *,
    expected_kind: MediaKind | None = None,
    byte_limit: int | None = None,
) -> dict[str, Any]:
    """Re-open and re-probe a regular local file at its execution boundary."""

    try:
        file_stat = os.lstat(path)
    except (OSError, TypeError) as exc:
        raise MediaLimitError("media file is unavailable") from exc
    if stat.S_ISLNK(file_stat.st_mode) or not stat.S_ISREG(file_stat.st_mode):
        raise MediaLimitError("media file must be a regular non-symlink file")
    if file_stat.st_size <= 0:
        raise MediaLimitError("media file is empty")
    if byte_limit is not None and file_stat.st_size > byte_limit:
        raise MediaLimitError("media file exceeds the configured byte limit")

    from .ffprobe_utils import run_ffprobe

    try:
        probe = run_ffprobe(path)
        return validate_media_probe(probe, expected_kind=expected_kind)
    except MediaLimitError:
        raise
    except Exception as exc:
        raise MediaLimitError("media could not be probed safely") from exc


def _even_dimension(value: float) -> int:
    rounded = max(2, int(round(value)))
    return rounded if rounded % 2 == 0 else rounded + 1


def validate_planned_video_work(
    width: Any,
    height: Any,
    *,
    fps: Any | None,
    duration: Any | None,
    field: str = "planned output",
) -> tuple[int, int]:
    resolved_width, resolved_height = validate_geometry(width, height, field=field)
    resolved_duration = _optional_positive_float(duration, f"{field} duration")
    resolved_fps = _optional_positive_float(fps, f"{field} frame rate")
    if resolved_fps is None:
        resolved_fps = float(settings.max_video_fps) if resolved_duration is not None else 1.0
    if resolved_fps > settings.max_video_fps:
        raise MediaLimitError(f"{field} frame rate exceeds the configured limit")
    frames = 1 if resolved_duration is None else max(1, math.ceil(resolved_duration * resolved_fps))
    if resolved_width * resolved_height * frames > settings.max_decoded_video_pixel_frames:
        raise MediaLimitError(f"{field} decoded work exceeds the configured limit")
    return resolved_width, resolved_height


def validate_thumbnail_geometry(probe: dict[str, Any], width: int | None) -> tuple[int, int]:
    streams = probe.get("streams") if isinstance(probe, dict) else None
    if not isinstance(streams, list):
        raise MediaLimitError("media stream metadata is invalid")
    results: list[tuple[int, int]] = []
    for stream in streams:
        if not isinstance(stream, dict) or stream.get("codec_type") != "video":
            continue
        if _is_attached_picture(stream):
            continue
        source_width, source_height = validate_geometry(
            stream.get("width"), stream.get("height"), field="source frame"
        )
        if width is None:
            result = validate_planned_video_work(
                source_width,
                source_height,
                fps=1,
                duration=None,
                field="thumbnail frame",
            )
        else:
            target_width = _optional_positive_int(width, "thumbnail width")
            if target_width is None:
                raise MediaLimitError("thumbnail width is invalid")
            target_height = _even_dimension(target_width * source_height / source_width)
            result = validate_planned_video_work(
                target_width,
                target_height,
                fps=1,
                duration=None,
                field="thumbnail frame",
            )
        results.append(result)
    if not results:
        raise MediaLimitError("media does not contain a selectable video stream")
    return results[0]


def validate_preset_geometry(
    probe: dict[str, Any],
    preset: dict[str, Any],
) -> tuple[int, int] | None:
    """Validate fixed and aspect-derived preset geometry before FFmpeg allocates it."""

    if not isinstance(preset, dict):
        raise MediaLimitError("preset definition is invalid")
    if str(preset.get("mime_type") or "").lower().startswith("audio/"):
        return None
    filter_values = preset.get("ffmpeg_args")
    if not isinstance(filter_values, list):
        raise MediaLimitError("preset definition is invalid")
    filter_text = next(
        (value for value in filter_values if isinstance(value, str) and "scale=" in value),
        None,
    )

    def validate_result(
        width: int,
        height: int,
        stream: dict[str, Any],
    ) -> tuple[int, int]:
        preset_fps = None
        for value in filter_values:
            if not isinstance(value, str):
                continue
            fps_match = _FILTER_FPS_RE.search(value)
            if fps_match:
                preset_fps = float(fps_match.group("fps"))
                break
        return validate_planned_video_work(
            width,
            height,
            fps=(
                preset_fps
                if preset_fps is not None
                else _stream_fps(
                    stream,
                    duration=_optional_positive_float(
                        stream.get("duration"), "video stream duration"
                    )
                    or _optional_positive_float(probe.get("duration_sec"), "media duration"),
                    frame_count=_optional_positive_int(
                        stream.get("nb_frames"), "video frame count", zero_is_none=True
                    ),
                )
            ),
            duration=_optional_positive_float(stream.get("duration"), "video stream duration")
            or _optional_positive_float(probe.get("duration_sec"), "media duration"),
            field="preset frame",
        )

    streams = probe.get("streams") if isinstance(probe, dict) else None
    if not isinstance(streams, list):
        raise MediaLimitError("media stream metadata is invalid")
    results: list[tuple[int, int]] = []
    for stream in streams:
        if not isinstance(stream, dict) or stream.get("codec_type") != "video":
            continue
        if _is_attached_picture(stream):
            continue
        source_width, source_height = validate_geometry(
            stream.get("width"), stream.get("height"), field="source frame"
        )
        if filter_text is None:
            results.append(validate_result(source_width, source_height, stream))
            continue
        match = _SCALE_RE.search(filter_text)
        if match is None:
            raise MediaLimitError("preset scale geometry is invalid")
        target_width = int(match.group("width"))
        target_height = int(match.group("height"))
        if target_width > 0 and target_height > 0:
            width, height = validate_geometry(target_width, target_height, field="preset frame")
        elif target_width in {-1, -2} and target_height > 0:
            width, height = validate_geometry(
                _even_dimension(target_height * source_width / source_height),
                target_height,
                field="preset frame",
            )
        elif target_height in {-1, -2} and target_width > 0:
            width, height = validate_geometry(
                target_width,
                _even_dimension(target_width * source_height / source_width),
                field="preset frame",
            )
        else:
            raise MediaLimitError("preset scale geometry is invalid")
        results.append(validate_result(width, height, stream))
    if not results:
        raise MediaLimitError("media does not contain a selectable video stream")
    return results[0]
