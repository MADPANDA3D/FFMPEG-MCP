"""Finite, bounded pre-enqueue validation for public MCP rendering requests."""

import math
from typing import Any

from .captions import parse_srt, parse_vtt
from .config import settings
from .media_limits import (
    MediaLimitError,
    validate_geometry,
    validate_planned_video_work,
    validate_preset_geometry,
    validate_thumbnail_geometry,
)
from .overlay_utils import LOGO_POSITIONS, TEXT_POSITIONS, sanitize_color
from .presets import get_preset
from .templates import get_template, validate_template_variables

_ANALYSIS_PASSES_PER_ASSET = 4
_MAX_VALIDATION_DEPTH = 8
_MAX_WORD_CHARS = 256
_MAX_WORKFLOW_ID_CHARS = 64
_MAX_WORKFLOW_STRING_CHARS = 512
_CAPTION_POSITIONS = {"bottom_safe", "mid", "top"}
_HIGHLIGHT_MODES = {"word"}
_ITERATE_STRATEGIES = {"audio_first", "balanced", "captions_first", "framing_first"}

_WORKFLOW_TOP_LEVEL_KEYS = {"nodes", "outputs"}
_WORKFLOW_NODE_KEYS = {"id", "type", "input", "inputs", "params"}
_WORKFLOW_PARAM_KEYS: dict[str, set[str]] = {
    "transcode": {"preset"},
    "trim": {"start_sec", "end_sec", "reencode"},
    "video_add_text": {
        "text",
        "position",
        "font_size",
        "font_color",
        "background_box",
        "box_color",
        "box_border_width",
        "font_name",
        "font_asset_id",
    },
    "video_add_logo": {
        "logo_asset_id",
        "logo_key",
        "position",
        "scale_pct",
        "opacity",
    },
    "video_concat": {
        "transition",
        "transition_duration",
        "target_width",
        "target_height",
        "include_audio",
    },
    "image_to_video": {
        "duration_sec",
        "width",
        "height",
        "fps",
        "background_color",
    },
    "images_to_slideshow": {
        "duration_per_image",
        "durations",
        "width",
        "height",
        "fps",
        "background_color",
    },
    "images_to_slideshow_ken_burns": {
        "duration_per_image",
        "durations",
        "width",
        "height",
        "fps",
        "background_color",
    },
    "audio_normalize": {"output_format", "target_lufs", "lra", "true_peak", "bitrate"},
    "audio_mix": {
        "output_format",
        "volumes",
        "normalize",
        "duration_mode",
        "bitrate",
    },
    "audio_duck": {
        "voice_asset_id",
        "music_asset_id",
        "output_format",
        "ratio",
        "threshold",
        "attack_ms",
        "release_ms",
        "music_gain",
        "bitrate",
    },
    "audio_mix_with_background": {
        "voice_asset_id",
        "music_asset_id",
        "output_format",
        "ducking",
        "ratio",
        "threshold",
        "attack_ms",
        "release_ms",
        "music_gain",
        "voice_gain",
        "bitrate",
    },
    "audio_fade": {
        "output_format",
        "fade_in_sec",
        "fade_out_sec",
        "fade_out_start",
        "bitrate",
    },
    "audio_trim_silence": {
        "output_format",
        "min_silence_sec",
        "threshold_db",
        "trim_leading",
        "trim_trailing",
        "bitrate",
    },
    "template_apply": {"template_name", "variables", "brand_kit_id", "quality"},
    "brand_kit_apply": {"brand_kit_id", "text", "position"},
}
_WORKFLOW_SINGLE_INPUT_TYPES = {
    "transcode",
    "trim",
    "video_add_text",
    "video_add_logo",
    "image_to_video",
    "audio_normalize",
    "audio_fade",
    "audio_trim_silence",
    "template_apply",
    "brand_kit_apply",
}


def _bounded_string(value: Any, field: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ValueError(f"{field} must be a bounded non-empty string")
    return value


def _validate_nested_string_bounds(value: Any, field: str) -> None:
    if isinstance(value, str):
        _bounded_string(value, field, maximum=_MAX_WORKFLOW_STRING_CHARS)
    elif isinstance(value, dict):
        for key, item in value.items():
            _validate_nested_string_bounds(item, f"{field}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _validate_nested_string_bounds(item, f"{field}[{index}]")


def finite_number(
    value: Any,
    field: str,
    *,
    minimum: float,
    maximum: float,
    integer: bool = False,
    allow_none: bool = True,
) -> int | float | None:
    if value is None and allow_none:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a finite number")
    try:
        number = int(value) if integer else float(value)
    except (OverflowError, TypeError, ValueError):
        raise ValueError(f"{field} must be a finite number") from None
    if integer and not isinstance(value, int):
        normalized = str(value).strip()
        if normalized not in {str(number), f"+{number}"}:
            raise ValueError(f"{field} must be an integer")
    if not math.isfinite(float(number)):
        raise ValueError(f"{field} must be a finite number")
    if number < minimum or number > maximum:
        raise ValueError(f"{field} must be between {minimum:g} and {maximum:g}")
    return number


def validate_finite_tree(value: Any, field: str = "payload", *, depth: int = 0) -> None:
    if depth > _MAX_VALIDATION_DEPTH:
        raise ValueError(f"{field} nesting exceeds the configured limit")
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"{field} must contain only finite numbers")
    if isinstance(value, str) and len(value) > settings.request_body_max_bytes:
        raise ValueError(f"{field} string exceeds the configured limit")
    if isinstance(value, dict):
        if len(value) > settings.max_batch_operations:
            raise ValueError(f"{field} contains too many fields")
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{field} keys must be strings")
            validate_finite_tree(item, f"{field}.{key}", depth=depth + 1)
    elif isinstance(value, list):
        if len(value) > max(settings.max_caption_word_timings, settings.max_batch_operations):
            raise ValueError(f"{field} contains too many items")
        for index, item in enumerate(value):
            validate_finite_tree(item, f"{field}[{index}]", depth=depth + 1)


def validate_dimensions(width: Any, height: Any, *, field: str) -> None:
    if width is None and height is None:
        return
    if width is None or height is None:
        present = width if width is not None else height
        finite_number(
            present,
            field,
            minimum=1,
            maximum=max(settings.max_frame_width, settings.max_frame_height),
            integer=True,
            allow_none=False,
        )
        return
    try:
        validate_geometry(width, height, field=field)
    except MediaLimitError as exc:
        raise ValueError(f"{field} exceeds the configured limit") from exc


def validate_words(words: Any) -> None:
    if words is None:
        return
    if not isinstance(words, list):
        raise ValueError("words_json must be a list")
    if len(words) > settings.max_caption_word_timings:
        raise ValueError("words_json exceeds the configured item limit")
    for index, word in enumerate(words):
        if not isinstance(word, dict):
            raise ValueError("words_json entries must be objects")
        if set(word) != {"word", "start", "end"}:
            raise ValueError("words_json entries require only word, start, and end")
        text = word.get("word")
        if not isinstance(text, str) or not text.strip() or len(text) > _MAX_WORD_CHARS:
            raise ValueError("words_json word is invalid")
        validate_finite_tree(word, f"words_json[{index}]")
        finite_number(
            word.get("start"),
            f"words_json[{index}].start",
            minimum=0,
            maximum=settings.max_duration_seconds,
            allow_none=False,
        )
        finite_number(
            word.get("end"),
            f"words_json[{index}].end",
            minimum=0,
            maximum=settings.max_duration_seconds,
            allow_none=False,
        )
        if float(word["end"]) <= float(word["start"]):
            raise ValueError("words_json end must be greater than start")


def validate_caption_controls(values: dict[str, Any]) -> None:
    validate_words(values.get("words_json"))
    for name in ("font_size", "caption_font_size"):
        finite_number(
            values.get(name),
            name,
            minimum=settings.min_font_size,
            maximum=settings.max_font_size,
            integer=True,
        )
    for name in ("padding_px", "safe_zone_bottom_px", "safe_zone_top_px"):
        finite_number(
            values.get(name),
            name,
            minimum=0,
            maximum=max(settings.max_frame_width, settings.max_frame_height),
            integer=True,
        )
    for name in (
        "caption_padding_px",
        "caption_safe_zone_bottom_px",
        "caption_safe_zone_top_px",
    ):
        finite_number(
            values.get(name),
            name,
            minimum=0,
            maximum=max(settings.max_frame_width, settings.max_frame_height),
            integer=True,
        )
    for name, maximum in (
        ("max_chars", settings.caption_max_chars),
        ("caption_max_chars", settings.caption_max_chars),
        ("max_lines", settings.caption_max_lines),
        ("caption_max_lines", settings.caption_max_lines),
        ("max_words", settings.caption_max_words),
        ("caption_max_words", settings.caption_max_words),
    ):
        finite_number(values.get(name), name, minimum=1, maximum=maximum, integer=True)
    for name in ("box_opacity", "caption_box_opacity"):
        finite_number(values.get(name), name, minimum=0, maximum=1)


def validate_caption_sources(values: dict[str, Any]) -> None:
    validate_caption_controls(values)
    for name in ("position", "caption_position"):
        position = values.get(name)
        if position is not None and (
            not isinstance(position, str) or position.strip().lower() not in _CAPTION_POSITIONS
        ):
            raise ValueError(f"{name} is unsupported")
    highlight_mode = values.get("highlight_mode")
    if highlight_mode is not None and (
        not isinstance(highlight_mode, str)
        or highlight_mode.strip().lower() not in _HIGHLIGHT_MODES
    ):
        raise ValueError("highlight_mode is unsupported")
    for name, parser in (("captions_srt", parse_srt), ("captions_vtt", parse_vtt)):
        source = values.get(name)
        if source is None:
            continue
        if (
            not isinstance(source, str)
            or len(source.encode("utf-8")) > settings.request_body_max_bytes
        ):
            raise ValueError(f"{name} exceeds the configured limit")
        if len(parser(source)) > settings.max_caption_segments:
            raise ValueError(f"{name} exceeds the configured segment limit")


def validate_audio_controls(values: dict[str, Any]) -> None:
    bounds = {
        "target_lufs": (-30.0, -5.0),
        "audio_target_lufs": (-30.0, -5.0),
        "lra": (0.000_001, 20.0),
        "audio_lra": (0.000_001, 20.0),
        "true_peak": (-10.0, 0.0),
        "audio_true_peak": (-10.0, 0.0),
        "ratio": (1.0, 20.0),
        "ducking_ratio": (1.0, 20.0),
        "threshold": (0.000_001, 1.0),
        "ducking_threshold": (0.000_001, 1.0),
        "music_gain": (0.000_001, 4.0),
        "voice_gain": (0.000_001, 4.0),
        "fade_in_sec": (0.0, float(settings.max_duration_seconds)),
        "fade_out_sec": (0.0, float(settings.max_duration_seconds)),
        "fade_out_start": (0.0, float(settings.max_duration_seconds)),
        "min_silence_sec": (0.000_001, float(settings.max_duration_seconds)),
        "trim_silence_min_sec": (0.000_001, float(settings.max_duration_seconds)),
        "threshold_db": (-120.0, 0.0),
        "trim_silence_threshold_db": (-120.0, 0.0),
    }
    for name, (minimum, maximum) in bounds.items():
        finite_number(values.get(name), name, minimum=minimum, maximum=maximum)
    for name in ("attack_ms", "ducking_attack_ms"):
        finite_number(values.get(name), name, minimum=1, maximum=2_000, integer=True)
    for name in ("release_ms", "ducking_release_ms"):
        finite_number(values.get(name), name, minimum=1, maximum=5_000, integer=True)
    volumes = values.get("volumes")
    if volumes is not None:
        if not isinstance(volumes, list):
            raise ValueError("volumes must be a list")
        for index, volume in enumerate(volumes):
            finite_number(volume, f"volumes[{index}]", minimum=0, maximum=4, allow_none=False)
    output_format = values.get("output_format")
    if output_format is not None and str(output_format).lower() not in {"mp3", "wav", "m4a"}:
        raise ValueError("output_format is unsupported")
    duration_mode = values.get("duration_mode")
    if duration_mode is not None and str(duration_mode).lower() not in {
        "longest",
        "shortest",
        "first",
    }:
        raise ValueError("duration_mode is unsupported")


def validate_thumbnail_request(asset: dict[str, Any], time_sec: Any, width: Any) -> None:
    finite_number(
        time_sec,
        "time_sec",
        minimum=0,
        maximum=settings.max_duration_seconds,
        allow_none=False,
    )
    finite_number(
        width,
        "width",
        minimum=1,
        maximum=settings.max_frame_width,
        integer=True,
    )
    if asset.get("width") and asset.get("height"):
        try:
            validate_thumbnail_geometry(asset, width)
        except MediaLimitError as exc:
            raise ValueError("thumbnail geometry exceeds the configured limit") from exc


def validate_trim_request(start_sec: Any, end_sec: Any) -> None:
    start = finite_number(
        start_sec,
        "start_sec",
        minimum=0,
        maximum=settings.max_duration_seconds,
        allow_none=False,
    )
    end = finite_number(
        end_sec,
        "end_sec",
        minimum=0,
        maximum=settings.max_duration_seconds,
        allow_none=False,
    )
    assert start is not None and end is not None
    if float(end) <= float(start):
        raise ValueError("end_sec must be greater than start_sec")


def validate_preset_request(asset: dict[str, Any], preset_name: str) -> None:
    preset = get_preset(preset_name)
    if asset.get("width") and asset.get("height"):
        try:
            validate_preset_geometry(asset, preset)
        except MediaLimitError as exc:
            raise ValueError("preset output exceeds the configured limit") from exc


def validate_image_video_plan(
    *, duration: Any, width: Any, height: Any, fps: Any, field: str = "output"
) -> None:
    resolved_duration = finite_number(
        duration,
        "duration_sec",
        minimum=0.001,
        maximum=settings.max_duration_seconds,
        allow_none=False,
    )
    resolved_width = width if width is not None else settings.default_image_width
    resolved_height = height if height is not None else settings.default_image_height
    resolved_fps = fps if fps is not None else settings.default_video_fps
    finite_number(
        resolved_fps,
        "fps",
        minimum=1,
        maximum=settings.max_video_fps,
        integer=True,
        allow_none=False,
    )
    try:
        validate_planned_video_work(
            resolved_width,
            resolved_height,
            fps=resolved_fps,
            duration=resolved_duration,
            field=field,
        )
    except MediaLimitError as exc:
        raise ValueError(f"{field} exceeds the configured limit") from exc


def validate_slideshow_plan(
    count: int,
    duration_per_image: Any,
    durations: Any,
    width: Any,
    height: Any,
    fps: Any,
) -> None:
    if count < 1 or count > settings.max_slideshow_images:
        raise ValueError("slideshow image count exceeds the configured limit")
    if durations is not None:
        if not isinstance(durations, list) or len(durations) != count:
            raise ValueError("durations must contain one value per image")
        total = 0.0
        for index, value in enumerate(durations):
            duration = finite_number(
                value,
                f"durations[{index}]",
                minimum=0.001,
                maximum=settings.max_duration_seconds,
                allow_none=False,
            )
            assert duration is not None
            total += float(duration)
    else:
        per_image = (
            duration_per_image
            if duration_per_image is not None
            else settings.default_image_duration_sec
        )
        resolved = finite_number(
            per_image,
            "duration_per_image",
            minimum=0.001,
            maximum=settings.max_duration_seconds,
            allow_none=False,
        )
        assert resolved is not None
        total = float(resolved) * count
    if total > settings.max_duration_seconds:
        raise ValueError("slideshow duration exceeds the configured limit")
    validate_image_video_plan(
        duration=total,
        width=width,
        height=height,
        fps=fps,
        field="slideshow output",
    )


def validate_batch_operations(operation_count: int, field: str) -> None:
    if operation_count < 1 or operation_count > settings.max_batch_operations:
        raise ValueError(f"{field} exceeds the configured operation limit")


def validate_preset_list(presets: Any, *, field: str = "presets") -> list[str]:
    if not isinstance(presets, list) or not presets:
        raise ValueError(f"{field} must be a non-empty list")
    if len(presets) > settings.max_batch_presets:
        raise ValueError(f"{field} exceeds the configured preset limit")
    resolved: list[str] = []
    for value in presets:
        if not isinstance(value, str) or not value or len(value) > _MAX_WORKFLOW_STRING_CHARS:
            raise ValueError(f"{field} contains an invalid preset")
        get_preset(value)
        resolved.append(value)
    if len(resolved) != len(set(resolved)):
        raise ValueError(f"{field} must not contain duplicates")
    validate_batch_operations(len(resolved), field)
    return resolved


def validate_brand_kit_request(brand_kit: Any) -> None:
    if not isinstance(brand_kit, dict):
        raise ValueError("brand_kit must be an object")
    validate_finite_tree(brand_kit, "brand_kit")
    validate_caption_controls(brand_kit)
    finite_number(brand_kit.get("logo_opacity"), "logo_opacity", minimum=0, maximum=1)
    finite_number(
        brand_kit.get("logo_scale_pct"),
        "logo_scale_pct",
        minimum=settings.logo_min_scale_pct,
        maximum=settings.logo_max_scale_pct,
        integer=True,
    )
    default_preset = brand_kit.get("default_preset")
    if default_preset is not None:
        if not isinstance(default_preset, str) or not default_preset:
            raise ValueError("default_preset is invalid")
        get_preset(default_preset)


def template_operation_count(template_name: str) -> int:
    template = get_template(template_name)
    layers = template.get("layers")
    if not isinstance(layers, list) or not layers:
        raise ValueError("template layers are invalid")
    if len(layers) > settings.max_template_layers:
        raise ValueError("template exceeds the configured layer limit")
    text_layers = sum(
        1 for layer in layers if isinstance(layer, dict) and layer.get("type") == "text"
    )
    if text_layers > settings.max_template_text_layers:
        raise ValueError("template exceeds the configured text-layer limit")
    return len(layers) + int(bool(template.get("include_brand_logo")))


def validate_template_request(template_name: str, variables: Any) -> int:
    if variables is not None and not isinstance(variables, dict):
        raise ValueError("variables must be an object")
    validate_finite_tree(variables or {}, "variables")
    template = get_template(template_name)
    validate_template_variables(template, variables)
    requested_preset = (variables or {}).get("preset")
    if requested_preset is not None:
        if not isinstance(requested_preset, str) or not requested_preset:
            raise ValueError("template preset is invalid")
        get_preset(requested_preset)
    operations = template_operation_count(template_name)
    validate_batch_operations(operations, "template")
    return operations


def validate_template_asset_plan(asset: dict[str, Any], template_name: str, variables: Any) -> int:
    operations = validate_template_request(template_name, variables)
    template = get_template(template_name)
    merged = validate_template_variables(template, variables)
    for layer in template.get("layers") or []:
        if not isinstance(layer, dict) or layer.get("type") != "transcode":
            continue
        preset_name = layer.get("preset")
        if isinstance(preset_name, str):
            try:
                preset_name = preset_name.format(**merged)
            except (KeyError, ValueError):
                raise ValueError("template preset is invalid") from None
        if not isinstance(preset_name, str) or not preset_name:
            raise ValueError("template transcode preset is invalid")
        validate_preset_request(asset, preset_name)
    return operations


def validate_campaign_plan(
    asset_count: int,
    preset_count: int,
    template_name: str | None,
) -> None:
    per_asset = preset_count
    if template_name:
        per_asset += validate_template_request(template_name, {})
    validate_batch_operations(asset_count * max(per_asset, 1), "campaign")


def validate_analysis_plan(asset_count: int) -> None:
    validate_batch_operations(asset_count * _ANALYSIS_PASSES_PER_ASSET, "analysis")


def validate_concat_asset_plan(
    assets: list[dict[str, Any]],
    *,
    target_width: Any | None,
    target_height: Any | None,
    transition: str | None,
    transition_duration: Any | None,
    allow_renderable_images: bool = False,
) -> None:
    """Reject aggregate concat duration/work before a job reaches the queue."""

    if len(assets) < 2 or len(assets) > settings.max_concat_clips:
        raise ValueError("concat input count exceeds the configured limit")
    normalized_transition = (transition or "none").strip().lower()
    if normalized_transition not in {"none", "crossfade"}:
        raise ValueError("transition is unsupported")
    resolved_transition = finite_number(
        transition_duration if transition_duration is not None else 0,
        "transition_duration",
        minimum=0,
        maximum=settings.max_duration_seconds,
        allow_none=False,
    )
    assert resolved_transition is not None

    durations: list[float] = []
    fps_values: list[float] = []
    planned_assets: list[dict[str, Any]] = []
    for asset in assets:
        if not isinstance(asset, dict):
            raise ValueError("concat asset metadata is invalid")
        mime_type = str(asset.get("mime_type") or "").lower()
        if mime_type.startswith("image/") and allow_renderable_images:
            planned = {
                "duration_sec": settings.default_image_duration_sec,
                "width": settings.default_image_width,
                "height": settings.default_image_height,
                "fps": settings.default_video_fps,
            }
        elif mime_type.startswith("video/"):
            planned = asset
        else:
            raise ValueError("concat inputs must be video or renderable image assets")
        duration = finite_number(
            planned.get("duration_sec"),
            "concat asset duration",
            minimum=0.000_001,
            maximum=settings.max_duration_seconds,
            allow_none=False,
        )
        fps = finite_number(
            planned.get("fps"),
            "concat asset frame rate",
            minimum=0.000_001,
            maximum=settings.max_video_fps,
            allow_none=False,
        )
        assert duration is not None and fps is not None
        validate_dimensions(planned.get("width"), planned.get("height"), field="concat asset")
        if planned.get("width") is None or planned.get("height") is None:
            raise ValueError("concat asset geometry is unavailable")
        durations.append(float(duration))
        fps_values.append(float(fps))
        planned_assets.append(planned)

    source_duration = sum(durations)
    if source_duration > settings.max_duration_seconds:
        raise ValueError("concat aggregate source duration exceeds the configured limit")
    if normalized_transition == "crossfade":
        if resolved_transition <= 0 or any(
            duration <= resolved_transition for duration in durations
        ):
            raise ValueError("transition duration must be shorter than every clip")
        output_duration = sum(durations) - float(resolved_transition) * (len(durations) - 1)
    else:
        output_duration = sum(durations)
    if output_duration > settings.max_duration_seconds:
        raise ValueError("concat output duration exceeds the configured limit")

    first = planned_assets[0]
    resolved_width = target_width if target_width is not None else first.get("width")
    resolved_height = target_height if target_height is not None else first.get("height")
    try:
        width, height = validate_geometry(
            resolved_width,
            resolved_height,
            field="concat output",
        )
        aggregate_frames = sum(
            max(1, math.ceil(duration * fps))
            for duration, fps in zip(durations, fps_values, strict=True)
        )
        aggregate_source_work = 0
        for planned, duration, fps in zip(planned_assets, durations, fps_values, strict=True):
            source_width, source_height = validate_geometry(
                planned.get("width"), planned.get("height"), field="concat source"
            )
            aggregate_source_work += (
                source_width * source_height * max(1, math.ceil(duration * fps))
            )
        if aggregate_source_work > settings.max_decoded_video_pixel_frames:
            raise MediaLimitError(
                "concat aggregate source decode work exceeds the configured limit"
            )
        if width * height * aggregate_frames > settings.max_decoded_video_pixel_frames:
            raise MediaLimitError("concat aggregate scaled work exceeds the configured limit")
        validate_planned_video_work(
            width,
            height,
            fps=max(fps_values),
            duration=output_duration,
            field="concat output",
        )
    except MediaLimitError as exc:
        raise ValueError(str(exc)) from exc


def validate_render_request(values: dict[str, Any], *, iterative: bool, template_name: str) -> None:
    validate_finite_tree(values, "render")
    validate_caption_controls(values)
    validate_audio_controls(values)
    for name in (
        "caption_font_color",
        "caption_box_color",
        "caption_highlight_color",
    ):
        color = values.get(name)
        if color is not None:
            sanitize_color(color, "white")
    for name in ("hook", "headline", "cta", "price", "quote", "author"):
        text = values.get(name)
        if text is not None and (not isinstance(text, str) or len(text) > settings.max_text_chars):
            raise ValueError(f"{name} exceeds the configured text limit")
    broll = values.get("broll_asset_ids") or []
    if not isinstance(broll, list):
        raise ValueError("broll_asset_ids must be a list")
    iterations = 1
    if iterative:
        requested_iterations = values.get("max_iterations")
        if requested_iterations is None:
            requested_iterations = min(2, settings.max_render_iterations)
        resolved_iterations = finite_number(
            requested_iterations,
            "max_iterations",
            minimum=1,
            maximum=settings.max_render_iterations,
            integer=True,
            allow_none=False,
        )
        assert resolved_iterations is not None
        iterations = int(resolved_iterations)
    for minimum_name, maximum_name, minimum, maximum in (
        ("caption_font_size_min", "caption_font_size_max", 1, settings.max_font_size),
        ("caption_box_opacity_min", "caption_box_opacity_max", 0, 1),
        ("music_gain_min", "music_gain_max", 0.000_001, 4),
    ):
        low = finite_number(
            values.get(minimum_name), minimum_name, minimum=minimum, maximum=maximum
        )
        high = finite_number(
            values.get(maximum_name), maximum_name, minimum=minimum, maximum=maximum
        )
        if low is not None and high is not None and float(low) > float(high):
            raise ValueError(f"{minimum_name} must not exceed {maximum_name}")
    finite_number(values.get("max_crop_pct"), "max_crop_pct", minimum=0, maximum=100)
    finite_number(
        values.get("min_duration_sec"),
        "min_duration_sec",
        minimum=0,
        maximum=settings.max_duration_seconds,
    )
    finite_number(values.get("pass_threshold"), "pass_threshold", minimum=0, maximum=100)
    strategy = values.get("strategy")
    if strategy is not None and (
        not isinstance(strategy, str) or strategy.strip().lower() not in _ITERATE_STRATEGIES
    ):
        raise ValueError("strategy is unsupported")
    has_audio = bool(values.get("voice_asset_id") or values.get("music_asset_id"))
    has_captions = bool(
        values.get("captions_srt") or values.get("captions_vtt") or values.get("words_json")
    )
    variant_count = 4 if values.get("include_16_9") else 3
    base_operations = 1 + len(broll) + int(bool(broll))
    base_operations += int(bool(values.get("voice_asset_id")))
    base_operations += int(bool(values.get("music_asset_id")))
    base_operations += int(bool(values.get("voice_asset_id") and values.get("trim_silence")))
    base_operations += int(has_audio)
    per_variant = template_operation_count(template_name)
    per_variant += int(has_audio)
    per_variant += int(values.get("quality") == "draft")
    per_variant += int(has_captions)
    validate_batch_operations(
        iterations * (base_operations + variant_count * per_variant), "render"
    )


def validate_workflow(workflow: Any, asset_resolver: Any | None = None) -> None:
    if not isinstance(workflow, dict):
        raise ValueError("workflow must be an object")
    unknown_workflow_keys = set(workflow) - _WORKFLOW_TOP_LEVEL_KEYS
    if unknown_workflow_keys:
        raise ValueError("workflow contains unsupported fields")
    validate_finite_tree(workflow, "workflow")
    nodes = workflow.get("nodes")
    if not isinstance(nodes, list) or not nodes:
        raise ValueError("workflow.nodes must be a non-empty list")
    if len(nodes) > settings.max_workflow_nodes:
        raise ValueError("workflow exceeds the configured node limit")
    node_ids: set[str] = set()
    missing_single_inputs: list[str] = []
    operation_count = 0
    for node in nodes:
        if not isinstance(node, dict):
            raise ValueError("workflow nodes must be objects")
        if set(node) - _WORKFLOW_NODE_KEYS:
            raise ValueError("workflow node contains unsupported fields")
        node_id = _bounded_string(
            node.get("id"), "workflow node id", maximum=_MAX_WORKFLOW_ID_CHARS
        )
        if node_id in node_ids:
            raise ValueError("workflow node ids must be unique non-empty strings")
        node_ids.add(node_id)
        node_type_raw = _bounded_string(
            node.get("type"), "workflow node type", maximum=_MAX_WORKFLOW_ID_CHARS
        )
        node_type = node_type_raw.strip().lower()
        params = node.get("params") or {}
        if not isinstance(params, dict):
            raise ValueError("workflow node params must be an object")
        allowed_params = _WORKFLOW_PARAM_KEYS.get(node_type)
        if allowed_params is None:
            raise ValueError("workflow contains an unsupported node type")
        if set(params) - allowed_params:
            raise ValueError("workflow node params contain unsupported fields")
        _validate_nested_string_bounds(params, f"workflow.{node_id}.params")
        input_ref = node.get("input")
        inputs = node.get("inputs")
        if input_ref is not None and inputs is not None:
            raise ValueError("workflow node cannot contain both input and inputs")
        if input_ref is not None:
            _bounded_string(input_ref, "workflow node input", maximum=_MAX_WORKFLOW_STRING_CHARS)
        if inputs is not None and not isinstance(inputs, list):
            raise ValueError("workflow node inputs must be a list")
        if isinstance(inputs, list):
            for ref in inputs:
                _bounded_string(ref, "workflow node input", maximum=_MAX_WORKFLOW_STRING_CHARS)
        input_count = len(inputs) if isinstance(inputs, list) else 0
        if input_count > max(settings.max_concat_clips, settings.max_slideshow_images):
            raise ValueError("workflow node input count exceeds the configured limit")
        if node_type in _WORKFLOW_SINGLE_INPUT_TYPES and input_ref is None:
            missing_single_inputs.append(node_type)
        if node_type == "transcode":
            if not isinstance(params.get("preset"), str):
                raise ValueError("transcode requires preset")
            get_preset(params["preset"])
        elif node_type == "trim":
            validate_trim_request(params.get("start_sec", 0), params.get("end_sec", 0))
        elif node_type == "video_add_text":
            validate_caption_controls(params)
            if params.get("position") is not None and params.get("position") not in TEXT_POSITIONS:
                raise ValueError("video_add_text position is unsupported")
            text = params.get("text")
            if not isinstance(text, str) or not text or len(text) > settings.max_text_chars:
                raise ValueError("video_add_text text is invalid")
            finite_number(
                params.get("box_border_width"),
                "box_border_width",
                minimum=0,
                maximum=settings.max_box_border_width,
                integer=True,
            )
        elif node_type == "video_add_logo":
            if params.get("position") is not None and params.get("position") not in LOGO_POSITIONS:
                raise ValueError("video_add_logo position is unsupported")
            if not params.get("logo_asset_id") and not params.get("logo_key"):
                raise ValueError("video_add_logo requires logo_asset_id or logo_key")
            finite_number(
                params.get("scale_pct"),
                "scale_pct",
                minimum=settings.logo_min_scale_pct,
                maximum=settings.logo_max_scale_pct,
                integer=True,
            )
            finite_number(
                params.get("opacity"),
                "opacity",
                minimum=0,
                maximum=settings.logo_max_opacity,
            )
        elif node_type == "video_concat":
            if input_count < 2 or input_count > settings.max_concat_clips:
                raise ValueError("video_concat input count exceeds the configured limit")
            validate_dimensions(
                params.get("target_width"), params.get("target_height"), field="concat"
            )
            finite_number(
                params.get("transition_duration"),
                "transition_duration",
                minimum=0,
                maximum=settings.max_duration_seconds,
            )
        elif node_type == "image_to_video":
            validate_image_video_plan(
                duration=params.get("duration_sec", settings.default_image_duration_sec),
                width=params.get("width"),
                height=params.get("height"),
                fps=params.get("fps"),
            )
        elif node_type in {"images_to_slideshow", "images_to_slideshow_ken_burns"}:
            validate_slideshow_plan(
                input_count,
                params.get("duration_per_image"),
                params.get("durations"),
                params.get("width"),
                params.get("height"),
                params.get("fps"),
            )
        elif node_type in {
            "audio_normalize",
            "audio_mix",
            "audio_duck",
            "audio_mix_with_background",
            "audio_fade",
            "audio_trim_silence",
        }:
            validate_audio_controls(params)
            output_format = params.get("output_format")
            if output_format is not None and output_format not in {"mp3", "wav", "m4a"}:
                raise ValueError("output_format is unsupported")
            if node_type == "audio_mix" and input_count > settings.max_audio_tracks:
                raise ValueError("audio_mix input count exceeds the configured limit")
            if node_type == "audio_mix" and input_count < 1:
                raise ValueError("audio_mix requires inputs")
            volumes = params.get("volumes")
            if volumes is not None and len(volumes) != input_count:
                raise ValueError("volumes length must match workflow inputs")
            if node_type == "audio_mix" and params.get("duration_mode") not in {
                None,
                "longest",
                "shortest",
                "first",
            }:
                raise ValueError("duration_mode is unsupported")
            if node_type in {"audio_duck", "audio_mix_with_background"}:
                _bounded_string(
                    params.get("voice_asset_id"),
                    "voice_asset_id",
                    maximum=_MAX_WORKFLOW_STRING_CHARS,
                )
                _bounded_string(
                    params.get("music_asset_id"),
                    "music_asset_id",
                    maximum=_MAX_WORKFLOW_STRING_CHARS,
                )
            if (
                node_type == "audio_trim_silence"
                and params.get("trim_leading") is False
                and params.get("trim_trailing") is False
            ):
                raise ValueError("audio_trim_silence requires a trim direction")
        elif node_type == "template_apply":
            template_name = params.get("template_name")
            if not isinstance(template_name, str):
                raise ValueError("template_apply requires template_name")
            operation_count += validate_template_request(template_name, params.get("variables"))
            if params.get("quality") not in {None, "draft", "final"}:
                raise ValueError("template quality is unsupported")
        elif node_type == "brand_kit_apply":
            validate_caption_controls(params)
            if params.get("position") is not None and params.get("position") not in TEXT_POSITIONS:
                raise ValueError("brand_kit_apply position is unsupported")
            _bounded_string(
                params.get("brand_kit_id"),
                "brand_kit_id",
                maximum=_MAX_WORKFLOW_STRING_CHARS,
            )
        operation_count += max(input_count, 1)
    validate_batch_operations(operation_count, "workflow")

    outputs = workflow.get("outputs") or []
    if not isinstance(outputs, list) or len(outputs) > len(nodes):
        raise ValueError("workflow.outputs must be a bounded list")
    for output in outputs:
        if output not in node_ids:
            raise ValueError("workflow output references an unknown node")

    dependencies: dict[str, set[str]] = {}
    for node in nodes:
        refs: list[str] = []
        if isinstance(node.get("input"), str):
            refs.append(node["input"])
        if isinstance(node.get("inputs"), list):
            refs.extend(ref for ref in node["inputs"] if isinstance(ref, str))
        dependencies[str(node["id"])] = {ref for ref in refs if ref in node_ids}
    pending = dict(dependencies)
    resolved: set[str] = set()
    while pending:
        ready = {node_id for node_id, refs in pending.items() if refs <= resolved}
        if not ready:
            raise ValueError("workflow contains a dependency cycle")
        resolved.update(ready)
        for node_id in ready:
            pending.pop(node_id)
    if missing_single_inputs:
        raise ValueError(f"{missing_single_inputs[0]} requires input")

    if asset_resolver is not None:
        for node in nodes:
            if str(node.get("type") or "").strip().lower() != "video_concat":
                continue
            refs = node.get("inputs") or []
            if any(ref in node_ids for ref in refs):
                continue
            assets = []
            for ref in refs:
                asset = asset_resolver(ref)
                if not asset:
                    raise ValueError("workflow concat asset not found")
                assets.append(asset)
            params = node.get("params") or {}
            validate_concat_asset_plan(
                assets,
                target_width=params.get("target_width"),
                target_height=params.get("target_height"),
                transition=params.get("transition"),
                transition_duration=params.get("transition_duration"),
            )
