"""Deterministic, provider-owned tool catalog for the FFmpeg MCP.

The manifest is deliberately independent of FastMCP internals.  The Portal can
ingest it through ``list_capabilities(include_descriptors=true)`` without losing
descriptions, schemas, safety annotations, or compatibility metadata.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import os
import re
import types
from collections.abc import Callable, Mapping
from typing import Any, get_args, get_origin, get_type_hints

SCHEMA_VERSION = "1.0.0"
SERVICE_ID = "ffmpeg"
CATALOG_VERSION = "2026-07-18.4"
DOCUMENTATION_URL = "https://github.com/MADPANDA3D/FFMPEG-MCP"


TOOL_DESCRIPTIONS: dict[str, str] = {
    "check_configuration": (
        "Use this before media work to verify that FFmpeg, storage, queue, signing, "
        "and optional export integrations are configured. It reads configuration "
        "presence only, contacts no provider, and never returns credential values."
    ),
    "list_capabilities": (
        "Use this to understand the FFmpeg MCP surface by category and risk. Set "
        "include_descriptors to true when the caller needs the complete, lossless, "
        "versioned ToolManifest for catalog ingestion; it performs no media work."
    ),
    "get_endpoint_coverage": (
        "Use this to map supported FFmpeg capability areas to native MCP tools and "
        "their contract tiers. FFmpeg is a local command-line provider rather than "
        "an HTTP API, so this reports capability coverage instead of REST endpoints."
    ),
    "get_tool_usage": (
        "Use this after discovery to retrieve one tool's complete descriptor, input "
        "and output schemas, safety annotations, aliases, and confirmation rule. It "
        "does not execute the selected tool."
    ),
    "find_tools": (
        "Use this to find FFmpeg tools from a natural-language task. It performs "
        "deterministic punctuation-normalized multi-token search over agent-ready "
        "descriptors and supports category, risk, tier, and result-limit filters."
    ),
    "media_ingest_from_url": (
        "Use this to copy an allowlisted public media URL into owned FFmpeg staging "
        "storage and return a stable asset ID. Do not pass private-network URLs or "
        "unsupported content; the server downloads and validates external bytes."
    ),
    "media_ingest_from_drive": (
        "Use this to copy a Google Drive file into owned FFmpeg staging storage by "
        "file ID and return an asset record. The Drive file must be reachable by the "
        "configured ingest path; this reads external content and creates local state."
    ),
    "media_probe": (
        "Use this to inspect an existing staged asset with ffprobe and return its "
        "container, stream, duration, codec, and dimension metadata. It does not "
        "alter media bytes, but it records the probe result on the local asset."
    ),
    "ffmpeg_transcode": (
        "Use this to transcode a staged asset with an approved preset. It queues a "
        "bounded FFmpeg job and returns a job ID; do not use it for arbitrary FFmpeg "
        "flags because this service intentionally permits presets only."
    ),
    "ffmpeg_thumbnail": (
        "Use this to extract a still thumbnail from a staged video at a requested "
        "timestamp and optional width. It queues a media job and returns a job ID "
        "whose result can be followed with job_status."
    ),
    "ffmpeg_extract_audio": (
        "Use this to extract an audio track from a staged media asset into a supported "
        "format and bitrate. It creates a queued processing job and returns a job ID."
    ),
    "ffmpeg_trim": (
        "Use this to create a time-bounded clip from a staged asset. It queues a new "
        "output asset between start_sec and end_sec; choose reencode=false only when "
        "stream-copy boundary accuracy is acceptable."
    ),
    "video_add_text": (
        "Use this to render a bounded text overlay on a staged video with approved "
        "font, color, box, and position controls. It creates a queued output asset "
        "and never accepts raw FFmpeg filter expressions."
    ),
    "video_add_logo": (
        "Use this to overlay either a staged logo asset or an allowlisted server logo "
        "on a video. It queues a new output asset with bounded position, scale, and "
        "opacity controls."
    ),
    "captions_burn_in": (
        "Use this to burn SRT, VTT, or timed-word captions into a staged video with "
        "bounded brand and safe-zone styling. Supply exactly one caption source; it "
        "queues a new rendered asset."
    ),
    "video_analyze": (
        "Use this to analyze technical and optional rubric quality signals for a "
        "staged asset. It queues analysis and returns a job ID or cached report; it "
        "does not publish or replace the source media."
    ),
    "asset_compare": (
        "Use this to score and rank multiple staged assets against one named rubric. "
        "It queues analysis and returns a job ID or cached ranking without changing "
        "the compared media."
    ),
    "video_concat": (
        "Use this to join staged clips into one ordered video with bounded transition, "
        "canvas, and audio controls. It queues a new output asset and preserves the "
        "input assets."
    ),
    "image_to_video": (
        "Use this to turn one staged image into a fixed-duration video canvas. It "
        "queues a new asset with bounded duration, dimensions, frame rate, and "
        "background color."
    ),
    "images_to_slideshow": (
        "Use this to create a slideshow from ordered staged images using fixed or "
        "per-image durations. It queues a new video asset and does not modify inputs."
    ),
    "images_to_slideshow_ken_burns": (
        "Use this to create a slideshow with bounded Ken Burns pan-and-zoom motion "
        "from ordered staged images. It queues a new video asset and preserves inputs."
    ),
    "audio_normalize": (
        "Use this to create a loudness-normalized audio asset with bounded LUFS, "
        "range, peak, format, and bitrate controls. It queues a new output asset."
    ),
    "audio_mix": (
        "Use this to combine multiple staged audio assets with optional per-track "
        "volume, normalization, and duration behavior. It queues one mixed output."
    ),
    "audio_duck": (
        "Use this to mix voice over music while automatically reducing music during "
        "speech. It queues a new audio asset using bounded compressor controls."
    ),
    "audio_mix_with_background": (
        "Use this to combine voice and background music with optional ducking and "
        "independent gains. It queues a new audio asset and preserves both inputs."
    ),
    "audio_fade": (
        "Use this to add bounded fade-in and fade-out timing to a staged audio asset. "
        "It queues a new output asset rather than modifying the source."
    ),
    "audio_trim_silence": (
        "Use this to remove leading and/or trailing silence from staged audio using a "
        "bounded duration and decibel threshold. It queues a new output asset."
    ),
    "template_list": (
        "Use this to list the approved server-side render templates before choosing "
        "one for template_apply. It is read-only and performs no media processing."
    ),
    "template_describe": (
        "Use this to inspect one approved render template's variables and layers "
        "before applying it. It is read-only and performs no media processing."
    ),
    "template_apply": (
        "Use this to render a staged asset through an approved template with bounded "
        "variables and optional brand kit. It queues a new output asset."
    ),
    "brand_kit_upsert": (
        "Use this to create or replace a local FFmpeg brand-kit definition used by "
        "later renders. It changes persistent configuration state but does not render "
        "media by itself."
    ),
    "brand_kit_get": (
        "Use this to retrieve one local FFmpeg brand-kit definition by ID. It is "
        "read-only and never returns external credentials."
    ),
    "brand_kit_list": (
        "Use this to list available local brand-kit IDs before selecting one for a "
        "render. It is read-only and returns no credential values."
    ),
    "brand_kit_delete": (
        "Use this only to permanently remove a local brand-kit definition. The Portal "
        "and native caller must supply the exact confirmation phrase before execution; "
        "media assets already rendered with the kit are not deleted."
    ),
    "brand_kit_apply": (
        "Use this to apply an existing brand kit and optional text overlay to a staged "
        "asset. It queues a new branded output and preserves the source."
    ),
    "batch_export_formats": (
        "Use this to render one staged asset into multiple explicitly named approved "
        "presets. It queues a bounded batch job and returns its job ID."
    ),
    "batch_export_social_formats": (
        "Use this to export one staged asset to configured social formats, optionally "
        "overriding the preset list. It queues a bounded batch job."
    ),
    "campaign_process": (
        "Use this to process a bounded set of staged assets across social presets and "
        "optional template/brand settings. It queues a campaign batch job."
    ),
    "render_social_ad": (
        "Use this to produce approved social-ad variants from staged media, copy, "
        "captions, brand, and audio inputs. It queues bounded 9:16, 1:1, 4:5, and "
        "optional 16:9 outputs."
    ),
    "render_testimonial_clip": (
        "Use this to produce approved testimonial variants from staged media, quote, "
        "author, captions, brand, and audio inputs. It queues new output assets."
    ),
    "render_offer_card": (
        "Use this to produce approved offer-card variants from staged media, headline, "
        "price, CTA, brand, captions, and audio inputs. It queues new output assets."
    ),
    "render_iterate": (
        "Use this to render, analyze, and boundedly iterate an approved social ad, "
        "testimonial, or offer card until its rubric threshold or iteration limit is "
        "reached. It queues a non-trivial workflow and never publishes results."
    ),
    "workflow_run": (
        "Use this to execute a bounded directed workflow of supported FFmpeg operations "
        "when a single native tool is insufficient. It accepts only approved node "
        "types and queues new output assets."
    ),
    "ffmpeg_list_presets": (
        "Use this to list approved encoding presets before transcode or batch export. "
        "It is read-only and does not run FFmpeg."
    ),
    "ffmpeg_describe_preset": (
        "Use this to inspect one approved encoding preset's container and settings "
        "before processing. It is read-only and does not run FFmpeg."
    ),
    "rubric_list": (
        "Use this to list approved media-quality rubrics before analysis or iteration. "
        "It is read-only and performs no analysis."
    ),
    "rubric_describe": (
        "Use this to inspect one media-quality rubric and its scoring rules before "
        "analysis. It is read-only and performs no media work."
    ),
    "ffmpeg_capabilities": (
        "Use this legacy-compatible summary to inspect presets, limits, queues, and "
        "supported media features. Prefer list_capabilities with include_descriptors "
        "for the lossless versioned agent catalog."
    ),
    "job_status": (
        "Use this to read the complete status, progress, outputs, QA, and safe error "
        "summary for a previously returned job ID. It does not retry or mutate jobs."
    ),
    "job_progress": (
        "Use this for a compact progress and phase readback for a previously returned "
        "job ID. It is read-only and does not retry the job."
    ),
    "job_logs": (
        "Use this to read the bounded, sanitized log summary for a previously returned "
        "job ID when status alone is insufficient. It never returns request headers."
    ),
    "metrics_snapshot": (
        "Use this to inspect local FFmpeg queue depth and aggregate runtime counters "
        "for operational diagnosis. It is read-only and returns no credentials."
    ),
    "media_get_download_url": (
        "Use this to obtain a time-limited signed download URL for an unexpired staged "
        "asset. It does not expose storage credentials or change the asset."
    ),
    "media_export_to_drive": (
        "Use this to upload a staged asset to the configured Google Drive destination "
        "and return its Drive file ID. It is disabled by default, writes externally, "
        "and requires the exact native confirmation phrase."
    ),
    "media_export_to_discord": (
        "Use this to upload a staged asset to a specific Discord channel with an "
        "optional message. It is disabled by default, writes externally, and requires "
        "the exact native confirmation phrase."
    ),
}


CATEGORY_TOOLS: dict[str, tuple[str, ...]] = {
    "navigation": (
        "check_configuration",
        "list_capabilities",
        "get_endpoint_coverage",
        "get_tool_usage",
        "find_tools",
    ),
    "ingest_storage": (
        "media_ingest_from_url",
        "media_ingest_from_drive",
        "media_probe",
        "media_get_download_url",
        "media_export_to_drive",
        "media_export_to_discord",
    ),
    "video": (
        "ffmpeg_transcode",
        "ffmpeg_thumbnail",
        "ffmpeg_trim",
        "video_add_text",
        "video_add_logo",
        "captions_burn_in",
        "video_concat",
        "image_to_video",
        "images_to_slideshow",
        "images_to_slideshow_ken_burns",
    ),
    "audio": (
        "ffmpeg_extract_audio",
        "audio_normalize",
        "audio_mix",
        "audio_duck",
        "audio_mix_with_background",
        "audio_fade",
        "audio_trim_silence",
    ),
    "analysis": ("video_analyze", "asset_compare", "rubric_list", "rubric_describe"),
    "templates_brand": (
        "template_list",
        "template_describe",
        "template_apply",
        "brand_kit_upsert",
        "brand_kit_get",
        "brand_kit_list",
        "brand_kit_delete",
        "brand_kit_apply",
    ),
    "batch_workflow": (
        "batch_export_formats",
        "batch_export_social_formats",
        "campaign_process",
        "render_social_ad",
        "render_testimonial_clip",
        "render_offer_card",
        "render_iterate",
        "workflow_run",
    ),
    "reference_operations": (
        "ffmpeg_list_presets",
        "ffmpeg_describe_preset",
        "ffmpeg_capabilities",
        "job_status",
        "job_progress",
        "job_logs",
        "metrics_snapshot",
    ),
}


ALIASES: dict[str, tuple[str, ...]] = {
    "check_configuration": ("ffmpeg_check_configuration", "configuration_status"),
    "list_capabilities": ("ffmpeg_capabilities_v2", "capability_catalog"),
    "get_endpoint_coverage": ("ffmpeg_endpoint_coverage", "coverage"),
    "get_tool_usage": ("ffmpeg_tool_usage", "describe_tool"),
    "find_tools": ("ffmpeg_find_tools", "search_tools"),
    "media_ingest_from_url": ("ingest_url", "import_media_url"),
    "media_ingest_from_drive": ("ingest_drive_file",),
    "media_probe": ("ffprobe", "probe_media"),
    "ffmpeg_transcode": ("transcode", "convert_media"),
    "ffmpeg_thumbnail": ("thumbnail", "extract_thumbnail"),
    "ffmpeg_extract_audio": ("extract_audio",),
    "ffmpeg_trim": ("trim_video", "clip_media"),
    "video_add_text": ("add_text_overlay",),
    "video_add_logo": ("add_logo_overlay",),
    "captions_burn_in": ("burn_captions", "subtitle_video"),
    "video_concat": ("concat_videos", "join_clips"),
    "image_to_video": ("still_to_video",),
    "images_to_slideshow": ("create_slideshow",),
    "images_to_slideshow_ken_burns": ("ken_burns_slideshow",),
    "audio_normalize": ("normalize_audio",),
    "audio_mix": ("mix_audio",),
    "audio_duck": ("duck_music",),
    "template_apply": ("apply_template",),
    "brand_kit_apply": ("apply_brand_kit",),
    "batch_export_social_formats": ("export_social_formats",),
    "render_social_ad": ("create_social_ad",),
    "render_testimonial_clip": ("create_testimonial_clip",),
    "render_offer_card": ("create_offer_card",),
    "workflow_run": ("run_media_workflow",),
    "ffmpeg_list_presets": ("list_presets",),
    "ffmpeg_describe_preset": ("describe_preset",),
    "ffmpeg_capabilities": ("capabilities",),
    "job_status": ("get_job_status",),
    "job_progress": ("get_job_progress",),
    "job_logs": ("get_job_logs",),
    "media_get_download_url": ("get_download_url",),
    "media_export_to_drive": ("export_to_drive",),
    "media_export_to_discord": ("export_to_discord",),
}


READ_ONLY_TOOLS = {
    "check_configuration",
    "list_capabilities",
    "get_endpoint_coverage",
    "get_tool_usage",
    "find_tools",
    "template_list",
    "template_describe",
    "brand_kit_get",
    "brand_kit_list",
    "ffmpeg_list_presets",
    "ffmpeg_describe_preset",
    "rubric_list",
    "rubric_describe",
    "ffmpeg_capabilities",
    "job_status",
    "job_progress",
    "job_logs",
    "metrics_snapshot",
    "media_get_download_url",
}
DESTRUCTIVE_TOOLS = {"brand_kit_delete"}
OPEN_WORLD_TOOLS = {
    "media_ingest_from_url",
    "media_ingest_from_drive",
    "media_export_to_drive",
    "media_export_to_discord",
}
IDEMPOTENT_TOOLS = READ_ONLY_TOOLS | {
    "brand_kit_upsert",
    "brand_kit_delete",
}
CONFIRMATION_PHRASES = {
    "brand_kit_delete": "DELETE BRAND KIT",
    "media_export_to_drive": "EXPORT TO GOOGLE DRIVE",
    "media_export_to_discord": "EXPORT TO DISCORD",
}
NAVIGATION_ROLES = {
    "check_configuration": "configuration",
    "list_capabilities": "catalog",
    "get_endpoint_coverage": "coverage",
    "get_tool_usage": "reference",
    "find_tools": "discovery",
    "ffmpeg_capabilities": "legacy_summary",
}


PARAMETER_DESCRIPTIONS: dict[str, str] = {
    "include_descriptors": (
        "When true, include the complete ordered ToolManifest descriptors at the response "
        "top level."
    ),
    "query": (
        "Natural-language task or tool terms; all normalized tokens must match the ranked "
        "descriptor text."
    ),
    "category": "Optional exact manifest category filter.",
    "risk": "Optional risk filter: read, write, or destructive.",
    "tier": "Optional contract tier filter: agent_ready, legacy, or hidden.",
    "limit": "Maximum number of ranked results to return; values are clamped from 1 through 25.",
    "tool_name": "Native tool name or exact compatibility alias to describe.",
    "job_id": "Stable processing job ID returned by an FFmpeg operation.",
    "asset_id": "Stable ID of one staged FFmpeg media asset.",
    "asset_ids": "Ordered stable IDs of staged FFmpeg media assets.",
    "primary_asset_id": "Stable ID of the primary staged media asset for the render.",
    "broll_asset_ids": "Optional ordered staged asset IDs used as supporting B-roll.",
    "voice_asset_id": "Stable ID of the staged voice or dialogue audio asset.",
    "music_asset_id": "Stable ID of the staged background music asset.",
    "reference_asset_id": "Optional staged asset ID used as the analysis reference.",
    "logo_asset_id": "Optional staged image asset ID used as the logo overlay.",
    "font_asset_id": "Optional staged font asset ID used by the overlay renderer.",
    "caption_font_asset_id": "Optional staged font asset ID used for caption rendering.",
    "url": "Allowlisted public HTTP(S) media URL to download and validate.",
    "drive_file_id": "Google Drive file ID reachable by the configured ingest integration.",
    "folder_id": "Optional Google Drive destination folder ID; defaults to the configured folder.",
    "channel_id": "Discord channel ID that should receive the exported asset.",
    "filename_hint": "Optional safe filename hint retained with the ingested asset.",
    "filename": "Optional safe filename for the exported attachment.",
    "ttl_hours": "Optional staged-asset lifetime in hours before automatic expiry.",
    "preset": "Exact approved FFmpeg preset name.",
    "presets": "Ordered approved FFmpeg preset names for batch output.",
    "priority": "Optional queue priority configured by the server, such as urgent or batch.",
    "time_sec": "Video timestamp in seconds at which to extract the thumbnail.",
    "start_sec": "Inclusive clip start time in seconds.",
    "end_sec": "Exclusive clip end time in seconds; it must be later than start_sec.",
    "reencode": "When true, re-encode for accurate trim boundaries; false requests stream copy.",
    "format": "Requested supported output format for extracted audio.",
    "output_format": "Requested supported container or codec family for the new audio asset.",
    "bitrate": "Optional FFmpeg bitrate string accepted by the bounded operation.",
    "width": "Optional positive output width in pixels.",
    "height": "Optional positive output height in pixels.",
    "target_width": "Optional positive output canvas width in pixels.",
    "target_height": "Optional positive output canvas height in pixels.",
    "fps": "Optional positive output frame rate in frames per second.",
    "duration_sec": "Optional output duration in seconds.",
    "duration_per_image": "Optional seconds to display each slideshow image.",
    "durations": "Optional per-image durations in seconds, ordered to match asset_ids.",
    "duration_mode": "How mixed tracks determine final duration, using a supported server mode.",
    "background_color": "FFmpeg-compatible safe color name or value for unused canvas space.",
    "text": "Text content to render, bounded by the server's maximum character limit.",
    "position": "Approved overlay or caption position name.",
    "font_size": "Font size in pixels within configured minimum and maximum bounds.",
    "font_color": "Safe FFmpeg color used for text.",
    "font_name": "Allowlisted server font filename or name.",
    "background_box": "When true, render a box behind overlay text.",
    "box_color": "Safe FFmpeg color used for the text background box.",
    "box_border_width": "Background-box padding/border width in pixels within server limits.",
    "logo_key": "Allowlisted server-side logo key; use instead of logo_asset_id.",
    "scale_pct": "Logo width as a bounded percentage of the video canvas.",
    "opacity": "Logo opacity from 0.0 through 1.0.",
    "captions_srt": "Inline SRT caption text; provide only one caption source.",
    "captions_vtt": "Inline WebVTT caption text; provide only one caption source.",
    "words_json": (
        "Timed word objects containing word, start, and end values; provide only one "
        "caption source."
    ),
    "brand_kit_id": "Existing local brand-kit identifier.",
    "brand_kit": "Complete brand-kit object to validate and create or replace.",
    "highlight_mode": "Approved caption word-highlight behavior.",
    "highlight_color": "Safe color used for highlighted caption words.",
    "box_opacity": "Caption background opacity from 0.0 through 1.0.",
    "padding_px": "Caption box padding in pixels within server limits.",
    "max_chars": "Maximum characters per caption line within server limits.",
    "max_lines": "Maximum lines in one caption cue within server limits.",
    "max_words": "Maximum words in one caption cue within server limits.",
    "safe_zone_profile": "Approved platform safe-zone profile for caption placement.",
    "safe_zone_bottom_px": "Additional bottom safe-zone inset in pixels.",
    "safe_zone_top_px": "Additional top safe-zone inset in pixels.",
    "rubric_name": "Exact approved quality rubric name.",
    "target_preset": "Optional approved preset used as the target for analysis scoring.",
    "transition": "Approved transition type between concatenated clips.",
    "transition_duration": "Transition duration in seconds within server limits.",
    "include_audio": "Whether concatenation should retain and combine audio streams.",
    "target_lufs": "Target integrated loudness in LUFS.",
    "lra": "Target loudness range used by normalization.",
    "true_peak": "Maximum true-peak target in dBTP.",
    "volumes": "Optional gain multipliers ordered to match asset_ids.",
    "normalize": "Whether the mixed result should receive loudness normalization.",
    "ratio": "Compressor ratio used for voice-driven music ducking.",
    "threshold": "Signal threshold used to trigger ducking.",
    "attack_ms": "Ducking compressor attack time in milliseconds.",
    "release_ms": "Ducking compressor release time in milliseconds.",
    "music_gain": "Background-music gain multiplier within server limits.",
    "voice_gain": "Voice-track gain multiplier within server limits.",
    "ducking": "Whether voice-driven music ducking is enabled.",
    "fade_in_sec": "Fade-in duration in seconds.",
    "fade_out_sec": "Fade-out duration in seconds.",
    "fade_out_start": "Optional timestamp in seconds where fade-out begins.",
    "min_silence_sec": "Minimum detected silence duration eligible for trimming.",
    "threshold_db": "Silence detection threshold in decibels.",
    "trim_leading": "Whether leading silence should be removed.",
    "trim_trailing": "Whether trailing silence should be removed.",
    "trim_silence": "Whether the render pipeline should trim leading and trailing silence.",
    "trim_silence_min_sec": (
        "Minimum silence duration in seconds that the render pipeline may trim."
    ),
    "name": "Exact preset, rubric, or template name requested by this tool.",
    "template_name": "Exact approved render-template name.",
    "variables": "Template variables accepted by the selected approved template.",
    "quality": "Approved render quality mode, normally draft or final.",
    "hook": "Optional short opening hook copy for a social render.",
    "headline": "Optional headline copy for the render.",
    "cta": "Optional call-to-action copy for the render.",
    "price": "Optional price or offer copy for the render.",
    "quote": "Optional testimonial quotation copy for the render.",
    "author": "Optional testimonial attribution copy for the render.",
    "message": "Optional Discord message sent with the exported file.",
    "confirmation": "Exact case-sensitive confirmation phrase declared by this tool descriptor.",
    "include_16_9": "When true, add a 16:9 variant to the default social outputs.",
    "framing_mode": "Approved fit, fill, crop, or safe framing behavior.",
    "workflow": (
        "Bounded workflow object containing approved nodes, dependencies, and output node IDs."
    ),
    "render_type": "Approved iterative render type: social_ad, testimonial_clip, or offer_card.",
    "strategy": "Approved iteration strategy controlling which bounded changes may be attempted.",
    "max_iterations": "Maximum bounded render-analysis iterations.",
    "pass_threshold": "Rubric score that ends iteration successfully.",
    "fail_fast": "When true, stop after the first non-recoverable QA failure.",
    "lock_framing": "When true, iteration may not change framing controls.",
    "lock_captions": "When true, iteration may not change caption controls.",
    "lock_audio": "When true, iteration may not change audio controls.",
    "allow_trim_silence": "Whether iteration may enable silence trimming.",
    "max_crop_pct": "Maximum percentage of the source frame that iteration may crop.",
    "min_duration_sec": "Minimum output duration iteration must preserve.",
}


def _category_for(name: str) -> str:
    for category, names in CATEGORY_TOOLS.items():
        if name in names:
            return category
    raise ValueError(f"Tool metadata has no category: {name}")


def _title(name: str) -> str:
    return " ".join(
        part.upper() if part in {"url", "qa"} else part.capitalize() for part in name.split("_")
    )


def _parameter_description(name: str, tool_name: str) -> str:
    direct = PARAMETER_DESCRIPTIONS.get(name)
    if direct:
        return direct
    prefixes = (
        "caption_",
        "ducking_",
        "audio_",
        "trim_silence_",
    )
    for prefix in prefixes:
        if name.startswith(prefix):
            base = PARAMETER_DESCRIPTIONS.get(name[len(prefix) :])
            if base:
                return base
    if name.endswith("_min"):
        base = _parameter_description(name[:-4], tool_name)
        return f"Minimum allowed value for this iteration control. {base}"
    if name.endswith("_max"):
        base = _parameter_description(name[:-4], tool_name)
        return f"Maximum allowed value for this iteration control. {base}"
    human = name.replace("_", " ")
    return (
        f"{human.capitalize()} used by {_title(tool_name)}; omit it to use the "
        "documented server default."
    )


def _json_type_schema(annotation: Any) -> dict[str, Any]:
    if annotation is inspect.Signature.empty or annotation is Any:
        return {}
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin in {types.UnionType, __import__("typing").Union}:
        return {"anyOf": [_json_type_schema(arg) for arg in args]}
    if annotation is type(None):
        return {"type": "null"}
    if annotation is str:
        return {"type": "string"}
    if annotation is bool:
        return {"type": "boolean"}
    if annotation is int:
        return {"type": "integer"}
    if annotation is float:
        return {"type": "number"}
    if origin is list:
        item_type = args[0] if args else Any
        return {"type": "array", "items": _json_type_schema(item_type)}
    if origin is dict or annotation is dict:
        return {"type": "object", "additionalProperties": True}
    return {}


_SCHEMA_MAX_ASSET_TTL_HOURS = 720
_SCHEMA_MAX_DURATION_SECONDS = 86_400


_NUMERIC_INPUT_BOUNDS: dict[str, dict[str, int | float]] = {
    "ttl_hours": {"minimum": 1, "maximum": _SCHEMA_MAX_ASSET_TTL_HOURS},
    "time_sec": {"minimum": 0, "maximum": _SCHEMA_MAX_DURATION_SECONDS},
    "start_sec": {"minimum": 0, "maximum": _SCHEMA_MAX_DURATION_SECONDS},
    "end_sec": {"minimum": 0, "maximum": _SCHEMA_MAX_DURATION_SECONDS},
    "duration_sec": {"exclusiveMinimum": 0, "maximum": _SCHEMA_MAX_DURATION_SECONDS},
    "duration_per_image": {
        "exclusiveMinimum": 0,
        "maximum": _SCHEMA_MAX_DURATION_SECONDS,
    },
    "transition_duration": {"minimum": 0, "maximum": _SCHEMA_MAX_DURATION_SECONDS},
    "width": {"minimum": 1, "maximum": 8_192},
    "height": {"minimum": 1, "maximum": 8_192},
    "target_width": {"minimum": 1, "maximum": 8_192},
    "target_height": {"minimum": 1, "maximum": 8_192},
    "fps": {"minimum": 1, "maximum": 120},
    "font_size": {"minimum": 1, "maximum": 160},
    "caption_font_size": {"minimum": 1, "maximum": 160},
    "caption_font_size_min": {"minimum": 1, "maximum": 160},
    "caption_font_size_max": {"minimum": 1, "maximum": 160},
    "box_border_width": {"minimum": 0, "maximum": 80},
    "padding_px": {"minimum": 0, "maximum": 400},
    "caption_padding_px": {"minimum": 0, "maximum": 400},
    "safe_zone_bottom_px": {"minimum": 0, "maximum": 400},
    "safe_zone_top_px": {"minimum": 0, "maximum": 400},
    "caption_safe_zone_bottom_px": {"minimum": 0, "maximum": 400},
    "caption_safe_zone_top_px": {"minimum": 0, "maximum": 400},
    "max_chars": {"minimum": 1, "maximum": 200},
    "caption_max_chars": {"minimum": 1, "maximum": 200},
    "max_lines": {"minimum": 1, "maximum": 6},
    "caption_max_lines": {"minimum": 1, "maximum": 6},
    "max_words": {"minimum": 1, "maximum": 30},
    "caption_max_words": {"minimum": 1, "maximum": 30},
    "opacity": {"minimum": 0, "maximum": 1},
    "logo_opacity": {"minimum": 0, "maximum": 1},
    "box_opacity": {"minimum": 0, "maximum": 1},
    "caption_box_opacity": {"minimum": 0, "maximum": 1},
    "caption_box_opacity_min": {"minimum": 0, "maximum": 1},
    "caption_box_opacity_max": {"minimum": 0, "maximum": 1},
    "scale_pct": {"minimum": 5, "maximum": 40},
    "logo_scale_pct": {"minimum": 5, "maximum": 40},
    "target_lufs": {"minimum": -30, "maximum": -5},
    "audio_target_lufs": {"minimum": -30, "maximum": -5},
    "lra": {"exclusiveMinimum": 0, "maximum": 20},
    "audio_lra": {"exclusiveMinimum": 0, "maximum": 20},
    "true_peak": {"minimum": -10, "maximum": 0},
    "audio_true_peak": {"minimum": -10, "maximum": 0},
    "ratio": {"minimum": 1, "maximum": 20},
    "ducking_ratio": {"minimum": 1, "maximum": 20},
    "threshold": {"exclusiveMinimum": 0, "maximum": 1},
    "ducking_threshold": {"exclusiveMinimum": 0, "maximum": 1},
    "attack_ms": {"minimum": 1, "maximum": 2_000},
    "ducking_attack_ms": {"minimum": 1, "maximum": 2_000},
    "release_ms": {"minimum": 1, "maximum": 5_000},
    "ducking_release_ms": {"minimum": 1, "maximum": 5_000},
    "music_gain": {"exclusiveMinimum": 0, "maximum": 4},
    "voice_gain": {"exclusiveMinimum": 0, "maximum": 4},
    "music_gain_min": {"exclusiveMinimum": 0, "maximum": 4},
    "music_gain_max": {"exclusiveMinimum": 0, "maximum": 4},
    "fade_in_sec": {"minimum": 0, "maximum": _SCHEMA_MAX_DURATION_SECONDS},
    "fade_out_sec": {"minimum": 0, "maximum": _SCHEMA_MAX_DURATION_SECONDS},
    "fade_out_start": {"minimum": 0, "maximum": _SCHEMA_MAX_DURATION_SECONDS},
    "min_silence_sec": {
        "exclusiveMinimum": 0,
        "maximum": _SCHEMA_MAX_DURATION_SECONDS,
    },
    "trim_silence_min_sec": {
        "exclusiveMinimum": 0,
        "maximum": _SCHEMA_MAX_DURATION_SECONDS,
    },
    "threshold_db": {"minimum": -120, "maximum": 0},
    "trim_silence_threshold_db": {"minimum": -120, "maximum": 0},
    "max_iterations": {"minimum": 1, "maximum": 5},
    "max_crop_pct": {"minimum": 0, "maximum": 100},
    "min_duration_sec": {"minimum": 0, "maximum": _SCHEMA_MAX_DURATION_SECONDS},
    "pass_threshold": {"minimum": 0, "maximum": 100},
    "limit": {"minimum": 1, "maximum": 50},
}

_ENUM_INPUTS: dict[str, list[str]] = {
    "format": ["mp3", "wav", "m4a"],
    "output_format": ["mp3", "wav", "m4a"],
    "duration_mode": ["longest", "shortest", "first"],
    "transition": ["none", "crossfade"],
    "quality": ["draft", "final"],
    "framing_mode": ["safe_pad", "crop"],
    "render_type": ["social_ad", "testimonial_clip", "offer_card"],
    "caption_position": ["bottom_safe", "mid", "top"],
    "highlight_mode": ["word"],
    "strategy": ["audio_first", "balanced", "captions_first", "framing_first"],
    "safe_zone_profile": ["tiktok", "reels", "shorts"],
}

_WORKFLOW_PARAM_NAMES = {
    "preset",
    "start_sec",
    "end_sec",
    "reencode",
    "text",
    "position",
    "font_size",
    "font_color",
    "background_box",
    "box_color",
    "box_border_width",
    "font_name",
    "font_asset_id",
    "logo_asset_id",
    "logo_key",
    "scale_pct",
    "opacity",
    "transition",
    "transition_duration",
    "target_width",
    "target_height",
    "include_audio",
    "duration_sec",
    "width",
    "height",
    "fps",
    "background_color",
    "duration_per_image",
    "durations",
    "output_format",
    "target_lufs",
    "lra",
    "true_peak",
    "bitrate",
    "volumes",
    "normalize",
    "duration_mode",
    "voice_asset_id",
    "music_asset_id",
    "ratio",
    "threshold",
    "attack_ms",
    "release_ms",
    "music_gain",
    "ducking",
    "voice_gain",
    "fade_in_sec",
    "fade_out_sec",
    "fade_out_start",
    "min_silence_sec",
    "threshold_db",
    "trim_leading",
    "trim_trailing",
    "template_name",
    "variables",
    "brand_kit_id",
    "quality",
}

_WORKFLOW_BOOLEAN_PARAMS = {
    "reencode",
    "background_box",
    "include_audio",
    "normalize",
    "ducking",
    "trim_leading",
    "trim_trailing",
}

_WORKFLOW_INTEGER_PARAMS = {
    "font_size",
    "box_border_width",
    "scale_pct",
    "target_width",
    "target_height",
    "width",
    "height",
    "fps",
    "attack_ms",
    "release_ms",
}


def _schema_allows_null(schema: Mapping[str, Any]) -> bool:
    return any(item.get("type") == "null" for item in schema.get("anyOf", []))


def _bounded_word_items_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["word", "start", "end"],
        "properties": {
            "word": {"type": "string", "minLength": 1, "maxLength": 256},
            "start": {
                "type": "number",
                "minimum": 0,
                "maximum": _SCHEMA_MAX_DURATION_SECONDS,
            },
            "end": {
                "type": "number",
                "exclusiveMinimum": 0,
                "maximum": _SCHEMA_MAX_DURATION_SECONDS,
            },
        },
        "additionalProperties": False,
    }


def _workflow_input_schema(description: str) -> dict[str, Any]:
    param_properties: dict[str, Any] = {}
    for name in sorted(_WORKFLOW_PARAM_NAMES):
        schema: dict[str, Any]
        numeric_bounds = _NUMERIC_INPUT_BOUNDS.get(name)
        if name in _WORKFLOW_BOOLEAN_PARAMS:
            schema = {"type": "boolean"}
        elif name in _WORKFLOW_INTEGER_PARAMS:
            schema = {"type": "integer", **(numeric_bounds or {})}
        elif numeric_bounds:
            schema = {"type": "number", **numeric_bounds}
        elif name == "durations":
            schema = {
                "type": "array",
                "maxItems": 60,
                "items": {
                    "type": "number",
                    "exclusiveMinimum": 0,
                    "maximum": _SCHEMA_MAX_DURATION_SECONDS,
                },
            }
        elif name == "volumes":
            schema = {
                "type": "array",
                "maxItems": 8,
                "items": {"type": "number", "minimum": 0, "maximum": 4},
            }
        elif name == "variables":
            schema = {
                "type": "object",
                "maxProperties": 100,
                "additionalProperties": True,
            }
        else:
            schema = {"type": "string", "minLength": 1, "maxLength": 512}
        if name in _ENUM_INPUTS:
            schema["enum"] = _ENUM_INPUTS[name]
        param_properties[name] = schema
    node_schema = {
        "type": "object",
        "required": ["id", "type"],
        "properties": {
            "id": {"type": "string", "minLength": 1, "maxLength": 64},
            "type": {
                "enum": [
                    "transcode",
                    "trim",
                    "video_add_text",
                    "video_add_logo",
                    "video_concat",
                    "image_to_video",
                    "images_to_slideshow",
                    "images_to_slideshow_ken_burns",
                    "audio_normalize",
                    "audio_mix",
                    "audio_duck",
                    "audio_mix_with_background",
                    "audio_fade",
                    "audio_trim_silence",
                    "template_apply",
                    "brand_kit_apply",
                ]
            },
            "input": {"type": "string", "minLength": 1, "maxLength": 512},
            "inputs": {
                "type": "array",
                "minItems": 1,
                "maxItems": 60,
                "items": {"type": "string", "minLength": 1, "maxLength": 512},
            },
            "params": {
                "type": "object",
                "maxProperties": 12,
                "properties": param_properties,
                "additionalProperties": False,
            },
        },
        "additionalProperties": False,
        "allOf": [
            {
                "if": {"properties": {"type": {"const": "video_add_text"}}},
                "then": {
                    "properties": {
                        "params": {
                            "properties": {"position": {"enum": ["bottom", "center", "top"]}}
                        }
                    }
                },
            },
            {
                "if": {"properties": {"type": {"const": "video_add_logo"}}},
                "then": {
                    "properties": {
                        "params": {
                            "properties": {
                                "position": {
                                    "enum": [
                                        "bottom-left",
                                        "bottom-right",
                                        "top-left",
                                        "top-right",
                                    ]
                                }
                            }
                        }
                    }
                },
            },
            {
                "if": {"properties": {"type": {"const": "brand_kit_apply"}}},
                "then": {
                    "properties": {
                        "params": {
                            "properties": {"position": {"enum": ["bottom", "center", "top"]}}
                        }
                    }
                },
            },
        ],
    }
    return {
        "type": "object",
        "description": description,
        "required": ["nodes"],
        "properties": {
            "nodes": {"type": "array", "minItems": 1, "maxItems": 40, "items": node_schema},
            "outputs": {
                "type": "array",
                "maxItems": 40,
                "items": {"type": "string", "minLength": 1, "maxLength": 64},
            },
        },
        "additionalProperties": False,
    }


def _apply_input_constraints(tool_name: str, name: str, schema: dict[str, Any]) -> None:
    schema.update(_NUMERIC_INPUT_BOUNDS.get(name, {}))
    if name == "position":
        per_tool_positions = {
            "brand_kit_apply": ["bottom", "center", "top"],
            "captions_burn_in": ["bottom_safe", "mid", "top"],
            "video_add_logo": ["bottom-left", "bottom-right", "top-left", "top-right"],
            "video_add_text": ["bottom", "center", "top"],
            "video_analyze": ["bottom_safe", "mid", "top"],
        }
        if tool_name in per_tool_positions:
            position_values: list[str | None] = list(per_tool_positions[tool_name])
            if _schema_allows_null(schema):
                position_values.append(None)
            schema["enum"] = position_values
    if name in _ENUM_INPUTS:
        values: list[str | None] = list(_ENUM_INPUTS[name])
        if _schema_allows_null(schema):
            values.append(None)
        schema["enum"] = values
    if name in {"asset_ids", "broll_asset_ids", "presets", "durations", "volumes"}:
        maxima = {
            "asset_ids": 50,
            "broll_asset_ids": 19,
            "presets": 12,
            "durations": 60,
            "volumes": 8,
        }
        schema["maxItems"] = maxima[name]
        if name in {"asset_ids", "presets"}:
            schema["minItems"] = 1
        if name in {"asset_ids", "broll_asset_ids", "presets"}:
            schema["items"] = {"type": "string", "minLength": 1, "maxLength": 512}
    if name == "asset_ids":
        per_tool_maxima = {
            "video_concat": 20,
            "images_to_slideshow": 60,
            "images_to_slideshow_ken_burns": 60,
            "audio_mix": 8,
        }
        schema["maxItems"] = per_tool_maxima.get(tool_name, 50)
    if name == "words_json":
        schema.update(
            {
                "maxItems": 2_000,
                "items": _bounded_word_items_schema(),
            }
        )
    if name == "workflow":
        description = str(schema.get("description") or "Bounded media workflow.")
        schema.clear()
        schema.update(_workflow_input_schema(description))
    if name in {"variables", "brand_kit"}:
        schema["maxProperties"] = 100
    if name in {"captions_srt", "captions_vtt"}:
        schema["maxLength"] = 131_072
    elif name in {"text", "hook", "headline", "cta", "price", "quote", "author"}:
        schema["maxLength"] = 200
    elif name == "url":
        schema["maxLength"] = 2_048
    elif name == "message":
        schema["maxLength"] = 2_000
    elif "string" in {schema.get("type"), *(item.get("type") for item in schema.get("anyOf", []))}:
        schema["maxLength"] = 512


def _input_schema(tool_name: str, function: Callable[..., Any]) -> dict[str, Any]:
    signature = inspect.signature(function)
    try:
        hints = get_type_hints(function)
    except (NameError, TypeError):
        hints = {}
    properties: dict[str, Any] = {}
    required: list[str] = []
    for name, parameter in signature.parameters.items():
        schema = _json_type_schema(hints.get(name, parameter.annotation))
        schema["description"] = _parameter_description(name, tool_name)
        _apply_input_constraints(tool_name, name, schema)
        if parameter.default is inspect.Signature.empty:
            required.append(name)
        elif parameter.default is not None and isinstance(
            parameter.default, (str, int, float, bool)
        ):
            schema["default"] = parameter.default
        properties[name] = schema
    result: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required:
        result["required"] = required
    return result


def _property(type_name: str, description: str, **extra: Any) -> dict[str, Any]:
    return {"type": type_name, "description": description, **extra}


def _object_property(description: str) -> dict[str, Any]:
    return {"type": "object", "description": description, "additionalProperties": True}


def _nullable_property(type_name: str, description: str, **extra: Any) -> dict[str, Any]:
    return {
        "description": description,
        "anyOf": [
            {"type": type_name, **extra},
            {"type": "null"},
        ],
    }


JOB_OUTPUT_TOOLS = {
    "ffmpeg_transcode",
    "ffmpeg_thumbnail",
    "ffmpeg_extract_audio",
    "ffmpeg_trim",
    "video_add_text",
    "video_add_logo",
    "captions_burn_in",
    "video_concat",
    "image_to_video",
    "images_to_slideshow",
    "images_to_slideshow_ken_burns",
    "audio_normalize",
    "audio_mix",
    "audio_duck",
    "audio_mix_with_background",
    "audio_fade",
    "audio_trim_silence",
    "template_apply",
    "brand_kit_apply",
    "batch_export_formats",
    "batch_export_social_formats",
    "campaign_process",
    "render_social_ad",
    "render_testimonial_clip",
    "render_offer_card",
    "workflow_run",
}


def _tool_manifest_output_schema() -> dict[str, Any]:
    nullable_string = {"anyOf": [{"type": "string"}, {"type": "null"}]}
    descriptor_fields = [
        "serviceId",
        "nativeToolName",
        "canonicalName",
        "aliases",
        "title",
        "description",
        "category",
        "deprecation",
        "inputSchema",
        "outputSchema",
        "annotations",
        "confirmation",
        "documentationUrl",
        "navigationRole",
        "catalogVersion",
        "tier",
        "descriptorHash",
    ]
    descriptor_schema = {
        "type": "object",
        "required": descriptor_fields,
        "properties": {
            "serviceId": {"const": SERVICE_ID},
            "nativeToolName": {"type": "string", "minLength": 1},
            "canonicalName": {"type": "string", "minLength": 1},
            "aliases": {"type": "array", "items": {"type": "string"}},
            "title": {"type": "string", "minLength": 1},
            "description": {"type": "string", "minLength": 1},
            "category": {"type": "string", "minLength": 1},
            "deprecation": {
                "type": "object",
                "required": [
                    "deprecated",
                    "since",
                    "replacement",
                    "sunsetAt",
                    "message",
                ],
                "properties": {
                    "deprecated": {"type": "boolean"},
                    "since": nullable_string,
                    "replacement": nullable_string,
                    "sunsetAt": nullable_string,
                    "message": nullable_string,
                },
                "additionalProperties": False,
            },
            "inputSchema": {"type": "object", "additionalProperties": True},
            "outputSchema": {"type": "object", "additionalProperties": True},
            "annotations": {
                "type": "object",
                "required": [
                    "readOnlyHint",
                    "destructiveHint",
                    "openWorldHint",
                    "idempotentHint",
                ],
                "properties": {
                    "readOnlyHint": {"type": "boolean"},
                    "destructiveHint": {"type": "boolean"},
                    "openWorldHint": {"type": "boolean"},
                    "idempotentHint": {"type": "boolean"},
                },
                "additionalProperties": False,
            },
            "confirmation": {
                "type": "object",
                "required": ["required", "parameter", "exactPhrase", "when"],
                "properties": {
                    "required": {"type": "boolean"},
                    "parameter": nullable_string,
                    "exactPhrase": nullable_string,
                    "when": nullable_string,
                },
                "additionalProperties": False,
            },
            "documentationUrl": {"type": "string", "minLength": 1},
            "navigationRole": nullable_string,
            "catalogVersion": {"type": "string", "minLength": 1},
            "tier": {"enum": ["agent_ready", "legacy", "hidden"]},
            "descriptorHash": {"type": "string", "pattern": "^[a-f0-9]{64}$"},
        },
        "additionalProperties": False,
    }
    return {
        "$defs": {"toolDescriptor": descriptor_schema},
        "type": "object",
        "required": [
            "schemaVersion",
            "serviceId",
            "catalogVersion",
            "buildSha",
            "descriptorHash",
            "counts",
            "tools",
        ],
        "properties": {
            "schemaVersion": {"const": SCHEMA_VERSION},
            "serviceId": {"const": SERVICE_ID},
            "catalogVersion": {"type": "string", "minLength": 1},
            "buildSha": {"type": "string", "minLength": 1},
            "descriptorHash": {"type": "string", "pattern": "^[a-f0-9]{64}$"},
            "counts": {
                "type": "object",
                "required": ["raw", "agentReady", "legacy", "hidden"],
                "properties": {
                    "raw": {"type": "integer", "minimum": 0},
                    "agentReady": {"type": "integer", "minimum": 0},
                    "legacy": {"type": "integer", "minimum": 0},
                    "hidden": {"type": "integer", "minimum": 0},
                },
                "additionalProperties": False,
            },
            "tools": {
                "type": "array",
                "items": {"$ref": "#/$defs/toolDescriptor"},
            },
        },
        "additionalProperties": False,
    }


def _output_schema(name: str) -> dict[str, Any]:
    if name == "list_capabilities":
        return _tool_manifest_output_schema()
    properties: dict[str, Any]
    required: list[str]
    additional = False
    if name in JOB_OUTPUT_TOOLS:
        properties = {
            "job_id": _property("string", "Stable ID used with the job readback tools."),
            "cache_hit": _property("boolean", "Whether a matching completed result was reused."),
            "output_asset_ids": _property(
                "array",
                "Completed output asset IDs when immediately available from cache.",
                items={"type": "string"},
            ),
        }
        required = ["job_id", "cache_hit"]
    elif name == "media_ingest_from_url" or name == "media_ingest_from_drive":
        properties = {
            "asset_id": _property("string", "Stable staged-media asset ID."),
            "mime_type": _property("string", "Validated media MIME type."),
            "size_bytes": _property("integer", "Validated asset byte size."),
            "sha256": _property("string", "SHA-256 digest of the staged bytes."),
            "original_filename": _property("string", "Stored safe original filename."),
            "expires_at": _property("integer", "Unix expiry timestamp for the staged asset."),
        }
        required = ["asset_id"]
    elif name == "media_probe":
        properties = {
            "format": _object_property("ffprobe container metadata."),
            "streams": _property(
                "array",
                "ffprobe audio, video, and subtitle stream metadata.",
                items={"type": "object", "additionalProperties": True},
            ),
        }
        required = []
        additional = True
    elif name == "video_analyze":
        properties = {
            "job_id": _property("string", "Stable analysis job ID."),
            "cache_hit": _property("boolean", "Whether a cached report was reused."),
            "report": _object_property("Completed analysis report when available from cache."),
        }
        required = ["job_id", "cache_hit"]
    elif name == "asset_compare":
        properties = {
            "job_id": _property("string", "Stable comparison job ID."),
            "cache_hit": _property("boolean", "Whether a cached ranking was reused."),
            "ranking": _property(
                "array",
                "Ranked asset reports when available from cache.",
                items={"type": "object", "additionalProperties": True},
            ),
        }
        required = ["job_id", "cache_hit"]
    elif name == "render_iterate":
        properties = {
            "job_id": _property("string", "Stable iterative-render job ID."),
            "cache_hit": _property("boolean", "Whether a cached iterative result was reused."),
            "result": _object_property("Completed iteration result when available from cache."),
        }
        required = ["job_id", "cache_hit"]
    elif name == "template_list":
        properties = {
            "templates": _property(
                "array",
                "Approved template summaries.",
                items={"type": "object", "additionalProperties": True},
            )
        }
        required = ["templates"]
    elif name == "template_describe":
        properties = {"template": _object_property("Complete approved template definition.")}
        required = ["template"]
    elif name in {"brand_kit_upsert", "brand_kit_get"}:
        properties = {"brand_kit": _object_property("Sanitized local brand-kit definition.")}
        required = ["brand_kit"]
    elif name == "brand_kit_list":
        properties = {
            "brand_kit_ids": _property(
                "array", "Available local brand-kit IDs.", items={"type": "string"}
            )
        }
        required = ["brand_kit_ids"]
    elif name == "brand_kit_delete":
        properties = {
            "deleted": _property("boolean", "Whether the brand-kit definition was removed.")
        }
        required = ["deleted"]
    elif name == "ffmpeg_list_presets":
        properties = {
            "presets": _property(
                "array",
                "Approved preset summaries.",
                items={"type": "object", "additionalProperties": True},
            )
        }
        required = ["presets"]
    elif name == "ffmpeg_describe_preset":
        properties = {"preset": _object_property("Complete approved preset definition.")}
        required = ["preset"]
    elif name == "rubric_list":
        properties = {
            "rubrics": _property(
                "array",
                "Approved rubric summaries.",
                items={"type": "object", "additionalProperties": True},
            )
        }
        required = ["rubrics"]
    elif name == "rubric_describe":
        properties = {"rubric": _object_property("Complete approved rubric definition.")}
        required = ["rubric"]
    elif name == "ffmpeg_capabilities":
        properties = {
            "tool_mode": _property("string", "Fixed individual-tool exposure mode."),
            "tool_names": _property(
                "array", "Exposed legacy tool names.", items={"type": "string"}
            ),
            "limits": _object_property("Configured safe media-processing limits."),
            "presets": _property(
                "array",
                "Approved encoding presets.",
                items={"type": "object", "additionalProperties": True},
            ),
        }
        required = ["tool_mode", "tool_names"]
        additional = True
    elif name == "check_configuration":
        properties = {
            "ok": _property("boolean", "Whether required FFmpeg service configuration is ready."),
            "serviceId": _property("string", "Canonical provider service ID."),
            "required": _object_property("Required configuration presence booleans only."),
            "optional": _object_property(
                "Optional integration configuration presence booleans only."
            ),
            "missing": _property(
                "array", "Names of missing required settings.", items={"type": "string"}
            ),
        }
        required = ["ok", "serviceId", "required", "optional", "missing"]
    elif name == "get_endpoint_coverage":
        properties = {
            "serviceId": _property("string", "Canonical provider service ID."),
            "providerKind": _property("string", "Provider interface type."),
            "source": _property("string", "Authoritative FFmpeg documentation URL."),
            "entries": _property(
                "array",
                "Filtered capability-to-tool coverage entries.",
                items={
                    "type": "object",
                    "properties": {
                        "capability": _property("string", "Manifest capability category."),
                        "nativeToolName": _property("string", "Native FFmpeg MCP tool name."),
                        "title": _property("string", "Human-readable tool title."),
                        "tier": _property("string", "Contract tier."),
                        "status": _property("string", "Coverage disposition."),
                        "providerInterface": _property("string", "Underlying provider interface."),
                        "documentationUrl": _property("string", "Tool documentation URL."),
                    },
                    "required": [
                        "capability",
                        "nativeToolName",
                        "title",
                        "tier",
                        "status",
                        "providerInterface",
                        "documentationUrl",
                    ],
                    "additionalProperties": False,
                },
            ),
        }
        required = ["serviceId", "providerKind", "source", "entries"]
    elif name == "get_tool_usage":
        properties = {"tool": _object_property("Complete lossless ToolManifest descriptor.")}
        required = ["tool"]
    elif name == "find_tools":
        properties = {
            "query": _property("string", "Normalized caller query."),
            "count": _property("integer", "Number of returned ranked matches."),
            "results": _property(
                "array",
                "Ranked compact tool matches.",
                items={
                    "type": "object",
                    "properties": {
                        "serviceId": _property("string", "Canonical provider service ID."),
                        "toolName": _property("string", "Native FFmpeg MCP tool name."),
                        "title": _property("string", "Human-readable tool title."),
                        "summary": _property("string", "Full tool-use description."),
                        "category": _property("string", "Manifest capability category."),
                        "risk": _property("string", "Normalized read, write, or destructive risk."),
                        "tier": _property("string", "Contract tier."),
                        "score": _property("integer", "Deterministic text-match score."),
                    },
                    "required": [
                        "serviceId",
                        "toolName",
                        "title",
                        "summary",
                        "category",
                        "risk",
                        "tier",
                        "score",
                    ],
                    "additionalProperties": False,
                },
            ),
        }
        required = ["query", "count", "results"]
    elif name == "job_status":
        properties = {
            "status": _property("string", "Provider job status."),
            "state": _property(
                "string",
                "Normalized queued, running, success, error, or unknown state.",
            ),
            "progress": _nullable_property("integer", "Progress percent when known."),
            "progress_pct": _nullable_property("integer", "Compatibility alias for progress."),
            "output_asset_ids": _nullable_property(
                "array", "Completed output asset IDs.", items={"type": "string"}
            ),
            "report": _nullable_property(
                "object", "Optional analysis report.", additionalProperties=True
            ),
            "ranking": _nullable_property(
                "array",
                "Optional comparison ranking.",
                items={"type": "object", "additionalProperties": True},
            ),
            "result": _nullable_property(
                "object", "Optional workflow or iteration result.", additionalProperties=True
            ),
            "qa": _object_property("Normalized QA outcome."),
            "error": _nullable_property("string", "Safe processing error summary when failed."),
            "logs_short": _nullable_property("string", "Bounded sanitized job log summary."),
            "last_log_line": _nullable_property("string", "Last non-empty sanitized log line."),
            "error_code": _nullable_property("string", "Normalized recoverable error category."),
            "started_at": _nullable_property("string", "UTC start timestamp when known."),
            "finished_at": _nullable_property("string", "UTC completion timestamp when known."),
            "cache_hit": _nullable_property("boolean", "Whether the job result came from cache."),
        }
        required = ["status", "state"]
    elif name == "job_progress":
        properties = {
            "job_id": _property("string", "Requested job ID."),
            "status": _property("string", "Provider job status."),
            "progress_pct": _nullable_property("integer", "Progress percent when known."),
            "phase": _property("string", "Current bounded processing phase when known."),
        }
        required = ["job_id", "status"]
    elif name == "job_logs":
        properties = {
            "job_id": _property("string", "Requested job ID."),
            "status": _property("string", "Provider job status."),
            "logs_short": _nullable_property("string", "Bounded sanitized job log summary."),
            "last_log_line": _nullable_property("string", "Last non-empty sanitized log line."),
            "error": _nullable_property("string", "Safe error summary when the job failed."),
        }
        required = ["job_id", "status"]
    elif name == "metrics_snapshot":
        properties = {
            "queue_depth": _object_property("Current queue depth keyed by configured queue name.")
        }
        required = ["queue_depth"]
        additional = True
    elif name == "media_get_download_url":
        properties = {
            "url": _property("string", "Time-limited signed public download URL."),
            "expires_at": _property("integer", "Unix timestamp at which the URL expires."),
        }
        required = ["url", "expires_at"]
    elif name == "media_export_to_drive":
        properties = {"drive_file_id": _property("string", "Created Google Drive file ID.")}
        required = ["drive_file_id"]
    elif name == "media_export_to_discord":
        properties = {"message_id": _property("string", "Created Discord message ID.")}
        required = ["message_id"]
    else:
        raise ValueError(f"Tool metadata has no output schema: {name}")
    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": additional,
    }
    if required:
        schema["required"] = required
    return schema


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _build_sha() -> str:
    value = os.getenv("MCP_BUILD_SHA", "").strip()
    if value and re.fullmatch(r"[A-Za-z0-9._-]{7,128}", value):
        return value
    return "unknown"


def _descriptor(name: str, function: Callable[..., Any]) -> dict[str, Any]:
    confirmation_phrase = CONFIRMATION_PHRASES.get(name)
    descriptor: dict[str, Any] = {
        "serviceId": SERVICE_ID,
        "nativeToolName": name,
        "canonicalName": f"{SERVICE_ID}.{name}",
        "aliases": list(ALIASES.get(name, ())),
        "title": _title(name),
        "description": TOOL_DESCRIPTIONS[name],
        "category": _category_for(name),
        "deprecation": {
            "deprecated": False,
            "since": None,
            "replacement": None,
            "sunsetAt": None,
            "message": None,
        },
        "inputSchema": _input_schema(name, function),
        "outputSchema": _output_schema(name),
        "annotations": {
            "readOnlyHint": name in READ_ONLY_TOOLS,
            "destructiveHint": name in DESTRUCTIVE_TOOLS,
            "openWorldHint": name in OPEN_WORLD_TOOLS,
            "idempotentHint": name in IDEMPOTENT_TOOLS,
        },
        "confirmation": {
            "required": confirmation_phrase is not None,
            "parameter": "confirmation" if confirmation_phrase is not None else None,
            "exactPhrase": confirmation_phrase,
            "when": (
                "Supply this exact phrase as the native confirmation argument at execution time."
                if confirmation_phrase is not None
                else None
            ),
        },
        "documentationUrl": f"{DOCUMENTATION_URL}#tool-catalog",
        "navigationRole": NAVIGATION_ROLES.get(name),
        "catalogVersion": CATALOG_VERSION,
        "tier": "agent_ready",
    }
    descriptor["descriptorHash"] = _sha256(descriptor)
    return descriptor


def build_tool_manifest(
    tool_functions: Mapping[str, Callable[..., Any]],
) -> dict[str, Any]:
    """Return the complete ordered manifest and fail closed on registry drift."""

    registry_names = set(tool_functions)
    metadata_names = set(TOOL_DESCRIPTIONS)
    if registry_names != metadata_names:
        missing = sorted(registry_names - metadata_names)
        stale = sorted(metadata_names - registry_names)
        raise ValueError(f"ToolManifest registry drift; missing={missing}, stale={stale}")
    descriptors = [_descriptor(name, tool_functions[name]) for name in sorted(tool_functions)]
    counts = {
        "raw": len(descriptors),
        "agentReady": sum(tool["tier"] == "agent_ready" for tool in descriptors),
        "legacy": sum(tool["tier"] == "legacy" for tool in descriptors),
        "hidden": sum(tool["tier"] == "hidden" for tool in descriptors),
    }
    return {
        "schemaVersion": SCHEMA_VERSION,
        "serviceId": SERVICE_ID,
        "catalogVersion": CATALOG_VERSION,
        "buildSha": _build_sha(),
        "descriptorHash": _sha256(descriptors),
        "counts": counts,
        "tools": descriptors,
    }


def resolve_tool_descriptor(manifest: Mapping[str, Any], requested_name: str) -> dict[str, Any]:
    cleaned = requested_name.strip().lower()
    for descriptor in manifest.get("tools", []):
        names = [
            descriptor["nativeToolName"],
            descriptor["canonicalName"],
            *descriptor.get("aliases", []),
        ]
        if cleaned in {str(name).lower() for name in names}:
            return dict(descriptor)
    raise ValueError(f"Unknown FFmpeg tool or alias: {requested_name}")


def _tokens(value: str) -> list[str]:
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    return [part for part in re.sub(r"[^a-z0-9]+", " ", normalized.lower()).split() if part]


def search_tool_manifest(
    manifest: Mapping[str, Any],
    query: str,
    *,
    category: str | None = None,
    risk: str | None = None,
    tier: str | None = "agent_ready",
    limit: int = 8,
) -> list[dict[str, Any]]:
    """Rank matching descriptors without vector search or external state."""

    query_tokens = _tokens(query)
    if not query_tokens:
        raise ValueError("query must include at least one letter or number")
    risk_value = risk.strip().lower() if risk else None
    if risk_value not in {None, "read", "write", "destructive"}:
        raise ValueError("risk must be one of: read, write, destructive")
    if tier not in {None, "agent_ready", "legacy", "hidden"}:
        raise ValueError("tier must be one of: agent_ready, legacy, hidden")
    bounded_limit = max(1, min(int(limit), 25))
    matches: list[tuple[int, str, dict[str, Any]]] = []
    for descriptor in manifest.get("tools", []):
        if tier and descriptor.get("tier") != tier:
            continue
        if category and descriptor.get("category") != category:
            continue
        annotations = descriptor["annotations"]
        descriptor_risk = (
            "destructive"
            if annotations["destructiveHint"]
            else "read"
            if annotations["readOnlyHint"]
            else "write"
        )
        if risk_value and descriptor_risk != risk_value:
            continue
        name_tokens = _tokens(descriptor["nativeToolName"])
        alias_tokens = _tokens(" ".join(descriptor.get("aliases", [])))
        title_tokens = _tokens(descriptor["title"])
        description_tokens = _tokens(descriptor["description"])
        searchable = set(name_tokens + alias_tokens + title_tokens + description_tokens)
        if not all(token in searchable for token in query_tokens):
            continue
        score = 0
        for token in query_tokens:
            if token in name_tokens:
                score += 12
            if token in alias_tokens:
                score += 8
            if token in title_tokens:
                score += 5
            if token in description_tokens:
                score += 2
        exact_normalized = " ".join(query_tokens)
        if exact_normalized == " ".join(name_tokens):
            score += 50
        result = {
            "serviceId": SERVICE_ID,
            "toolName": descriptor["nativeToolName"],
            "title": descriptor["title"],
            "summary": descriptor["description"],
            "category": descriptor["category"],
            "risk": descriptor_risk,
            "tier": descriptor["tier"],
            "score": score,
        }
        matches.append((score, descriptor["nativeToolName"], result))
    matches.sort(key=lambda item: (-item[0], item[1]))
    return [item[2] for item in matches[:bounded_limit]]


def capability_groups(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return compact categories while leaving full descriptors opt-in."""

    grouped: dict[str, list[dict[str, Any]]] = {}
    for descriptor in manifest.get("tools", []):
        annotations = descriptor["annotations"]
        risk = (
            "destructive"
            if annotations["destructiveHint"]
            else "read"
            if annotations["readOnlyHint"]
            else "write"
        )
        grouped.setdefault(descriptor["category"], []).append(
            {
                "toolName": descriptor["nativeToolName"],
                "title": descriptor["title"],
                "risk": risk,
                "tier": descriptor["tier"],
            }
        )
    return [{"category": category, "tools": grouped[category]} for category in sorted(grouped)]
