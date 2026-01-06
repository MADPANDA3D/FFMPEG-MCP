from typing import Any

from overlay_utils import (
    DEFAULT_LOGO_OPACITY,
    DEFAULT_LOGO_POSITION,
    DEFAULT_LOGO_SCALE_PCT,
    DEFAULT_TEXT_POSITION,
    LOGO_POSITIONS,
    TEXT_POSITIONS,
    sanitize_font_size,
    sanitize_color,
    sanitize_opacity,
    sanitize_position,
    sanitize_scale_pct,
)


ALLOWED_KEYS = {
    "brand_kit_id",
    "name",
    "logo_asset_id",
    "logo_key",
    "logo_position",
    "logo_scale_pct",
    "logo_opacity",
    "font_name",
    "font_asset_id",
    "font_color",
    "box_color",
    "background_box",
    "text_position",
    "default_preset",
    "auto_logo",
    "caption_font_name",
    "caption_font_asset_id",
    "caption_font_size",
    "caption_position",
    "caption_text_color",
    "caption_box_color",
    "caption_box_opacity",
    "caption_highlight_color",
    "caption_padding_px",
    "caption_max_chars",
    "caption_max_lines",
    "caption_max_words",
    "caption_safe_zone_bottom_px",
    "caption_safe_zone_top_px",
}


def sanitize_brand_kit(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("brand kit payload must be an object")
    unknown = set(payload.keys()) - ALLOWED_KEYS
    if unknown:
        raise ValueError(f"Unknown brand kit fields: {', '.join(sorted(unknown))}")
    kit_id = (payload.get("brand_kit_id") or "").strip()
    if not kit_id:
        raise ValueError("brand_kit_id is required")

    cleaned: dict[str, Any] = {"brand_kit_id": kit_id}
    if payload.get("name"):
        cleaned["name"] = str(payload["name"]).strip()

    if payload.get("logo_asset_id"):
        cleaned["logo_asset_id"] = str(payload["logo_asset_id"]).strip()
    if payload.get("logo_key"):
        cleaned["logo_key"] = str(payload["logo_key"]).strip()

    if payload.get("logo_position"):
        cleaned["logo_position"] = sanitize_position(
            payload["logo_position"], LOGO_POSITIONS
        )
    else:
        cleaned["logo_position"] = DEFAULT_LOGO_POSITION

    if payload.get("logo_scale_pct") is not None:
        cleaned["logo_scale_pct"] = sanitize_scale_pct(
            payload["logo_scale_pct"], DEFAULT_LOGO_SCALE_PCT
        )
    else:
        cleaned["logo_scale_pct"] = DEFAULT_LOGO_SCALE_PCT

    if payload.get("logo_opacity") is not None:
        cleaned["logo_opacity"] = sanitize_opacity(
            payload["logo_opacity"], DEFAULT_LOGO_OPACITY
        )
    else:
        cleaned["logo_opacity"] = DEFAULT_LOGO_OPACITY

    if payload.get("font_name"):
        cleaned["font_name"] = str(payload["font_name"]).strip()
    if payload.get("font_asset_id"):
        cleaned["font_asset_id"] = str(payload["font_asset_id"]).strip()
    if payload.get("font_color"):
        cleaned["font_color"] = sanitize_color(payload["font_color"], "white")
    if payload.get("box_color"):
        cleaned["box_color"] = sanitize_color(payload["box_color"], "black@0.6")
    if payload.get("background_box") is not None:
        cleaned["background_box"] = bool(payload["background_box"])

    if payload.get("text_position"):
        cleaned["text_position"] = sanitize_position(
            payload["text_position"], TEXT_POSITIONS
        )
    else:
        cleaned["text_position"] = DEFAULT_TEXT_POSITION

    if payload.get("default_preset"):
        cleaned["default_preset"] = str(payload["default_preset"]).strip()
    if payload.get("auto_logo") is not None:
        cleaned["auto_logo"] = bool(payload["auto_logo"])
    else:
        cleaned["auto_logo"] = False

    if payload.get("caption_font_name"):
        cleaned["caption_font_name"] = str(payload["caption_font_name"]).strip()
    if payload.get("caption_font_asset_id"):
        cleaned["caption_font_asset_id"] = str(payload["caption_font_asset_id"]).strip()
    if payload.get("caption_font_size") is not None:
        cleaned["caption_font_size"] = sanitize_font_size(payload["caption_font_size"], 48)
    if payload.get("caption_position"):
        cleaned["caption_position"] = sanitize_position(
            payload["caption_position"], {"bottom_safe", "mid", "top"}
        )
    if payload.get("caption_text_color"):
        cleaned["caption_text_color"] = sanitize_color(payload["caption_text_color"], "white")
    if payload.get("caption_box_color"):
        cleaned["caption_box_color"] = sanitize_color(payload["caption_box_color"], "black")
    if payload.get("caption_box_opacity") is not None:
        try:
            opacity = float(payload["caption_box_opacity"])
        except (TypeError, ValueError):
            raise ValueError("caption_box_opacity must be a number") from None
        if opacity < 0 or opacity > 1:
            raise ValueError("caption_box_opacity must be between 0 and 1")
        cleaned["caption_box_opacity"] = opacity
    if payload.get("caption_highlight_color"):
        cleaned["caption_highlight_color"] = sanitize_color(payload["caption_highlight_color"], "yellow")
    if payload.get("caption_padding_px") is not None:
        try:
            padding = int(payload["caption_padding_px"])
        except (TypeError, ValueError):
            raise ValueError("caption_padding_px must be an integer") from None
        if padding < 0 or padding > 200:
            raise ValueError("caption_padding_px out of range")
        cleaned["caption_padding_px"] = padding
    if payload.get("caption_max_chars") is not None:
        try:
            max_chars = int(payload["caption_max_chars"])
        except (TypeError, ValueError):
            raise ValueError("caption_max_chars must be an integer") from None
        if max_chars <= 0 or max_chars > 200:
            raise ValueError("caption_max_chars out of range")
        cleaned["caption_max_chars"] = max_chars
    if payload.get("caption_max_lines") is not None:
        try:
            max_lines = int(payload["caption_max_lines"])
        except (TypeError, ValueError):
            raise ValueError("caption_max_lines must be an integer") from None
        if max_lines <= 0 or max_lines > 6:
            raise ValueError("caption_max_lines out of range")
        cleaned["caption_max_lines"] = max_lines
    if payload.get("caption_max_words") is not None:
        try:
            max_words = int(payload["caption_max_words"])
        except (TypeError, ValueError):
            raise ValueError("caption_max_words must be an integer") from None
        if max_words <= 0 or max_words > 30:
            raise ValueError("caption_max_words out of range")
        cleaned["caption_max_words"] = max_words
    if payload.get("caption_safe_zone_bottom_px") is not None:
        try:
            bottom_px = int(payload["caption_safe_zone_bottom_px"])
        except (TypeError, ValueError):
            raise ValueError("caption_safe_zone_bottom_px must be an integer") from None
        if bottom_px < 0 or bottom_px > 400:
            raise ValueError("caption_safe_zone_bottom_px out of range")
        cleaned["caption_safe_zone_bottom_px"] = bottom_px
    if payload.get("caption_safe_zone_top_px") is not None:
        try:
            top_px = int(payload["caption_safe_zone_top_px"])
        except (TypeError, ValueError):
            raise ValueError("caption_safe_zone_top_px must be an integer") from None
        if top_px < 0 or top_px > 400:
            raise ValueError("caption_safe_zone_top_px out of range")
        cleaned["caption_safe_zone_top_px"] = top_px

    return cleaned
