from typing import Any

from overlay_utils import (
    DEFAULT_LOGO_OPACITY,
    DEFAULT_LOGO_POSITION,
    DEFAULT_LOGO_SCALE_PCT,
    DEFAULT_TEXT_POSITION,
    LOGO_POSITIONS,
    TEXT_POSITIONS,
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

    return cleaned
