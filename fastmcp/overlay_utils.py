import os
import re
from typing import Iterable

from config import settings
from redis_store import get_asset
from storage import download_to_temp, local_path_from_key


CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")
ALLOWED_COLOR_NAMES = {
    "white",
    "black",
    "red",
    "green",
    "blue",
    "yellow",
    "orange",
    "purple",
    "pink",
    "gray",
    "grey",
}
ALLOWED_FONT_EXTS = {".ttf", ".otf", ".ttc", ".woff", ".woff2"}
ALLOWED_LOGO_EXTS = {".png", ".jpg", ".jpeg", ".webp"}
TEXT_POSITIONS = {"top", "center", "bottom"}
LOGO_POSITIONS = {"top-left", "top-right", "bottom-left", "bottom-right"}

DEFAULT_TEXT_POSITION = "bottom"
DEFAULT_FONT_SIZE = 48
DEFAULT_FONT_COLOR = "white"
DEFAULT_BOX_COLOR = "black@0.6"
DEFAULT_BOX_BORDER_WIDTH = 24
DEFAULT_LOGO_POSITION = "bottom-right"
DEFAULT_LOGO_SCALE_PCT = 15
DEFAULT_LOGO_OPACITY = 1.0


def sanitize_text(text: str) -> str:
    if not isinstance(text, str):
        raise ValueError("text must be a string")
    cleaned = text.replace("\r\n", "\n").replace("\r", "\n").replace("\t", " ")
    cleaned = CONTROL_CHAR_RE.sub("", cleaned).strip()
    if not cleaned:
        raise ValueError("text is empty after sanitization")
    if len(cleaned) > settings.max_text_chars:
        raise ValueError(f"text exceeds max length ({settings.max_text_chars})")
    return cleaned


def sanitize_position(value: str, allowed: Iterable[str]) -> str:
    if not value:
        raise ValueError("position is required")
    value = value.strip().lower()
    allowed_set = {item.lower() for item in allowed}
    if value not in allowed_set:
        raise ValueError(f"invalid position (allowed: {', '.join(sorted(allowed_set))})")
    return value


def sanitize_font_size(value: int | None, default: int) -> int:
    if value is None:
        return default
    try:
        size = int(value)
    except (TypeError, ValueError):
        raise ValueError("font_size must be an integer") from None
    if size < settings.min_font_size or size > settings.max_font_size:
        raise ValueError(
            f"font_size must be between {settings.min_font_size} and {settings.max_font_size}"
        )
    return size


def sanitize_box_border(value: int | None, default: int) -> int:
    if value is None:
        return default
    try:
        width = int(value)
    except (TypeError, ValueError):
        raise ValueError("box_border_width must be an integer") from None
    if width < 0 or width > settings.max_box_border_width:
        raise ValueError(
            f"box_border_width must be between 0 and {settings.max_box_border_width}"
        )
    return width


def sanitize_scale_pct(value: int | None, default: int) -> int:
    if value is None:
        return default
    try:
        pct = int(value)
    except (TypeError, ValueError):
        raise ValueError("scale_pct must be an integer") from None
    if pct < settings.logo_min_scale_pct or pct > settings.logo_max_scale_pct:
        raise ValueError(
            f"scale_pct must be between {settings.logo_min_scale_pct} and {settings.logo_max_scale_pct}"
        )
    return pct


def sanitize_opacity(value: float | None, default: float) -> float:
    if value is None:
        return default
    try:
        opacity = float(value)
    except (TypeError, ValueError):
        raise ValueError("opacity must be a number") from None
    if opacity < 0 or opacity > settings.logo_max_opacity:
        raise ValueError(f"opacity must be between 0 and {settings.logo_max_opacity}")
    return opacity


def sanitize_color(value: str | None, default: str) -> str:
    if not value:
        return default
    raw = value.strip().lower()
    if any(char in raw for char in [":", ","]):
        raise ValueError("invalid color value")
    base = raw
    alpha_part = None
    if "@" in raw:
        base, alpha_part = raw.split("@", 1)
    if base.startswith("#"):
        hex_part = base[1:]
    elif base.startswith("0x"):
        hex_part = base[2:]
    else:
        hex_part = ""
    if hex_part:
        if len(hex_part) != 6 or any(char not in "0123456789abcdef" for char in hex_part):
            raise ValueError("invalid hex color")
        base = f"#{hex_part}"
    elif base not in ALLOWED_COLOR_NAMES:
        raise ValueError("invalid color value")
    if alpha_part is not None:
        try:
            alpha = float(alpha_part)
        except ValueError:
            raise ValueError("invalid alpha value") from None
        if alpha < 0 or alpha > 1:
            raise ValueError("alpha must be between 0 and 1")
        alpha_str = f"{alpha:.3f}".rstrip("0").rstrip(".")
        base = f"{base}@{alpha_str}"
    return base


def escape_drawtext_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace(":", "\\:").replace(",", "\\,")


def _is_within_dir(path: str, base_dir: str) -> bool:
    try:
        base_dir = os.path.realpath(base_dir)
        path = os.path.realpath(path)
        return os.path.commonpath([path, base_dir]) == base_dir
    except (ValueError, OSError):
        return False


def _find_file_in_dirs(filename: str, dirs: Iterable[str]) -> str | None:
    for directory in dirs:
        if not directory:
            continue
        candidate = os.path.join(directory, filename)
        if not _is_within_dir(candidate, directory):
            continue
        if os.path.exists(candidate):
            return candidate
    return None


def _ensure_basename(name: str) -> str:
    name = name.strip()
    if not name:
        raise ValueError("filename is required")
    if os.path.basename(name) != name:
        raise ValueError("invalid filename")
    if ".." in name or os.path.sep in name or (os.path.altsep and os.path.altsep in name):
        raise ValueError("invalid filename")
    return name


def resolve_font_path(font_name: str | None, font_asset_id: str | None) -> tuple[str, bool]:
    if font_asset_id:
        asset = get_asset(font_asset_id)
        if not asset:
            raise ValueError("font_asset_id not found")
        storage_key = asset.get("storage_key")
        if not storage_key:
            raise ValueError("font asset storage missing")
        _, ext = os.path.splitext(storage_key)
        if ext.lower() not in ALLOWED_FONT_EXTS:
            raise ValueError("unsupported font file extension")
        if settings.storage_backend == "s3":
            path = download_to_temp(storage_key)
            return path, True
        path = local_path_from_key(storage_key)
        if not os.path.exists(path):
            raise ValueError("font asset file missing")
        return path, False

    if not font_name:
        font_name = settings.font_default
    if os.path.isabs(font_name):
        if not any(_is_within_dir(font_name, directory) for directory in settings.font_dirs):
            raise ValueError("font path not allowed")
        if settings.font_allowlist and os.path.basename(font_name) not in settings.font_allowlist:
            raise ValueError("font is not allowlisted")
        if not os.path.exists(font_name):
            raise ValueError("font file not found")
        _, ext = os.path.splitext(font_name)
        if ext.lower() not in ALLOWED_FONT_EXTS:
            raise ValueError("unsupported font file extension")
        return font_name, False

    filename = _ensure_basename(font_name)
    if settings.font_allowlist and filename not in settings.font_allowlist:
        raise ValueError("font is not allowlisted")
    resolved = _find_file_in_dirs(filename, settings.font_dirs)
    if not resolved:
        raise ValueError("font file not found in font dirs")
    _, ext = os.path.splitext(resolved)
    if ext.lower() not in ALLOWED_FONT_EXTS:
        raise ValueError("unsupported font file extension")
    return resolved, False


def resolve_logo_path(
    logo_asset_id: str | None, logo_key: str | None
) -> tuple[str, bool]:
    if logo_asset_id:
        asset = get_asset(logo_asset_id)
        if not asset:
            raise ValueError("logo_asset_id not found")
        mime_type = (asset.get("mime_type") or "").lower()
        if not mime_type.startswith("image/"):
            raise ValueError("logo_asset_id must reference an image asset")
        storage_key = asset.get("storage_key")
        if not storage_key:
            raise ValueError("logo asset storage missing")
        _, ext = os.path.splitext(storage_key)
        if ext and ext.lower() not in ALLOWED_LOGO_EXTS:
            raise ValueError("unsupported logo file extension")
        if settings.storage_backend == "s3":
            path = download_to_temp(storage_key)
            return path, True
        path = local_path_from_key(storage_key)
        if not os.path.exists(path):
            raise ValueError("logo asset file missing")
        return path, False

    if not logo_key:
        raise ValueError("logo_asset_id or logo_key is required")
    filename = _ensure_basename(logo_key)
    if settings.logo_allowlist and filename not in settings.logo_allowlist:
        raise ValueError("logo key is not allowlisted")
    candidate = os.path.join(settings.logo_dir, filename)
    if not _is_within_dir(candidate, settings.logo_dir):
        raise ValueError("logo path not allowed")
    if not os.path.exists(candidate):
        raise ValueError("logo file not found")
    _, ext = os.path.splitext(candidate)
    if ext.lower() not in ALLOWED_LOGO_EXTS:
        raise ValueError("unsupported logo file extension")
    return candidate, False
