import re
from typing import Any

from .config import settings

BRAND_KIT_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
RESERVED_BRAND_KIT_IDS = frozenset({"all"})


def validate_brand_kit_id(value: Any) -> str:
    """Return a storage-safe brand-kit identifier or fail with a stable message."""

    if not isinstance(value, str):
        raise ValueError("brand_kit_id is invalid")
    brand_kit_id = value.strip()
    if not BRAND_KIT_ID_PATTERN.fullmatch(brand_kit_id):
        raise ValueError("brand_kit_id is invalid")
    if brand_kit_id.casefold() in RESERVED_BRAND_KIT_IDS:
        raise ValueError("brand_kit_id is reserved")
    return brand_kit_id


def validate_brand_kit_record(record: dict[str, Any]) -> None:
    """Enforce final persistence bounds, including server-added timestamps."""

    validate_brand_kit_id(record.get("brand_kit_id"))

    def visit(value: Any) -> None:
        if isinstance(value, str):
            if len(value) > settings.brand_kit_max_string_chars:
                raise ValueError("brand kit string exceeds configured limit")
            return
        if isinstance(value, dict):
            for nested in value.values():
                visit(nested)
            return
        if isinstance(value, (list, tuple)):
            for nested in value:
                visit(nested)

    visit(record)
