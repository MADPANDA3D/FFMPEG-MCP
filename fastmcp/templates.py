from typing import Any


TEMPLATES = {
    "promo_vertical_basic": {
        "purpose": "9:16 promo with headline, price, CTA, and optional logo.",
        "defaults": {
            "headline": "Your Headline",
            "cta": "Shop Now",
            "price": "$29",
        },
        "schema": {
            "fields": [
                {
                    "name": "headline",
                    "type": "string",
                    "required": True,
                    "max_chars": 40,
                    "default": "Your Headline",
                    "description": "Primary headline text.",
                    "font_size": 72,
                },
                {
                    "name": "price",
                    "type": "string",
                    "required": False,
                    "max_chars": 16,
                    "default": "$29",
                    "description": "Price or offer highlight.",
                    "font_size": 56,
                },
                {
                    "name": "cta",
                    "type": "string",
                    "required": False,
                    "max_chars": 24,
                    "default": "Shop Now",
                    "description": "Short call-to-action.",
                    "font_size": 48,
                },
            ]
        },
        "include_brand_logo": True,
        "layers": [
            {"type": "transcode", "preset": "mp4_social_vertical_1080x1920_safe_pad"},
            {
                "type": "text",
                "text": "{headline}",
                "position": "center",
                "font_size": 72,
                "background_box": True,
                "box_color": "black@0.5",
            },
            {
                "type": "text",
                "text": "{price}",
                "position": "bottom",
                "font_size": 56,
                "background_box": True,
                "box_color": "black@0.55",
            },
            {
                "type": "text",
                "text": "{cta}",
                "position": "top",
                "font_size": 48,
                "background_box": True,
                "box_color": "black@0.55",
            },
        ],
    },
    "square_product_card": {
        "purpose": "1:1 product card with centered headline + price.",
        "defaults": {
            "headline": "Product Name",
            "price": "$29",
        },
        "schema": {
            "fields": [
                {
                    "name": "headline",
                    "type": "string",
                    "required": True,
                    "max_chars": 40,
                    "default": "Product Name",
                    "description": "Product or offer headline.",
                    "font_size": 68,
                },
                {
                    "name": "price",
                    "type": "string",
                    "required": False,
                    "max_chars": 16,
                    "default": "$29",
                    "description": "Price or offer highlight.",
                    "font_size": 52,
                },
            ]
        },
        "include_brand_logo": True,
        "layers": [
            {"type": "transcode", "preset": "mp4_social_square_1080x1080_safe_pad"},
            {
                "type": "text",
                "text": "{headline}",
                "position": "center",
                "font_size": 68,
                "background_box": True,
                "box_color": "black@0.5",
            },
            {
                "type": "text",
                "text": "{price}",
                "position": "bottom",
                "font_size": 52,
                "background_box": True,
                "box_color": "black@0.55",
            },
        ],
    },
    "youtube_title_card": {
        "purpose": "16:9 title card with headline and optional logo.",
        "defaults": {
            "headline": "Episode Title",
        },
        "schema": {
            "fields": [
                {
                    "name": "headline",
                    "type": "string",
                    "required": True,
                    "max_chars": 60,
                    "default": "Episode Title",
                    "description": "YouTube title text.",
                    "font_size": 72,
                }
            ]
        },
        "include_brand_logo": True,
        "layers": [
            {"type": "transcode", "preset": "mp4_youtube_1920x1080"},
            {
                "type": "text",
                "text": "{headline}",
                "position": "center",
                "font_size": 72,
                "background_box": True,
                "box_color": "black@0.5",
            },
        ],
    },
    "social_ad_basic": {
        "purpose": "Social ad layout with hook, headline, and CTA line.",
        "defaults": {
            "preset": "mp4_social_vertical_1080x1920_safe_pad",
            "hook": "Your Hook",
            "headline": "Your Headline",
            "cta": "Shop Now",
            "price": "$29",
        },
        "schema": {
            "fields": [
                {
                    "name": "hook",
                    "type": "string",
                    "required": False,
                    "max_chars": 36,
                    "default": "Your Hook",
                    "description": "Short hook line.",
                    "font_size": 60,
                },
                {
                    "name": "headline",
                    "type": "string",
                    "required": True,
                    "max_chars": 44,
                    "default": "Your Headline",
                    "description": "Primary headline text.",
                    "font_size": 72,
                },
                {
                    "name": "cta",
                    "type": "string",
                    "required": False,
                    "max_chars": 24,
                    "default": "Shop Now",
                    "description": "Short call-to-action.",
                    "font_size": 52,
                },
                {
                    "name": "price",
                    "type": "string",
                    "required": False,
                    "max_chars": 16,
                    "default": "$29",
                    "description": "Price or offer highlight.",
                    "font_size": 52,
                },
            ]
        },
        "internal_variables": ["preset"],
        "include_brand_logo": True,
        "layers": [
            {"type": "transcode", "preset": "{preset}"},
            {
                "type": "text",
                "text": "{hook}",
                "position": "top",
                "font_size": 60,
                "background_box": True,
                "box_color": "black@0.5",
                "optional": True,
            },
            {
                "type": "text",
                "text": "{headline}",
                "position": "center",
                "font_size": 72,
                "background_box": True,
                "box_color": "black@0.5",
            },
            {
                "type": "text",
                "text": "{cta} {price}",
                "position": "bottom",
                "font_size": 52,
                "background_box": True,
                "box_color": "black@0.55",
            },
        ],
    },
    "testimonial_clip_basic": {
        "purpose": "Testimonial clip with quote and author line.",
        "defaults": {
            "preset": "mp4_social_vertical_1080x1920_safe_pad",
            "quote": "Best purchase I've made.",
            "author": "Happy Customer",
        },
        "schema": {
            "fields": [
                {
                    "name": "quote",
                    "type": "string",
                    "required": True,
                    "max_chars": 90,
                    "default": "Best purchase I've made.",
                    "description": "Testimonial quote text.",
                    "font_size": 62,
                },
                {
                    "name": "author",
                    "type": "string",
                    "required": False,
                    "max_chars": 40,
                    "default": "Happy Customer",
                    "description": "Attribution or author name.",
                    "font_size": 48,
                },
            ]
        },
        "internal_variables": ["preset"],
        "include_brand_logo": True,
        "layers": [
            {"type": "transcode", "preset": "{preset}"},
            {
                "type": "text",
                "text": "\"{quote}\"",
                "position": "center",
                "font_size": 62,
                "background_box": True,
                "box_color": "black@0.45",
            },
            {
                "type": "text",
                "text": "- {author}",
                "position": "bottom",
                "font_size": 48,
                "background_box": True,
                "box_color": "black@0.5",
                "optional": True,
            },
        ],
    },
    "offer_card_basic": {
        "purpose": "Offer card with headline, price, and CTA.",
        "defaults": {
            "preset": "mp4_social_vertical_1080x1920_safe_pad",
            "headline": "Limited Offer",
            "price": "$29",
            "cta": "Shop Now",
        },
        "schema": {
            "fields": [
                {
                    "name": "headline",
                    "type": "string",
                    "required": True,
                    "max_chars": 44,
                    "default": "Limited Offer",
                    "description": "Offer headline text.",
                    "font_size": 64,
                },
                {
                    "name": "price",
                    "type": "string",
                    "required": False,
                    "max_chars": 16,
                    "default": "$29",
                    "description": "Price or discount line.",
                    "font_size": 64,
                },
                {
                    "name": "cta",
                    "type": "string",
                    "required": False,
                    "max_chars": 24,
                    "default": "Shop Now",
                    "description": "Short call-to-action.",
                    "font_size": 48,
                },
            ]
        },
        "internal_variables": ["preset"],
        "include_brand_logo": True,
        "layers": [
            {"type": "transcode", "preset": "{preset}"},
            {
                "type": "text",
                "text": "{headline}",
                "position": "top",
                "font_size": 64,
                "background_box": True,
                "box_color": "black@0.5",
            },
            {
                "type": "text",
                "text": "{price}",
                "position": "center",
                "font_size": 64,
                "background_box": True,
                "box_color": "black@0.5",
            },
            {
                "type": "text",
                "text": "{cta}",
                "position": "bottom",
                "font_size": 48,
                "background_box": True,
                "box_color": "black@0.55",
            },
        ],
    },
}


def _schema_fields(template: dict[str, Any]) -> list[dict[str, Any]]:
    schema = template.get("schema") or {}
    fields = schema.get("fields") or []
    return [dict(field) for field in fields if isinstance(field, dict)]


def _build_defaults(template: dict[str, Any]) -> dict[str, Any]:
    defaults = dict(template.get("defaults", {}))
    for field in _schema_fields(template):
        name = field.get("name")
        if not name or name in defaults:
            continue
        if "default" in field:
            defaults[name] = field.get("default")
    return defaults


def get_template(name: str) -> dict[str, Any]:
    template = TEMPLATES.get(name)
    if not template:
        raise ValueError(f"Unknown template: {name}")
    return template


def validate_template_variables(template: dict[str, Any], variables: dict[str, Any] | None) -> dict[str, Any]:
    vars_in = variables or {}
    if not isinstance(vars_in, dict):
        raise ValueError("variables must be an object")

    fields = _schema_fields(template)
    internal_vars = set(template.get("internal_variables") or [])
    allowed = {field.get("name") for field in fields if field.get("name")} | internal_vars
    unknown = set(vars_in.keys()) - allowed
    if unknown:
        raise ValueError(f"Unknown template variables: {', '.join(sorted(unknown))}")

    merged = _build_defaults(template)
    for key, value in vars_in.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        merged[key] = value

    for field in fields:
        name = field.get("name")
        if not name:
            continue
        required = bool(field.get("required"))
        value = merged.get(name)
        if value is None or (isinstance(value, str) and not value.strip()):
            if required:
                raise ValueError(f"{name} is required")
            continue

        field_type = (field.get("type") or "string").lower()
        if field_type == "string":
            text = str(value).strip()
            if not text:
                if required:
                    raise ValueError(f"{name} is required")
                continue
            max_chars = field.get("max_chars")
            if max_chars is not None:
                try:
                    max_chars = int(max_chars)
                except (TypeError, ValueError):
                    max_chars = None
            if max_chars and len(text) > max_chars:
                font_size = field.get("font_size")
                if font_size:
                    raise ValueError(
                        f"{name} too long for safe zone (max {max_chars} chars at font size {font_size})"
                    )
                raise ValueError(f"{name} exceeds max length ({max_chars})")
            merged[name] = text
        elif field_type == "number":
            try:
                num = float(value)
            except (TypeError, ValueError):
                raise ValueError(f"{name} must be a number") from None
            min_value = field.get("min")
            max_value = field.get("max")
            if min_value is not None and num < float(min_value):
                raise ValueError(f"{name} must be >= {min_value}")
            if max_value is not None and num > float(max_value):
                raise ValueError(f"{name} must be <= {max_value}")
            merged[name] = num
        elif field_type == "boolean":
            merged[name] = bool(value)
        else:
            raise ValueError(f"Unsupported schema type for {name}")

    return merged


def describe_template(name: str) -> dict[str, Any]:
    template = get_template(name)
    fields = _schema_fields(template)
    required_fields = [field.get("name") for field in fields if field.get("required")]
    return {
        "name": name,
        "purpose": template.get("purpose"),
        "defaults": _build_defaults(template),
        "schema": {
            "fields": fields,
            "required": [name for name in required_fields if name],
        },
        "layers": template.get("layers", []),
        "include_brand_logo": bool(template.get("include_brand_logo")),
    }


def list_templates() -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "purpose": template.get("purpose"),
            "layer_count": len(template.get("layers", [])),
        }
        for name, template in TEMPLATES.items()
    ]
