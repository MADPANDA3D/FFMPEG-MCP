TEMPLATES = {
    "promo_vertical_basic": {
        "purpose": "9:16 promo with headline, price, CTA, and optional logo.",
        "defaults": {
            "headline": "Your Headline",
            "cta": "Shop Now",
            "price": "$29",
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
}


def get_template(name: str) -> dict:
    template = TEMPLATES.get(name)
    if not template:
        raise ValueError(f"Unknown template: {name}")
    return template


def describe_template(name: str) -> dict:
    template = get_template(name)
    return {
        "name": name,
        "purpose": template.get("purpose"),
        "defaults": template.get("defaults", {}),
        "layers": template.get("layers", []),
        "include_brand_logo": bool(template.get("include_brand_logo")),
    }


def list_templates() -> list[dict]:
    return [
        {
            "name": name,
            "purpose": template.get("purpose"),
            "layer_count": len(template.get("layers", [])),
        }
        for name, template in TEMPLATES.items()
    ]
