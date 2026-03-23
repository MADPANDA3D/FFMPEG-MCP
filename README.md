<p align="center">
  <img src="./assets/brand/header.jpg" alt="FFmpeg MCP Server header" />
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="MIT License" /></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white" alt="Python 3.11+" /></a>
  <a href="https://modelcontextprotocol.io/"><img src="https://img.shields.io/badge/MCP-Server-000000" alt="MCP Server" /></a>
  <a href="https://ffmpeg.org/"><img src="https://img.shields.io/badge/FFmpeg-enabled-000000?logo=ffmpeg&logoColor=white" alt="FFmpeg" /></a>
  <a href="https://github.com/MADPANDA3D/FFMPEG-MCP/releases"><img src="https://img.shields.io/github/v/release/MADPANDA3D/FFMPEG-MCP?display_name=tag&color=0e8a16" alt="release" /></a>
  <a href="https://github.com/MADPANDA3D/FFMPEG-MCP/issues"><img src="https://img.shields.io/github/issues/MADPANDA3D/FFMPEG-MCP?color=ff8c00" alt="open issues" /></a>
  <a href="https://github.com/MADPANDA3D/FFMPEG-MCP"><img src="https://img.shields.io/github/stars/MADPANDA3D/FFMPEG-MCP?color=f1c40f" alt="stars" /></a>
</p>

<h1 align="center">FFmpeg MCP (FastMCP)</h1>

<p align="center">Preset-driven FFmpeg MCP server with async jobs, staging storage, and signed downloads.</p>

## Overview

FFMPEG MCP is a FastMCP service that runs FFmpeg jobs asynchronously with strict presets, safe ingest rules, and signed download URLs. It is designed for production workflows, marketing pipelines, and agent-driven media processing on a VPS.

## Status

- Stage: stable
- Maintainer: MADPANDA3D
- Support: GitHub issues

## Quick Start

```bash
cd fastmcp
cp .env.example .env

docker-compose -f fastmcp/docker-compose.yaml up -d --build
```

## Features

- FastMCP HTTP server + async worker (RQ + Redis)
- URL ingest with allowlist, magic-byte validation, size/duration caps
- Preset-only FFmpeg operations (no arbitrary flags)
- Marketing presets, overlays, captions, and templates
- Batch exports, campaign processing, and workflow chaining
- Signed download URLs (`/download/{asset_id}`)
- Optional exports to Drive and Discord
- `ffmpeg_capabilities` self-description endpoint

## Tech Stack

- Runtime: Python 3.11+
- Media: FFmpeg + FFprobe
- Queue: RQ + Redis
- MCP: FastMCP
- Storage: local or S3-compatible

## Configuration

Important defaults:
- `MCP_HTTP_PORT=8087`
- `PUBLIC_BASE_URL=https://ffmpeg-mcp.yourdomain.com`
- `DOWNLOAD_SIGNING_SECRET=...` (required for signed URLs)
- `STORAGE_BACKEND=local` (or `s3`)
- `ALLOWED_DOMAINS=cdn.discordapp.com,media.discordapp.net,googleusercontent.com,...`
- `MAX_INGEST_BYTES`, `MAX_OUTPUT_BYTES`, `MAX_DURATION_SECONDS`
- `FONT_DIRS`, `FONT_DEFAULT` for text overlays
- `LOGO_DIR` for logo overlays (`logo_key` values)
- `ALLOW_IMAGE_INGEST=true` for image tools
- `SOCIAL_PRESETS=...` for batch social exports
- `QUEUE_NAME_URGENT`, `QUEUE_NAME_BATCH` for priority queues

## Connect to n8n

If n8n runs in Docker on the same host:

```
http://ffmpeg-mcp:8087/mcp
```

External:

```
http://<vps-ip>:8087/mcp
```

## Deployment (Nginx Proxy Manager)

- Attach the container to `npm_default` (already in compose).
- Forward Hostname/IP: `ffmpeg-mcp`
- Forward Port: `8087`
- Websockets: ON
- HTTP/2: OFF
- Allow `/download/` paths (signed URL delivery).

## Tool Modes

Default tools:
- `MCP_TOOL_MODE=individual`

Single wrapper mode:
- `MCP_TOOL_MODE=router`
- Tool exposed: `FFMPEG_MCP`
- Call with `{ "tool": "<tool_name>", "arguments": { ... } }`

## Portal-only auth (production)

This deployment now supports portal-mediated access mode:
- `MCP_AUTH_MODE=portal_only`
- Required headers on `/mcp` requests:
  - `Content-Type: application/json`
  - `Accept: application/json, text/event-stream` (or at least `application/json`)
  - `X-MADPANDA-PORTAL-GRANT: <server-side-secret>`
  - `Authorization: Bearer <portal-signed-user-jwt>`
- Required JWT claims: `sub`, `exp`, `nbf`, and scope `mcp:ffmpeg`

Unauthorized requests return JSON-RPC errors with signup guidance:
- `https://madpanda3d.com/lab/mad-mcps`

## Tool Catalog

<details>
<summary>Tools (individual mode)</summary>

Ingest + storage:
- `media_ingest_from_url`
- `media_ingest_from_drive`
- `media_probe`
- `media_get_download_url`
- `media_export_to_drive`
- `media_export_to_discord`

Core video:
- `ffmpeg_transcode`
- `ffmpeg_thumbnail`
- `ffmpeg_trim`
- `video_add_text`
- `video_add_logo`
- `captions_burn_in`
- `video_concat`

Analysis + QA:
- `video_analyze`
- `asset_compare`
- `rubric_list`
- `rubric_describe`

Image to video:
- `image_to_video`
- `images_to_slideshow`
- `images_to_slideshow_ken_burns`

Audio:
- `ffmpeg_extract_audio`
- `audio_normalize`
- `audio_mix`
- `audio_duck`
- `audio_mix_with_background`
- `audio_fade`
- `audio_trim_silence`

Templates + brand:
- `template_list`
- `template_describe`
- `template_apply`
- `brand_kit_upsert`
- `brand_kit_get`
- `brand_kit_list`
- `brand_kit_delete`
- `brand_kit_apply`

Batch + workflow:
- `batch_export_formats`
- `batch_export_social_formats`
- `campaign_process`
- `render_social_ad`
- `render_testimonial_clip`
- `render_offer_card`
- `render_iterate`
- `workflow_run`

Meta:
- `ffmpeg_list_presets`
- `ffmpeg_describe_preset`
- `ffmpeg_capabilities`
- `job_status`
- `job_progress`
- `job_logs`
- `metrics_snapshot`

</details>

## Presets

<details>
<summary>Sample presets</summary>

- `mp4_web_720p_small`
- `mp4_social_vertical_1080x1920_safe_pad`
- `mp4_social_portrait_1080x1350`
- `mp4_youtube_1920x1080`
- `mp4_social_vertical_1080x1920_lower_third`
- `mp3_voice_128k_loudnorm`
- `mp3_voice_64k`
- `wav_pcm_16k_mono`
- `gif_preview_lowfps`

</details>

## Overlays

<details>
<summary>Text + logo overlays</summary>

Text overlay (`video_add_text`):
- Required: `asset_id`, `text`
- Optional: `position`, `font_size`, `font_color`, `background_box`, `box_color`,
  `box_border_width`, `font_name`, `font_asset_id`

Logo overlay (`video_add_logo`):
- Required: `asset_id`, `logo_asset_id` or `logo_key`
- Optional: `position`, `scale_pct`, `opacity`

Example (router mode):

```json
{
  "tool": "video_add_text",
  "arguments": {
    "asset_id": "ASSET_ID",
    "text": "Summer Sale",
    "position": "bottom",
    "background_box": true,
    "font_size": 56
  }
}
```

</details>

## Captions

<details>
<summary>Caption burn-in</summary>

- Required: `asset_id` plus one of `captions_srt`, `captions_vtt`, or `words_json`
- `words_json` shape: `[{ "word": "Hello", "start": 1.23, "end": 1.56 }]`
- Supports `brand_kit_id`, `highlight_mode`, `position`, `font_size`, `font_color`,
  `box_color`, `box_opacity`, `padding_px`, `safe_zone_profile` and more

</details>

## Marketing Render Tools

<details>
<summary>Render + iterate</summary>

One-shot renders:
- `render_social_ad`
- `render_testimonial_clip`
- `render_offer_card`
- `render_iterate`

Defaults:
- Variants: 9:16 + 1:1 + 4:5 (set `include_16_9=true` for 16:9)
- Inputs: `primary_asset_id` required; optional `broll_asset_ids`, `voice_asset_id`, `music_asset_id`
- Quality: `quality=draft|final`

</details>

## Analysis + QA

<details>
<summary>Rubrics and scoring</summary>

- `video_analyze` returns audio/video/caption metrics with optional rubric scoring.
- `asset_compare` ranks assets by rubric score.
- Rubrics include `social_reel_v1`, `testimonial_v1`, `insta_reel_v1`, `youtube_short_v1`.

</details>

## Templates + Brand Kits

<details>
<summary>Templates and branding</summary>

Example template apply:

```json
{
  "tool": "template_apply",
  "arguments": {
    "asset_id": "ASSET_ID",
    "template_name": "promo_vertical_basic",
    "variables": {
      "headline": "Summer Sale",
      "price": "$29",
      "cta": "Shop Now"
    },
    "brand_kit_id": "acme"
  }
}
```

Example brand kit:

```json
{
  "tool": "brand_kit_upsert",
  "arguments": {
    "brand_kit": {
      "brand_kit_id": "acme",
      "name": "Acme Co",
      "logo_key": "acme.png",
      "font_name": "DejaVuSans.ttf",
      "font_color": "white",
      "box_color": "black@0.6",
      "auto_logo": true
    }
  }
}
```

</details>

## Batch + Workflows

<details>
<summary>Batch exports and workflows</summary>

```json
{
  "tool": "batch_export_social_formats",
  "arguments": {
    "asset_id": "ASSET_ID"
  }
}
```

```json
{
  "tool": "workflow_run",
  "arguments": {
    "workflow": {
      "nodes": [
        {"id": "base", "type": "transcode", "input": "ASSET_ID", "params": {"preset": "mp4_web_720p_small"}},
        {"id": "logo", "type": "video_add_logo", "input": "base", "params": {"logo_key": "acme.png"}}
      ],
      "outputs": ["logo"]
    }
  }
}
```

</details>

## Example curl Flow

<details>
<summary>Initialize and list tools</summary>

```bash
curl -i -X POST http://localhost:8087/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "X-MADPANDA-PORTAL-GRANT: <portal-grant-secret>" \
  -H "Authorization: Bearer <portal-signed-jwt>" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}'

curl -i -X POST http://localhost:8087/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -H "X-MADPANDA-PORTAL-GRANT: <portal-grant-secret>" \
  -H "Authorization: Bearer <portal-signed-jwt>" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
```

</details>

## Notes

- Allowlist failures return explicit host lists for faster debugging.
- `job_status` returns progress, logs, and cache hit status.
- Signed URLs require `PUBLIC_BASE_URL` + `DOWNLOAD_SIGNING_SECRET`.

## License

MIT.

## Support

[![Donate to the Project](https://img.shields.io/badge/Donate_to_the_Project-Support_Development-ff69b4?style=for-the-badge&logo=heart&logoColor=white)](https://donate.stripe.com/cNidRbdkAbdP8iU7SD4ko0b)

## Affiliate Links

<details>
<summary>Services I use (affiliate)</summary>

Using these links helps support continued development.

### Hostinger VPS
- [KVM 1](https://www.hostinger.com/cart?product=vps%3Avps_kvm_1&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a491-d783-7057-85d2-27de6e01e2c5)
- [KVM 2](https://www.hostinger.com/cart?product=vps%3Avps_kvm_2&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a492-26cf-7333-b6d7-692e17bf8ce1)
- [KVM 4](https://www.hostinger.com/cart?product=vps%3Avps_kvm_4&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a492-531e-70d3-83f5-e28eb919466d)
- [KVM 8](https://www.hostinger.com/cart?product=vps%3Avps_kvm_8&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a492-7ce9-70fb-b96c-2184abc56764)

### Cloud Hosting
- [Cloud Economy](https://www.hostinger.com/cart?product=hosting%3Acloud_economy&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a48f-e7fa-7358-9ff0-f9ba2e8d6e36)
- [Cloud Professional](https://www.hostinger.com/cart?product=hosting%3Acloud_professional&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a490-20fd-70bc-959e-a1f2cd9a69a6)
- [Cloud Enterprise](https://www.hostinger.com/cart?product=hosting%3Acloud_enterprise&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a490-5972-72e4-850f-40d618988dc1)

### Web Hosting
- [Premium](https://www.hostinger.com/cart?product=hosting%3Ahostinger_premium&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a48f-4c21-7199-9918-8f31a3f6a0d9)
- [Business](https://www.hostinger.com/cart?product=hosting%3Ahostinger_business&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a48f-1135-72ba-acbb-13e0e7550db0)

### Website Builder
- [Premium](https://www.hostinger.com/cart?product=hosting%3Ahostinger_premium&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a492-f240-7309-b3fe-9f6909fbc769&product_type=website-builder)
- [Business](https://www.hostinger.com/cart?product=hosting%3Ahostinger_business&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a492-7ce9-70fb-b96c-2184abc56764)

### Agency Hosting
- [Startup](https://www.hostinger.com/cart?product=hosting%3Aagency_startup&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a490-d03c-71de-9acf-08fd4fa911de)
- [Growth](https://www.hostinger.com/cart?product=hosting%3Aagency_growth&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a491-6af4-731f-8947-f1458f07fa5b)
- [Professional](https://www.hostinger.com/cart?product=hosting%3Aagency_professional&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a491-03fb-73f8-9910-044a0a33393a)

### Email
- [Business Pro](https://www.hostinger.com/cart?product=hostinger_mail%3Apro&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a493-5c27-727b-b7f9-8747ffb4e5ee)
- [Business Premium](https://www.hostinger.com/cart?product=hostinger_mail%3Apremium&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a493-a3fc-72b8-a961-94ed6e1c70e6)

### Reach
- [Reach 500](https://www.hostinger.com/cart?product=reach%3A500&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a494-3ebf-7367-b409-9948de50a297)
- [Reach 1000](https://www.hostinger.com/cart?product=reach%3A1000&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a494-8bb9-726e-bb8d-9de9a72a3c21)
- [Reach 2500](https://www.hostinger.com/cart?product=reach%3A2500&period=12&referral_type=cart_link&REFERRALCODE=ZUWMADPANOFE&referral_id=0199a494-c9c1-7191-b600-cafa2e9adafc)

</details>

## Contact

Open an issue in `MADPANDA3D/FFMPEG-MCP`.

<p align="center">
  <img src="https://assets.zyrosite.com/cdn-cgi/image/format=auto,w=316,fit=crop,q=95/dJo56xnDoJCnbgxg/official-logo-mxBMZGQ8Owc8p2M2.jpeg" width="160" alt="MADPANDA3D logo" />
  <br />
  <strong>MADPANDA3D</strong>
</p>
