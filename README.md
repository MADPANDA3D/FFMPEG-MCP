# FFMPEG MCP (FastMCP)

Preset-driven FFmpeg MCP server with async jobs, staging storage, and signed
download URLs. Built to match the Google/Discord MCP deployment style on VPS.

## What you get

- FastMCP HTTP server + async worker (RQ + Redis)
- URL ingest with allowlist, magic-byte validation, size/duration caps
- Preset-only FFmpeg operations (no arbitrary flags)
- Marketing-focused presets (social crops, safe pads, audio normalization, text placeholders)
- Text + logo overlays (drawtext + watermark) via async jobs
- Caption burn-in + one-shot marketing render tools
- Rubrics + analyze/iterate/compare for closed-loop quality
- Templates + Brand Kits for one-call marketing outputs
- Batch exports, campaign processing, and workflow chaining
- Signed download URLs (`/download/{asset_id}`)
- Optional exports to Drive and Discord
- `ffmpeg_capabilities` self-description endpoint

## Setup

### 1) Configure env

```bash
cd fastmcp
cp .env.example .env
```

Important defaults:
- `MCP_HTTP_PORT=8087`
- `PUBLIC_BASE_URL=https://ffmpeg-mcp.yourdomain.com`
- `DOWNLOAD_SIGNING_SECRET=...` (required for local signed URLs)
- `STORAGE_BACKEND=local` (or `s3`)
- `ALLOWED_DOMAINS=cdn.discordapp.com,media.discordapp.net,googleusercontent.com,...`
- `MAX_INGEST_BYTES`, `MAX_OUTPUT_BYTES`, `MAX_DURATION_SECONDS`
- `FONT_DIRS`, `FONT_DEFAULT` for text overlays
- `LOGO_DIR` for logo overlays (`logo_key` values)
- `ALLOW_IMAGE_INGEST=true` for image tools
- `SOCIAL_PRESETS=...` for batch social exports
- `QUEUE_NAME_URGENT`, `QUEUE_NAME_BATCH` for priority queues (optional)

### 2) Run the server

```bash
docker-compose -f fastmcp/docker-compose.yaml up -d --build
```

## Connect to n8n

If n8n runs in Docker on the same host:

```
http://ffmpeg-mcp:8087/mcp
```

External:

```
http://<vps-ip>:8087/mcp
```

## VPS Deployment (Nginx Proxy Manager)

Attach the container to `npm_default` (already done in compose).

NPM host settings:
- Forward Hostname/IP: `ffmpeg-mcp`
- Forward Port: `8087`
- Websockets: ON
- HTTP/2: OFF

Also allow `/download/` paths (signed URL delivery).

## Tool modes (agent vs n8n)

Default is individual tools:
- `MCP_TOOL_MODE=individual`
- Tools are listed as `media_ingest_from_url`, `ffmpeg_transcode`, etc.

Single wrapper mode (for agents expecting a single tool):
- `MCP_TOOL_MODE=router`
- Only tool exposed: `FFMPEG_MCP`
- Call with `{ "tool": "<tool_name>", "arguments": { ... } }`

## Tools (individual mode)

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

## Presets

Start with `ffmpeg_list_presets` or `ffmpeg_capabilities` for the full list.
Use `ffmpeg_describe_preset(name)` for safe profile details.

Sample presets:
- `mp4_web_720p_small`
- `mp4_social_vertical_1080x1920_safe_pad`
- `mp4_social_portrait_1080x1350`
- `mp4_youtube_1920x1080`
- `mp4_social_vertical_1080x1920_lower_third`
- `mp3_voice_128k_loudnorm`
- `mp3_voice_64k`
- `wav_pcm_16k_mono`
- `gif_preview_lowfps`

## Overlay tools

Text overlay (`video_add_text`):
- Required: `asset_id`, `text`
- Optional: `position` (`top|center|bottom`), `font_size`, `font_color`,
  `background_box`, `box_color`, `box_border_width`, `font_name`, `font_asset_id`

Logo overlay (`video_add_logo`):
- Required: `asset_id`, `logo_asset_id` or `logo_key`
- Optional: `position` (`top-left|top-right|bottom-left|bottom-right`),
  `scale_pct`, `opacity`

Examples (router mode):

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

```json
{
  "tool": "video_add_logo",
  "arguments": {
    "asset_id": "ASSET_ID",
    "logo_key": "brand.png",
    "position": "bottom-right",
    "scale_pct": 15,
    "opacity": 0.9
  }
}
```

Notes:
- `font_name` is resolved from `FONT_DIRS` (and `FONT_ALLOWLIST` if set).
- `logo_key` is resolved from `LOGO_DIR` (and `LOGO_ALLOWLIST` if set).
- Async tools accept optional `priority` (`urgent`, `batch`, or default) if queues are configured.

## Captions

Burn-in captions (`captions_burn_in`):
- Required: `asset_id` plus one of `captions_srt`, `captions_vtt`, or `words_json`
- `words_json` shape: `[{ "word": "Hello", "start": 1.23, "end": 1.56 }]` (seconds)
- Optional: `brand_kit_id`, `highlight_mode="word"`, `position`, `font_size`, `font_color`,
  `box_color`, `box_opacity`, `padding_px`, `max_chars`, `max_lines`, `max_words`,
  `safe_zone_bottom_px`, `safe_zone_top_px`, `font_name`, `font_asset_id`

## Marketing render tools

One-shot renders (captioned + non-captioned variants when captions are supplied):
- `render_social_ad`
- `render_testimonial_clip`
- `render_offer_card`
- `render_iterate` (render + analyze + auto-tune until rubric threshold)

Defaults:
- Variants: 9:16 + 1:1 + 4:5 (set `include_16_9=true` for 16:9)
- Inputs: `primary_asset_id` required; optional `broll_asset_ids`, `voice_asset_id`, `music_asset_id`
- Quality: `quality=draft|final` (draft uses 720p presets and optional watermark)

Caption inputs: pass `captions_srt`, `captions_vtt`, or `words_json` to generate captioned outputs.
Iteration constraints: `lock_framing`, `lock_captions`, `lock_audio`, `allow_trim_silence`.

## Analysis + QA

- `video_analyze` returns audio/video/caption metrics with optional rubric scoring.
- `video_analyze` accepts `reference_asset_id` to return deltas vs a golden reference.
- `asset_compare` ranks assets by rubric score.
- `rubric_list` / `rubric_describe` expose scoring profiles.
- Rubrics include `social_reel_v1`, `testimonial_v1`, `insta_reel_v1`, `youtube_short_v1`.
- `job_status` includes `qa` (pass/score/failed_checks/recommended_fix) plus
  `report` (analyze), `ranking` (compare), and `result` (iterate).
- `render_iterate` includes `iterations[].changes` for compact diffs.

## Templates

List and apply:
- `template_list`
- `template_describe`
- `template_apply`

Notes:
- `template_describe` returns a schema (required fields, defaults, max chars).
- `template_apply` accepts `quality=draft|final` to map to draft presets.
- `campaign_process` accepts `quality=draft|final` to map presets for draft outputs.

Example:

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

## Brand kits

Create or update:

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

Apply:

```json
{
  "tool": "brand_kit_apply",
  "arguments": {
    "asset_id": "ASSET_ID",
    "brand_kit_id": "acme",
    "text": "Acme Co"
  }
}
```

## Batch + workflows

Batch exports:

```json
{
  "tool": "batch_export_social_formats",
  "arguments": {
    "asset_id": "ASSET_ID"
  }
}
```

Campaign:

```json
{
  "tool": "campaign_process",
  "arguments": {
    "asset_ids": ["A1", "A2"],
    "template_name": "promo_vertical_basic",
    "brand_kit_id": "acme"
  }
}
```

Workflow:

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

## Example curl flow

```bash
# 1) Initialize MCP session
curl -i -X POST http://localhost:8087/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"test","version":"1.0"}}}'

# 2) List tools
curl -i -X POST http://localhost:8087/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
```

## Notes

- Allowlist failures return explicit host lists for faster debugging.
- `job_status` returns progress, logs, and cache hit status.
- Signed URLs require `PUBLIC_BASE_URL` + `DOWNLOAD_SIGNING_SECRET`.
