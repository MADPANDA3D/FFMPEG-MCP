# FFMPEG MCP (FastMCP)

Preset-driven FFmpeg MCP server with async jobs, staging storage, and signed
download URLs. Built to match the Google/Discord MCP deployment style on VPS.

## What you get

- FastMCP HTTP server + async worker (RQ + Redis)
- URL ingest with allowlist, magic-byte validation, size/duration caps
- Preset-only FFmpeg operations (no arbitrary flags)
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

- `media_ingest_from_url`
- `media_ingest_from_drive`
- `media_probe`
- `ffmpeg_transcode`
- `ffmpeg_thumbnail`
- `ffmpeg_extract_audio`
- `ffmpeg_trim`
- `ffmpeg_list_presets`
- `ffmpeg_describe_preset`
- `ffmpeg_capabilities`
- `job_status`
- `media_get_download_url`
- `media_export_to_drive`
- `media_export_to_discord`

## Presets

Start with `ffmpeg_list_presets` or `ffmpeg_capabilities` for the full list.
Use `ffmpeg_describe_preset(name)` for safe profile details.

Default presets include:
- `mp4_web_720p_small`
- `mp4_web_1080p`
- `mp4_web_480p_tiny`
- `mp4_social_vertical_1080x1920`
- `mp4_social_square_1080x1080`
- `mp4_social_reel_1080x1920_high`
- `mp3_voice_128k`
- `mp3_voice_64k`
- `wav_pcm_16k_mono`
- `gif_preview_lowfps`

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
