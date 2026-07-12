# MEMORY.md — Project Memory
> Filled in by the agent during onboarding and updated every session.
> Append-only — never delete entries. Mark outdated info with [SUPERSEDED — DATE].

---

## Project Identity

- **Project name:** FFmpeg MCP
- **What it does (one sentence):** FastMCP media-processing server for preset-driven FFmpeg jobs, async workers, safe ingest, and signed downloads.
- **Type:** [ ] Client deliverable  [ ] Internal tool  [x] Platform component  [x] MCP server
- **Client / owner:** MADPANDA3D
- **Current phase:** Stable production maintenance on VPS; `.whoami` stack seeded on 2026-04-23 so MCP Command can dispatch remediation safely.

## Tech Stack

- **Frontend:** None
- **Backend:** Python 3.11+ + FastMCP
- **Database:** Redis queue backend plus local/S3-compatible storage
- **Package manager:** pip-style requirements
- **Key libraries:** `mcp`, `uvicorn`, `redis`, `rq`, `httpx`, `aiofiles`, `boto3`, Google API clients
- **Repo path:** `/home/services/ffmpeg-mcp/fastmcp`

## Infrastructure & Deployment

- **Deployment target:** VPS Docker on `mcp-network`
- **Service/container:** `ffmpeg-mcp` plus worker container
- **Working directory:** `/home/services/ffmpeg-mcp/fastmcp`
- **Default HTTP port:** `8087`

## Key Facts

- Supports async worker processing through RQ + Redis.
- Preset-only FFmpeg operations are intentional to keep agent behavior predictable.
- Optional exports exist for Drive and Discord.

## Session Notes

### 2026-04-23 — Initial stack seed
- Seeded this `.whoami` stack so MCP Command can treat the service as mapped and present.
- The current live portal has an FFmpeg capabilities timeout ticket and FFmpeg remains one of the services MCP Command is expected to remediate.

### 2026-05-09 — TKT-000044 ticket history layout
- Verified the ffmpeg MCP ticket history path already matches the meta-root standard: `/home/services/ffmpeg-mcp/tickets`.
- Preserved `tickets/in-progress`, `tickets/completed`, `tickets/failed`, and `tickets/blocked` with `.gitkeep` files for support-agent automation.
- No stale ticket-path references were found in local docs/session files.

### 2026-07-12 — Agent-first ToolManifest baseline
- The provider-owned catalog contract is `fastmcp/tool_manifest.py`, shared schema `1.0.0`, service ID `ffmpeg`, and catalog version `2026-07-12.2`.
- The native individual-mode registry contains 55 tools: the existing 50 plus `check_configuration`, `list_capabilities`, `get_endpoint_coverage`, `get_tool_usage`, and `find_tools`.
- Portal catalog ingestion should call `list_capabilities` with `include_descriptors: true`; the complete ordered manifest is returned at the `structuredContent` top level.
- Descriptor hashes are deterministic over canonical descriptors and intentionally exclude the runtime `buildSha`; neither hashes nor navigation responses contain credential values.
- `brand_kit_delete` is classified destructive and declares the exact Portal confirmation phrase `DELETE BRAND KIT`.
- FFmpeg confirmation is intentionally out-of-band: `confirmation.parameter` is null and the phrase is consumed by `portal.call_destructive_tool`, not forwarded to the native `brand_kit_delete(brand_kit_id)` function.
- The first implementation is committed on `feat/tool-manifest` and was not deployed during this slice.
