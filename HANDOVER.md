# HANDOVER.md

## 2026-05-09 - TKT-000044 Ticket History Standardization

- Status: succeeded
- Ticket: TKT-000044
- Service: ffmpeg
- Work completed: confirmed ticket history lives at `/home/services/ffmpeg-mcp/tickets`; preserved status directories with `.gitkeep`; moved `TKT-000044.md` from `in-progress` to `completed`.
- Runtime impact: none; no MCP code, config, dependencies, secrets, Docker rebuild, or container restart.
- Verification: checked ticket directories, searched local docs/session files for stale ticket path references, verified there is only one `tickets` directory under the assigned repo.

## 2026-07-12 - Agent-First ToolManifest (MAD-639 / MAD-641)

- Status: implemented and verified on branch `feat/tool-manifest`; intentionally not deployed.
- Added a deterministic provider-owned ToolManifest covering all 55 native tools (50 existing plus five navigation tools) with complete schemas, safety annotations, aliases, confirmation metadata, catalog/build identity, and per-tool/aggregate hashes.
- Added `check_configuration`, `list_capabilities`, `get_endpoint_coverage`, `get_tool_usage`, and `find_tools`; `list_capabilities(include_descriptors=true)` returns the lossless manifest in MCP `structuredContent`.
- Preserved the existing grant-only middleware, legacy `ffmpeg_capabilities`, router mode, media behavior, credentials, and running containers.
- Verification: 12/12 focused tests; Ruff clean; mypy clean for changed source; Python compile clean; Compose config valid; test image built; ephemeral HTTP smoke returned missing/invalid grant 401, valid grant 200, health/tools-list count 55, and structured manifest count 55.

### 2026-07-12 confirmation contract correction

- Bumped the FFmpeg catalog to `2026-07-12.2` and made destructive confirmation explicitly out-of-band.
- `brand_kit_delete` still requires exact Portal phrase `DELETE BRAND KIT`; its manifest parameter is null because the native provider function consumes only `brand_kit_id`.
- No runtime provider behavior or deployment changed.
