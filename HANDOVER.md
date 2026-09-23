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

## 2026-09-23 - TKT-000470 Reusable Reel Pipeline

- Status: implemented and locally validated; source changes intentionally left uncommitted for the host runner.
- Added 12 agent-ready Portal tools for direct uploads, durable clip-library management, immutable template versions, and MADPANDA3D Reel contract validation/render delegation.
- Direct upload completion enforces declared MIME, size, and SHA-256 and deduplicates durable originals by digest.
- Reel plans use the existing bounded workflow engine and preserve all prior tool names and schemas.
- Added focused tests and `docs/ffmpeg-skill-gap-assessment.md`; no reference source was copied.
- Runner next: execute the configured dependency-complete test gate, rebuild/restart both API and worker, smoke `/health` for 67 tools, refresh Portal discovery, and exercise one safe synthetic end-to-end upload/dedup/render/preview/download flow.

### 2026-09-23 repair cycle 2

- Corrected `reel_library.py`, `tests/test_reel_library.py`, and the gap assessment from mode `0600` to `0644`; cycle-1 candidate validation had failed because the non-root container user could not read them.
- Re-ran Python compilation, three focused Reel contract tests, 67-tool registry/manifest consistency, mode assertions, and `git diff --check`; all passed.
- Source remains intentionally uncommitted for the host runner. The runner still owns the dependency-complete candidate gate, Compose rebuild/restart, production health/discovery/configuration smoke, and exact-SHA commit/push.

### 2026-09-23 repair cycle 3

- Added explicit ToolManifest output schemas for every new direct-upload, clip-library, versioned-template, and MADPANDA3D Reel tool after cycle-2 validation stopped at `clip_library_archive`.
- Verified Python compilation, a dependency-free complete 67-descriptor manifest build with all 12 new output schemas, readable `0644` file modes, and `git diff --check`.
- The sandbox host lacks runtime dependencies, so the host runner still owns the configured dependency-complete unittest gate and deployment smoke.
