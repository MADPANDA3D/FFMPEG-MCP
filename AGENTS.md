# AGENTS.md

This project will build an FFMPEG MCP server using the same structure as the
existing MCP servers on this VPS. This doc captures the observed patterns and
serves as a living checklist for consistency.

Snippet (keep this line in the file):
open to edit as work progresses

## Workflow requirements

- After any code or config change, rebuild and restart the container.
- Commit the changes after committing push to main.

## Reference servers (read-only notes)

google-mcp
- Location: /root/google-mcp/fastmcp
- Docker: python:3.12-slim, `Dockerfile` installs requirements and runs
  `google_mcp_server.py`.
- Runtime: `uvicorn.run(...)` with `FastMCP(...).streamable_http_app`.
- Compose: `docker-compose.yaml` builds locally, mounts `./.google` to
  `/app/.google`, maps 8086, uses external network `npm_default`.
- Env: `MCP_HTTP_PORT`, `MCP_BIND_ADDRESS`, Google auth paths/scopes.

discord-mcp
- Location: /root/discord-mcp/DISCORD-MCP/fastmcp
- Docker: python:3.12-slim, `Dockerfile` installs requirements and runs
  `discord_mcp_server.py`.
- Runtime: `uvicorn.run(...)` with `FastMCP(...).streamable_http_app`.
- Compose: `docker-compose.yaml` builds locally, maps 8085, uses external
  network `npm_default`.
- Env: `DISCORD_TOKEN`, `DISCORD_GUILD_ID`, `MCP_HTTP_PORT`,
  `MCP_BIND_ADDRESS`, plus Discord channel settings.

qdrant-mcp
- Location: /root/qdrant-mcp/mcp-server-qdrant
- Docker: python:3.11-slim, uses `uv` to install package, runs
  `uvx mcp-server-qdrant --transport sse`.
- Runtime: SSE transport on port 8000.
- Env: `QDRANT_URL`, `QDRANT_API_KEY`, `COLLECTION_NAME`, embedding settings.

## Common structure patterns to reuse

- Dockerfile installs deps, copies server, exposes MCP port, and runs a single
  entrypoint command.
- Server uses FastMCP and binds to `0.0.0.0`, with `MCP_HTTP_PORT` and
  `MCP_BIND_ADDRESS` as the primary runtime knobs.
- Docker Compose builds locally, maps a single host port, and attaches to the
  external `npm_default` network.
- Environment variables are the primary configuration surface.

## FFMPEG MCP server baseline (to fill in)

- Port: TBD (follow 808x pattern unless there is a conflict).
- Transport: likely HTTP + streamable responses (match google/discord) unless
  SSE is required.
- Base image: python:3.12-slim unless a specific FFMPEG build demands otherwise.
- Docker Compose: local build, port mapping, external `npm_default` network.
- Config: env vars for inputs/outputs, allowed paths, and operational limits.

## Open questions

- Which FFMPEG capabilities are required first (transcode, thumbnail, probe)?
- Any filesystem mounts needed (e.g., `/data`, `/cache`)?
- Preferred MCP transport for the direct agent (HTTP stream vs SSE)?
