# linux/amd64 Python 3.12.13 slim-bookworm pin. Update the digest deliberately.
FROM python:3.14.6-slim-bookworm@sha256:86f975aca15cf04a40b399eebede9aea7c82eae084d1f1a0a6ef6bcaae871a30 AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore \
    UV_PROJECT_ENVIRONMENT=/opt/ffmpeg-mcp

WORKDIR /build

RUN mkdir /tmp/uv \
    && python -m pip download --no-cache-dir --no-deps --only-binary=:all: \
      --dest /tmp/uv uv==0.11.29 \
    && echo "eec03a8b63d55915694db3af4e91324b39ced49e2aeac7af37851c7eb3f470ea  /tmp/uv/uv-0.11.29-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.whl" \
      | sha256sum --check --strict \
    && python -m pip install --no-cache-dir --no-deps /tmp/uv/*.whl \
    && rm -rf /tmp/uv

COPY pyproject.toml uv.lock README.md LICENSE NOTICE ./
COPY src ./src

RUN uv sync --frozen --no-dev --no-editable \
    && rm -rf /root/.cache /tmp/*


FROM python:3.14.6-slim-bookworm@sha256:86f975aca15cf04a40b399eebede9aea7c82eae084d1f1a0a6ef6bcaae871a30

ARG BUILD_SHA=development
ARG SOURCE_FINGERPRINT=development
ARG IMAGE_VERSION=1.0.0

LABEL org.opencontainers.image.title="MADPANDA3D FFMPEG MCP" \
      org.opencontainers.image.description="Dual-mode asynchronous FFmpeg media MCP server" \
      org.opencontainers.image.source="https://github.com/MADPANDA3D/FFMPEG-MCP" \
      org.opencontainers.image.revision="${BUILD_SHA}" \
      org.opencontainers.image.version="${IMAGE_VERSION}" \
      org.opencontainers.image.licenses="MIT" \
      com.madpanda.source-fingerprint="${SOURCE_FINGERPRINT}"

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates ffmpeg fonts-dejavu-core util-linux \
    && test -x /usr/bin/prlimit \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 app \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin app \
    && mkdir -p /data/assets /data/staging /data/logos \
    && chown -R 10001:10001 /data \
    && test "$(id -u app)" = "10001" \
    && test "$(id -g app)" = "10001"

WORKDIR /app

COPY --from=builder --chown=10001:10001 /opt/ffmpeg-mcp /opt/ffmpeg-mcp
COPY --chown=10001:10001 scripts/runtime_smoke.py ./scripts/runtime_smoke.py
COPY LICENSE NOTICE /usr/share/licenses/mad-mcp-ffmpeg/
RUN chmod 0444 /usr/share/licenses/mad-mcp-ffmpeg/LICENSE \
    /usr/share/licenses/mad-mcp-ffmpeg/NOTICE

ENV PATH="/opt/ffmpeg-mcp/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp \
    MCP_BIND_ADDRESS=0.0.0.0 \
    MCP_HTTP_PORT=8087 \
    MCP_MODE=standalone \
    MCP_BUILD_SHA="${BUILD_SHA}" \
    MCP_SOURCE_FINGERPRINT="${SOURCE_FINGERPRINT}" \
    MCP_IMAGE_REFERENCE=development \
    MCP_EXPECTED_TOOL_COUNT=55 \
    MCP_ALLOWED_HOSTS=localhost,127.0.0.1,[::1],ffmpeg-mcp \
    MCP_ALLOWED_ORIGINS="" \
    MCP_REQUEST_BODY_MAX_BYTES=131072 \
    MCP_RESPONSE_BODY_MAX_BYTES=2097152 \
    REDIS_URL="" \
    STORAGE_LOCAL_DIR=/data/assets \
    STORAGE_TEMP_DIR=/data/staging \
    LOGO_DIR=/data/logos

EXPOSE 8087
USER 10001:10001

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD ["python", "-c", "import json,os,urllib.request; p=json.load(urllib.request.urlopen('http://127.0.0.1:8087/health',timeout=3)); raise SystemExit(0 if p.get('status')=='healthy' and p.get('tool_count')==int(os.environ['MCP_EXPECTED_TOOL_COUNT']) else 1)"]

CMD ["mad-mcp-ffmpeg"]
