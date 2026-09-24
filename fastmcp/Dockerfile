FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ARG BUILD_SHA=unknown
ARG BUILD_VERSION=2026-07-12.3

LABEL org.opencontainers.image.title="MADPANDA3D FFmpeg MCP" \
      org.opencontainers.image.revision="${BUILD_SHA}" \
      org.opencontainers.image.version="${BUILD_VERSION}"

COPY . ./

RUN groupadd --system --gid 10001 mcp && \
    useradd --system --uid 10001 --gid 10001 --home-dir /nonexistent --no-create-home mcp && \
    mkdir -p /data/assets /data/staging /data/logos && \
    chown -R 10001:10001 /data

ENV MCP_HTTP_PORT="8087"
ENV MCP_BIND_ADDRESS="0.0.0.0"
ENV BUILD_SHA="${BUILD_SHA}"
ENV HOME="/tmp"
ENV PYTHONDONTWRITEBYTECODE="1"

EXPOSE 8087

USER 10001:10001

CMD ["python", "ffmpeg_mcp_server.py"]
