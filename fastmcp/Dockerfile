FROM python:3.12-slim

ARG BUILD_SHA=unknown
ARG BUILD_VERSION=2026-07-12.2

LABEL org.opencontainers.image.title="MADPANDA3D FFmpeg MCP" \
      org.opencontainers.image.revision="${BUILD_SHA}" \
      org.opencontainers.image.version="${BUILD_VERSION}"

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

ENV MCP_HTTP_PORT="8087"
ENV MCP_BIND_ADDRESS="0.0.0.0"
ENV BUILD_SHA="${BUILD_SHA}"

EXPOSE 8087

CMD ["python", "ffmpeg_mcp_server.py"]
