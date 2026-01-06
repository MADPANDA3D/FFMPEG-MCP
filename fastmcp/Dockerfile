FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . ./

ENV MCP_HTTP_PORT="8087"
ENV MCP_BIND_ADDRESS="0.0.0.0"

EXPOSE 8087

CMD ["python", "ffmpeg_mcp_server.py"]
