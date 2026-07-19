#!/usr/bin/env python3
"""Create a private runtime environment without printing generated secrets."""

from __future__ import annotations

import argparse
import os
import secrets
from pathlib import Path
from urllib.parse import urlsplit


def _validated_public_base_url(value: str) -> str:
    if (
        not value
        or value != value.strip()
        or "*" in value
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in value
        )
    ):
        raise ValueError("PUBLIC_BASE_URL must be an exact HTTP(S) origin")
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError:
        raise ValueError("PUBLIC_BASE_URL must be an exact HTTP(S) origin") from None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or "%" in parsed.netloc
        or "\\" in parsed.netloc
        or parsed.netloc.endswith(":")
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("PUBLIC_BASE_URL must be an exact HTTP(S) origin")
    return value.rstrip("/")


def build_environment(mode: str, public_base_url: str | None = None) -> str:
    if mode not in {"standalone", "portal"}:
        raise ValueError("mode must be exactly standalone or portal")
    if mode == "portal" and not public_base_url:
        raise ValueError("PUBLIC_BASE_URL is required in Portal mode")
    access_token = secrets.token_urlsafe(48) if mode == "standalone" else ""
    portal_grant = secrets.token_urlsafe(48) if mode == "portal" else ""
    principal_secret = secrets.token_urlsafe(48)
    download_secret = secrets.token_urlsafe(48)
    redis_password = secrets.token_urlsafe(48)
    public_base_url = _validated_public_base_url(public_base_url or "http://127.0.0.1:8087")
    return "\n".join(
        (
            f"MCP_MODE={mode}",
            f"MCP_ACCESS_TOKEN={access_token}",
            f"MCP_PORTAL_GRANT_TOKEN={portal_grant}",
            "MCP_PORTAL_GRANT_HEADER=X-MADPANDA-PORTAL-GRANT",
            f"MCP_PRINCIPAL_HASH_SECRET={principal_secret}",
            "MCP_HOST_PORT=8087",
            "MCP_ALLOWED_HOSTS=localhost,127.0.0.1,[::1],ffmpeg-mcp",
            "MCP_ALLOWED_ORIGINS=",
            "MCP_REQUEST_BODY_MAX_BYTES=131072",
            "MCP_REQUEST_BODY_TIMEOUT_SECONDS=10",
            "MCP_RESPONSE_BODY_MAX_BYTES=2097152",
            "MCP_RATE_LIMIT_PRINCIPAL_RPM=300",
            "MCP_RATE_LIMIT_WINDOW_SECONDS=60",
            "MCP_RATE_LIMIT_RETRY_AFTER_SECONDS=60",
            f"REDIS_PASSWORD={redis_password}",
            f"REDIS_URL=redis://:{redis_password}@redis:6379/0",
            "REDIS_MAXMEMORY_BYTES=201326592",
            "JOB_ADMISSION_OWNER_MAX_ACTIVE=4",
            "JOB_ADMISSION_GLOBAL_MAX_ACTIVE=32",
            "JOB_ADMISSION_OWNER_RPM=30",
            "JOB_ADMISSION_EXECUTION_BUFFER_SECONDS=3720",
            "METRICS_TTL_SECONDS=86400",
            "BRAND_KIT_MAX_COUNT=25",
            "BRAND_KIT_MAX_SERIALIZED_BYTES=16384",
            "BRAND_KIT_MAX_STRING_CHARS=256",
            "STORAGE_STAGING_MAX_AGE_SECONDS=7200",
            "INGEST_STAGING_OWNER_MAX_ACTIVE=2",
            "INGEST_STAGING_GLOBAL_MAX_ACTIVE=8",
            "INGEST_STAGING_OWNER_MAX_BYTES=1000000000",
            "INGEST_STAGING_GLOBAL_MAX_BYTES=4000000000",
            "INGEST_STAGING_LEASE_SECONDS=600",
            "INGEST_STAGING_HEARTBEAT_SECONDS=30",
            "S3_CONNECT_TIMEOUT_SECONDS=10",
            "S3_READ_TIMEOUT_SECONDS=60",
            "STORAGE_ASGI_MAX_CONCURRENCY=4",
            "STORAGE_ASGI_ADMISSION_TIMEOUT_SECONDS=5",
            "STORAGE_ASGI_OPERATION_TIMEOUT_SECONDS=120",
            "ASSET_QUOTA_OWNER_MAX_COUNT=100",
            "ASSET_QUOTA_OWNER_MAX_BYTES=5368709120",
            "ASSET_QUOTA_GLOBAL_MAX_COUNT=400",
            "ASSET_QUOTA_GLOBAL_MAX_BYTES=21474836480",
            "ASSET_RESERVATION_LEASE_SECONDS=300",
            "ASSET_RESERVATION_HEARTBEAT_SECONDS=30",
            "ASSET_DELETE_LEASE_SECONDS=180",
            "ASSET_DELETE_RETRY_BASE_SECONDS=60",
            "ASSET_DELETE_RETRY_MAX_SECONDS=3600",
            "JOB_STORAGE_MAX_OUTPUT_COUNT=25",
            "JOB_STORAGE_MAX_OUTPUT_BYTES=2147483648",
            "JOB_STORAGE_MAX_MATERIALIZE_BYTES=2147483648",
            "FFMPEG_RLIMIT_AS_BYTES=3221225472",
            "FFPROBE_RLIMIT_AS_BYTES=536870912",
            "FFMPEG_RLIMIT_CPU_SECONDS=1800",
            "FFPROBE_RLIMIT_CPU_SECONDS=60",
            "MEDIA_RLIMIT_NOFILE=256",
            "FFMPEG_THREADS=2",
            "MAX_FRAME_WIDTH=8192",
            "MAX_FRAME_HEIGHT=8192",
            "MAX_FRAME_PIXELS=33177600",
            "MAX_MEDIA_STREAMS=16",
            "MAX_VIDEO_FPS=120",
            "MAX_AUDIO_CHANNELS=8",
            "MAX_AUDIO_SAMPLE_RATE=192000",
            "MAX_DECODED_VIDEO_PIXEL_FRAMES=250000000000",
            "MAX_DECODED_AUDIO_SAMPLE_CHANNELS=6000000000",
            "MAX_BATCH_OPERATIONS=100",
            "MAX_RENDER_ITERATIONS=5",
            "MAX_CAPTION_WORD_TIMINGS=2000",
            f"PUBLIC_BASE_URL={public_base_url}",
            f"DOWNLOAD_SIGNING_SECRET={download_secret}",
            "MCP_BUILD_SHA=development",
            "MCP_SOURCE_FINGERPRINT=development",
            "MCP_IMAGE_REFERENCE=development",
            "MCP_PORTAL_NETWORK=",
            "",
        )
    )


def create_environment(env_path: Path, mode: str, public_base_url: str | None = None) -> bool:
    """Atomically create one mode-0600 environment; never overwrite."""

    try:
        descriptor = os.open(env_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return False
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(build_environment(mode, public_base_url))
        env_path.chmod(0o600)
    except BaseException:
        env_path.unlink(missing_ok=True)
        raise
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create an ignored mode-0600 .env with fresh service secrets."
    )
    parser.add_argument("--mode", choices=("standalone", "portal"), default="standalone")
    parser.add_argument(
        "--public-base-url",
        help="Exact externally reachable HTTP(S) origin used by signed local downloads.",
    )
    args = parser.parse_args()
    if args.mode == "portal" and not args.public_base_url:
        parser.error("--public-base-url is required in Portal mode")
    try:
        public_base_url = _validated_public_base_url(
            args.public_base_url or "http://127.0.0.1:8087"
        )
    except ValueError as exc:
        parser.error(str(exc))
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not create_environment(env_path, args.mode, public_base_url):
        print("Runtime environment already exists; no values changed.")
        return
    print(
        f"Created ignored mode-0600 {args.mode} environment with fresh service and "
        "principal, Redis, service, and download-signing secrets; no value was printed."
    )


if __name__ == "__main__":
    main()
