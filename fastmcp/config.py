import os
from dataclasses import dataclass, field
from typing import Iterable


def _get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name)
    if value is None or value == "":
        return default if default is not None else ""
    return value


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _get_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _split_csv(value: str) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def _normalize_domains(domains: Iterable[str]) -> list[str]:
    normalized: list[str] = []
    for domain in domains:
        cleaned = domain.strip().lower()
        if cleaned:
            normalized.append(cleaned)
    return normalized


@dataclass(frozen=True)
class Settings:
    mcp_http_port: int = _get_int("MCP_HTTP_PORT", 8087)
    mcp_bind_address: str = _get_env("MCP_BIND_ADDRESS", "0.0.0.0")
    tool_mode: str = _get_env("MCP_TOOL_MODE", "individual").lower()

    redis_url: str = _get_env("REDIS_URL", "redis://redis:6379/0")
    queue_name: str = _get_env("QUEUE_NAME", "av-jobs")

    storage_backend: str = _get_env("STORAGE_BACKEND", "local").lower()
    storage_local_dir: str = _get_env("STORAGE_LOCAL_DIR", "/data/assets")
    storage_temp_dir: str = _get_env("STORAGE_TEMP_DIR", "/data/staging")

    s3_bucket: str = _get_env("S3_BUCKET", "")
    s3_region: str = _get_env("S3_REGION", "")
    s3_endpoint_url: str = _get_env("S3_ENDPOINT_URL", "")
    s3_access_key: str = _get_env("S3_ACCESS_KEY", "")
    s3_secret_key: str = _get_env("S3_SECRET_KEY", "")

    public_base_url: str = _get_env("PUBLIC_BASE_URL", "")
    download_url_ttl_seconds: int = _get_int("DOWNLOAD_URL_TTL_SECONDS", 3600)
    download_signing_secret: str = _get_env("DOWNLOAD_SIGNING_SECRET", "")

    max_ingest_bytes: int = _get_int("MAX_INGEST_BYTES", 500_000_000)
    max_output_bytes: int = _get_int("MAX_OUTPUT_BYTES", 500_000_000)
    max_duration_seconds: int = _get_int("MAX_DURATION_SECONDS", 3600)
    ingest_timeout_seconds: int = _get_int("INGEST_TIMEOUT_SECONDS", 300)
    ingest_stream_chunk_bytes: int = _get_int("INGEST_STREAM_CHUNK_BYTES", 65536)
    ingest_range_chunk_bytes: int = _get_int("INGEST_RANGE_CHUNK_BYTES", 8 * 1024 * 1024)

    asset_ttl_hours: int = _get_int("ASSET_TTL_HOURS", 24)
    job_ttl_hours: int = _get_int("JOB_TTL_HOURS", 24)
    cleanup_interval_seconds: int = _get_int("CLEANUP_INTERVAL_SECONDS", 900)
    job_stale_seconds: int = _get_int("JOB_STALE_SECONDS", 0)

    allowed_domains: list[str] = field(
        default_factory=lambda: _normalize_domains(
            _split_csv(
                _get_env(
                    "ALLOWED_DOMAINS",
                    "cdn.discordapp.com,media.discordapp.net,googleusercontent.com,drive.google.com,docs.google.com",
                )
            )
        )
    )
    allowed_content_types: list[str] = field(
        default_factory=lambda: _split_csv(
            _get_env("ALLOWED_CONTENT_TYPES", "video/*,audio/*,application/octet-stream")
        )
    )

    ffmpeg_bin: str = _get_env("FFMPEG_BIN", "ffmpeg")
    ffprobe_bin: str = _get_env("FFPROBE_BIN", "ffprobe")
    ffmpeg_timeout_seconds: int = _get_int("FFMPEG_TIMEOUT_SECONDS", 900)

    discord_bot_token: str = _get_env("DISCORD_BOT_TOKEN", "")
    discord_api_base: str = _get_env("DISCORD_API_BASE", "https://discord.com/api/v10")
    discord_max_upload_bytes: int = _get_int("DISCORD_MAX_UPLOAD_BYTES", 25_000_000)

    google_drive_credentials_path: str = _get_env("GOOGLE_DRIVE_CREDENTIALS_PATH", "")
    google_drive_impersonate_user: str = _get_env("GOOGLE_DRIVE_IMPERSONATE_USER", "")
    google_drive_folder_default: str = _get_env("GOOGLE_DRIVE_FOLDER_DEFAULT", "")

    log_requests: bool = _get_bool("MCP_LOG_REQUESTS", False)
    log_level: str = _get_env("MCP_LOG_LEVEL", "INFO")

    def asset_ttl_seconds(self) -> int:
        return max(self.asset_ttl_hours, 1) * 3600

    def job_ttl_seconds(self) -> int:
        return max(self.job_ttl_hours, 1) * 3600

    def stale_job_seconds(self) -> int:
        if self.job_stale_seconds > 0:
            return self.job_stale_seconds
        return self.ffmpeg_timeout_seconds + 120


settings = Settings()
