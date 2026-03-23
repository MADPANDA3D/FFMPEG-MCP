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
    auth_mode: str = _get_env("MCP_AUTH_MODE", "off").lower()
    portal_grant_header: str = _get_env("MCP_PORTAL_GRANT_HEADER", "X-MADPANDA-PORTAL-GRANT")
    portal_grant_secret: str = _get_env("MCP_PORTAL_GRANT_SECRET", "")
    jwt_alg: str = _get_env("MCP_JWT_ALG", "HS256").upper()
    jwt_public_key: str = _get_env("MCP_JWT_PUBLIC_KEY", "")
    jwt_secret: str = _get_env("MCP_JWT_SECRET", "")
    jwt_issuer: str = _get_env("MCP_JWT_ISSUER", "")
    jwt_audience: str = _get_env("MCP_JWT_AUDIENCE", "")
    jwt_required_scope: str = _get_env("MCP_JWT_REQUIRED_SCOPE", "mcp:ffmpeg")
    signup_url: str = _get_env("MCP_SIGNUP_URL", "https://madpanda3d.com/lab/mad-mcps")
    rate_limit_user_rpm: int = _get_int("MCP_RATE_LIMIT_USER_RPM", 120)
    rate_limit_ip_rpm: int = _get_int("MCP_RATE_LIMIT_IP_RPM", 300)
    max_active_jobs_per_user: int = _get_int("MCP_MAX_ACTIVE_JOBS_PER_USER", 5)
    rate_limit_retry_after_seconds: int = _get_int("MCP_RATE_LIMIT_RETRY_AFTER_SECONDS", 60)

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
            _get_env(
                "ALLOWED_CONTENT_TYPES",
                "video/*,audio/*,image/*,application/octet-stream",
            )
        )
    )
    allow_image_ingest: bool = _get_bool("ALLOW_IMAGE_INGEST", True)

    ffmpeg_bin: str = _get_env("FFMPEG_BIN", "ffmpeg")
    ffprobe_bin: str = _get_env("FFPROBE_BIN", "ffprobe")
    ffmpeg_timeout_seconds: int = _get_int("FFMPEG_TIMEOUT_SECONDS", 900)
    ffmpeg_text_timeout_seconds: int = _get_int("FFMPEG_TEXT_TIMEOUT_SECONDS", 0)
    ffmpeg_logo_timeout_seconds: int = _get_int("FFMPEG_LOGO_TIMEOUT_SECONDS", 0)
    ffmpeg_concat_timeout_seconds: int = _get_int("FFMPEG_CONCAT_TIMEOUT_SECONDS", 0)
    ffmpeg_image_timeout_seconds: int = _get_int("FFMPEG_IMAGE_TIMEOUT_SECONDS", 0)
    ffmpeg_slideshow_timeout_seconds: int = _get_int("FFMPEG_SLIDESHOW_TIMEOUT_SECONDS", 0)
    ffmpeg_audio_timeout_seconds: int = _get_int("FFMPEG_AUDIO_TIMEOUT_SECONDS", 0)
    ffmpeg_template_timeout_seconds: int = _get_int("FFMPEG_TEMPLATE_TIMEOUT_SECONDS", 0)
    ffmpeg_workflow_timeout_seconds: int = _get_int("FFMPEG_WORKFLOW_TIMEOUT_SECONDS", 0)
    ffmpeg_batch_timeout_seconds: int = _get_int("FFMPEG_BATCH_TIMEOUT_SECONDS", 0)

    max_text_chars: int = _get_int("MAX_TEXT_CHARS", 200)
    min_font_size: int = _get_int("MIN_FONT_SIZE", 16)
    max_font_size: int = _get_int("MAX_FONT_SIZE", 160)
    max_box_border_width: int = _get_int("MAX_BOX_BORDER_WIDTH", 80)
    overlay_margin_px: int = _get_int("OVERLAY_MARGIN_PX", 32)

    font_dirs: list[str] = field(
        default_factory=lambda: _split_csv(
            _get_env("FONT_DIRS", "/usr/share/fonts/truetype/dejavu")
        )
    )
    font_allowlist: list[str] = field(
        default_factory=lambda: _split_csv(_get_env("FONT_ALLOWLIST", ""))
    )
    font_default: str = _get_env(
        "FONT_DEFAULT", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
    )

    logo_dir: str = _get_env("LOGO_DIR", "/data/logos")
    logo_allowlist: list[str] = field(
        default_factory=lambda: _split_csv(_get_env("LOGO_ALLOWLIST", ""))
    )
    logo_min_scale_pct: int = _get_int("LOGO_MIN_SCALE_PCT", 5)
    logo_max_scale_pct: int = _get_int("LOGO_MAX_SCALE_PCT", 40)
    logo_max_opacity: float = _get_float("LOGO_MAX_OPACITY", 1.0)

    default_video_fps: int = _get_int("DEFAULT_VIDEO_FPS", 30)
    default_image_duration_sec: float = _get_float("DEFAULT_IMAGE_DURATION_SEC", 3.0)
    default_image_width: int = _get_int("DEFAULT_IMAGE_WIDTH", 1080)
    default_image_height: int = _get_int("DEFAULT_IMAGE_HEIGHT", 1080)

    max_concat_clips: int = _get_int("MAX_CONCAT_CLIPS", 20)
    max_slideshow_images: int = _get_int("MAX_SLIDESHOW_IMAGES", 60)
    max_audio_tracks: int = _get_int("MAX_AUDIO_TRACKS", 8)
    max_template_layers: int = _get_int("MAX_TEMPLATE_LAYERS", 12)
    max_template_text_layers: int = _get_int("MAX_TEMPLATE_TEXT_LAYERS", 6)
    max_workflow_nodes: int = _get_int("MAX_WORKFLOW_NODES", 40)
    max_batch_assets: int = _get_int("MAX_BATCH_ASSETS", 50)
    max_batch_presets: int = _get_int("MAX_BATCH_PRESETS", 12)
    max_caption_segments: int = _get_int("MAX_CAPTION_SEGMENTS", 200)
    caption_max_chars: int = _get_int("CAPTION_MAX_CHARS", 72)
    caption_max_lines: int = _get_int("CAPTION_MAX_LINES", 2)
    caption_max_words: int = _get_int("CAPTION_MAX_WORDS", 10)
    caption_line_spacing: int = _get_int("CAPTION_LINE_SPACING", 8)
    caption_font_size: int = _get_int("CAPTION_FONT_SIZE", 48)
    caption_position: str = _get_env("CAPTION_POSITION", "bottom_safe")
    caption_text_color: str = _get_env("CAPTION_TEXT_COLOR", "white")
    caption_box_color: str = _get_env("CAPTION_BOX_COLOR", "black")
    caption_box_opacity: float = _get_float("CAPTION_BOX_OPACITY", 0.6)
    caption_highlight_color: str = _get_env("CAPTION_HIGHLIGHT_COLOR", "yellow")
    caption_padding_px: int = _get_int("CAPTION_PADDING_PX", 24)
    caption_safe_zone_bottom_px: int = _get_int("CAPTION_SAFE_ZONE_BOTTOM_PX", 96)
    caption_safe_zone_top_px: int = _get_int("CAPTION_SAFE_ZONE_TOP_PX", 64)
    auto_caption_font_size_min: int = _get_int("AUTO_CAPTION_FONT_SIZE_MIN", 16)
    auto_caption_font_size_max: int = _get_int("AUTO_CAPTION_FONT_SIZE_MAX", 160)
    auto_caption_box_opacity_min: float = _get_float("AUTO_CAPTION_BOX_OPACITY_MIN", 0.4)
    auto_caption_box_opacity_max: float = _get_float("AUTO_CAPTION_BOX_OPACITY_MAX", 0.85)
    auto_music_gain_min: float = _get_float("AUTO_MUSIC_GAIN_MIN", 0.4)
    auto_music_gain_max: float = _get_float("AUTO_MUSIC_GAIN_MAX", 1.0)
    auto_max_crop_pct: float = _get_float("AUTO_MAX_CROP_PCT", 45.0)
    auto_min_duration_sec: float = _get_float("AUTO_MIN_DURATION_SEC", 0.0)

    social_presets: list[str] = field(
        default_factory=lambda: _split_csv(
            _get_env(
                "SOCIAL_PRESETS",
                "mp4_social_vertical_1080x1920,mp4_social_square_1080x1080,"
                "mp4_social_portrait_1080x1350,mp4_youtube_1920x1080",
            )
        )
    )

    audio_norm_i: float = _get_float("AUDIO_NORM_I", -16.0)
    audio_norm_lra: float = _get_float("AUDIO_NORM_LRA", 11.0)
    audio_norm_tp: float = _get_float("AUDIO_NORM_TP", -1.5)
    audio_sample_rate: int = _get_int("AUDIO_SAMPLE_RATE", 44100)
    audio_min_silence_sec: float = _get_float("AUDIO_MIN_SILENCE_SEC", 0.5)
    audio_silence_db: float = _get_float("AUDIO_SILENCE_DB", -50.0)
    audio_fade_default_sec: float = _get_float("AUDIO_FADE_DEFAULT_SEC", 1.0)
    ducking_ratio: float = _get_float("AUDIO_DUCKING_RATIO", 8.0)
    ducking_threshold: float = _get_float("AUDIO_DUCKING_THRESHOLD", 0.02)
    ducking_attack_ms: int = _get_int("AUDIO_DUCKING_ATTACK_MS", 20)
    ducking_release_ms: int = _get_int("AUDIO_DUCKING_RELEASE_MS", 200)
    ducking_music_gain: float = _get_float("AUDIO_DUCKING_MUSIC_GAIN", 0.8)

    draft_max_dimension: int = _get_int("DRAFT_MAX_DIMENSION", 720)
    draft_crf: int = _get_int("DRAFT_CRF", 28)
    draft_preset: str = _get_env("DRAFT_PRESET", "ultrafast")
    draft_audio_bitrate: str = _get_env("DRAFT_AUDIO_BITRATE", "96k")
    draft_watermark_enabled: bool = _get_bool("DRAFT_WATERMARK_ENABLED", True)
    draft_watermark_text: str = _get_env("DRAFT_WATERMARK_TEXT", "DRAFT")
    draft_watermark_opacity: float = _get_float("DRAFT_WATERMARK_OPACITY", 0.35)
    draft_watermark_font_size: int = _get_int("DRAFT_WATERMARK_FONT_SIZE", 48)

    queue_name_urgent: str = _get_env("QUEUE_NAME_URGENT", "")
    queue_name_batch: str = _get_env("QUEUE_NAME_BATCH", "")
    queue_names_raw: list[str] = field(default_factory=lambda: _split_csv(_get_env("QUEUE_NAMES", "")))

    log_structured: bool = _get_bool("MCP_LOG_STRUCTURED", False)

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

    def text_timeout_seconds(self) -> int:
        return self.ffmpeg_text_timeout_seconds or self.ffmpeg_timeout_seconds

    def logo_timeout_seconds(self) -> int:
        return self.ffmpeg_logo_timeout_seconds or self.ffmpeg_timeout_seconds

    def concat_timeout_seconds(self) -> int:
        return self.ffmpeg_concat_timeout_seconds or self.ffmpeg_timeout_seconds

    def image_timeout_seconds(self) -> int:
        return self.ffmpeg_image_timeout_seconds or self.ffmpeg_timeout_seconds

    def slideshow_timeout_seconds(self) -> int:
        return self.ffmpeg_slideshow_timeout_seconds or self.ffmpeg_timeout_seconds

    def audio_timeout_seconds(self) -> int:
        return self.ffmpeg_audio_timeout_seconds or self.ffmpeg_timeout_seconds

    def template_timeout_seconds(self) -> int:
        return self.ffmpeg_template_timeout_seconds or self.ffmpeg_timeout_seconds

    def workflow_timeout_seconds(self) -> int:
        return self.ffmpeg_workflow_timeout_seconds or self.ffmpeg_timeout_seconds

    def batch_timeout_seconds(self) -> int:
        return self.ffmpeg_batch_timeout_seconds or self.ffmpeg_timeout_seconds

    def queue_names(self) -> list[str]:
        names = list(self.queue_names_raw)
        if not names:
            names = [self.queue_name]
        if self.queue_name_urgent and self.queue_name_urgent not in names:
            names.append(self.queue_name_urgent)
        if self.queue_name_batch and self.queue_name_batch not in names:
            names.append(self.queue_name_batch)
        return names


settings = Settings()
