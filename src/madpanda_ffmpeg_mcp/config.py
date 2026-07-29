import math
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from urllib.parse import urlsplit


def _get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name)
    if value is None:
        return default if default is not None else ""
    return value


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer") from None


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a finite number") from None
    if not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    return value


def _get_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true"}:
        return True
    if normalized in {"0", "false"}:
        return False
    raise ValueError(f"{name} must be exactly true, false, 1, or 0")


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


def _is_exact_host(value: str) -> bool:
    if not value or value != value.strip() or "*" in value:
        return False
    if any(character in value for character in "/?#@"):
        return False
    try:
        parsed = urlsplit(f"//{value}")
        _ = parsed.port
    except ValueError:
        return False
    return bool(parsed.hostname) and parsed.username is None and parsed.password is None


def _is_exact_origin(value: str) -> bool:
    if (
        not value
        or value != value.strip()
        or "*" in value
        or any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in value
        )
    ):
        return False
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme in {"http", "https"}
        and bool(parsed.hostname)
        and parsed.username is None
        and parsed.password is None
        and "%" not in parsed.netloc
        and "\\" not in parsed.netloc
        and not parsed.netloc.endswith(":")
        and parsed.path == ""
        and parsed.query == ""
        and parsed.fragment == ""
    )


def _is_safe_secret(value: str, minimum: int = 32) -> bool:
    return (
        len(value) >= minimum
        and value == value.strip()
        and not any(
            character.isspace() or ord(character) < 32 or ord(character) == 127
            for character in value
        )
    )


def _are_safe_separate_storage_roots(local_dir: str, temp_dir: str) -> bool:
    if (
        not local_dir
        or not temp_dir
        or "\x00" in local_dir
        or "\x00" in temp_dir
        or not os.path.isabs(local_dir)
        or not os.path.isabs(temp_dir)
    ):
        return False
    try:
        resolved_local = os.path.realpath(local_dir)
        resolved_temp = os.path.realpath(temp_dir)
        common = os.path.commonpath((resolved_local, resolved_temp))
    except (OSError, TypeError, ValueError):
        return False
    return resolved_local != resolved_temp and common not in {resolved_local, resolved_temp}


@dataclass(frozen=True)
class Settings:
    mcp_http_port: int = _get_int("MCP_HTTP_PORT", 8087)
    mcp_bind_address: str = _get_env("MCP_BIND_ADDRESS", "0.0.0.0")
    source_fingerprint: str = _get_env("MCP_SOURCE_FINGERPRINT", "development")
    image_reference: str = _get_env("MCP_IMAGE_REFERENCE", "development")
    mcp_mode: str = _get_env("MCP_MODE", "")
    mcp_access_token: str = _get_env("MCP_ACCESS_TOKEN", "")
    principal_hash_secret: str = _get_env("MCP_PRINCIPAL_HASH_SECRET", "")
    portal_grant_header: str = _get_env("MCP_PORTAL_GRANT_HEADER", "X-MADPANDA-PORTAL-GRANT")
    portal_grant_token: str = _get_env("MCP_PORTAL_GRANT_TOKEN", "")
    portal_subject_header: str = "X-MADPANDA-PORTAL-SUBJECT"
    signup_url: str = _get_env("MCP_SIGNUP_URL", "https://madpanda3d.com/lab/mad-mcps")
    allowed_hosts: list[str] = field(
        default_factory=lambda: _split_csv(
            _get_env("MCP_ALLOWED_HOSTS", "localhost,127.0.0.1,[::1],testserver")
        )
    )
    allowed_origins: list[str] = field(
        default_factory=lambda: _split_csv(_get_env("MCP_ALLOWED_ORIGINS", ""))
    )
    request_body_max_bytes: int = _get_int("MCP_REQUEST_BODY_MAX_BYTES", 131_072)
    request_body_timeout_seconds: float = _get_float("MCP_REQUEST_BODY_TIMEOUT_SECONDS", 10.0)
    response_body_max_bytes: int = _get_int("MCP_RESPONSE_BODY_MAX_BYTES", 2_097_152)
    rate_limit_principal_rpm: int = _get_int("MCP_RATE_LIMIT_PRINCIPAL_RPM", 300)
    rate_limit_window_seconds: int = _get_int("MCP_RATE_LIMIT_WINDOW_SECONDS", 60)
    rate_limit_retry_after_seconds: int = _get_int("MCP_RATE_LIMIT_RETRY_AFTER_SECONDS", 60)
    job_error_max_chars: int = _get_int("JOB_ERROR_MAX_CHARS", 160)
    job_log_max_chars: int = _get_int("JOB_LOG_MAX_CHARS", 512)

    redis_url: str = _get_env("REDIS_URL", "redis://redis:6379/0")
    redis_connect_timeout_seconds: float = _get_float("REDIS_CONNECT_TIMEOUT_SECONDS", 2.0)
    redis_socket_timeout_seconds: float = _get_float("REDIS_SOCKET_TIMEOUT_SECONDS", 5.0)
    redis_maxmemory_bytes: int = _get_int("REDIS_MAXMEMORY_BYTES", 192 * 1024 * 1024)
    queue_name: str = _get_env("QUEUE_NAME", "av-jobs")
    job_admission_owner_max_active: int = _get_int("JOB_ADMISSION_OWNER_MAX_ACTIVE", 4)
    job_admission_global_max_active: int = _get_int("JOB_ADMISSION_GLOBAL_MAX_ACTIVE", 32)
    job_admission_owner_rpm: int = _get_int("JOB_ADMISSION_OWNER_RPM", 30)
    job_admission_execution_buffer_seconds: int = _get_int(
        "JOB_ADMISSION_EXECUTION_BUFFER_SECONDS", 3_720
    )
    metrics_ttl_seconds: int = _get_int("METRICS_TTL_SECONDS", 86_400)
    brand_kit_max_count: int = _get_int("BRAND_KIT_MAX_COUNT", 25)
    brand_kit_max_serialized_bytes: int = _get_int("BRAND_KIT_MAX_SERIALIZED_BYTES", 16_384)
    brand_kit_max_string_chars: int = _get_int("BRAND_KIT_MAX_STRING_CHARS", 256)

    storage_backend: str = _get_env("STORAGE_BACKEND", "local").lower()
    storage_local_dir: str = _get_env("STORAGE_LOCAL_DIR", "/data/assets")
    storage_temp_dir: str = _get_env("STORAGE_TEMP_DIR", "/data/staging")
    storage_staging_max_age_seconds: int = _get_int("STORAGE_STAGING_MAX_AGE_SECONDS", 7_200)
    ingest_staging_owner_max_active: int = _get_int("INGEST_STAGING_OWNER_MAX_ACTIVE", 2)
    ingest_staging_global_max_active: int = _get_int("INGEST_STAGING_GLOBAL_MAX_ACTIVE", 8)
    ingest_staging_owner_max_bytes: int = _get_int("INGEST_STAGING_OWNER_MAX_BYTES", 1_000_000_000)
    ingest_staging_global_max_bytes: int = _get_int(
        "INGEST_STAGING_GLOBAL_MAX_BYTES", 4_000_000_000
    )
    ingest_staging_lease_seconds: int = _get_int("INGEST_STAGING_LEASE_SECONDS", 600)
    ingest_staging_heartbeat_seconds: int = _get_int("INGEST_STAGING_HEARTBEAT_SECONDS", 30)

    s3_bucket: str = _get_env("S3_BUCKET", "")
    s3_region: str = _get_env("S3_REGION", "")
    s3_endpoint_url: str = _get_env("S3_ENDPOINT_URL", "")
    s3_access_key: str = _get_env("S3_ACCESS_KEY", "")
    s3_secret_key: str = _get_env("S3_SECRET_KEY", "")
    s3_connect_timeout_seconds: int = _get_int("S3_CONNECT_TIMEOUT_SECONDS", 10)
    s3_read_timeout_seconds: int = _get_int("S3_READ_TIMEOUT_SECONDS", 60)
    storage_asgi_max_concurrency: int = _get_int("STORAGE_ASGI_MAX_CONCURRENCY", 4)
    storage_asgi_admission_timeout_seconds: float = _get_float(
        "STORAGE_ASGI_ADMISSION_TIMEOUT_SECONDS", 5.0
    )
    storage_asgi_operation_timeout_seconds: float = _get_float(
        "STORAGE_ASGI_OPERATION_TIMEOUT_SECONDS", 120.0
    )
    asset_quota_owner_max_count: int = _get_int("ASSET_QUOTA_OWNER_MAX_COUNT", 100)
    asset_quota_owner_max_bytes: int = _get_int(
        "ASSET_QUOTA_OWNER_MAX_BYTES", 5 * 1024 * 1024 * 1024
    )
    asset_quota_global_max_count: int = _get_int("ASSET_QUOTA_GLOBAL_MAX_COUNT", 400)
    asset_quota_global_max_bytes: int = _get_int(
        "ASSET_QUOTA_GLOBAL_MAX_BYTES", 20 * 1024 * 1024 * 1024
    )
    asset_reservation_lease_seconds: int = _get_int("ASSET_RESERVATION_LEASE_SECONDS", 300)
    asset_reservation_heartbeat_seconds: int = _get_int("ASSET_RESERVATION_HEARTBEAT_SECONDS", 30)
    asset_delete_lease_seconds: int = _get_int("ASSET_DELETE_LEASE_SECONDS", 180)
    asset_delete_retry_base_seconds: int = _get_int("ASSET_DELETE_RETRY_BASE_SECONDS", 60)
    asset_delete_retry_max_seconds: int = _get_int("ASSET_DELETE_RETRY_MAX_SECONDS", 3600)
    job_storage_max_output_count: int = _get_int("JOB_STORAGE_MAX_OUTPUT_COUNT", 25)
    job_storage_max_output_bytes: int = _get_int(
        "JOB_STORAGE_MAX_OUTPUT_BYTES", 2 * 1024 * 1024 * 1024
    )
    job_storage_max_materialize_bytes: int = _get_int(
        "JOB_STORAGE_MAX_MATERIALIZE_BYTES", 2 * 1024 * 1024 * 1024
    )

    public_base_url: str = _get_env("PUBLIC_BASE_URL", "")
    download_url_ttl_seconds: int = _get_int("DOWNLOAD_URL_TTL_SECONDS", 3600)
    download_signing_secret: str = _get_env("DOWNLOAD_SIGNING_SECRET", "")

    max_ingest_bytes: int = _get_int("MAX_INGEST_BYTES", 500_000_000)
    max_output_bytes: int = _get_int("MAX_OUTPUT_BYTES", 500_000_000)
    max_duration_seconds: int = _get_int("MAX_DURATION_SECONDS", 3600)
    ingest_timeout_seconds: int = _get_int("INGEST_TIMEOUT_SECONDS", 300)
    ingest_stream_chunk_bytes: int = _get_int("INGEST_STREAM_CHUNK_BYTES", 65536)
    ingest_range_chunk_bytes: int = _get_int("INGEST_RANGE_CHUNK_BYTES", 8 * 1024 * 1024)
    ingest_allow_http: bool = _get_bool("INGEST_ALLOW_HTTP", False)
    ingest_allow_any_public_domain: bool = _get_bool("INGEST_ALLOW_ANY_PUBLIC_DOMAIN", False)
    ingest_max_redirects: int = _get_int("INGEST_MAX_REDIRECTS", 3)

    asset_ttl_hours: int = _get_int("ASSET_TTL_HOURS", 24)
    max_asset_ttl_hours: int = _get_int("MAX_ASSET_TTL_HOURS", 168)
    job_ttl_hours: int = _get_int("JOB_TTL_HOURS", 24)
    cleanup_interval_seconds: int = _get_int("CLEANUP_INTERVAL_SECONDS", 900)
    job_stale_seconds: int = _get_int("JOB_STALE_SECONDS", 0)

    allowed_domains: list[str] = field(
        default_factory=lambda: _normalize_domains(
            _split_csv(
                _get_env(
                    "ALLOWED_DOMAINS",
                    "cdn.discordapp.com,media.discordapp.net,drive.usercontent.google.com,drive.google.com,docs.google.com",
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
    ffprobe_timeout_seconds: int = _get_int("FFPROBE_TIMEOUT_SECONDS", 60)
    ffmpeg_rlimit_as_bytes: int = _get_int("FFMPEG_RLIMIT_AS_BYTES", 3_221_225_472)
    ffprobe_rlimit_as_bytes: int = _get_int("FFPROBE_RLIMIT_AS_BYTES", 536_870_912)
    ffmpeg_rlimit_cpu_seconds: int = _get_int("FFMPEG_RLIMIT_CPU_SECONDS", 1_800)
    ffprobe_rlimit_cpu_seconds: int = _get_int("FFPROBE_RLIMIT_CPU_SECONDS", 60)
    media_rlimit_nofile: int = _get_int("MEDIA_RLIMIT_NOFILE", 256)
    ffmpeg_threads: int = _get_int("FFMPEG_THREADS", 2)
    max_frame_width: int = _get_int("MAX_FRAME_WIDTH", 8_192)
    max_frame_height: int = _get_int("MAX_FRAME_HEIGHT", 8_192)
    max_frame_pixels: int = _get_int("MAX_FRAME_PIXELS", 33_177_600)
    max_media_streams: int = _get_int("MAX_MEDIA_STREAMS", 16)
    max_video_fps: float = _get_float("MAX_VIDEO_FPS", 120.0)
    max_audio_channels: int = _get_int("MAX_AUDIO_CHANNELS", 8)
    max_audio_sample_rate: int = _get_int("MAX_AUDIO_SAMPLE_RATE", 192_000)
    max_decoded_video_pixel_frames: int = _get_int(
        "MAX_DECODED_VIDEO_PIXEL_FRAMES", 250_000_000_000
    )
    max_decoded_audio_sample_channels: int = _get_int(
        "MAX_DECODED_AUDIO_SAMPLE_CHANNELS", 6_000_000_000
    )
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
    font_default: str = _get_env("FONT_DEFAULT", "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")

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
    max_batch_operations: int = _get_int("MAX_BATCH_OPERATIONS", 100)
    max_render_iterations: int = _get_int("MAX_RENDER_ITERATIONS", 5)
    max_caption_segments: int = _get_int("MAX_CAPTION_SEGMENTS", 200)
    max_caption_word_timings: int = _get_int("MAX_CAPTION_WORD_TIMINGS", 2_000)
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
    queue_names_raw: list[str] = field(
        default_factory=lambda: _split_csv(_get_env("QUEUE_NAMES", ""))
    )

    log_structured: bool = _get_bool("MCP_LOG_STRUCTURED", False)

    discord_bot_token: str = _get_env("DISCORD_BOT_TOKEN", "")
    discord_token_header: str = _get_env("MCP_DISCORD_TOKEN_HEADER", "X-Discord-Bot-Token")
    discord_max_upload_bytes: int = _get_int("DISCORD_MAX_UPLOAD_BYTES", 25_000_000)
    discord_export_enabled: bool = _get_bool("DISCORD_EXPORT_ENABLED", False)
    discord_allowed_channel_ids: list[str] = field(
        default_factory=lambda: _split_csv(_get_env("DISCORD_ALLOWED_CHANNEL_IDS", ""))
    )
    discord_max_response_bytes: int = _get_int("DISCORD_MAX_RESPONSE_BYTES", 1_000_000)
    discord_http_timeout_seconds: int = _get_int("DISCORD_HTTP_TIMEOUT_SECONDS", 60)

    google_drive_credentials_path: str = _get_env("GOOGLE_DRIVE_CREDENTIALS_PATH", "")
    google_drive_impersonate_user: str = _get_env("GOOGLE_DRIVE_IMPERSONATE_USER", "")
    google_drive_folder_default: str = _get_env("GOOGLE_DRIVE_FOLDER_DEFAULT", "")
    google_drive_ingest_enabled: bool = _get_bool("GOOGLE_DRIVE_INGEST_ENABLED", False)
    google_drive_export_enabled: bool = _get_bool("GOOGLE_DRIVE_EXPORT_ENABLED", False)
    google_drive_allowed_file_ids: list[str] = field(
        default_factory=lambda: _split_csv(_get_env("GOOGLE_DRIVE_ALLOWED_FILE_IDS", ""))
    )
    google_drive_allowed_folder_ids: list[str] = field(
        default_factory=lambda: _split_csv(_get_env("GOOGLE_DRIVE_ALLOWED_FOLDER_IDS", ""))
    )

    log_requests: bool = _get_bool("MCP_LOG_REQUESTS", False)
    log_level: str = _get_env("MCP_LOG_LEVEL", "INFO")

    def asset_ttl_seconds(self) -> int:
        return min(max(self.asset_ttl_hours, 1), max(self.max_asset_ttl_hours, 1)) * 3600

    @property
    def request_max_bytes(self) -> int:
        """Compatibility alias; the BODY-named environment variable is canonical."""

        return self.request_body_max_bytes

    @property
    def response_max_bytes(self) -> int:
        """Compatibility alias; the BODY-named environment variable is canonical."""

        return self.response_body_max_bytes

    def job_ttl_seconds(self) -> int:
        return max(self.job_ttl_hours, 1) * 3600

    def job_admission_reservation_ttl_seconds(self) -> int:
        """Keep crash reservations through queue retention plus execution."""

        return self.job_ttl_seconds() + self.job_admission_execution_buffer_seconds

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

    def runtime_errors(self) -> list[str]:
        """Return startup errors without ever including configured secret values."""

        errors: list[str] = []
        if self.mcp_mode not in {"standalone", "portal"}:
            errors.append("MCP_MODE must be exactly standalone or portal")
        if not _is_safe_secret(self.principal_hash_secret):
            errors.append("MCP_PRINCIPAL_HASH_SECRET must contain at least 32 characters")
        if self.mcp_mode == "standalone" and not _is_safe_secret(self.mcp_access_token):
            errors.append("MCP_ACCESS_TOKEN must contain at least 32 characters in standalone mode")
        if self.mcp_mode == "portal" and not _is_safe_secret(self.portal_grant_token):
            errors.append(
                "MCP_PORTAL_GRANT_TOKEN must contain at least 32 characters in portal mode"
            )
        if not re.fullmatch(r"[A-Za-z0-9-]+", self.portal_grant_header):
            errors.append("MCP_PORTAL_GRANT_HEADER must be a valid single header name")
        if self.portal_grant_header.lower() in {
            "authorization",
            self.portal_subject_header.lower(),
            "host",
            "origin",
            "content-length",
            "content-type",
            "accept",
            "transfer-encoding",
            "cookie",
            "x-request-id",
        }:
            errors.append("MCP_PORTAL_GRANT_HEADER must be distinct from protected headers")
        if not re.fullmatch(r"[A-Za-z0-9-]+", self.discord_token_header):
            errors.append("MCP_DISCORD_TOKEN_HEADER must be a valid single header name")
        if self.discord_token_header.lower() in {
            "authorization",
            self.portal_grant_header.lower(),
            self.portal_subject_header.lower(),
            "host",
            "origin",
            "content-length",
            "content-type",
            "accept",
            "transfer-encoding",
            "cookie",
            "x-request-id",
        }:
            errors.append("MCP_DISCORD_TOKEN_HEADER must be distinct from protected headers")
        if not self.allowed_hosts or any(not _is_exact_host(value) for value in self.allowed_hosts):
            errors.append("MCP_ALLOWED_HOSTS must contain exact hosts and no wildcards")
        if any(not _is_exact_origin(value) for value in self.allowed_origins):
            errors.append("MCP_ALLOWED_ORIGINS must contain exact HTTP(S) origins and no wildcards")
        bounded_values = (
            ("MCP_REQUEST_BODY_MAX_BYTES", self.request_body_max_bytes, 1, 10 * 1024 * 1024),
            ("MCP_RESPONSE_BODY_MAX_BYTES", self.response_body_max_bytes, 1, 32 * 1024 * 1024),
            ("MCP_RATE_LIMIT_PRINCIPAL_RPM", self.rate_limit_principal_rpm, 1, 100_000),
            ("MCP_RATE_LIMIT_WINDOW_SECONDS", self.rate_limit_window_seconds, 1, 3_600),
            ("MCP_RATE_LIMIT_RETRY_AFTER_SECONDS", self.rate_limit_retry_after_seconds, 1, 3_600),
            (
                "REDIS_MAXMEMORY_BYTES",
                self.redis_maxmemory_bytes,
                32 * 1024 * 1024,
                192 * 1024 * 1024,
            ),
            (
                "JOB_ADMISSION_OWNER_MAX_ACTIVE",
                self.job_admission_owner_max_active,
                1,
                1_000,
            ),
            (
                "JOB_ADMISSION_GLOBAL_MAX_ACTIVE",
                self.job_admission_global_max_active,
                1,
                10_000,
            ),
            ("JOB_ADMISSION_OWNER_RPM", self.job_admission_owner_rpm, 1, 100_000),
            (
                "JOB_ADMISSION_EXECUTION_BUFFER_SECONDS",
                self.job_admission_execution_buffer_seconds,
                60,
                86_400,
            ),
            ("METRICS_TTL_SECONDS", self.metrics_ttl_seconds, 60, 7 * 24 * 3_600),
            ("BRAND_KIT_MAX_COUNT", self.brand_kit_max_count, 1, 1_000),
            (
                "BRAND_KIT_MAX_SERIALIZED_BYTES",
                self.brand_kit_max_serialized_bytes,
                1_024,
                1024 * 1024,
            ),
            ("BRAND_KIT_MAX_STRING_CHARS", self.brand_kit_max_string_chars, 16, 4_096),
            ("S3_CONNECT_TIMEOUT_SECONDS", self.s3_connect_timeout_seconds, 1, 30),
            ("S3_READ_TIMEOUT_SECONDS", self.s3_read_timeout_seconds, 1, 300),
            ("STORAGE_ASGI_MAX_CONCURRENCY", self.storage_asgi_max_concurrency, 1, 16),
            (
                "STORAGE_STAGING_MAX_AGE_SECONDS",
                self.storage_staging_max_age_seconds,
                60,
                86_400,
            ),
            (
                "INGEST_STAGING_OWNER_MAX_ACTIVE",
                self.ingest_staging_owner_max_active,
                1,
                64,
            ),
            (
                "INGEST_STAGING_GLOBAL_MAX_ACTIVE",
                self.ingest_staging_global_max_active,
                1,
                256,
            ),
            (
                "INGEST_STAGING_OWNER_MAX_BYTES",
                self.ingest_staging_owner_max_bytes,
                1,
                50 * 1024 * 1024 * 1024,
            ),
            (
                "INGEST_STAGING_GLOBAL_MAX_BYTES",
                self.ingest_staging_global_max_bytes,
                1,
                200 * 1024 * 1024 * 1024,
            ),
            (
                "INGEST_STAGING_LEASE_SECONDS",
                self.ingest_staging_lease_seconds,
                60,
                7_200,
            ),
            (
                "INGEST_STAGING_HEARTBEAT_SECONDS",
                self.ingest_staging_heartbeat_seconds,
                5,
                300,
            ),
            ("ASSET_QUOTA_OWNER_MAX_COUNT", self.asset_quota_owner_max_count, 1, 1_000),
            (
                "ASSET_QUOTA_OWNER_MAX_BYTES",
                self.asset_quota_owner_max_bytes,
                1,
                50 * 1024 * 1024 * 1024,
            ),
            ("ASSET_QUOTA_GLOBAL_MAX_COUNT", self.asset_quota_global_max_count, 1, 4_000),
            (
                "ASSET_QUOTA_GLOBAL_MAX_BYTES",
                self.asset_quota_global_max_bytes,
                1,
                200 * 1024 * 1024 * 1024,
            ),
            (
                "ASSET_RESERVATION_LEASE_SECONDS",
                self.asset_reservation_lease_seconds,
                60,
                3_600,
            ),
            (
                "ASSET_RESERVATION_HEARTBEAT_SECONDS",
                self.asset_reservation_heartbeat_seconds,
                5,
                300,
            ),
            ("ASSET_DELETE_LEASE_SECONDS", self.asset_delete_lease_seconds, 60, 3_600),
            (
                "ASSET_DELETE_RETRY_BASE_SECONDS",
                self.asset_delete_retry_base_seconds,
                1,
                3_600,
            ),
            (
                "ASSET_DELETE_RETRY_MAX_SECONDS",
                self.asset_delete_retry_max_seconds,
                60,
                86_400,
            ),
            ("JOB_STORAGE_MAX_OUTPUT_COUNT", self.job_storage_max_output_count, 1, 100),
            (
                "JOB_STORAGE_MAX_OUTPUT_BYTES",
                self.job_storage_max_output_bytes,
                1,
                20 * 1024 * 1024 * 1024,
            ),
            (
                "JOB_STORAGE_MAX_MATERIALIZE_BYTES",
                self.job_storage_max_materialize_bytes,
                1,
                20 * 1024 * 1024 * 1024,
            ),
            ("MAX_INGEST_BYTES", self.max_ingest_bytes, 1, 5_000_000_000),
            ("MAX_OUTPUT_BYTES", self.max_output_bytes, 1, 5_000_000_000),
            ("MAX_DURATION_SECONDS", self.max_duration_seconds, 1, 86_400),
            ("INGEST_TIMEOUT_SECONDS", self.ingest_timeout_seconds, 1, 3_600),
            ("INGEST_STREAM_CHUNK_BYTES", self.ingest_stream_chunk_bytes, 1, 16 * 1024 * 1024),
            ("INGEST_RANGE_CHUNK_BYTES", self.ingest_range_chunk_bytes, 1, 64 * 1024 * 1024),
            ("INGEST_MAX_REDIRECTS", self.ingest_max_redirects, 0, 10),
            ("ASSET_TTL_HOURS", self.asset_ttl_hours, 1, 720),
            ("MAX_ASSET_TTL_HOURS", self.max_asset_ttl_hours, 1, 720),
            ("JOB_TTL_HOURS", self.job_ttl_hours, 1, 720),
            ("CLEANUP_INTERVAL_SECONDS", self.cleanup_interval_seconds, 1, 86_400),
            ("DOWNLOAD_URL_TTL_SECONDS", self.download_url_ttl_seconds, 1, 86_400),
            ("FFMPEG_TIMEOUT_SECONDS", self.ffmpeg_timeout_seconds, 1, 3_600),
            ("FFPROBE_TIMEOUT_SECONDS", self.ffprobe_timeout_seconds, 1, 300),
            (
                "FFMPEG_RLIMIT_AS_BYTES",
                self.ffmpeg_rlimit_as_bytes,
                256 * 1024 * 1024,
                16 * 1024 * 1024 * 1024,
            ),
            (
                "FFPROBE_RLIMIT_AS_BYTES",
                self.ffprobe_rlimit_as_bytes,
                128 * 1024 * 1024,
                4 * 1024 * 1024 * 1024,
            ),
            ("FFMPEG_RLIMIT_CPU_SECONDS", self.ffmpeg_rlimit_cpu_seconds, 1, 3_600),
            ("FFPROBE_RLIMIT_CPU_SECONDS", self.ffprobe_rlimit_cpu_seconds, 1, 300),
            ("MEDIA_RLIMIT_NOFILE", self.media_rlimit_nofile, 32, 4_096),
            ("FFMPEG_THREADS", self.ffmpeg_threads, 1, 4),
            ("MAX_FRAME_WIDTH", self.max_frame_width, 16, 8_192),
            ("MAX_FRAME_HEIGHT", self.max_frame_height, 16, 8_192),
            ("MAX_FRAME_PIXELS", self.max_frame_pixels, 256, 33_177_600),
            ("MAX_MEDIA_STREAMS", self.max_media_streams, 1, 16),
            ("MAX_AUDIO_CHANNELS", self.max_audio_channels, 1, 8),
            ("MAX_AUDIO_SAMPLE_RATE", self.max_audio_sample_rate, 8_000, 192_000),
            ("MAX_CONCAT_CLIPS", self.max_concat_clips, 2, 20),
            ("MAX_SLIDESHOW_IMAGES", self.max_slideshow_images, 1, 60),
            ("MAX_AUDIO_TRACKS", self.max_audio_tracks, 1, 8),
            ("MAX_TEMPLATE_LAYERS", self.max_template_layers, 1, 12),
            ("MAX_TEMPLATE_TEXT_LAYERS", self.max_template_text_layers, 0, 6),
            ("MAX_WORKFLOW_NODES", self.max_workflow_nodes, 1, 40),
            ("MAX_BATCH_ASSETS", self.max_batch_assets, 1, 50),
            ("MAX_BATCH_PRESETS", self.max_batch_presets, 1, 12),
            ("MAX_BATCH_OPERATIONS", self.max_batch_operations, 1, 100),
            ("MAX_RENDER_ITERATIONS", self.max_render_iterations, 1, 5),
            ("MAX_CAPTION_SEGMENTS", self.max_caption_segments, 1, 200),
            ("MAX_CAPTION_WORD_TIMINGS", self.max_caption_word_timings, 1, 2_000),
            ("CAPTION_MAX_CHARS", self.caption_max_chars, 1, 200),
            ("CAPTION_MAX_LINES", self.caption_max_lines, 1, 6),
            ("CAPTION_MAX_WORDS", self.caption_max_words, 1, 30),
            ("MAX_TEXT_CHARS", self.max_text_chars, 1, 200),
            ("MIN_FONT_SIZE", self.min_font_size, 1, 160),
            ("MAX_FONT_SIZE", self.max_font_size, 1, 160),
            ("MAX_BOX_BORDER_WIDTH", self.max_box_border_width, 0, 80),
            ("OVERLAY_MARGIN_PX", self.overlay_margin_px, 0, 8_192),
            ("LOGO_MIN_SCALE_PCT", self.logo_min_scale_pct, 1, 40),
            ("LOGO_MAX_SCALE_PCT", self.logo_max_scale_pct, 1, 40),
            ("DEFAULT_VIDEO_FPS", self.default_video_fps, 1, 120),
            ("DEFAULT_IMAGE_WIDTH", self.default_image_width, 16, 8_192),
            ("DEFAULT_IMAGE_HEIGHT", self.default_image_height, 16, 8_192),
            ("CAPTION_LINE_SPACING", self.caption_line_spacing, 0, 200),
            ("CAPTION_FONT_SIZE", self.caption_font_size, 1, 160),
            ("CAPTION_PADDING_PX", self.caption_padding_px, 0, 400),
            (
                "CAPTION_SAFE_ZONE_BOTTOM_PX",
                self.caption_safe_zone_bottom_px,
                0,
                400,
            ),
            ("CAPTION_SAFE_ZONE_TOP_PX", self.caption_safe_zone_top_px, 0, 400),
            ("AUTO_CAPTION_FONT_SIZE_MIN", self.auto_caption_font_size_min, 1, 160),
            ("AUTO_CAPTION_FONT_SIZE_MAX", self.auto_caption_font_size_max, 1, 160),
            ("AUDIO_SAMPLE_RATE", self.audio_sample_rate, 8_000, 192_000),
            ("AUDIO_DUCKING_ATTACK_MS", self.ducking_attack_ms, 1, 2_000),
            ("AUDIO_DUCKING_RELEASE_MS", self.ducking_release_ms, 1, 5_000),
            ("DRAFT_MAX_DIMENSION", self.draft_max_dimension, 16, 8_192),
            ("DRAFT_CRF", self.draft_crf, 0, 51),
            ("DRAFT_WATERMARK_FONT_SIZE", self.draft_watermark_font_size, 1, 160),
            (
                "MAX_DECODED_VIDEO_PIXEL_FRAMES",
                self.max_decoded_video_pixel_frames,
                1_000_000,
                250_000_000_000,
            ),
            (
                "MAX_DECODED_AUDIO_SAMPLE_CHANNELS",
                self.max_decoded_audio_sample_channels,
                1_000_000,
                6_000_000_000,
            ),
            ("DISCORD_MAX_UPLOAD_BYTES", self.discord_max_upload_bytes, 1, 100_000_000),
            ("DISCORD_MAX_RESPONSE_BYTES", self.discord_max_response_bytes, 1, 16 * 1024 * 1024),
            ("DISCORD_HTTP_TIMEOUT_SECONDS", self.discord_http_timeout_seconds, 1, 120),
            ("JOB_ERROR_MAX_CHARS", self.job_error_max_chars, 32, 1_024),
            ("JOB_LOG_MAX_CHARS", self.job_log_max_chars, 64, 4_096),
        )
        for name, value, minimum, maximum in bounded_values:
            if value < minimum or value > maximum:
                errors.append(f"{name} must be between {minimum} and {maximum}")
        if self.asset_ttl_hours > self.max_asset_ttl_hours:
            errors.append("ASSET_TTL_HOURS must not exceed MAX_ASSET_TTL_HOURS")
        for float_name, float_value, float_minimum, float_maximum in (
            (
                "STORAGE_ASGI_ADMISSION_TIMEOUT_SECONDS",
                self.storage_asgi_admission_timeout_seconds,
                0.1,
                30.0,
            ),
            (
                "STORAGE_ASGI_OPERATION_TIMEOUT_SECONDS",
                self.storage_asgi_operation_timeout_seconds,
                1.0,
                900.0,
            ),
        ):
            if float_value < float_minimum or float_value > float_maximum:
                errors.append(f"{float_name} must be between {float_minimum} and {float_maximum}")
        if self.storage_backend not in {"local", "s3"}:
            errors.append("STORAGE_BACKEND must be exactly local or s3")
        if self.storage_backend == "local" and not _are_safe_separate_storage_roots(
            self.storage_local_dir,
            self.storage_temp_dir,
        ):
            errors.append(
                "STORAGE_LOCAL_DIR and STORAGE_TEMP_DIR must be absolute, distinct, "
                "non-nested paths"
            )
        if self.storage_backend == "s3" and not self.s3_bucket:
            errors.append("S3_BUCKET is required for S3 storage")
        if bool(self.s3_access_key) != bool(self.s3_secret_key):
            errors.append("S3_ACCESS_KEY and S3_SECRET_KEY must be configured together")
        if self.ingest_staging_owner_max_active > self.ingest_staging_global_max_active:
            errors.append("INGEST_STAGING_OWNER_MAX_ACTIVE must not exceed the global active limit")
        if self.ingest_staging_owner_max_bytes > self.ingest_staging_global_max_bytes:
            errors.append("INGEST_STAGING_OWNER_MAX_BYTES must not exceed the global byte limit")
        if self.ingest_staging_owner_max_bytes < self.max_ingest_bytes:
            errors.append("INGEST_STAGING_OWNER_MAX_BYTES must reserve one maximum-size ingest")
        if self.ingest_staging_global_max_bytes < self.max_ingest_bytes:
            errors.append("INGEST_STAGING_GLOBAL_MAX_BYTES must reserve one maximum-size ingest")
        if self.ingest_staging_heartbeat_seconds * 3 > self.ingest_staging_lease_seconds:
            errors.append("INGEST_STAGING_LEASE_SECONDS must be at least three heartbeat intervals")
        minimum_ingest_staging_lease = (
            self.ingest_timeout_seconds
            + self.ffprobe_timeout_seconds
            + math.ceil(self.storage_asgi_operation_timeout_seconds)
            + 60
        )
        if self.ingest_staging_lease_seconds < minimum_ingest_staging_lease:
            errors.append(
                "INGEST_STAGING_LEASE_SECONDS must cover ingest, probe, and storage deadlines"
            )
        if self.asset_quota_owner_max_count > self.asset_quota_global_max_count:
            errors.append("ASSET_QUOTA_OWNER_MAX_COUNT must not exceed the global count quota")
        if self.asset_quota_owner_max_bytes > self.asset_quota_global_max_bytes:
            errors.append("ASSET_QUOTA_OWNER_MAX_BYTES must not exceed the global byte quota")
        largest_asset = max(self.max_ingest_bytes, self.max_output_bytes)
        if self.asset_quota_owner_max_bytes < largest_asset:
            errors.append("ASSET_QUOTA_OWNER_MAX_BYTES must allow one maximum-size asset")
        if self.job_storage_max_output_bytes < self.max_output_bytes:
            errors.append("JOB_STORAGE_MAX_OUTPUT_BYTES must allow one maximum-size output")
        if self.job_storage_max_materialize_bytes < largest_asset:
            errors.append("JOB_STORAGE_MAX_MATERIALIZE_BYTES must allow one maximum-size input")
        if self.job_storage_max_output_count > self.asset_quota_owner_max_count:
            errors.append("JOB_STORAGE_MAX_OUTPUT_COUNT must not exceed the owner asset quota")
        if self.job_storage_max_output_count > self.asset_quota_global_max_count:
            errors.append("JOB_STORAGE_MAX_OUTPUT_COUNT must not exceed the global asset quota")
        if self.job_storage_max_output_bytes > self.asset_quota_owner_max_bytes:
            errors.append("JOB_STORAGE_MAX_OUTPUT_BYTES must not exceed the owner byte quota")
        if self.job_storage_max_output_bytes > self.asset_quota_global_max_bytes:
            errors.append("JOB_STORAGE_MAX_OUTPUT_BYTES must not exceed the global byte quota")
        if self.asset_reservation_heartbeat_seconds * 3 > self.asset_reservation_lease_seconds:
            errors.append(
                "ASSET_RESERVATION_LEASE_SECONDS must be at least three heartbeat intervals"
            )
        minimum_reservation_lease = (
            int(self.storage_asgi_operation_timeout_seconds + self.s3_read_timeout_seconds) + 30
        )
        if self.asset_reservation_lease_seconds < minimum_reservation_lease:
            errors.append("ASSET_RESERVATION_LEASE_SECONDS must cover the bounded storage put")
        if self.asset_delete_retry_base_seconds > self.asset_delete_retry_max_seconds:
            errors.append("ASSET_DELETE_RETRY_BASE_SECONDS must not exceed the maximum retry delay")
        minimum_delete_lease = (
            int(
                max(
                    self.storage_asgi_operation_timeout_seconds,
                    self.s3_connect_timeout_seconds + self.s3_read_timeout_seconds,
                )
            )
            + 30
        )
        if self.asset_delete_lease_seconds < minimum_delete_lease:
            errors.append("ASSET_DELETE_LEASE_SECONDS must cover the bounded S3 delete call")
        if not 1.0 <= self.max_video_fps <= 120.0:
            errors.append("MAX_VIDEO_FPS must be between 1 and 120")
        if self.max_frame_pixels > self.max_frame_width * self.max_frame_height:
            errors.append("MAX_FRAME_PIXELS must not exceed MAX_FRAME_WIDTH times MAX_FRAME_HEIGHT")
        if self.default_image_width * self.default_image_height > self.max_frame_pixels:
            errors.append("default image geometry must not exceed MAX_FRAME_PIXELS")
        if self.default_image_width > self.max_frame_width:
            errors.append("DEFAULT_IMAGE_WIDTH must not exceed MAX_FRAME_WIDTH")
        if self.default_image_height > self.max_frame_height:
            errors.append("DEFAULT_IMAGE_HEIGHT must not exceed MAX_FRAME_HEIGHT")
        if self.default_video_fps > self.max_video_fps:
            errors.append("DEFAULT_VIDEO_FPS must not exceed MAX_VIDEO_FPS")
        if not 0 < self.default_image_duration_sec <= self.max_duration_seconds:
            errors.append("DEFAULT_IMAGE_DURATION_SEC must be positive and bounded by duration")
        if self.min_font_size > self.max_font_size:
            errors.append("MIN_FONT_SIZE must not exceed MAX_FONT_SIZE")
        if self.logo_min_scale_pct > self.logo_max_scale_pct:
            errors.append("LOGO_MIN_SCALE_PCT must not exceed LOGO_MAX_SCALE_PCT")
        if not 0 <= self.logo_max_opacity <= 1:
            errors.append("LOGO_MAX_OPACITY must be between 0 and 1")
        if self.max_template_text_layers > self.max_template_layers:
            errors.append("MAX_TEMPLATE_TEXT_LAYERS must not exceed MAX_TEMPLATE_LAYERS")
        if self.max_batch_presets > self.max_batch_operations:
            errors.append("MAX_BATCH_PRESETS must not exceed MAX_BATCH_OPERATIONS")
        if not self.social_presets or len(self.social_presets) > self.max_batch_presets:
            errors.append("SOCIAL_PRESETS must contain a bounded non-empty preset list")
        if len(self.social_presets) != len(set(self.social_presets)):
            errors.append("SOCIAL_PRESETS must not contain duplicates")
        if not -30 <= self.audio_norm_i <= -5:
            errors.append("AUDIO_NORM_I must be between -30 and -5")
        if not 0 < self.audio_norm_lra <= 20:
            errors.append("AUDIO_NORM_LRA must be greater than zero and at most 20")
        if not -10 <= self.audio_norm_tp <= 0:
            errors.append("AUDIO_NORM_TP must be between -10 and 0")
        if not 1 <= self.ducking_ratio <= 20:
            errors.append("AUDIO_DUCKING_RATIO must be between 1 and 20")
        if not 0 < self.ducking_threshold <= 1:
            errors.append("AUDIO_DUCKING_THRESHOLD must be greater than zero and at most 1")
        if not 0 < self.ducking_music_gain <= 4:
            errors.append("AUDIO_DUCKING_MUSIC_GAIN must be greater than zero and at most 4")
        if not 0 <= self.caption_box_opacity <= 1:
            errors.append("CAPTION_BOX_OPACITY must be between 0 and 1")
        if not 0 <= self.auto_caption_box_opacity_min <= 1:
            errors.append("AUTO_CAPTION_BOX_OPACITY_MIN must be between 0 and 1")
        if not 0 <= self.auto_caption_box_opacity_max <= 1:
            errors.append("AUTO_CAPTION_BOX_OPACITY_MAX must be between 0 and 1")
        if self.auto_caption_box_opacity_min > self.auto_caption_box_opacity_max:
            errors.append(
                "AUTO_CAPTION_BOX_OPACITY_MIN must not exceed AUTO_CAPTION_BOX_OPACITY_MAX"
            )
        if self.auto_caption_font_size_min > self.auto_caption_font_size_max:
            errors.append("AUTO_CAPTION_FONT_SIZE_MIN must not exceed AUTO_CAPTION_FONT_SIZE_MAX")
        if not 0 < self.auto_music_gain_min <= 4:
            errors.append("AUTO_MUSIC_GAIN_MIN must be greater than zero and at most 4")
        if not 0 < self.auto_music_gain_max <= 4:
            errors.append("AUTO_MUSIC_GAIN_MAX must be greater than zero and at most 4")
        if self.auto_music_gain_min > self.auto_music_gain_max:
            errors.append("AUTO_MUSIC_GAIN_MIN must not exceed AUTO_MUSIC_GAIN_MAX")
        if not 0 <= self.auto_max_crop_pct <= 100:
            errors.append("AUTO_MAX_CROP_PCT must be between 0 and 100")
        if not 0 <= self.auto_min_duration_sec <= self.max_duration_seconds:
            errors.append("AUTO_MIN_DURATION_SEC must be bounded by MAX_DURATION_SECONDS")
        if not 0 < self.audio_min_silence_sec <= self.max_duration_seconds:
            errors.append("AUDIO_MIN_SILENCE_SEC must be positive and bounded by duration")
        if not -120 <= self.audio_silence_db <= 0:
            errors.append("AUDIO_SILENCE_DB must be between -120 and 0")
        if not 0 <= self.audio_fade_default_sec <= self.max_duration_seconds:
            errors.append("AUDIO_FADE_DEFAULT_SEC must be bounded by MAX_DURATION_SECONDS")
        if not 0 <= self.draft_watermark_opacity <= 1:
            errors.append("DRAFT_WATERMARK_OPACITY must be between 0 and 1")
        if self.ffprobe_rlimit_as_bytes > self.ffmpeg_rlimit_as_bytes:
            errors.append("FFPROBE_RLIMIT_AS_BYTES must not exceed FFMPEG_RLIMIT_AS_BYTES")
        if self.job_admission_owner_max_active > self.job_admission_global_max_active:
            errors.append(
                "JOB_ADMISSION_OWNER_MAX_ACTIVE must not exceed JOB_ADMISSION_GLOBAL_MAX_ACTIVE"
            )
        if self.storage_backend == "local" and not _is_safe_secret(self.download_signing_secret):
            errors.append(
                "DOWNLOAD_SIGNING_SECRET must contain at least 32 characters for local storage"
            )
        if self.storage_backend == "local" and not _is_exact_origin(self.public_base_url):
            errors.append("PUBLIC_BASE_URL must be an exact HTTP(S) origin for local storage")
        optional_timeouts = {
            "FFMPEG_TEXT_TIMEOUT_SECONDS": self.ffmpeg_text_timeout_seconds,
            "FFMPEG_LOGO_TIMEOUT_SECONDS": self.ffmpeg_logo_timeout_seconds,
            "FFMPEG_CONCAT_TIMEOUT_SECONDS": self.ffmpeg_concat_timeout_seconds,
            "FFMPEG_IMAGE_TIMEOUT_SECONDS": self.ffmpeg_image_timeout_seconds,
            "FFMPEG_SLIDESHOW_TIMEOUT_SECONDS": self.ffmpeg_slideshow_timeout_seconds,
            "FFMPEG_AUDIO_TIMEOUT_SECONDS": self.ffmpeg_audio_timeout_seconds,
            "FFMPEG_TEMPLATE_TIMEOUT_SECONDS": self.ffmpeg_template_timeout_seconds,
            "FFMPEG_WORKFLOW_TIMEOUT_SECONDS": self.ffmpeg_workflow_timeout_seconds,
            "FFMPEG_BATCH_TIMEOUT_SECONDS": self.ffmpeg_batch_timeout_seconds,
        }
        for name, value in optional_timeouts.items():
            if value < 0 or value > 3_600:
                errors.append(f"{name} must be zero or between 1 and 3600")
        maximum_job_runtime = max(
            self.ffmpeg_timeout_seconds,
            *(value or self.ffmpeg_timeout_seconds for value in optional_timeouts.values()),
        )
        if self.storage_staging_max_age_seconds <= max(
            self.ingest_timeout_seconds,
            self.ingest_staging_lease_seconds,
            maximum_job_runtime,
        ):
            errors.append(
                "STORAGE_STAGING_MAX_AGE_SECONDS must exceed the longest configured "
                "ingest or job operation timeout"
            )
        if self.job_admission_execution_buffer_seconds < maximum_job_runtime + 120:
            errors.append(
                "JOB_ADMISSION_EXECUTION_BUFFER_SECONDS must cover the maximum "
                "configured job timeout plus the RQ margin and safety margin"
            )
        if not 0.1 <= self.redis_connect_timeout_seconds <= 10:
            errors.append("REDIS_CONNECT_TIMEOUT_SECONDS must be between 0.1 and 10")
        if not 0.1 <= self.redis_socket_timeout_seconds <= 30:
            errors.append("REDIS_SOCKET_TIMEOUT_SECONDS must be between 0.1 and 30")
        if not 0.1 <= self.request_body_timeout_seconds <= 30:
            errors.append("MCP_REQUEST_BODY_TIMEOUT_SECONDS must be between 0.1 and 30")
        if any(domain == "googleusercontent.com" for domain in self.allowed_domains):
            errors.append("ALLOWED_DOMAINS must not contain broad googleusercontent.com")
        if self.ingest_allow_http:
            errors.append("INGEST_ALLOW_HTTP must remain false")
        if self.ingest_allow_any_public_domain:
            errors.append("INGEST_ALLOW_ANY_PUBLIC_DOMAIN must remain false")
        return errors

    def validate_runtime(self) -> None:
        errors = self.runtime_errors()
        if errors:
            raise RuntimeError("; ".join(errors))

    def validate_worker_runtime(self) -> None:
        """Validate shared processing bounds without requiring HTTP auth secrets."""

        server_only_prefixes = (
            "MCP_MODE ",
            "MCP_ACCESS_TOKEN ",
            "MCP_PORTAL_GRANT_TOKEN ",
            "MCP_PRINCIPAL_HASH_SECRET ",
            "MCP_PORTAL_GRANT_HEADER ",
            "MCP_ALLOWED_HOSTS ",
            "MCP_ALLOWED_ORIGINS ",
            "MCP_REQUEST_BODY_MAX_BYTES ",
            "MCP_REQUEST_BODY_TIMEOUT_SECONDS ",
            "MCP_RESPONSE_BODY_MAX_BYTES ",
            "MCP_RATE_LIMIT_",
            "PUBLIC_BASE_URL ",
            "DOWNLOAD_SIGNING_SECRET ",
        )
        errors = [
            error for error in self.runtime_errors() if not error.startswith(server_only_prefixes)
        ]
        if errors:
            raise RuntimeError("; ".join(errors))


settings = Settings()
