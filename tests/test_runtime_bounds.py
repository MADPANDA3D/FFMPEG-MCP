import asyncio
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from madpanda_ffmpeg_mcp import config, redis_store, server, worker


def _valid_settings(**overrides):
    values = {
        "mcp_mode": "standalone",
        "mcp_access_token": "a" * 40,
        "principal_hash_secret": "h" * 40,
        "allowed_hosts": ["localhost", "127.0.0.1", "[::1]"],
        "allowed_origins": ["https://portal.example"],
        "ingest_allow_http": False,
        "ingest_allow_any_public_domain": False,
        "public_base_url": "https://downloads.example.invalid",
        "download_signing_secret": "d" * 40,
    }
    values.update(overrides)
    return replace(server.settings, **values)


class ConfigurationBoundTests(unittest.TestCase):
    def test_malformed_environment_values_do_not_fall_back_to_defaults(self):
        with patch.dict("os.environ", {"TEST_EMPTY": ""}):
            self.assertEqual(config._get_env("TEST_EMPTY", "unsafe-default"), "")
        with (
            patch.dict("os.environ", {"TEST_INTEGER": "nope"}),
            self.assertRaisesRegex(ValueError, "TEST_INTEGER"),
        ):
            config._get_int("TEST_INTEGER", 10)
        with (
            patch.dict("os.environ", {"TEST_FLOAT": "NaN"}),
            self.assertRaisesRegex(ValueError, "TEST_FLOAT"),
        ):
            config._get_float("TEST_FLOAT", 1.0)
        with (
            patch.dict("os.environ", {"TEST_BOOL": "yes"}),
            self.assertRaisesRegex(ValueError, "TEST_BOOL"),
        ):
            config._get_bool("TEST_BOOL", False)

    def test_valid_configuration_and_canonical_body_names(self):
        configured = _valid_settings()
        self.assertEqual(configured.runtime_errors(), [])
        self.assertEqual(configured.request_body_max_bytes, 131_072)
        self.assertEqual(configured.response_body_max_bytes, 2_097_152)
        self.assertEqual(configured.request_max_bytes, configured.request_body_max_bytes)

    def test_security_limits_fail_outside_hard_bounds(self):
        invalid = (
            ("request_body_max_bytes", 0, "MCP_REQUEST_BODY_MAX_BYTES"),
            ("request_body_max_bytes", 10 * 1024 * 1024 + 1, "MCP_REQUEST_BODY_MAX_BYTES"),
            ("response_body_max_bytes", 0, "MCP_RESPONSE_BODY_MAX_BYTES"),
            ("max_ingest_bytes", 5_000_000_001, "MAX_INGEST_BYTES"),
            ("max_output_bytes", 0, "MAX_OUTPUT_BYTES"),
            ("max_duration_seconds", 86_401, "MAX_DURATION_SECONDS"),
            ("ffmpeg_timeout_seconds", 3_601, "FFMPEG_TIMEOUT_SECONDS"),
            ("ffprobe_timeout_seconds", 301, "FFPROBE_TIMEOUT_SECONDS"),
            ("max_frame_width", 8_193, "MAX_FRAME_WIDTH"),
            ("max_video_fps", 121, "MAX_VIDEO_FPS"),
            ("max_concat_clips", 21, "MAX_CONCAT_CLIPS"),
            ("max_batch_operations", 101, "MAX_BATCH_OPERATIONS"),
            ("max_render_iterations", 6, "MAX_RENDER_ITERATIONS"),
            ("max_caption_word_timings", 2_001, "MAX_CAPTION_WORD_TIMINGS"),
            ("redis_connect_timeout_seconds", 11, "REDIS_CONNECT_TIMEOUT_SECONDS"),
            ("request_body_timeout_seconds", 31, "MCP_REQUEST_BODY_TIMEOUT_SECONDS"),
            ("discord_http_timeout_seconds", 121, "DISCORD_HTTP_TIMEOUT_SECONDS"),
        )
        for field, value, expected in invalid:
            with self.subTest(field=field):
                errors = _valid_settings(**{field: value}).runtime_errors()
                self.assertTrue(any(expected in error for error in errors), errors)

    def test_media_planning_cross_fields_fail_closed(self):
        cases = (
            (
                {"default_image_width": 8_193},
                "DEFAULT_IMAGE_WIDTH",
            ),
            (
                {"max_template_layers": 4, "max_template_text_layers": 5},
                "MAX_TEMPLATE_TEXT_LAYERS",
            ),
            (
                {"auto_music_gain_min": 2.0, "auto_music_gain_max": 1.0},
                "AUTO_MUSIC_GAIN_MIN",
            ),
            (
                {"social_presets": ["same", "same"]},
                "SOCIAL_PRESETS",
            ),
            (
                {"ffprobe_rlimit_as_bytes": 4_000_000_000},
                "FFPROBE_RLIMIT_AS_BYTES",
            ),
        )
        for overrides, expected in cases:
            with self.subTest(overrides=overrides):
                errors = _valid_settings(**overrides).runtime_errors()
                self.assertTrue(any(expected in error for error in errors), errors)

    def test_storage_lifecycle_configuration_fails_closed(self):
        gib = 1024 * 1024 * 1024
        cases = (
            ({"storage_backend": "filesystem"}, "STORAGE_BACKEND"),
            ({"storage_backend": "s3", "s3_bucket": ""}, "S3_BUCKET"),
            (
                {"s3_access_key": "configured", "s3_secret_key": ""},
                "S3_ACCESS_KEY and S3_SECRET_KEY",
            ),
            (
                {
                    "ingest_staging_owner_max_active": 9,
                    "ingest_staging_global_max_active": 8,
                },
                "INGEST_STAGING_OWNER_MAX_ACTIVE",
            ),
            (
                {
                    "ingest_staging_owner_max_bytes": 5_000_000_001,
                    "ingest_staging_global_max_bytes": 5_000_000_000,
                },
                "INGEST_STAGING_OWNER_MAX_BYTES",
            ),
            (
                {"ingest_staging_owner_max_bytes": 499_999_999},
                "reserve one maximum-size ingest",
            ),
            (
                {"ingest_staging_global_max_bytes": 499_999_999},
                "reserve one maximum-size ingest",
            ),
            (
                {
                    "ingest_staging_lease_seconds": 600,
                    "ingest_staging_heartbeat_seconds": 201,
                },
                "three heartbeat intervals",
            ),
            (
                {"ingest_staging_lease_seconds": 539},
                "cover ingest, probe, and storage deadlines",
            ),
            (
                {"asset_quota_owner_max_count": 401, "asset_quota_global_max_count": 400},
                "global count quota",
            ),
            (
                {"asset_quota_owner_max_bytes": 6 * gib, "asset_quota_global_max_bytes": 5 * gib},
                "global byte quota",
            ),
            (
                {"asset_quota_owner_max_bytes": 499_999_999},
                "allow one maximum-size asset",
            ),
            (
                {"job_storage_max_output_count": 26, "asset_quota_owner_max_count": 25},
                "owner asset quota",
            ),
            (
                {"job_storage_max_output_bytes": 499_999_999},
                "allow one maximum-size output",
            ),
            (
                {"job_storage_max_materialize_bytes": 499_999_999},
                "allow one maximum-size input",
            ),
            (
                {"asset_reservation_lease_seconds": 60, "asset_reservation_heartbeat_seconds": 21},
                "three heartbeat intervals",
            ),
            (
                {"asset_reservation_lease_seconds": 200},
                "cover the bounded storage put",
            ),
            ({"asset_reservation_lease_seconds": 59}, "ASSET_RESERVATION_LEASE_SECONDS"),
            ({"asset_reservation_heartbeat_seconds": 4}, "ASSET_RESERVATION_HEARTBEAT_SECONDS"),
            ({"asset_delete_lease_seconds": 149}, "cover the bounded S3 delete call"),
            (
                {"asset_delete_retry_base_seconds": 61, "asset_delete_retry_max_seconds": 60},
                "must not exceed the maximum retry delay",
            ),
            ({"asset_delete_retry_base_seconds": 0}, "ASSET_DELETE_RETRY_BASE_SECONDS"),
            ({"asset_delete_retry_max_seconds": 59}, "ASSET_DELETE_RETRY_MAX_SECONDS"),
        )
        for overrides, expected in cases:
            with self.subTest(overrides=overrides):
                errors = _valid_settings(**overrides).runtime_errors()
                self.assertTrue(any(expected in error for error in errors), errors)

    def test_ttl_order_origins_secrets_and_broad_ingest_fail_closed(self):
        cases = (
            ({"asset_ttl_hours": 169, "max_asset_ttl_hours": 168}, "ASSET_TTL_HOURS"),
            ({"allowed_origins": ["https://portal.example/path"]}, "MCP_ALLOWED_ORIGINS"),
            ({"allowed_origins": ["https://user@portal.example"]}, "MCP_ALLOWED_ORIGINS"),
            ({"mcp_access_token": "a" * 31}, "MCP_ACCESS_TOKEN"),
            ({"mcp_access_token": "a" * 32 + "\n"}, "MCP_ACCESS_TOKEN"),
            ({"download_signing_secret": "d" * 31}, "DOWNLOAD_SIGNING_SECRET"),
            ({"public_base_url": ""}, "PUBLIC_BASE_URL"),
            ({"public_base_url": "https://downloads.example/path"}, "PUBLIC_BASE_URL"),
            ({"public_base_url": "https://downloads.example:"}, "PUBLIC_BASE_URL"),
            ({"public_base_url": "https://down\nloads.example"}, "PUBLIC_BASE_URL"),
            ({"ingest_allow_http": True}, "INGEST_ALLOW_HTTP"),
            ({"ingest_allow_any_public_domain": True}, "INGEST_ALLOW_ANY_PUBLIC_DOMAIN"),
            ({"allowed_domains": ["googleusercontent.com"]}, "googleusercontent.com"),
        )
        for overrides, expected in cases:
            with self.subTest(overrides=overrides):
                errors = _valid_settings(**overrides).runtime_errors()
                self.assertTrue(any(expected in error for error in errors), errors)

    def test_worker_validation_does_not_require_server_secrets(self):
        configured = _valid_settings(
            mcp_mode="",
            mcp_access_token="",
            portal_grant_token="",
            principal_hash_secret="",
            public_base_url="",
            download_signing_secret="",
        )
        configured.validate_worker_runtime()

    def test_redis_clients_have_finite_timeouts_and_rq_is_binary_safe(self):
        configured = _valid_settings(
            redis_connect_timeout_seconds=1.5,
            redis_socket_timeout_seconds=4.0,
        )
        redis_store._redis_client = None
        with (
            patch.object(redis_store, "settings", configured),
            patch.object(redis_store.redis.Redis, "from_url", return_value=object()) as create,
        ):
            redis_store.get_redis()
        kwargs = create.call_args.kwargs
        self.assertTrue(kwargs["decode_responses"])
        self.assertEqual(kwargs["socket_connect_timeout"], 1.5)
        self.assertEqual(kwargs["socket_timeout"], 4.0)
        self.assertFalse(kwargs["retry_on_timeout"])
        redis_store._redis_client = None
        redis_store._rq_redis_client = None
        with (
            patch.object(redis_store, "settings", configured),
            patch.object(redis_store.redis.Redis, "from_url", return_value=object()) as create,
        ):
            redis_store.get_rq_redis()
        kwargs = create.call_args.kwargs
        self.assertFalse(kwargs["decode_responses"])
        self.assertEqual(kwargs["socket_connect_timeout"], 1.5)
        self.assertEqual(kwargs["socket_timeout"], 4.0)
        self.assertFalse(kwargs["retry_on_timeout"])
        redis_store._rq_redis_client = None


class RequestResponseBoundTests(unittest.IsolatedAsyncioTestCase):
    async def test_chunked_request_stops_at_cumulative_cap(self):
        messages = iter(
            [
                {"type": "http.request", "body": b"1234", "more_body": True},
                {"type": "http.request", "body": b"5678", "more_body": False},
            ]
        )

        async def receive():
            return next(messages)

        with self.assertRaises(server._BodyTooLarge):
            await server._read_bounded_body(receive, 7, 1)

    async def test_slow_chunked_request_hits_authenticated_body_deadline(self):
        calls = 0

        async def receive():
            nonlocal calls
            calls += 1
            if calls == 1:
                return {"type": "http.request", "body": b"1234", "more_body": True}
            await asyncio.sleep(1)
            return {"type": "http.request", "body": b"", "more_body": False}

        with self.assertRaises(TimeoutError):
            await server._read_bounded_body(receive, 32, 0.01)

    async def test_oversized_response_is_buffered_before_any_external_send(self):
        async def inner(scope, receive, send):
            del scope, receive
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send(
                {
                    "type": "http.response.body",
                    "body": b"private-response-bytes",
                    "more_body": False,
                }
            )

        with self.assertRaises(server._ResponseTooLarge):
            await server._call_with_bounded_response(inner, {}, AsyncMock(), 4)

    async def test_optional_integrations_are_off_before_provider_calls(self):
        configured = _valid_settings(
            google_drive_ingest_enabled=False,
            google_drive_export_enabled=False,
            discord_export_enabled=False,
        )
        with (
            patch.object(server, "settings", configured),
            patch.object(server, "ingest_from_url", new=AsyncMock()) as ingest,
            self.assertRaisesRegex(ValueError, "ingest is disabled"),
        ):
            await server.tool_ingest_from_drive("A" * 20)
        ingest.assert_not_awaited()

        with (
            patch.object(server, "settings", configured),
            patch.object(server, "get_asset") as asset,
            self.assertRaisesRegex(ValueError, "export is disabled"),
        ):
            await server.tool_export_to_drive("asset", "EXPORT TO GOOGLE DRIVE")
        asset.assert_not_called()

        with (
            patch.object(server, "settings", configured),
            patch.object(server, "get_asset") as asset,
            self.assertRaisesRegex(ValueError, "export is disabled"),
        ):
            await server.tool_export_to_discord("asset", "channel", "EXPORT TO DISCORD")
        asset.assert_not_called()

    def test_rq_argument_retention_uses_bounded_job_ttl(self):
        queue = SimpleNamespace(enqueue=Mock())
        configured = _valid_settings(job_ttl_hours=2)
        with (
            patch.object(server, "settings", configured),
            patch.object(server, "get_queue", return_value=queue),
            patch.object(server, "require_owner_hash", return_value="a" * 64),
            patch.object(server, "reserve_job_admission"),
            patch.object(server, "save_job"),
            patch.object(server, "record_cache_miss"),
            patch.object(server, "log_event"),
        ):
            server._enqueue_job("test", lambda: None, ())

        kwargs = queue.enqueue.call_args.kwargs
        self.assertEqual(kwargs["ttl"], 7200)
        self.assertEqual(kwargs["result_ttl"], 7200)
        self.assertEqual(kwargs["failure_ttl"], 7200)

    async def test_drive_url_cannot_bypass_file_allowlist(self):
        file_id = "A" * 20
        configured = _valid_settings(
            google_drive_ingest_enabled=True,
            google_drive_allowed_file_ids=["B" * 20],
        )
        with (
            patch.object(server, "settings", configured),
            patch.object(server, "ingest_from_url", new=AsyncMock()) as ingest,
            self.assertRaisesRegex(ValueError, "not allowlisted"),
        ):
            await server.tool_ingest_from_url(
                f"https://drive.google.com/uc?export=download&id={file_id}"
            )
        ingest.assert_not_awaited()

    def test_server_and_worker_export_main(self):
        self.assertTrue(callable(server.main))
        self.assertTrue(callable(worker.main))


if __name__ == "__main__":
    unittest.main()
