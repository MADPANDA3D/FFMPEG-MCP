import hashlib
import os
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import httpx

from madpanda_ffmpeg_mcp import ingest
from madpanda_ffmpeg_mcp.redis_store import IngestStagingReservation


def _settings(**overrides):
    values = {
        "allowed_domains": ["media.example.com", "cdn.example.com"],
        "allowed_content_types": ["video/*", "application/octet-stream"],
        "allow_image_ingest": True,
        "ingest_allow_http": False,
        "ingest_allow_any_public_domain": False,
        "ingest_max_redirects": 3,
        "ingest_timeout_seconds": 30,
        "ingest_stream_chunk_bytes": 64,
        "max_ingest_bytes": 1024,
        "asset_ttl_hours": 24,
        "max_asset_ttl_hours": 168,
        "storage_temp_dir": "/tmp",
        "max_duration_seconds": 3600,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


async def _public_dns(host: str, port: int, timeout: float) -> set[str]:
    del host, port, timeout
    return {"93.184.216.34"}


class IngestPolicyTests(unittest.TestCase):
    def test_https_and_exact_allowlist_fail_closed(self):
        with patch.object(ingest, "settings", _settings()):
            with self.assertRaisesRegex(ingest.IngestError, "HTTPS"):
                ingest.normalize_ingest_url("http://media.example.com/video.mp4")
            with self.assertRaisesRegex(ingest.IngestError, "not allowed"):
                ingest.normalize_ingest_url("https://sub.media.example.com/video.mp4")

        with (
            patch.object(ingest, "settings", _settings(allowed_domains=[])),
            self.assertRaisesRegex(ingest.IngestError, "none configured"),
        ):
            ingest.normalize_ingest_url("https://media.example.com/video.mp4")

        with (
            patch.object(
                ingest,
                "settings",
                _settings(
                    allowed_domains=[],
                    ingest_allow_any_public_domain=True,
                    ingest_allow_http=True,
                ),
            ),
            self.assertRaisesRegex(ingest.IngestError, "not allowed"),
        ):
            ingest.normalize_ingest_url("https://public.example/video.mp4")
        with (
            patch.object(
                ingest,
                "settings",
                _settings(ingest_allow_http=True),
            ),
            self.assertRaisesRegex(ingest.IngestError, "HTTPS"),
        ):
            ingest.normalize_ingest_url("http://media.example.com/video.mp4")

    def test_url_credentials_and_drive_query_injection_are_rejected(self):
        with patch.object(
            ingest,
            "settings",
            _settings(allowed_domains=["drive.google.com"]),
        ):
            with self.assertRaisesRegex(ingest.IngestError, "credentials"):
                ingest.normalize_ingest_url(
                    "https://user:password@drive.google.com/file/d/abc123/view"
                )
            with self.assertRaisesRegex(ingest.IngestError, "file id"):
                ingest.normalize_ingest_url("https://drive.google.com/uc?id=abc123%26confirm%3Dt")


class IngestNetworkTests(unittest.IsolatedAsyncioTestCase):
    async def test_private_and_metadata_dns_answers_are_rejected(self):
        for address in ["127.0.0.1", "10.0.0.5", "169.254.169.254", "::1", "ff02::1"]:

            async def resolve(host, port, timeout, answer=address):
                del host, port, timeout
                return {answer}

            with (
                self.subTest(address=address),
                patch.object(ingest, "settings", _settings()),
                patch.object(ingest, "_resolve_host_addresses", side_effect=resolve),
                self.assertRaisesRegex(ingest.IngestError, "non-public"),
            ):
                await ingest._validate_request_target(
                    "https://media.example.com/video.mp4",
                    time.monotonic(),
                )

    async def test_redirect_target_is_validated_before_second_connection(self):
        transport_calls: list[str] = []

        async def resolve(host, port, timeout):
            del port, timeout
            if host == "metadata.example":
                return {"169.254.169.254"}
            return {"93.184.216.34"}

        def handler(request: httpx.Request) -> httpx.Response:
            transport_calls.append(request.url.host)
            return httpx.Response(
                302,
                headers={"location": "https://metadata.example/latest"},
            )

        with (
            patch.object(
                ingest,
                "settings",
                _settings(allowed_domains=["source.example", "metadata.example"]),
            ),
            patch.object(ingest, "_resolve_host_addresses", side_effect=resolve),
        ):
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler),
                follow_redirects=False,
                trust_env=False,
            ) as client:
                with self.assertRaisesRegex(ingest.IngestError, "non-public"):
                    await ingest._request_with_safe_redirects(
                        client,
                        "GET",
                        "https://source.example/video.mp4",
                        time.monotonic(),
                    )

        self.assertEqual(transport_calls, ["source.example"])

    async def test_safe_redirect_is_followed_manually(self):
        transport_calls: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            transport_calls.append(request.url.host)
            if request.url.host == "media.example.com":
                return httpx.Response(
                    302,
                    headers={"location": "https://cdn.example.com/video.mp4"},
                )
            return httpx.Response(200, content=b"ok")

        with (
            patch.object(ingest, "settings", _settings()),
            patch.object(ingest, "_resolve_host_addresses", side_effect=_public_dns),
        ):
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler),
                follow_redirects=False,
                trust_env=False,
            ) as client:
                response = await ingest._request_with_safe_redirects(
                    client,
                    "GET",
                    "https://media.example.com/start",
                    time.monotonic(),
                )
                try:
                    self.assertEqual(response.status_code, 200)
                finally:
                    await response.aclose()

        self.assertEqual(transport_calls, ["media.example.com", "cdn.example.com"])

    async def test_streaming_download_stops_at_byte_limit_and_removes_partial_file(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=b"12345")

        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                patch.object(
                    ingest,
                    "settings",
                    _settings(max_ingest_bytes=4, storage_temp_dir=temp_dir),
                ),
                patch.object(ingest, "_resolve_host_addresses", side_effect=_public_dns),
            ):
                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(handler),
                    follow_redirects=False,
                    trust_env=False,
                ) as client:
                    with self.assertRaisesRegex(ingest.IngestError, "max ingest size"):
                        await ingest._download_streaming(
                            client,
                            "https://media.example.com/video.mp4",
                            temp_dir,
                            time.monotonic(),
                            hashlib.sha256(),
                            ingest._RemoteIngestStagingLease(
                                reservation=IngestStagingReservation(
                                    reservation_id="1" * 32,
                                    token="2" * 32,
                                    owner_hash="a" * 64,
                                    reserved_bytes=4,
                                    lease_until=int(time.time()) + 60,
                                ),
                                confirmed_lease_until=int(time.time()) + 60,
                            ),
                        )
            self.assertEqual(os.listdir(temp_dir), [])

    async def test_requested_ttl_is_bounded_before_network_access(self):
        with (
            patch.object(ingest, "settings", _settings()),
            self.assertRaisesRegex(ingest.IngestError, "between 1 and 168"),
        ):
            await ingest.ingest_from_url(
                "https://media.example.com/video.mp4",
                None,
                169,
            )


class IngestDurationTests(unittest.TestCase):
    def test_audio_and_video_require_finite_positive_duration(self):
        invalid_values = [None, 0, -1, float("nan"), float("inf")]
        with patch.object(ingest, "settings", _settings(max_duration_seconds=60)):
            for value in invalid_values:
                with (
                    self.subTest(value=value),
                    self.assertRaisesRegex(ingest.IngestError, "unavailable or invalid"),
                ):
                    ingest._validate_media_duration(
                        "video/mp4",
                        {"duration_sec": value},
                    )
            with self.assertRaisesRegex(ingest.IngestError, "max duration"):
                ingest._validate_media_duration(
                    "audio/mpeg",
                    {"duration_sec": 61},
                )

    def test_valid_media_and_durationless_images_are_allowed(self):
        with patch.object(ingest, "settings", _settings(max_duration_seconds=60)):
            ingest._validate_media_duration("video/mp4", {"duration_sec": 60})
            ingest._validate_media_duration("image/png", {"duration_sec": None})


if __name__ == "__main__":
    unittest.main()
