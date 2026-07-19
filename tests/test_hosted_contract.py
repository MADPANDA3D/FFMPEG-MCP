import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

from madpanda_ffmpeg_mcp import server


def _configured(mode: str):
    return replace(
        server.settings,
        mcp_mode=mode,
        mcp_access_token="a" * 40,
        portal_grant_token="g" * 40,
        principal_hash_secret="h" * 40,
        allowed_hosts=["localhost", "127.0.0.1"],
        allowed_origins=[],
        redis_url="redis://test",
        storage_backend="local",
        storage_local_dir="/data/assets",
        public_base_url="https://example.invalid",
        download_signing_secret="d" * 40,
    )


class HostedContractTests(unittest.TestCase):
    def test_initialize_identity_matches_package_release(self):
        options = server.mcp._mcp_server.create_initialization_options()

        self.assertEqual(options.server_name, "ffmpeg-mcp")
        self.assertEqual(options.server_version, server.__version__)

    def test_configuration_status_is_mode_aware_and_presence_only(self):
        for mode in ("standalone", "portal"):
            configured = _configured(mode)
            with (
                self.subTest(mode=mode),
                patch.object(server, "settings", configured),
                patch.object(server, "get_redis", return_value=SimpleNamespace(ping=lambda: True)),
                patch.object(server.shutil, "which", return_value="/usr/bin/tool"),
            ):
                result = server._configuration_status()
            self.assertTrue(result["ok"], result)
            serialized = repr(result)
            self.assertNotIn(configured.mcp_access_token, serialized)
            self.assertNotIn(configured.portal_grant_token, serialized)
            self.assertNotIn(configured.principal_hash_secret, serialized)

    def test_health_payload_reports_release_identity_and_exact_catalog(self):
        configured = replace(
            _configured("portal"),
            source_fingerprint="source-test",
            image_reference="image-test",
        )
        with (
            patch.object(server, "settings", configured),
            patch.object(server, "get_redis", return_value=SimpleNamespace(ping=lambda: True)),
            patch.object(server.shutil, "which", return_value="/usr/bin/tool"),
            patch.dict(server.os.environ, {"MCP_BUILD_SHA": "abcdef1234567890"}, clear=False),
        ):
            payload = server._health_payload()

        self.assertEqual(payload["status"], "healthy")
        self.assertEqual(payload["server_version"], server.__version__)
        self.assertEqual(payload["build_sha"], "abcdef1234567890")
        self.assertEqual(payload["source_fingerprint"], "source-test")
        self.assertEqual(payload["image_reference"], "image-test")
        self.assertEqual(payload["tool_count"], 55)
        self.assertEqual(payload["raw_tool_count"], 55)
        self.assertEqual(payload["exposed_tool_count"], 55)
        self.assertTrue(payload["configuration_ready"])

    def test_s3_configuration_does_not_require_local_download_signing(self):
        configured = replace(
            _configured("standalone"),
            storage_backend="s3",
            s3_bucket="media",
            s3_access_key="access",
            s3_secret_key="secret",
            public_base_url="",
            download_signing_secret="",
        )
        with (
            patch.object(server, "settings", configured),
            patch.object(server, "get_redis", return_value=SimpleNamespace(ping=lambda: True)),
            patch.object(server.shutil, "which", return_value="/usr/bin/tool"),
        ):
            result = server._configuration_status()

        self.assertTrue(result["ok"], result)
        self.assertTrue(result["required"]["download_signing_configured"])


if __name__ == "__main__":
    unittest.main()
