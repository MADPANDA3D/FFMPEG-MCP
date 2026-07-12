from dataclasses import replace
import unittest
from unittest.mock import patch

from starlette.testclient import TestClient

import ffmpeg_mcp_server as server


def _ok_app(calls: list[str]):
    async def _app(scope, receive, send):
        calls.append(scope.get("path", ""))
        body = b'{"ok":true}'
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    return _app


def _grant_settings():
    return replace(
        server.settings,
        auth_mode="grant_only",
        portal_grant_header="X-MADPANDA-PORTAL-GRANT",
        portal_grant_secret="test-portal-grant",
    )


class HostedContractTests(unittest.TestCase):
    def test_missing_and_invalid_grants_are_rejected_before_malformed_json_is_read(self):
        calls: list[str] = []
        app = server.build_portal_grant_preparse_wrapper(_ok_app(calls))
        with patch.object(server, "settings", _grant_settings()):
            client = TestClient(app)
            try:
                missing = client.post(
                    "/mcp",
                    headers={"content-type": "application/json"},
                    content=b"{",
                )
                invalid = client.post(
                    "/mcp",
                    headers={
                        "content-type": "application/json",
                        "X-MADPANDA-PORTAL-GRANT": "wrong",
                    },
                    content=b"{",
                )
            finally:
                client.close()

        self.assertEqual(missing.status_code, 401)
        self.assertEqual(invalid.status_code, 401)
        self.assertEqual(missing.json()["error"]["code"], -32001)
        self.assertEqual(invalid.json()["error"]["code"], -32001)
        self.assertEqual(calls, [])

    def test_valid_grant_reaches_the_wrapped_transport(self):
        calls: list[str] = []
        app = server.build_portal_grant_preparse_wrapper(_ok_app(calls))
        with patch.object(server, "settings", _grant_settings()):
            client = TestClient(app)
            try:
                response = client.post(
                    "/mcp",
                    headers={
                        "content-type": "application/json",
                        "X-MADPANDA-PORTAL-GRANT": "test-portal-grant",
                    },
                    json={"jsonrpc": "2.0", "id": 1, "method": "initialize"},
                )
            finally:
                client.close()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(calls, ["/mcp"])

    def test_health_payload_reports_exact_manifest_and_configuration_truth(self):
        configured = replace(
            _grant_settings(),
            redis_url="redis://test",
            storage_backend="local",
            storage_local_dir="/data/assets",
            public_base_url="https://example.invalid",
            download_signing_secret="configured",
        )
        with (
            patch.object(server, "settings", configured),
            patch.object(server.shutil, "which", return_value="/usr/bin/tool"),
            patch.dict(server.os.environ, {"BUILD_SHA": "abcdef1234567890"}, clear=False),
        ):
            payload = server._health_payload()

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["status"], "healthy")
        self.assertEqual(payload["service"], "ffmpeg-mcp")
        self.assertEqual(payload["build_sha"], "abcdef1234567890")
        self.assertEqual(payload["catalog_version"], "2026-07-12.3")
        self.assertEqual(len(payload["descriptor_hash"]), 64)
        self.assertEqual(payload["tool_count"], 55)
        self.assertEqual(payload["raw_tool_count"], 55)
        self.assertEqual(payload["exposed_tool_count"], 55)
        self.assertEqual(payload["agent_ready_tool_count"], 55)
        self.assertEqual(payload["documented_tool_count"], 55)
        self.assertTrue(payload["configuration_ready"])
        self.assertTrue(payload["configuration"]["ready"])
