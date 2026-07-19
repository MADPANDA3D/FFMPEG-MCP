import json
import unittest
from dataclasses import replace
from unittest.mock import patch

from madpanda_ffmpeg_mcp import server

TOKEN_A = "a" * 40
TOKEN_B = "b" * 40
HASH_SECRET = "h" * 40
GRANT = "g" * 40
SUBJECT = "tenant:example"


def _settings(mode: str = "standalone", **overrides):
    values = {
        "mcp_mode": mode,
        "mcp_access_token": TOKEN_A,
        "principal_hash_secret": HASH_SECRET,
        "portal_grant_token": GRANT,
        "allowed_hosts": ["127.0.0.1", "localhost", "[::1]"],
        "allowed_origins": ["https://portal.example"],
        "rate_limit_principal_rpm": 10,
        "request_body_max_bytes": 512,
        "response_body_max_bytes": 512,
        "public_base_url": "http://127.0.0.1:8087",
        "download_signing_secret": "d" * 40,
    }
    values.update(overrides)
    return replace(server.settings, **values)


def _base_headers(mode: str = "standalone") -> list[tuple[bytes, bytes]]:
    headers = [
        (b"host", b"127.0.0.1:8087"),
        (b"content-type", b"application/json"),
        (b"accept", b"application/json"),
    ]
    if mode == "portal":
        headers.extend(
            [
                (b"x-madpanda-portal-grant", GRANT.encode()),
                (b"x-madpanda-portal-subject", SUBJECT.encode()),
            ]
        )
    else:
        headers.append((b"authorization", f"Bearer {TOKEN_A}".encode()))
    return headers


async def _invoke(
    app,
    *,
    headers: list[tuple[bytes, bytes]],
    body: bytes = b'{"jsonrpc":"2.0","id":1,"method":"initialize"}',
    method: str = "POST",
    path: str = "/mcp",
):
    messages: list[dict] = []
    receive_calls = 0
    sent = False

    async def receive():
        nonlocal receive_calls, sent
        receive_calls += 1
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": b"",
        "headers": headers,
    }
    await app(scope, receive, send)
    status = next(item["status"] for item in messages if item["type"] == "http.response.start")
    response = b"".join(
        item.get("body", b"") for item in messages if item["type"] == "http.response.body"
    )
    return status, response, receive_calls, messages


class AccessModeTests(unittest.IsolatedAsyncioTestCase):
    async def _app(self, configured, captured: dict | None = None):
        captured = captured if captured is not None else {}

        async def inner(scope, receive, send):
            captured["headers"] = list(scope["headers"])
            captured["owner_hash"] = server.require_owner_hash()
            await receive()
            payload = b'{"ok":true}'
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-type", b"application/json")],
                }
            )
            await send({"type": "http.response.body", "body": payload})

        with (
            patch.object(server, "settings", configured),
            patch.object(server.mcp, "streamable_http_app", return_value=inner),
        ):
            return server.build_app()

    async def test_unknown_off_and_router_modes_fail_closed(self):
        for mode in ("", "off", "router", "unknown"):
            with (
                self.subTest(mode=mode),
                patch.object(server, "settings", _settings(mode)),
                self.assertRaisesRegex(RuntimeError, "MCP_MODE"),
            ):
                server.build_app()

    async def test_standalone_exact_bearer_and_stable_owner(self):
        captured: dict = {}
        configured = _settings()
        app = await self._app(configured, captured)
        with (
            patch.object(server, "settings", configured),
            patch.object(server, "_register_principal_hit", return_value=(1, 60)),
        ):
            status, _, _, _ = await _invoke(app, headers=_base_headers())
        self.assertEqual(status, 200)
        first_owner = captured["owner_hash"]

        rotated = _settings(mcp_access_token=TOKEN_B)
        scope = {"headers": [(b"authorization", f"Bearer {TOKEN_B}".encode())]}
        with patch.object(server, "settings", rotated):
            second_owner = server._authenticate(scope)
        self.assertEqual(first_owner, second_owner)
        self.assertNotIn(TOKEN_A, first_owner)

    async def test_unauthorized_duplicate_header_is_rejected_before_body_read(self):
        configured = _settings(request_body_max_bytes=8)
        app = await self._app(configured)
        headers = _base_headers() + [(b"authorization", f"Bearer {TOKEN_A}".encode())]
        with patch.object(server, "settings", configured):
            status, body, reads, _ = await _invoke(app, headers=headers, body=b"{" * 100)
        self.assertEqual(status, 401)
        self.assertEqual(reads, 0)
        self.assertEqual(json.loads(body)["error"]["message"], "Unauthorized")

    async def test_portal_requires_grant_subject_and_rejects_authorization(self):
        configured = _settings("portal")
        app = await self._app(configured)
        with (
            patch.object(server, "settings", configured),
            patch.object(server, "_register_principal_hit", return_value=(1, 60)),
        ):
            valid, _, _, _ = await _invoke(app, headers=_base_headers("portal"))
            invalid, _, reads, _ = await _invoke(
                app,
                headers=_base_headers("portal")
                + [(b"authorization", f"Bearer {TOKEN_A}".encode())],
            )
        self.assertEqual(valid, 200)
        self.assertEqual(invalid, 401)
        self.assertEqual(reads, 0)

    async def test_rate_limit_fails_closed_and_ignores_forwarded_for(self):
        configured = _settings()
        app = await self._app(configured)
        headers = _base_headers() + [(b"x-forwarded-for", b"169.254.169.254")]
        with (
            patch.object(server, "settings", configured),
            patch.object(server, "_register_principal_hit", side_effect=RuntimeError),
        ):
            status, _, reads, _ = await _invoke(app, headers=headers)
        self.assertEqual(status, 503)
        self.assertEqual(reads, 0)

    async def test_validated_headers_are_stripped_before_fastmcp(self):
        captured: dict = {}
        configured = _settings("portal")
        app = await self._app(configured, captured)
        headers = _base_headers("portal") + [
            (b"cookie", b"session=secret"),
            (b"x-forwarded-host", b"evil.example"),
        ]
        with (
            patch.object(server, "settings", configured),
            patch.object(server, "_register_principal_hit", return_value=(1, 60)),
        ):
            status, _, _, _ = await _invoke(app, headers=headers)
        self.assertEqual(status, 200)
        forwarded_names = {name.lower() for name, _ in captured["headers"]}
        self.assertNotIn(b"authorization", forwarded_names)
        self.assertNotIn(b"x-madpanda-portal-grant", forwarded_names)
        self.assertNotIn(b"x-madpanda-portal-subject", forwarded_names)
        self.assertNotIn(b"cookie", forwarded_names)
        self.assertFalse(any(name.startswith(b"x-forwarded-") for name in forwarded_names))

    async def test_exact_origin_and_normalized_ipv6_host(self):
        configured = _settings()
        app = await self._app(configured)
        with (
            patch.object(server, "settings", configured),
            patch.object(server, "_register_principal_hit", return_value=(1, 60)),
        ):
            headers = _base_headers()
            headers[0] = (b"host", b"[::1]:8087")
            headers.append((b"origin", b"https://portal.example"))
            valid, _, _, _ = await _invoke(app, headers=headers)
            headers[-1] = (b"origin", b"https://portal.example/evil")
            invalid, _, _, _ = await _invoke(app, headers=headers)
        self.assertEqual(valid, 200)
        self.assertEqual(invalid, 403)


if __name__ == "__main__":
    unittest.main()
