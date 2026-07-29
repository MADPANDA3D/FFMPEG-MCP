import sys
import types
import unittest

sys.modules.setdefault("httpx", types.SimpleNamespace(AsyncClient=object))

from discord_export import DiscordExportError, _build_headers


class DiscordExportCredentialTests(unittest.TestCase):
    def test_request_scoped_token_builds_authorization_header(self):
        self.assertEqual(
            _build_headers(
                "user-owned-token-1234567890",
                allow_environment_fallback=False,
            ),
            {"Authorization": "Bot user-owned-token-1234567890"},
        )

    def test_portal_mode_fails_closed_without_request_token(self):
        with self.assertRaisesRegex(
            DiscordExportError,
            "request-scoped Discord BYOK credential",
        ):
            _build_headers("", allow_environment_fallback=False)


if __name__ == "__main__":
    unittest.main()
