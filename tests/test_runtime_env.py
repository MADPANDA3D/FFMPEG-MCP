import stat
import tempfile
import unittest
from pathlib import Path

from scripts import init_runtime_env


class RuntimeEnvironmentInitializerTests(unittest.TestCase):
    def test_portal_mode_requires_and_records_an_exact_public_origin(self) -> None:
        with self.assertRaisesRegex(ValueError, "required in Portal mode"):
            init_runtime_env.build_environment("portal")

        environment = init_runtime_env.build_environment(
            "portal",
            "https://ffmpeg-mcp.example.invalid/",
        )
        self.assertIn("MCP_MODE=portal\n", environment)
        self.assertIn("MCP_ACCESS_TOKEN=\n", environment)
        self.assertIn(
            "PUBLIC_BASE_URL=https://ffmpeg-mcp.example.invalid\n",
            environment,
        )

    def test_public_origin_validation_rejects_ambiguous_or_unroutable_shapes(self) -> None:
        for value in (
            "",
            " ffmpeg.example.invalid",
            "https://*.example.invalid",
            "https://user@example.invalid",
            "https://example.invalid:",
            "https://exa mple.invalid",
            "https://example%2Finvalid",
            "https://example.invalid/path",
            "https://example.invalid?query=1",
            "ftp://example.invalid",
        ):
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, "exact HTTP"):
                init_runtime_env._validated_public_base_url(value)

    def test_environment_creation_is_private_and_never_overwrites(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            self.assertTrue(init_runtime_env.create_environment(env_path, "standalone"))
            original = env_path.read_text(encoding="utf-8")
            self.assertEqual(stat.S_IMODE(env_path.stat().st_mode), 0o600)
            self.assertFalse(init_runtime_env.create_environment(env_path, "standalone"))
            self.assertEqual(env_path.read_text(encoding="utf-8"), original)


if __name__ == "__main__":
    unittest.main()
