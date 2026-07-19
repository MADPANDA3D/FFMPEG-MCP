import importlib.util
import json
import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import call, patch

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "runtime_smoke.py"
SCRIPT_ENV = {
    "MCP_MODE": "standalone",
    "EXPECTED_BUILD_SHA": "test-build",
    "EXPECTED_SOURCE_FINGERPRINT": "test-source",
    "EXPECTED_IMAGE_REFERENCE": "test-image",
}


def _load_runtime_smoke():
    spec = importlib.util.spec_from_file_location("runtime_smoke_under_test", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load runtime smoke script")
    module = importlib.util.module_from_spec(spec)
    with patch.dict(os.environ, SCRIPT_ENV):
        spec.loader.exec_module(module)
    return module


runtime_smoke = _load_runtime_smoke()


class RuntimeSmokeRoundtripTests(unittest.TestCase):
    def test_media_roundtrip_requires_explicit_true_flag(self):
        for value in ("true", " TRUE ", "True"):
            with (
                self.subTest(value=value),
                patch.dict(os.environ, {"MCP_SMOKE_MEDIA_ROUNDTRIP": value}),
            ):
                self.assertTrue(runtime_smoke.media_roundtrip_enabled())

        for value in ("", "false", "1", "yes"):
            with (
                self.subTest(value=value),
                patch.dict(os.environ, {"MCP_SMOKE_MEDIA_ROUNDTRIP": value}),
            ):
                self.assertFalse(runtime_smoke.media_roundtrip_enabled())

    def test_authenticated_owner_matches_runtime_identity_derivation(self):
        from madpanda_ffmpeg_mcp import config
        from madpanda_ffmpeg_mcp.tenant import hash_principal

        secret = "owner-secret-" * 4
        cases = (
            (
                "standalone",
                "standalone-owner",
                "standalone",
            ),
            (
                "portal",
                runtime_smoke.PORTAL_SUBJECT,
                "portal-subject",
            ),
        )
        for mode, principal, namespace in cases:
            expected = hash_principal(principal, secret, namespace=namespace)
            configured = SimpleNamespace(mcp_mode=mode, principal_hash_secret=secret)
            with (
                self.subTest(mode=mode),
                patch.object(runtime_smoke, "MODE", mode),
                patch.object(config, "settings", configured),
            ):
                self.assertEqual(runtime_smoke.authenticated_owner(), expected)

    def test_tool_payload_accepts_structured_or_json_text_results(self):
        structured = {"result": {"structuredContent": {"job_id": "job-1"}}}
        self.assertEqual(runtime_smoke.tool_payload(structured, "tool"), {"job_id": "job-1"})

        text_result = {
            "result": {
                "content": [
                    {"type": "text", "text": json.dumps({"state": "success"})},
                ]
            }
        }
        self.assertEqual(runtime_smoke.tool_payload(text_result, "tool"), {"state": "success"})

        with self.assertRaisesRegex(AssertionError, "MCP error"):
            runtime_smoke.tool_payload({"result": {"isError": True}}, "tool")

    def test_signed_url_dereference_rejects_nonlocal_hosts_before_connecting(self):
        with (
            patch.object(runtime_smoke.http.client, "HTTPConnection") as connection,
            self.assertRaisesRegex(AssertionError, "not local"),
        ):
            runtime_smoke.dereference_local_signed_url(
                "http://downloads.example.invalid/download/asset?exp=1&sig=test"
            )
        connection.assert_not_called()

    def test_cleanup_runs_in_reverse_and_continues_after_delete_failure(self):
        from madpanda_ffmpeg_mcp import redis_store, storage

        owner_hash = "a" * 64
        with (
            patch.object(
                redis_store,
                "get_asset_control",
                side_effect=[{"state": "active"}, {"state": "active"}, None],
            ) as get_control,
            patch.object(storage, "delete_managed_asset", side_effect=[False, True]) as delete,
        ):
            failures = runtime_smoke.cleanup_managed_assets(
                owner_hash,
                ["input-asset", "output-asset"],
            )

        self.assertEqual(failures, ["output-asset"])
        self.assertEqual(
            delete.call_args_list,
            [
                call("output-asset", force=True, owner_hash=owner_hash),
                call("input-asset", force=True, owner_hash=owner_hash),
            ],
        )
        self.assertEqual(
            get_control.call_args_list,
            [call("output-asset"), call("input-asset"), call("input-asset")],
        )


if __name__ == "__main__":
    unittest.main()
