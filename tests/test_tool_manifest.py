import hashlib
import json
import os
import unittest
from unittest.mock import patch

from madpanda_ffmpeg_mcp import server
from madpanda_ffmpeg_mcp.tool_manifest import (
    CATALOG_VERSION,
    SCHEMA_VERSION,
    SERVICE_ID,
    build_tool_manifest,
)


class ToolManifestTests(unittest.TestCase):
    def test_manifest_is_lossless_complete_and_registry_owned(self) -> None:
        manifest = build_tool_manifest(server.TOOL_REGISTRY)

        self.assertEqual(manifest["schemaVersion"], SCHEMA_VERSION)
        self.assertEqual(manifest["serviceId"], SERVICE_ID)
        self.assertEqual(manifest["catalogVersion"], CATALOG_VERSION)
        self.assertEqual(
            set(manifest),
            {
                "schemaVersion",
                "serviceId",
                "catalogVersion",
                "buildSha",
                "descriptorHash",
                "counts",
                "tools",
            },
        )
        self.assertEqual(
            manifest["counts"],
            {
                "raw": 55,
                "agentReady": 55,
                "legacy": 0,
                "hidden": 0,
            },
        )
        self.assertEqual(
            [tool["nativeToolName"] for tool in manifest["tools"]],
            sorted(server.TOOL_REGISTRY),
        )

        required_fields = {
            "serviceId",
            "nativeToolName",
            "canonicalName",
            "aliases",
            "title",
            "description",
            "category",
            "deprecation",
            "inputSchema",
            "outputSchema",
            "annotations",
            "confirmation",
            "documentationUrl",
            "navigationRole",
            "catalogVersion",
            "descriptorHash",
            "tier",
        }
        for descriptor in manifest["tools"]:
            self.assertTrue(required_fields.issubset(descriptor))
            self.assertEqual(set(descriptor), required_fields)
            self.assertEqual(
                descriptor["canonicalName"],
                f"ffmpeg.{descriptor['nativeToolName']}",
            )
            self.assertGreater(len(descriptor["description"]), 80)
            self.assertEqual(
                set(descriptor["deprecation"]),
                {"deprecated", "since", "replacement", "sunsetAt", "message"},
            )
            self.assertEqual(
                set(descriptor["confirmation"]),
                {"required", "parameter", "exactPhrase", "when"},
            )
            self.assertEqual(descriptor["inputSchema"]["type"], "object")
            self.assertFalse(descriptor["inputSchema"]["additionalProperties"])
            for parameter in descriptor["inputSchema"]["properties"].values():
                self.assertTrue(parameter["description"].strip())
            self.assertEqual(descriptor["outputSchema"]["type"], "object")
            self.assertIn("properties", descriptor["outputSchema"])
            self.assertEqual(
                set(descriptor["annotations"]),
                {"readOnlyHint", "destructiveHint", "openWorldHint", "idempotentHint"},
            )
            self.assertTrue(
                all(isinstance(value, bool) for value in descriptor["annotations"].values())
            )
            self.assertIn(descriptor["tier"], {"agent_ready", "legacy", "hidden"})
            self.assertRegex(descriptor["descriptorHash"], r"^[a-f0-9]{64}$")
            unhashed = {key: value for key, value in descriptor.items() if key != "descriptorHash"}
            encoded = json.dumps(
                unhashed,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            self.assertEqual(descriptor["descriptorHash"], hashlib.sha256(encoded).hexdigest())
        self.assertRegex(manifest["descriptorHash"], r"^[a-f0-9]{64}$")
        encoded_tools = json.dumps(
            manifest["tools"],
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        self.assertEqual(manifest["descriptorHash"], hashlib.sha256(encoded_tools).hexdigest())

        capability = next(
            item for item in manifest["tools"] if item["nativeToolName"] == "list_capabilities"
        )
        self.assertEqual(
            capability["outputSchema"]["properties"]["tools"]["items"],
            {"$ref": "#/$defs/toolDescriptor"},
        )

    def test_descriptor_hash_excludes_runtime_build_sha(self) -> None:
        with patch.dict(os.environ, {"MCP_BUILD_SHA": "abcdef1234567"}, clear=False):
            first = build_tool_manifest(server.TOOL_REGISTRY)
        with patch.dict(os.environ, {"MCP_BUILD_SHA": "7654321fedcba"}, clear=False):
            second = build_tool_manifest(server.TOOL_REGISTRY)

        self.assertNotEqual(first["buildSha"], second["buildSha"])
        self.assertEqual(first["descriptorHash"], second["descriptorHash"])
        self.assertEqual(first["tools"], second["tools"])

    def test_registry_drift_fails_closed(self) -> None:
        incomplete = dict(server.TOOL_REGISTRY)
        incomplete.pop("ffmpeg_transcode")
        with self.assertRaisesRegex(ValueError, "registry drift"):
            build_tool_manifest(incomplete)

    def test_destructive_confirmation_is_explicit(self) -> None:
        manifest = build_tool_manifest(server.TOOL_REGISTRY)
        tools = {item["nativeToolName"]: item for item in manifest["tools"]}

        delete = tools["brand_kit_delete"]
        self.assertTrue(delete["annotations"]["destructiveHint"])
        self.assertTrue(delete["confirmation"]["required"])
        self.assertEqual(
            set(delete["confirmation"]),
            {"required", "parameter", "exactPhrase", "when"},
        )
        self.assertEqual(delete["confirmation"]["parameter"], "confirmation")
        self.assertEqual(delete["confirmation"]["exactPhrase"], "DELETE BRAND KIT")
        self.assertIn("native confirmation argument", delete["confirmation"]["when"])
        self.assertEqual(
            set(delete["inputSchema"]["properties"]),
            {"brand_kit_id", "confirmation"},
        )
        self.assertIn("confirmation", delete["inputSchema"]["required"])
        self.assertFalse(tools["brand_kit_get"]["confirmation"]["required"])


class NavigationToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_fastmcp_registry_publishes_titles_descriptions_and_annotations(
        self,
    ) -> None:
        server.register_tools()
        tools = await server.mcp.list_tools()

        self.assertEqual(len(tools), 55)
        by_name = {tool.name: tool for tool in tools}
        navigation = {
            "check_configuration",
            "list_capabilities",
            "get_endpoint_coverage",
            "get_tool_usage",
            "find_tools",
        }
        for name in server.TOOL_REGISTRY:
            tool = by_name[name]
            self.assertTrue(tool.title)
            self.assertTrue(tool.description)
            self.assertIsNotNone(tool.annotations)
            self.assertIsInstance(tool.annotations.readOnlyHint, bool)
            self.assertIsInstance(tool.annotations.destructiveHint, bool)
            self.assertIsInstance(tool.annotations.openWorldHint, bool)
            self.assertIsInstance(tool.annotations.idempotentHint, bool)
            if name in navigation:
                self.assertEqual(tool.outputSchema["type"], "object")

    async def test_list_capabilities_can_return_lossless_manifest(self) -> None:
        result = await server.tool_list_capabilities(include_descriptors=True)

        self.assertEqual(result["schemaVersion"], SCHEMA_VERSION)
        self.assertEqual(result["serviceId"], SERVICE_ID)
        self.assertEqual(len(result["tools"]), 55)
        self.assertEqual(result["counts"]["raw"], 55)
        self.assertRegex(result["descriptorHash"], r"^[a-f0-9]{64}$")

    async def test_list_capabilities_is_compact_by_default(self) -> None:
        result = await server.tool_list_capabilities()

        self.assertEqual(result["tools"], [])
        self.assertEqual(result["counts"]["agentReady"], 55)

    async def test_find_tools_ranks_punctuation_normalized_multi_token_queries(
        self,
    ) -> None:
        result = await server.tool_find_tools("social-ad render")

        self.assertGreater(result["count"], 0)
        self.assertEqual(result["results"][0]["toolName"], "render_social_ad")

        alias_result = await server.tool_find_tools("extract-thumbnail")
        self.assertEqual(alias_result["results"][0]["toolName"], "ffmpeg_thumbnail")

    async def test_find_tools_applies_risk_and_limit_filters(self) -> None:
        result = await server.tool_find_tools("brand kit", risk="destructive", limit=25)

        self.assertEqual(result["count"], 1)
        self.assertEqual(result["results"][0]["toolName"], "brand_kit_delete")
        self.assertEqual(result["results"][0]["risk"], "destructive")

    async def test_tool_usage_resolves_alias_without_executing_provider(self) -> None:
        result = await server.tool_get_tool_usage("extract_thumbnail")

        self.assertEqual(result["tool"]["nativeToolName"], "ffmpeg_thumbnail")
        self.assertIn("time_sec", result["tool"]["inputSchema"]["properties"])

    async def test_endpoint_coverage_accounts_for_every_tool(self) -> None:
        result = await server.tool_get_endpoint_coverage()

        self.assertEqual(result["providerKind"], "local-cli")
        self.assertEqual(len(result["entries"]), 55)
        self.assertTrue(all(item["status"] == "covered" for item in result["entries"]))

    async def test_configuration_readback_contains_presence_only(self) -> None:
        result = await server.tool_check_configuration()

        serialized = repr(result)
        if server.settings.portal_grant_token:
            self.assertNotIn(server.settings.portal_grant_token, serialized)
        if server.settings.discord_bot_token:
            self.assertNotIn(server.settings.discord_bot_token, serialized)
        self.assertEqual(set(result), {"ok", "serviceId", "required", "optional", "missing"})
        self.assertTrue(all(isinstance(value, bool) for value in result["required"].values()))
        self.assertTrue(all(isinstance(value, bool) for value in result["optional"].values()))


if __name__ == "__main__":
    unittest.main()
