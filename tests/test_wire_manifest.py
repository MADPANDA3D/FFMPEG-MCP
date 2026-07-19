import unittest
from dataclasses import replace
from unittest.mock import AsyncMock, patch

from madpanda_ffmpeg_mcp import server


class WireManifestTests(unittest.IsolatedAsyncioTestCase):
    async def test_exactly_55_individual_tools_match_manifest_wire_schemas(self):
        server.register_tools()
        server.register_tools()
        wire_tools = await server.mcp.list_tools()
        manifest = server._current_tool_manifest()
        descriptors = {item["nativeToolName"]: item for item in manifest["tools"]}
        by_name = {item.name: item for item in wire_tools}

        self.assertEqual(len(wire_tools), 55)
        self.assertEqual(set(by_name), set(server.TOOL_REGISTRY))
        self.assertEqual(set(by_name), set(descriptors))
        self.assertNotIn("FFMPEG_MCP", by_name)
        for name, tool in by_name.items():
            descriptor = descriptors[name]
            self.assertEqual(tool.inputSchema, descriptor["inputSchema"])
            self.assertEqual(tool.outputSchema, descriptor["outputSchema"])
            self.assertEqual(tool.title, descriptor["title"])
            self.assertEqual(tool.description, descriptor["description"])
            self.assertEqual(
                tool.annotations.model_dump(exclude_none=True),
                descriptor["annotations"],
            )
            self.assertEqual(
                tool.meta,
                {
                    "com.madpanda/catalogVersion": descriptor["catalogVersion"],
                    "com.madpanda/descriptorHash": descriptor["descriptorHash"],
                    "com.madpanda/tier": descriptor["tier"],
                },
            )
            internal = server.mcp._tool_manager.get_tool(name)
            self.assertIsNotNone(internal.fn_metadata.output_model)

    async def test_runtime_output_validation_rejects_extra_fields(self):
        descriptors = {
            item["nativeToolName"]: item for item in server._current_tool_manifest()["tools"]
        }
        cases = (
            (
                "ffmpeg_transcode",
                {"job_id": "job-1", "cache_hit": False, "unexpected": True},
            ),
            ("ffmpeg_list_presets", {"presets": [], "unexpected": True}),
        )
        for name, result in cases:

            async def tool_result(result=result):
                return result

            validated = server._validated_tool_function(
                tool_result, descriptors[name]["outputSchema"]
            )
            with (
                self.subTest(tool=name),
                self.assertRaisesRegex(RuntimeError, "declared schema"),
            ):
                await validated()

    async def test_job_readback_schemas_accept_intentional_null_placeholders(self):
        descriptors = {
            item["nativeToolName"]: item for item in server._current_tool_manifest()["tools"]
        }
        cases = (
            ("job_status", server.tool_job_status),
            ("job_progress", server.tool_job_progress),
            ("job_logs", server.tool_job_logs),
        )
        with patch.object(server, "get_job", return_value=None):
            for name, function in cases:
                with self.subTest(tool=name):
                    validated = server._validated_tool_function(
                        function,
                        descriptors[name]["outputSchema"],
                    )
                    result = await validated("missing-job")
                    self.assertEqual(result["status"], "unknown")

        queued = {
            "status": "queued",
            "progress": 0,
            "output_asset_ids": None,
            "error": None,
            "logs_short": None,
            "started_at": None,
            "finished_at": None,
            "cache_hit": False,
        }
        with (
            patch.object(server, "get_job", return_value={"status": "queued"}),
            patch.object(server, "_sync_job_status", return_value=queued),
        ):
            validated = server._validated_tool_function(
                server.tool_job_status,
                descriptors["job_status"]["outputSchema"],
            )
            result = await validated("queued-job")
        self.assertEqual(result["state"], "queued")
        self.assertIsNone(result["output_asset_ids"])

    async def test_confirmation_contract_is_native_and_exact(self):
        manifest = server._current_tool_manifest()
        descriptors = {item["nativeToolName"]: item for item in manifest["tools"]}
        expected = {
            "brand_kit_delete": "DELETE BRAND KIT",
            "media_export_to_drive": "EXPORT TO GOOGLE DRIVE",
            "media_export_to_discord": "EXPORT TO DISCORD",
        }
        required = {
            name
            for name, descriptor in descriptors.items()
            if descriptor["confirmation"]["required"]
        }
        self.assertEqual(required, set(expected))
        for name, phrase in expected.items():
            descriptor = descriptors[name]
            self.assertEqual(descriptor["confirmation"]["parameter"], "confirmation")
            self.assertEqual(descriptor["confirmation"]["exactPhrase"], phrase)
            self.assertIn("confirmation", descriptor["inputSchema"]["required"])
            self.assertIn("confirmation", descriptor["inputSchema"]["properties"])

    async def test_unknown_arguments_are_rejected_at_runtime(self):
        server.register_tools()
        with self.assertRaisesRegex(Exception, "extra_forbidden"):
            await server.mcp._tool_manager.call_tool(
                "template_list", {"unexpected": "ignored-no-longer"}
            )

    async def test_wrong_confirmations_have_no_side_effect(self):
        with (
            patch.object(server, "delete_brand_kit") as delete,
            self.assertRaisesRegex(ValueError, "exactly match"),
        ):
            await server.tool_brand_kit_delete("kit", "wrong")
        delete.assert_not_called()

        drive_settings = replace(server.settings, google_drive_export_enabled=True)
        with (
            patch.object(server, "settings", drive_settings),
            patch.object(server, "get_asset") as get_asset,
            patch.object(server, "upload_file") as upload,
            self.assertRaisesRegex(ValueError, "exactly match"),
        ):
            await server.tool_export_to_drive("asset", "wrong")
        get_asset.assert_not_called()
        upload.assert_not_called()

        discord_settings = replace(server.settings, discord_export_enabled=True)
        with (
            patch.object(server, "settings", discord_settings),
            patch.object(server, "get_asset") as get_asset,
            patch.object(server, "send_file", new=AsyncMock()) as send,
            self.assertRaisesRegex(ValueError, "exactly match"),
        ):
            await server.tool_export_to_discord("asset", "channel", "wrong")
        get_asset.assert_not_called()
        send.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
