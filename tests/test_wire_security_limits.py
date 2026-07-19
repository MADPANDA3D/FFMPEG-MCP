import math
import unittest
from unittest.mock import patch

from jsonschema import Draft202012Validator
from mcp.server.fastmcp.exceptions import ToolError

from madpanda_ffmpeg_mcp import server

SAFE_ASSET = {
    "asset_id": "asset",
    "mime_type": "video/mp4",
    "width": 1_920,
    "height": 1_080,
    "duration_sec": 10.0,
    "fps": 30.0,
}

VIDEO_PRESETS = [
    "mp4_web_720p_small",
    "mp4_web_1080p",
    "mp4_web_480p_tiny",
    "mp4_social_vertical_1080x1920",
    "mp4_social_square_1080x1080",
    "mp4_social_vertical_1080x1920_safe_pad",
    "mp4_social_vertical_720x1280",
    "mp4_social_vertical_720x1280_safe_pad",
    "mp4_social_square_720x720",
    "mp4_social_square_720x720_safe_pad",
    "mp4_social_square_1080x1080_safe_pad",
]


class RegisteredToolInputLimitTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        server.register_tools()

    async def _assert_rejected_without_enqueue(self, tool_name: str, arguments: dict) -> None:
        with (
            patch.object(server, "get_asset", return_value=dict(SAFE_ASSET)),
            patch.object(server, "_enqueue_job") as enqueue,
            self.assertRaises(ToolError),
        ):
            await server.mcp._tool_manager.call_tool(tool_name, arguments)
        enqueue.assert_not_called()

    async def test_nonfinite_and_schema_bounded_inputs_never_enqueue(self) -> None:
        cases = (
            (
                "video_add_logo",
                {
                    "asset_id": "asset",
                    "logo_key": "logo.png",
                    "opacity": math.nan,
                },
            ),
            (
                "batch_export_formats",
                {"asset_id": "asset", "presets": VIDEO_PRESETS + ["mp4_youtube_1920x1080"] * 2},
            ),
            (
                "render_iterate",
                {
                    "render_type": "social_ad",
                    "primary_asset_id": "asset",
                    "max_iterations": 6,
                },
            ),
        )
        for tool_name, arguments in cases:
            with self.subTest(tool_name=tool_name):
                await self._assert_rejected_without_enqueue(tool_name, arguments)

    async def test_invalid_position_highlight_and_strategy_enums_never_enqueue(self) -> None:
        cases = (
            (
                "video_add_text",
                {"asset_id": "asset", "text": "hello", "position": "top-left"},
            ),
            (
                "video_add_logo",
                {"asset_id": "asset", "logo_key": "logo.png", "position": "center"},
            ),
            (
                "captions_burn_in",
                {
                    "asset_id": "asset",
                    "captions_srt": "1\n00:00:00,000 --> 00:00:01,000\nhello",
                    "position": "bottom",
                },
            ),
            (
                "render_social_ad",
                {"primary_asset_id": "asset", "highlight_mode": "sentence"},
            ),
            (
                "render_iterate",
                {
                    "render_type": "social_ad",
                    "primary_asset_id": "asset",
                    "strategy": "unbounded",
                },
            ),
        )
        for tool_name, arguments in cases:
            with self.subTest(tool_name=tool_name):
                await self._assert_rejected_without_enqueue(tool_name, arguments)

    async def test_capabilities_read_back_all_enforced_media_limits(self) -> None:
        capabilities = await server.tool_capabilities()
        limits = capabilities["limits"]
        names = {
            "ffmpeg_rlimit_as_bytes",
            "ffprobe_rlimit_as_bytes",
            "ffmpeg_rlimit_cpu_seconds",
            "ffprobe_rlimit_cpu_seconds",
            "media_rlimit_nofile",
            "ffmpeg_threads",
            "ingest_staging_owner_max_active",
            "ingest_staging_global_max_active",
            "ingest_staging_owner_max_bytes",
            "ingest_staging_global_max_bytes",
            "ingest_staging_lease_seconds",
            "ingest_staging_heartbeat_seconds",
            "max_frame_width",
            "max_frame_height",
            "max_frame_pixels",
            "max_media_streams",
            "max_video_fps",
            "max_audio_channels",
            "max_audio_sample_rate",
            "max_decoded_video_pixel_frames",
            "max_decoded_audio_sample_channels",
            "max_batch_operations",
            "max_render_iterations",
            "max_caption_word_timings",
        }
        self.assertTrue(names <= limits.keys())
        self.assertEqual(limits["text_positions"], ["bottom", "center", "top"])
        self.assertEqual(
            limits["logo_positions"],
            ["bottom-left", "bottom-right", "top-left", "top-right"],
        )
        self.assertEqual(limits["caption_positions"], ["bottom_safe", "mid", "top"])

    async def test_semantic_cross_product_and_workflow_contract_never_enqueue(self) -> None:
        campaign = {
            "asset_ids": [f"asset-{index}" for index in range(10)],
            "presets": VIDEO_PRESETS,
        }
        invalid_workflow = {
            "nodes": [
                {
                    "id": "trim",
                    "type": "trim",
                    "input": "asset",
                    "params": {"preset": "mp4_web_720p_small"},
                }
            ]
        }
        await self._assert_rejected_without_enqueue("campaign_process", campaign)
        await self._assert_rejected_without_enqueue("workflow_run", {"workflow": invalid_workflow})

    async def test_workflow_nested_param_type_errors_never_enqueue(self) -> None:
        invalid_workflows = (
            {
                "nodes": [
                    {
                        "id": "trim",
                        "type": "trim",
                        "input": "asset",
                        "params": {"start_sec": 0, "end_sec": 1, "reencode": "false"},
                    }
                ]
            },
            {
                "nodes": [
                    {
                        "id": "image",
                        "type": "image_to_video",
                        "input": "asset",
                        "params": {"duration_sec": 1, "fps": "30"},
                    }
                ]
            },
            {
                "nodes": [
                    {
                        "id": "image",
                        "type": "image_to_video",
                        "input": "asset",
                        "params": {"duration_sec": True, "fps": 30},
                    }
                ]
            },
            {
                "nodes": [
                    {
                        "id": "mix",
                        "type": "audio_mix",
                        "inputs": ["asset"],
                        "params": {"volumes": {"track": 1}, "normalize": True},
                    }
                ]
            },
            {
                "nodes": [
                    {
                        "id": "template",
                        "type": "template_apply",
                        "input": "asset",
                        "params": {"template_name": "social_ad", "variables": "invalid"},
                    }
                ]
            },
        )
        for workflow in invalid_workflows:
            with (
                self.subTest(workflow=workflow),
                patch.object(server, "validate_workflow") as validate,
            ):
                await self._assert_rejected_without_enqueue("workflow_run", {"workflow": workflow})
            validate.assert_not_called()

    def test_raw_json_parser_rejects_nonfinite_constants(self) -> None:
        self.assertIsNone(server._safe_parse_json(b'{"value": NaN}'))
        self.assertIsNone(server._safe_parse_json(b'{"value": Infinity}'))
        self.assertEqual(server._safe_parse_json(b'{"value": 1}'), {"value": 1})


class PublishedInputSchemaLimitTests(unittest.TestCase):
    def test_manifest_publishes_draft_valid_closed_bounds(self) -> None:
        manifest = server._current_tool_manifest()
        self.assertEqual(manifest["counts"]["raw"], 55)
        tools = {item["nativeToolName"]: item for item in manifest["tools"]}
        for descriptor in tools.values():
            Draft202012Validator.check_schema(descriptor["inputSchema"])

        captions = tools["captions_burn_in"]["inputSchema"]["properties"]["words_json"]
        self.assertEqual(captions["maxItems"], 2_000)
        self.assertFalse(captions["items"]["additionalProperties"])
        self.assertEqual(captions["items"]["properties"]["start"]["maximum"], 86_400)
        self.assertEqual(captions["items"]["properties"]["end"]["maximum"], 86_400)

        campaign = tools["campaign_process"]["inputSchema"]["properties"]
        self.assertEqual(campaign["asset_ids"]["maxItems"], 50)
        self.assertEqual(campaign["presets"]["maxItems"], 12)

        render = tools["render_iterate"]["inputSchema"]["properties"]
        self.assertEqual(render["max_iterations"]["minimum"], 1)
        self.assertEqual(render["max_iterations"]["maximum"], 5)
        self.assertEqual(
            render["strategy"]["enum"],
            ["audio_first", "balanced", "captions_first", "framing_first", None],
        )

        expected_positions = {
            "video_add_text": {"bottom", "center", "top", None},
            "brand_kit_apply": {"bottom", "center", "top", None},
            "video_add_logo": {
                "bottom-left",
                "bottom-right",
                "top-left",
                "top-right",
                None,
            },
            "captions_burn_in": {"bottom_safe", "mid", "top", None},
            "video_analyze": {"bottom_safe", "mid", "top", None},
        }
        for tool_name, expected in expected_positions.items():
            position = tools[tool_name]["inputSchema"]["properties"]["position"]
            self.assertEqual(set(position["enum"]), expected)

        render_social = tools["render_social_ad"]["inputSchema"]["properties"]
        self.assertEqual(
            set(render_social["caption_position"]["enum"]),
            {"bottom_safe", "mid", "top", None},
        )
        self.assertEqual(set(render_social["highlight_mode"]["enum"]), {"word", None})

        workflow = tools["workflow_run"]["inputSchema"]["properties"]["workflow"]
        self.assertFalse(workflow["additionalProperties"])
        self.assertEqual(workflow["properties"]["nodes"]["maxItems"], 40)
        node = workflow["properties"]["nodes"]["items"]
        self.assertFalse(node["additionalProperties"])
        self.assertFalse(node["properties"]["params"]["additionalProperties"])

        workflow_params = node["properties"]["params"]["properties"]
        self.assertEqual(workflow_params["reencode"]["type"], "boolean")
        self.assertEqual(workflow_params["fps"]["type"], "integer")
        self.assertEqual(workflow_params["duration_sec"]["type"], "number")
        self.assertEqual(workflow_params["duration_sec"]["maximum"], 86_400)
        self.assertEqual(workflow_params["durations"]["items"]["maximum"], 86_400)
        self.assertEqual(workflow_params["variables"]["type"], "object")
        self.assertEqual(workflow_params["variables"]["maxProperties"], 100)
        self.assertEqual(workflow_params["transition"]["type"], "string")
        self.assertEqual(workflow_params["transition"]["enum"], ["none", "crossfade"])

        valid_workflow = {
            "nodes": [
                {
                    "id": "trim",
                    "type": "trim",
                    "input": "asset",
                    "params": {"start_sec": 0, "end_sec": 1.25, "reencode": False},
                },
                {
                    "id": "mix",
                    "type": "audio_mix",
                    "inputs": ["asset", "trim"],
                    "params": {
                        "volumes": [0.5, 1.0],
                        "normalize": True,
                        "duration_mode": "shortest",
                    },
                },
            ],
            "outputs": ["mix"],
        }
        workflow_validator = Draft202012Validator(tools["workflow_run"]["inputSchema"])
        self.assertTrue(workflow_validator.is_valid({"workflow": valid_workflow}))
        invalid_logo_position = {
            "nodes": [
                {
                    "id": "logo",
                    "type": "video_add_logo",
                    "input": "asset",
                    "params": {"logo_key": "logo.png", "position": "center"},
                }
            ]
        }
        invalid_text_position = {
            "nodes": [
                {
                    "id": "text",
                    "type": "video_add_text",
                    "input": "asset",
                    "params": {"text": "hello", "position": "top-left"},
                }
            ]
        }
        self.assertFalse(workflow_validator.is_valid({"workflow": invalid_logo_position}))
        self.assertFalse(workflow_validator.is_valid({"workflow": invalid_text_position}))

        ingest_schema = tools["media_ingest_from_url"]["inputSchema"]
        ttl_schema = ingest_schema["properties"]["ttl_hours"]
        self.assertEqual(ttl_schema["maximum"], 720)
        ingest_validator = Draft202012Validator(ingest_schema)
        self.assertTrue(
            ingest_validator.is_valid({"url": "https://example.com/video.mp4", "ttl_hours": 720})
        )
        self.assertFalse(
            ingest_validator.is_valid({"url": "https://example.com/video.mp4", "ttl_hours": 721})
        )

        trim_schema = tools["ffmpeg_trim"]["inputSchema"]
        self.assertEqual(trim_schema["properties"]["start_sec"]["maximum"], 86_400)
        self.assertEqual(trim_schema["properties"]["end_sec"]["maximum"], 86_400)

        discord_schema = tools["media_export_to_discord"]["inputSchema"]
        self.assertEqual(discord_schema["properties"]["message"]["maxLength"], 2_000)
        discord_validator = Draft202012Validator(discord_schema)
        discord_arguments = {
            "asset_id": "asset",
            "channel_id": "channel",
            "confirmation": "EXPORT TO DISCORD",
        }
        self.assertTrue(discord_validator.is_valid({**discord_arguments, "message": "x" * 2_000}))
        self.assertFalse(discord_validator.is_valid({**discord_arguments, "message": "x" * 2_001}))


if __name__ == "__main__":
    unittest.main()
