import math
import unittest
from dataclasses import replace
from unittest.mock import patch

from madpanda_ffmpeg_mcp import media_limits, public_limits


def _settings(**overrides):
    return replace(public_limits.settings, **overrides)


class PublicNumericLimitTests(unittest.TestCase):
    def test_nan_and_infinity_are_rejected(self):
        cases = (
            (math.nan, False),
            (math.inf, False),
            (-math.inf, False),
            (math.inf, True),
        )
        for value, integer in cases:
            with (
                self.subTest(value=value, integer=integer),
                self.assertRaisesRegex(ValueError, "finite number"),
            ):
                public_limits.finite_number(
                    value,
                    "value",
                    minimum=0,
                    maximum=100,
                    integer=integer,
                    allow_none=False,
                )

    def test_brand_style_payload_rejects_nan_and_unknown_default_preset(self):
        with self.assertRaisesRegex(ValueError, "finite numbers"):
            public_limits.validate_finite_tree(
                {"brand_kit_id": "brand", "logo_opacity": math.nan},
                "brand_kit",
            )

        public_limits.validate_preset_request({}, "mp4_social_vertical_1080x1920")
        with self.assertRaisesRegex(ValueError, "Unknown preset"):
            public_limits.validate_preset_request({}, "not-a-real-preset")


class PublicOperationLimitTests(unittest.TestCase):
    def test_social_default_preset_list_is_subject_to_operation_cap(self):
        configured = _settings(
            max_batch_operations=2,
            social_presets=[
                "mp4_social_vertical_1080x1920",
                "mp4_social_square_1080x1080",
                "mp4_social_portrait_1080x1350",
            ],
        )
        with (
            patch.object(public_limits, "settings", configured),
            self.assertRaisesRegex(ValueError, "operation limit"),
        ):
            public_limits.validate_campaign_plan(
                asset_count=1,
                preset_count=len(configured.social_presets),
                template_name=None,
            )

    def test_campaign_counts_asset_preset_and_template_cross_product(self):
        configured = _settings(max_batch_operations=19)
        with (
            patch.object(public_limits, "settings", configured),
            self.assertRaisesRegex(ValueError, "campaign.*operation limit"),
        ):
            public_limits.validate_campaign_plan(
                asset_count=2,
                preset_count=5,
                template_name="promo_vertical_basic",
            )

    def test_render_rejects_zero_and_excessive_iteration_counts(self):
        configured = _settings(max_render_iterations=3)
        with patch.object(public_limits, "settings", configured):
            for value in (0, 4):
                with (
                    self.subTest(max_iterations=value),
                    self.assertRaisesRegex(ValueError, "max_iterations"),
                ):
                    public_limits.validate_render_request(
                        {"max_iterations": value},
                        iterative=True,
                        template_name="social_ad_basic",
                    )

    def test_workflow_counts_nested_template_layers(self):
        configured = _settings(max_batch_operations=5)
        workflow = {
            "nodes": [
                {
                    "id": "template",
                    "type": "template_apply",
                    "input": "asset-id",
                    "params": {
                        "template_name": "promo_vertical_basic",
                        "variables": {},
                    },
                }
            ]
        }
        with (
            patch.object(public_limits, "settings", configured),
            self.assertRaisesRegex(ValueError, "workflow.*operation limit"),
        ):
            public_limits.validate_workflow(workflow)

    def test_workflow_rejects_dependency_cycle(self):
        workflow = {
            "nodes": [
                {
                    "id": "first",
                    "type": "transcode",
                    "inputs": ["second"],
                    "params": {"preset": "mp4_social_square_1080x1080"},
                },
                {
                    "id": "second",
                    "type": "transcode",
                    "inputs": ["first"],
                    "params": {"preset": "mp4_social_square_1080x1080"},
                },
            ]
        }
        with self.assertRaisesRegex(ValueError, "dependency cycle"):
            public_limits.validate_workflow(workflow)


class PublicDerivedMediaLimitTests(unittest.TestCase):
    @staticmethod
    def _video_asset(**overrides):
        asset = {
            "mime_type": "video/mp4",
            "duration_sec": 10.0,
            "width": 1_920,
            "height": 1_080,
            "fps": 30.0,
        }
        asset.update(overrides)
        return asset

    def test_concat_rejects_aggregate_duration_before_enqueue(self):
        assets = [
            self._video_asset(duration_sec=2_000),
            self._video_asset(duration_sec=2_000),
        ]
        with self.assertRaisesRegex(ValueError, "source duration"):
            public_limits.validate_concat_asset_plan(
                assets,
                target_width=None,
                target_height=None,
                transition=None,
                transition_duration=None,
            )

    def test_concat_partial_dimension_uses_the_exact_first_asset_other_side(self):
        assets = [self._video_asset(height=720), self._video_asset()]
        with patch.object(
            public_limits,
            "validate_planned_video_work",
            wraps=media_limits.validate_planned_video_work,
        ) as validate_work:
            public_limits.validate_concat_asset_plan(
                assets,
                target_width=640,
                target_height=None,
                transition=None,
                transition_duration=None,
            )
        self.assertEqual(validate_work.call_args.args[:2], (640, 720))

    def test_raw_workflow_concat_uses_asset_metadata_before_enqueue(self):
        workflow = {
            "nodes": [
                {
                    "id": "concat",
                    "type": "video_concat",
                    "inputs": ["first", "second"],
                    "params": {},
                }
            ]
        }
        assets = {
            "first": self._video_asset(duration_sec=2_000),
            "second": self._video_asset(duration_sec=2_000),
        }
        with self.assertRaisesRegex(ValueError, "source duration"):
            public_limits.validate_workflow(workflow, asset_resolver=assets.get)

    def test_concat_tiny_target_does_not_hide_source_decode_work(self):
        assets = [
            self._video_asset(duration_sec=1, width=100, height=100, fps=1),
            self._video_asset(duration_sec=1, width=100, height=100, fps=1),
        ]
        configured = _settings(max_decoded_video_pixel_frames=15_000)
        with (
            patch.object(public_limits, "settings", configured),
            patch.object(media_limits, "settings", configured),
            self.assertRaisesRegex(ValueError, "source decode work"),
        ):
            public_limits.validate_concat_asset_plan(
                assets,
                target_width=1,
                target_height=1,
                transition=None,
                transition_duration=None,
            )

    def test_slideshow_rejects_excessive_total_duration(self):
        configured = _settings(max_duration_seconds=10)
        with (
            patch.object(public_limits, "settings", configured),
            patch.object(media_limits, "settings", configured),
            self.assertRaisesRegex(ValueError, "slideshow duration"),
        ):
            public_limits.validate_slideshow_plan(
                2,
                duration_per_image=None,
                durations=[6, 6],
                width=10,
                height=10,
                fps=1,
            )

    def test_slideshow_rejects_excessive_decoded_work(self):
        configured = _settings(
            max_duration_seconds=10,
            max_decoded_video_pixel_frames=1_999,
        )
        with (
            patch.object(public_limits, "settings", configured),
            patch.object(media_limits, "settings", configured),
            self.assertRaisesRegex(ValueError, "slideshow output.*configured limit"),
        ):
            public_limits.validate_slideshow_plan(
                2,
                duration_per_image=1,
                durations=None,
                width=10,
                height=10,
                fps=10,
            )

    def test_proportional_preset_rejects_pathological_derived_geometry(self):
        asset = {"width": 1, "height": 8_192, "duration_sec": 1, "fps": 1}
        with self.assertRaisesRegex(ValueError, "preset output.*configured limit"):
            public_limits.validate_preset_request(asset, "gif_preview_lowfps")

    def test_thumbnail_rejects_pathological_derived_geometry(self):
        asset = {"width": 1, "height": 8_192}
        with self.assertRaisesRegex(ValueError, "thumbnail geometry.*configured limit"):
            public_limits.validate_thumbnail_request(asset, time_sec=0, width=8_192)


if __name__ == "__main__":
    unittest.main()
