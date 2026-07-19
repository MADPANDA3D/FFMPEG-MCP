import inspect
import json
import os
import tempfile
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import call, patch

from madpanda_ffmpeg_mcp import ffmpeg_utils, ffprobe_utils, jobs, media_limits, media_process


def _settings(**overrides):
    values = {
        "max_duration_seconds": 3_600,
        "max_frame_width": 8_192,
        "max_frame_height": 8_192,
        "max_frame_pixels": 33_177_600,
        "max_media_streams": 16,
        "max_video_fps": 120.0,
        "max_audio_channels": 8,
        "max_audio_sample_rate": 192_000,
        "max_decoded_video_pixel_frames": 250_000_000_000,
        "max_decoded_audio_sample_channels": 6_000_000_000,
    }
    values.update(overrides)
    return replace(media_limits.settings, **values)


def _video_stream(**overrides):
    stream = {
        "index": 0,
        "codec_type": "video",
        "codec_name": "h264",
        "width": 1_920,
        "height": 1_080,
        "duration": 10.0,
        "avg_frame_rate": 30.0,
        "r_frame_rate": 30.0,
        "nb_frames": 300,
        "disposition": {"attached_pic": 0},
    }
    stream.update(overrides)
    return stream


class MediaProbeLimitTests(unittest.TestCase):
    def test_selected_video_stream_skips_cover_art_and_keeps_absolute_index(self):
        cover = _video_stream(index=2, disposition={"attached_pic": 1})
        content = _video_stream(index=7, width=1_280, height=720)
        probe = {"duration_sec": 10.0, "streams": [cover, content]}
        self.assertEqual(media_limits.selected_video_stream_index(probe), 7)
        self.assertEqual(media_limits.selected_video_stream(probe), content)

    def test_every_stream_is_checked_not_only_the_first(self):
        probe = {
            "duration_sec": 10.0,
            "width": 1_920,
            "height": 1_080,
            "streams": [_video_stream(), _video_stream(index=1, width=9_000)],
        }
        with (
            patch.object(media_limits, "settings", _settings()),
            self.assertRaisesRegex(media_limits.MediaLimitError, "width exceeds"),
        ):
            media_limits.validate_media_probe(probe, expected_kind="video")

    def test_alternate_and_implied_rates_cannot_hide_hostile_fps(self):
        cases = (
            _video_stream(avg_frame_rate=30.0, r_frame_rate=240.0),
            _video_stream(duration=1.0, avg_frame_rate=None, r_frame_rate=None, nb_frames=121),
        )
        with patch.object(media_limits, "settings", _settings()):
            for stream in cases:
                with (
                    self.subTest(stream=stream),
                    self.assertRaisesRegex(media_limits.MediaLimitError, "frame rate exceeds"),
                ):
                    media_limits.validate_media_probe(
                        {"duration_sec": stream["duration"], "streams": [stream]},
                        expected_kind="video",
                    )

    def test_unknown_fps_uses_configured_ceiling_for_decoded_work(self):
        stream = _video_stream(
            width=100,
            height=100,
            duration=10.0,
            avg_frame_rate=None,
            r_frame_rate=None,
            nb_frames=None,
        )
        configured = _settings(max_decoded_video_pixel_frames=11_999_999)
        with (
            patch.object(media_limits, "settings", configured),
            self.assertRaisesRegex(media_limits.MediaLimitError, "decoded work"),
        ):
            media_limits.validate_media_probe(
                {"duration_sec": 10.0, "streams": [stream]}, expected_kind="video"
            )

    def test_zero_duration_still_is_treated_as_durationless(self):
        stream = _video_stream(
            width=640,
            height=480,
            duration=0.0,
            avg_frame_rate=25.0,
            r_frame_rate=25.0,
            nb_frames=1,
        )
        with patch.object(media_limits, "settings", _settings()):
            media_limits.validate_media_probe(
                {"duration_sec": None, "streams": [stream]}, expected_kind="image"
            )

    def test_stream_duration_is_used_when_container_duration_is_missing(self):
        video = _video_stream(duration=8.0, nb_frames=240)
        audio = {
            "index": 1,
            "codec_type": "audio",
            "codec_name": "aac",
            "duration": 8.0,
            "sample_rate": 48_000,
            "channels": 2,
        }
        with patch.object(media_limits, "settings", _settings()):
            media_limits.validate_media_probe(
                {"duration_sec": None, "streams": [video, audio]}, expected_kind="video"
            )

    def test_attached_picture_counts_once_without_hostile_fps_semantics(self):
        audio = {
            "index": 0,
            "codec_type": "audio",
            "codec_name": "aac",
            "duration": 10.0,
            "sample_rate": 48_000,
            "channels": 2,
        }
        cover = _video_stream(
            index=1,
            width=1_000,
            height=1_000,
            avg_frame_rate="999999/1",
            r_frame_rate="999999/1",
            nb_frames=None,
            disposition={"attached_pic": 1},
        )
        with patch.object(media_limits, "settings", _settings()):
            media_limits.validate_media_probe(
                {"duration_sec": 10.0, "streams": [audio, cover]}, expected_kind="audio"
            )

    def test_derived_preset_work_is_bounded_not_only_dimensions(self):
        probe = {
            "duration_sec": 3_600.0,
            "width": 1_920,
            "height": 1_080,
            "fps": 120.0,
            "streams": [_video_stream(duration=3_600.0, nb_frames=432_000)],
        }
        preset = {"ffmpeg_args": ["-vf", "scale=1920:1080"]}
        with (
            patch.object(media_limits, "settings", _settings()),
            self.assertRaisesRegex(media_limits.MediaLimitError, "decoded work"),
        ):
            media_limits.validate_preset_geometry(probe, preset)

    def test_derived_preset_checks_every_selectable_video_stream(self):
        probe = {
            "duration_sec": 1.0,
            "streams": [
                _video_stream(index=0, duration=1.0, nb_frames=30),
                _video_stream(
                    index=3,
                    width=8_192,
                    height=1,
                    duration=1.0,
                    nb_frames=30,
                ),
            ],
        }
        preset = {
            "mime_type": "video/mp4",
            "ffmpeg_args": ["-vf", "scale=-2:720"],
        }
        with (
            patch.object(media_limits, "settings", _settings()),
            self.assertRaisesRegex(media_limits.MediaLimitError, "width exceeds"),
        ):
            media_limits.validate_preset_geometry(probe, preset)

    def test_concat_rejects_aggregate_duration_and_preserves_partial_dimension_defaults(self):
        probes = [
            {"duration_sec": 2_000.0, "streams": [_video_stream(index=0, duration=2_000.0)]},
            {"duration_sec": 2_000.0, "streams": [_video_stream(index=4, duration=2_000.0)]},
        ]
        with (
            patch.object(media_limits, "settings", _settings()),
            self.assertRaisesRegex(media_limits.MediaLimitError, "source duration"),
        ):
            media_limits.validate_concat_probe_plan(
                probes,
                target_width=None,
                target_height=None,
                transition="none",
                transition_duration=0,
            )

        short_probes = [
            {
                "duration_sec": 1.0,
                "streams": [_video_stream(index=0, duration=1.0, nb_frames=30)],
            },
            {
                "duration_sec": 1.0,
                "streams": [_video_stream(index=4, duration=1.0, nb_frames=30)],
            },
        ]
        with patch.object(media_limits, "settings", _settings()):
            width, height, duration = media_limits.validate_concat_probe_plan(
                short_probes,
                target_width=640,
                target_height=None,
                transition="none",
                transition_duration=0,
            )
        self.assertEqual((width, height, duration), (640, 1_080, 2.0))

    def test_concat_crossfade_still_bounds_all_source_decode_work(self):
        stream_values = {
            "width": 10,
            "height": 10,
            "duration": 10.0,
            "avg_frame_rate": 10.0,
            "r_frame_rate": 10.0,
            "nb_frames": 100,
        }
        probes = [
            {
                "duration_sec": 10.0,
                "streams": [_video_stream(index=0, **stream_values)],
            },
            {
                "duration_sec": 10.0,
                "streams": [_video_stream(index=4, **stream_values)],
            },
        ]
        configured = _settings(max_decoded_video_pixel_frames=11_000)
        with (
            patch.object(media_limits, "settings", configured),
            self.assertRaisesRegex(media_limits.MediaLimitError, "aggregate source decode work"),
        ):
            media_limits.validate_concat_probe_plan(
                probes,
                target_width=10,
                target_height=10,
                transition="crossfade",
                transition_duration=9,
            )

    def test_concat_tiny_target_does_not_hide_aggregate_source_decode_work(self):
        stream_values = {
            "width": 100,
            "height": 100,
            "duration": 1.0,
            "avg_frame_rate": 1.0,
            "r_frame_rate": 1.0,
            "nb_frames": 1,
        }
        probes = [
            {"duration_sec": 1.0, "streams": [_video_stream(index=0, **stream_values)]},
            {"duration_sec": 1.0, "streams": [_video_stream(index=4, **stream_values)]},
        ]
        configured = _settings(max_decoded_video_pixel_frames=15_000)
        with (
            patch.object(media_limits, "settings", configured),
            self.assertRaisesRegex(media_limits.MediaLimitError, "source decode work"),
        ):
            media_limits.validate_concat_probe_plan(
                probes,
                target_width=1,
                target_height=1,
                transition="none",
                transition_duration=0,
            )


class MediaSubprocessPolicyTests(unittest.TestCase):
    def test_environment_is_fixed_and_does_not_inherit_fontconfig_or_loader_values(self):
        with patch.dict(
            os.environ,
            {
                "FONTCONFIG_PATH": "/attacker/fonts",
                "LD_PRELOAD": "/attacker/library.so",
                "HTTP_PROXY": "http://attacker.invalid",
            },
        ):
            environment = media_process.sanitized_media_environment("/tmp/staging")
        self.assertNotIn("FONTCONFIG_PATH", environment)
        self.assertNotIn("LD_PRELOAD", environment)
        self.assertNotIn("HTTP_PROXY", environment)
        self.assertEqual(environment["HOME"], "/tmp")
        self.assertEqual(environment["AV_LOG_FORCE_NOCOLOR"], "1")
        self.assertEqual(environment["OMP_NUM_THREADS"], "1")
        self.assertEqual(environment["OPENBLAS_NUM_THREADS"], "1")

    def test_stream_specifier_thread_overrides_are_rejected(self):
        configured = SimpleNamespace(
            ffmpeg_bin="ffmpeg",
            ffmpeg_timeout_seconds=30,
            max_output_bytes=1_024,
        )
        with (
            patch.object(ffmpeg_utils, "settings", configured),
            patch.object(ffmpeg_utils, "run_bounded_process") as run,
        ):
            for option in ("-threads", "-threads:v", "-threads:v:0", "-threads=99"):
                with (
                    self.subTest(option=option),
                    self.assertRaisesRegex(ffmpeg_utils.FfmpegError, "managed by server policy"),
                ):
                    ffmpeg_utils.run_ffmpeg(
                        ["-i", "/tmp/input.mp4", option, "99", "/tmp/output.mp4"]
                    )
        run.assert_not_called()

    def test_ffprobe_reports_conservative_maximum_fps_and_canonical_stream_values(self):
        payload = json.dumps(
            {
                "format": {"duration": "2"},
                "streams": [
                    {
                        "index": 0,
                        "codec_type": "video",
                        "codec_name": "h264",
                        "width": 640,
                        "height": 480,
                        "duration": "2.000000",
                        "avg_frame_rate": "24/1",
                        "r_frame_rate": "60/1",
                        "nb_frames": "180",
                        "disposition": {"attached_pic": 0},
                    }
                ],
            }
        ).encode()
        configured = SimpleNamespace(ffprobe_bin="ffprobe", ffprobe_timeout_seconds=20)
        with (
            tempfile.NamedTemporaryFile() as handle,
            patch.object(ffprobe_utils, "settings", configured),
            patch.object(
                ffprobe_utils,
                "run_bounded_process",
                return_value=(0, payload, b"", False),
            ),
        ):
            probe = ffprobe_utils.run_ffprobe(handle.name)
        self.assertEqual(probe["fps"], 90.0)
        self.assertEqual(probe["streams"][0]["duration"], 2.0)
        self.assertEqual(probe["streams"][0]["avg_frame_rate"], 24.0)
        self.assertEqual(probe["streams"][0]["r_frame_rate"], 60.0)
        self.assertEqual(probe["streams"][0]["nb_frames"], 180)

    def test_ffprobe_normalizes_zero_format_duration_for_stills(self):
        payload = json.dumps(
            {
                "format": {"duration": "0.000000"},
                "streams": [
                    {
                        "index": 0,
                        "codec_type": "video",
                        "codec_name": "png",
                        "width": 640,
                        "height": 480,
                        "duration": "0.000000",
                        "avg_frame_rate": "25/1",
                        "r_frame_rate": "25/1",
                        "nb_frames": "1",
                        "disposition": {"attached_pic": 0},
                    }
                ],
            }
        ).encode()
        configured = SimpleNamespace(ffprobe_bin="ffprobe", ffprobe_timeout_seconds=20)
        with (
            tempfile.NamedTemporaryFile() as handle,
            patch.object(ffprobe_utils, "settings", configured),
            patch.object(
                ffprobe_utils,
                "run_bounded_process",
                return_value=(0, payload, b"", False),
            ),
        ):
            probe = ffprobe_utils.run_ffprobe(handle.name)
        self.assertIsNone(probe["duration_sec"])
        self.assertIsNone(probe["streams"][0]["duration"])


class WorkerMediaGateTests(unittest.TestCase):
    def test_input_cleanup_continues_after_one_unlink_failure(self):
        with patch.object(jobs.os, "remove", side_effect=(OSError("busy"), None)) as remove:
            jobs._cleanup_inputs([("first.tmp", True), ("second.tmp", True), ("kept.tmp", False)])

        self.assertEqual(
            remove.call_args_list,
            [call("first.tmp"), call("second.tmp")],
        )

    def test_output_probe_failure_prevents_persistence_for_every_media_family(self):
        configured = SimpleNamespace(max_output_bytes=1_024)
        for mime_type, extension in (
            ("video/mp4", ".mp4"),
            ("audio/mp4", ".m4a"),
            ("image/jpeg", ".jpg"),
        ):
            with tempfile.NamedTemporaryFile() as handle:
                handle.write(b"media")
                handle.flush()
                with (
                    self.subTest(mime_type=mime_type),
                    patch.object(jobs, "settings", configured),
                    patch.object(
                        jobs,
                        "validate_media_file",
                        side_effect=media_limits.MediaLimitError("hostile"),
                    ),
                    patch.object(jobs, "persist_asset") as persist_asset,
                    self.assertRaisesRegex(jobs.JobError, "configured limit"),
                ):
                    jobs._create_output_asset(
                        handle.name,
                        mime_type,
                        extension,
                        "parent",
                        60,
                    )
                persist_asset.assert_not_called()

    def test_validated_probe_overrides_optional_metadata_before_persistence(self):
        configured = SimpleNamespace(max_output_bytes=1_024)
        trusted_probe = {
            "duration_sec": None,
            "width": 640,
            "height": 480,
            "fps": 25.0,
            "video_codec": "mjpeg",
            "audio_codec": None,
            "streams": [_video_stream(width=640, height=480, duration=0, nb_frames=1)],
        }
        with (
            tempfile.NamedTemporaryFile() as handle,
            patch.object(jobs, "settings", configured),
            patch.object(jobs, "validate_media_file", return_value=trusted_probe),
            patch.object(
                jobs,
                "persist_asset",
                side_effect=lambda _path, asset, _extension, **_kwargs: asset,
            ) as persist_asset,
        ):
            handle.write(b"media")
            handle.flush()
            asset = jobs._create_output_asset(
                handle.name,
                "image/jpeg",
                ".jpg",
                "parent",
                60,
                {"width": 99_999, "height": 99_999},
            )
        self.assertEqual((asset["width"], asset["height"]), (640, 480))
        persist_asset.assert_called_once()
        self.assertTrue(persist_asset.call_args.kwargs["count_as_job_output"])

    def test_input_gate_rejects_downloaded_media_before_worker_use(self):
        configured = SimpleNamespace(
            storage_backend="s3",
            max_ingest_bytes=1_024,
            max_output_bytes=1_024,
        )
        with tempfile.NamedTemporaryFile(delete=False) as handle:
            path = handle.name
            handle.write(b"media")
        try:
            with (
                patch.object(jobs, "settings", configured),
                patch.object(jobs, "download_to_temp", return_value=path),
                patch.object(
                    jobs,
                    "validate_media_file",
                    side_effect=media_limits.MediaLimitError("hostile"),
                ),
                self.assertRaisesRegex(jobs.JobError, "configured limit"),
            ):
                jobs._resolve_input_path(
                    {"storage_key": "tenant/input.mp4", "mime_type": "video/mp4"}
                )
            self.assertFalse(os.path.exists(path))
        finally:
            if os.path.exists(path):
                os.remove(path)


class WorkerStreamSelectionTests(unittest.TestCase):
    def test_media_worker_entrypoints_publish_explicit_map_arguments(self):
        functions = (
            jobs.transcode_job,
            jobs.thumbnail_job,
            jobs.extract_audio_job,
            jobs.trim_job,
            jobs.video_add_text_job,
            jobs.captions_burn_in_job,
            jobs.video_add_logo_job,
            jobs.video_concat_job,
            jobs.image_to_video_job,
            jobs.images_to_slideshow_job,
            jobs.images_to_slideshow_ken_burns_job,
            jobs.audio_normalize_job,
            jobs.audio_mix_job,
            jobs.audio_duck_job,
            jobs.audio_fade_job,
            jobs.audio_trim_silence_job,
            jobs.video_replace_audio_job,
            jobs.video_analyze_job,
        )
        for function in functions:
            with self.subTest(function=function.__name__):
                self.assertIn('"-map"', inspect.getsource(function))

    def test_transcode_maps_exact_non_cover_video_and_audio_stream_indexes(self):
        probe = {
            "duration_sec": 1.0,
            "width": 640,
            "height": 360,
            "fps": 30.0,
            "streams": [
                _video_stream(index=0, disposition={"attached_pic": 1}),
                _video_stream(index=4, width=640, height=360, duration=1.0, nb_frames=30),
                {
                    "index": 7,
                    "codec_type": "audio",
                    "codec_name": "aac",
                    "duration": 1.0,
                    "sample_rate": 48_000,
                    "channels": 2,
                },
            ],
        }
        preset = {
            "output_ext": ".mp4",
            "mime_type": "video/mp4",
            "ffmpeg_args": ["-vf", "scale=640:360"],
        }
        configured = SimpleNamespace(storage_temp_dir="/tmp", max_duration_seconds=3_600)
        with (
            patch.object(jobs, "settings", configured),
            patch.object(jobs, "get_current_job", return_value=None),
            patch.object(jobs, "get_asset", return_value={"mime_type": "video/mp4"}),
            patch.object(jobs, "_ensure_temp_dir"),
            patch.object(jobs, "_resolve_input_path", return_value=("/tmp/input.mp4", False)),
            patch.object(jobs, "get_preset", return_value=preset),
            patch.object(jobs, "_probe_or_error", return_value=probe),
            patch.object(jobs, "_log_job_started"),
            patch.object(jobs, "_finish_job"),
            patch.object(jobs, "_record_job_metrics"),
            patch.object(jobs, "run_ffmpeg", side_effect=jobs.FfmpegError("stop")) as run,
            self.assertRaises(jobs.FfmpegError),
        ):
            jobs.transcode_job("asset", "preset")
        command = run.call_args.args[0]
        mapped = [command[index + 1] for index, value in enumerate(command) if value == "-map"]
        self.assertEqual(mapped, ["0:4", "0:7"])


if __name__ == "__main__":
    unittest.main()
