import json
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx

from madpanda_ffmpeg_mcp import (
    discord_export,
    drive_utils,
    ffmpeg_utils,
    ffprobe_utils,
    media_process,
)
from madpanda_ffmpeg_mcp.overlay_utils import escape_drawtext_value


class MediaCommandSecurityTests(unittest.TestCase):
    def test_ffmpeg_uses_local_protocols_no_stdin_and_no_shell(self):
        settings = SimpleNamespace(
            ffmpeg_bin="ffmpeg",
            ffmpeg_timeout_seconds=30,
            max_output_bytes=1234,
        )
        with (
            patch.object(ffmpeg_utils, "settings", settings),
            patch.object(
                ffmpeg_utils,
                "run_bounded_process",
                return_value=(0, b"", b"ok", False),
            ) as run,
        ):
            ffmpeg_utils.run_ffmpeg(["-i", "/tmp/input.mp4", "/tmp/output.mp4"])

        command = run.call_args.args[0]
        self.assertIn("-nostdin", command)
        self.assertEqual(command[command.index("-protocol_whitelist") + 1], "file")
        self.assertFalse(run.call_args.kwargs["capture_stdout"])
        self.assertEqual(run.call_args.kwargs["output_path"], "/tmp/output.mp4")
        self.assertEqual(run.call_args.kwargs["output_limit_bytes"], 1234)

    def test_ffmpeg_null_sink_needs_no_output_file_quota(self):
        settings = SimpleNamespace(
            ffmpeg_bin="ffmpeg",
            ffmpeg_timeout_seconds=30,
            max_output_bytes=1234,
        )
        with (
            patch.object(ffmpeg_utils, "settings", settings),
            patch.object(
                ffmpeg_utils,
                "run_bounded_process",
                return_value=(0, b"", b"analysis", False),
            ) as run,
        ):
            logs = ffmpeg_utils.run_ffmpeg(["-i", "/tmp/input.mp4", "-f", "null", "-"])

        self.assertEqual(logs, "analysis")
        self.assertIsNone(run.call_args.kwargs["output_path"])
        self.assertIsNone(run.call_args.kwargs["output_limit_bytes"])

    def test_ffmpeg_failure_is_stable_and_does_not_return_diagnostics(self):
        settings = SimpleNamespace(
            ffmpeg_bin="ffmpeg",
            ffmpeg_timeout_seconds=30,
            max_output_bytes=1234,
        )
        with (
            patch.object(ffmpeg_utils, "settings", settings),
            patch.object(
                ffmpeg_utils,
                "run_bounded_process",
                return_value=(1, b"", b"sensitive local path", False),
            ),
            self.assertRaisesRegex(
                ffmpeg_utils.FfmpegError,
                "^FFmpeg processing failed$",
            ),
        ):
            ffmpeg_utils.run_ffmpeg(["-i", "/tmp/input.mp4", "/tmp/output.mp4"])

    def test_ffmpeg_rejects_remote_or_compound_protocols_before_execution(self):
        settings = SimpleNamespace(
            ffmpeg_bin="ffmpeg",
            ffmpeg_timeout_seconds=30,
            max_output_bytes=1234,
        )
        with (
            patch.object(ffmpeg_utils, "settings", settings),
            patch.object(ffmpeg_utils, "run_bounded_process") as run,
        ):
            for source in ["https://example.com/video.mp4", "concat:/tmp/a|/tmp/b"]:
                with (
                    self.subTest(source=source),
                    self.assertRaises(ffmpeg_utils.FfmpegError),
                ):
                    ffmpeg_utils.run_ffmpeg(["-i", source, "/tmp/output.mp4"])
        run.assert_not_called()

    def test_ffprobe_uses_bounded_local_only_execution(self):
        settings = SimpleNamespace(ffprobe_bin="ffprobe", ffprobe_timeout_seconds=20)
        payload = json.dumps({"format": {"duration": "1.5"}, "streams": []}).encode()
        with (
            tempfile.NamedTemporaryFile() as handle,
            patch.object(ffprobe_utils, "settings", settings),
            patch.object(
                ffprobe_utils,
                "run_bounded_process",
                return_value=(0, payload, b"", False),
            ) as run,
        ):
            result = ffprobe_utils.run_ffprobe(handle.name)

        command = run.call_args.args[0]
        self.assertEqual(command[command.index("-protocol_whitelist") + 1], "file")
        self.assertIn("-show_entries", command)
        self.assertTrue(run.call_args.kwargs["capture_stdout"])
        self.assertEqual(run.call_args.kwargs["timeout_seconds"], 20)
        self.assertEqual(
            run.call_args.kwargs["stdout_limit_bytes"],
            ffprobe_utils._MAX_FFPROBE_JSON_BYTES,
        )
        self.assertEqual(result["duration_sec"], 1.5)

    def test_ffprobe_rejects_oversized_invalid_and_nonfinite_output(self):
        settings = SimpleNamespace(ffprobe_bin="ffprobe", ffprobe_timeout_seconds=20)
        invalid_payloads = [
            b"not-json",
            b'{"format":{"duration":NaN},"streams":[]}',
            b'{"format":{"duration":"NaN"},"streams":[]}',
        ]
        with (
            tempfile.NamedTemporaryFile() as handle,
            patch.object(
                ffprobe_utils,
                "settings",
                settings,
            ),
        ):
            for payload in invalid_payloads:
                with (
                    self.subTest(payload=payload),
                    patch.object(
                        ffprobe_utils,
                        "run_bounded_process",
                        return_value=(0, payload, b"", False),
                    ),
                    self.assertRaisesRegex(RuntimeError, "invalid"),
                ):
                    ffprobe_utils.run_ffprobe(handle.name)
            with (
                patch.object(
                    ffprobe_utils,
                    "run_bounded_process",
                    side_effect=media_process.CapturedOutputLimitError,
                ),
                self.assertRaisesRegex(RuntimeError, "safety limit"),
            ):
                ffprobe_utils.run_ffprobe(handle.name)

    def test_ffprobe_rejects_symlinks_and_urls(self):
        settings = SimpleNamespace(ffprobe_bin="ffprobe", ffprobe_timeout_seconds=20)
        with tempfile.TemporaryDirectory() as temp_dir:
            source = os.path.join(temp_dir, "source.mp4")
            linked = os.path.join(temp_dir, "linked.mp4")
            with open(source, "wb") as handle:
                handle.write(b"media")
            os.symlink(source, linked)
            with patch.object(ffprobe_utils, "settings", settings):
                with self.assertRaisesRegex(RuntimeError, "local regular"):
                    ffprobe_utils.run_ffprobe(linked)
                with self.assertRaisesRegex(RuntimeError, "local regular"):
                    ffprobe_utils.run_ffprobe("https://example.com/video.mp4")

    def test_drawtext_escaping_covers_filtergraph_delimiters_and_controls(self):
        value = "a:b,c;d[e]'f\"g%h\\i\n"
        escaped = escape_drawtext_value(value)
        for token in ["\\:", "\\,", "\\;", "\\[", "\\]", "\\'", '\\"', "\\%", "\\\\", "\\n"]:
            self.assertIn(token, escaped)
        with self.assertRaises(ValueError):
            escape_drawtext_value("bad\x00value")


class BoundedProcessTests(unittest.TestCase):
    def _assert_process_gone(self, pid: int) -> None:
        for _ in range(50):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
            time.sleep(0.01)
        self.fail(f"process {pid} survived cleanup")

    def test_stderr_is_drained_but_only_the_bounded_tail_is_retained(self):
        returncode, stdout, stderr, truncated = media_process.run_bounded_process(
            [sys.executable, "-c", "import sys; sys.stderr.buffer.write(b'x' * 10000)"],
            timeout_seconds=5,
            capture_stdout=False,
            stdout_limit_bytes=0,
            stderr_tail_bytes=128,
        )
        self.assertEqual(returncode, 0)
        self.assertEqual(stdout, b"")
        self.assertEqual(stderr, b"x" * 128)
        self.assertTrue(truncated)

    def test_stdout_limit_terminates_the_process(self):
        with self.assertRaises(media_process.CapturedOutputLimitError):
            media_process.run_bounded_process(
                [sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'x' * 4096)"],
                timeout_seconds=5,
                capture_stdout=True,
                stdout_limit_bytes=64,
                stderr_tail_bytes=64,
            )

    def test_timeout_terminates_and_reaps_the_process_group_leader(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pid_path = os.path.join(temp_dir, "pid")
            code = (
                "import os, pathlib, sys, time; "
                "pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); "
                "time.sleep(30)"
            )
            with self.assertRaises(TimeoutError):
                media_process.run_bounded_process(
                    [sys.executable, "-c", code, pid_path],
                    timeout_seconds=0.2,
                    capture_stdout=False,
                    stdout_limit_bytes=0,
                    stderr_tail_bytes=64,
                )
            pid = int(Path(pid_path).read_text(encoding="utf-8"))
            self._assert_process_gone(pid)

    def test_output_file_quota_terminates_and_reaps_the_process(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = os.path.join(temp_dir, "output.bin")
            pid_path = os.path.join(temp_dir, "pid")
            code = "\n".join(
                [
                    "import os, pathlib, sys, time",
                    "pathlib.Path(sys.argv[2]).write_text(str(os.getpid()))",
                    "with open(sys.argv[1], 'wb', buffering=0) as handle:",
                    "    while True:",
                    "        handle.write(b'x' * 4096)",
                    "        time.sleep(0.001)",
                ]
            )
            with self.assertRaises(media_process.FileOutputLimitError):
                media_process.run_bounded_process(
                    [sys.executable, "-c", code, output_path, pid_path],
                    timeout_seconds=5,
                    capture_stdout=False,
                    stdout_limit_bytes=0,
                    stderr_tail_bytes=64,
                    output_path=output_path,
                    output_limit_bytes=1024,
                )
            pid = int(Path(pid_path).read_text(encoding="utf-8"))
            self._assert_process_gone(pid)

    def test_hard_limits_are_visible_at_target_process_start(self):
        code = (
            "import json,resource; "
            "names=['RLIMIT_AS','RLIMIT_CPU','RLIMIT_CORE','RLIMIT_NOFILE','RLIMIT_FSIZE']; "
            "print(json.dumps({name:resource.getrlimit(getattr(resource,name)) "
            "for name in names}))"
        )
        returncode, stdout, _, _ = media_process.run_bounded_process(
            [sys.executable, "-c", code],
            timeout_seconds=5,
            capture_stdout=True,
            stdout_limit_bytes=4096,
            stderr_tail_bytes=64,
            output_limit_bytes=1_048_576,
            address_space_limit_bytes=536_870_912,
            cpu_limit_seconds=5,
            nofile_limit=64,
            disable_core_dumps=True,
        )
        self.assertEqual(returncode, 0)
        limits = json.loads(stdout)
        self.assertEqual(limits["RLIMIT_AS"], [536_870_912, 536_870_912])
        self.assertEqual(limits["RLIMIT_CPU"], [5, 5])
        self.assertEqual(limits["RLIMIT_CORE"], [0, 0])
        self.assertEqual(limits["RLIMIT_NOFILE"], [64, 64])
        self.assertEqual(limits["RLIMIT_FSIZE"], [1_048_576, 1_048_576])

    def test_missing_prlimit_wrapper_fails_closed_before_spawn(self):
        with (
            patch.object(media_process, "_PRLIMIT_BIN", "/missing/prlimit"),
            patch.object(media_process.subprocess, "Popen") as popen,
            self.assertRaisesRegex(
                media_process.ProcessSafetyError,
                "resource enforcement is unavailable",
            ),
        ):
            media_process.run_bounded_process(
                [sys.executable, "-c", "pass"],
                timeout_seconds=5,
                capture_stdout=False,
                stdout_limit_bytes=0,
                stderr_tail_bytes=64,
                address_space_limit_bytes=536_870_912,
            )
        popen.assert_not_called()


def _discord_settings(**overrides):
    values = {
        "mcp_mode": "standalone",
        "discord_bot_token": "test-token",
        "discord_allowed_channel_ids": ["123"],
        "discord_max_upload_bytes": 1024,
        "discord_max_response_bytes": 1024,
        "discord_http_timeout_seconds": 30,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class DiscordBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_export_uses_fixed_origin_and_allowlisted_destination(self):
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"id": "message-1"})

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            follow_redirects=False,
            trust_env=False,
        )
        with tempfile.NamedTemporaryFile() as handle:
            handle.write(b"media")
            handle.flush()
            with (
                patch.object(discord_export, "settings", _discord_settings()),
                patch.object(discord_export, "_build_http_client", return_value=client),
            ):
                message_id = await discord_export.send_file(
                    channel_id="123",
                    file_path=handle.name,
                    filename="media.mp4",
                    message="ready",
                    mime_type="video/mp4",
                )

        self.assertEqual(message_id, "message-1")
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].url.scheme, "https")
        self.assertEqual(requests[0].url.host, "discord.com")
        self.assertEqual(requests[0].url.path, "/api/v10/channels/123/messages")
        self.assertEqual(requests[0].headers["Authorization"], "Bot test-token")

    async def test_export_fails_closed_without_destination_allowlist(self):
        with (
            tempfile.NamedTemporaryFile() as handle,
            patch.object(
                discord_export,
                "settings",
                _discord_settings(discord_allowed_channel_ids=[]),
            ),
            self.assertRaisesRegex(discord_export.DiscordExportError, "disabled"),
        ):
            await discord_export.send_file(
                channel_id="123",
                file_path=handle.name,
                filename="media.mp4",
                message=None,
                mime_type="video/mp4",
            )

    async def test_portal_export_uses_only_request_scoped_byok(self):
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={"id": "message-2"})

        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
            follow_redirects=False,
            trust_env=False,
        )
        with tempfile.NamedTemporaryFile() as handle:
            handle.write(b"media")
            handle.flush()
            with (
                patch.object(
                    discord_export,
                    "settings",
                    _discord_settings(
                        mcp_mode="portal",
                        discord_bot_token="must-not-be-used",
                        discord_allowed_channel_ids=[],
                    ),
                ),
                patch.object(discord_export, "_build_http_client", return_value=client),
            ):
                message_id = await discord_export.send_file(
                    channel_id="456",
                    file_path=handle.name,
                    filename="media.mp4",
                    message=None,
                    mime_type="video/mp4",
                    discord_bot_token="calling-user-token",
                    allow_environment_fallback=False,
                    require_allowlisted_channel=False,
                )

        self.assertEqual(message_id, "message-2")
        self.assertEqual(requests[0].headers["Authorization"], "Bot calling-user-token")

    async def test_portal_export_fails_closed_without_request_scoped_byok(self):
        with (
            tempfile.NamedTemporaryFile() as handle,
            patch.object(
                discord_export,
                "settings",
                _discord_settings(
                    mcp_mode="portal",
                    discord_bot_token="must-not-be-used",
                    discord_allowed_channel_ids=[],
                ),
            ),
            self.assertRaisesRegex(
                discord_export.DiscordExportError, "request-scoped Discord BYOK"
            ),
        ):
            await discord_export.send_file(
                channel_id="456",
                file_path=handle.name,
                filename="media.mp4",
                message=None,
                mime_type="video/mp4",
                allow_environment_fallback=False,
                require_allowlisted_channel=False,
            )

    async def test_bounded_discord_response_rejects_oversize_body(self):
        response = httpx.Response(200, content=b'{"id":"message-1"}')
        with (
            patch.object(
                discord_export,
                "settings",
                _discord_settings(discord_max_response_bytes=4),
            ),
            self.assertRaisesRegex(discord_export.DiscordExportError, "size limit"),
        ):
            await discord_export._read_bounded_json(response)


def _drive_settings(**overrides):
    values = {"google_drive_allowed_folder_ids": ["folder-1"]}
    values.update(overrides)
    return SimpleNamespace(**values)


class DriveBoundaryTests(unittest.TestCase):
    def test_drive_export_requires_allowlisted_folder_and_disables_retries(self):
        execute = MagicMock(return_value={"id": "file-1"})
        create = MagicMock(return_value=SimpleNamespace(execute=execute))
        service = SimpleNamespace(files=lambda: SimpleNamespace(create=create))

        with tempfile.NamedTemporaryFile() as handle:
            handle.write(b"media")
            handle.flush()
            with (
                patch.object(drive_utils, "settings", _drive_settings()),
                patch.object(drive_utils, "get_drive_service", return_value=service),
                patch.object(drive_utils, "MediaFileUpload", return_value=object()),
            ):
                file_id = drive_utils.upload_file(
                    handle.name,
                    "media.mp4",
                    "video/mp4",
                    "folder-1",
                )

        self.assertEqual(file_id, "file-1")
        self.assertEqual(execute.call_args.kwargs, {"num_retries": 0})
        self.assertEqual(create.call_args.kwargs["body"]["parents"], ["folder-1"])

    def test_drive_export_fails_closed_for_unconfigured_or_unknown_folder(self):
        with tempfile.NamedTemporaryFile() as handle:
            for allowed, folder in [([], "folder-1"), (["folder-1"], "folder-2")]:
                with (
                    self.subTest(allowed=allowed, folder=folder),
                    patch.object(
                        drive_utils,
                        "settings",
                        _drive_settings(google_drive_allowed_folder_ids=allowed),
                    ),
                    self.assertRaises(drive_utils.DriveError),
                ):
                    drive_utils.upload_file(
                        handle.name,
                        "media.mp4",
                        "video/mp4",
                        folder,
                    )


if __name__ == "__main__":
    unittest.main()
