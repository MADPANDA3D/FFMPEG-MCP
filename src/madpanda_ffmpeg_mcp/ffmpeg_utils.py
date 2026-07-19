import os
import re

from .config import settings
from .media_process import (
    FileOutputLimitError,
    ProcessSafetyError,
    run_bounded_process,
    sanitized_media_environment,
)


class FfmpegError(RuntimeError):
    pass


_LOCAL_PROTOCOLS = "file"
_MAX_MEDIA_TIMEOUT_SECONDS = 3600
_STDERR_TAIL_BYTES = 4000
_REMOTE_PROTOCOL_RE = re.compile(
    r"^(?:https?|ftp|sftp|rtmp|rtmps|rtsp|tcp|udp|smb|concat|crypto|data|subfile|cache|async):",
    flags=re.IGNORECASE,
)
_URI_SCHEME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


def _trim_logs(text: str, limit: int = 4000) -> str:
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[-limit:]


def _validate_args(args: list[str]) -> list[str]:
    if not isinstance(args, list) or not args or not all(isinstance(item, str) for item in args):
        raise FfmpegError("FFmpeg arguments must be a list of strings")
    for item in args:
        if "\x00" in item or _REMOTE_PROTOCOL_RE.match(item.strip()):
            raise FfmpegError("Remote and compound FFmpeg protocols are not allowed")
        if (
            item == "-threads"
            or item.startswith("-threads:")
            or item.startswith("-threads=")
            or item == "-filter_threads"
            or item.startswith("-filter_threads:")
            or item.startswith("-filter_threads=")
            or item == "-filter_complex_threads"
            or item.startswith("-filter_complex_threads:")
            or item.startswith("-filter_complex_threads=")
        ):
            raise FfmpegError("FFmpeg thread controls are managed by server policy")
    return list(args)


def _bounded_thread_args(args: list[str], thread_count: int) -> list[str]:
    bounded: list[str] = []
    for item in args:
        if item == "-i":
            bounded.extend(["-threads", str(thread_count)])
        bounded.append(item)
    bounded[-1:-1] = ["-threads", str(thread_count)]
    return bounded


def _output_path(args: list[str]) -> str | None:
    target = args[-1].strip()
    if target == "-":
        return None
    if not target or target.startswith("-") or _URI_SCHEME_RE.match(target):
        raise FfmpegError("FFmpeg output must be a local file path or the null sink")
    return os.path.abspath(target)


def run_ffmpeg(args: list[str], timeout: int | None = None) -> str:
    safe_args = _validate_args(args)
    output_path = _output_path(safe_args)
    resolved_timeout = timeout if timeout is not None else settings.ffmpeg_timeout_seconds
    resolved_timeout = min(max(int(resolved_timeout), 1), _MAX_MEDIA_TIMEOUT_SECONDS)
    thread_count = min(max(int(getattr(settings, "ffmpeg_threads", 2)), 1), 16)
    cmd = [
        settings.ffmpeg_bin,
        "-nostdin",
        "-hide_banner",
        "-protocol_whitelist",
        _LOCAL_PROTOCOLS,
        "-filter_threads",
        str(thread_count),
        "-filter_complex_threads",
        str(thread_count),
        "-y",
    ] + _bounded_thread_args(safe_args, thread_count)
    try:
        returncode, _, stderr, _ = run_bounded_process(
            cmd,
            timeout_seconds=resolved_timeout,
            capture_stdout=False,
            stdout_limit_bytes=0,
            stderr_tail_bytes=_STDERR_TAIL_BYTES,
            output_path=output_path,
            output_limit_bytes=int(settings.max_output_bytes) if output_path else None,
            address_space_limit_bytes=int(
                getattr(settings, "ffmpeg_rlimit_as_bytes", 3_221_225_472)
            ),
            cpu_limit_seconds=int(getattr(settings, "ffmpeg_rlimit_cpu_seconds", 1_800)),
            nofile_limit=int(getattr(settings, "media_rlimit_nofile", 256)),
            disable_core_dumps=True,
            environment=sanitized_media_environment(
                getattr(settings, "storage_temp_dir", os.path.dirname(output_path or "") or "/tmp")
            ),
        )
    except TimeoutError as exc:
        raise FfmpegError("FFmpeg processing timed out") from exc
    except FileOutputLimitError as exc:
        raise FfmpegError("FFmpeg output exceeded the configured size limit") from exc
    except (OSError, ProcessSafetyError) as exc:
        raise FfmpegError("FFmpeg could not start safely") from exc
    logs = stderr.decode("utf-8", errors="replace")
    if returncode != 0:
        raise FfmpegError("FFmpeg processing failed")
    return _trim_logs(logs)
