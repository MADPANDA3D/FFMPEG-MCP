import os
import selectors
import signal
import stat
import subprocess
import time
from contextlib import suppress
from typing import IO, cast


class CapturedOutputLimitError(RuntimeError):
    pass


class FileOutputLimitError(RuntimeError):
    pass


class ProcessSafetyError(RuntimeError):
    pass


_SAFE_MEDIA_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
_PRLIMIT_BIN = "/usr/bin/prlimit"


def sanitized_media_environment(temp_dir: str) -> dict[str, str]:
    """Build a minimal child environment without dynamic-loader or proxy injection."""

    resolved_temp_dir = os.path.abspath(temp_dir)
    if "\x00" in resolved_temp_dir:
        raise ProcessSafetyError("Media temporary directory is invalid")
    return {
        "AV_LOG_FORCE_NOCOLOR": "1",
        "HOME": "/tmp",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "OMP_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "PATH": _SAFE_MEDIA_PATH,
        "TMPDIR": resolved_temp_dir,
        "TZ": "UTC",
    }


def _append_tail(buffer: bytearray, chunk: bytes, limit: int) -> bool:
    if not chunk:
        return False
    if limit <= 0:
        return True
    truncated = len(buffer) + len(chunk) > limit
    buffer.extend(chunk)
    if len(buffer) > limit:
        del buffer[:-limit]
    return truncated


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    if process.returncode is not None:
        return
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    time.sleep(0.1)
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)
    try:
        process.wait(timeout=1.0)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=1.0)


def _output_size_exceeded(path: str, limit: int) -> bool:
    try:
        file_stat = os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise ProcessSafetyError("Unable to inspect media output safely") from exc
    if stat.S_ISLNK(file_stat.st_mode) or not stat.S_ISREG(file_stat.st_mode):
        raise ProcessSafetyError("Media output must be a regular non-symlink file")
    return file_stat.st_size > limit


def _resource_limited_command(
    command: list[str],
    *,
    output_limit_bytes: int,
    address_space_limit_bytes: int | None,
    cpu_limit_seconds: int | None,
    nofile_limit: int | None,
    disable_core_dumps: bool,
) -> list[str]:
    """Wrap an argv so limits are installed before the media binary is exec'd."""

    limits: list[str] = []
    if disable_core_dumps:
        limits.append("--core=0:0")
    if nofile_limit is not None:
        limits.append(f"--nofile={nofile_limit}:{nofile_limit}")
    if address_space_limit_bytes is not None:
        limits.append(f"--as={address_space_limit_bytes}:{address_space_limit_bytes}")
    if cpu_limit_seconds is not None:
        limits.append(f"--cpu={cpu_limit_seconds}:{cpu_limit_seconds}")
    if output_limit_bytes:
        limits.append(f"--fsize={output_limit_bytes}:{output_limit_bytes}")
    if not limits:
        return command
    if not os.path.isfile(_PRLIMIT_BIN) or not os.access(_PRLIMIT_BIN, os.X_OK):
        raise ProcessSafetyError("Linux media resource enforcement is unavailable")
    return [_PRLIMIT_BIN, *limits, "--", *command]


def run_bounded_process(
    command: list[str],
    *,
    timeout_seconds: float,
    capture_stdout: bool,
    stdout_limit_bytes: int,
    stderr_tail_bytes: int,
    output_path: str | None = None,
    output_limit_bytes: int | None = None,
    address_space_limit_bytes: int | None = None,
    cpu_limit_seconds: int | None = None,
    nofile_limit: int | None = None,
    disable_core_dumps: bool = False,
    environment: dict[str, str] | None = None,
) -> tuple[int, bytes, bytes, bool]:
    if os.name != "posix" or not hasattr(os, "killpg"):
        raise ProcessSafetyError("Process-group enforcement is unavailable")
    if not command or not all(isinstance(item, str) and "\x00" not in item for item in command):
        raise ProcessSafetyError("Media command is invalid")
    timeout_seconds = float(timeout_seconds)
    if timeout_seconds <= 0:
        raise ProcessSafetyError("Media timeout must be positive")
    if capture_stdout and stdout_limit_bytes <= 0:
        raise ProcessSafetyError("Captured stdout requires a positive byte limit")
    if stderr_tail_bytes < 0:
        raise ProcessSafetyError("stderr tail limit cannot be negative")
    for value, label in (
        (address_space_limit_bytes, "address-space"),
        (cpu_limit_seconds, "CPU"),
        (nofile_limit, "file-descriptor"),
    ):
        if value is not None and (isinstance(value, bool) or int(value) <= 0):
            raise ProcessSafetyError(f"Media {label} limit must be positive")
    if environment is not None and any(
        not isinstance(key, str) or not isinstance(value, str) or "\x00" in key or "\x00" in value
        for key, value in environment.items()
    ):
        raise ProcessSafetyError("Media subprocess environment is invalid")
    if output_path is not None:
        if output_limit_bytes is None or output_limit_bytes <= 0:
            raise ProcessSafetyError("Media output requires a positive byte limit")
        output_path = os.path.abspath(output_path)
        if _output_size_exceeded(output_path, output_limit_bytes):
            raise FileOutputLimitError("Media output exceeded its byte limit")
    output_limit = int(output_limit_bytes or 0)
    bounded_command = _resource_limited_command(
        command,
        output_limit_bytes=output_limit,
        address_space_limit_bytes=(
            int(address_space_limit_bytes) if address_space_limit_bytes is not None else None
        ),
        cpu_limit_seconds=int(cpu_limit_seconds) if cpu_limit_seconds is not None else None,
        nofile_limit=int(nofile_limit) if nofile_limit is not None else None,
        disable_core_dumps=disable_core_dumps,
    )

    selector = selectors.DefaultSelector()
    try:
        process = subprocess.Popen(
            bounded_command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE if capture_stdout else subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            shell=False,
            close_fds=True,
            start_new_session=True,
            env=environment,
        )
    except BaseException:
        if "process" in locals():
            _terminate_process_group(process)
            for stream in (process.stdout, process.stderr):
                if stream is not None and not stream.closed:
                    stream.close()
        selector.close()
        raise
    stdout = bytearray()
    stderr_tail = bytearray()
    stderr_truncated = False
    streams = [process.stderr]
    if process.stdout is not None:
        streams.append(process.stdout)
    try:
        for stream in streams:
            if stream is None:
                continue
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ)
    except BaseException:
        _terminate_process_group(process)
        selector.close()
        for stream in (process.stdout, process.stderr):
            if stream is not None and not stream.closed:
                stream.close()
        raise

    deadline = time.monotonic() + timeout_seconds
    try:
        while process.poll() is None or selector.get_map():
            if time.monotonic() >= deadline:
                raise TimeoutError("Media process timed out")
            if output_path is not None and _output_size_exceeded(
                output_path,
                output_limit,
            ):
                raise FileOutputLimitError("Media output exceeded its byte limit")

            events = selector.select(timeout=min(0.01, max(deadline - time.monotonic(), 0.0)))
            for key, _ in events:
                stream = cast(IO[bytes], key.fileobj)
                try:
                    chunk = os.read(stream.fileno(), 64 * 1024)
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(stream)
                    stream.close()
                    continue
                if stream is process.stdout:
                    if len(stdout) + len(chunk) > stdout_limit_bytes:
                        raise CapturedOutputLimitError(
                            "Media process stdout exceeded its byte limit"
                        )
                    stdout.extend(chunk)
                else:
                    stderr_truncated = (
                        _append_tail(stderr_tail, chunk, stderr_tail_bytes) or stderr_truncated
                    )

        returncode = process.wait(timeout=1.0)
        if output_path is not None and _output_size_exceeded(
            output_path,
            output_limit,
        ):
            raise FileOutputLimitError("Media output exceeded its byte limit")
        if output_path is not None and returncode != 0:
            try:
                output_size = os.lstat(output_path).st_size
            except FileNotFoundError:
                output_size = 0
            if output_size >= output_limit:
                raise FileOutputLimitError("Media output reached its byte limit")
        return returncode, bytes(stdout), bytes(stderr_tail), stderr_truncated
    except BaseException:
        _terminate_process_group(process)
        raise
    finally:
        selector.close()
        for stream in (process.stdout, process.stderr):
            if stream is not None and not stream.closed:
                stream.close()
