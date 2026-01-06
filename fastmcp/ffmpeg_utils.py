import subprocess

from config import settings


class FfmpegError(RuntimeError):
    pass


def _trim_logs(text: str, limit: int = 4000) -> str:
    if not text:
        return ""
    if len(text) <= limit:
        return text
    return text[-limit:]


def run_ffmpeg(args: list[str], timeout: int | None = None) -> str:
    cmd = [settings.ffmpeg_bin, "-y"] + args
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout or settings.ffmpeg_timeout_seconds,
        check=False,
    )
    logs = (result.stderr or "") + ("\n" + result.stdout if result.stdout else "")
    if result.returncode != 0:
        raise FfmpegError(_trim_logs(logs))
    return _trim_logs(logs)
