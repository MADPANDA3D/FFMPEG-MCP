import json
import os
import re
import stat
from typing import Any

import httpx

from .config import settings


class DiscordExportError(RuntimeError):
    pass


DISCORD_API_ORIGIN = "https://discord.com/api/v10"
_MAX_DISCORD_HTTP_TIMEOUT_SECONDS = 120
_MAX_DISCORD_RESPONSE_BYTES = 5_000_000


def _build_headers() -> dict[str, str]:
    if not settings.discord_bot_token:
        raise DiscordExportError("DISCORD_BOT_TOKEN is required")
    return {"Authorization": f"Bot {settings.discord_bot_token}"}


def _configured_channel_ids() -> set[str]:
    raw = settings.discord_allowed_channel_ids
    values = raw.split(",") if isinstance(raw, str) else raw or []
    return {str(value).strip() for value in values if str(value).strip()}


def _validate_destination(channel_id: str) -> str:
    channel_id = str(channel_id or "").strip()
    if not re.fullmatch(r"[0-9]{1,32}", channel_id):
        raise DiscordExportError("Discord channel id is invalid")
    allowed = _configured_channel_ids()
    if not allowed:
        raise DiscordExportError(
            "Discord exports are disabled until an allowed channel is configured"
        )
    if channel_id not in allowed:
        raise DiscordExportError("Discord channel is not allowlisted")
    return channel_id


def _validate_filename(filename: str) -> str:
    if not isinstance(filename, str):
        raise DiscordExportError("Discord filename is invalid")
    filename = filename.strip()
    if (
        not filename
        or os.path.basename(filename) != filename
        or any(ord(char) < 32 or ord(char) == 127 for char in filename)
        or len(filename) > 255
    ):
        raise DiscordExportError("Discord filename is invalid")
    return filename


def _open_regular_file(file_path: str):
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(file_path, flags)
    except OSError as exc:
        raise DiscordExportError("Discord upload source is not a readable regular file") from exc
    try:
        file_stat = os.fstat(descriptor)
        if not stat.S_ISREG(file_stat.st_mode):
            raise DiscordExportError("Discord upload source must be a regular file")
        if file_stat.st_size > settings.discord_max_upload_bytes:
            raise DiscordExportError("Discord upload source exceeds the configured size limit")
        return os.fdopen(descriptor, "rb")
    except Exception:
        os.close(descriptor)
        raise


def _build_http_client() -> httpx.AsyncClient:
    timeout_seconds = min(
        max(float(settings.discord_http_timeout_seconds), 1.0),
        _MAX_DISCORD_HTTP_TIMEOUT_SECONDS,
    )
    return httpx.AsyncClient(
        follow_redirects=False,
        timeout=httpx.Timeout(timeout_seconds, connect=min(timeout_seconds, 10.0)),
        transport=httpx.AsyncHTTPTransport(retries=0),
        trust_env=False,
    )


async def _read_bounded_json(response: httpx.Response) -> dict[str, Any]:
    max_bytes = min(
        max(int(settings.discord_max_response_bytes), 1),
        _MAX_DISCORD_RESPONSE_BYTES,
    )
    content_length = response.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > max_bytes:
                raise DiscordExportError("Discord response exceeded the configured size limit")
        except ValueError:
            pass
    body = bytearray()
    async for chunk in response.aiter_bytes():
        if len(body) + len(chunk) > max_bytes:
            raise DiscordExportError("Discord response exceeded the configured size limit")
        body.extend(chunk)
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DiscordExportError("Discord returned an invalid response") from exc
    if not isinstance(payload, dict):
        raise DiscordExportError("Discord returned an invalid response")
    return payload


async def send_file(
    *,
    channel_id: str,
    file_path: str,
    filename: str,
    message: str | None,
    mime_type: str | None,
) -> str:
    channel_id = _validate_destination(channel_id)
    filename = _validate_filename(filename)
    if message is not None and (not isinstance(message, str) or len(message) > 2000):
        raise DiscordExportError("Discord message is invalid")
    headers = _build_headers()
    url = f"{DISCORD_API_ORIGIN}/channels/{channel_id}/messages"
    payload: dict[str, Any] = {}
    if message:
        payload["content"] = message

    with _open_regular_file(file_path) as handle:
        files = {"files[0]": (filename, handle, mime_type or "application/octet-stream")}
        data = {"payload_json": json.dumps(payload)} if payload else {}
        async with _build_http_client() as client:
            async with client.stream(
                "POST",
                url,
                headers=headers,
                data=data,
                files=files,
            ) as resp:
                if resp.status_code < 200 or resp.status_code >= 300:
                    raise DiscordExportError(f"Discord upload failed ({resp.status_code})")
                response = await _read_bounded_json(resp)
            message_id = response.get("id")
            if not isinstance(message_id, str) or not message_id:
                raise DiscordExportError("Discord upload failed to return message id")
            return message_id
