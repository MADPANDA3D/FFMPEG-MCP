import json
from typing import Any

import httpx

from config import settings


class DiscordExportError(RuntimeError):
    pass


def _build_headers() -> dict[str, str]:
    if not settings.discord_bot_token:
        raise DiscordExportError("DISCORD_BOT_TOKEN is required")
    return {"Authorization": f"Bot {settings.discord_bot_token}"}


async def send_file(
    *,
    channel_id: str,
    file_path: str,
    filename: str,
    message: str | None,
    mime_type: str | None,
) -> str:
    headers = _build_headers()
    url = f"{settings.discord_api_base.rstrip('/')}/channels/{channel_id}/messages"
    payload: dict[str, Any] = {}
    if message:
        payload["content"] = message

    with open(file_path, "rb") as handle:
        files = {"files[0]": (filename, handle, mime_type or "application/octet-stream")}
        data = {"payload_json": json.dumps(payload)} if payload else {}
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.post(url, headers=headers, data=data, files=files)
            if resp.status_code >= 400:
                raise DiscordExportError(f"Discord upload failed ({resp.status_code})")
            response = resp.json()
            message_id = response.get("id")
            if not message_id:
                raise DiscordExportError("Discord upload failed to return message id")
            return message_id
