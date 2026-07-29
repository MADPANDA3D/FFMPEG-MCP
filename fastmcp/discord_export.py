import json
from typing import Any

import httpx

from config import settings


class DiscordExportError(RuntimeError):
    pass


def _build_headers(
    request_token: str | None = None,
    *,
    allow_environment_fallback: bool = True,
) -> dict[str, str]:
    token = str(request_token or "").strip()
    if not token and allow_environment_fallback:
        token = settings.discord_bot_token
    if not token:
        raise DiscordExportError(
            "A request-scoped Discord BYOK credential is required"
        )
    return {"Authorization": f"Bot {token}"}


async def send_file(
    *,
    channel_id: str,
    file_path: str,
    filename: str,
    message: str | None,
    mime_type: str | None,
    discord_bot_token: str | None = None,
) -> str:
    headers = _build_headers(
        discord_bot_token,
        allow_environment_fallback=settings.auth_mode not in {"portal_only", "grant_only"},
    )
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
