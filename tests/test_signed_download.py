import json
import os
import tempfile
import time
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from madpanda_ffmpeg_mcp import redis_store, server, storage


async def _invoke_download(path: str, query: str) -> tuple[int, bytes]:
    messages: list[dict] = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    await server._download_handler(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "query_string": query.encode("ascii"),
            "headers": [],
        },
        receive,
        send,
    )
    status = next(
        message["status"] for message in messages if message["type"] == "http.response.start"
    )
    body = b"".join(
        message.get("body", b"") for message in messages if message["type"] == "http.response.body"
    )
    return status, body


class SignedDownloadTests(unittest.IsolatedAsyncioTestCase):
    async def test_signed_url_is_retention_bounded_and_uses_internal_lookup(self):
        asset_id = "a" * 32
        storage_key = f"aa/aa/{asset_id}.mp4"
        payload = b"synthetic-media"
        now = 1_700_000_000
        asset_expires_at = now + 15
        with tempfile.TemporaryDirectory() as temp_dir:
            configured = replace(
                server.settings,
                storage_backend="local",
                storage_local_dir=temp_dir,
                storage_temp_dir=temp_dir,
                public_base_url="https://downloads.example.invalid",
                download_signing_secret="s" * 40,
                download_url_ttl_seconds=60,
            )
            with (
                patch.object(server, "settings", configured),
                patch.object(storage, "settings", configured),
                patch.object(storage.time, "time", return_value=now),
            ):
                file_path = storage.local_path_from_key(storage_key)
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                with open(file_path, "wb") as handle:
                    handle.write(payload)
                url, expires_at = storage.generate_download_url(
                    asset_id,
                    storage_key,
                    asset_expires_at,
                )
                parsed = urlsplit(url)
                signature = parse_qs(parsed.query)["sig"][0]
                self.assertEqual(expires_at, asset_expires_at)
                self.assertRegex(signature, r"^[A-Za-z0-9_-]{43}$")
                with patch.object(
                    server,
                    "get_signed_download_asset",
                    return_value={
                        "storage_key": storage_key,
                        "mime_type": "video/mp4",
                        "original_filename": "sample.mp4",
                        "expires_at": asset_expires_at,
                    },
                ) as signed_lookup:
                    status, body = await _invoke_download(parsed.path, parsed.query)
                signed_lookup.assert_called_once_with(asset_id)

        self.assertEqual(status, 200)
        self.assertEqual(body, payload)

    async def test_invalid_and_duplicate_queries_fail_closed(self):
        asset_id = "b" * 32
        configured = replace(
            server.settings,
            storage_backend="local",
            public_base_url="https://downloads.example.invalid",
            download_signing_secret="s" * 40,
            download_url_ttl_seconds=60,
        )
        with (
            patch.object(server, "settings", configured),
            patch.object(storage, "settings", configured),
        ):
            url, _ = storage.generate_download_url(
                asset_id,
                "bb/bb/output.mp4",
                int(time.time()) + 60,
            )
            parsed = urlsplit(url)
            values = parse_qs(parsed.query)
            exp = values["exp"][0]
            sig = values["sig"][0]
            invalid_queries = (
                f"exp={exp}&sig={'A' * 43}",
                f"exp={exp}&sig={sig}=",
                f"exp={exp}&sig={sig}&sig={sig}",
                f"exp={exp}&exp={exp}&sig={sig}",
                f"exp={exp}&sig={sig}&extra=1",
            )
            for query in invalid_queries:
                with self.subTest(query=query):
                    status, body = await _invoke_download(parsed.path, query)
                    self.assertEqual(status, 403)
                    self.assertEqual(body, b"Forbidden")

    async def test_internal_signed_lookup_exposes_only_active_unexpired_assets(self):
        now = 1_700_000_000
        asset_id = "c" * 32
        record = {
            "asset_id": asset_id,
            "owner_hash": "d" * 64,
            "state": "active",
            "reservation_token": "e" * 32,
            "storage_key": f"cc/cc/{asset_id}.mp4",
            "size_bytes": 10,
            "expires_at": now + 30,
        }
        client = SimpleNamespace(get=lambda _key: json.dumps(record))
        with (
            patch.object(redis_store, "get_redis", return_value=client),
            patch.object(redis_store, "_now_ts", return_value=now),
        ):
            self.assertIsNone(redis_store.get_asset(asset_id))
            visible = redis_store.get_signed_download_asset(asset_id)
            self.assertEqual(visible["asset_id"], asset_id)
            self.assertNotIn("owner_hash", visible)

            record["state"] = "reserved"
            self.assertIsNone(redis_store.get_signed_download_asset(asset_id))
            record["state"] = "active"
            record["expires_at"] = now
            self.assertIsNone(redis_store.get_signed_download_asset(asset_id))


if __name__ == "__main__":
    unittest.main()
