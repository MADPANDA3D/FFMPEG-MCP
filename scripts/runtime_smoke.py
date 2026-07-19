#!/usr/bin/env python3
"""Provider-free container smoke for both authenticated access modes."""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import tempfile
import time
import uuid
from importlib.metadata import version
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

HOST = "127.0.0.1"
PORT = int(os.getenv("MCP_HTTP_PORT", "8087"))
MODE = os.environ["MCP_MODE"]
EXPECTED_TOOL_COUNT = int(os.getenv("MCP_EXPECTED_TOOL_COUNT", "55"))
EXPECTED_BUILD_SHA = os.environ["EXPECTED_BUILD_SHA"]
EXPECTED_SOURCE_FINGERPRINT = os.environ["EXPECTED_SOURCE_FINGERPRINT"]
EXPECTED_IMAGE_REFERENCE = os.environ["EXPECTED_IMAGE_REFERENCE"]
EXPECTED_CATALOG_VERSION = "2026-07-18.4"
ACCESS_TOKEN = os.getenv("MCP_ACCESS_TOKEN", "")
PORTAL_GRANT = os.getenv("MCP_PORTAL_GRANT_TOKEN", "")
PORTAL_HEADER = os.getenv("MCP_PORTAL_GRANT_HEADER", "X-MADPANDA-PORTAL-GRANT")
PORTAL_SUBJECT = "smoke:synthetic-tenant"
PACKAGE_VERSION = version("mad-mcp-ffmpeg")


def auth_headers(*, valid: bool = True) -> dict[str, str]:
    if MODE == "standalone":
        token = ACCESS_TOKEN if valid else "wrong-standalone-token-000000000000"
        return {"Authorization": f"Bearer {token}"}
    if MODE == "portal":
        token = PORTAL_GRANT if valid else "wrong-portal-grant-0000000000000000"
        return {
            PORTAL_HEADER: token,
            "X-MADPANDA-PORTAL-SUBJECT": PORTAL_SUBJECT,
        }
    raise AssertionError(f"unexpected MCP_MODE={MODE!r}")


def request(
    method: str,
    path: str,
    *,
    payload: dict[str, Any] | bytes | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, str], Any]:
    body = (
        json.dumps(payload, separators=(",", ":")).encode()
        if isinstance(payload, dict)
        else payload
    )
    merged = {
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
    }
    if headers:
        merged.update(headers)
    connection = http.client.HTTPConnection(HOST, PORT, timeout=8)
    try:
        connection.request(method, path, body=body, headers=merged)
        response = connection.getresponse()
        raw = response.read()
        response_headers = {key.lower(): value for key, value in response.getheaders()}
    finally:
        connection.close()
    try:
        decoded: Any = json.loads(raw) if raw else None
    except json.JSONDecodeError:
        decoded = raw.decode("utf-8", errors="replace")
    return response.status, response_headers, decoded


def rpc(method: str, request_id: int, params: dict[str, Any]) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def media_roundtrip_enabled() -> bool:
    return os.getenv("MCP_SMOKE_MEDIA_ROUNDTRIP", "").strip().lower() == "true"


def authenticated_owner() -> str:
    from madpanda_ffmpeg_mcp.config import settings
    from madpanda_ffmpeg_mcp.tenant import hash_principal

    require(settings.mcp_mode == MODE, "smoke mode differs from runtime settings")
    if MODE == "standalone":
        return hash_principal(
            "standalone-owner",
            settings.principal_hash_secret,
            namespace="standalone",
        )
    if MODE == "portal":
        return hash_principal(
            PORTAL_SUBJECT,
            settings.principal_hash_secret,
            namespace="portal-subject",
        )
    raise AssertionError(f"unexpected MCP_MODE={MODE!r}")


def tool_payload(response: Any, tool_name: str) -> dict[str, Any]:
    require(isinstance(response, dict), f"{tool_name} response is not JSON")
    result = response.get("result")
    require(isinstance(result, dict), f"{tool_name} response has no result")
    if result.get("isError", False):
        error_summary = json.dumps(result.get("content"), default=str, sort_keys=True)[:1_000]
        raise AssertionError(f"{tool_name} returned an MCP error: {error_summary}")
    structured = result.get("structuredContent")
    if isinstance(structured, dict):
        return structured
    for item in result.get("content", []):
        if not isinstance(item, dict) or item.get("type") != "text":
            continue
        try:
            decoded = json.loads(item.get("text", ""))
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(decoded, dict):
            return decoded
    raise AssertionError(f"{tool_name} returned no structured object")


def call_tool(
    headers: dict[str, str],
    request_id: int,
    name: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    status, _, response = request(
        "POST",
        "/mcp",
        payload=rpc(
            "tools/call",
            request_id,
            {"name": name, "arguments": arguments},
        ),
        headers=headers,
    )
    require(status == 200, f"{name} failed with HTTP {status}")
    return tool_payload(response, name)


def create_synthetic_input(owner_hash: str) -> dict[str, Any]:
    from madpanda_ffmpeg_mcp.config import settings
    from madpanda_ffmpeg_mcp.ffmpeg_utils import run_ffmpeg
    from madpanda_ffmpeg_mcp.ffprobe_utils import run_ffprobe
    from madpanda_ffmpeg_mcp.media_limits import validate_media_probe
    from madpanda_ffmpeg_mcp.storage import persist_asset
    from madpanda_ffmpeg_mcp.tenant import tenant_context
    from madpanda_ffmpeg_mcp.utils import utc_now_iso, utc_now_ts

    os.makedirs(settings.storage_temp_dir, exist_ok=True)
    descriptor, temp_path = tempfile.mkstemp(
        dir=settings.storage_temp_dir,
        prefix="runtime-smoke-",
        suffix=".mp4",
    )
    os.close(descriptor)
    os.unlink(temp_path)
    try:
        run_ffmpeg(
            [
                "-f",
                "lavfi",
                "-i",
                "color=c=blue:s=64x64:r=10:d=1",
                "-c:v",
                "libx264",
                "-pix_fmt",
                "yuv420p",
                "-movflags",
                "+faststart",
                temp_path,
            ],
            timeout=30,
        )
        probe = validate_media_probe(run_ffprobe(temp_path), expected_kind="video")
        asset_id = uuid.uuid4().hex
        now = utc_now_ts()
        asset = {
            "asset_id": asset_id,
            "source": "runtime_smoke",
            "original_filename": "runtime-smoke.mp4",
            "mime_type": "video/mp4",
            "sha256": hashlib.sha256(Path(temp_path).read_bytes()).hexdigest(),
            "created_at": utc_now_iso(),
            "expires_at": now + min(settings.asset_ttl_seconds(), 300),
            **probe,
        }
        with tenant_context(owner_hash):
            return persist_asset(temp_path, asset, ".mp4")
    finally:
        Path(temp_path).unlink(missing_ok=True)


def wait_for_job(headers: dict[str, str], job_id: str) -> dict[str, Any]:
    try:
        timeout_seconds = float(os.getenv("MCP_SMOKE_JOB_TIMEOUT_SECONDS", "60"))
    except ValueError as exc:
        raise AssertionError("MCP_SMOKE_JOB_TIMEOUT_SECONDS must be numeric") from exc
    require(1 <= timeout_seconds <= 300, "MCP_SMOKE_JOB_TIMEOUT_SECONDS is out of range")
    deadline = time.monotonic() + timeout_seconds
    request_id = 101
    while time.monotonic() < deadline:
        status = call_tool(headers, request_id, "job_status", {"job_id": job_id})
        state = status.get("state")
        if state == "success":
            return status
        require(state in {"queued", "running"}, f"media job failed: state={state!r}")
        request_id += 1
        time.sleep(0.25)
    raise AssertionError("media job did not finish before the smoke deadline")


def dereference_local_signed_url(url: str) -> tuple[dict[str, str], bytes]:
    parsed = urlsplit(url)
    require(parsed.scheme == "http", "signed smoke URL must use local HTTP")
    require(parsed.hostname in {"127.0.0.1", "localhost"}, "signed smoke URL is not local")
    require(parsed.username is None and parsed.password is None, "signed smoke URL has credentials")
    require((parsed.port or 80) == PORT, "signed smoke URL uses an unexpected port")
    require(not parsed.fragment, "signed smoke URL has a fragment")
    target = parsed.path + (f"?{parsed.query}" if parsed.query else "")
    connection = http.client.HTTPConnection(HOST, PORT, timeout=8)
    try:
        connection.request("GET", target, headers={"Accept": "application/octet-stream"})
        response = connection.getresponse()
        body = response.read()
        headers = {key.lower(): value for key, value in response.getheaders()}
        require(response.status == 200, f"signed download failed with HTTP {response.status}")
    finally:
        connection.close()
    return headers, body


def cleanup_managed_assets(owner_hash: str, asset_ids: list[str]) -> list[str]:
    from madpanda_ffmpeg_mcp.redis_store import get_asset_control
    from madpanda_ffmpeg_mcp.storage import delete_managed_asset
    from madpanda_ffmpeg_mcp.tenant import tenant_context

    failures: list[str] = []
    with tenant_context(owner_hash):
        for asset_id in reversed(dict.fromkeys(asset_ids)):
            try:
                if get_asset_control(asset_id) is None:
                    continue
                deleted = delete_managed_asset(asset_id, force=True, owner_hash=owner_hash)
                if not deleted or get_asset_control(asset_id) is not None:
                    failures.append(asset_id)
            except Exception:
                failures.append(asset_id)
    return failures


def run_media_roundtrip(headers: dict[str, str]) -> dict[str, Any]:
    from madpanda_ffmpeg_mcp.config import settings
    from madpanda_ffmpeg_mcp.ffprobe_utils import run_ffprobe
    from madpanda_ffmpeg_mcp.media_limits import validate_media_probe
    from madpanda_ffmpeg_mcp.redis_store import get_asset
    from madpanda_ffmpeg_mcp.storage import local_path_from_key
    from madpanda_ffmpeg_mcp.tenant import tenant_context
    from madpanda_ffmpeg_mcp.utils import utc_now_ts

    require(settings.storage_backend == "local", "media roundtrip requires local shared storage")
    owner_hash = authenticated_owner()
    managed_asset_ids: list[str] = []
    cleanup_failures: list[str] = []
    summary: dict[str, Any] | None = None
    try:
        input_asset = create_synthetic_input(owner_hash)
        input_asset_id = input_asset.get("asset_id")
        if not isinstance(input_asset_id, str) or not input_asset_id:
            raise AssertionError("managed synthetic input has no asset id")
        managed_asset_ids.append(input_asset_id)

        submitted = call_tool(
            headers,
            100,
            "ffmpeg_thumbnail",
            {"asset_id": input_asset_id, "time_sec": 0.1, "width": 64},
        )
        job_id = submitted.get("job_id")
        if not isinstance(job_id, str) or not job_id:
            raise AssertionError("thumbnail returned no job_id")
        require(submitted.get("cache_hit") is False, "synthetic thumbnail unexpectedly hit cache")
        job = wait_for_job(headers, job_id)
        output_ids = job.get("output_asset_ids")
        if not isinstance(output_ids, list) or len(output_ids) != 1:
            raise AssertionError("thumbnail job returned an invalid output list")
        output_asset_id = output_ids[0]
        if not isinstance(output_asset_id, str) or not output_asset_id:
            raise AssertionError("thumbnail output asset id is invalid")
        managed_asset_ids.append(output_asset_id)

        with tenant_context(owner_hash):
            output_asset = get_asset(output_asset_id)
        if output_asset is None:
            raise AssertionError("worker output is not visible to the authenticated owner")
        require(
            output_asset.get("parent_asset_id") == input_asset_id,
            "worker output parent does not match the synthetic input",
        )
        storage_key = output_asset.get("storage_key")
        if not isinstance(storage_key, str) or not storage_key:
            raise AssertionError("worker output has no storage key")
        output_path = local_path_from_key(storage_key)
        require(os.path.isfile(output_path), "worker output is absent from shared storage")
        output_bytes = Path(output_path).read_bytes()
        require(bool(output_bytes), "worker output is empty")
        require(len(output_bytes) == output_asset.get("size_bytes"), "output size readback differs")
        validate_media_probe(run_ffprobe(output_path), expected_kind="image")

        probe = call_tool(headers, 201, "media_probe", {"asset_id": output_asset_id})
        require(isinstance(probe.get("streams"), list), "MCP output probe has no streams")

        signed = call_tool(
            headers,
            202,
            "media_get_download_url",
            {"asset_id": output_asset_id},
        )
        signed_url = signed.get("url")
        signed_expiry = signed.get("expires_at")
        asset_expiry = output_asset.get("expires_at")
        if not isinstance(signed_url, str) or not signed_url:
            raise AssertionError("signed URL is missing")
        if isinstance(signed_expiry, bool) or not isinstance(signed_expiry, int):
            raise AssertionError("signed URL expiry is invalid")
        if isinstance(asset_expiry, bool) or not isinstance(asset_expiry, int):
            raise AssertionError("output retention is invalid")
        require(utc_now_ts() < signed_expiry <= asset_expiry, "signed URL exceeds retention")
        query_expiry = parse_qs(urlsplit(signed_url).query, strict_parsing=True).get("exp")
        require(query_expiry == [str(signed_expiry)], "signed URL expiry does not match response")
        download_headers, downloaded = dereference_local_signed_url(signed_url)
        require(downloaded == output_bytes, "signed download differs from shared output")
        require(
            download_headers.get("content-length") == str(len(output_bytes)),
            "signed download length differs",
        )
        summary = {
            "ok": True,
            "job_state": job.get("state"),
            "output_bytes": len(output_bytes),
        }
    finally:
        cleanup_failures = cleanup_managed_assets(owner_hash, managed_asset_ids)

    require(not cleanup_failures, "managed smoke asset cleanup failed")
    require(summary is not None, "media roundtrip produced no summary")
    return summary


def main() -> None:
    status, _, health = request("GET", "/health", headers={"Accept": "application/json"})
    require(status == 200, f"health status={status}")
    require(isinstance(health, dict), "health is not JSON")
    require(health.get("status") == "healthy", f"health={health}")
    require(health.get("server_version") == PACKAGE_VERSION, f"version={health}")
    require(health.get("tool_count") == EXPECTED_TOOL_COUNT, f"tool_count={health}")
    require(
        health.get("catalog_version") == EXPECTED_CATALOG_VERSION,
        f"catalog={health}",
    )
    require(health.get("build_sha") == EXPECTED_BUILD_SHA, f"build_sha={health}")
    require(
        health.get("source_fingerprint") == EXPECTED_SOURCE_FINGERPRINT,
        f"source_fingerprint={health}",
    )
    require(health.get("image_reference") == EXPECTED_IMAGE_REFERENCE, f"image={health}")

    status, _, denied = request("POST", "/mcp", payload=b"malformed-before-auth")
    require(status == 401, f"missing auth was not rejected first: {status} {denied}")

    status, _, denied = request(
        "POST", "/mcp", payload=b"malformed-before-auth", headers=auth_headers(valid=False)
    )
    require(status == 401, f"invalid auth was not rejected first: {status} {denied}")

    origin_headers = auth_headers()
    origin_headers["Origin"] = "https://untrusted.invalid"
    status, _, denied = request(
        "POST", "/mcp", payload=rpc("tools/list", 2, {}), headers=origin_headers
    )
    require(status == 403, f"browser Origin was not rejected: {status} {denied}")

    status, response_headers, initialized = request(
        "POST",
        "/mcp",
        payload=rpc(
            "initialize",
            3,
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "ffmpeg-mcp-image-smoke", "version": "1"},
            },
        ),
        headers=auth_headers(),
    )
    require(status == 200, f"initialize failed: {status} {initialized}")
    initialization_result = initialized.get("result") if isinstance(initialized, dict) else None
    require(isinstance(initialization_result, dict), f"initialize={initialized}")
    server_info = initialization_result.get("serverInfo")
    require(isinstance(server_info, dict), f"serverInfo={server_info}")
    require(server_info.get("name") == "ffmpeg-mcp", f"serverInfo.name={server_info}")
    require(
        server_info.get("version") == PACKAGE_VERSION,
        f"serverInfo.version={server_info}",
    )

    discovery_headers = auth_headers()
    session_id = response_headers.get("mcp-session-id")
    if session_id:
        discovery_headers["Mcp-Session-Id"] = session_id
    status, _, tools = request(
        "POST", "/mcp", payload=rpc("tools/list", 4, {}), headers=discovery_headers
    )
    require(status == 200, f"tools/list failed: {status} {tools}")
    listed = tools.get("result", {}).get("tools", []) if isinstance(tools, dict) else []
    require(len(listed) == EXPECTED_TOOL_COUNT, f"tools/list count={len(listed)}")
    names = {tool.get("name") for tool in listed if isinstance(tool, dict)}
    for required in ("list_capabilities", "media_ingest_from_url", "ffmpeg_transcode"):
        require(required in names, f"required tool is missing: {required}")

    status, _, capability = request(
        "POST",
        "/mcp",
        payload=rpc(
            "tools/call",
            5,
            {"name": "list_capabilities", "arguments": {"include_descriptors": False}},
        ),
        headers=discovery_headers,
    )
    require(status == 200, f"local navigation failed: {status} {capability}")
    require(not capability.get("result", {}).get("isError", False), f"navigation={capability}")

    service_capabilities = call_tool(discovery_headers, 6, "ffmpeg_capabilities", {})
    limits = service_capabilities.get("limits")
    require(isinstance(limits, dict), "ffmpeg_capabilities has no limits")
    for required_limit in (
        "ingest_staging_owner_max_active",
        "ingest_staging_global_max_active",
        "ingest_staging_owner_max_bytes",
        "ingest_staging_global_max_bytes",
        "ingest_staging_lease_seconds",
        "ingest_staging_heartbeat_seconds",
    ):
        require(required_limit in limits, f"capability limit is missing: {required_limit}")

    summary: dict[str, Any] = {"ok": True, "mode": MODE, "tool_count": len(listed)}
    if media_roundtrip_enabled():
        summary["media_roundtrip"] = run_media_roundtrip(discovery_headers)
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
