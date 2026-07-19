#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
  echo "usage: $0 IMAGE BUILD_SHA [SOURCE_FINGERPRINT] [IMAGE_REFERENCE]" >&2
  exit 2
fi

image=$1
build_sha=$2
source_fingerprint=${3:-development}
image_reference=${4:-development}
portal_grant=ci-portal-grant-000000000000000000000000000000000000000000
access_token=ci-standalone-token-000000000000000000000000000000000000000000
download_secret=ci-download-signing-secret-000000000000000000000000000000000000000
principal_secret=ci-principal-hash-secret-00000000000000000000000000000000000000
redis_password=ci-redis-password-000000000000000000000000000000000000000
redis_image=redis:7.4.5-alpine@sha256:0302cccee2b2043e61b497c8f4075467c5f7ba27a9f38be7e092634f2734baed
minio_access_key=ci-minio-access
minio_secret_key=ci-minio-secret-000000000000000000000000000000000000000
minio_image=minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:a1a8bd4ac40ad7881a245bab97323e18f971e4d4cba2c2007ec1bedd21cbaba2
network="ffmpeg-mcp-smoke-$$"
redis_container="$network-redis"
minio_container="$network-minio"
redis_contract_container="$network-redis-contracts"
s3_contract_container="$network-s3-contracts"
active_container=

cleanup() {
  if [[ -n "$active_container" ]]; then
    docker rm -f "$active_container" >/dev/null 2>&1 || true
  fi
  docker rm -f "$s3_contract_container" >/dev/null 2>&1 || true
  docker rm -f "$redis_contract_container" >/dev/null 2>&1 || true
  docker rm -f "$minio_container" >/dev/null 2>&1 || true
  docker rm -f "$redis_container" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT

test "$(docker image inspect --format '{{.Config.User}}' "$image")" = "10001:10001"
test -n "$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$image")"

docker network create --internal "$network" >/dev/null
docker run -d --rm --name "$redis_container" --network "$network" --network-alias redis \
  --user 999:1000 --read-only --cap-drop ALL --security-opt no-new-privileges --pids-limit 128 \
  -e "REDIS_PASSWORD=$redis_password" \
  --tmpfs /data:rw,noexec,nosuid,nodev,size=128m,uid=999,gid=1000,mode=0700 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m \
  "$redis_image" sh -c 'exec redis-server --save "" --appendonly no --maxmemory 100663296 --maxmemory-policy noeviction --requirepass "$REDIS_PASSWORD"' >/dev/null

docker run -d --rm --name "$minio_container" --network "$network" --network-alias minio \
  --user 10001:10001 --read-only --cap-drop ALL --security-opt no-new-privileges --pids-limit 256 \
  -e HOME=/tmp \
  -e "MINIO_ROOT_USER=$minio_access_key" \
  -e "MINIO_ROOT_PASSWORD=$minio_secret_key" \
  --tmpfs /data:rw,noexec,nosuid,nodev,size=128m,uid=10001,gid=10001,mode=0700 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m,uid=10001,gid=10001,mode=0700 \
  "$minio_image" server /data --address :9000 --console-address :9001 >/dev/null

redis_ready=false
for _ in {1..20}; do
  if docker exec -e "REDISCLI_AUTH=$redis_password" "$redis_container" redis-cli ping \
    2>/dev/null | grep -qx PONG; then
    redis_ready=true
    break
  fi
  sleep 1
done
if [[ "$redis_ready" != true ]]; then
  docker logs "$redis_container" >&2 || true
  exit 1
fi
test "$(docker exec "$redis_container" id -u)" = 999
test "$(docker exec "$redis_container" id -g)" = 1000

minio_ready=false
for _ in {1..30}; do
  if docker exec "$minio_container" \
    curl --connect-timeout 1 --max-time 2 --fail --silent --show-error \
    http://127.0.0.1:9000/minio/health/ready \
    >/dev/null 2>&1; then
    minio_ready=true
    break
  fi
  sleep 1
done
if [[ "$minio_ready" != true ]]; then
  docker logs "$minio_container" >&2 || true
  exit 1
fi

# Execute the exact Lua admission and brand-kit contracts against the pinned Redis image.
timeout --signal=TERM --kill-after=5s 120s \
  docker run --rm -i --name "$redis_contract_container" \
  --network "$network" --user 10001:10001 \
  -e "REDIS_URL=redis://:$redis_password@redis:6379/0" \
  "$image" python - <<'PY'
import time
from dataclasses import replace

from rq import SimpleWorker
from rq.job import Job

from madpanda_ffmpeg_mcp import redis_store
from madpanda_ffmpeg_mcp import server
from madpanda_ffmpeg_mcp.task_queue import get_queue
from madpanda_ffmpeg_mcp.tenant import tenant_context

owner_a = "a" * 64
owner_b = "b" * 64
owner_c = "c" * 64
redis_store.settings = replace(
    redis_store.settings,
    job_admission_owner_max_active=1,
    job_admission_global_max_active=2,
    job_admission_owner_rpm=2,
    brand_kit_max_count=1,
    asset_quota_owner_max_count=1,
    asset_quota_owner_max_bytes=10,
    asset_quota_global_max_count=2,
    asset_quota_global_max_bytes=20,
    asset_delete_retry_base_seconds=1,
    asset_delete_retry_max_seconds=60,
    max_ingest_bytes=5,
    ingest_staging_owner_max_active=1,
    ingest_staging_global_max_active=2,
    ingest_staging_owner_max_bytes=15,
    ingest_staging_global_max_bytes=20,
    ingest_staging_lease_seconds=60,
    ingest_staging_heartbeat_seconds=5,
)
client = redis_store.get_redis()
client.flushdb()

# Remote URL bytes cannot enter shared staging without a durable owner/global
# reservation. Exercise the exact Lua contract, including lease and token fences.
staging_a = redis_store.reserve_ingest_staging(owner_hash=owner_a)
assert staging_a.reserved_bytes == 5
assert client.hlen(redis_store.INGEST_STAGING_RESERVATIONS_KEY) == 1
assert redis_store.refresh_ingest_staging(staging_a) is not None
assert client.zscore(
    redis_store.INGEST_STAGING_EXPIRY_KEY,
    staging_a.reservation_id,
) is not None
wrong_staging_token = replace(staging_a, token="f" * 32)
assert redis_store.refresh_ingest_staging(wrong_staging_token) is None
assert not redis_store.release_ingest_staging(wrong_staging_token)
try:
    redis_store.reserve_ingest_staging(owner_hash=owner_a)
except redis_store.IngestStagingAdmissionError as exc:
    assert str(exc) == "tenant remote-ingest staging limit reached"
else:
    raise AssertionError("owner staging admission limit did not reject")
staging_b = redis_store.reserve_ingest_staging(owner_hash=owner_b)
try:
    redis_store.reserve_ingest_staging(owner_hash=owner_c)
except redis_store.IngestStagingAdmissionError as exc:
    assert str(exc) == "service remote-ingest staging limit reached"
else:
    raise AssertionError("global staging admission limit did not reject")

# Lowering the live global quota below the old hash cardinality must still scan
# the fixed hard ceiling, reap expired crash leases, and admit within the new cap.
redis_store.settings = replace(
    redis_store.settings,
    ingest_staging_global_max_active=1,
    ingest_staging_owner_max_bytes=5,
    ingest_staging_global_max_bytes=5,
)
original_now = redis_store._now_ts
redis_store._now_ts = lambda: max(staging_a.lease_until, staging_b.lease_until)
try:
    staging_c = redis_store.reserve_ingest_staging(owner_hash=owner_c)
finally:
    redis_store._now_ts = original_now
assert client.hlen(redis_store.INGEST_STAGING_RESERVATIONS_KEY) == 1
assert client.hexists(
    redis_store.INGEST_STAGING_RESERVATIONS_KEY,
    staging_c.reservation_id,
)
assert redis_store.release_ingest_staging(staging_c)
assert redis_store.release_ingest_staging(staging_c)
assert client.hlen(redis_store.INGEST_STAGING_RESERVATIONS_KEY) == 0
assert client.zcard(redis_store.INGEST_STAGING_EXPIRY_KEY) == 0

# Prove the byte branches independently: active-count ceilings remain above the
# live cardinality while one maximum-size charge exhausts owner bytes and two
# charges exhaust global bytes.
redis_store.settings = replace(
    redis_store.settings,
    ingest_staging_owner_max_active=3,
    ingest_staging_global_max_active=4,
    ingest_staging_owner_max_bytes=5,
    ingest_staging_global_max_bytes=10,
)
staging_byte_a = redis_store.reserve_ingest_staging(owner_hash=owner_a)
try:
    redis_store.reserve_ingest_staging(owner_hash=owner_a)
except redis_store.IngestStagingAdmissionError as exc:
    assert str(exc) == "tenant remote-ingest staging limit reached"
else:
    raise AssertionError("owner staging byte limit did not reject")
staging_byte_b = redis_store.reserve_ingest_staging(owner_hash=owner_b)
try:
    redis_store.reserve_ingest_staging(owner_hash=owner_c)
except redis_store.IngestStagingAdmissionError as exc:
    assert str(exc) == "service remote-ingest staging limit reached"
else:
    raise AssertionError("global staging byte limit did not reject")
assert redis_store.release_ingest_staging(staging_byte_a)
assert redis_store.release_ingest_staging(staging_byte_b)
assert client.hlen(redis_store.INGEST_STAGING_RESERVATIONS_KEY) == 0
assert client.zcard(redis_store.INGEST_STAGING_EXPIRY_KEY) == 0

client.flushdb()
redis_store.reserve_job_admission("a1", owner_hash=owner_a)
try:
    redis_store.reserve_job_admission("a-owner-limit", owner_hash=owner_a)
except redis_store.JobAdmissionError as exc:
    assert str(exc) == "tenant active job limit reached"
else:
    raise AssertionError("owner admission limit did not reject")
redis_store.reserve_job_admission("b1", owner_hash=owner_b)
before_refresh = client.zscore(redis_store.JOB_ADMISSION_GLOBAL_KEY, "b1")
redis_store.refresh_job_admission("b1", 120, owner_hash=owner_b)
after_refresh = client.zscore(redis_store.JOB_ADMISSION_GLOBAL_KEY, "b1")
assert before_refresh is not None and after_refresh is not None
assert after_refresh < before_refresh
try:
    redis_store.reserve_job_admission("c-global-limit", owner_hash=owner_c)
except redis_store.JobAdmissionError as exc:
    assert str(exc) == "service active job limit reached"
else:
    raise AssertionError("global admission limit did not reject")
assert redis_store.release_job_admission("a1", owner_hash=owner_a)
redis_store.reserve_job_admission("a2", owner_hash=owner_a)
assert redis_store.release_job_admission("a2", owner_hash=owner_a)
try:
    redis_store.reserve_job_admission("a-rate-limit", owner_hash=owner_a)
except redis_store.JobAdmissionError as exc:
    assert str(exc) == "tenant enqueue rate limit reached"
else:
    raise AssertionError("enqueue rate limit did not reject")

with tenant_context(owner_a):
    redis_store.save_brand_kit({"brand_kit_id": "one", "name": "first"})
    redis_store.save_brand_kit({"brand_kit_id": "one", "name": "updated"})
    assert redis_store.get_brand_kit("one")["name"] == "updated"
    try:
        redis_store.save_brand_kit({"brand_kit_id": "two"})
    except redis_store.BrandKitLimitError as exc:
        assert str(exc) == "brand kit limit reached"
    else:
        raise AssertionError("brand-kit count limit did not reject")

# Execute the durable asset lifecycle against real Redis Lua and prove quota
# remains charged through delete retry, then releases exactly once on finalize.
client.flushdb()
asset_id = "1" * 32
reservation_token = "2" * 32
asset = {
    "asset_id": asset_id,
    "storage_key": f"11/11/{asset_id}.mp4",
    "storage_uri": f"local://11/11/{asset_id}.mp4",
    "size_bytes": 5,
    "expires_at": int(time.time()) + 300,
}
with tenant_context(owner_a):
    redis_store.reserve_asset(asset, reservation_token)
    assert redis_store.get_asset(asset_id) is None
    redis_store.commit_asset(asset, reservation_token)
    assert redis_store.get_asset(asset_id)["size_bytes"] == 5
assert client.hget(redis_store._asset_quota_owner_key(owner_a), "count") == "1"
assert client.hget(redis_store._asset_quota_owner_key(owner_a), "bytes") == "5"
assert client.hget(redis_store.ASSET_QUOTA_GLOBAL_KEY, "count") == "1"
assert client.hget(redis_store.ASSET_QUOTA_GLOBAL_KEY, "bytes") == "5"

# Reject the next owner reservation while owner A's first asset is charged.
owner_limit_id = "9" * 32
owner_limit_asset = {
    **asset,
    "asset_id": owner_limit_id,
    "storage_key": f"99/99/{owner_limit_id}.mp4",
    "storage_uri": f"local://99/99/{owner_limit_id}.mp4",
}
with tenant_context(owner_a):
    try:
        redis_store.reserve_asset(owner_limit_asset, "a" * 32)
    except redis_store.AssetQuotaError as exc:
        assert str(exc) == "tenant managed-storage quota reached"
    else:
        raise AssertionError("owner asset quota did not reject")
assert redis_store.get_asset_control(owner_limit_id) is None

# Fill the second global slot with owner B, then reject owner C globally.
global_fill_id = "b" * 32
global_fill_asset = {
    **asset,
    "asset_id": global_fill_id,
    "storage_key": f"bb/bb/{global_fill_id}.mp4",
    "storage_uri": f"local://bb/bb/{global_fill_id}.mp4",
}
with tenant_context(owner_b):
    redis_store.reserve_asset(global_fill_asset, "c" * 32)
    redis_store.commit_asset(global_fill_asset, "c" * 32)
assert client.hget(redis_store.ASSET_QUOTA_GLOBAL_KEY, "count") == "2"
assert client.hget(redis_store.ASSET_QUOTA_GLOBAL_KEY, "bytes") == "10"
global_limit_id = "d" * 32
global_limit_asset = {
    **asset,
    "asset_id": global_limit_id,
    "storage_key": f"dd/dd/{global_limit_id}.mp4",
    "storage_uri": f"local://dd/dd/{global_limit_id}.mp4",
}
with tenant_context(owner_c):
    try:
        redis_store.reserve_asset(global_limit_asset, "e" * 32)
    except redis_store.AssetQuotaError as exc:
        assert str(exc) == "service managed-storage quota reached"
    else:
        raise AssertionError("global asset quota did not reject")
assert redis_store.get_asset_control(global_limit_id) is None
global_fill_claim = redis_store.claim_asset_deletion(
    global_fill_id,
    "f" * 32,
    force=True,
    owner_hash=owner_b,
)
assert global_fill_claim is not None
assert redis_store.finalize_asset_deletion(global_fill_claim)
assert client.hget(redis_store.ASSET_QUOTA_GLOBAL_KEY, "count") == "1"
assert client.exists(redis_store._asset_quota_owner_key(owner_b)) == 0

with tenant_context(owner_b):
    assert redis_store.get_asset(asset_id) is None
    assert redis_store.claim_asset_deletion(
        asset_id, "3" * 32, force=True, owner_hash=owner_b
    ) is None
claimed = redis_store.claim_asset_deletion(
    asset_id, "4" * 32, force=True, owner_hash=owner_a
)
assert claimed is not None
retry_at = redis_store.schedule_asset_delete_retry(asset_id, "4" * 32)
assert retry_at is not None
assert client.hget(redis_store.ASSET_QUOTA_GLOBAL_KEY, "count") == "1"
original_now = redis_store._now_ts
redis_store._now_ts = lambda: retry_at
try:
    claimed = redis_store.claim_asset_deletion(asset_id, "5" * 32)
finally:
    redis_store._now_ts = original_now
assert claimed is not None
assert redis_store.finalize_asset_deletion(claimed)
assert redis_store.finalize_asset_deletion(claimed)
assert client.exists(redis_store.ASSET_QUOTA_GLOBAL_KEY) == 0
assert client.exists(redis_store._asset_quota_owner_key(owner_a)) == 0
assert client.zcard(redis_store.ASSET_EXPIRY_SET) == 0

# A commit acknowledgement can be lost after Redis has already activated the
# asset. The reservation token must still fence an ambiguity-safe abort.
ambiguous_id = "6" * 32
ambiguous_token = "7" * 32
ambiguous_asset = {
    **asset,
    "asset_id": ambiguous_id,
    "storage_key": f"66/66/{ambiguous_id}.mp4",
    "storage_uri": f"local://66/66/{ambiguous_id}.mp4",
}
with tenant_context(owner_a):
    redis_store.reserve_asset(ambiguous_asset, ambiguous_token)
    redis_store.commit_asset(ambiguous_asset, ambiguous_token)
assert (
    redis_store.abort_asset_reservation(
        ambiguous_id,
        "0" * 32,
        "5" * 32,
        owner_hash=owner_a,
    )
    is None
)
assert redis_store.get_asset_control(ambiguous_id)["state"] == "active"
claimed = redis_store.abort_asset_reservation(
    ambiguous_id,
    ambiguous_token,
    "8" * 32,
    owner_hash=owner_a,
)
assert claimed is not None and claimed["state"] == "deleting"
assert redis_store.finalize_asset_deletion(claimed)
assert client.exists(redis_store.ASSET_QUOTA_GLOBAL_KEY) == 0
assert client.exists(redis_store._asset_quota_owner_key(owner_a)) == 0
assert client.zcard(redis_store.ASSET_EXPIRY_SET) == 0

client.flushdb()
with tenant_context(owner_a):
    job_id = server._enqueue_job("rq_roundtrip", len, ((1, 2, 3),))
queue = get_queue()
worker = SimpleWorker([queue], connection=redis_store.get_rq_redis())
assert worker.work(burst=True, logging_level="WARNING")
rq_job = Job.fetch(job_id, connection=redis_store.get_rq_redis())
assert rq_job.return_value() == 3
assert client.zscore(redis_store.JOB_ADMISSION_GLOBAL_KEY, job_id) is None
assert client.zscore(redis_store._job_admission_owner_key(owner_a), job_id) is None
assert client.zscore(redis_store._job_admission_rate_key(owner_a), job_id) is not None
client.flushdb()
PY

# Exercise the managed lifecycle against a pinned, synthetic S3-compatible backend.
timeout --signal=TERM --kill-after=5s 120s \
  docker run --rm -i --name "$s3_contract_container" \
  --network "$network" --user 10001:10001 \
  --tmpfs /data:rw,nosuid,nodev,size=16m,uid=10001,gid=10001,mode=0700 \
  -e "REDIS_URL=redis://:$redis_password@redis:6379/0" \
  -e STORAGE_BACKEND=s3 \
  -e S3_BUCKET=ffmpeg-smoke \
  -e S3_REGION=us-east-1 \
  -e S3_ENDPOINT_URL=http://minio:9000 \
  -e "S3_ACCESS_KEY=$minio_access_key" \
  -e "S3_SECRET_KEY=$minio_secret_key" \
  -e S3_CONNECT_TIMEOUT_SECONDS=5 \
  -e S3_READ_TIMEOUT_SECONDS=10 \
  -e STORAGE_ASGI_OPERATION_TIMEOUT_SECONDS=20 \
  -e MAX_INGEST_BYTES=64 \
  -e MAX_OUTPUT_BYTES=64 \
  -e ASSET_QUOTA_OWNER_MAX_COUNT=2 \
  -e ASSET_QUOTA_OWNER_MAX_BYTES=128 \
  -e ASSET_QUOTA_GLOBAL_MAX_COUNT=4 \
  -e ASSET_QUOTA_GLOBAL_MAX_BYTES=256 \
  -e JOB_STORAGE_MAX_OUTPUT_COUNT=2 \
  -e JOB_STORAGE_MAX_OUTPUT_BYTES=64 \
  -e JOB_STORAGE_MAX_MATERIALIZE_BYTES=64 \
  -e AWS_EC2_METADATA_DISABLED=true \
  "$image" python - <<'PY'
import os
import time
import urllib.request
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from botocore.exceptions import ClientError

from madpanda_ffmpeg_mcp import redis_store, storage
from madpanda_ffmpeg_mcp.tenant import tenant_context

owner = "1" * 64
asset_id = "2" * 32
payload = b"managed-s3-smoke"
asset = {
    "asset_id": asset_id,
    "expires_at": int(time.time()) + 300,
    "mime_type": "application/octet-stream",
    "original_filename": "smoke.bin",
}

storage.settings.validate_worker_runtime()
client = storage.get_storage_client()
assert client is not None
client.create_bucket(Bucket=storage.settings.s3_bucket)
redis = redis_store.get_redis()
redis.flushdb()

source = Path(storage.settings.storage_temp_dir) / "managed-s3-smoke.bin"
source.parent.mkdir(parents=True, exist_ok=True)
source.write_bytes(payload)
with tenant_context(owner):
    persisted = storage.persist_asset(str(source), asset, ".bin")
    visible = redis_store.get_asset(asset_id)
assert not source.exists()
assert visible is not None and visible["size_bytes"] == len(payload)
assert redis.hget(redis_store._asset_quota_owner_key(owner), "count") == "1"
assert redis.hget(redis_store._asset_quota_owner_key(owner), "bytes") == str(len(payload))
assert redis.hget(redis_store.ASSET_QUOTA_GLOBAL_KEY, "count") == "1"
assert redis.hget(redis_store.ASSET_QUOTA_GLOBAL_KEY, "bytes") == str(len(payload))
storage_key = persisted["storage_key"]

response = client.get_object(Bucket=storage.settings.s3_bucket, Key=storage_key)
try:
    assert int(response["ContentLength"]) == len(payload)
    assert response["Body"].read() == payload
finally:
    response["Body"].close()

downloaded = storage.download_to_temp(storage_key)
try:
    assert Path(downloaded).read_bytes() == payload
finally:
    os.unlink(downloaded)

remaining_lifetime = asset["expires_at"] - int(time.time())
url, signed_expiry = storage.generate_download_url(
    asset_id,
    storage_key,
    asset["expires_at"],
)
assert signed_expiry <= asset["expires_at"]
signed_query = {key.lower(): value for key, value in parse_qs(urlparse(url).query).items()}
if "x-amz-expires" in signed_query:
    assert 0 < int(signed_query["x-amz-expires"][0]) <= remaining_lifetime
else:
    assert 0 < int(signed_query["expires"][0]) - int(time.time()) <= remaining_lifetime
with urllib.request.urlopen(url, timeout=10) as signed_response:
    assert signed_response.read() == payload

oversized_key = "unmanaged/oversized.bin"
client.put_object(
    Bucket=storage.settings.s3_bucket,
    Key=oversized_key,
    Body=b"x" * 65,
    ContentLength=65,
)
try:
    storage.download_to_temp(oversized_key)
except storage.StorageError as exc:
    assert "size limit" in str(exc)
else:
    raise AssertionError("oversized S3 download did not reject")
client.delete_object(Bucket=storage.settings.s3_bucket, Key=oversized_key)

assert storage.delete_managed_asset(asset_id, force=True, owner_hash=owner)
assert redis_store.get_asset_control(asset_id) is None
assert redis.exists(redis_store.ASSET_QUOTA_GLOBAL_KEY) == 0
assert redis.exists(redis_store._asset_quota_owner_key(owner)) == 0
assert redis.zcard(redis_store.ASSET_EXPIRY_SET) == 0
try:
    client.head_object(Bucket=storage.settings.s3_bucket, Key=storage_key)
except ClientError as exc:
    assert exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}
else:
    raise AssertionError("deleted S3 object still exists")
client.delete_bucket(Bucket=storage.settings.s3_bucket)
redis.flushdb()
PY

for mode in portal standalone; do
  active_container="$network-$mode"
  mode_env=(-e "MCP_MODE=$mode")
  if [[ "$mode" == portal ]]; then
    mode_env+=(-e "MCP_PORTAL_GRANT_TOKEN=$portal_grant")
  else
    mode_env+=(-e "MCP_ACCESS_TOKEN=$access_token")
  fi

  docker run -d --rm --name "$active_container" --network "$network" \
    --init --read-only --user 10001:10001 --cap-drop ALL \
    --security-opt no-new-privileges --pids-limit 512 \
    --tmpfs /tmp:rw,noexec,nosuid,nodev,size=128m,mode=1777 \
    --tmpfs /data:rw,nosuid,nodev,size=256m,uid=10001,gid=10001,mode=0700 \
    "${mode_env[@]}" \
    -e "EXPECTED_BUILD_SHA=$build_sha" \
    -e "EXPECTED_SOURCE_FINGERPRINT=$source_fingerprint" \
    -e "EXPECTED_IMAGE_REFERENCE=$image_reference" \
    -e "MCP_IMAGE_REFERENCE=$image_reference" \
    -e MCP_EXPECTED_TOOL_COUNT=55 \
    -e "MCP_PRINCIPAL_HASH_SECRET=$principal_secret" \
    -e MCP_ALLOWED_HOSTS=127.0.0.1,localhost,ffmpeg-mcp \
    -e "REDIS_URL=redis://:$redis_password@redis:6379/0" \
    -e PUBLIC_BASE_URL=http://127.0.0.1:8087 \
    -e "DOWNLOAD_SIGNING_SECRET=$download_secret" \
    "$image" >/dev/null

  ready=false
  for _ in {1..45}; do
    if docker exec "$active_container" python -c \
      "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8087/health', timeout=2).read()" \
      >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "$ready" != true ]]; then
    docker logs "$active_container" >&2 || true
    exit 1
  fi
  if ! smoke_output=$(docker exec "$active_container" python /app/scripts/runtime_smoke.py 2>&1); then
    printf '%s\n' "$smoke_output" >&2
    docker logs "$active_container" >&2 || true
    exit 1
  fi
  printf '%s\n' "$smoke_output"
  if ! docker exec -i "$active_container" python - <<'PY'
import pathlib
import subprocess

output = pathlib.Path("/tmp/ffmpeg-mcp-synthetic-smoke.mp4")
subprocess.run(
    [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-f",
        "lavfi",
        "-i",
        "color=c=black:s=16x16:d=0.2:r=10",
        "-an",
        "-c:v",
        "mpeg4",
        "-y",
        str(output),
    ],
    check=True,
    timeout=30,
)
probe = subprocess.run(
    [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(output),
    ],
    check=True,
    capture_output=True,
    text=True,
    timeout=15,
)
assert output.stat().st_size > 0
assert float(probe.stdout.strip()) > 0
output.unlink()
PY
  then
    docker logs "$active_container" >&2 || true
    exit 1
  fi
  docker rm -f "$active_container" >/dev/null
  active_container=
done

active_container="$network-worker"
docker run -d --rm --name "$active_container" --network "$network" \
  --init --read-only --user 10001:10001 --cap-drop ALL \
  --security-opt no-new-privileges --pids-limit 512 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=128m,mode=1777 \
  --tmpfs /data:rw,nosuid,nodev,size=256m,uid=10001,gid=10001,mode=0700 \
  -e "REDIS_URL=redis://:$redis_password@redis:6379/0" \
  "$image" mad-mcp-ffmpeg-worker >/dev/null
sleep 3
test "$(docker inspect --format '{{.State.Running}}' "$active_container")" = true
docker rm -f "$active_container" >/dev/null
active_container=
