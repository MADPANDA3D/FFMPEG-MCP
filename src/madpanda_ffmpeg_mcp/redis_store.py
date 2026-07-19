import hashlib
import hmac
import json
import re
import time
import uuid
from dataclasses import dataclass
from typing import Any

import redis

from .brand_kit_policy import validate_brand_kit_id, validate_brand_kit_record
from .config import settings
from .tenant import current_owner_hash, require_owner_hash

ASSET_PREFIX = "asset:"
JOB_PREFIX = "job:"
ASSET_EXPIRY_SET = "asset:expiry"
ASSET_QUOTA_GLOBAL_KEY = "asset:quota:global"
ASSET_QUOTA_OWNER_PREFIX = "asset:quota:owner:"
INGEST_STAGING_RESERVATIONS_KEY = "ingest:staging:reservations:v1"
INGEST_STAGING_EXPIRY_KEY = "ingest:staging:expiry:v1"
_INGEST_STAGING_SCAN_LIMIT = 256
JOB_EXPIRY_SET = "job:expiry"
CACHE_PREFIX = "cache:result:"
BRAND_KIT_PREFIX = "brandkit:"
JOB_ADMISSION_GLOBAL_KEY = "job:admission:active"
JOB_ADMISSION_OWNER_PREFIX = "job:admission:owner:"
JOB_ADMISSION_RATE_PREFIX = "job:admission:rate:"
_OWNER_HASH_PATTERN = re.compile(r"^[a-f0-9]{64}$")

_ASSET_RESERVE_SCRIPT = """
local existing = redis.call('GET', KEYS[1])
if existing then
  local ok, record = pcall(cjson.decode, existing)
  if not ok or type(record) ~= 'table' then
    return {0, 6}
  end
  if record['state'] == 'reserved'
    and record['reservation_token'] == ARGV[2]
    and record['owner_hash'] == ARGV[3]
    and tonumber(record['size_bytes']) == tonumber(ARGV[4])
    and record['storage_key'] == ARGV[11]
    and tonumber(record['expires_at']) == tonumber(ARGV[12]) then
    return {1, 0}
  end
  return {0, 4}
end

local function quota_value(key, field)
  local raw = redis.call('HGET', key, field)
  if raw == false then
    return 0
  end
  local value = tonumber(raw)
  if value == nil or value < 0 or value ~= math.floor(value) then
    return nil
  end
  return value
end

local owner_count = quota_value(KEYS[2], 'count')
local owner_bytes = quota_value(KEYS[2], 'bytes')
local global_count = quota_value(KEYS[3], 'count')
local global_bytes = quota_value(KEYS[3], 'bytes')
if owner_count == nil or owner_bytes == nil or global_count == nil or global_bytes == nil then
  return {0, 6}
end
local size_bytes = tonumber(ARGV[4])
if owner_count + 1 > tonumber(ARGV[5]) or owner_bytes + size_bytes > tonumber(ARGV[6]) then
  return {0, 1}
end
if global_count + 1 > tonumber(ARGV[7]) or global_bytes + size_bytes > tonumber(ARGV[8]) then
  return {0, 2}
end

redis.call('HINCRBY', KEYS[2], 'count', 1)
redis.call('HINCRBY', KEYS[2], 'bytes', size_bytes)
redis.call('HINCRBY', KEYS[3], 'count', 1)
redis.call('HINCRBY', KEYS[3], 'bytes', size_bytes)
redis.call('SET', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[4], tonumber(ARGV[9]), ARGV[10])
return {1, 0}
"""

_ASSET_ABORT_RESERVATION_SCRIPT = """
local raw = redis.call('GET', KEYS[1])
if raw == false then
  return ''
end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table' or record['owner_hash'] ~= ARGV[5] then
  return ''
end
if record['state'] == 'deleting' and record['delete_token'] == ARGV[4] then
  return cjson.encode(record)
end
if (record['state'] ~= 'reserved' and record['state'] ~= 'active')
  or record['reservation_token'] ~= ARGV[2] then
  return ''
end
record['state'] = 'deleting'
record['delete_token'] = ARGV[4]
record['lease_until'] = tonumber(ARGV[3])
redis.call('SET', KEYS[1], cjson.encode(record))
redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[1])
return cjson.encode(record)
"""

_ASSET_COMMIT_SCRIPT = """
local raw = redis.call('GET', KEYS[1])
if raw == false then
  return 0
end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table' then
  return -2
end
if record['state'] == 'active' and record['reservation_token'] == ARGV[2] then
  return 1
end
if record['state'] ~= 'reserved' or record['reservation_token'] ~= ARGV[2] then
  return 0
end
local next_ok, next_record = pcall(cjson.decode, ARGV[3])
if not next_ok or type(next_record) ~= 'table'
  or next_record['state'] ~= 'active'
  or next_record['reservation_token'] ~= ARGV[2]
  or next_record['owner_hash'] ~= record['owner_hash']
  or tonumber(next_record['size_bytes']) ~= tonumber(record['size_bytes'])
  or next_record['storage_key'] ~= record['storage_key']
  or tonumber(next_record['expires_at']) ~= tonumber(record['expires_at']) then
  return -2
end
redis.call('SET', KEYS[1], ARGV[3])
redis.call('ZADD', KEYS[2], tonumber(ARGV[4]), ARGV[1])
return 1
"""

_ASSET_REFRESH_RESERVATION_SCRIPT = """
local raw = redis.call('GET', KEYS[1])
if raw == false then
  return 0
end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table'
  or record['state'] ~= 'reserved'
  or record['reservation_token'] ~= ARGV[2]
  or record['owner_hash'] ~= ARGV[4] then
  return 0
end
record['lease_until'] = tonumber(ARGV[3])
redis.call('SET', KEYS[1], cjson.encode(record))
redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[1])
return 1
"""

_ASSET_UPDATE_SCRIPT = """
local raw = redis.call('GET', KEYS[1])
if raw == false then
  return 0
end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table'
  or record['state'] ~= 'active'
  or record['owner_hash'] ~= ARGV[2]
  or tonumber(record['expires_at']) <= tonumber(ARGV[3]) then
  return 0
end
local next_ok, next_record = pcall(cjson.decode, ARGV[4])
if not next_ok or type(next_record) ~= 'table'
  or next_record['state'] ~= 'active'
  or next_record['owner_hash'] ~= record['owner_hash']
  or next_record['reservation_token'] ~= record['reservation_token']
  or tonumber(next_record['size_bytes']) ~= tonumber(record['size_bytes'])
  or next_record['storage_key'] ~= record['storage_key']
  or tonumber(next_record['expires_at']) ~= tonumber(record['expires_at']) then
  return -2
end
redis.call('SET', KEYS[1], ARGV[4])
return 1
"""

_ASSET_CLAIM_DELETE_SCRIPT = """
local raw = redis.call('GET', KEYS[1])
if raw == false then
  redis.call('ZREM', KEYS[2], ARGV[1])
  return ''
end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table' then
  return ''
end
local now = tonumber(ARGV[2])
if record['state'] == 'deleting' and record['delete_token'] == ARGV[4] then
  return cjson.encode(record)
end
local eligible = false
local actual_due = nil
if record['state'] == 'active' then
  actual_due = tonumber(record['expires_at'])
  local expired = actual_due ~= nil and actual_due <= now
  local forced = ARGV[5] == '1' and ARGV[6] ~= '' and record['owner_hash'] == ARGV[6]
  eligible = expired or forced
elseif record['state'] == 'reserved' then
  actual_due = tonumber(record['lease_until'])
  eligible = actual_due ~= nil and actual_due <= now
elseif record['state'] == 'delete_pending' then
  actual_due = tonumber(record['retry_at'])
  eligible = actual_due ~= nil and actual_due <= now
elseif record['state'] == 'deleting' then
  actual_due = tonumber(record['lease_until'])
  eligible = actual_due ~= nil and actual_due <= now
end
if not eligible then
  if actual_due ~= nil then
    redis.call('ZADD', KEYS[2], actual_due, ARGV[1])
  end
  return ''
end
record['state'] = 'deleting'
record['delete_token'] = ARGV[4]
record['lease_until'] = tonumber(ARGV[3])
record['retry_at'] = nil
redis.call('SET', KEYS[1], cjson.encode(record))
redis.call('ZADD', KEYS[2], tonumber(ARGV[3]), ARGV[1])
return cjson.encode(record)
"""

_ASSET_DELETE_RETRY_SCRIPT = """
local raw = redis.call('GET', KEYS[1])
if raw == false then
  return 0
end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table' then
  return 0
end
if record['state'] == 'delete_pending' and record['delete_token'] == ARGV[2] then
  return tonumber(record['retry_at']) or 0
end
if record['state'] ~= 'deleting' or record['delete_token'] ~= ARGV[2] then
  return 0
end
local attempts = math.min((tonumber(record['delete_attempts']) or 0) + 1, 30)
local delay = math.min(tonumber(ARGV[4]) * (2 ^ (attempts - 1)), tonumber(ARGV[5]))
local retry_at = tonumber(ARGV[3]) + delay
record['state'] = 'delete_pending'
record['delete_attempts'] = attempts
record['retry_at'] = retry_at
record['lease_until'] = nil
redis.call('SET', KEYS[1], cjson.encode(record))
redis.call('ZADD', KEYS[2], retry_at, ARGV[1])
return retry_at
"""

_ASSET_FINALIZE_DELETE_SCRIPT = """
local raw = redis.call('GET', KEYS[1])
if raw == false then
  redis.call('ZREM', KEYS[4], ARGV[1])
  return 1
end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table'
  or record['state'] ~= 'deleting'
  or record['delete_token'] ~= ARGV[2]
  or record['owner_hash'] ~= ARGV[3]
  or tonumber(record['size_bytes']) ~= tonumber(ARGV[4]) then
  return 0
end

local function quota_value(key, field)
  local raw_value = redis.call('HGET', key, field)
  if raw_value == false then
    return nil
  end
  local value = tonumber(raw_value)
  if value == nil or value < 0 or value ~= math.floor(value) then
    return nil
  end
  return value
end

local owner_count = quota_value(KEYS[2], 'count')
local owner_bytes = quota_value(KEYS[2], 'bytes')
local global_count = quota_value(KEYS[3], 'count')
local global_bytes = quota_value(KEYS[3], 'bytes')
local size_bytes = tonumber(ARGV[4])
if owner_count == nil or owner_bytes == nil or global_count == nil or global_bytes == nil
  or owner_count < 1 or global_count < 1
  or owner_bytes < size_bytes or global_bytes < size_bytes then
  return -2
end

redis.call('HINCRBY', KEYS[2], 'count', -1)
redis.call('HINCRBY', KEYS[2], 'bytes', -size_bytes)
redis.call('HINCRBY', KEYS[3], 'count', -1)
redis.call('HINCRBY', KEYS[3], 'bytes', -size_bytes)
if owner_count == 1 and owner_bytes == size_bytes then
  redis.call('DEL', KEYS[2])
end
if global_count == 1 and global_bytes == size_bytes then
  redis.call('DEL', KEYS[3])
end
record['state'] = 'removed'
redis.call('SET', KEYS[1], cjson.encode(record))
redis.call('DEL', KEYS[1])
redis.call('ZREM', KEYS[4], ARGV[1])
return 1
"""

_INGEST_STAGING_RESERVE_SCRIPT = """
local hash_count = redis.call('HLEN', KEYS[1])
local expiry_count = redis.call('ZCARD', KEYS[2])
local scan_limit = tonumber(ARGV[12])
if hash_count ~= expiry_count or hash_count > scan_limit then
  return {0, 6}
end

local reservation_ids = redis.call('ZRANGE', KEYS[2], 0, scan_limit - 1)
local owner_count = 0
local owner_bytes = 0
local global_count = 0
local global_bytes = 0
local existing_match = false
for _, reservation_id in ipairs(reservation_ids) do
  local raw = redis.call('HGET', KEYS[1], reservation_id)
  if raw == false then
    return {0, 6}
  end
  local ok, record = pcall(cjson.decode, raw)
  local reserved_bytes = ok and type(record) == 'table' and tonumber(record['reserved_bytes'])
  local lease_until = ok and type(record) == 'table' and tonumber(record['lease_until'])
  if not ok or type(record) ~= 'table'
    or record['state'] ~= 'active'
    or record['reservation_id'] ~= reservation_id
    or type(record['owner_hash']) ~= 'string'
    or type(record['token']) ~= 'string'
    or reserved_bytes == nil or reserved_bytes < 1 or reserved_bytes ~= math.floor(reserved_bytes)
    or lease_until == nil or lease_until ~= math.floor(lease_until) then
    return {0, 6}
  end
  if lease_until <= tonumber(ARGV[6]) then
    redis.call('HDEL', KEYS[1], reservation_id)
    redis.call('ZREM', KEYS[2], reservation_id)
  else
    global_count = global_count + 1
    global_bytes = global_bytes + reserved_bytes
    if record['owner_hash'] == ARGV[3] then
      owner_count = owner_count + 1
      owner_bytes = owner_bytes + reserved_bytes
    end
    if reservation_id == ARGV[1] then
      if record['owner_hash'] == ARGV[3]
        and record['token'] == ARGV[2]
        and reserved_bytes == tonumber(ARGV[4]) then
        existing_match = true
      else
        return {0, 4}
      end
    end
  end
end

if existing_match then
  return {1, 0}
end
if owner_count + 1 > tonumber(ARGV[7]) or owner_bytes + tonumber(ARGV[4]) > tonumber(ARGV[8]) then
  return {0, 1}
end
if global_count + 1 > tonumber(ARGV[9])
  or global_bytes + tonumber(ARGV[4]) > tonumber(ARGV[10]) then
  return {0, 2}
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[5])
redis.call('ZADD', KEYS[2], tonumber(ARGV[11]), ARGV[1])
return {1, 0}
"""

_INGEST_STAGING_REFRESH_SCRIPT = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if raw == false then
  return 0
end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table'
  or record['state'] ~= 'active'
  or record['reservation_id'] ~= ARGV[1]
  or record['token'] ~= ARGV[2]
  or record['owner_hash'] ~= ARGV[3]
  or tonumber(record['lease_until']) == nil
  or tonumber(record['lease_until']) <= tonumber(ARGV[4])
  or tonumber(ARGV[5]) <= tonumber(ARGV[4]) then
  return 0
end
record['lease_until'] = tonumber(ARGV[5])
redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(record))
redis.call('ZADD', KEYS[2], tonumber(ARGV[5]), ARGV[1])
return tonumber(ARGV[5])
"""

_INGEST_STAGING_RELEASE_SCRIPT = """
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if raw == false then
  redis.call('ZREM', KEYS[2], ARGV[1])
  return 1
end
local ok, record = pcall(cjson.decode, raw)
if not ok or type(record) ~= 'table' then
  return -2
end
if record['state'] ~= 'active'
  or record['reservation_id'] ~= ARGV[1]
  or record['token'] ~= ARGV[2]
  or record['owner_hash'] ~= ARGV[3] then
  return 0
end
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
"""

_JOB_ADMISSION_SCRIPT = """
local now_ms = tonumber(ARGV[2])
local expires_ms = tonumber(ARGV[3])
local window_start_ms = now_ms - 60000
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now_ms)
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now_ms)
redis.call('ZREMRANGEBYSCORE', KEYS[3], '-inf', window_start_ms)
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[4]) then
  return {0, 1}
end
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[5]) then
  return {0, 2}
end
if redis.call('ZCARD', KEYS[3]) >= tonumber(ARGV[6]) then
  return {0, 3}
end
redis.call('ZADD', KEYS[1], expires_ms, ARGV[1])
redis.call('ZADD', KEYS[2], expires_ms, ARGV[1])
redis.call('ZADD', KEYS[3], now_ms, ARGV[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[7]))
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[7]))
redis.call('EXPIRE', KEYS[3], 61)
return {1, 0}
"""

_JOB_ADMISSION_RELEASE_SCRIPT = """
local global_removed = redis.call('ZREM', KEYS[1], ARGV[1])
local owner_removed = redis.call('ZREM', KEYS[2], ARGV[1])
if redis.call('ZCARD', KEYS[2]) == 0 then
  redis.call('DEL', KEYS[2])
end
return global_removed + owner_removed
"""

_JOB_ADMISSION_REFRESH_SCRIPT = """
if redis.call('ZSCORE', KEYS[1], ARGV[1]) == false
  or redis.call('ZSCORE', KEYS[2], ARGV[1]) == false then
  return 0
end
redis.call('ZADD', KEYS[1], tonumber(ARGV[2]), ARGV[1])
redis.call('ZADD', KEYS[2], tonumber(ARGV[2]), ARGV[1])
local requested_ttl = tonumber(ARGV[3])
if redis.call('TTL', KEYS[1]) < requested_ttl then
  redis.call('EXPIRE', KEYS[1], requested_ttl)
end
if redis.call('TTL', KEYS[2]) < requested_ttl then
  redis.call('EXPIRE', KEYS[2], requested_ttl)
end
return 1
"""

_JOB_ADMISSION_ROLLBACK_SCRIPT = """
local global_removed = redis.call('ZREM', KEYS[1], ARGV[1])
local owner_removed = redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('ZREM', KEYS[3], ARGV[1])
if redis.call('ZCARD', KEYS[2]) == 0 then
  redis.call('DEL', KEYS[2])
end
if redis.call('ZCARD', KEYS[3]) == 0 then
  redis.call('DEL', KEYS[3])
end
return global_removed + owner_removed
"""

_BRAND_KIT_SAVE_SCRIPT = """
if redis.call('SISMEMBER', KEYS[2], ARGV[1]) == 0
  and redis.call('SCARD', KEYS[2]) >= tonumber(ARGV[3]) then
  return 0
end
redis.call('SET', KEYS[1], ARGV[2])
redis.call('SADD', KEYS[2], ARGV[1])
return 1
"""

_BRAND_KIT_DELETE_SCRIPT = """
local deleted = redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[1])
return deleted
"""


_redis_client: Any | None = None
_rq_redis_client: Any | None = None


class JobAdmissionError(RuntimeError):
    """A stable, non-secret job admission rejection."""


class IngestStagingAdmissionError(RuntimeError):
    """A stable, non-secret remote-ingest staging admission rejection."""


@dataclass(frozen=True, slots=True)
class IngestStagingReservation:
    """A token-fenced lease for one conservatively sized staging writer."""

    reservation_id: str
    token: str
    owner_hash: str
    reserved_bytes: int
    lease_until: int


class AssetQuotaError(RuntimeError):
    """A tenant or service-wide managed-storage quota was reached."""


class AssetStateError(RuntimeError):
    """Managed asset state is unavailable, corrupt, or conflicting."""


class BrandKitLimitError(RuntimeError):
    """The tenant has reached its configured brand-kit count."""


class BrandKitStorageError(RuntimeError):
    """Brand-kit persistence is unavailable."""


def get_redis() -> Any:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=settings.redis_connect_timeout_seconds,
            socket_timeout=settings.redis_socket_timeout_seconds,
            retry_on_timeout=False,
        )
    return _redis_client


def get_rq_redis() -> Any:
    """Return a binary-safe Redis client for RQ's serialized job records."""

    global _rq_redis_client
    if _rq_redis_client is None:
        _rq_redis_client = redis.Redis.from_url(
            settings.redis_url,
            decode_responses=False,
            socket_connect_timeout=settings.redis_connect_timeout_seconds,
            socket_timeout=settings.redis_socket_timeout_seconds,
            retry_on_timeout=False,
        )
    return _rq_redis_client


def _now_ts() -> int:
    return int(time.time())


def _owner_for_write(record: dict[str, Any]) -> str:
    current = current_owner_hash()
    stored = record.get("owner_hash")
    if current is not None:
        if isinstance(stored, str) and not hmac.compare_digest(stored, current):
            raise PermissionError("cross-tenant write denied")
        return current
    if isinstance(stored, str) and _OWNER_HASH_PATTERN.fullmatch(stored):
        return stored
    raise RuntimeError("tenant context is required for persistence")


def _stored_record(record: dict[str, Any]) -> dict[str, Any]:
    result = dict(record)
    result["owner_hash"] = _owner_for_write(result)
    return result


def _assert_existing_owner(client: Any, key: str, owner_hash: str) -> None:
    raw = client.get(key)
    if not raw:
        return
    try:
        existing = json.loads(raw)
    except (TypeError, ValueError):
        raise PermissionError("existing record is not tenant-safe") from None
    stored_owner = existing.get("owner_hash") if isinstance(existing, dict) else None
    if not isinstance(stored_owner, str) or not hmac.compare_digest(stored_owner, owner_hash):
        raise PermissionError("cross-tenant overwrite denied")


def _visible_record(raw: str | bytes | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        record = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(record, dict):
        return None
    current = current_owner_hash()
    stored = record.get("owner_hash")
    if current is not None and (
        not isinstance(stored, str) or not hmac.compare_digest(stored, current)
    ):
        return None
    record.pop("owner_hash", None)
    return record


def _decoded_record(raw: str | bytes | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        record = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return record if isinstance(record, dict) else None


def _visible_asset_record(
    raw: str | bytes | None,
    *,
    allow_signed_download: bool = False,
) -> dict[str, Any] | None:
    record = _decoded_record(raw)
    if record is None or record.get("state") != "active":
        return None
    try:
        if int(record.get("expires_at", 0)) <= _now_ts():
            return None
    except (TypeError, ValueError):
        return None
    current = current_owner_hash()
    if current is None and not allow_signed_download:
        return None
    stored = record.get("owner_hash")
    if not isinstance(stored, str) or (
        current is not None and not hmac.compare_digest(stored, current)
    ):
        return None
    for field in (
        "owner_hash",
        "state",
        "reservation_token",
        "delete_token",
        "lease_until",
        "retry_at",
        "delete_attempts",
    ):
        record.pop(field, None)
    return record


def _tenant_brand_kit_key(owner_hash: str, brand_kit_id: str) -> str:
    return f"{BRAND_KIT_PREFIX}{owner_hash}:{brand_kit_id}"


def _tenant_brand_kit_set(owner_hash: str) -> str:
    return f"{BRAND_KIT_PREFIX}{owner_hash}:all"


def _asset_quota_owner_key(owner_hash: str) -> str:
    return f"{ASSET_QUOTA_OWNER_PREFIX}{owner_hash}"


def _job_admission_owner_key(owner_hash: str) -> str:
    return f"{JOB_ADMISSION_OWNER_PREFIX}{owner_hash}"


def _job_admission_rate_key(owner_hash: str) -> str:
    return f"{JOB_ADMISSION_RATE_PREFIX}{owner_hash}"


def _validated_owner_hash(owner_hash: str | None = None) -> str:
    owner = owner_hash or require_owner_hash()
    if not _OWNER_HASH_PATTERN.fullmatch(owner):
        raise RuntimeError("tenant context is required")
    return owner


def _validated_asset_id(asset_id: Any) -> str:
    if not isinstance(asset_id, str) or not re.fullmatch(r"[a-f0-9]{32}", asset_id):
        raise AssetStateError("asset id is invalid")
    return asset_id


def reserve_ingest_staging(
    *,
    owner_hash: str | None = None,
) -> IngestStagingReservation:
    """Reserve bounded owner/global staging capacity before remote I/O begins."""

    owner = _validated_owner_hash(owner_hash)
    reservation_id = uuid.uuid4().hex
    token = uuid.uuid4().hex
    reserved_bytes = settings.max_ingest_bytes
    now = _now_ts()
    lease_until = now + settings.ingest_staging_lease_seconds
    record = {
        "state": "active",
        "reservation_id": reservation_id,
        "token": token,
        "owner_hash": owner,
        "reserved_bytes": reserved_bytes,
        "created_at": now,
        "lease_until": lease_until,
    }
    serialized = json.dumps(record, ensure_ascii=True, separators=(",", ":"))
    result = None
    for _attempt in range(2):
        try:
            result = get_redis().eval(
                _INGEST_STAGING_RESERVE_SCRIPT,
                2,
                INGEST_STAGING_RESERVATIONS_KEY,
                INGEST_STAGING_EXPIRY_KEY,
                reservation_id,
                token,
                owner,
                reserved_bytes,
                serialized,
                now,
                settings.ingest_staging_owner_max_active,
                settings.ingest_staging_owner_max_bytes,
                settings.ingest_staging_global_max_active,
                settings.ingest_staging_global_max_bytes,
                lease_until,
                _INGEST_STAGING_SCAN_LIMIT,
            )
            break
        except Exception:
            continue
    if result is None:
        raise IngestStagingAdmissionError("remote-ingest staging admission unavailable") from None
    try:
        accepted, code = int(result[0]), int(result[1])
    except (IndexError, TypeError, ValueError):
        raise IngestStagingAdmissionError("remote-ingest staging admission unavailable") from None
    if accepted:
        return IngestStagingReservation(
            reservation_id=reservation_id,
            token=token,
            owner_hash=owner,
            reserved_bytes=reserved_bytes,
            lease_until=lease_until,
        )
    messages = {
        1: "tenant remote-ingest staging limit reached",
        2: "service remote-ingest staging limit reached",
    }
    raise IngestStagingAdmissionError(
        messages.get(code, "remote-ingest staging admission unavailable")
    )


def refresh_ingest_staging(reservation: IngestStagingReservation) -> int | None:
    """Extend a still-live staging lease without changing its original charge."""

    owner = _validated_owner_hash(reservation.owner_hash)
    now = _now_ts()
    lease_until = now + settings.ingest_staging_lease_seconds
    for _attempt in range(2):
        try:
            refreshed = int(
                get_redis().eval(
                    _INGEST_STAGING_REFRESH_SCRIPT,
                    2,
                    INGEST_STAGING_RESERVATIONS_KEY,
                    INGEST_STAGING_EXPIRY_KEY,
                    reservation.reservation_id,
                    reservation.token,
                    owner,
                    now,
                    lease_until,
                )
            )
        except Exception:
            continue
        return refreshed if refreshed > now else None
    return None


def release_ingest_staging(reservation: IngestStagingReservation) -> bool:
    """Idempotently release one staging lease; stale tokens cannot release it."""

    owner = _validated_owner_hash(reservation.owner_hash)
    for _attempt in range(2):
        try:
            released = int(
                get_redis().eval(
                    _INGEST_STAGING_RELEASE_SCRIPT,
                    2,
                    INGEST_STAGING_RESERVATIONS_KEY,
                    INGEST_STAGING_EXPIRY_KEY,
                    reservation.reservation_id,
                    reservation.token,
                    owner,
                )
            )
        except Exception:
            continue
        return released == 1
    return False


def reserve_job_admission(job_id: str, *, owner_hash: str | None = None) -> None:
    """Atomically reserve owner/global capacity and one owner enqueue-rate slot."""

    owner = _validated_owner_hash(owner_hash)
    now_ms = int(time.time() * 1_000)
    reservation_ttl = settings.job_admission_reservation_ttl_seconds()
    expires_ms = now_ms + reservation_ttl * 1_000
    try:
        result = get_redis().eval(
            _JOB_ADMISSION_SCRIPT,
            3,
            JOB_ADMISSION_GLOBAL_KEY,
            _job_admission_owner_key(owner),
            _job_admission_rate_key(owner),
            job_id,
            now_ms,
            expires_ms,
            settings.job_admission_owner_max_active,
            settings.job_admission_global_max_active,
            settings.job_admission_owner_rpm,
            reservation_ttl,
        )
        accepted, code = (int(result[0]), int(result[1]))
    except Exception:
        raise JobAdmissionError("job queue temporarily unavailable") from None
    if accepted:
        return
    messages = {
        1: "tenant active job limit reached",
        2: "service active job limit reached",
        3: "tenant enqueue rate limit reached",
    }
    raise JobAdmissionError(messages.get(code, "job queue temporarily unavailable"))


def release_job_admission(job_id: str, *, owner_hash: str | None = None) -> bool:
    """Release active capacity; a Redis outage remains bounded by reservation TTL."""

    owner = _validated_owner_hash(owner_hash)
    try:
        removed = get_redis().eval(
            _JOB_ADMISSION_RELEASE_SCRIPT,
            2,
            JOB_ADMISSION_GLOBAL_KEY,
            _job_admission_owner_key(owner),
            job_id,
        )
    except Exception:
        return False
    return bool(removed)


def refresh_job_admission(
    job_id: str,
    execution_ttl_seconds: int,
    *,
    owner_hash: str | None = None,
) -> None:
    """Shorten a dequeued job's crash lease to its bounded execution window."""

    owner = _validated_owner_hash(owner_hash)
    if (
        isinstance(execution_ttl_seconds, bool)
        or execution_ttl_seconds < 120
        or execution_ttl_seconds > settings.job_admission_execution_buffer_seconds
    ):
        raise JobAdmissionError("job queue temporarily unavailable")
    now_ms = int(time.time() * 1_000)
    expires_ms = now_ms + execution_ttl_seconds * 1_000
    try:
        refreshed = get_redis().eval(
            _JOB_ADMISSION_REFRESH_SCRIPT,
            2,
            JOB_ADMISSION_GLOBAL_KEY,
            _job_admission_owner_key(owner),
            job_id,
            expires_ms,
            execution_ttl_seconds,
        )
    except Exception:
        raise JobAdmissionError("job queue temporarily unavailable") from None
    if not refreshed:
        raise JobAdmissionError("job admission reservation unavailable")


def rollback_job_admission(job_id: str, *, owner_hash: str | None = None) -> bool:
    """Roll back active and rate reservations after persistence/enqueue failure."""

    owner = _validated_owner_hash(owner_hash)
    try:
        removed = get_redis().eval(
            _JOB_ADMISSION_ROLLBACK_SCRIPT,
            3,
            JOB_ADMISSION_GLOBAL_KEY,
            _job_admission_owner_key(owner),
            _job_admission_rate_key(owner),
            job_id,
        )
    except Exception:
        return False
    return bool(removed)


def reserve_asset(asset: dict[str, Any], reservation_token: str) -> str:
    """Reserve durable owner/global quota before any object write begins."""

    stored = _stored_record(asset)
    owner = stored["owner_hash"]
    asset_id = stored.get("asset_id")
    storage_key = stored.get("storage_key")
    asset_id = _validated_asset_id(asset_id)
    if not isinstance(storage_key, str) or not storage_key:
        raise AssetStateError("asset storage key is invalid")
    if not isinstance(reservation_token, str) or not re.fullmatch(
        r"[a-f0-9]{32}", reservation_token
    ):
        raise AssetStateError("asset reservation token is invalid")
    size_bytes = stored.get("size_bytes")
    expires_at = stored.get("expires_at")
    if isinstance(size_bytes, bool) or not isinstance(size_bytes, int) or size_bytes < 0:
        raise AssetStateError("asset size is invalid")
    if isinstance(expires_at, bool) or not isinstance(expires_at, int) or expires_at <= _now_ts():
        raise AssetStateError("asset expiry is invalid")
    lease_until = _now_ts() + settings.asset_reservation_lease_seconds
    control = {
        **stored,
        "state": "reserved",
        "reservation_token": reservation_token,
        "lease_until": lease_until,
        "delete_attempts": 0,
    }
    serialized = json.dumps(control, ensure_ascii=True, separators=(",", ":"))
    result = None
    for _attempt in range(2):
        try:
            result = get_redis().eval(
                _ASSET_RESERVE_SCRIPT,
                4,
                f"{ASSET_PREFIX}{asset_id}",
                _asset_quota_owner_key(owner),
                ASSET_QUOTA_GLOBAL_KEY,
                ASSET_EXPIRY_SET,
                serialized,
                reservation_token,
                owner,
                size_bytes,
                settings.asset_quota_owner_max_count,
                settings.asset_quota_owner_max_bytes,
                settings.asset_quota_global_max_count,
                settings.asset_quota_global_max_bytes,
                lease_until,
                asset_id,
                storage_key,
                expires_at,
            )
            break
        except Exception:
            continue
    if result is None:
        raise AssetStateError("asset storage state is unavailable") from None
    accepted, code = int(result[0]), int(result[1])
    if accepted:
        return owner
    if code == 1:
        raise AssetQuotaError("tenant managed-storage quota reached")
    if code == 2:
        raise AssetQuotaError("service managed-storage quota reached")
    if code == 4:
        raise AssetStateError("asset id already exists")
    raise AssetStateError("asset storage state is unavailable")


def refresh_asset_reservation(
    asset_id: str,
    reservation_token: str,
    *,
    owner_hash: str | None = None,
) -> bool:
    """Extend a live put reservation without changing its charged quota."""

    asset_id = _validated_asset_id(asset_id)
    owner = _validated_owner_hash(owner_hash)
    lease_until = _now_ts() + settings.asset_reservation_lease_seconds
    for _attempt in range(2):
        try:
            refreshed = get_redis().eval(
                _ASSET_REFRESH_RESERVATION_SCRIPT,
                2,
                f"{ASSET_PREFIX}{asset_id}",
                ASSET_EXPIRY_SET,
                asset_id,
                reservation_token,
                lease_until,
                owner,
            )
        except Exception:
            continue
        return bool(refreshed)
    return False


def commit_asset(asset: dict[str, Any], reservation_token: str) -> None:
    """Idempotently publish a reserved object as active and readable."""

    stored = _stored_record(asset)
    asset_id = _validated_asset_id(stored["asset_id"])
    expires_at = stored.get("expires_at")
    if isinstance(expires_at, bool) or not isinstance(expires_at, int):
        raise AssetStateError("asset expiry is invalid")
    control = {
        **stored,
        "state": "active",
        "reservation_token": reservation_token,
        "delete_attempts": 0,
    }
    control.pop("lease_until", None)
    serialized = json.dumps(control, ensure_ascii=True, separators=(",", ":"))
    committed = None
    for _attempt in range(2):
        try:
            committed = int(
                get_redis().eval(
                    _ASSET_COMMIT_SCRIPT,
                    2,
                    f"{ASSET_PREFIX}{asset_id}",
                    ASSET_EXPIRY_SET,
                    asset_id,
                    reservation_token,
                    serialized,
                    expires_at,
                )
            )
            break
        except Exception:
            continue
    if committed is None:
        raise AssetStateError("asset storage state is unavailable") from None
    if committed != 1:
        raise AssetStateError("asset reservation is unavailable")


def get_asset_control(asset_id: str) -> dict[str, Any] | None:
    """Return the internal durable record for cleanup and recovery only."""

    asset_id = _validated_asset_id(asset_id)
    return _decoded_record(get_redis().get(f"{ASSET_PREFIX}{asset_id}"))


def get_asset(asset_id: str) -> dict[str, Any] | None:
    try:
        asset_id = _validated_asset_id(asset_id)
    except AssetStateError:
        return None
    return _visible_asset_record(get_redis().get(f"{ASSET_PREFIX}{asset_id}"))


def get_signed_download_asset(asset_id: str) -> dict[str, Any] | None:
    """Resolve an active asset only after the caller verifies its signed URL."""

    try:
        asset_id = _validated_asset_id(asset_id)
    except AssetStateError:
        return None
    return _visible_asset_record(
        get_redis().get(f"{ASSET_PREFIX}{asset_id}"),
        allow_signed_download=True,
    )


def update_asset(asset_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
    try:
        asset_id = _validated_asset_id(asset_id)
    except AssetStateError:
        return None
    owner = require_owner_hash()
    control = get_asset_control(asset_id)
    if control is None or _visible_asset_record(json.dumps(control)) is None:
        return None
    if not hmac.compare_digest(str(control.get("owner_hash", "")), owner):
        return None
    protected = {
        "asset_id",
        "owner_hash",
        "state",
        "reservation_token",
        "delete_token",
        "lease_until",
        "retry_at",
        "delete_attempts",
        "size_bytes",
        "storage_key",
        "storage_uri",
        "expires_at",
    }
    if protected.intersection(updates):
        raise AssetStateError("asset lifecycle fields cannot be updated")
    updated = {**control, **updates}
    serialized = json.dumps(updated, ensure_ascii=True, separators=(",", ":"))
    try:
        saved = int(
            get_redis().eval(
                _ASSET_UPDATE_SCRIPT,
                1,
                f"{ASSET_PREFIX}{asset_id}",
                asset_id,
                owner,
                _now_ts(),
                serialized,
            )
        )
    except Exception:
        raise AssetStateError("asset storage state is unavailable") from None
    if saved != 1:
        return None
    return _visible_asset_record(serialized)


def _claimed_asset(raw: str | bytes | None) -> dict[str, Any] | None:
    record = _decoded_record(raw)
    if record is None or record.get("state") != "deleting":
        return None
    return record


def abort_asset_reservation(
    asset_id: str,
    reservation_token: str,
    delete_token: str,
    *,
    owner_hash: str | None = None,
) -> dict[str, Any] | None:
    """Token-safely claim a completed failed put for immediate cleanup."""

    owner = _validated_owner_hash(owner_hash)
    asset_id = _validated_asset_id(asset_id)
    if not re.fullmatch(r"[a-f0-9]{32}", delete_token):
        raise AssetStateError("asset deletion token is invalid")
    lease_until = _now_ts() + settings.asset_delete_lease_seconds
    for _attempt in range(2):
        try:
            raw = get_redis().eval(
                _ASSET_ABORT_RESERVATION_SCRIPT,
                2,
                f"{ASSET_PREFIX}{asset_id}",
                ASSET_EXPIRY_SET,
                asset_id,
                reservation_token,
                lease_until,
                delete_token,
                owner,
            )
        except Exception:
            continue
        return _claimed_asset(raw)
    return None


def claim_asset_deletion(
    asset_id: str,
    delete_token: str,
    *,
    force: bool = False,
    owner_hash: str | None = None,
) -> dict[str, Any] | None:
    """Claim one due asset with a unique lease token for idempotent deletion."""

    asset_id = _validated_asset_id(asset_id)
    owner = _validated_owner_hash(owner_hash) if force else ""
    if not re.fullmatch(r"[a-f0-9]{32}", delete_token):
        raise AssetStateError("asset deletion token is invalid")
    now = _now_ts()
    lease_until = now + settings.asset_delete_lease_seconds
    for _attempt in range(2):
        try:
            raw = get_redis().eval(
                _ASSET_CLAIM_DELETE_SCRIPT,
                2,
                f"{ASSET_PREFIX}{asset_id}",
                ASSET_EXPIRY_SET,
                asset_id,
                now,
                lease_until,
                delete_token,
                "1" if force else "0",
                owner,
            )
        except Exception:
            continue
        return _claimed_asset(raw)
    return None


def schedule_asset_delete_retry(
    asset_id: str,
    delete_token: str,
) -> int | None:
    asset_id = _validated_asset_id(asset_id)
    for _attempt in range(2):
        try:
            retry_at = int(
                get_redis().eval(
                    _ASSET_DELETE_RETRY_SCRIPT,
                    2,
                    f"{ASSET_PREFIX}{asset_id}",
                    ASSET_EXPIRY_SET,
                    asset_id,
                    delete_token,
                    _now_ts(),
                    settings.asset_delete_retry_base_seconds,
                    settings.asset_delete_retry_max_seconds,
                )
            )
        except Exception:
            continue
        return retry_at if retry_at > 0 else None
    return None


def finalize_asset_deletion(asset: dict[str, Any]) -> bool:
    asset_id = asset.get("asset_id")
    owner = asset.get("owner_hash")
    delete_token = asset.get("delete_token")
    size_bytes = asset.get("size_bytes")
    if (
        not isinstance(asset_id, str)
        or not re.fullmatch(r"[a-f0-9]{32}", asset_id)
        or not isinstance(owner, str)
        or not _OWNER_HASH_PATTERN.fullmatch(owner)
        or not isinstance(delete_token, str)
        or isinstance(size_bytes, bool)
        or not isinstance(size_bytes, int)
        or size_bytes < 0
    ):
        return False
    for _attempt in range(2):
        try:
            finalized = int(
                get_redis().eval(
                    _ASSET_FINALIZE_DELETE_SCRIPT,
                    4,
                    f"{ASSET_PREFIX}{asset_id}",
                    _asset_quota_owner_key(owner),
                    ASSET_QUOTA_GLOBAL_KEY,
                    ASSET_EXPIRY_SET,
                    asset_id,
                    delete_token,
                    owner,
                    size_bytes,
                )
            )
        except Exception:
            continue
        return finalized == 1
    return False


def save_job(job: dict[str, Any], ttl_seconds: int) -> None:
    client = get_redis()
    job_id = job["job_id"]
    key = f"{JOB_PREFIX}{job_id}"
    ttl_grace = ttl_seconds + settings.cleanup_interval_seconds
    stored = _stored_record(job)
    _assert_existing_owner(client, key, stored["owner_hash"])
    client.set(key, json.dumps(stored, ensure_ascii=True), ex=ttl_grace)
    expires_at = stored.get("expires_at")
    if expires_at:
        try:
            expires_ts = int(expires_at)
        except ValueError:
            expires_ts = _now_ts() + ttl_seconds
        client.zadd(JOB_EXPIRY_SET, {job_id: expires_ts})


def get_job(job_id: str) -> dict[str, Any] | None:
    client = get_redis()
    raw = client.get(f"{JOB_PREFIX}{job_id}")
    return _visible_record(raw)


def update_job(job_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
    job = get_job(job_id)
    if job is None:
        return None
    job.update(updates)
    ttl = settings.job_ttl_seconds()
    save_job(job, ttl)
    return job


def delete_job(job_id: str) -> None:
    client = get_redis()
    if current_owner_hash() is not None and get_job(job_id) is None:
        return
    client.delete(f"{JOB_PREFIX}{job_id}")
    client.zrem(JOB_EXPIRY_SET, job_id)


def build_cache_key(
    namespace: str,
    payload: dict[str, Any],
    *,
    owner_hash: str | None = None,
) -> str:
    owner = owner_hash or require_owner_hash()
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{CACHE_PREFIX}{owner}:{namespace}:{digest}"


def get_cached_result(cache_key: str) -> dict[str, Any] | None:
    client = get_redis()
    raw = client.get(cache_key)
    return _visible_record(raw)


def set_cached_result(cache_key: str, payload: dict[str, Any], ttl_seconds: int) -> None:
    client = get_redis()
    owner = require_owner_hash()
    if not cache_key.startswith(f"{CACHE_PREFIX}{owner}:"):
        raise PermissionError("cross-tenant cache write denied")
    ttl_grace = ttl_seconds + settings.cleanup_interval_seconds
    stored = _stored_record(payload)
    _assert_existing_owner(client, cache_key, owner)
    client.set(cache_key, json.dumps(stored, ensure_ascii=True), ex=ttl_grace)


def delete_cached_result(cache_key: str) -> None:
    client = get_redis()
    owner = current_owner_hash()
    if owner is not None:
        expected_prefix = f"{CACHE_PREFIX}{owner}:"
        if (
            not cache_key.startswith(expected_prefix)
            or _visible_record(client.get(cache_key)) is None
        ):
            return
    client.delete(cache_key)


def save_brand_kit(brand_kit: dict[str, Any]) -> None:
    client = get_redis()
    owner = require_owner_hash()
    validate_brand_kit_record(brand_kit)
    brand_kit_id = validate_brand_kit_id(brand_kit["brand_kit_id"])
    key = _tenant_brand_kit_key(owner, brand_kit_id)
    stored = _stored_record(brand_kit)
    serialized = json.dumps(stored, ensure_ascii=True, separators=(",", ":"))
    if len(serialized.encode("utf-8")) > settings.brand_kit_max_serialized_bytes:
        raise ValueError("brand kit payload exceeds configured limit")
    try:
        saved = client.eval(
            _BRAND_KIT_SAVE_SCRIPT,
            2,
            key,
            _tenant_brand_kit_set(owner),
            brand_kit_id,
            serialized,
            settings.brand_kit_max_count,
        )
    except Exception:
        raise BrandKitStorageError("brand kit persistence unavailable") from None
    if not saved:
        raise BrandKitLimitError("brand kit limit reached")


def get_brand_kit(brand_kit_id: str) -> dict[str, Any] | None:
    client = get_redis()
    owner = require_owner_hash()
    brand_kit_id = validate_brand_kit_id(brand_kit_id)
    raw = client.get(_tenant_brand_kit_key(owner, brand_kit_id))
    return _visible_record(raw)


def list_brand_kits() -> list[str]:
    client = get_redis()
    owner = require_owner_hash()
    members = sorted(str(value) for value in client.smembers(_tenant_brand_kit_set(owner)))
    return members[: settings.brand_kit_max_count]


def delete_brand_kit(brand_kit_id: str) -> None:
    client = get_redis()
    owner = require_owner_hash()
    brand_kit_id = validate_brand_kit_id(brand_kit_id)
    key = _tenant_brand_kit_key(owner, brand_kit_id)
    try:
        client.eval(
            _BRAND_KIT_DELETE_SCRIPT,
            2,
            key,
            _tenant_brand_kit_set(owner),
            brand_kit_id,
        )
    except Exception:
        raise BrandKitStorageError("brand kit persistence unavailable") from None


def list_expired_assets(now_ts: int | None = None) -> list[str]:
    client = get_redis()
    now_ts = _now_ts() if now_ts is None else now_ts
    members = client.zrangebyscore(
        ASSET_EXPIRY_SET,
        0,
        now_ts,
        start=0,
        num=settings.asset_quota_global_max_count,
    )
    return [member for member in members if re.fullmatch(r"[a-f0-9]{32}", member)]


def list_expired_jobs(now_ts: int | None = None) -> list[str]:
    client = get_redis()
    now_ts = now_ts or _now_ts()
    return list(client.zrangebyscore(JOB_EXPIRY_SET, 0, now_ts))
