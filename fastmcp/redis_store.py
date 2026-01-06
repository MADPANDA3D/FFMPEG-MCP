import hashlib
import json
import time
from typing import Any

import redis

from config import settings


ASSET_PREFIX = "asset:"
JOB_PREFIX = "job:"
ASSET_EXPIRY_SET = "asset:expiry"
JOB_EXPIRY_SET = "job:expiry"
CACHE_PREFIX = "cache:result:"
BRAND_KIT_PREFIX = "brandkit:"
BRAND_KIT_SET = "brandkit:all"


_redis_client: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
    return _redis_client


def _now_ts() -> int:
    return int(time.time())


def save_asset(asset: dict[str, Any], ttl_seconds: int) -> None:
    client = get_redis()
    asset_id = asset["asset_id"]
    key = f"{ASSET_PREFIX}{asset_id}"
    ttl_grace = ttl_seconds + settings.cleanup_interval_seconds
    client.set(key, json.dumps(asset, ensure_ascii=True), ex=ttl_grace)
    expires_at = asset.get("expires_at")
    if expires_at:
        try:
            expires_ts = int(expires_at)
        except ValueError:
            expires_ts = _now_ts() + ttl_seconds
        client.zadd(ASSET_EXPIRY_SET, {asset_id: expires_ts})


def get_asset(asset_id: str) -> dict[str, Any] | None:
    client = get_redis()
    raw = client.get(f"{ASSET_PREFIX}{asset_id}")
    if not raw:
        return None
    return json.loads(raw)


def update_asset(asset_id: str, updates: dict[str, Any]) -> dict[str, Any] | None:
    asset = get_asset(asset_id)
    if asset is None:
        return None
    asset.update(updates)
    ttl = settings.asset_ttl_seconds()
    save_asset(asset, ttl)
    return asset


def delete_asset(asset_id: str) -> None:
    client = get_redis()
    client.delete(f"{ASSET_PREFIX}{asset_id}")
    client.zrem(ASSET_EXPIRY_SET, asset_id)


def save_job(job: dict[str, Any], ttl_seconds: int) -> None:
    client = get_redis()
    job_id = job["job_id"]
    key = f"{JOB_PREFIX}{job_id}"
    ttl_grace = ttl_seconds + settings.cleanup_interval_seconds
    client.set(key, json.dumps(job, ensure_ascii=True), ex=ttl_grace)
    expires_at = job.get("expires_at")
    if expires_at:
        try:
            expires_ts = int(expires_at)
        except ValueError:
            expires_ts = _now_ts() + ttl_seconds
        client.zadd(JOB_EXPIRY_SET, {job_id: expires_ts})


def get_job(job_id: str) -> dict[str, Any] | None:
    client = get_redis()
    raw = client.get(f"{JOB_PREFIX}{job_id}")
    if not raw:
        return None
    return json.loads(raw)


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
    client.delete(f"{JOB_PREFIX}{job_id}")
    client.zrem(JOB_EXPIRY_SET, job_id)


def build_cache_key(namespace: str, payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"{CACHE_PREFIX}{namespace}:{digest}"


def get_cached_result(cache_key: str) -> dict[str, Any] | None:
    client = get_redis()
    raw = client.get(cache_key)
    if not raw:
        return None
    return json.loads(raw)


def set_cached_result(cache_key: str, payload: dict[str, Any], ttl_seconds: int) -> None:
    client = get_redis()
    ttl_grace = ttl_seconds + settings.cleanup_interval_seconds
    client.set(cache_key, json.dumps(payload, ensure_ascii=True), ex=ttl_grace)


def delete_cached_result(cache_key: str) -> None:
    client = get_redis()
    client.delete(cache_key)


def save_brand_kit(brand_kit: dict[str, Any]) -> None:
    client = get_redis()
    brand_kit_id = brand_kit["brand_kit_id"]
    key = f"{BRAND_KIT_PREFIX}{brand_kit_id}"
    client.set(key, json.dumps(brand_kit, ensure_ascii=True))
    client.sadd(BRAND_KIT_SET, brand_kit_id)


def get_brand_kit(brand_kit_id: str) -> dict[str, Any] | None:
    client = get_redis()
    raw = client.get(f"{BRAND_KIT_PREFIX}{brand_kit_id}")
    if not raw:
        return None
    return json.loads(raw)


def list_brand_kits() -> list[str]:
    client = get_redis()
    return list(client.smembers(BRAND_KIT_SET))


def delete_brand_kit(brand_kit_id: str) -> None:
    client = get_redis()
    client.delete(f"{BRAND_KIT_PREFIX}{brand_kit_id}")
    client.srem(BRAND_KIT_SET, brand_kit_id)


def list_expired_assets(now_ts: int | None = None) -> list[str]:
    client = get_redis()
    now_ts = now_ts or _now_ts()
    return list(client.zrangebyscore(ASSET_EXPIRY_SET, 0, now_ts))


def list_expired_jobs(now_ts: int | None = None) -> list[str]:
    client = get_redis()
    now_ts = now_ts or _now_ts()
    return list(client.zrangebyscore(JOB_EXPIRY_SET, 0, now_ts))
