import json
import logging
import time
from typing import Any

from config import settings
from redis_store import get_redis
from utils import utc_now_iso


logger = logging.getLogger("ffmpeg_mcp.metrics")

METRIC_PREFIX = "metrics:"


def _metric_key(name: str) -> str:
    return f"{METRIC_PREFIX}{name}"


def _safe_incr(key: str, amount: int | float) -> None:
    try:
        client = get_redis()
        if isinstance(amount, float):
            client.incrbyfloat(key, amount)
        else:
            client.incrby(key, amount)
    except Exception:
        return


def record_cache_hit(job_type: str) -> None:
    _safe_incr(_metric_key(f"cache_hit:{job_type}"), 1)


def record_cache_miss(job_type: str) -> None:
    _safe_incr(_metric_key(f"cache_miss:{job_type}"), 1)


def record_job_duration(job_type: str, duration_ms: int, status: str) -> None:
    _safe_incr(_metric_key(f"job_count:{job_type}:{status}"), 1)
    _safe_incr(_metric_key(f"job_duration_ms:{job_type}:{status}"), duration_ms)


def log_event(event: str, payload: dict[str, Any]) -> None:
    if not settings.log_structured:
        return
    record = {"event": event, "ts": utc_now_iso(), **payload}
    try:
        logger.info(json.dumps(record, ensure_ascii=True, separators=(",", ":")))
    except Exception:
        return


def job_timer() -> float:
    return time.monotonic()
