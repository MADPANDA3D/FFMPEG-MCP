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


def collect_metrics_snapshot() -> dict[str, Any]:
    client = get_redis()
    cache_hits: dict[str, float] = {}
    cache_misses: dict[str, float] = {}
    job_counts: dict[tuple[str, str], float] = {}
    job_durations: dict[tuple[str, str], float] = {}

    for key in client.scan_iter(match=f"{METRIC_PREFIX}*"):
        name = key[len(METRIC_PREFIX):]
        raw = client.get(key)
        if raw is None:
            continue
        try:
            value = float(raw) if "." in str(raw) else int(raw)
        except (TypeError, ValueError):
            continue
        if name.startswith("cache_hit:"):
            job_type = name.split(":", 1)[1]
            cache_hits[job_type] = value
        elif name.startswith("cache_miss:"):
            job_type = name.split(":", 1)[1]
            cache_misses[job_type] = value
        elif name.startswith("job_count:"):
            _, job_type, status = name.split(":", 2)
            job_counts[(job_type, status)] = value
        elif name.startswith("job_duration_ms:"):
            _, job_type, status = name.split(":", 2)
            job_durations[(job_type, status)] = value

    avg_runtime_ms: dict[str, int] = {}
    for (job_type, status), duration in job_durations.items():
        if status != "success":
            continue
        count = job_counts.get((job_type, status), 0) or 0
        if count:
            avg_runtime_ms[job_type] = int(duration / count)

    cache_hit_rate: dict[str, float] = {}
    for job_type in set(cache_hits) | set(cache_misses):
        total = cache_hits.get(job_type, 0) + cache_misses.get(job_type, 0)
        if total:
            cache_hit_rate[job_type] = round(cache_hits.get(job_type, 0) / total, 4)

    job_count_flat: dict[str, float] = {
        f"{job_type}:{status}": count for (job_type, status), count in job_counts.items()
    }

    return {
        "avg_runtime_ms": avg_runtime_ms,
        "cache_hit_rate": cache_hit_rate,
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "job_counts": job_count_flat,
    }
