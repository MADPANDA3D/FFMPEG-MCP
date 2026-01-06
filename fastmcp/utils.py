import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import urlparse


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_now_ts() -> int:
    return int(time.time())


def sanitize_filename(name: str, fallback: str = "media") -> str:
    if not name:
        return fallback
    base = os.path.basename(name)
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "_", base).strip("._")
    return cleaned or fallback


def get_hostname(url: str) -> str:
    return urlparse(url).hostname or ""
