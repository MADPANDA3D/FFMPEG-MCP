"""Request and worker tenant identity without retaining raw principals."""

from __future__ import annotations

import contextvars
import hashlib
import hmac
import re
from collections.abc import Iterator
from contextlib import contextmanager

_OWNER_HASH: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "ffmpeg_owner_hash", default=None
)
_OWNER_HASH_PATTERN = re.compile(r"^[a-f0-9]{64}$")


def hash_principal(value: str, secret: str, *, namespace: str) -> str:
    """Derive a stable non-reversible tenant identifier with domain separation."""

    if not value or not secret:
        raise ValueError("principal and hash secret are required")
    payload = f"{namespace}\0{value}".encode()
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def _rq_owner_hash() -> str | None:
    try:
        from rq import get_current_job

        job = get_current_job()
    except Exception:
        return None
    if job is None or not isinstance(job.meta, dict):
        return None
    candidate = job.meta.get("owner_hash")
    if isinstance(candidate, str) and _OWNER_HASH_PATTERN.fullmatch(candidate):
        return candidate
    return None


def current_owner_hash() -> str | None:
    """Return the request owner or inherit it from the active RQ job metadata."""

    candidate = _OWNER_HASH.get()
    if isinstance(candidate, str) and _OWNER_HASH_PATTERN.fullmatch(candidate):
        return candidate
    return _rq_owner_hash()


def require_owner_hash() -> str:
    owner_hash = current_owner_hash()
    if owner_hash is None:
        raise RuntimeError("tenant context is required")
    return owner_hash


def set_owner_hash(owner_hash: str) -> contextvars.Token[str | None]:
    if not _OWNER_HASH_PATTERN.fullmatch(owner_hash):
        raise ValueError("owner hash must be a SHA-256 hex digest")
    return _OWNER_HASH.set(owner_hash)


def reset_owner_hash(token: contextvars.Token[str | None]) -> None:
    _OWNER_HASH.reset(token)


@contextmanager
def tenant_context(owner_hash: str) -> Iterator[None]:
    token = set_owner_hash(owner_hash)
    try:
        yield
    finally:
        reset_owner_hash(token)
