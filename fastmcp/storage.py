import base64
import hashlib
import hmac
import os
import time
from urllib.parse import urlencode

import boto3

from config import settings


class StorageError(RuntimeError):
    pass


def _ensure_local_dirs() -> None:
    os.makedirs(settings.storage_local_dir, exist_ok=True)
    os.makedirs(settings.storage_temp_dir, exist_ok=True)


def build_storage_key(asset_id: str, ext: str) -> str:
    clean_ext = ext if ext.startswith(".") else f".{ext}" if ext else ""
    prefix = os.path.join(asset_id[:2], asset_id[2:4])
    filename = f"{asset_id}{clean_ext}"
    return os.path.join(prefix, filename)


def local_path_from_key(storage_key: str) -> str:
    return os.path.join(settings.storage_local_dir, storage_key)


def _ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _build_local_signed_url(asset_id: str, expires_at: int) -> str:
    if not settings.public_base_url:
        raise StorageError("PUBLIC_BASE_URL is required for local download URLs")
    if not settings.download_signing_secret:
        raise StorageError("DOWNLOAD_SIGNING_SECRET is required for local download URLs")
    payload = f"{asset_id}:{expires_at}".encode("utf-8")
    signature = hmac.new(
        settings.download_signing_secret.encode("utf-8"), payload, hashlib.sha256
    ).digest()
    sig = base64.urlsafe_b64encode(signature).decode("utf-8").rstrip("=")
    query = urlencode({"exp": str(expires_at), "sig": sig})
    return f"{settings.public_base_url.rstrip('/')}/download/{asset_id}?{query}"


def verify_local_signature(asset_id: str, expires_at: int, signature: str) -> bool:
    if not settings.download_signing_secret:
        return False
    payload = f"{asset_id}:{expires_at}".encode("utf-8")
    expected = hmac.new(
        settings.download_signing_secret.encode("utf-8"), payload, hashlib.sha256
    ).digest()
    expected_sig = base64.urlsafe_b64encode(expected).decode("utf-8").rstrip("=")
    return hmac.compare_digest(expected_sig, signature)


def get_storage_client():
    if settings.storage_backend != "s3":
        return None
    if not settings.s3_bucket:
        raise StorageError("S3_BUCKET is required for S3 storage")
    session = boto3.session.Session(
        aws_access_key_id=settings.s3_access_key or None,
        aws_secret_access_key=settings.s3_secret_key or None,
        region_name=settings.s3_region or None,
    )
    return session.client("s3", endpoint_url=settings.s3_endpoint_url or None)


def put_file(temp_path: str, asset_id: str, ext: str) -> tuple[str, str, int]:
    _ensure_local_dirs()
    storage_key = build_storage_key(asset_id, ext)
    if settings.storage_backend == "s3":
        client = get_storage_client()
        if client is None:
            raise StorageError("S3 client not available")
        client.upload_file(temp_path, settings.s3_bucket, storage_key)
        size_bytes = os.path.getsize(temp_path)
        os.remove(temp_path)
        storage_uri = f"s3://{settings.s3_bucket}/{storage_key}"
        return storage_key, storage_uri, size_bytes

    dest_path = local_path_from_key(storage_key)
    _ensure_parent_dir(dest_path)
    os.replace(temp_path, dest_path)
    size_bytes = os.path.getsize(dest_path)
    storage_uri = f"local://{storage_key}"
    return storage_key, storage_uri, size_bytes


def download_to_temp(storage_key: str) -> str:
    if settings.storage_backend != "s3":
        return local_path_from_key(storage_key)
    client = get_storage_client()
    if client is None:
        raise StorageError("S3 client not available")
    _ensure_local_dirs()
    import tempfile

    handle, temp_path = tempfile.mkstemp(dir=settings.storage_temp_dir, prefix="s3_")
    os.close(handle)
    client.download_file(settings.s3_bucket, storage_key, temp_path)
    return temp_path


def delete_file(storage_key: str) -> None:
    if settings.storage_backend == "s3":
        client = get_storage_client()
        if client is None:
            raise StorageError("S3 client not available")
        client.delete_object(Bucket=settings.s3_bucket, Key=storage_key)
        return
    path = local_path_from_key(storage_key)
    if os.path.exists(path):
        os.remove(path)


def generate_download_url(asset_id: str, storage_key: str) -> tuple[str, int]:
    expires_at = int(time.time()) + settings.download_url_ttl_seconds
    if settings.storage_backend == "s3":
        client = get_storage_client()
        if client is None:
            raise StorageError("S3 client not available")
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": settings.s3_bucket, "Key": storage_key},
            ExpiresIn=settings.download_url_ttl_seconds,
        )
        return url, expires_at
    url = _build_local_signed_url(asset_id, expires_at)
    return url, expires_at
