import os
import re

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from .config import settings


class DriveError(RuntimeError):
    pass


def _get_credentials() -> Credentials:
    path = settings.google_drive_credentials_path
    if not path:
        raise DriveError("GOOGLE_DRIVE_CREDENTIALS_PATH is required")
    if not os.path.exists(path):
        raise DriveError("Drive credentials file not found")
    creds = Credentials.from_service_account_file(
        path,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    if settings.google_drive_impersonate_user:
        creds = creds.with_subject(settings.google_drive_impersonate_user)
    return creds


def _configured_folder_ids() -> set[str]:
    raw = settings.google_drive_allowed_folder_ids
    values = raw.split(",") if isinstance(raw, str) else raw or []
    return {str(value).strip() for value in values if str(value).strip()}


def _validate_folder_id(folder_id: str | None) -> str:
    folder_id = str(folder_id or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,256}", folder_id):
        raise DriveError("Google Drive folder id is invalid")
    allowed = _configured_folder_ids()
    if not allowed:
        raise DriveError("Google Drive exports are disabled until an allowed folder is configured")
    if folder_id not in allowed:
        raise DriveError("Google Drive folder is not allowlisted")
    return folder_id


def _validate_upload_source(path: str) -> None:
    if os.path.islink(path) or not os.path.isfile(path):
        raise DriveError("Drive upload source must be a regular non-symlink file")


def get_drive_service():
    creds = _get_credentials()
    return build(
        "drive",
        "v3",
        credentials=creds,
        cache_discovery=False,
        num_retries=0,
    )


def upload_file(path: str, filename: str, mime_type: str, folder_id: str | None) -> str:
    folder_id = _validate_folder_id(folder_id)
    _validate_upload_source(path)
    service = get_drive_service()
    metadata: dict[str, object] = {"name": filename, "parents": [folder_id]}
    media = MediaFileUpload(path, mimetype=mime_type or None, resumable=True)
    created = (
        service.files().create(body=metadata, media_body=media, fields="id").execute(num_retries=0)
    )
    file_id = created.get("id")
    if not file_id:
        raise DriveError("Drive upload failed")
    return file_id
