import os

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from config import settings


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


def get_drive_service():
    creds = _get_credentials()
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def upload_file(path: str, filename: str, mime_type: str, folder_id: str | None) -> str:
    service = get_drive_service()
    metadata: dict[str, object] = {"name": filename}
    if folder_id:
        metadata["parents"] = [folder_id]
    media = MediaFileUpload(path, mimetype=mime_type or None, resumable=True)
    created = service.files().create(body=metadata, media_body=media, fields="id").execute()
    file_id = created.get("id")
    if not file_id:
        raise DriveError("Drive upload failed")
    return file_id
