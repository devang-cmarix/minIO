from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
from .config import settings

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

class DriveClient:
    def __init__(self):
        creds = Credentials.from_service_account_file(
            settings.GOOGLE_CREDENTIALS, scopes=SCOPES
        )
        self.service = build("drive", "v3", credentials=creds)

    def list_files(self):
        response = self.service.files().list(
            pageSize=20,
            fields="files(id, name, mimeType, modifiedTime)"
        ).execute()
        return response.get("files", [])
