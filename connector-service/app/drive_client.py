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
        files = []
        page_token = None

        while True:
            response = self.service.files().list(
                pageSize=1000,  # fetch many per page
                fields="nextPageToken, files(id, name, mimeType, modifiedTime)",
                pageToken=page_token
            ).execute()

            files.extend(response.get("files", []))

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        return files

