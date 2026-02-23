import io
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]

class DriveDownloader:
    def __init__(self, creds_path):
        creds = Credentials.from_service_account_file(
            creds_path, scopes=SCOPES
        )
        self.service = build("drive", "v3", credentials=creds)

    def download(self, file_id):
        request = self.service.files().get_media(fileId=file_id)
        file_stream = io.BytesIO()
        downloader = request.execute()
        file_stream.write(downloader)
        file_stream.seek(0)
        return file_stream
