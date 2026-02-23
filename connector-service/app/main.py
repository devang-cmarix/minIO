import time
from datetime import datetime
from .drive_client import DriveClient
from .publisher import EventPublisher
from .config import settings
from .logger import get_logger

logger = get_logger("connector")

def run():
    drive = DriveClient()
    publisher = EventPublisher()
    seen_files = set()

    while True:
        try:
            files = drive.list_files()
            # for f in files:
            #     if f["id"] not in seen_files:
            #         event = {
            #             "event": "file.upload",
            #             "file_id": f["id"],
            #             "file_name": f["name"],
            #             "mime_type": f["mimeType"],
            #             "timestamp": datetime.utcnow().isoformat()
            #         }
            #         publisher.publish(event)
            #         seen_files.add(f["id"])

            for f in files:
                # Skip folders
                if f["mimeType"] == "application/vnd.google-apps.folder":
                    continue

                if f["id"] not in seen_files:
                    event = {
                        "event": "file.upload",
                        "file_id": f["id"],
                        "file_name": f["name"],
                        "mime_type": f["mimeType"],
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    publisher.publish(event)
                    seen_files.add(f["id"])

            
        except Exception as e:
            logger.exception("Error during sync loop")

        time.sleep(settings.POLL_INTERVAL)

if __name__ == "__main__":
    run()
