import time
from datetime import datetime
from .drive_client import DriveClient
from .publisher import EventPublisher
from .config import settings
from .logger import get_logger
from concurrent.futures import ThreadPoolExecutor

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

            # # Publish in parallel
            # if new_events:
            #     with ThreadPoolExecutor(max_workers=10) as executor:
            #         executor.submit(publisher.publish, new_events)

            
        except Exception as e:
            logger.exception("Error during sync loop")

        time.sleep(settings.POLL_INTERVAL)

if __name__ == "__main__":
    run()
