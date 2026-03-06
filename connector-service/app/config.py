import os

#GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS", "credentials.json")

class Settings:
    GOOGLE_CREDENTIALS = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "/app/credentials.json"
)
    RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    QUEUE_NAME = os.getenv("QUEUE_NAME", "file.upload")
    POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))

settings = Settings()
