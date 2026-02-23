import os

class Settings:
    RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    QUEUE_NAME = os.getenv("QUEUE_NAME", "file.upload")

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS = os.getenv("MINIO_ACCESS", "minio")
    MINIO_SECRET = os.getenv("MINIO_SECRET", "minio123")
    MINIO_BUCKET = os.getenv("MINIO_BUCKET", "drive-bucket")

settings = Settings()
