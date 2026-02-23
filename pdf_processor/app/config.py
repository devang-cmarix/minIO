import os

class Settings:
    RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
    QUEUE_NAME = "pdf.jobs"

    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS = os.getenv("MINIO_ACCESS", "minio")
    MINIO_SECRET = os.getenv("MINIO_SECRET", "minio123")
    MINIO_BUCKET = "drive-bucket"

    MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017/")
    DB_NAME = "documents"

settings = Settings()
