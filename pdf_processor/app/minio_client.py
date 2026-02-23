from minio import Minio
from .config import settings

client = Minio(
    settings.MINIO_ENDPOINT,
    access_key=settings.MINIO_ACCESS,
    secret_key=settings.MINIO_SECRET,
    secure=False,
)
