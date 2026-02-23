from minio import Minio
from .config import settings

def get_minio_client():
    client = Minio(
        settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS,
        secret_key=settings.MINIO_SECRET,
        secure=False,
    )

    if not client.bucket_exists(settings.MINIO_BUCKET):
        client.make_bucket(settings.MINIO_BUCKET)

    return client
