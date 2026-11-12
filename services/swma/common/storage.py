"""S3 client factory: RustFS locally, real Amazon S3 in the cloud.

Locally ``S3_ACCESS_KEY`` is set and the client talks to RustFS over HTTP.
On AWS the keys are left empty and credentials come from the EC2 instance's
IAM role (instance metadata), so no secret ever sits in a file.
"""
from minio import Minio
from minio.credentials import IamAwsProvider

from . import settings


def s3_client() -> Minio:
    if settings.S3_ACCESS_KEY:
        return Minio(settings.S3_ENDPOINT, settings.S3_ACCESS_KEY, settings.S3_SECRET_KEY,
                     secure=settings.S3_SECURE, region=settings.S3_REGION)
    return Minio(settings.S3_ENDPOINT, credentials=IamAwsProvider(), secure=True, region=settings.S3_REGION)


def buckets() -> dict[str, str]:
    return {"checkpoints": settings.BUCKET_CHECKPOINTS, "archive": settings.BUCKET_ARCHIVE,
            "logs": settings.BUCKET_LOGS}
