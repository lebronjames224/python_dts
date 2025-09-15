from __future__ import annotations
from typing import Optional
from urllib.parse import quote
import botocore
import boto3

from src.common.config import settings

def _client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name=settings.minio_region,
        config=botocore.config.Config(s3={"addressing_style": "path"}),
    )

def _ensure_bucket(c):
    try:
        c.head_bucket(Bucket=settings.minio_bucket)
    except Exception:
        # Create if missing (idempotent enough for MinIO)
        c.create_bucket(Bucket=settings.minio_bucket)

def put_bytes(body: bytes, key: str, content_type: Optional[str] = None) -> str:
    """
    Store bytes in MinIO and return an s3:// URL pointer (scheme kept generic).
    """
    c = _client()
    _ensure_bucket(c)
    extra = {"ContentType": content_type} if content_type else {}
    c.put_object(Bucket=settings.minio_bucket, Key=key, Body=body, **extra)
    return f"s3://{settings.minio_bucket}/{quote(key)}"

def presign_get(key: str, expires_seconds: int = 3600) -> str:
    """
    Return a temporary HTTP URL to download via MinIO.
    """
    c = _client()
    return c.generate_presigned_url(
        "get_object",
        Params={"Bucket": settings.minio_bucket, "Key": key},
        ExpiresIn=expires_seconds,
    )