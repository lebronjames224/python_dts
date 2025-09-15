import os
from dataclasses import dataclass

@dataclass
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "postgresql+psycopg2://app:app@localhost:5432/jobs")
    rabbitmq_url: str = os.getenv("RABBITMQ_URL", "amqp://app:app@localhost:5672/")
    scheduler_segments: str = os.getenv("SCHEDULER_SEGMENTS", "0-127")  # e.g., "0-63,96-127"
    leader_lock_key: int = int(os.getenv("LEADER_LOCK_KEY", "424242"))
    max_concurrency: int = int(os.getenv("MAX_CONCURRENCY", "4"))
    worker_id: str = os.getenv("WORKER_ID", "w-local")
    minio_endpoint: str = "http://127.0.0.1:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "crawls"
    minio_region: str | None = "us-east-1"

settings = Settings()


def parse_segments(seg_str: str) -> list[int]:
    parts = seg_str.split(",")
    segments: list[int] = []
    for p in parts:
        if "-" in p:
            a, b = p.split("-")
            segments.extend(list(range(int(a), int(b) + 1)))
        else:
            segments.append(int(p))
    return segments