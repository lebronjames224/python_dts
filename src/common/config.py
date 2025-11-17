import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Load a .env beside the repo root if present so local development mirrors production env vars.
ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env", override=False)

@dataclass
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "postgresql+psycopg2://app:app@localhost:5432/jobs")
    rabbitmq_url: str = os.getenv("RABBITMQ_URL", "amqp://app:app@localhost:5672/")
    rabbitmq_queue: str = os.getenv("RABBITMQ_QUEUE", "jobs")
    rabbitmq_prefetch: int = int(os.getenv("RABBITMQ_PREFETCH", "0"))  # 0 -> fall back to max concurrency
    rabbitmq_heartbeat: int = int(os.getenv("RABBITMQ_HEARTBEAT", "0"))
    scheduler_segments: str = os.getenv("SCHEDULER_SEGMENTS", "0-127")  # e.g., "0-63,96-127"
    leader_lock_key: int = int(os.getenv("LEADER_LOCK_KEY", "424242"))
    scheduler_batch_size: int = int(os.getenv("SCHEDULER_BATCH_SIZE", "100"))
    scheduler_poll_seconds: float = float(os.getenv("SCHEDULER_POLL_SECONDS", "1.0"))
    max_concurrency: int = int(os.getenv("MAX_CONCURRENCY", "4"))
    worker_id: str = os.getenv("WORKER_ID", "w-local")
    heartbeat_interval: int = int(os.getenv("HEARTBEAT_INTERVAL", "3"))
    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "crawls"
    minio_region: str | None = "us-east-1"
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

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
