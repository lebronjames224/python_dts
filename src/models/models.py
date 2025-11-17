from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, Enum, Boolean, ForeignKey, Index,
    DateTime, func
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()

JobStatusEnum = ("pending", "scheduled", "running", "completed", "failed", "canceled")

class Job(Base):
    __tablename__ = "jobs"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    user_id = Column(String(64), nullable=True)
    payload = Column(JSONB, nullable=True)
    frequency = Column(String(32), nullable=False, default="once")  # once|cron
    cron = Column(String(128), nullable=True)  # when frequency == "cron"
    max_retries = Column(Integer, nullable=False, default=3)
    retry_count = Column(Integer, nullable=False, default=0)
    status = Column(String(32), nullable=False, default="pending")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    schedules = relationship("JobSchedule", back_populates="job", cascade="all, delete-orphan")

class JobSchedule(Base):
    __tablename__ = "job_schedules"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    job_id = Column(BigInteger, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    next_run_time = Column(BigInteger, nullable=False)  # epoch seconds (minute precision OK)
    last_run_time = Column(BigInteger, nullable=True)
    segment = Column(Integer, nullable=False, default=0)

    job = relationship("Job", back_populates="schedules")

Index("ix_schedules_next_segment", JobSchedule.next_run_time, JobSchedule.segment)

class JobExecution(Base):
    __tablename__ = "job_executions"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    job_id = Column(BigInteger, ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False)
    worker_id = Column(String(128), nullable=True)
    status = Column(String(32), nullable=False, default="running")
    started_at = Column(DateTime(timezone=True), server_default=func.now())
    finished_at = Column(DateTime(timezone=True), nullable=True)
    result_payload = Column(JSONB, nullable=True)
    error = Column(Text, nullable=True)

class Worker(Base):
    __tablename__ = "workers"
    id = Column(String(128), primary_key=True)
    last_heartbeat = Column(DateTime(timezone=True), server_default=func.now())
    capacity = Column(Integer, nullable=False, default=4)
    is_scheduler = Column(Boolean, nullable=False, default=False)


class Page(Base):
    __tablename__ = "pages"

    id            = Column(BigInteger, primary_key=True)
    url_norm      = Column(Text, nullable=False, unique=True)   # canonical URL
    url           = Column(Text, nullable=False)
    final_url     = Column(Text)
    etag          = Column(Text)
    last_modified = Column(Text)
    status_code   = Column(Integer)
    content_type  = Column(Text)
    size_bytes    = Column(Integer)
    duration_ms   = Column(Integer)
    content_hash  = Column(Text)
    title         = Column(Text)
    preview_text  = Column(Text)
    stored_object = Column(Text)  # e.g., s3://crawls/<key>
    fetched_at    = Column(DateTime(timezone=True), server_default=func.now())

    versions = relationship("PageVersion", back_populates="page", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_pages_fetched_at", fetched_at),
        Index("ix_pages_content_hash", content_hash),
    )

class PageVersion(Base):
    __tablename__ = "page_versions"

    id            = Column(BigInteger, primary_key=True)
    page_id       = Column(BigInteger, ForeignKey("pages.id", ondelete="CASCADE"), nullable=False)
    fetched_at    = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    content_hash  = Column(Text, nullable=False)
    stored_object = Column(Text)

    page = relationship("Page", back_populates="versions")

    __table_args__ = (
        Index("ix_page_versions_page_ts", page_id, fetched_at),
    )
