from sqlalchemy import (
    Column, Integer, BigInteger, String, Text, Enum, Boolean, ForeignKey, Index,
    DateTime, func
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

JobStatusEnum = ("pending", "scheduled", "running", "completed", "failed", "canceled")

class Job(Base):
    __tablename__ = "jobs"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    user_id = Column(String(64), nullable=True)
    payload = Column(Text, nullable=True)
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
    error = Column(Text, nullable=True)

class Worker(Base):
    __tablename__ = "workers"
    id = Column(String(128), primary_key=True)
    last_heartbeat = Column(DateTime(timezone=True), server_default=func.now())
    capacity = Column(Integer, nullable=False, default=4)
    is_scheduler = Column(Boolean, nullable=False, default=False)