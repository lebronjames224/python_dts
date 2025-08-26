from fastapi import FastAPI, HTTPException
from src.common.db import session_scope, engine
from src.models.models import Base, Job, JobSchedule
from src.schemas.schemas import JobCreate ,JobOut
from src.common.timeutils import epoch_minute, next_from_cron
from src.common.config import parse_segments
import time

app = FastAPI(title="Job Scheduler API")

Base.metadata.create_all(bind=engine)

@app.post("/jobs", response_model=JobOut)
def submit_job(j: JobCreate):
    with session_scope() as s:
        job = Job(name=j.name, payload=j.payload, frequency=j.frequency, cron=j.cron, max_retries=j.max_retries)
        s.add(job)
        s.flush()
        segment = job.id % 128
        if j.frequency == "once":
            if not j.next_run_time:
                raise HTTPException(400, "next_run_time required for one-time jobs")
            nrt = epoch_minute(j.next_run_time)
        else:
            if not j.cron:
                raise HTTPException(400, "cron expression required for cron jobs")
            nrt = epoch_minute(next_from_cron(j.cron))
        s.add(JobSchedule(job_id=job.id, next_run_time=nrt, segment=segment))
        s.flush()
        job.status = "pending"
        return JobOut(id=job.id, name=job.name, status=job.status)

@app.get("/jobs/{job_id}", response_model=JobOut)
def get_job(job_id: int):
    with session_scope() as s:
        job = s.get(Job, job_id)
        if not job:
            raise HTTPException(404, "job not found")
        return JobOut(id=job.id, name=job.name, status=job.status)

@app.delete("/jobs/{job_id}")
def cancel_job(job_id: int):
    with session_scope() as s:
        job = s.get(Job, job_id)
        if not job:
            raise HTTPException(404, "job not found")
        job.status = "canceled"
        # (optional) also remove or disable its schedules
        s.query(JobSchedule).filter(JobSchedule.job_id == job_id).delete()
        return {"ok": True}