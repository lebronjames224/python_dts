import json
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import text

from src.common.config import settings
from src.common.db import engine, session_scope
from src.common.logging import get_logger
from src.common.messaging import Rabbit
from src.tasks.registry import TASKS, PermanentTaskError

logger = get_logger(__name__)

TOTAL_SEGMENTS = 128  # Keep in sync with scheduler partitioning

def _ensure_job_execution_schema():
    try:
        with engine.connect() as conn:
            conn.execute(
                text("ALTER TABLE IF EXISTS job_executions ADD COLUMN IF NOT EXISTS result_payload JSONB")
            )
    except Exception:
        logger.exception("Unable to ensure job_executions.result_payload column")

_ensure_job_execution_schema()

r = Rabbit()

def heartbeat():
    while True:
        with session_scope() as s:
            s.execute(
                text(
                    """
                    INSERT INTO workers(id, last_heartbeat, capacity, is_scheduler)
                    VALUES (:id, NOW(), :cap, false)
                    ON CONFLICT (id) DO UPDATE SET last_heartbeat = NOW(), capacity = EXCLUDED.capacity
                    """
                ),
                {"id": settings.worker_id, "cap": settings.max_concurrency},
            )
        time.sleep(settings.heartbeat_interval)

threading.Thread(target=heartbeat, daemon=True).start()

executor = ThreadPoolExecutor(max_workers=settings.max_concurrency)

def _normalize_payload(payload):
    if isinstance(payload, dict) or payload is None:
        return payload or {}
    if isinstance(payload, str):
        stripped = payload.strip()
        if stripped.startswith("{"):
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                pass
        return {"_raw": payload}
    return {"_raw": payload}

def _record_success(job_id: int, exec_id: int, result):
    result_json = json.dumps(result) if isinstance(result, dict) else None
    with session_scope() as s:
        s.execute(
            text(
                "UPDATE job_executions "
                "SET status='completed', finished_at=NOW(), result_payload = :r::jsonb "
                "WHERE id=:eid"
            ),
            {"eid": exec_id, "r": result_json},
        )
        s.execute(text("UPDATE jobs SET status='completed', retry_count=0 WHERE id=:jid"), {"jid": job_id})

def _record_failure(job_id: int, exec_id: int | None, error_text: str, retryable: bool):
    truncated = error_text[-6000:]
    if exec_id is not None:
        with session_scope() as s:
            s.execute(
                text(
                    "UPDATE job_executions SET status='failed', finished_at=NOW(), error=:e WHERE id=:eid"
                ),
                {"eid": exec_id, "e": truncated},
            )
    if not retryable:
        with session_scope() as s:
            s.execute(text("UPDATE jobs SET status='failed' WHERE id=:jid"), {"jid": job_id})
        return

    with session_scope() as s:
        row = s.execute(
            text("SELECT retry_count, max_retries FROM jobs WHERE id=:jid FOR UPDATE"),
            {"jid": job_id},
        ).fetchone()
        if not row:
            return
        rc, max_retries = row
        rc += 1
        if rc <= max_retries:
            backoff = [60, 300, 600, 1800][min(rc - 1, 3)]
            next_rt = int(time.time()) + backoff
            next_rt -= next_rt % 60
            s.execute(
                text("UPDATE jobs SET retry_count=:rc, status='pending' WHERE id=:jid"),
                {"jid": job_id, "rc": rc},
            )
            s.execute(
                text(
                    "INSERT INTO job_schedules(job_id, next_run_time, segment) "
                    "VALUES (:jid, :nrt, :seg)"
                ),
                {"jid": job_id, "nrt": next_rt, "seg": job_id % TOTAL_SEGMENTS},
            )
        else:
            s.execute(text("UPDATE jobs SET status='failed' WHERE id=:jid"), {"jid": job_id})

def run_task(body: dict) -> str:
    job_id = body["job_id"]
    exec_id = None
    try:
        with session_scope() as s:
            exec_id = s.execute(
                text(
                    "INSERT INTO job_executions(job_id, worker_id, status) "
                    "VALUES (:jid, :wid, 'running') RETURNING id"
                ),
                {"jid": job_id, "wid": settings.worker_id},
            ).scalar_one()
            job_row = s.execute(
                text("SELECT name, payload FROM jobs WHERE id = :jid"),
                {"jid": job_id},
            ).fetchone()

        if not job_row:
            raise RuntimeError(f"Job {job_id} not found")

        name, payload = job_row
        task_fn = TASKS.get(name)
        if not task_fn:
            raise PermanentTaskError(f"Unknown task: {name!r}")

        payload_dict = _normalize_payload(payload)
        logger.info("Worker %s running job_id=%s task=%s", settings.worker_id, job_id, name)

        result = task_fn(payload_dict)

        _record_success(job_id, exec_id, result)
        logger.info("Job %s completed", job_id)
        return "ack"

    except PermanentTaskError as exc:
        logger.warning("Job %s failed permanently: %s", job_id, exc)
        _record_failure(job_id, exec_id, str(exc), retryable=False)
        return "ack"
    except Exception:
        err = traceback.format_exc()
        logger.exception("Job %s errored", job_id)
        _record_failure(job_id, exec_id, err, retryable=True)
        return "ack"

def on_message(ch, method, properties, body_bytes):
    body = json.loads(body_bytes)
    delivery_tag = method.delivery_tag
    future = executor.submit(run_task, body)

    def _done(fut):
        try:
            outcome = fut.result()
        except Exception:
            logger.exception("Worker thread crashed, requeuing message")
            r.nack_threadsafe(delivery_tag, requeue=True)
            return

        if outcome == "ack":
            r.ack_threadsafe(delivery_tag)
        elif outcome == "nack_requeue":
            r.nack_threadsafe(delivery_tag, requeue=True)
        else:
            r.nack_threadsafe(delivery_tag, requeue=False)

    future.add_done_callback(_done)

logger.info("Worker %s starting...", settings.worker_id)
r.consume(on_message)
