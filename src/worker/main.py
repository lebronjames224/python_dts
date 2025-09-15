import json
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import traceback
from sqlalchemy import text

from src.common.config import settings
from src.common.db import session_scope
from src.common.messaging import Rabbit
from src.tasks.registry import TASKS

r = Rabbit()

# Heartbeat thread
def heartbeat():
    while True:
        with session_scope() as s:
            s.execute(text("""
                INSERT INTO workers(id, last_heartbeat, capacity, is_scheduler)
                VALUES (:id, NOW(), :cap, false)
                ON CONFLICT (id) DO UPDATE SET last_heartbeat = NOW(), capacity = EXCLUDED.capacity
            """), {"id": settings.worker_id, "cap": settings.max_concurrency})
        time.sleep(3)

threading.Thread(target=heartbeat, daemon=True).start()

# Concurrency gate
executor = ThreadPoolExecutor(max_workers=settings.max_concurrency)

def run_task(body: dict) -> str:
    """Run the job and return an ack/nack signal."""
    job_id = body["job_id"]
    exec_id = None
    try:
        # 1) record start
        with session_scope() as s:
            exec_id = s.execute(text(
                "INSERT INTO job_executions(job_id, worker_id, status) "
                "VALUES (:jid, :wid, 'running') RETURNING id"
            ), {"jid": job_id, "wid": settings.worker_id}).fetchone()[0]

        # 2) fetch name + payload and normalize payload to a dict
        with session_scope() as s:
            name, payload = s.execute(text(
                "SELECT name, payload FROM jobs WHERE id = :jid"
            ), {"jid": job_id}).fetchone()

        task_fn = TASKS.get(name)
        if not task_fn:
            raise RuntimeError(f"Unknown task: {name!r}")

        # Normalize payload:
        # - if it's already a dict (JSONB), use it
        # - if it's a JSON string, parse it
        # - otherwise keep a best-effort wrapper
        if isinstance(payload, dict) or payload is None:
            payload_dict = payload or {}
        elif isinstance(payload, str):
            try:
                payload_dict = json.loads(payload) if payload.strip().startswith("{") else {"_raw": payload}
            except Exception:
                payload_dict = {"_raw": payload}
        else:
            payload_dict = {"_raw": payload}

        # 3) call the task; tasks should accept a dict and may return a small result dict
        result = task_fn(payload_dict)

        # 4) mark success (optionally persist result_json if it's a dict)
        with session_scope() as s:
            s.execute(text(
                "UPDATE job_executions "
                "SET status='completed', finished_at=NOW() "
                "WHERE id=:eid"
            ), {"eid": exec_id, "r": json.dumps(result) if isinstance(result, dict) else None})
            s.execute(text("UPDATE jobs SET status='completed' WHERE id=:jid"), {"jid": job_id})
        return "ack"

    except Exception:
        # capture error for debugging
        err = traceback.format_exc()
        if exec_id is not None:
            with session_scope() as s:
                s.execute(text(
                    "UPDATE job_executions SET status='failed', finished_at=NOW(), error=:e WHERE id=:eid"
                ), {"eid": exec_id, "e": err})

        # retry/backoff (unchanged)
        with session_scope() as s:
            rc, mr, freq, cron = s.execute(text(
                "SELECT retry_count, max_retries, frequency, cron FROM jobs WHERE id=:jid"
            ), {"jid": job_id}).fetchone()
            rc += 1
            if rc <= mr:
                backoff = [60, 300, 600, 1800][min(rc - 1, 3)]
                next_rt = int(time.time()) + backoff
                next_rt -= next_rt % 60
                s.execute(text("UPDATE jobs SET retry_count=:rc, status='pending' WHERE id=:jid"),
                          {"jid": job_id, "rc": rc})
                s.execute(text(
                    "INSERT INTO job_schedules(job_id, next_run_time, segment) VALUES (:jid, :nrt, :seg)"
                ), {"jid": job_id, "nrt": next_rt, "seg": job_id % 128})
                return "ack"
            else:
                s.execute(text("UPDATE jobs SET status='failed' WHERE id=:jid"), {"jid": job_id})
                return "ack"
def on_message(ch, method, properties, body_bytes):
    body = json.loads(body_bytes)
    delivery_tag = method.delivery_tag

    future = executor.submit(run_task, body)

    def _done(fut):
        try:
            outcome = fut.result()
        except Exception:
            r.nack_threadsafe(delivery_tag, requeue=True)
            return

        if outcome == "ack":
            r.ack_threadsafe(delivery_tag)
        elif outcome == "nack_requeue":
            r.nack_threadsafe(delivery_tag, requeue=True)
        else:
            r.nack_threadsafe(delivery_tag, requeue=False)

    future.add_done_callback(_done)

print(f"Worker {settings.worker_id} starting...")
r.consume(on_message)