import time
import json
from sqlalchemy import text
from src.common.db import session_scope, engine
from src.common.config import settings, parse_segments
from src.common.messaging import Rabbit
from src.common.timeutils import epoch_minute

N_SEGMENTS = 128

LEADER_SQL = "SELECT pg_try_advisory_lock(:key) as ok"

r = Rabbit()

assigned_segments = parse_segments(settings.scheduler_segments)

print(f"Scheduler starting; segments={assigned_segments}")

while True:
    with engine.connect() as conn:
        ok = conn.execute(text(LEADER_SQL), {"key": settings.leader_lock_key}).scalar()
    if not ok:
        time.sleep(1)
        continue

    now_min = epoch_minute()
    with session_scope() as s:
        rows = s.execute(text(
            """
            SELECT js.id, js.job_id
            FROM job_schedules js
            JOIN jobs j ON j.id = js.job_id
            WHERE js.next_run_time <= :now
              AND j.status = 'pending'
              AND js.segment = ANY(:segments)
            FOR UPDATE SKIP LOCKED
            LIMIT 100
            """
        ), {"now": now_min, "segments": assigned_segments}).fetchall()

        for (sched_id, job_id) in rows:
            s.execute(text("UPDATE jobs SET status='scheduled' WHERE id=:jid"), {"jid": job_id})
            r.publish({"job_id": job_id, "scheduled_at": int(time.time())})
            res = s.execute(text("SELECT frequency, cron FROM jobs WHERE id=:jid"), {"jid": job_id}).fetchone()
            freq, cron = res
            if freq == "once":
                s.execute(text("DELETE FROM job_schedules WHERE id=:sid"), {"sid": sched_id})
            else:
                from src.common.timeutils import next_from_cron
                next_rt = epoch_minute(next_from_cron(cron))
                s.execute(text("UPDATE job_schedules SET last_run_time = next_run_time, next_run_time = :n WHERE id=:sid"), {"sid": sched_id, "n": next_rt})
    time.sleep(1)