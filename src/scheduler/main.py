import time
from typing import Sequence

from sqlalchemy import text

from src.common.config import parse_segments, settings
from src.common.db import engine, session_scope
from src.common.logging import get_logger
from src.common.messaging import Rabbit
from src.common.timeutils import epoch_minute, next_from_cron

LEADER_SQL = "SELECT pg_try_advisory_lock(:key) as ok"

logger = get_logger(__name__)

class SchedulerService:
    def __init__(self):
        self.rabbit = Rabbit()
        self.assigned_segments: Sequence[int] = parse_segments(settings.scheduler_segments)
        logger.info(
            "Scheduler starting segments=%s batch=%d poll=%.2fs",
            ",".join(map(str, self.assigned_segments)),
            settings.scheduler_batch_size,
            settings.scheduler_poll_seconds,
        )

    def _claim_leader(self) -> bool:
        with engine.connect() as conn:
            return bool(conn.execute(text(LEADER_SQL), {"key": settings.leader_lock_key}).scalar())

    def _schedule(self, row, session):
        job_id = row["job_id"]
        schedule_id = row["schedule_id"]
        session.execute(text("UPDATE jobs SET status='scheduled' WHERE id=:jid"), {"jid": job_id})
        self.rabbit.publish({"job_id": job_id, "scheduled_at": int(time.time())})

        if row["frequency"] == "once":
            session.execute(text("DELETE FROM job_schedules WHERE id=:sid"), {"sid": schedule_id})
            return

        cron = row["cron"]
        if not cron:
            logger.warning("Job %s frequency=cron but cron expression missing; disabling schedule", job_id)
            session.execute(text("DELETE FROM job_schedules WHERE id=:sid"), {"sid": schedule_id})
            return

        next_rt = epoch_minute(next_from_cron(cron))
        session.execute(
            text("UPDATE job_schedules SET last_run_time = next_run_time, next_run_time = :n WHERE id=:sid"),
            {"sid": schedule_id, "n": next_rt},
        )

    def tick(self):
        if not self._claim_leader():
            return
        now_min = epoch_minute()
        scheduled = 0
        sql = text(
            """
            SELECT js.id as schedule_id,
                   js.job_id,
                   j.frequency,
                   j.cron
            FROM job_schedules js
            JOIN jobs j ON j.id = js.job_id
            WHERE js.next_run_time <= :now
              AND j.status = 'pending'
              AND js.segment = ANY(:segments)
            FOR UPDATE SKIP LOCKED
            LIMIT :limit
            """
        )
        with session_scope() as session:
            rows = session.execute(
                sql,
                {
                    "now": now_min,
                    "segments": list(self.assigned_segments),
                    "limit": settings.scheduler_batch_size,
                },
            ).mappings().all()
            for row in rows:
                self._schedule(row, session)
                scheduled += 1
        if scheduled:
            logger.info("Scheduled %d jobs", scheduled)

    def run(self):
        while True:
            try:
                self.tick()
            except Exception:
                logger.exception("Scheduler tick failed")
            time.sleep(settings.scheduler_poll_seconds)

def main():
    SchedulerService().run()

if __name__ == "__main__":
    main()
