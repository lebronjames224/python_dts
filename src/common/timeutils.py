import time
from croniter import croniter

MINUTE = 60

def epoch_minute(now: int | None = None) -> int:
    now = now or int(time.time())
    return now - (now % MINUTE)

def next_from_cron(expr: str, base: int | None = None) -> int:
    base = base or int(time.time())
    it = croniter(expr, base)
    return int(it.get_next())