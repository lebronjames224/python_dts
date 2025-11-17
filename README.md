# Python Distributed Task Scheduler

A lightweight yet production-friendly job platform composed of a FastAPI control plane, a PostgreSQL metadata store, a RabbitMQ queue, and a pool of workers that execute user-defined tasks. The scheduler promotes due jobs into the queue, workers execute in parallel with bounded concurrency, and results/metadata are persisted for observability.

## Architecture

- **API (`src/api`)** – Accepts job submissions (`POST /jobs`), exposes status lookup and cancellation endpoints, and writes schedules directly into Postgres.
- **Scheduler (`src/scheduler`)** – Cohort of stateless processes that acquire a Postgres advisory lock for leadership, fan out across hash-based segments, and enqueue due jobs in batches.
- **Worker (`src/worker`)** – Thread-pooled consumer pulling from RabbitMQ, running registered task functions from `src/tasks/registry.py`, persisting success/failure details, retrying with exponential backoff, and archiving task artifacts to MinIO via the blobstore helper.
- **Common libraries (`src/common`)** – Configuration loader, shared logging setup, RabbitMQ client wrapper, DB session helpers, blobstore helpers, and cron/time utilities.

Data flows API → Postgres → Scheduler → RabbitMQ → Worker → Postgres (+ MinIO for artifacts). Each step is idempotent and uses explicit status transitions to remain safe across restarts.

## Getting Started

### Requirements

- Docker + Docker Compose (recommended), or Python 3.12 with PostgreSQL/RabbitMQ/MinIO available locally.
- `pip install -r requirements.txt` if running directly.

### Local development with Docker Compose

```bash
cp .env .env.local  # optional: keep personal overrides separate
docker compose up --build
```

Running services (default `.env` values assume containers can reach each other by service name):

| Service   | Port | Notes                                  |
|-----------|------|----------------------------------------|
| API       | 8000 | FastAPI + interactive docs             |
| Postgres  | 5432 | Primary metadata store                 |
| RabbitMQ  | 5672 / 15672 | AMQP + web console             |
| MinIO     | 9000 / 9001 | S3-compatible storage + console |

Volumes keep data between restarts. Hot-reload is enabled for the API/worker/scheduler via the `.:/app` bind mount.

### Running without Docker

1. Start Postgres, RabbitMQ, and MinIO (or any S3-compatible) manually.
2. Export the necessary environment variables (see below) or edit `.env`.
3. Install Python dependencies: `pip install -r requirements.txt`.
4. Run individual processes:
   ```bash
   uvicorn src.api.main:app --reload
   python -m src.scheduler.main
   python -m src.worker.main
   ```

## Configuration

`src/common/config.py` loads `.env` automatically and exposes strongly-typed settings. Important knobs:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+psycopg2://app:app@localhost:5432/jobs` | SQLAlchemy database URL |
| `RABBITMQ_URL` | `amqp://app:app@localhost:5672/` | Connection string consumed by Pika |
| `RABBITMQ_QUEUE` | `jobs` | Queue name shared by scheduler + workers |
| `RABBITMQ_PREFETCH` | `0` | Overrides worker prefetch (falls back to `MAX_CONCURRENCY`) |
| `MAX_CONCURRENCY` | `4` | Threads per worker process |
| `WORKER_ID` | `w-local` | Logical ID recorded in DB/heartbeat table |
| `SCHEDULER_SEGMENTS` | `0-127` | Hash-space slices a scheduler instance is responsible for |
| `SCHEDULER_BATCH_SIZE` | `100` | # jobs dequeued per tick |
| `SCHEDULER_POLL_SECONDS` | `1.0` | Tick interval |
| `LEADER_LOCK_KEY` | `424242` | Advisory lock key for scheduler leadership |
| `HEARTBEAT_INTERVAL` | `3` | Seconds between worker heartbeats |
| `LOG_LEVEL` | `INFO` | Logger verbosity |
| `MINIO_*` | defaults in `.env` | MinIO/S3 credentials for blob storage |

Update `.env` or export environment vars to match your deployment.

## Database

SQLAlchemy models live in `src/models/models.py`. Use Alembic (already declared in `requirements.txt`) to version schema changes. For quick local iteration the worker ensures the `job_executions.result_payload` column exists by issuing a guarded `ALTER TABLE` on start, but formal migrations should be preferred.

## API Usage

- **Create job** `POST /jobs`
  ```jsonc
  {
    "name": "crawl_url",
    "payload": {"url": "https://example.com"},
    "frequency": "once",
    "next_run_time": 1713280500,
    "max_retries": 3
  }
  ```
  - `frequency: "once"` requires `next_run_time` (epoch seconds, minute precision).
  - `frequency: "cron"` requires a POSIX cron expression in `cron`.
- **Inspect job** `GET /jobs/{id}`
- **Cancel job** `DELETE /jobs/{id}`

Responses are shaped by `src/schemas/schemas.py`.

## Writing Tasks

Tasks live in `src/tasks/registry.py`. Decorate a callable with `@task("name")` and accept a dict payload:

```python
from src.tasks.registry import task, PermanentTaskError

@task("send_email")
def send_email(payload: dict):
    to = payload.get("to")
    if not to:
        raise PermanentTaskError("missing recipient")
    # ...send...
    return {"delivered": True}
```

Workers automatically:

- Deserialize payloads stored as JSONB or plain text.
- Pass results back into `job_executions.result_payload`.
- Mark jobs failed without retries when `PermanentTaskError` is raised.

Task helpers include `_canon`, `_read_capped`, `_title_and_preview`, and MinIO blob storage utilities, used by the bundled `crawl_url` example task.

## Observability & Reliability

- Shared logging is configured via `src/common/logging.py`, producing consistent timestamped logs across every process. Set `LOG_LEVEL=DEBUG` for deeper traces.
- Workers emit heartbeats (`workers` table) and track each execution in `job_executions` including error stacks and stored result JSON for triage.
- Scheduler leadership relies on `pg_try_advisory_lock`, making horizontal scaling as simple as running multiple scheduler containers with disjoint `SCHEDULER_SEGMENTS`.
- Retries use exponential backoff (1–30 minutes) capped by per-job `max_retries`. Failed jobs remain in the DB for inspection.

## Extending

- **Add new transports** – Swap RabbitMQ for another broker by implementing a compatible wrapper in `src/common/messaging.py`.
- **Custom storage** – `src/common/blobstore.py` abstracts MinIO/S3; extend or replace as needed.
- **Metrics/Tracing** – Integrate OpenTelemetry or Prometheus by instrumenting the scheduler/worker loops and HTTP endpoints.

With the architecture modularized, you can deploy the API, scheduler, and worker processes independently and scale each tier according to workload. Happy scheduling!
