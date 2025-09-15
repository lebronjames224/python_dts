# Dockerfile
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# system deps (only if you need them)
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

# copy and install deps first (better layer caching)
COPY requirements.txt .
# tip: prefer psycopg2-binary in requirements to avoid build toolchain
RUN pip install --no-cache-dir -r requirements.txt

# copy code
COPY . .

