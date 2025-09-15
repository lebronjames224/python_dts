# src/tasks/registry.py
from typing import Any, Callable, Dict, Union, Optional
import json, time, hashlib, re
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
import requests
from sqlalchemy import text

from src.common.db import engine
from src.common.blobstore import put_bytes


USER_AGENT = "py-scheduler/1.0 (+https://your.example)"
MAX_BYTES = 512_000
ALLOWED_TYPES = {"text/html", "text/plain", "application/json"}


TASKS: Dict[str, Callable[[Any], Any]] = {}

class PermanentTaskError(Exception):
    """Non-retryable failure (validation, robots, 4xx except 429, etc.)."""

def task(name: str):
    def deco(fn: Callable[[Any], Any]):
        TASKS[name] = fn
        return fn
    return deco

def _canon(url: str) -> str:
    """Canonicalize: drop fragments & utm_*, sort query params."""
    p = urlparse(url)
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not k.lower().startswith("utm_")]
    q.sort()
    return urlunparse(p._replace(query=urlencode(q), fragment=""))

def _read_capped(resp: requests.Response, limit: int) -> bytes:
    buf = bytearray(); total = 0
    for chunk in resp.iter_content(8192):
        if not chunk:
            continue
        total += len(chunk)
        if total > limit:
            buf.extend(chunk[: max(0, limit - (total - len(chunk)))])
            break
        buf.extend(chunk)
    return bytes(buf)

def _title_and_preview(body: bytes, ctype: str | None) -> tuple[Optional[str], Optional[str]]:
    title = None
    preview = None
    try:
        if ctype == "text/html":
            try:
                from bs4 import BeautifulSoup  # optional
                soup = BeautifulSoup(body, "html.parser")
                title = (soup.title.string or "").strip() if soup.title else None
                preview = soup.get_text(separator=" ", strip=True)[:2000]
            except Exception:
                # Fallback: naive title extraction
                m = re.search(rb"<title[^>]*>(.*?)</title>", body, re.I | re.S)
                if m:
                    title = m.group(1).decode(errors="ignore").strip()
                preview = body.decode(errors="ignore")[:2000]
        else:
            preview = body.decode(errors="ignore")[:2000]
    except Exception:
        pass
    return title, preview

@task("crawl_url")
def crawl_url(payload: Union[dict, str, None]):
    """
    Accepts:
      - dict: {"url": "..."}  (preferred)
      - str:  raw URL or JSON string like '{"url":"..."}'
    Stores full body in MinIO (via put_bytes) and metadata in Postgres.
    """
    # ---- normalize payload -> URL string
    url: Optional[str] = None
    if isinstance(payload, dict):
        url = (payload.get("url") or payload.get("_raw") or "").strip()
    elif isinstance(payload, str):
        s = payload.strip()
        if s.startswith("{"):
            try:
                data = json.loads(s)
                url = (data.get("url") or "").strip()
            except Exception:
                url = s
        else:
            url = s
    else:
        url = ""
    if not url:
        raise ValueError("payload must include 'url'")

    url_norm = _canon(url)
    p = urlparse(url_norm)
    if p.scheme not in {"http", "https"} or not p.netloc:
        raise ValueError("invalid URL")

    # ---- load prior ETag/Last-Modified (for conditional GET)
    etag = last_mod = None
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT etag, last_modified, content_hash FROM pages WHERE url_norm=:u"
        ), {"u": url_norm}).fetchone()
        prev_hash = row.content_hash if row else None
        if row:
            etag, last_mod = row.etag, row.last_modified

    headers = {"User-Agent": USER_AGENT}
    if etag:
        headers["If-None-Match"] = etag
    if last_mod:
        headers["If-Modified-Since"] = last_mod

    # ---- fetch (streamed, with size cap)
    t0 = time.time()
    resp = requests.get(url_norm, headers=headers, timeout=(5, 15), stream=True, allow_redirects=True)
    status = resp.status_code

    # 304 Not Modified → just bump timestamps/metrics and return
    if status == 304:
        dur_ms = int((time.time() - t0) * 1000)
        with engine.begin() as conn:
            conn.execute(text("""
              INSERT INTO pages(url_norm, url, final_url, status_code, duration_ms, fetched_at)
              VALUES (:un, :u, :fu, 304, :dur, NOW())
              ON CONFLICT (url_norm) DO UPDATE SET
                url=EXCLUDED.url, final_url=EXCLUDED.final_url,
                status_code=EXCLUDED.status_code, duration_ms=EXCLUDED.duration_ms,
                fetched_at=NOW()
            """), {"un": url_norm, "u": url, "fu": str(resp.url), "dur": dur_ms})
        return {"url": url_norm, "status_code": 304, "not_modified": True}

    # Treat 4xx (except 429) as permanent errors in your worker if you add a PermanentTaskError class.
    if 400 <= status < 500 and status != 429:
        # Here we just raise; your worker will retry unless you add a PermanentTaskError branch.
        raise requests.HTTPError(f"HTTP {status} for {url_norm}")

    resp.raise_for_status()

    # ---- Content-type filter
    ctype = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
    if ctype and ALLOWED_TYPES and ctype not in ALLOWED_TYPES:
        raise ValueError(f"unsupported content-type: {ctype}")

    # ---- Body (capped), hash, title/preview
    body = _read_capped(resp, MAX_BYTES)
    dur_ms = int((time.time() - t0) * 1000)
    chash = hashlib.sha256(body).hexdigest()
    title, preview = _title_and_preview(body, ctype)

    # ---- Store body in MinIO (key by content hash)
    ext = "html" if ctype == "text/html" else "txt" if ctype == "text/plain" else "bin"
    key = f"pages/{chash[:2]}/{chash}.{ext}"
    stored_url = put_bytes(body, key, ctype or "application/octet-stream")

    # ---- Upsert metadata; add a version row only if content changed
    changed = (prev_hash != chash)
    with engine.begin() as conn:
        conn.execute(text("""
          INSERT INTO pages(
            url_norm, url, final_url, etag, last_modified, status_code,
            content_type, size_bytes, duration_ms, content_hash, title,
            preview_text, stored_object, fetched_at
          ) VALUES (
            :un, :u, :fu, :e, :lm, :sc,
            :ct, :sz, :dur, :h, :ti,
            :pv, :so, NOW()
          )
          ON CONFLICT (url_norm) DO UPDATE SET
            url=EXCLUDED.url,
            final_url=EXCLUDED.final_url,
            etag=EXCLUDED.etag,
            last_modified=EXCLUDED.last_modified,
            status_code=EXCLUDED.status_code,
            content_type=EXCLUDED.content_type,
            size_bytes=EXCLUDED.size_bytes,
            duration_ms=EXCLUDED.duration_ms,
            content_hash=EXCLUDED.content_hash,
            title=EXCLUDED.title,
            preview_text=EXCLUDED.preview_text,
            stored_object=EXCLUDED.stored_object,
            fetched_at=NOW()
        """), {
            "un": url_norm, "u": url, "fu": str(resp.url),
            "e": resp.headers.get("ETag"),
            "lm": resp.headers.get("Last-Modified"),
            "sc": status, "ct": ctype or None, "sz": len(body),
            "dur": dur_ms, "h": chash, "ti": title, "pv": preview,
            "so": stored_url
        })

        if changed:
            page_id = conn.execute(text("SELECT id FROM pages WHERE url_norm=:u"), {"u": url_norm}).scalar()
            conn.execute(text("""
              INSERT INTO page_versions(page_id, fetched_at, content_hash, stored_object)
              VALUES (:pid, NOW(), :h, :so)
            """), {"pid": page_id, "h": chash, "so": stored_url})

    return {
        "url": url_norm,
        "final_url": str(resp.url),
        "status_code": status,
        "duration_ms": dur_ms,
        "bytes": len(body),
        "content_type": ctype or None,
        "content_hash": chash,
        "title": title,
        "changed": changed,
        "stored_object": stored_url
    }
