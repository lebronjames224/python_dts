import json, requests
from typing import Callable, Dict


TASKS: Dict[str, Callable[[str], None]] = {}

def task(name: str):
    def deco(fn: Callable[[str], None]):
        TASKS[name] = fn
        return fn
    return deco

@task("echo")
def echo(payload: str):
    print(f"[task:echo] {payload}")

@task("crawl_url")
def crawl_url(payload: str):
    # Be forgiving: allow either JSON {"url": "..."} or a raw URL string
    try:
        data = json.loads(payload) if payload else {}
        url = data.get("url") or payload
    except Exception:
        url = (payload or "").strip()
    if not url:
        raise ValueError("payload must include 'url' or be a URL string")

    resp = requests.get(url, timeout=10, headers={"User-Agent": "py-scheduler/1.0"})
    print(resp)
    resp.raise_for_status()
    size = len(resp.content)
    print(f"[task:crawl_url] {url} -> {size} bytes (status {resp.status_code})", flush=True)
    return {"url": url, "status_code": resp.status_code, "bytes": size}
