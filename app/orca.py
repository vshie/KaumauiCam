"""Orca camera data logger.

Polls the camera's HTTP JSON endpoint (default
``http://192.168.0.142:5000/data``) every ``orca_interval_secs`` and
appends one row per poll to ``/app/data/orca.csv``.

The camera's ``/data`` payload is treated as the schema: every scalar
(and flattened nested) field becomes a CSV column. ``timestamp_iso`` is
always column 1; a ``payload_json`` column at the end keeps the raw
body so later fields still survive if the camera adds keys after the
header was written.

The CSV is cumulative (append-only) and lives on the bind-mounted
``/app/data`` volume so it survives container rebuilds.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import requests

logger = logging.getLogger(__name__)

DEFAULT_CSV_PATH = os.environ.get("WAILOA_ORCA_CSV", "/app/data/orca.csv")
DEFAULT_URL = "http://192.168.0.142:5000/data"
HTTP_TIMEOUT_S = 5.0
MIN_INTERVAL_S = 5.0
MAX_INTERVAL_S = 3600.0
DEFAULT_INTERVAL_S = 60.0

META_COLS: Tuple[str, ...] = ("timestamp_iso", "timestamp_epoch", "payload_json")

_state_lock = threading.Lock()
_thread: Optional[threading.Thread] = None
_stop = threading.Event()
_wake = threading.Event()
_get_cfg: Optional[Callable[[], Dict[str, Any]]] = None
_last_sample: Dict[str, Any] = {}
_last_write_ts: float = 0.0
_last_error: Optional[str] = None
_last_error_ts: float = 0.0
_rows_logged: int = 0
_columns: List[str] = []


def _csv_path() -> str:
    return DEFAULT_CSV_PATH


def _ensure_dir() -> None:
    d = os.path.dirname(_csv_path())
    if d:
        os.makedirs(d, exist_ok=True)


def _col(prefix: str, name: str) -> str:
    raw = re.sub(r"[^A-Za-z0-9]+", "_", str(name)).strip("_").lower() or "field"
    key = raw if not prefix else f"{prefix}_{raw}"
    if key in META_COLS:
        key = "cam_" + key
    return key


def flatten_json(obj: Any, prefix: str = "") -> Dict[str, str]:
    """Turn nested JSON into a flat str->str map suitable for CSV cells."""
    out: Dict[str, str] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(flatten_json(v, _col(prefix, k)))
        return out
    if isinstance(obj, list):
        if not prefix:
            prefix = "item"
        if not obj:
            out[prefix] = ""
            return out
        if all(not isinstance(x, (dict, list)) for x in obj):
            out[prefix] = ",".join("" if x is None else str(x) for x in obj)
            return out
        for i, v in enumerate(obj):
            out.update(flatten_json(v, _col(prefix, str(i))))
        return out
    if not prefix:
        prefix = "value"
    if obj is None:
        out[prefix] = ""
    elif isinstance(obj, bool):
        out[prefix] = "1" if obj else "0"
    else:
        out[prefix] = str(obj)
    return out


def normalize_url(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return DEFAULT_URL
    if "://" not in s:
        s = "http://" + s
    parsed = urlparse(s)
    if not parsed.netloc:
        return DEFAULT_URL
    path = parsed.path or "/data"
    return urlunparse((parsed.scheme or "http", parsed.netloc, path, "", parsed.query, ""))


def _iso_utc(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _fetch(url: str) -> Tuple[Optional[Any], Optional[str], Optional[str]]:
    """GET the camera JSON. Returns ``(parsed, raw_text, error)``."""
    try:
        r = requests.get(url, timeout=HTTP_TIMEOUT_S)
    except requests.RequestException as e:
        return None, None, f"request: {e}"
    body = r.text or ""
    if r.status_code != 200:
        snippet = body.strip().replace("\n", " ")[:180]
        return None, body, f"HTTP {r.status_code}" + (f" ({snippet})" if snippet else "")
    try:
        return r.json(), body, None
    except ValueError:
        return None, body, "response is not JSON"


def _read_header(path: str) -> List[str]:
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return []
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            row = next(reader, None)
            return [c.strip() for c in row] if row else []
    except OSError:
        return []


def _header_for(flat: Dict[str, str], existing: List[str]) -> List[str]:
    extra = sorted(k for k in flat.keys() if k not in META_COLS)
    if not existing:
        return ["timestamp_iso", "timestamp_epoch", *extra, "payload_json"]
    header = list(existing)
    # Keep payload_json last; insert newly seen keys just before it.
    insert_at = header.index("payload_json") if "payload_json" in header else len(header)
    known = set(header)
    for k in extra:
        if k not in known:
            header.insert(insert_at, k)
            insert_at += 1
            known.add(k)
    if "payload_json" not in known:
        header.append("payload_json")
    return header


def _rewrite_header(path: str, new_header: List[str]) -> None:
    """Expand an existing CSV with newly discovered columns."""
    rows: List[Dict[str, str]] = []
    with open(path, "r", encoding="utf-8", newline="") as inf:
        reader = csv.DictReader(inf)
        for row in reader:
            rows.append(row)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as outf:
        w = csv.DictWriter(outf, fieldnames=new_header, extrasaction="ignore")
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in new_header})
    os.replace(tmp, path)


def _append_row(flat: Dict[str, str], ts: float, raw_json: str) -> List[str]:
    path = _csv_path()
    _ensure_dir()
    existing = _read_header(path)
    header = _header_for(flat, existing)
    if existing and header != existing:
        _rewrite_header(path, header)
        existing = header
    new_file = not os.path.exists(path) or os.path.getsize(path) == 0
    row = {
        "timestamp_iso": _iso_utc(ts),
        "timestamp_epoch": f"{ts:.0f}",
        **flat,
        "payload_json": raw_json,
    }
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(header)
        w.writerow([row.get(k, "") for k in header])
    return header


def _row_count(path: str) -> int:
    if not os.path.isfile(path):
        return 0
    try:
        with open(path, "rb") as f:
            n = 0
            for _ in f:
                n += 1
            return max(0, n - 1)
    except OSError:
        return 0


def _file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _resolve_cfg() -> Dict[str, Any]:
    if _get_cfg is None:
        return {}
    try:
        return _get_cfg() or {}
    except Exception:
        logger.exception("orca: cfg load failed")
        return {}


def _interval_secs(cfg: Dict[str, Any]) -> float:
    raw = cfg.get("orca_interval_secs", DEFAULT_INTERVAL_S)
    try:
        v = float(raw)
    except (TypeError, ValueError):
        v = DEFAULT_INTERVAL_S
    return max(MIN_INTERVAL_S, min(MAX_INTERVAL_S, v))


def _url(cfg: Dict[str, Any]) -> str:
    return normalize_url(str(cfg.get("orca_url") or DEFAULT_URL))


def _enabled(cfg: Dict[str, Any]) -> bool:
    return bool(cfg.get("orca_enabled", True))


def _compact_json(obj: Any, fallback: str = "") -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, default=str, separators=(",", ":"))
    except (TypeError, ValueError):
        return fallback


def _sample(url: str) -> Tuple[Dict[str, str], str, Optional[str]]:
    parsed, raw_text, err = _fetch(url)
    if err:
        return {}, raw_text or "", err
    flat = flatten_json(parsed)
    raw_json = _compact_json(parsed, fallback=(raw_text or "").strip())
    return flat, raw_json, None


def _loop() -> None:
    global _last_sample, _last_write_ts, _last_error, _last_error_ts, _rows_logged, _columns
    _rows_logged = _row_count(_csv_path())
    _columns = _read_header(_csv_path())
    while not _stop.is_set():
        cfg = _resolve_cfg()
        interval = _interval_secs(cfg)
        if not _enabled(cfg):
            _wake.wait(timeout=min(interval, 30.0))
            _wake.clear()
            continue

        url = _url(cfg)
        ts = time.time()
        try:
            flat, raw_json, err = _sample(url)
        except Exception as e:
            logger.exception("orca: sample exception")
            with _state_lock:
                _last_error = f"sample: {e}"
                _last_error_ts = ts
            _wake.wait(timeout=interval)
            _wake.clear()
            continue

        if err:
            with _state_lock:
                _last_error = f"{url}: {err}"
                _last_error_ts = ts
            _wake.wait(timeout=interval)
            _wake.clear()
            continue

        try:
            header = _append_row(flat, ts, raw_json)
            with _state_lock:
                _last_sample = {"ts": ts, **flat}
                _last_write_ts = ts
                _rows_logged += 1
                _columns = header
                _last_error = None
        except OSError as e:
            logger.exception("orca: csv write failed")
            with _state_lock:
                _last_error = f"write: {e}"
                _last_error_ts = ts

        _wake.wait(timeout=interval)
        _wake.clear()


def start(get_cfg: Callable[[], Dict[str, Any]]) -> None:
    """Spawn the poller thread. Idempotent — second call is a no-op."""
    global _thread, _get_cfg
    _get_cfg = get_cfg
    if _thread and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="orca-logger")
    _thread.start()
    logger.info("orca logger thread started -> %s (%s)", _csv_path(), DEFAULT_URL)


def stop() -> None:
    _stop.set()
    _wake.set()


def poke() -> None:
    """Force the next iteration immediately (used after Settings save)."""
    _wake.set()


def status() -> Dict[str, Any]:
    path = _csv_path()
    cfg = _resolve_cfg()
    with _state_lock:
        last = dict(_last_sample) if _last_sample else None
        last_write = _last_write_ts
        err = _last_error
        err_ts = _last_error_ts
        rows = _rows_logged
        cols = list(_columns)
    return {
        "now": time.time(),
        "enabled": _enabled(cfg),
        "url": _url(cfg),
        "interval_secs": _interval_secs(cfg),
        "csv_path": path,
        "csv_size_bytes": _file_size(path),
        "rows_logged": rows,
        "last_sample": last,
        "last_write_ts": last_write or None,
        "last_error": err,
        "last_error_ts": err_ts or None,
        "columns": cols or _read_header(path),
    }


def fetch_live(url: Optional[str] = None) -> Dict[str, Any]:
    """One-shot poll for the Settings Refresh button."""
    cfg = _resolve_cfg()
    u = normalize_url(url or _url(cfg))
    ts = time.time()
    flat, raw_json, err = _sample(u)
    return {
        "ts": ts,
        "url": u,
        "sample": flat,
        "payload_json": raw_json,
        "ok": err is None,
        "error": err,
    }


def delete_csv() -> Dict[str, Any]:
    global _rows_logged, _last_sample, _last_write_ts, _columns
    path = _csv_path()
    deleted = False
    try:
        if os.path.isfile(path):
            os.remove(path)
            deleted = True
    except OSError as e:
        logger.warning("orca: delete csv failed: %s", e)
        return {"ok": False, "error": str(e)}
    with _state_lock:
        _rows_logged = 0
        _last_sample = {}
        _last_write_ts = 0.0
        _columns = []
    return {"ok": True, "deleted": deleted, "path": path}


def csv_path() -> str:
    return _csv_path()


def csv_preview(max_rows: int = 5) -> str:
    path = _csv_path()
    if not os.path.isfile(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except OSError:
        return ""
    if not lines:
        return ""
    header = lines[0]
    data = lines[1:]
    tail = data[-max_rows:] if max_rows > 0 else []
    out = io.StringIO()
    out.write(header)
    out.writelines(tail)
    return out.getvalue()
