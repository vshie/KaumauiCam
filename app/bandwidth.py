"""SQLite-backed YouTube upload bandwidth accounting."""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import Any, Dict, Optional

DB_PATH = os.environ.get("KAUMAUI_STATE_DB", "/app/data/state.db")
_lock = threading.Lock()


def _conn() -> sqlite3.Connection:
    d = os.path.dirname(DB_PATH)
    if d:
        os.makedirs(d, exist_ok=True)
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    with _lock:
        c = _conn()
        try:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS bandwidth (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    bytes INTEGER NOT NULL,
                    session_id TEXT
                )
                """
            )
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS bandwidth_monthly (
                    y INTEGER NOT NULL,
                    m INTEGER NOT NULL,
                    bytes INTEGER NOT NULL,
                    PRIMARY KEY (y, m)
                )
                """
            )
            c.commit()
        finally:
            c.close()


def _month_start_ts(when: Optional[float] = None) -> float:
    lt = time.localtime(when or time.time())
    return time.mktime((lt.tm_year, lt.tm_mon, 1, 0, 0, 0, 0, 0, -1))


def record_delta(delta_bytes: int, session_id: Optional[str] = None) -> None:
    if delta_bytes <= 0:
        return
    with _lock:
        c = _conn()
        try:
            c.execute(
                "INSERT INTO bandwidth (ts, bytes, session_id) VALUES (?, ?, ?)",
                (time.time(), int(delta_bytes), session_id),
            )
            c.commit()
        finally:
            c.close()


def reset_month_manual() -> None:
    """Clear accumulated bytes for the current calendar month."""
    t0 = _month_start_ts()
    with _lock:
        c = _conn()
        try:
            c.execute("DELETE FROM bandwidth WHERE ts >= ?", (t0,))
            c.commit()
        finally:
            c.close()


def _sum_range(t0: float, t1: float) -> int:
    c = _conn()
    try:
        r = c.execute(
            "SELECT COALESCE(SUM(bytes), 0) FROM bandwidth WHERE ts >= ? AND ts < ?",
            (t0, t1),
        ).fetchone()[0]
        return int(r)
    finally:
        c.close()


def status(overhead_pct: float = 0.0, quota_gb: float = 0.0) -> Dict[str, Any]:
    now = time.time()
    lt = time.localtime(now)
    day_start = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, 0, 0, -1))
    month_start = _month_start_ts(now)
    # next month start for range
    if lt.tm_mon == 12:
        next_m = time.mktime((lt.tm_year + 1, 1, 1, 0, 0, 0, 0, 0, -1))
    else:
        next_m = time.mktime((lt.tm_year, lt.tm_mon + 1, 1, 0, 0, 0, 0, 0, -1))
    month_bytes = _sum_range(month_start, next_m)
    day_end = now + 1.0
    day_bytes = _sum_range(day_start, day_end)
    mult = 1.0 + (overhead_pct / 100.0)
    month_adj = int(month_bytes * mult)
    day_adj = int(day_bytes * mult)
    quota_bytes = int(max(0.0, quota_gb) * (1024**3))
    remaining = max(0, quota_bytes - month_adj) if quota_bytes else None
    return {
        "month_bytes": month_bytes,
        "month_bytes_adjusted": month_adj,
        "day_bytes": day_bytes,
        "day_bytes_adjusted": day_adj,
        "quota_bytes": quota_bytes,
        "remaining_bytes": remaining,
        "overhead_pct": overhead_pct,
    }


def session_sum_since(session_start: float, session_id: Optional[str] = None) -> int:
    c = _conn()
    try:
        if session_id:
            r = c.execute(
                "SELECT COALESCE(SUM(bytes), 0) FROM bandwidth WHERE ts >= ? AND session_id = ?",
                (session_start, session_id),
            ).fetchone()[0]
        else:
            r = c.execute(
                "SELECT COALESCE(SUM(bytes), 0) FROM bandwidth WHERE ts >= ?",
                (session_start,),
            ).fetchone()[0]
        return int(r)
    finally:
        c.close()
