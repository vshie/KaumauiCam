"""SQLite-backed YouTube upload bandwidth accounting and session log."""

from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional

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
            # Per-YouTube-session lifecycle log. We track every ffmpeg invocation
            # so the UI can show the actual broadcast history (when a session
            # started, how long it lasted, why it ended, how many bytes went
            # out) instead of just per-tick byte deltas. `end_reason` is one of:
            #   "stopped"   -- explicit stop() call (schedule end / user click)
            #   "stalled"   -- watchdog killed a wedged ffmpeg (no RTMP bytes)
            #   "died"      -- ffmpeg exited on its own (network drop, RTSP
            #                  hiccup, RTMP push refused). exit_code captures
            #                  ffmpeg's return code; last_stderr captures the
            #                  final ~5 lines of ffmpeg stderr for diagnosis.
            #   "orphaned"  -- ended_ts was NULL at process boot, meaning the
            #                  prior Python interpreter died before recording
            #                  a clean end_reason. See reconciliation below.
            #   NULL        -- session is currently running (single row max).
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS yt_sessions (
                    session_id TEXT PRIMARY KEY,
                    started_ts REAL NOT NULL,
                    ended_ts REAL,
                    bytes INTEGER NOT NULL DEFAULT 0,
                    exit_code INTEGER,
                    end_reason TEXT,
                    last_stderr TEXT
                )
                """
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS yt_sessions_started_idx ON yt_sessions(started_ts DESC)"
            )
            # Boot-time reconciliation. If we see any rows with ended_ts NULL,
            # they're orphans from a previous interpreter that died before
            # firing a session-end event (container kill / OOM / SIGKILL).
            # The current process is starting fresh so by definition none of
            # those ffmpegs are still running. Close them out with
            # end_reason="orphaned" and roll up their bytes from the per-tick
            # bandwidth table so the broadcast-history UI doesn't show them
            # as still-Running indefinitely.
            now = time.time()
            orphans = c.execute(
                "SELECT session_id, started_ts FROM yt_sessions WHERE ended_ts IS NULL"
            ).fetchall()
            for row in orphans:
                sid = row[0]
                started = float(row[1] or now)
                bytes_row = c.execute(
                    "SELECT COALESCE(SUM(bytes), 0) FROM bandwidth WHERE session_id = ?",
                    (sid,),
                ).fetchone()
                total_bytes = int(bytes_row[0]) if bytes_row else 0
                # Use the last bandwidth tick as a better ended_ts estimate
                # than "now": for an old crashed session, "now" could be
                # weeks later and would inflate the displayed duration.
                last_tick = c.execute(
                    "SELECT COALESCE(MAX(ts), ?) FROM bandwidth WHERE session_id = ?",
                    (started, sid),
                ).fetchone()
                ended_ts = float(last_tick[0]) if last_tick and last_tick[0] else started
                c.execute(
                    "UPDATE yt_sessions SET ended_ts = ?, bytes = ?, end_reason = ? "
                    "WHERE session_id = ? AND ended_ts IS NULL",
                    (ended_ts, total_bytes, "orphaned", sid),
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


def record_session_start(session_id: str, started_ts: float) -> None:
    """Open a new yt_sessions row when ffmpeg is launched. INSERT OR REPLACE
    so a re-used session_id (shouldn't happen with uuid4 prefixes, but cheap
    insurance) cleanly resets the row instead of failing with PRIMARY KEY
    conflict mid-stream."""
    if not session_id:
        return
    with _lock:
        c = _conn()
        try:
            c.execute(
                "INSERT OR REPLACE INTO yt_sessions (session_id, started_ts, bytes) VALUES (?, ?, 0)",
                (session_id, started_ts),
            )
            c.commit()
        finally:
            c.close()


def record_session_end(
    session_id: str,
    ended_ts: float,
    exit_code: Optional[int],
    end_reason: str,
    last_stderr: Optional[str],
) -> None:
    """Close out a yt_sessions row. We re-aggregate the session's byte total
    from the per-tick `bandwidth` rows here rather than tracking a running
    counter on the row, so the final number always matches the bandwidth
    accounting (which is the source of truth for quota math)."""
    if not session_id:
        return
    with _lock:
        c = _conn()
        try:
            r = c.execute(
                "SELECT COALESCE(SUM(bytes), 0) FROM bandwidth WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            total_bytes = int(r[0]) if r else 0
            c.execute(
                "UPDATE yt_sessions SET ended_ts = ?, bytes = ?, exit_code = ?, end_reason = ?, last_stderr = ? WHERE session_id = ?",
                (ended_ts, total_bytes, exit_code, end_reason, last_stderr, session_id),
            )
            c.commit()
        finally:
            c.close()


def recent_sessions(limit: int = 50, since_ts: Optional[float] = None) -> List[Dict[str, Any]]:
    """Return the most recent YouTube sessions, newest first. Used by the
    /api/stream/sessions endpoint and the UI's broadcast history table.
    `since_ts` filters to sessions started at or after that Unix timestamp.

    For sessions that are still running (ended_ts IS NULL), the `bytes`
    column on the row is whatever record_session_start() wrote on insert,
    which is always 0 -- record_session_end() is the only writer that
    rolls up the per-tick bandwidth deltas into the row total. To avoid
    showing "0 B" in the UI for the currently-live broadcast, we
    re-aggregate `bytes` from the bandwidth table at read time for any
    row without a recorded ended_ts. Cheap because the live row is
    indexed by session_id and contains at most a few thousand ticks.
    """
    limit = max(1, min(int(limit), 500))
    c = _conn()
    try:
        if since_ts is not None:
            rows = c.execute(
                "SELECT session_id, started_ts, ended_ts, bytes, exit_code, end_reason, last_stderr "
                "FROM yt_sessions WHERE started_ts >= ? ORDER BY started_ts DESC LIMIT ?",
                (float(since_ts), limit),
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT session_id, started_ts, ended_ts, bytes, exit_code, end_reason, last_stderr "
                "FROM yt_sessions ORDER BY started_ts DESC LIMIT ?",
                (limit,),
            ).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            if d.get("ended_ts") is None:
                live = c.execute(
                    "SELECT COALESCE(SUM(bytes), 0) FROM bandwidth WHERE session_id = ?",
                    (d["session_id"],),
                ).fetchone()
                if live is not None:
                    d["bytes"] = int(live[0] or 0)
            out.append(d)
        return out
    finally:
        c.close()


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
