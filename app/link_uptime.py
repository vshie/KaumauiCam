"""SQLite-backed Starlink/internet link uptime tracking via ping 8.8.8.8.

A daemon thread issues a single ICMP echo request to 8.8.8.8 every
PING_INTERVAL_SECS, recording each result (success + RTT) into the
`link_pings` table. The Streaming UI reads back aggregated buckets so the
operator can see general Starlink uptime even across periods when the
link was actually down -- the broadcast page itself can't load while the
modem is offline, but the data is preserved server-side and surfaces on
the next successful page load. Without this, transient outages just
disappear from the UI's perspective.

We deliberately ping a hard-coded IP (8.8.8.8) rather than a hostname so
DNS failure is reported as "DNS timeouts but link healthy" by other
parts of the system rather than masking actual ICMP loss.
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("kaumaui.link")

DB_PATH = os.environ.get("KAUMAUI_STATE_DB", "/app/data/state.db")
PING_TARGET = "8.8.8.8"
# Probe cadence. 10s gives 8640 rows/day -- trivial for SQLite -- and a
# fast enough signal that a 30-second outage shows up as ~3 missed bars
# rather than getting averaged into a single mostly-green bucket.
PING_INTERVAL_SECS = 10.0
# Per-ping wait for an ICMP reply. Starlink RTT is typically 30-60 ms;
# 3s is well above the worst-case round trip while still keeping the
# probe loop snappy when the link is truly down.
PING_TIMEOUT_SECS = 3
RETENTION_DAYS = 30

_lock = threading.Lock()
_thread: Optional[threading.Thread] = None
_thread_started = False

_state_lock = threading.Lock()
_last_success_ts: Optional[float] = None
_last_failure_ts: Optional[float] = None
_last_rtt_ms: Optional[float] = None
_last_check_ts: Optional[float] = None
_consecutive_fails = 0
_ping_unavailable = False


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
                CREATE TABLE IF NOT EXISTS link_pings (
                    ts REAL NOT NULL,
                    success INTEGER NOT NULL,
                    rtt_ms REAL
                )
                """
            )
            c.execute(
                "CREATE INDEX IF NOT EXISTS link_pings_ts_idx ON link_pings(ts)"
            )
            cutoff = time.time() - RETENTION_DAYS * 24 * 3600
            c.execute("DELETE FROM link_pings WHERE ts < ?", (cutoff,))
            c.commit()
        finally:
            c.close()


# Match `time=12.3 ms` or `time<1 ms` in ping's per-reply line.
_PING_RTT_RE = re.compile(r"time[=<]\s*([0-9.]+)\s*ms")
# Fallback for ping summary line: `rtt min/avg/max/mdev = a/b/c/d ms`.
_PING_AVG_RE = re.compile(r"=\s*[0-9.]+/([0-9.]+)/")


def _ping_once(
    target: str = PING_TARGET, timeout: int = PING_TIMEOUT_SECS
) -> Tuple[bool, Optional[float]]:
    """Run a single ping and return ``(success, rtt_ms)``.

    A non-zero exit from ``ping`` (no reply within ``-W`` seconds) returns
    ``(False, None)``. Any other failure -- ping not installed, raw socket
    permission denied, subprocess hang -- also returns failure rather than
    raising, so the surrounding loop can record the outage and keep going.
    """
    global _ping_unavailable
    try:
        proc = subprocess.run(
            ["ping", "-n", "-q", "-c", "1", "-W", str(timeout), target],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout + 2,
        )
    except FileNotFoundError:
        if not _ping_unavailable:
            _ping_unavailable = True
            logger.error("ping binary not found; uptime tracking will record failures only")
        return False, None
    except subprocess.TimeoutExpired:
        return False, None
    except OSError as e:
        logger.warning("ping subprocess OSError: %s", e)
        return False, None
    if proc.returncode != 0:
        return False, None
    text = proc.stdout.decode("utf-8", errors="replace")
    m = _PING_RTT_RE.search(text)
    if m:
        try:
            return True, float(m.group(1))
        except ValueError:
            return True, None
    m2 = _PING_AVG_RE.search(text)
    if m2:
        try:
            return True, float(m2.group(1))
        except ValueError:
            return True, None
    return True, None


def record_ping(ts: float, success: bool, rtt_ms: Optional[float]) -> None:
    with _lock:
        c = _conn()
        try:
            c.execute(
                "INSERT INTO link_pings (ts, success, rtt_ms) VALUES (?, ?, ?)",
                (ts, 1 if success else 0, rtt_ms if rtt_ms is not None else None),
            )
            c.commit()
        finally:
            c.close()


def _prune(now: float) -> None:
    cutoff = now - RETENTION_DAYS * 24 * 3600
    with _lock:
        c = _conn()
        try:
            c.execute("DELETE FROM link_pings WHERE ts < ?", (cutoff,))
            c.commit()
        finally:
            c.close()


def _ping_loop() -> None:
    global _last_success_ts, _last_failure_ts, _last_rtt_ms
    global _last_check_ts, _consecutive_fails
    last_prune = 0.0
    while True:
        ts = time.time()
        try:
            success, rtt_ms = _ping_once()
        except Exception:
            logger.exception("ping check raised")
            success, rtt_ms = False, None
        try:
            record_ping(ts, success, rtt_ms)
        except Exception:
            logger.exception("link ping write failed")
        with _state_lock:
            _last_check_ts = ts
            if success:
                _last_success_ts = ts
                _last_rtt_ms = rtt_ms
                _consecutive_fails = 0
            else:
                _last_failure_ts = ts
                _consecutive_fails += 1
        if ts - last_prune > 3600:
            try:
                _prune(ts)
            except Exception:
                logger.exception("link prune failed")
            last_prune = ts
        time.sleep(PING_INTERVAL_SECS)


def start() -> None:
    """Idempotent start of the background ping thread."""
    global _thread, _thread_started
    if _thread_started:
        return
    _thread_started = True
    _thread = threading.Thread(target=_ping_loop, daemon=True, name="link-uptime")
    _thread.start()


def _summary_window(t0: float, t1: float) -> Dict[str, Any]:
    c = _conn()
    try:
        row = c.execute(
            "SELECT COUNT(*) AS n, "
            "       COALESCE(SUM(success), 0) AS ok, "
            "       AVG(CASE WHEN success = 1 THEN rtt_ms END) AS avg_rtt "
            "FROM link_pings WHERE ts >= ? AND ts < ?",
            (t0, t1),
        ).fetchone()
        n = int(row["n"] or 0)
        ok = int(row["ok"] or 0)
        avg_rtt = row["avg_rtt"]
        return {
            "from": t0,
            "to": t1,
            "checks": n,
            "successes": ok,
            "uptime_pct": (100.0 * ok / n) if n else None,
            "avg_rtt_ms": float(avg_rtt) if avg_rtt is not None else None,
        }
    finally:
        c.close()


def status() -> Dict[str, Any]:
    """Current link state plus 24-hour and 1-hour summaries."""
    now = time.time()
    summary_24h = _summary_window(now - 24 * 3600, now)
    summary_1h = _summary_window(now - 3600, now)
    with _state_lock:
        last_check = _last_check_ts
        last_success = _last_success_ts
        last_failure = _last_failure_ts
        last_rtt = _last_rtt_ms
        fails = _consecutive_fails
        unavailable = _ping_unavailable
    up = (fails == 0) and (last_success is not None)
    return {
        "target": PING_TARGET,
        "interval_secs": PING_INTERVAL_SECS,
        "now": now,
        "last_check_ts": last_check,
        "last_success_ts": last_success,
        "last_failure_ts": last_failure,
        "last_rtt_ms": last_rtt,
        "consecutive_failures": fails,
        "up": up,
        "ping_unavailable": unavailable,
        "summary_24h": summary_24h,
        "summary_1h": summary_1h,
    }


def buckets(since_ts: float, bucket_secs: int) -> List[Dict[str, Any]]:
    """Aggregated uptime buckets between ``since_ts`` and now.

    Returns one entry per bucket, oldest first. Empty buckets (no checks
    recorded -- e.g. before pings ever started, or while the container
    itself was down) are returned with ``checks=0`` so the UI can render
    gaps explicitly rather than misrepresenting them as uptime.
    """
    bucket_secs = max(30, int(bucket_secs))
    now = time.time()
    if since_ts >= now:
        return []
    c = _conn()
    try:
        anchor = int(since_ts // bucket_secs) * bucket_secs
        last_anchor = int(now // bucket_secs) * bucket_secs
        rows = c.execute(
            "SELECT CAST((ts - ?) / ? AS INTEGER) AS b, "
            "       COUNT(*) AS n, "
            "       COALESCE(SUM(success), 0) AS ok, "
            "       AVG(CASE WHEN success = 1 THEN rtt_ms END) AS avg_rtt "
            "FROM link_pings WHERE ts >= ? AND ts < ? "
            "GROUP BY b ORDER BY b",
            (anchor, bucket_secs, anchor, last_anchor + bucket_secs),
        ).fetchall()
        by_b: Dict[int, sqlite3.Row] = {int(r["b"]): r for r in rows}
        out: List[Dict[str, Any]] = []
        total = (last_anchor - anchor) // bucket_secs + 1
        for i in range(total):
            r = by_b.get(i)
            n = int(r["n"]) if r else 0
            ok = int(r["ok"]) if r else 0
            rtt = float(r["avg_rtt"]) if r and r["avg_rtt"] is not None else None
            out.append(
                {
                    "ts": anchor + i * bucket_secs,
                    "checks": n,
                    "successes": ok,
                    "uptime_pct": (100.0 * ok / n) if n else None,
                    "avg_rtt_ms": rtt,
                }
            )
        return out
    finally:
        c.close()
