"""NOAA tide logger for Hilo Bay (CO-OPS station 1617760), next to the
Wailoa River mouth.

Polls NOAA's public CO-OPS data API (no auth) every 15 minutes on a
background thread:

- Observed water level (6-minute samples, metres above MLLW) is merged
  into ``/app/data/tide.csv``, the source of truth for the Live tab's
  observed line. The first poll after a process start (or a CSV delete)
  backfills ``BACKFILL_SECS``; later polls re-fetch from an hour before
  our newest sample so NOAA's revisions of preliminary values land too.
- Predicted tide (6-minute curve) and the predicted highs/lows are kept
  in memory only: they're deterministic and cheap to re-fetch.

``latest()`` feeds ``tide_overlay``, which puts the current level, its
trend and the next high/low on the camera's video.
"""

from __future__ import annotations

import calendar
import csv
import io
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# --- Endpoint ---------------------------------------------------------------
NOAA_API = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
STATION = os.environ.get("WAILOA_TIDE_STATION", "1617760")
DATUM = "MLLW"
UNIT = "m"
# NOAA asks API users to identify themselves with ``application``.
APPLICATION = "WailoaCam"
HTTP_TIMEOUT_S = 15.0

# --- Poll cadence ----------------------------------------------------------
# NOAA publishes a new observed sample every 6 minutes; polling faster
# than that only re-fetches the same rows.
MIN_INTERVAL_S = 360.0
MAX_INTERVAL_S = 3600.0
DEFAULT_INTERVAL_S = 900.0
RETRY_BACKOFF_S = 300.0

# --- Storage ---------------------------------------------------------------
CSV_PATH = os.environ.get("WAILOA_TIDE_CSV", "/app/data/tide.csv")
CSV_HEADER: Tuple[str, ...] = ("timestamp_iso", "timestamp_epoch", "water_level_m", "quality")

# --- Windows ---------------------------------------------------------------
BACKFILL_SECS = 7 * 24 * 3600
# Re-fetch this far behind our newest sample on every poll. NOAA marks
# fresh samples preliminary (``q: "p"``) and occasionally revises them.
REFETCH_SECS = 3600
# The Live tab charts observed + predicted over [now - 48 h, now + 24 h].
OBSERVED_WINDOW_SECS = 48 * 3600
FORECAST_SECS = 24 * 3600
# High/low events are fetched a little past the chart so the overlay's
# "next high/low" is never empty near the end of the forecast window.
EVENTS_AHEAD_SECS = 36 * 3600

# Trend = mean of the last ``TREND_LOOKBACK_SECS`` vs the window before;
# changes smaller than ``TREND_DEADBAND_M`` (slack water) show no trend.
TREND_LOOKBACK_SECS = 30 * 60
TREND_DEADBAND_M = 0.01
# An observed level older than this is not shown as "current" (station
# outage); the chart still draws whatever history there is.
STALE_SECS = 2 * 3600

# --- Module state ----------------------------------------------------------
_state_lock = threading.Lock()
_thread: Optional[threading.Thread] = None
_stop = threading.Event()
_wake = threading.Event()
_get_cfg: Optional[Callable[[], Dict[str, Any]]] = None

_last_fetched_ts: float = 0.0
_last_error: Optional[str] = None
_last_error_ts: float = 0.0
_station_name: Optional[str] = None
_backfill_done = False
# [[ms, m], ...] and [{"type": "H"|"L", "ms", "m"}, ...] from the last poll.
_predicted: List[List[float]] = []
_events: List[Dict[str, Any]] = []
# Called (no args) after a successful poll; see ``set_on_new_data``.
_on_new_data: Optional[Callable[[], None]] = None
# Parsed CSV memo for ``_observed()``, keyed by (mtime_ns, size).
_observed_memo: Tuple[Tuple[int, int], List[List[float]]] = ((0, 0), [])


# --- API calls -------------------------------------------------------------


def _noaa_date(secs: float) -> str:
    return time.strftime("%Y%m%d %H:%M", time.gmtime(secs))


def _parse_noaa_time(t: str) -> int:
    """NOAA ``"YYYY-MM-DD HH:MM"`` (we always ask for GMT) -> epoch secs."""
    return calendar.timegm(time.strptime(t, "%Y-%m-%d %H:%M"))


def _fetch(product: str, begin_secs: float, hours: int, **extra: str) -> Dict[str, Any]:
    params = {
        "station": STATION,
        "product": product,
        "datum": DATUM,
        "time_zone": "gmt",
        "units": "metric",
        "format": "json",
        "application": APPLICATION,
        "begin_date": _noaa_date(begin_secs),
        "range": str(int(hours)),
        **extra,
    }
    r = requests.get(NOAA_API, params=params, timeout=HTTP_TIMEOUT_S)
    if r.status_code != 200:
        raise RuntimeError(f"{product} HTTP {r.status_code}: {r.text.strip()[:200]}")
    body = r.json() or {}
    err = body.get("error")
    if err:
        raise RuntimeError(f"{product}: {err.get('message') or err}")
    return body


def _hours_between(begin_secs: float, end_secs: float) -> int:
    return max(1, int((end_secs - begin_secs + 3599) // 3600))


def _fetch_observed(begin_secs: float, end_secs: float) -> Tuple[Dict[int, Tuple[float, str]], Optional[str]]:
    """``({epoch_secs: (level_m, quality)}, station_name)``; empty values skipped."""
    body = _fetch("water_level", begin_secs, _hours_between(begin_secs, end_secs))
    out: Dict[int, Tuple[float, str]] = {}
    for row in body.get("data") or []:
        try:
            out[_parse_noaa_time(row["t"])] = (float(row["v"]), str(row.get("q") or ""))
        except (KeyError, TypeError, ValueError):
            continue
    name = (body.get("metadata") or {}).get("name")
    return out, name


def _fetch_predicted(begin_secs: float, end_secs: float) -> List[List[float]]:
    body = _fetch("predictions", begin_secs, _hours_between(begin_secs, end_secs), interval="6")
    out: List[List[float]] = []
    for row in body.get("predictions") or []:
        try:
            out.append([_parse_noaa_time(row["t"]) * 1000, float(row["v"])])
        except (KeyError, TypeError, ValueError):
            continue
    out.sort()
    return out


def _fetch_events(begin_secs: float, end_secs: float) -> List[Dict[str, Any]]:
    body = _fetch("predictions", begin_secs, _hours_between(begin_secs, end_secs), interval="hilo")
    out: List[Dict[str, Any]] = []
    for row in body.get("predictions") or []:
        kind = str(row.get("type") or "").upper()
        if kind not in ("H", "L"):
            continue
        try:
            out.append({"type": kind, "ms": _parse_noaa_time(row["t"]) * 1000, "m": float(row["v"])})
        except (KeyError, TypeError, ValueError):
            continue
    out.sort(key=lambda e: e["ms"])
    return out


# --- CSV persistence -------------------------------------------------------


def _iso_hst(secs: int) -> str:
    """HST is a stable UTC-10 offset (Hawaii doesn't observe DST)."""
    return time.strftime("%Y-%m-%dT%H:%M:%S-10:00", time.gmtime(secs - 10 * 3600))


def _read_csv() -> Dict[int, List[str]]:
    """``{epoch_secs: row_cells}`` for every sample in the CSV."""
    rows: Dict[int, List[str]] = {}
    if not os.path.isfile(CSV_PATH):
        return rows
    try:
        with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) < 3:
                    continue
                try:
                    rows[int(row[1].strip())] = row
                except ValueError:
                    continue
    except OSError:
        return {}
    return rows


def _row(secs: int, level: float, quality: str) -> List[str]:
    return [_iso_hst(secs), str(secs), f"{level:.3f}", quality]


def _merge_rows(samples: Dict[int, Tuple[float, str]]) -> int:
    """Merge samples into ``tide.csv``. Returns rows added or revised.

    New samples newer than everything on disk (the normal case) are
    appended; if a backfill found older ones or NOAA revised a value we
    already have, the file is rewritten in time order instead.
    """
    have = _read_csv()
    new: Dict[int, List[str]] = {}
    revised: Dict[int, List[str]] = {}
    for secs in sorted(samples):
        level, quality = samples[secs]
        row = _row(secs, level, quality)
        old = have.get(secs)
        if old is None:
            new[secs] = row
        elif old[2:4] != row[2:4]:
            revised[secs] = row
    if not new and not revised:
        return 0
    d = os.path.dirname(CSV_PATH)
    if d:
        os.makedirs(d, exist_ok=True)
    if revised or (have and new and min(new) < max(have)):
        merged = {**have, **new, **revised}
        tmp = CSV_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)
            for secs in sorted(merged):
                writer.writerow(merged[secs])
        os.replace(tmp, CSV_PATH)
    else:
        new_file = not os.path.isfile(CSV_PATH) or os.path.getsize(CSV_PATH) == 0
        with open(CSV_PATH, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            if new_file:
                writer.writerow(CSV_HEADER)
            for secs in sorted(new):
                writer.writerow(new[secs])
    return len(new) + len(revised)


def _observed() -> List[List[float]]:
    """Every observed sample in the CSV as ``[[ms, m], ...]``, time order.

    Memoized on the file's mtime/size: the Live tab and the overlay read
    this far more often than the CSV changes.
    """
    global _observed_memo
    try:
        st = os.stat(CSV_PATH)
        key = (st.st_mtime_ns, st.st_size)
    except OSError:
        key = (0, 0)
    if key == _observed_memo[0]:
        return _observed_memo[1]
    rows = _read_csv()
    points: List[List[float]] = []
    for secs in sorted(rows):
        try:
            points.append([secs * 1000, float(rows[secs][2])])
        except ValueError:
            continue
    _observed_memo = (key, points)
    return points


def _row_count() -> int:
    if not os.path.isfile(CSV_PATH):
        return 0
    try:
        with open(CSV_PATH, "rb") as f:
            return max(0, sum(1 for _ in f) - 1)
    except OSError:
        return 0


def _file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


# --- Poll loop -------------------------------------------------------------


def _resolve_cfg() -> Dict[str, Any]:
    if _get_cfg is None:
        return {}
    try:
        return _get_cfg() or {}
    except Exception:
        logger.exception("tide: cfg load failed")
        return {}


def _interval_secs(cfg: Dict[str, Any]) -> float:
    try:
        v = float(cfg.get("tide_interval_secs", DEFAULT_INTERVAL_S))
    except (TypeError, ValueError):
        v = DEFAULT_INTERVAL_S
    return max(MIN_INTERVAL_S, min(MAX_INTERVAL_S, v))


def _enabled(cfg: Dict[str, Any]) -> bool:
    return bool(cfg.get("tide_enabled", True))


def _poll_once() -> Tuple[bool, str]:
    """One fetch of observed + predicted + high/low. Returns (ok, message).

    Observed samples are merged before the prediction fetches, so a
    prediction failure still keeps the CSV current. On any failure the
    CSV and the cached predictions are left as they were.
    """
    global _last_fetched_ts, _last_error, _last_error_ts, _station_name
    global _backfill_done, _predicted, _events
    now_s = time.time()
    obs = _observed()
    backfill = not obs or not _backfill_done
    if backfill:
        since = now_s - BACKFILL_SECS
    else:
        since = max(obs[-1][0] / 1000.0 - REFETCH_SECS, now_s - BACKFILL_SECS)
    try:
        samples, name = _fetch_observed(since, now_s)
    except Exception as e:
        return False, f"observed: {e}"
    try:
        changed = _merge_rows(samples)
    except OSError as e:
        return False, f"csv merge: {e}"
    _backfill_done = True
    try:
        predicted = _fetch_predicted(now_s - OBSERVED_WINDOW_SECS, now_s + FORECAST_SECS)
        events = _fetch_events(now_s - OBSERVED_WINDOW_SECS, now_s + EVENTS_AHEAD_SECS)
    except Exception as e:
        return False, f"predictions: {e}"
    with _state_lock:
        _predicted = predicted
        _events = events
        _station_name = name or _station_name
        _last_fetched_ts = time.time()
        _last_error = None
        _last_error_ts = 0.0
    if _on_new_data is not None:
        try:
            _on_new_data()
        except Exception:
            logger.exception("tide: on_new_data hook failed")
    kind = "backfill" if backfill else "incremental"
    return True, f"ok ({kind}) · {changed} new/revised row{'' if changed == 1 else 's'}"


def _loop() -> None:
    global _last_error, _last_error_ts
    while not _stop.is_set():
        cfg = _resolve_cfg()
        if not _enabled(cfg):
            _wake.wait(timeout=30.0)
            _wake.clear()
            continue
        try:
            ok, msg = _poll_once()
        except Exception as e:
            logger.exception("tide: poll exception")
            ok, msg = False, str(e)
        if ok:
            logger.info("tide: %s", msg)
            wait = _interval_secs(cfg)
        else:
            logger.warning("tide: %s", msg)
            with _state_lock:
                _last_error = msg
                _last_error_ts = time.time()
            wait = min(RETRY_BACKOFF_S, _interval_secs(cfg))
        _wake.wait(timeout=wait)
        _wake.clear()


# --- Public API ------------------------------------------------------------


def start(get_cfg: Callable[[], Dict[str, Any]]) -> None:
    """Spawn the poller. Idempotent."""
    global _thread, _get_cfg
    _get_cfg = get_cfg
    if _thread and _thread.is_alive():
        return
    _stop.clear()
    _thread = threading.Thread(target=_loop, daemon=True, name="tide-poller")
    _thread.start()
    logger.info("tide poller thread started (station=%s csv=%s)", STATION, CSV_PATH)


def stop() -> None:
    _stop.set()
    _wake.set()


def poke() -> None:
    """Force an immediate poll (used after Settings save)."""
    _wake.set()


def set_on_new_data(fn: Optional[Callable[[], None]]) -> None:
    """Register a hook run on the poller thread after each successful poll."""
    global _on_new_data
    _on_new_data = fn


def latest() -> Dict[str, Any]:
    """Current tide for the video overlay.

    ``{"level_m", "ts", "trend", "stale", "next_event"}``: ``ts`` is the
    newest observed sample (epoch secs), ``trend`` is ``"rising"``,
    ``"falling"`` or None (slack / not enough data), ``next_event`` is the
    next predicted high or low after now as ``{"type", "level_m", "ts"}``.
    """
    obs = _observed()
    now_s = time.time()
    with _state_lock:
        events = list(_events)
    nxt = next((e for e in events if e["ms"] / 1000.0 > now_s), None)
    out: Dict[str, Any] = {
        "level_m": None,
        "ts": None,
        "trend": None,
        "stale": True,
        "next_event": (
            {"type": nxt["type"], "level_m": nxt["m"], "ts": nxt["ms"] / 1000.0} if nxt else None
        ),
    }
    if not obs:
        return out
    t_ms, level = obs[-1]
    out.update(level_m=level, ts=t_ms / 1000.0, stale=now_s - t_ms / 1000.0 > STALE_SECS)
    # Mean of the last TREND_LOOKBACK_SECS vs the window before it: single
    # 6-minute samples at Hilo jump several cm with wave action.
    win_ms = TREND_LOOKBACK_SECS * 1000
    recent = [v for pt_ms, v in obs if t_ms - pt_ms < win_ms]
    before = [v for pt_ms, v in obs if win_ms <= t_ms - pt_ms < 2 * win_ms]
    if recent and before:
        delta = sum(recent) / len(recent) - sum(before) / len(before)
        if abs(delta) >= TREND_DEADBAND_M:
            out["trend"] = "rising" if delta > 0 else "falling"
    return out


def series() -> Dict[str, Any]:
    """Observed (last 48 h), predicted (-48 h..+24 h) and highs/lows for the Live tab."""
    now = time.time()
    start_ms = (now - OBSERVED_WINDOW_SECS) * 1000
    end_ms = (now + FORECAST_SECS) * 1000
    with _state_lock:
        predicted = [p for p in _predicted if start_ms <= p[0] <= end_ms]
        events = [e for e in _events if start_ms <= e["ms"] <= end_ms]
        fetched = _last_fetched_ts
        name = _station_name
    observed = [p for p in _observed() if p[0] >= start_ms]
    return {
        "observed": observed,
        "predicted": predicted,
        "events": events,
        "latest": latest(),
        "start_ms": start_ms,
        "end_ms": end_ms,
        "now": now,
        "fetched_ts": fetched or None,
        "station": STATION,
        "station_name": name,
        "datum": DATUM,
        "unit": UNIT,
    }


def status() -> Dict[str, Any]:
    cfg = _resolve_cfg()
    with _state_lock:
        fetched = _last_fetched_ts
        err = _last_error
        err_ts = _last_error_ts
        name = _station_name
    obs = _observed()
    return {
        "now": time.time(),
        "enabled": _enabled(cfg),
        "interval_secs": _interval_secs(cfg),
        "station": STATION,
        "station_name": name,
        "last_fetched_ts": fetched or None,
        "last_error": err,
        "last_error_ts": err_ts or None,
        "newest_sample_ts": (obs[-1][0] / 1000.0) if obs else None,
        "csv_path": CSV_PATH,
        "csv_size_bytes": _file_size(CSV_PATH),
        "rows_logged": _row_count(),
    }


def csv_path() -> str:
    return CSV_PATH


def csv_preview(max_rows: int = 5) -> str:
    if not os.path.isfile(CSV_PATH):
        return ""
    try:
        with open(CSV_PATH, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except OSError:
        return ""
    if not lines:
        return ""
    out = io.StringIO()
    out.write(lines[0])
    out.writelines(lines[1:][-max_rows:] if max_rows > 0 else [])
    return out.getvalue()


def delete_csv() -> Dict[str, Any]:
    """Wipe the cumulative CSV; the next poll backfills ``BACKFILL_SECS``."""
    global _backfill_done
    _backfill_done = False
    deleted = False
    try:
        if os.path.isfile(CSV_PATH):
            os.remove(CSV_PATH)
            deleted = True
    except OSError as e:
        logger.warning("tide: delete csv failed: %s", e)
        return {"ok": False, "error": str(e)}
    return {"ok": True, "deleted": deleted, "path": CSV_PATH}


def refresh_now() -> Dict[str, Any]:
    """One-shot poll for the Settings ``Refresh now`` button."""
    global _last_error, _last_error_ts
    ok, msg = _poll_once()
    if not ok:
        with _state_lock:
            _last_error = msg
            _last_error_ts = time.time()
    return {"ok": ok, "message": msg, "status": status()}
