"""HydroVu water-quality scraper.

Uses the public-access token from the printed instructions
(``https://www.hydrovu.com/#/?token=...``) to fetch the last seven days
of measurements from the Wailoa Sensor on Hawai'i Sea Grant's HydroVu
account. Runs on a background thread, refreshes once per hour, caches
the reduced series to ``/app/data/hydrovu_cache.json`` for the Live tab,
and appends newly-seen samples to ``/app/data/hydrovu.csv``.

The HydroVu endpoints are undocumented; they are the same ones the
public dashboard SPA calls. The token is exchanged for a short-lived
bearer token via ``GET /api/auth/tokenrefresh`` (mirroring the SPA's
``loginViaToken`` flow), and reused for the location list and
measurement fetch. If HydroVu changes any of that this module is the
single place to update.
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
from urllib.parse import parse_qs, unquote, urlparse

import requests

logger = logging.getLogger(__name__)

# --- Endpoints -------------------------------------------------------------
HYDROVU_BASE = "https://www.hydrovu.com"
TOKEN_REFRESH_PATH = "/api/auth/tokenrefresh"
LOCATION_PATH = "/api/location"
MEASUREMENT_PATH_TMPL = "/api/company/{company_id}/measurement"

# Sensor location name we look for. A ``startswith`` match so a suffix
# rename like "Wailoa Sensor - 1176679" keeps matching.
WAILOA_LOCATION_PREFIX = "Wailoa Sensor"

HTTP_TIMEOUT_S = 15.0

# --- Poll cadence ----------------------------------------------------------
MIN_INTERVAL_S = 60.0
MAX_INTERVAL_S = 24 * 3600.0
DEFAULT_INTERVAL_S = 3600.0

# On a fresh error we back off before retrying so a broken token can't
# hammer HydroVu once a minute for the operator's whole shift.
RETRY_BACKOFF_S = 300.0

# --- Storage ---------------------------------------------------------------
CACHE_PATH = os.environ.get("WAILOA_HYDROVU_CACHE", "/app/data/hydrovu_cache.json")
CSV_PATH = os.environ.get("WAILOA_HYDROVU_CSV", "/app/data/hydrovu.csv")

# --- Series schema ---------------------------------------------------------
# Parameter id (HydroVu's key) -> (display label, unit query value, CSV column).
# The unit values are what the SPA's measurement POST body sets, so
# HydroVu returns pre-converted numbers rather than raw sensor units.
PARAMS: Tuple[Tuple[str, str, str, str], ...] = (
    ("salinity", "Salinity", "psu", "salinity_psu"),
    ("turbidity", "Turbidity", "ntu", "turbidity_ntu"),
    ("do", "DO", "milligramsPerLiter", "do_mg_l"),
    ("ph", "pH", "ph", "ph"),
    ("temperature", "Temperature", "celsius", "temperature_c"),
    ("orp", "ORP", "millivolt", "orp_mv"),
)

CSV_HEADER: Tuple[str, ...] = (
    "timestamp_iso",
    "timestamp_epoch",
    *[p[3] for p in PARAMS],
)

WINDOW_SECS = 7 * 24 * 3600

# Length of one HydroVu time slice at resolution 13 (measured: consecutive
# ``time_slice`` keys differ by exactly 491520 s, ~5.7 days). The
# ``timeSlice >=`` filter matches a slice by its *start*, so a slice that
# began before the window start is dropped even though most of its samples
# fall inside the window -- the chart then only showed data since the most
# recent slice boundary. Query one slice further back and trim the points.
SLICE_SECS = 491520

# --- Module state ----------------------------------------------------------
_state_lock = threading.Lock()
_thread: Optional[threading.Thread] = None
_stop = threading.Event()
_wake = threading.Event()
_get_cfg: Optional[Callable[[], Dict[str, Any]]] = None

_last_fetched_ts: float = 0.0
_last_error: Optional[str] = None
_last_error_ts: float = 0.0
_last_series_points: Dict[str, int] = {}
_cached_company_id: Optional[str] = None
_cached_location_id: Optional[int] = None


# --- Token helpers ---------------------------------------------------------

_URL_TOKEN_RE = re.compile(r"[?#&]token=([^&#]+)", re.IGNORECASE)


def _extract_token(raw: str) -> str:
    """Accept the raw token, the full HydroVu URL, or the URL-encoded token.

    Ignoring shape means the operator can paste whatever they got in the
    email (a full ``https://www.hydrovu.com/#/?token=...`` link or just
    the string that follows ``token=``) and the poller still finds it.
    """
    s = (raw or "").strip()
    if not s:
        return ""
    match = _URL_TOKEN_RE.search(s)
    if match:
        s = match.group(1)
    if "%" in s:
        try:
            s = unquote(s)
        except Exception:
            pass
    return s


def _bearer_from_response(resp: requests.Response) -> Optional[str]:
    """HydroVu's tokenrefresh returns the refreshed session in the
    ``Authorization`` response header. Return the raw token (no ``Bearer``
    prefix) so we can prefix it consistently."""
    header = resp.headers.get("Authorization") or resp.headers.get("authorization")
    if not header:
        return None
    return header.replace("Bearer", "").strip() or None


# --- API calls -------------------------------------------------------------


def _refresh_session(public_token: str) -> Tuple[str, str]:
    """Exchange the public URL token for (session_bearer, company_id).

    The SPA (see main.b9008c1beeab...js `refreshToken`) calls this once
    on load and reuses the returned bearer for the rest of the session.
    HydroVu will 401 if we send the raw token to /api/company/...
    directly, so we do the same round-trip.
    """
    url = HYDROVU_BASE + TOKEN_REFRESH_PATH
    headers = {"Authorization": f"Bearer {public_token}", "Accept": "application/json"}
    r = requests.get(url, headers=headers, timeout=HTTP_TIMEOUT_S)
    if r.status_code != 200:
        raise RuntimeError(f"tokenrefresh HTTP {r.status_code}: {r.text.strip()[:200]}")
    body = r.json() or {}
    session = _bearer_from_response(r)
    if not session:
        raise RuntimeError("tokenrefresh returned no Authorization header")
    company = body.get("company") or {}
    cid = company.get("id")
    if cid is None:
        raise RuntimeError("tokenrefresh response missing company.id")
    return session, str(cid)


def _find_location_id(session_bearer: str) -> int:
    """Locate the Wailoa Sensor id in the account's active locations.

    We cache the id in memory so subsequent polls skip this call. If the
    location moves (id changes) the next poll after a container restart
    will discover the new one.
    """
    q = json.dumps({"active": True}, separators=(",", ":"))
    r = requests.get(
        HYDROVU_BASE + LOCATION_PATH,
        headers={"Authorization": f"Bearer {session_bearer}", "Accept": "application/json"},
        params={"q": q},
        timeout=HTTP_TIMEOUT_S,
    )
    if r.status_code != 200:
        raise RuntimeError(f"location list HTTP {r.status_code}: {r.text.strip()[:200]}")
    data = (r.json() or {}).get("data") or []
    for row in data:
        name = str(row.get("name") or "")
        if name.startswith(WAILOA_LOCATION_PREFIX):
            try:
                return int(row.get("id"))
            except (TypeError, ValueError):
                continue
    names = [str(row.get("name") or "") for row in data]
    raise RuntimeError(
        f"no location starting with {WAILOA_LOCATION_PREFIX!r}; saw: {', '.join(names) or 'none'}"
    )


def _fetch_measurements(
    session_bearer: str,
    company_id: str,
    location_id: int,
    since_secs: int,
    until_secs: int,
) -> Dict[str, Any]:
    """POST /api/company/<id>/measurement, mirroring the SPA's payload.

    ``q`` is a JSON blob (double-encoded, matching the SPA). The
    per-parameter unit fields tell HydroVu which unit to return values
    in, so we don't have to guess whether the account defaults have
    switched between e.g. millivolt and volt.
    """
    q = {
        "resolution": 13,
        "timeSlice": [
            {"value": int(since_secs), "op": ">="},
            {"value": int(until_secs), "op": "<="},
        ],
        "active": True,
        "locationIds": [int(location_id)],
    }
    body: Dict[str, Any] = {"q": json.dumps(q, separators=(",", ":"))}
    for pid, _label, unit, _col in PARAMS:
        body[pid] = unit
    url = HYDROVU_BASE + MEASUREMENT_PATH_TMPL.format(company_id=company_id)
    r = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {session_bearer}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        data=json.dumps(body, separators=(",", ":")),
        timeout=HTTP_TIMEOUT_S,
    )
    if r.status_code != 200:
        raise RuntimeError(f"measurement HTTP {r.status_code}: {r.text.strip()[:200]}")
    payload = r.json() or {}
    return payload


# --- Series reduction ------------------------------------------------------


def _reduce_series(payload: Dict[str, Any], since_ms: int = 0) -> Dict[str, Dict[str, Any]]:
    """Turn the measurement payload into ``{param: {unit, points: [[ms, v], ...]}}``.

    HydroVu returns each parameter's ``raw`` list as a mix of samples and
    ``null`` placeholders (gaps in the source data). Skip the nulls, keep
    the ``time`` field as milliseconds since epoch (the SPA does the
    same), and sort by time so a later ``merge`` into the CSV is monotone.
    """
    rows = (payload or {}).get("data") or []
    if not rows:
        return {pid: {"unit": unit, "points": []} for pid, _lbl, unit, _col in PARAMS}
    # One row per HydroVu time slice (a fixed ~5.7-day bucket at resolution
    # 13), so a 7-day window that straddles a slice boundary comes back as
    # two rows. Reading only rows[0] silently dropped the newer slice.
    out: Dict[str, Dict[str, Any]] = {}
    for pid, _label, want_unit, _col in PARAMS:
        by_time: Dict[int, float] = {}
        unit = None
        for row in rows:
            entry = (row.get("data_blob") or {}).get(pid) or {}
            unit = unit or entry.get("unit")
            for sample in entry.get("raw") or []:
                if not sample:
                    continue
                t = sample.get("time")
                v = sample.get("value")
                if t is None or v is None or int(t) < since_ms:
                    continue
                try:
                    by_time[int(t)] = float(v)
                except (TypeError, ValueError):
                    continue
        points = [[t, by_time[t]] for t in sorted(by_time)]
        out[pid] = {"unit": unit or want_unit, "points": points}
    return out


# --- Cache / CSV persistence ----------------------------------------------


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _atomic_write(path: str, text: str) -> None:
    _ensure_dir(path)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


def _load_cache() -> Dict[str, Any]:
    if not os.path.isfile(CACHE_PATH):
        return {"series": {}, "fetched_ts": 0}
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {"series": {}, "fetched_ts": 0}
    except (OSError, ValueError):
        return {"series": {}, "fetched_ts": 0}


def _write_cache(series: Dict[str, Dict[str, Any]], fetched_ts: float, location_id: Optional[int]) -> None:
    body = {
        "series": series,
        "fetched_ts": fetched_ts,
        "location_id": location_id,
        "params": [
            {"id": pid, "label": label, "unit": unit, "column": col}
            for pid, label, unit, col in PARAMS
        ],
    }
    _atomic_write(CACHE_PATH, json.dumps(body, separators=(",", ":")))


def _iso_hst(ms: int) -> str:
    """HST is a stable UTC-10 offset (Hawaii doesn't observe DST), so we
    format the timestamp without pulling in tzdata."""
    secs = ms / 1000.0 - 10 * 3600
    return time.strftime("%Y-%m-%dT%H:%M:%S-10:00", time.gmtime(secs))


def _read_csv_timestamps() -> Tuple[set, List[str]]:
    """Return (set of epoch-ms strings already in the CSV, current header).

    An empty file (either missing or header-only) returns an empty set
    and no header. Keeping timestamps as strings avoids float rounding
    when we compare against the ints we'd write.
    """
    if not os.path.isfile(CSV_PATH):
        return set(), []
    have: set = set()
    header: List[str] = []
    try:
        with open(CSV_PATH, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            first = next(reader, None)
            if first:
                header = [c.strip() for c in first]
            try:
                epoch_idx = header.index("timestamp_epoch")
            except ValueError:
                epoch_idx = 1
            for row in reader:
                if len(row) > epoch_idx:
                    have.add(row[epoch_idx].strip())
    except OSError:
        return set(), []
    return have, header


def _append_new_rows(series: Dict[str, Dict[str, Any]]) -> int:
    """Merge new measurement timestamps into ``hydrovu.csv``. Returns rows added.

    Rows are keyed by ``timestamp_epoch``: a scrape returns the whole
    last week every hour, so without this dedup the file would grow by
    ~600 rows per poll instead of the couple of dozen fresh ones.
    """
    # Union of all times seen across parameters -- some rows may only
    # populate a subset when a probe hiccups.
    by_ts: Dict[int, Dict[str, float]] = {}
    for pid, _label, _unit, col in PARAMS:
        s = series.get(pid) or {}
        for ms, v in s.get("points") or []:
            row = by_ts.setdefault(int(ms), {})
            row[col] = float(v)
    if not by_ts:
        return 0
    _ensure_dir(CSV_PATH)
    have, existing = _read_csv_timestamps()
    new_file = not existing
    added = 0
    with open(CSV_PATH, "a", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(CSV_HEADER)
        for ms in sorted(by_ts.keys()):
            secs = ms // 1000
            key = str(secs)
            if key in have:
                continue
            row = by_ts[ms]
            cells = [
                _iso_hst(ms),
                key,
                *[_format(row.get(col)) for _pid, _label, _unit, col in PARAMS],
            ]
            writer.writerow(cells)
            have.add(key)
            added += 1
    return added


def _format(v: Optional[float]) -> str:
    if v is None:
        return ""
    # Six significant digits keeps the CSV readable without dropping the
    # precision the sensor reports. HydroVu itself only ever returns floats.
    return f"{v:.6g}"


def _row_count() -> int:
    if not os.path.isfile(CSV_PATH):
        return 0
    try:
        with open(CSV_PATH, "rb") as f:
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


# --- Poll loop -------------------------------------------------------------


def _resolve_cfg() -> Dict[str, Any]:
    if _get_cfg is None:
        return {}
    try:
        return _get_cfg() or {}
    except Exception:
        logger.exception("hydrovu: cfg load failed")
        return {}


def _interval_secs(cfg: Dict[str, Any]) -> float:
    raw = cfg.get("hydrovu_interval_secs", DEFAULT_INTERVAL_S)
    try:
        v = float(raw)
    except (TypeError, ValueError):
        v = DEFAULT_INTERVAL_S
    return max(MIN_INTERVAL_S, min(MAX_INTERVAL_S, v))


def _enabled(cfg: Dict[str, Any]) -> bool:
    return bool(cfg.get("hydrovu_enabled", True))


def _token(cfg: Dict[str, Any]) -> str:
    return _extract_token(cfg.get("hydrovu_token") or "")


def _poll_once(cfg: Dict[str, Any]) -> Tuple[bool, str]:
    """One end-to-end scrape. Returns (ok, message).

    On success writes the JSON cache and appends any newly-seen samples
    to the CSV. On failure keeps the existing cache/CSV untouched so a
    transient HydroVu blip doesn't blank the graphs.
    """
    global _last_fetched_ts, _last_error, _last_error_ts, _last_series_points
    global _cached_company_id, _cached_location_id
    token = _token(cfg)
    if not token:
        return False, "no token configured"
    try:
        session_bearer, company_id = _refresh_session(token)
    except Exception as e:
        return False, f"tokenrefresh: {e}"
    if _cached_location_id is None or _cached_company_id != company_id:
        try:
            location_id = _find_location_id(session_bearer)
        except Exception as e:
            return False, f"locate sensor: {e}"
        _cached_location_id = location_id
        _cached_company_id = company_id
    else:
        location_id = _cached_location_id
    now_s = int(time.time())
    since = now_s - WINDOW_SECS
    try:
        payload = _fetch_measurements(
            session_bearer, company_id, location_id, since - SLICE_SECS, now_s
        )
    except Exception as e:
        return False, f"measurement fetch: {e}"
    series = _reduce_series(payload, since_ms=since * 1000)
    fetched_ts = time.time()
    try:
        _write_cache(series, fetched_ts, location_id)
    except OSError as e:
        logger.warning("hydrovu: cache write failed: %s", e)
    try:
        added = _append_new_rows(series)
    except OSError as e:
        logger.warning("hydrovu: csv append failed: %s", e)
        added = 0
    with _state_lock:
        _last_fetched_ts = fetched_ts
        _last_error = None
        _last_error_ts = 0.0
        _last_series_points = {pid: len(series[pid]["points"]) for pid in series}
    return True, f"ok · {added} new row{'' if added == 1 else 's'}"


def _loop() -> None:
    global _last_error, _last_error_ts
    while not _stop.is_set():
        cfg = _resolve_cfg()
        if not _enabled(cfg):
            _wake.wait(timeout=30.0)
            _wake.clear()
            continue
        token = _token(cfg)
        if not token:
            with _state_lock:
                _last_error = "no token configured"
                _last_error_ts = time.time()
            _wake.wait(timeout=60.0)
            _wake.clear()
            continue
        try:
            ok, msg = _poll_once(cfg)
        except Exception as e:
            logger.exception("hydrovu: poll exception")
            ok, msg = False, str(e)
        if ok:
            logger.info("hydrovu: %s", msg)
            wait = _interval_secs(cfg)
        else:
            logger.warning("hydrovu: %s", msg)
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
    _thread = threading.Thread(target=_loop, daemon=True, name="hydrovu-poller")
    _thread.start()
    logger.info(
        "hydrovu poller thread started (cache=%s csv=%s)",
        CACHE_PATH,
        CSV_PATH,
    )


def stop() -> None:
    _stop.set()
    _wake.set()


def poke() -> None:
    """Force an immediate poll (used after Settings save)."""
    _wake.set()


def status() -> Dict[str, Any]:
    cfg = _resolve_cfg()
    with _state_lock:
        fetched = _last_fetched_ts
        err = _last_error
        err_ts = _last_error_ts
        pts = dict(_last_series_points)
    token_set = bool(_token(cfg))
    return {
        "now": time.time(),
        "enabled": _enabled(cfg),
        "interval_secs": _interval_secs(cfg),
        "token_set": token_set,
        "last_fetched_ts": fetched or None,
        "last_error": err,
        "last_error_ts": err_ts or None,
        "series_points": pts,
        "cache_path": CACHE_PATH,
        "csv_path": CSV_PATH,
        "csv_size_bytes": _file_size(CSV_PATH),
        "rows_logged": _row_count(),
        "location_id": _cached_location_id,
    }


def series() -> Dict[str, Any]:
    """Latest cached series for the Live tab. Also returns the parameter
    metadata so the client doesn't have to hard-code labels/units."""
    body = _load_cache()
    body["params"] = [
        {"id": pid, "label": label, "unit": unit, "column": col}
        for pid, label, unit, col in PARAMS
    ]
    body["now"] = time.time()
    return body


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
    header = lines[0]
    data = lines[1:]
    tail = data[-max_rows:] if max_rows > 0 else []
    out = io.StringIO()
    out.write(header)
    out.writelines(tail)
    return out.getvalue()


def delete_csv() -> Dict[str, Any]:
    """Wipe the cumulative CSV; logging continues on the next poll."""
    deleted = False
    try:
        if os.path.isfile(CSV_PATH):
            os.remove(CSV_PATH)
            deleted = True
    except OSError as e:
        logger.warning("hydrovu: delete csv failed: %s", e)
        return {"ok": False, "error": str(e)}
    return {"ok": True, "deleted": deleted, "path": CSV_PATH}


def refresh_now() -> Dict[str, Any]:
    """One-shot poll for the Settings ``Refresh now`` button."""
    cfg = _resolve_cfg()
    ok, msg = _poll_once(cfg)
    return {"ok": ok, "message": msg, "status": status()}
