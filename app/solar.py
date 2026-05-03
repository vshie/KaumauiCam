"""Victron solar data logger.

Polls the on-board ESPHome device (Fishpond, default 192.168.20.66) at
``solar_interval_secs`` and appends one row per poll to a CSV file at
``/app/data/solar.csv``. The CSV is the deliverable: the operator
downloads it from the Settings page (cumulative, append-only,
``timestamp_iso`` is the first column).

Why ESPHome's per-entity REST API rather than the SSE ``/events`` stream
or the native API? REST is dead simple, fits the existing polling
patterns in this codebase (link_uptime, youtube_monitor), survives
reboots/network blips with no reconnect logic, and has cheap per-poll
cost (~12 sequential GETs in <1 s once a minute). The SSE stream would
be slightly more efficient but the added reconnect/resync state machine
isn't worth it for a 60s log cadence.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# Persisted alongside state.db / config.json — i.e. the host bind
# /usr/blueos/extensions/kaumauicam, so the file survives container
# rebuilds and reboots without ever touching the USB drive.
DEFAULT_CSV_PATH = os.environ.get("KAUMAUI_SOLAR_CSV", "/app/data/solar.csv")

# Order is significant: this is the on-disk column order. ``timestamp_iso``
# is required by spec to be column 1; the rest are grouped logically
# (battery, PV, load, daily/lifetime yield, status text, identity).
CSV_COLUMNS: Tuple[str, ...] = (
    "timestamp_iso",
    "timestamp_epoch",
    "battery_voltage_v",
    "battery_current_a",
    "battery_temperature_c",
    "pv_voltage_v",
    "pv_power_w",
    "load_output",
    "load_current_a",
    "yield_today_wh",
    "yield_total_wh",
    "yield_yesterday_wh",
    "max_power_today_w",
    "max_power_yesterday_w",
    "charging_mode",
    "mppt_tracking",
    "error",
    "device_type",
    "serial",
    "firmware",
)

# (column_key, esphome_domain, esphome_entity_id, kind)
# kind: "value" -> use JSON ``value`` field (numeric / null)
#       "state" -> use JSON ``state`` field (text / boolean rendered as ON/OFF)
SENSOR_MAP: Tuple[Tuple[str, str, str, str], ...] = (
    ("battery_voltage_v", "sensor", "victron_battery_voltage", "value"),
    ("battery_current_a", "sensor", "victron_battery_current", "value"),
    ("battery_temperature_c", "sensor", "victron_battery_temperature", "value"),
    ("pv_voltage_v", "sensor", "victron_pv_voltage", "value"),
    ("pv_power_w", "sensor", "victron_pv_power_watts", "value"),
    ("load_output", "binary_sensor", "victron_load_output", "state"),
    ("load_current_a", "sensor", "victron_load_current", "value"),
    ("yield_today_wh", "sensor", "victron_yield_today", "value"),
    ("yield_total_wh", "sensor", "victron_yield_total", "value"),
    ("yield_yesterday_wh", "sensor", "victron_yield_yesterday", "value"),
    ("max_power_today_w", "sensor", "victron_max_power_today", "value"),
    ("max_power_yesterday_w", "sensor", "victron_max_power_yesterday", "value"),
    ("charging_mode", "text_sensor", "victron_charging_mode", "state"),
    ("mppt_tracking", "text_sensor", "victron_mppt_tracking", "state"),
    ("error", "text_sensor", "victron_error", "state"),
    ("device_type", "text_sensor", "victron_device_type", "state"),
    ("serial", "text_sensor", "victron_serial", "state"),
    ("firmware", "text_sensor", "victron_firmware", "state"),
)

HTTP_TIMEOUT_S = 2.5
MIN_INTERVAL_S = 5.0
MAX_INTERVAL_S = 3600.0
DEFAULT_INTERVAL_S = 60.0
DEFAULT_HOST = "192.168.20.66"
# After this many consecutive failures in a single batch we abort the
# remaining requests. Without this a single unreachable host takes
# len(SENSOR_MAP) * HTTP_TIMEOUT_S seconds (~45s for 18 entities @ 2.5s)
# which can exceed the polling interval and queue up samples.
BATCH_FAIL_BAIL = 3

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


def _csv_path() -> str:
    return DEFAULT_CSV_PATH


def _ensure_dir() -> None:
    d = os.path.dirname(_csv_path())
    if d:
        os.makedirs(d, exist_ok=True)


def _coerce(kind: str, raw: Optional[Dict[str, Any]]) -> str:
    """Convert one ESPHome JSON response into a CSV cell.

    "value" -> numeric (or "" if null/NA).
    "state" -> rendered text. Booleans collapse to ON/OFF; bare strings
    are passed through unmodified (CSV writer handles quoting). We never
    propagate the unit suffix (e.g. " V", " W", " Wh") so the column
    values are pure numbers and downstream tools (Excel, pandas) parse
    them without fuss; the column header carries the unit instead.
    """
    if not raw:
        return ""
    if kind == "value":
        v = raw.get("value")
        if v is None:
            return ""
        if isinstance(v, bool):
            return "1" if v else "0"
        return str(v)
    s = raw.get("state")
    if isinstance(s, bool):
        return "ON" if s else "OFF"
    if s is None:
        return ""
    return str(s)


def _fetch_one(host: str, domain: str, entity_id: str) -> Optional[Dict[str, Any]]:
    """GET one ESPHome entity. Returns the parsed JSON dict, or None on
    any failure (which we treat as "this column is blank for this row"
    rather than aborting the whole sample — a partial row is more useful
    than a missing one when only one entity is briefly offline)."""
    url = f"http://{host}/{domain}/{entity_id}"
    try:
        r = requests.get(url, timeout=HTTP_TIMEOUT_S)
        if r.status_code != 200:
            return None
        return r.json()
    except (requests.RequestException, ValueError):
        return None


def _sample(host: str) -> Tuple[Dict[str, Any], List[str], int]:
    """Poll every entity in SENSOR_MAP. Returns ``(sample_dict, missing_keys,
    error_count)`` where sample_dict is the column->string mapping the
    CSV writer expects.

    If the first ``BATCH_FAIL_BAIL`` requests in a row fail (host
    unreachable / DNS down / firewall) we abort and mark the rest as
    missing rather than waiting through 18 individual timeouts -- which
    can otherwise exceed the polling interval."""
    sample: Dict[str, Any] = {}
    missing: List[str] = []
    errors = 0
    consecutive_fail = 0
    bailed = False
    for col, domain, entity, kind in SENSOR_MAP:
        if bailed:
            sample[col] = ""
            missing.append(col)
            errors += 1
            continue
        raw = _fetch_one(host, domain, entity)
        if raw is None:
            errors += 1
            consecutive_fail += 1
            missing.append(col)
            sample[col] = ""
            if consecutive_fail >= BATCH_FAIL_BAIL:
                bailed = True
            continue
        consecutive_fail = 0
        sample[col] = _coerce(kind, raw)
    return sample, missing, errors


def _iso_utc(ts: float) -> str:
    """RFC 3339-ish UTC timestamp, second precision. Picked over
    ``datetime.now().isoformat()`` because it's stable on machines with
    no NTP / no local TZ configured (the BlueOS Pi often boots without
    network for a few seconds and gets the wrong wall clock)."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _append_row(sample: Dict[str, str], ts: float) -> None:
    """Append one row to the CSV. Creates the file with a header row on
    the first write. Uses csv.writer to handle quoting/escaping for the
    text-sensor cells (e.g. an error string that contains a comma)."""
    path = _csv_path()
    _ensure_dir()
    new_file = not os.path.exists(path) or os.path.getsize(path) == 0
    row = {
        "timestamp_iso": _iso_utc(ts),
        "timestamp_epoch": f"{ts:.0f}",
        **{k: sample.get(k, "") for k in CSV_COLUMNS if k not in ("timestamp_iso", "timestamp_epoch")},
    }
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(CSV_COLUMNS)
        w.writerow([row[k] for k in CSV_COLUMNS])


def _row_count(path: str) -> int:
    """Cheap row count for the status line. Counts data rows
    (excludes the header). On an unmounted SD or missing file, returns
    0 rather than raising."""
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
    """Snapshot the few config keys this module cares about. Reading
    ``cfgmod.load()`` once per cycle (rather than caching) means the
    operator can flip ``solar_enabled`` or change the host on the
    Settings page and the next cycle picks it up — no restart."""
    if _get_cfg is None:
        return {}
    try:
        return _get_cfg() or {}
    except Exception:
        logger.exception("solar: cfg load failed")
        return {}


def _interval_secs(cfg: Dict[str, Any]) -> float:
    raw = cfg.get("solar_interval_secs", DEFAULT_INTERVAL_S)
    try:
        v = float(raw)
    except (TypeError, ValueError):
        v = DEFAULT_INTERVAL_S
    return max(MIN_INTERVAL_S, min(MAX_INTERVAL_S, v))


def _host(cfg: Dict[str, Any]) -> str:
    return str(cfg.get("solar_host") or DEFAULT_HOST).strip() or DEFAULT_HOST


def _enabled(cfg: Dict[str, Any]) -> bool:
    return bool(cfg.get("solar_enabled", True))


def _loop() -> None:
    global _last_sample, _last_write_ts, _last_error, _last_error_ts, _rows_logged
    # Initial row count from disk so the UI shows a real total even if
    # the process restarts mid-deployment.
    _rows_logged = _row_count(_csv_path())
    while not _stop.is_set():
        cfg = _resolve_cfg()
        interval = _interval_secs(cfg)
        if not _enabled(cfg):
            # Logging disabled — sleep on _wake so the operator's
            # "Save" click can flip us back on without waiting up to
            # ``interval`` seconds.
            _wake.wait(timeout=min(interval, 30.0))
            _wake.clear()
            continue

        host = _host(cfg)
        ts = time.time()
        try:
            sample, missing, errors = _sample(host)
        except Exception as e:
            logger.exception("solar: sample exception")
            with _state_lock:
                _last_error = f"sample: {e}"
                _last_error_ts = ts
            _wake.wait(timeout=interval)
            _wake.clear()
            continue

        all_missing = (errors == len(SENSOR_MAP))
        try:
            _append_row(sample, ts)
            with _state_lock:
                _last_sample = {"ts": ts, **sample}
                _last_write_ts = ts
                _rows_logged += 1
                if all_missing:
                    _last_error = f"all entities unreachable on {host}"
                    _last_error_ts = ts
                elif missing:
                    # Partial sample — record so the UI can warn but
                    # don't treat it as a hard error since we still
                    # logged a row.
                    _last_error = f"partial sample, missing: {', '.join(missing[:6])}"
                    _last_error_ts = ts
                else:
                    _last_error = None
        except OSError as e:
            logger.exception("solar: csv write failed")
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
    _thread = threading.Thread(target=_loop, daemon=True, name="solar-logger")
    _thread.start()
    logger.info("solar logger thread started -> %s", _csv_path())


def stop() -> None:
    _stop.set()
    _wake.set()


def poke() -> None:
    """Force the next iteration to run immediately. Used after the
    operator hits Save on the Settings page so a config change takes
    effect within ~1s rather than up to ``solar_interval_secs``."""
    _wake.set()


def status() -> Dict[str, Any]:
    """Snapshot for /api/solar/status."""
    path = _csv_path()
    cfg = _resolve_cfg()
    with _state_lock:
        last = dict(_last_sample) if _last_sample else None
        last_write = _last_write_ts
        err = _last_error
        err_ts = _last_error_ts
        rows = _rows_logged
    return {
        "now": time.time(),
        "enabled": _enabled(cfg),
        "host": _host(cfg),
        "interval_secs": _interval_secs(cfg),
        "csv_path": path,
        "csv_size_bytes": _file_size(path),
        "rows_logged": rows,
        "last_sample": last,
        "last_write_ts": last_write or None,
        "last_error": err,
        "last_error_ts": err_ts or None,
        "columns": list(CSV_COLUMNS),
    }


def latest_sample() -> Dict[str, Any]:
    """Return the most recent in-memory sample dict (or {} if we haven't
    logged anything yet). Used by the Settings page to show live
    readings without re-hitting the device."""
    with _state_lock:
        return dict(_last_sample) if _last_sample else {}


def fetch_live(host: Optional[str] = None) -> Dict[str, Any]:
    """One-shot poll for the Settings 'Refresh' button. Independent of
    the background loop's cadence so the UI feels snappy when the
    operator is troubleshooting wiring on the device."""
    cfg = _resolve_cfg()
    h = (host or _host(cfg)).strip() or DEFAULT_HOST
    ts = time.time()
    sample, missing, errors = _sample(h)
    return {
        "ts": ts,
        "host": h,
        "sample": sample,
        "missing": missing,
        "errors": errors,
        "ok": errors < len(SENSOR_MAP),
    }


def delete_csv() -> Dict[str, Any]:
    """Remove the CSV file. Resets the in-memory row counter and the
    'last sample' state so the Settings panel reflects the wipe
    immediately. Safe if the file is missing."""
    global _rows_logged, _last_sample, _last_write_ts
    path = _csv_path()
    deleted = False
    try:
        if os.path.isfile(path):
            os.remove(path)
            deleted = True
    except OSError as e:
        logger.warning("solar: delete csv failed: %s", e)
        return {"ok": False, "error": str(e)}
    with _state_lock:
        _rows_logged = 0
        _last_sample = {}
        _last_write_ts = 0.0
    return {"ok": True, "deleted": deleted, "path": path}


def csv_path() -> str:
    return _csv_path()


def csv_preview(max_rows: int = 5) -> str:
    """Header + last N rows, returned as a single string. Cheap enough
    (we only read the tail) for a Settings preview pane that the user
    can glance at to confirm logging is working without downloading the
    whole file. Best-effort: if the file is huge we still scan it
    linearly, but the file is small (CSV-text) so it's fine."""
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
    # Slice the data rows first, then take the tail. Otherwise a small
    # file (e.g. 3 lines, max_rows=3) duplicates the header.
    data = lines[1:]
    tail = data[-max_rows:] if max_rows > 0 else []
    out = io.StringIO()
    out.write(header)
    out.writelines(tail)
    return out.getvalue()
