"""15-minute slot schedule (YouTube) + recording cycle (Recordings)."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Set, Tuple

_WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_ALL_DAYS_LIST = list(_WEEKDAYS)
_LEGACY_KEYS = frozenset({"window_start", "window_stop", "interval_min", "duration_min"})
SCHEDULE_TIMEZONE_LABEL = "Pacific/Honolulu"
SCHEDULE_TIMEZONE = dt.timezone(dt.timedelta(hours=-10), SCHEDULE_TIMEZONE_LABEL)

# Fixed daytime window the recording cycle operates within, in minutes
# since local (Pacific/Honolulu) midnight. Deliberately not exposed as
# user config: the operator asked for the simplest possible surface --
# just record duration + pause duration -- and this fishpond deployment
# only needs daylight footage. If a future deployment needs configurable
# window bounds, they'd be added here alongside these constants.
RECORDING_WINDOW_START_MIN = 7 * 60 + 45  # 07:45 HST
RECORDING_WINDOW_STOP_MIN = 18 * 60       # 18:00 HST


# Recording-cycle defaults. Kept module-local so the UI and config
# module reference the same numbers.
_DEFAULT_RECORD_SECS = 60
_DEFAULT_PAUSE_SECS = 120


def normalize_recordings_cycle(cycle: Any) -> Dict[str, Any]:
    """Coerce untrusted input (loaded JSON or POST body) into a valid
    ``{enabled, record_secs, pause_secs}`` dict. Non-positive record
    durations fall back to the default so a stray 0 in config.json
    doesn't wedge the scheduler; negative pauses become 0. Kept in
    this module so both config.load() and the scheduler agree."""
    if not isinstance(cycle, dict):
        cycle = {}
    try:
        r = int(round(float(cycle.get("record_secs", _DEFAULT_RECORD_SECS))))
    except (TypeError, ValueError):
        r = _DEFAULT_RECORD_SECS
    try:
        p = int(round(float(cycle.get("pause_secs", _DEFAULT_PAUSE_SECS))))
    except (TypeError, ValueError):
        p = _DEFAULT_PAUSE_SECS
    if r <= 0:
        r = _DEFAULT_RECORD_SECS
    if p < 0:
        p = 0
    return {
        "enabled": bool(cycle.get("enabled", False)),
        "record_secs": r,
        "pause_secs": p,
    }


def recording_active(now: dt.datetime, cycle: Any) -> bool:
    """True iff the recording cycle wants ffmpeg running at ``now``.

    Rules, in order: cycle disabled -> False; outside the fixed HST
    window -> False; otherwise the elapsed time from the window's start
    is mapped into the (record + pause) sawtooth and we return True
    during the record phase. Boundary at t == record_secs is treated as
    the start of the pause (strict ``<``), matching how the scheduler
    tick will see the transition."""
    c = normalize_recordings_cycle(cycle)
    if not c["enabled"]:
        return False
    mins = _minutes_since_midnight(now)
    if mins < RECORDING_WINDOW_START_MIN or mins >= RECORDING_WINDOW_STOP_MIN:
        return False
    # Elapsed seconds since the window opened today, including the
    # sub-minute component so the cycle boundary is precise even though
    # the scheduler ticks at 2s.
    elapsed = (mins - RECORDING_WINDOW_START_MIN) * 60 + now.second
    cycle_len = c["record_secs"] + c["pause_secs"]
    if cycle_len <= 0:
        return False
    return (elapsed % cycle_len) < c["record_secs"]


def recording_preview(cycle: Any) -> Dict[str, Any]:
    """Return a preview summary the UI can render below the two inputs:
    how many recordings the cycle produces per day and their total
    duration. ``valid`` is false with a human-readable ``reason`` if
    the input is malformed (0 record duration, etc.) so the UI can
    surface it without duplicating the check."""
    c = normalize_recordings_cycle(cycle)
    r = c["record_secs"]
    p = c["pause_secs"]
    window_secs = (RECORDING_WINDOW_STOP_MIN - RECORDING_WINDOW_START_MIN) * 60
    if r <= 0:
        return {
            "valid": False,
            "reason": "Record duration must be positive.",
            "n_recordings": 0,
            "total_recorded_secs": 0,
            "cycle_secs": max(0, r + p),
            "window_secs": window_secs,
        }
    cycle_len = r + p
    full = window_secs // cycle_len
    remainder = window_secs - full * cycle_len
    partial = min(remainder, r)
    n = full + (1 if partial > 0 else 0)
    total = full * r + partial
    return {
        "valid": True,
        "reason": "",
        "n_recordings": int(n),
        "total_recorded_secs": int(total),
        "cycle_secs": int(cycle_len),
        "window_secs": int(window_secs),
    }


def schedule_now() -> dt.datetime:
    """Return the current schedule clock time in Pacific/Honolulu."""
    return dt.datetime.now(SCHEDULE_TIMEZONE)


def _parse_hhmm(s: str) -> Tuple[int, int]:
    parts = str(s).strip().split(":")
    h = int(parts[0])
    m = int(parts[1]) if len(parts) > 1 else 0
    return h, m


def _minutes_since_midnight(t: dt.datetime) -> int:
    return t.hour * 60 + t.minute


def _in_daily_window(now: dt.datetime, window_start: str, window_stop: str) -> bool:
    t = _minutes_since_midnight(now)
    sh, sm = _parse_hhmm(window_start)
    eh, em = _parse_hhmm(window_stop)
    s = sh * 60 + sm
    e = eh * 60 + em
    if s <= e:
        return s <= t < e
    return t >= s or t < e


def _window_open_today(now: dt.datetime, window_start: str, window_stop: str) -> dt.datetime:
    sh, sm = _parse_hhmm(window_start)
    d = now.date()
    open_t = dt.datetime.combine(d, dt.time(sh, sm), tzinfo=now.tzinfo)
    eh, em = _parse_hhmm(window_stop)
    s = sh * 60 + sm
    e = eh * 60 + em
    if s > e and _minutes_since_midnight(now) < e:
        open_t = open_t - dt.timedelta(days=1)
    return open_t


def _legacy_cycle_should_be_on(
    now: dt.datetime,
    window_start: str,
    window_stop: str,
    interval_min: int,
    duration_min: int,
) -> bool:
    if interval_min <= 0 or duration_min < 0:
        return False
    if duration_min >= interval_min:
        return _in_daily_window(now, window_start, window_stop)
    if not _in_daily_window(now, window_start, window_stop):
        return False
    open_t = _window_open_today(now, window_start, window_stop)
    delta = (now - open_t).total_seconds() / 60.0
    if delta < 0:
        delta = 0
    pos = delta % float(interval_min)
    return pos < float(duration_min)


def _normalize_days(days: object) -> List[str]:
    if not isinstance(days, list) or not days:
        return list(_ALL_DAYS_LIST)
    out: List[str] = []
    for d in days:
        if isinstance(d, str):
            x = d.strip().lower()[:3]
            if x in _WEEKDAYS and x not in out:
                out.append(x)
    return out if out else list(_ALL_DAYS_LIST)


def _normalize_slots(slots: object) -> List[int]:
    if not isinstance(slots, list):
        return []
    out: Set[int] = set()
    for s in slots:
        try:
            i = int(s)
        except (TypeError, ValueError):
            continue
        if 0 <= i < 96:
            out.add(i)
    return sorted(out)


def slot_active(now: dt.datetime, sched: dict) -> bool:
    if not sched.get("enabled"):
        return False
    wd = _WEEKDAYS[now.weekday()]
    if wd not in _normalize_days(sched.get("days")):
        return False
    idx = (now.hour * 60 + now.minute) // 15
    return idx in set(_normalize_slots(sched.get("slots")))


def should_be_on(now: dt.datetime, sched: dict) -> bool:
    """
    True if the schedule wants the service on now, or within the next few seconds
    (same local calendar day) so we do not tear down at 15-minute boundaries between
    adjacent selected slots.
    """
    if slot_active(now, sched):
        return True
    soon = now + dt.timedelta(seconds=10)
    if soon.date() != now.date():
        return False
    return slot_active(soon, sched)


def has_remaining_slots_today(now: dt.datetime, sched: dict) -> bool:
    """True iff the schedule has any active slot at or after ``now`` and
    before local midnight (same weekday). The YouTube Data API broadcast
    manager consults this to decide whether ``want_yt`` going false is
    just a between-slots gap (keep the broadcast open for the next slot)
    or the end of the day (transition it to ``complete`` so the archive
    finalises). Ignores the ``enabled`` flag off intentionally -- an
    all-disabled schedule has "no remaining slots today"."""
    if not sched.get("enabled"):
        return False
    wd = _WEEKDAYS[now.weekday()]
    if wd not in _normalize_days(sched.get("days")):
        return False
    if slot_active(now, sched):
        return True
    current_slot = (now.hour * 60 + now.minute) // 15
    slots = _normalize_slots(sched.get("slots"))
    return any(s > current_slot for s in slots)


def migrate_legacy_schedule(sched: dict) -> dict:
    """
    Normalize days/slots; if legacy window/interval keys are present, derive slots
    from them (sample each 15-min slot) and strip legacy keys.
    """
    if not isinstance(sched, dict):
        return {"enabled": False, "days": list(_ALL_DAYS_LIST), "slots": []}

    out = dict(sched)

    if _LEGACY_KEYS.intersection(out.keys()):
        ws = str(out.get("window_start", "06:00"))
        we = str(out.get("window_stop", "18:00"))
        try:
            interval_min = int(out.get("interval_min", 60))
        except (TypeError, ValueError):
            interval_min = 60
        try:
            duration_min = int(out.get("duration_min", 20))
        except (TypeError, ValueError):
            duration_min = 20
        base = dt.datetime(2000, 1, 1, 0, 0, 0)
        slots: List[int] = []
        for idx in range(96):
            minute = idx * 15
            t = base + dt.timedelta(minutes=minute)
            if _legacy_cycle_should_be_on(t, ws, we, interval_min, duration_min):
                slots.append(idx)
        for k in _LEGACY_KEYS:
            out.pop(k, None)
        out["slots"] = slots
        if "days" not in out or not out["days"]:
            out["days"] = list(_ALL_DAYS_LIST)

    for k in _LEGACY_KEYS:
        out.pop(k, None)
    out["enabled"] = bool(out.get("enabled", False))
    out["days"] = _normalize_days(out.get("days"))
    out["slots"] = _normalize_slots(out.get("slots"))
    return out
