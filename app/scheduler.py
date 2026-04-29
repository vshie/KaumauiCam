"""15-minute slot schedule + legacy migration."""

from __future__ import annotations

import datetime as dt
from typing import Dict, List, Set, Tuple

_WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_ALL_DAYS_LIST = list(_WEEKDAYS)
_LEGACY_KEYS = frozenset({"window_start", "window_stop", "interval_min", "duration_min"})
SCHEDULE_TIMEZONE_LABEL = "Pacific/Honolulu"
SCHEDULE_TIMEZONE = dt.timezone(dt.timedelta(hours=-10), SCHEDULE_TIMEZONE_LABEL)


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
