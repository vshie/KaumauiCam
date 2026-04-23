"""Daily window + interval/duration cycle helpers."""

from __future__ import annotations

import datetime as dt
from typing import Tuple


def _parse_hhmm(s: str) -> Tuple[int, int]:
    parts = s.strip().split(":")
    h = int(parts[0])
    m = int(parts[1]) if len(parts) > 1 else 0
    return h, m


def _minutes_since_midnight(t: dt.datetime) -> int:
    return t.hour * 60 + t.minute


def in_daily_window(now: dt.datetime, window_start: str, window_stop: str) -> bool:
    """
    Window [start, stop) in local time. If start > stop, treat as overnight
    (e.g. 22:00 -> 06:00 means active from 22:00 to midnight and midnight to 06:00).
    """
    t = _minutes_since_midnight(now)
    sh, sm = _parse_hhmm(window_start)
    eh, em = _parse_hhmm(window_stop)
    s = sh * 60 + sm
    e = eh * 60 + em
    if s <= e:
        return s <= t < e
    return t >= s or t < e


def _window_open_today(now: dt.datetime, window_start: str) -> dt.datetime:
    sh, sm = _parse_hhmm(window_start)
    d = now.date()
    open_t = dt.datetime.combine(d, dt.time(sh, sm), tzinfo=now.tzinfo)
    # If overnight window and now is before stop in early morning, anchor to yesterday's start
    eh, em = _parse_hhmm(window_stop)
    s = sh * 60 + sm
    e = eh * 60 + em
    if s > e and _minutes_since_midnight(now) < e:
        open_t = open_t - dt.timedelta(days=1)
    return open_t


def cycle_should_be_on(
    now: dt.datetime,
    window_start: str,
    window_stop: str,
    interval_min: int,
    duration_min: int,
) -> bool:
    """
    Inside window: cycle length = interval_min, first duration_min minutes of each cycle ON.
    """
    if interval_min <= 0 or duration_min < 0:
        return False
    if duration_min >= interval_min:
        return in_daily_window(now, window_start, window_stop)
    if not in_daily_window(now, window_start, window_stop):
        return False
    open_t = _window_open_today(now, window_start)
    # If we're in overnight segment before stop but after midnight, open_t already yesterday
    delta = (now - open_t).total_seconds() / 60.0
    if delta < 0:
        delta = 0
    pos = delta % float(interval_min)
    return pos < float(duration_min)
