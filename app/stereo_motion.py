"""Recent motion events from the C3 motion-detection Parquet output.

The vendored c3record script (``motion_writer.py``) writes one row per
color frame -- ``file_name, frame_id, timestamp, motion, smoothed_motion``
-- to ``motion_*.parquet`` files in the ``stereo_motion`` folder beside the
stereo MKVs. It does not store the detector's per-frame boolean, so we
re-derive it with the detector's own rule, ``smoothed_motion >=
motion_sensitivity_threshold``, using the *current* configured threshold
(changing it in the UI re-evaluates history consistently).

Consecutive motion frames form an event; events separated by less than
MERGE_GAP_SECS are merged so one fish passing through doesn't read as a
dozen flickers. Upstream only flushes Parquet at process shutdown, so new
events appear once per recording burst.
"""

from __future__ import annotations

import glob
import logging
import os
import threading
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)

MOTION_GLOB = "motion_*.parquet"
MERGE_GAP_SECS = 5.0

_lock = threading.Lock()
# (path, mtime, threshold) -> events for that file, oldest first.
_cache: Dict[Tuple[str, float, float], List[Dict[str, Any]]] = {}


def _events_in_file(path: str, threshold: float) -> List[Dict[str, Any]]:
    import polars as pl  # heavy import; only paid when motion data exists

    df = (
        pl.read_parquet(path, columns=["timestamp", "smoothed_motion", "file_name"])
        .sort("timestamp")
    )
    events: List[Dict[str, Any]] = []
    cur: Dict[str, Any] | None = None
    for ts, sm, fname in df.iter_rows():
        if sm is None or ts is None or sm < threshold:
            continue
        t = ts.timestamp()
        if cur is not None and t - cur["end_epoch"] <= MERGE_GAP_SECS:
            cur["end_epoch"] = t
            cur["peak"] = max(cur["peak"], float(sm))
            continue
        cur = {
            "start_iso": ts.isoformat(),
            "start_epoch": t,
            "end_epoch": t,
            "peak": float(sm),
            "file_name": fname,
        }
        events.append(cur)
    for e in events:
        e["duration_secs"] = round(e["end_epoch"] - e["start_epoch"], 2)
    return events


def recent_events(motion_dir: str, threshold: float, limit: int = 5) -> List[Dict[str, Any]]:
    """Newest ``limit`` motion events, newest first. Reads files newest
    first and stops once enough events are found; per-file results are
    cached by mtime so repeated polls are cheap."""
    try:
        paths = glob.glob(os.path.join(motion_dir, MOTION_GLOB))
    except OSError:
        return []
    files: List[Tuple[float, str]] = []
    for p in paths:
        try:
            files.append((os.path.getmtime(p), p))
        except OSError:
            continue
    files.sort(reverse=True)

    out: List[Dict[str, Any]] = []
    with _lock:
        # Forget files that changed, vanished, or were read at another threshold.
        current = {(p, m, threshold) for m, p in files}
        for k in [k for k in _cache if k not in current]:
            del _cache[k]
        for mtime, p in files:
            key = (p, mtime, threshold)
            if key not in _cache:
                try:
                    _cache[key] = _events_in_file(p, threshold)
                except Exception as e:
                    # Also covers a file caught mid-write; its mtime changes
                    # once upstream finishes, which invalidates this entry.
                    logger.warning("stereo motion: skipping unreadable %s: %s", p, e)
                    _cache[key] = []
            out.extend(_cache[key])
            if len(out) >= limit:
                break
    out.sort(key=lambda e: e["start_epoch"], reverse=True)
    return out[:limit]
