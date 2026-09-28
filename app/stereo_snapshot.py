"""Still JPEG of the C3 stereo camera's color view, for the Live tab.

The C3 is a DepthAI device that serves exactly one client, and while a
burst is recording that client is the vendored ``c3record`` child. The
color JPEGs it pulls off the camera never leave that process, and
``c3record`` stays unmodified, so we can't tap them live. Waking the
camera just for a snapshot would fight the scheduled bursts for the
device and pay a 10-20 s bootup each time.

Instead we decode one frame from the newest *finished* MKV segment. The
color view is H.264 on the first video track (``mux.video_0`` in
``c3_video.py``), keyframes land every few frames, and a finalized
segment is fully indexed, so this is a quick ffmpeg call. Freshness
follows the record/pause cycle: about one segment old mid-burst, and as
old as the last burst during pauses and overnight -- the UI shows the
capture time so that's visible.

The extracted JPEG is cached on ``/app/data`` and only regenerated when
a newer finished segment appears, so polling from the UI is cheap.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import subprocess
import threading
from typing import Any, Dict, Optional

from stereo_recorder import SEGMENT_FORMAT, latest_finished_segment

logger = logging.getLogger(__name__)

SNAPSHOT_PATH = "/app/data/stereo_snapshot.jpg"
META_PATH = "/app/data/stereo_snapshot.json"

# Downscaled: the tile is roughly a third of a 22vw side column, so a
# full 1080p/4K frame would only cost bandwidth over Starlink.
SNAPSHOT_WIDTH = 960
FFMPEG_TIMEOUT_SECS = 15.0

_lock = threading.Lock()


def _captured(path: str) -> dt.datetime:
    """Capture time from the upstream C3Record filename (UTC), falling
    back to the file's mtime if it doesn't parse."""
    try:
        return dt.datetime.strptime(os.path.basename(path), SEGMENT_FORMAT).replace(
            tzinfo=dt.timezone.utc
        )
    except ValueError:
        return dt.datetime.fromtimestamp(os.path.getmtime(path), tz=dt.timezone.utc)


def _load_meta() -> Optional[Dict[str, Any]]:
    try:
        with open(META_PATH, "r", encoding="utf-8") as f:
            meta = json.load(f)
    except (OSError, ValueError):
        return None
    if not os.path.isfile(SNAPSHOT_PATH):
        return None
    return meta


def _extract(src: str) -> bool:
    tmp = SNAPSHOT_PATH + ".tmp.jpg"
    cmd = [
        "ffmpeg",
        "-nostdin",
        "-hide_banner",
        "-loglevel", "error",
        "-i", src,
        "-map", "0:v:0",
        "-frames:v", "1",
        "-vf", f"scale={SNAPSHOT_WIDTH}:-2",
        "-q:v", "4",
        "-y",
        tmp,
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=FFMPEG_TIMEOUT_SECS)
    except (OSError, subprocess.TimeoutExpired) as e:
        logger.warning("stereo snapshot: ffmpeg failed on %s: %s", src, e)
        return False
    if r.returncode != 0 or not os.path.isfile(tmp) or os.path.getsize(tmp) == 0:
        logger.warning(
            "stereo snapshot: ffmpeg rc=%s on %s: %s",
            r.returncode, src, (r.stderr or b"").decode(errors="replace").strip()[-300:],
        )
        try:
            os.remove(tmp)
        except OSError:
            pass
        return False
    # Atomic swap so a concurrent GET never serves a half-written JPEG.
    os.replace(tmp, SNAPSHOT_PATH)
    return True


def get_snapshot(dest_dir: str, running: bool) -> Optional[Dict[str, Any]]:
    """Refresh the cached snapshot if a newer finished segment exists and
    return its metadata, or None if there is no stereo footage yet.

    On extraction failure the previous good snapshot keeps being served."""
    with _lock:
        meta = _load_meta()
        src = latest_finished_segment(dest_dir, running) if dest_dir else None
        if src:
            try:
                mtime = os.path.getmtime(src)
            except OSError:
                mtime = None
            name = os.path.basename(src)
            stale = not meta or meta.get("source") != name or meta.get("source_mtime") != mtime
            if mtime is not None and stale and _extract(src):
                cap = _captured(src)
                meta = {
                    "source": name,
                    "source_mtime": mtime,
                    "captured_iso": cap.isoformat(),
                    "captured_epoch": cap.timestamp(),
                }
                try:
                    with open(META_PATH, "w", encoding="utf-8") as f:
                        json.dump(meta, f)
                except OSError:
                    logger.exception("stereo snapshot: writing meta failed")
        return meta
