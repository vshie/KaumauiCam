"""Camera heading and current Hilo Bay tide, burned into the top-right of
the camera video.

Same mechanism as ``wq_overlay``: one text overlay drawn by the Axis
camera's encoder (Dynamic Overlay API), so it reaches the stream-copied
YouTube H.264, the local preview and recordings at no CPU cost to the Pi.
The text is two lines, e.g.::

    Tide 0.62 m MLLW, falling | next low 0.24 m 16:22 | 10:48 HST
    Camera Facing: NW

- Tide: newest observed level from ``tide.latest()``, whether it's
  rising or falling, the next predicted high/low, and the time NOAA
  measured the level. Dropped when NOAA's data is stale.
- ``Camera Facing: NW``: estimated compass direction of the view, from the PTZ
  pan angle (``heading.estimate``). Re-read every ``HEADING_POLL_S`` and
  right after a PTZ stop/home, so it follows the operator moving the
  camera.

Both lines share one overlay because the Axis positions are fixed
corners: a second top-right overlay would sit on top of the first rather
than stack under it. A ``\n`` in the text does give a second line in
the same box (checked on the M5526-E, AXIS OS 11.7, 2026-11-17; lines
are left-aligned in the box). The camera's clock overlay is top-left and
the water-quality line bottom-left. Ours is found by its text starting
with one of ``OVERLAY_PREFIXES`` (whichever line leads), so it survives
restarts and camera reboots; other overlays are never touched.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import heading
import tide

logger = logging.getLogger(__name__)

HEADING_PREFIX = "Camera Facing:"
TIDE_PREFIX = "Tide"
OVERLAY_PREFIXES = (HEADING_PREFIX, TIDE_PREFIX)
POSITION = "topRight"
FONT_SIZE = 32

# Pan is re-read this often so the heading follows the camera. The camera
# is only written to when the text changes, or every REAPPLY_S to restore
# the overlay after a camera reboot.
HEADING_POLL_S = 30.0
REAPPLY_S = 600.0
RETRY_S = 120.0

HST_OFFSET_S = -10 * 3600
EVENT_NAMES = {"H": "high", "L": "low"}

_get_cfg: Optional[Callable[[], Dict[str, Any]]] = None
_get_camera: Optional[Callable[[], Any]] = None
_thread: Optional[threading.Thread] = None
_wake = threading.Event()
_lock = threading.Lock()
_status: Dict[str, Any] = {
    "text": None,
    "applied_ts": None,
    "heading": None,
    "error": None,
    "error_ts": None,
}


def _fmt_hm(ts: float) -> str:
    t = time.gmtime(ts + HST_OFFSET_S)
    return f"{t.tm_hour:02d}:{t.tm_min:02d}"


def tide_text(latest: Dict[str, Any]) -> Optional[str]:
    """Tide part of the line for ``tide.latest()``, or None without a current level."""
    latest = latest or {}
    level = latest.get("level_m")
    if level is None or latest.get("stale"):
        return None
    head = f"{TIDE_PREFIX} {level:.2f} m MLLW"
    if latest.get("trend"):
        head += f", {latest['trend']}"
    parts = [head]
    nxt = latest.get("next_event")
    if nxt:
        parts.append(f"next {EVENT_NAMES.get(nxt['type'], nxt['type'])} {nxt['level_m']:.2f} m {_fmt_hm(nxt['ts'])}")
    parts.append(f"{_fmt_hm(latest['ts'])} HST")
    return " | ".join(parts)


def build_text(heading_code: Optional[str], latest: Optional[Dict[str, Any]]) -> Optional[str]:
    """Overlay text (tide line, then heading line), or None when neither
    has anything to show.

    ASCII only (the camera's overlay font is the safe bet there), with
    ``%`` escaped because Axis expands ``%``-modifiers (``%F``, ``%X``...)
    in overlay text.
    """
    lines: List[str] = []
    t = tide_text(latest) if latest is not None else None
    if t:
        lines.append(t)
    if heading_code:
        lines.append(f"{HEADING_PREFIX} {heading_code}")
    if not lines:
        return None
    return "\n".join(lines).replace("%", "%%")


def _apply(cam: Any, text: Optional[str]) -> None:
    if text is None:
        for i in cam.overlay_remove_prefixed(OVERLAY_PREFIXES):
            logger.info("topright_overlay: removed overlay %d", i)
        return
    i = cam.overlay_upsert_text(OVERLAY_PREFIXES, text, position=POSITION, font_size=FONT_SIZE)
    if i is not None:
        logger.info("topright_overlay: added overlay %d", i)


def _tick() -> None:
    cfg = _get_cfg() if _get_cfg else {}
    cam = _get_camera() if _get_camera else None
    if cam is None:
        raise RuntimeError("no camera configured")
    est = None
    if cfg.get("heading_overlay_enabled", True):
        est = heading.estimate(heading.pan_of(cam.ptz_position()), cfg)
    show_tide = bool(cfg.get("tide_overlay_enabled", True)) and bool(cfg.get("tide_enabled", True))
    text = build_text(est["code"] if est else None, tide.latest() if show_tide else None)
    with _lock:
        prev, applied_ts = _status["text"], _status["applied_ts"]
    if text != prev or applied_ts is None or time.time() - applied_ts >= REAPPLY_S:
        _apply(cam, text)
        applied_ts = time.time()
        if text != prev:
            logger.info("topright_overlay: %s", text or "off")
    with _lock:
        _status.update(text=text, applied_ts=applied_ts, heading=est, error=None, error_ts=None)


def _loop() -> None:
    while True:
        try:
            _tick()
            wait = HEADING_POLL_S
        except Exception as e:
            with _lock:
                first = _status["error"] is None
                # Force a full re-apply once the camera is back.
                _status.update(error=str(e), error_ts=time.time(), applied_ts=None)
            if first:
                logger.warning("topright_overlay: %s", e)
            wait = RETRY_S
        _wake.wait(timeout=wait)
        _wake.clear()


def start(get_cfg: Callable[[], Dict[str, Any]], get_camera: Callable[[], Any]) -> None:
    """Spawn the overlay thread and hook it to new tide data. Idempotent."""
    global _thread, _get_cfg, _get_camera
    _get_cfg = get_cfg
    _get_camera = get_camera
    tide.set_on_new_data(poke)
    if _thread and _thread.is_alive():
        return
    _thread = threading.Thread(target=_loop, daemon=True, name="topright-overlay")
    _thread.start()


def poke() -> None:
    _wake.set()


def status() -> Dict[str, Any]:
    with _lock:
        return dict(_status)
