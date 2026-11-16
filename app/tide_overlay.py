"""Current Hilo Bay tide burned into the camera video.

Same mechanism as ``wq_overlay``: one text overlay drawn by the Axis
camera's encoder (Dynamic Overlay API), so it reaches the stream-copied
YouTube H.264, the local preview and recordings at no CPU cost to the Pi.
The line holds the newest observed level from ``tide.latest()``, whether
it's rising or falling, the next predicted high/low, and the time NOAA
measured the level.

It sits top-right: the camera's clock overlay is top-left and the water
quality line runs along the bottom-left. Our overlay is found by its
fixed text prefix (``OVERLAY_PREFIX``), so it survives restarts and camera
reboots; other overlays are never touched.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, Optional

import tide

logger = logging.getLogger(__name__)

OVERLAY_PREFIX = "Tide"
POSITION = "topRight"
FONT_SIZE = 32

# Re-apply even without a tide wake, to restore the overlay after a
# camera reboot and to drop a level that has gone stale.
INTERVAL_S = 600.0
RETRY_S = 120.0

HST_OFFSET_S = -10 * 3600
EVENT_NAMES = {"H": "high", "L": "low"}

_get_cfg: Optional[Callable[[], Dict[str, Any]]] = None
_get_camera: Optional[Callable[[], Any]] = None
_thread: Optional[threading.Thread] = None
_wake = threading.Event()
_lock = threading.Lock()
_status: Dict[str, Any] = {"text": None, "applied_ts": None, "error": None, "error_ts": None}


def _fmt_hm(ts: float) -> str:
    t = time.gmtime(ts + HST_OFFSET_S)
    return f"{t.tm_hour:02d}:{t.tm_min:02d}"


def build_text(latest: Dict[str, Any]) -> Optional[str]:
    """Overlay line for ``tide.latest()``, or None if there's no current level.

    ASCII only, with ``%`` escaped because Axis expands ``%``-modifiers in
    overlay text (see ``wq_overlay.build_text``).
    """
    latest = latest or {}
    level = latest.get("level_m")
    if level is None or latest.get("stale"):
        return None
    head = f"{OVERLAY_PREFIX} {level:.2f} m MLLW"
    if latest.get("trend"):
        head += f", {latest['trend']}"
    parts = [head]
    nxt = latest.get("next_event")
    if nxt:
        parts.append(f"next {EVENT_NAMES.get(nxt['type'], nxt['type'])} {nxt['level_m']:.2f} m {_fmt_hm(nxt['ts'])}")
    parts.append(f"{_fmt_hm(latest['ts'])} HST")
    return " | ".join(parts).replace("%", "%%")


def _apply(cam: Any, text: Optional[str]) -> None:
    if text is None:
        for i in cam.overlay_remove_prefixed(OVERLAY_PREFIX):
            logger.info("tide_overlay: removed overlay %d", i)
        return
    i = cam.overlay_upsert_text(OVERLAY_PREFIX, text, position=POSITION, font_size=FONT_SIZE)
    if i is not None:
        logger.info("tide_overlay: added overlay %d", i)


def _tick() -> None:
    cfg = _get_cfg() if _get_cfg else {}
    enabled = bool(cfg.get("tide_overlay_enabled", True)) and bool(cfg.get("tide_enabled", True))
    text = build_text(tide.latest()) if enabled else None
    cam = _get_camera() if _get_camera else None
    if cam is None:
        raise RuntimeError("no camera configured")
    _apply(cam, text)
    with _lock:
        if text != _status["text"]:
            logger.info("tide_overlay: %s", text or "off")
        _status.update(text=text, applied_ts=time.time(), error=None, error_ts=None)


def _loop() -> None:
    while True:
        try:
            _tick()
            wait = INTERVAL_S
        except Exception as e:
            with _lock:
                first = _status["error"] is None
                _status.update(error=str(e), error_ts=time.time())
            if first:
                logger.warning("tide_overlay: %s", e)
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
    _thread = threading.Thread(target=_loop, daemon=True, name="tide-overlay")
    _thread.start()


def poke() -> None:
    _wake.set()


def status() -> Dict[str, Any]:
    with _lock:
        return dict(_status)
