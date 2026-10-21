"""Latest Kaumaui water-quality readings burned into the camera video.

The YouTube stream is the Axis camera's own H.264, stream-copied by
ffmpeg (see youtube.py); re-encoding 1080p on the armv7 Pi to draw text
isn't viable. Instead the camera draws it: one text overlay added through
the Axis Dynamic Overlay API (``AxisCamera.overlay_*``), holding the
newest salinity / pH / DO / temperature from ``hydrovu.latest()`` plus
the time HydroVu says they were measured. HydroVu uploads arrive in
batches several hours late, so the measurement time is part of the text
rather than implied to be "now".

Because the camera encodes it, the text also appears in the local
preview and in mono recordings, the same as the camera's existing
top-left "Kaumaui Cam <clock>" overlay. Historical line graphs live only
on the extension's Live tab.

Our overlay is found by its fixed text prefix (``OVERLAY_PREFIX``) rather
than a stored identity, so it survives container restarts and camera
reboots (dynamic overlays don't persist across a camera reboot; the next
tick re-adds it). Other overlays on the camera are never touched.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import hydrovu

logger = logging.getLogger(__name__)

OVERLAY_PREFIX = "Water quality"
POSITION = "bottomLeft"
# Camera font size in pixels at the channel's 1920x1080. The existing
# top-left clock overlay uses 48; a smaller size keeps the longer line
# off most of the picture.
FONT_SIZE = 32

# Re-check even without a HydroVu wake, to restore the overlay after a
# camera reboot and to drop params that age out of hydrovu's active window.
INTERVAL_S = 600.0
RETRY_S = 120.0

# (hydrovu param id, label on the video, decimals). Order is the display
# order the operator asked for.
SHOWN = (
    ("salinity", "Salinity", 2),
    ("ph", "pH", 2),
    ("do", "DO", 2),
    ("temperature", "Temp", 1),
)
UNIT_TEXT = {"psu": "psu", "ph": "", "milligramsPerLiter": "mg/L", "celsius": "C"}

HST_OFFSET_S = -10 * 3600
MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

_get_cfg: Optional[Callable[[], Dict[str, Any]]] = None
_get_camera: Optional[Callable[[], Any]] = None
_thread: Optional[threading.Thread] = None
_wake = threading.Event()
_lock = threading.Lock()
_status: Dict[str, Any] = {"text": None, "applied_ts": None, "error": None, "error_ts": None}


def _fmt_time(ts: float) -> str:
    t = time.gmtime(ts + HST_OFFSET_S)
    return f"{MONTHS[t.tm_mon - 1]} {t.tm_mday} {t.tm_hour:02d}:{t.tm_min:02d} HST"


def build_text(latest: Dict[str, Any]) -> Optional[str]:
    """Overlay line for ``hydrovu.latest()``, or None if nothing to show.

    Kept to ASCII: the camera's overlay font is the safest bet there,
    and ``%`` is escaped because Axis expands ``%``-modifiers (``%F``,
    ``%X``...) in overlay text.
    """
    params = (latest or {}).get("params") or {}
    parts: List[str] = []
    times: List[float] = []
    for pid, label, decimals in SHOWN:
        p = params.get(pid)
        if not p:
            continue
        unit = UNIT_TEXT.get(p.get("unit") or "", "")
        parts.append(f"{label} {p['value']:.{decimals}f}{(' ' + unit) if unit else ''}")
        times.append(float(p["ts"]))
    if not parts:
        return None
    line = f"{OVERLAY_PREFIX} - " + " | ".join(parts) + f" | measured {_fmt_time(max(times))}"
    return line.replace("%", "%%")


def _ours(cam: Any) -> List[int]:
    return [
        int(o["identity"])
        for o in cam.overlay_list_text()
        if str(o.get("text") or "").startswith(OVERLAY_PREFIX)
    ]


def _apply(cam: Any, text: Optional[str]) -> None:
    ids = _ours(cam)
    if text is None:
        for i in ids:
            cam.overlay_remove(i)
            logger.info("wq_overlay: removed overlay %d", i)
        return
    if ids:
        cam.overlay_set_text(ids[0], text)
        # Duplicates only appear if an earlier add raced a list; tidy them.
        for i in ids[1:]:
            cam.overlay_remove(i)
    else:
        i = cam.overlay_add_text(text, position=POSITION, font_size=FONT_SIZE)
        logger.info("wq_overlay: added overlay %d", i)


def _tick() -> None:
    cfg = _get_cfg() if _get_cfg else {}
    enabled = bool(cfg.get("wq_overlay_enabled", True)) and bool(cfg.get("hydrovu_enabled", True))
    text = build_text(hydrovu.latest()) if enabled else None
    cam = _get_camera() if _get_camera else None
    if cam is None:
        raise RuntimeError("no camera configured")
    _apply(cam, text)
    with _lock:
        if text != _status["text"]:
            logger.info("wq_overlay: %s", text or "off")
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
                logger.warning("wq_overlay: %s", e)
            wait = RETRY_S
        _wake.wait(timeout=wait)
        _wake.clear()


def start(get_cfg: Callable[[], Dict[str, Any]], get_camera: Callable[[], Any]) -> None:
    """Spawn the overlay thread and hook it to new HydroVu data. Idempotent."""
    global _thread, _get_cfg, _get_camera
    _get_cfg = get_cfg
    _get_camera = get_camera
    hydrovu.set_on_new_data(poke)
    if _thread and _thread.is_alive():
        return
    _thread = threading.Thread(target=_loop, daemon=True, name="wq-overlay")
    _thread.start()


def poke() -> None:
    _wake.set()


def status() -> Dict[str, Any]:
    with _lock:
        return dict(_status)
