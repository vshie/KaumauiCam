"""Estimated compass heading of the camera's view.

The AXIS M5526-E has no compass: ``ptz.cgi?query=position`` reports pan
in degrees (-180..180) from the camera's own mechanical zero, and its
Geolocation API heading is an unset, operator-entered field. So the
heading is pan plus a calibration offset (``heading_pan_offset_deg``),
taken once by pointing the camera at a known direction and recording
what it was (Settings -> Camera heading, ``/api/heading/calibrate``).

Pan grows clockwise seen from above (panning right turns the view
toward the east when it faces north), so heading = pan + offset.

The result is only shown as a 16-point compass code (N, NNE, ... NNW):
the calibration itself is "it's facing NW", good to a compass point
(22.5 deg), so degrees would imply precision we don't have.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

POINTS = (
    "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
    "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
)
POINT_DEG = 360.0 / len(POINTS)


def code_for(deg: float) -> str:
    """16-point compass code for a bearing in degrees."""
    return POINTS[int(((deg % 360.0) + POINT_DEG / 2) // POINT_DEG) % len(POINTS)]


def deg_for(code: str) -> float:
    """Centre bearing of a compass code (``ValueError`` if unknown)."""
    return POINTS.index(code.strip().upper()) * POINT_DEG


def offset_for(code: str, pan: float) -> float:
    """Calibration offset that makes ``pan`` read as ``code``."""
    return (deg_for(code) - pan) % 360.0


def estimate(pan: Optional[float], cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """``{"deg", "code"}`` for a pan angle, or None without one."""
    if pan is None:
        return None
    try:
        offset = float(cfg.get("heading_pan_offset_deg", 0.0))
    except (TypeError, ValueError):
        offset = 0.0
    deg = (float(pan) + offset) % 360.0
    return {"deg": round(deg, 1), "code": code_for(deg)}


def pan_of(position: Dict[str, Any]) -> Optional[float]:
    """Pan from ``AxisCamera.ptz_position()`` (string values), or None."""
    try:
        return float(position.get("pan"))
    except (TypeError, ValueError):
        return None
