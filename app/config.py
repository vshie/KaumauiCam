"""JSON config persisted under /app/data/config.json."""

from __future__ import annotations

import json
import os
import threading
from copy import deepcopy
from typing import Any, Dict

DEFAULT_CONFIG: Dict[str, Any] = {
    "camera_host": "192.168.20.20",
    "camera_user": "root",
    "camera_pass": "campass",
    "youtube_stream_key": "",
    "youtube_schedule": {
        "enabled": False,
        "window_start": "06:00",
        "window_stop": "18:00",
        "interval_min": 60,
        "duration_min": 20,
    },
    "recordings_schedule": {
        "enabled": False,
        "window_start": "06:00",
        "window_stop": "18:00",
        "interval_min": 60,
        "duration_min": 30,
    },
    "recordings_storage": "auto",  # auto | usb | sd
    "recordings_profile": "DefaultFishPond",
    "monthly_quota_gb": 100.0,
    "bandwidth_overhead_pct": 3.0,
}

CONFIG_PATH = os.environ.get("KAUMAUI_CONFIG", "/app/data/config.json")
_lock = threading.Lock()


def _ensure_dir() -> None:
    d = os.path.dirname(CONFIG_PATH)
    if d:
        os.makedirs(d, exist_ok=True)


def load() -> Dict[str, Any]:
    with _lock:
        if not os.path.isfile(CONFIG_PATH):
            cfg = deepcopy(DEFAULT_CONFIG)
            _ensure_dir()
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2)
            return cfg
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        merged = deepcopy(DEFAULT_CONFIG)
        merged.update(data)
        if "youtube_schedule" in data:
            merged["youtube_schedule"] = {**DEFAULT_CONFIG["youtube_schedule"], **data["youtube_schedule"]}
        if "recordings_schedule" in data:
            merged["recordings_schedule"] = {
                **DEFAULT_CONFIG["recordings_schedule"],
                **data["recordings_schedule"],
            }
        return merged


def save(cfg: Dict[str, Any]) -> None:
    with _lock:
        _ensure_dir()
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)


def update(partial: Dict[str, Any]) -> Dict[str, Any]:
    cfg = load()
    for k, v in partial.items():
        if k in ("youtube_schedule", "recordings_schedule") and isinstance(v, dict):
            cfg[k] = {**cfg.get(k, {}), **v}
        else:
            cfg[k] = v
    save(cfg)
    return cfg
