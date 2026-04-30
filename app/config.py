"""JSON config persisted under /app/data/config.json."""

from __future__ import annotations

import json
import os
import threading
from copy import deepcopy
from typing import Any, Dict, List

from scheduler import migrate_legacy_schedule

_ALL_DAYS: List[str] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

DEFAULT_SCHEDULE: Dict[str, Any] = {
    "enabled": False,
    "days": list(_ALL_DAYS),
    "slots": [],
}

DEFAULT_CONFIG: Dict[str, Any] = {
    "camera_host": "192.168.20.20",
    "camera_user": "root",
    "camera_pass": "campass",
    "youtube_stream_key": "",
    "youtube_schedule": deepcopy(DEFAULT_SCHEDULE),
    "recordings_schedule": deepcopy(DEFAULT_SCHEDULE),
    "recordings_storage": "auto",  # auto | usb | sd
    "recordings_profile": "DefaultFishPond",
    "monthly_quota_gb": 100.0,
    "bandwidth_overhead_pct": 3.0,
    # YouTube broadcast health monitor (see app/youtube_monitor.py).
    # ``youtube_channel_url`` is the public-channel-page URL (any of
    # /@handle, /@handle/streams, /@handle/live, /channel/UC..., bare
    # @handle) whose ``/live`` endpoint we poll to confirm the broadcast
    # is actually live to viewers. Empty string disables the monitor.
    # ``youtube_health_autorestart`` enables the supervisor watchdog
    # that force-restarts ffmpeg when YouTube has confirmed the stream
    # is not live for ``youtube_health_unhealthy_grace_secs`` while we
    # expect it to be -- i.e. the "Preparing stream" lockup recovery.
    "youtube_channel_url": "",
    "youtube_health_autorestart": True,
    "youtube_health_unhealthy_grace_secs": 90.0,
    "youtube_health_min_session_age_secs": 60.0,
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
        if "youtube_schedule" in data and isinstance(data["youtube_schedule"], dict):
            merged["youtube_schedule"] = migrate_legacy_schedule(
                {**deepcopy(DEFAULT_SCHEDULE), **data["youtube_schedule"]}
            )
        else:
            merged["youtube_schedule"] = migrate_legacy_schedule(deepcopy(merged["youtube_schedule"]))
        if "recordings_schedule" in data and isinstance(data["recordings_schedule"], dict):
            merged["recordings_schedule"] = migrate_legacy_schedule(
                {**deepcopy(DEFAULT_SCHEDULE), **data["recordings_schedule"]}
            )
        else:
            merged["recordings_schedule"] = migrate_legacy_schedule(deepcopy(merged["recordings_schedule"]))
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
            merged = {**cfg.get(k, {}), **v}
            cfg[k] = migrate_legacy_schedule(merged)
        else:
            cfg[k] = v
    save(cfg)
    return cfg
