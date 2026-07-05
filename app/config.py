"""JSON config persisted under /app/data/config.json."""

from __future__ import annotations

import json
import os
import threading
from copy import deepcopy
from typing import Any, Dict, List

from scheduler import migrate_legacy_schedule, normalize_recordings_cycle

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
    # Recording cycle inside the fixed 7:45 AM - 6:00 PM HST daytime
    # window. Each day the recorder records for ``record_secs``, pauses
    # for ``pause_secs``, and repeats until the window closes. See
    # scheduler.RECORDING_WINDOW_{START,STOP}_MIN and recording_active().
    # Defaults produce ~205 clips totalling ~3h 25m per day; ``enabled``
    # is false so the recorder stays off until the operator opts in.
    "recordings_cycle": {
        "enabled": False,
        "record_secs": 60,
        "pause_secs": 120,
    },
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
    #
    # The watchdog has two modes -- see _scheduler_loop in app/main.py:
    #   * Kickoff (first ``youtube_health_kickoff_grace_secs`` of a
    #     broadcast attempt): be patient with YouTube taking its time
    #     to register the stream, but bounce immediately if the
    #     internet ping monitor reports the link is down (a wedged
    #     ffmpeg started during a marginal link rarely recovers cleanly,
    #     a fresh start is more reliable).
    #   * Post-kickoff: tolerate brief Starlink outages -- ffmpeg can
    #     ride them out and YouTube usually reclaims the broadcast on
    #     its own. Only after the link has been steady for
    #     ``youtube_health_post_link_recovery_secs`` do we re-evaluate
    #     YouTube health and bounce if the broadcast is still not live.
    "youtube_channel_url": "",
    "youtube_health_autorestart": True,
    "youtube_health_unhealthy_grace_secs": 90.0,
    "youtube_health_min_session_age_secs": 60.0,
    "youtube_health_kickoff_grace_secs": 360.0,
    "youtube_health_post_link_recovery_secs": 60.0,
    # YouTube Data API v3 broadcast lifecycle manager (see
    # app/youtube_api.py and docs/youtube-api-setup.md).
    #
    # With OAuth connected + ``youtube_api_mode`` true, the extension
    # inserts one broadcast per HST calendar day, binds it to a
    # persistent liveStream, and transitions it to live/complete on the
    # right schedule -- so streams start reliably every day with no
    # manual YouTube Studio work. When disabled or not connected, the
    # extension falls back to the legacy pasted ``youtube_stream_key``
    # path (which requires the operator to arm a broadcast in Studio
    # manually).
    #
    # Client ID / secret come from a Google Cloud OAuth client of type
    # "TVs and Limited Input devices" -- see the setup guide for the
    # (one-time) Cloud Console walkthrough. Refresh tokens are
    # persisted separately in /app/data/youtube_oauth.json (not in
    # config.json) so they can be file-mode 0600.
    "youtube_oauth_client_id": "",
    "youtube_oauth_client_secret": "",
    "youtube_api_mode": False,
    "youtube_broadcast_title_template": "Kaumaui Cam - {date}",
    "youtube_broadcast_privacy": "public",
    # Victron solar logger (see app/solar.py). Polls the on-board
    # ESPHome device (Fishpond) at ``solar_host`` every
    # ``solar_interval_secs`` and appends one row to /app/data/solar.csv.
    # ``timestamp_iso`` is always the first column; the file is
    # cumulative (append-only, no rotation) and downloadable / deletable
    # from the Settings page.
    "solar_enabled": True,
    "solar_host": "192.168.20.66",
    "solar_interval_secs": 60.0,
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
        # Migration: earlier versions of the extension used a 15-min-slot
        # ``recordings_schedule`` (same shape as youtube_schedule). The
        # recordings tab has since been simplified to a fixed daytime
        # window + record/pause cycle. If we find the legacy key on
        # disk, seed the new ``recordings_cycle`` (preserving only the
        # ``enabled`` flag; the slot pattern doesn't map cleanly to a
        # single duration/pause) and drop the old key so subsequent
        # saves stay clean.
        legacy_rs = data.get("recordings_schedule")
        if "recordings_cycle" not in data and isinstance(legacy_rs, dict):
            merged["recordings_cycle"] = normalize_recordings_cycle({
                **merged["recordings_cycle"],
                "enabled": bool(legacy_rs.get("enabled", False)),
            })
        elif "recordings_cycle" in data:
            merged["recordings_cycle"] = normalize_recordings_cycle(
                {**merged["recordings_cycle"], **(data.get("recordings_cycle") or {})}
            )
        else:
            merged["recordings_cycle"] = normalize_recordings_cycle(
                merged["recordings_cycle"]
            )
        merged.pop("recordings_schedule", None)
        return merged


def save(cfg: Dict[str, Any]) -> None:
    with _lock:
        _ensure_dir()
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)


def update(partial: Dict[str, Any]) -> Dict[str, Any]:
    cfg = load()
    for k, v in partial.items():
        if k == "youtube_schedule" and isinstance(v, dict):
            merged = {**cfg.get(k, {}), **v}
            cfg[k] = migrate_legacy_schedule(merged)
        elif k == "recordings_cycle" and isinstance(v, dict):
            merged = {**cfg.get(k, {}), **v}
            cfg[k] = normalize_recordings_cycle(merged)
        elif k == "recordings_schedule":
            # Old clients may still POST this key -- silently ignore
            # rather than 400ing so a stale browser tab doesn't wedge
            # settings saves.
            continue
        else:
            cfg[k] = v
    # Guarantee the legacy key never leaks back onto disk via a
    # partial update (paranoia; ``load()`` already strips it).
    cfg.pop("recordings_schedule", None)
    save(cfg)
    return cfg
