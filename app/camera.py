"""Axis VAPIX HTTP digest client for PTZ, snapshots, and stream profiles.

Also owns RTSP URL resolution for Axis-built and free-form custom URLs
(see ``resolve_rtsp_url``).
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

import requests
from requests.auth import HTTPDigestAuth

logger = logging.getLogger(__name__)

# Purpose keys passed to resolve_rtsp_url / exposed on GET /api/config.
RTSP_PURPOSE_LIVE = "livepreview"
RTSP_PURPOSE_YOUTUBE = "youtubelive"
RTSP_PURPOSE_RECORD = "record"

_CUSTOM_URL_KEYS = {
    RTSP_PURPOSE_LIVE: "rtsp_url_live",
    RTSP_PURPOSE_YOUTUBE: "rtsp_url_youtube",
    RTSP_PURPOSE_RECORD: "rtsp_url_record",
}


def axis_rtsp_url(
    host: str, user: str, password: str, streamprofile: Optional[str] = None
) -> str:
    """Build the Axis VAPIX RTSP URL for an optional stream profile name."""
    u = quote(user or "", safe="")
    p = quote(password or "", safe="")
    q = f"?streamprofile={streamprofile}" if streamprofile else ""
    return f"rtsp://{u}:{p}@{host}:554/axis-media/media.amp{q}"


def dahua_style_rtsp_url(host: str, user: str, password: str, subtype: int = 0) -> str:
    """MC800S5 main/sub stream URL (``/stream0`` main, ``/stream1`` sub)."""
    u = quote(user or "", safe="")
    p = quote(password or "", safe="")
    path = "/stream1" if int(subtype) else "/stream0"
    return f"rtsp://{u}:{p}@{host}:554{path}"


def resolve_rtsp_url(cfg: Dict[str, Any], purpose: str) -> str:
    """
    Resolve the RTSP URL for live preview, YouTube, or recording.

    In ``custom`` mode, returns the matching ``rtsp_url_*`` field. YouTube
    and record fall back to ``rtsp_url_live`` when their own field is
    empty. If still empty (or mode is ``axis``), builds the Axis URL from
    host/user/pass + the appropriate stream profile name.
    """
    mode = str(cfg.get("rtsp_mode") or "axis").strip().lower()
    if mode == "custom":
        key = _CUSTOM_URL_KEYS.get(purpose, "rtsp_url_live")
        url = str(cfg.get(key) or "").strip()
        if not url and purpose != RTSP_PURPOSE_LIVE:
            url = str(cfg.get("rtsp_url_live") or "").strip()
        if url:
            return url

    host = str(cfg.get("camera_host") or "").strip()
    user = str(cfg.get("camera_user") or "")
    password = str(cfg.get("camera_pass") or "")
    if purpose == RTSP_PURPOSE_YOUTUBE:
        profile: Optional[str] = "youtubelive"
    elif purpose == RTSP_PURPOSE_RECORD:
        profile = str(cfg.get("recordings_profile") or "DefaultFishPond").strip() or None
    else:
        profile = "livepreview"
    return axis_rtsp_url(host, user, password, profile)


def resolved_rtsp_urls(cfg: Dict[str, Any]) -> Dict[str, str]:
    """Map of purpose → URL currently in effect (for Settings UI display)."""
    return {
        RTSP_PURPOSE_LIVE: resolve_rtsp_url(cfg, RTSP_PURPOSE_LIVE),
        RTSP_PURPOSE_YOUTUBE: resolve_rtsp_url(cfg, RTSP_PURPOSE_YOUTUBE),
        RTSP_PURPOSE_RECORD: resolve_rtsp_url(cfg, RTSP_PURPOSE_RECORD),
    }


def is_axis_mode(cfg: Dict[str, Any]) -> bool:
    return str(cfg.get("rtsp_mode") or "axis").strip().lower() != "custom"


def camera_control(cfg: Dict[str, Any]):
    """
    Return the lens/PTZ backend for the current config.

    Axis mode → ``AxisCamera`` (VAPIX). Custom mode → ``VendorLensCamera``
    (MC800S5_AF ``/setPTZCmd`` zoom/focus).
    """
    host = str(cfg.get("camera_host") or "").strip()
    user = str(cfg.get("camera_user") or "")
    password = str(cfg.get("camera_pass") or "")
    if is_axis_mode(cfg):
        return AxisCamera(host, user, password)
    from vendor_lens import VendorLensCamera

    return VendorLensCamera(host, user, password)


class AxisCamera:
    backend = "axis"

    def __init__(self, host: str, user: str, password: str, timeout: float = 10.0):
        self.base = f"http://{host}".rstrip("/")
        self.host = host
        self.user = user
        self.password = password
        self.auth = HTTPDigestAuth(user, password)
        self.timeout = timeout

    def _get(self, path: str, **kwargs) -> requests.Response:
        url = f"{self.base}{path}"
        return requests.get(url, auth=self.auth, timeout=self.timeout, **kwargs)

    def _post(self, path: str, **kwargs) -> requests.Response:
        url = f"{self.base}{path}"
        return requests.post(url, auth=self.auth, timeout=self.timeout, **kwargs)

    def rtsp_url(self, streamprofile: Optional[str] = None) -> str:
        return axis_rtsp_url(self.host, self.user, self.password, streamprofile)

    def ptz_position(self) -> Dict[str, Any]:
        r = self._get("/axis-cgi/com/ptz.cgi?query=position")
        r.raise_for_status()
        out: Dict[str, Any] = {"backend": self.backend}
        for line in r.text.strip().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
        return out

    def ptz_continuous(
        self,
        pan: float = 0.0,
        tilt: float = 0.0,
        zoom: float = 0.0,
        focus: float = 0.0,
    ) -> None:
        del focus  # Axis continuous focus not used; AF is toggled separately
        r = self._get(
            f"/axis-cgi/com/ptz.cgi?continuouspantiltmove={pan},{tilt}&continuouszoommove={zoom}"
        )
        r.raise_for_status()

    def ptz_stop(self) -> None:
        r = self._get("/axis-cgi/com/ptz.cgi?continuouspantiltmove=0,0&continuouszoommove=0")
        r.raise_for_status()

    def ptz_absolute(self, pan: Optional[float] = None, tilt: Optional[float] = None, zoom: Optional[int] = None) -> None:
        parts = []
        if pan is not None:
            parts.append(f"pan={pan}")
        if tilt is not None:
            parts.append(f"tilt={tilt}")
        if zoom is not None:
            parts.append(f"zoom={zoom}")
        if not parts:
            return
        r = self._get("/axis-cgi/com/ptz.cgi?" + "&".join(parts))
        r.raise_for_status()

    def ptz_goto_preset(self, name: str) -> None:
        r = self._get("/axis-cgi/com/ptz.cgi", params={"gotoserverpresetname": name})
        r.raise_for_status()

    def autofocus(self, on: bool = True) -> None:
        r = self._get(f"/axis-cgi/com/ptz.cgi?autofocus={'on' if on else 'off'}")
        r.raise_for_status()

    def snapshot_jpeg(self) -> bytes:
        r = self._get("/axis-cgi/jpg/image.cgi?resolution=1280x720")
        r.raise_for_status()
        return r.content

    def param_list(self, group: str) -> Dict[str, str]:
        r = self._get(f"/axis-cgi/param.cgi?action=list&group={group}")
        r.raise_for_status()
        kv: Dict[str, str] = {}
        for line in r.text.strip().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                kv[k.strip()] = v.strip()
        return kv

    def param_update(self, updates: Dict[str, str]) -> None:
        params: Dict[str, str] = {"action": "update"}
        params.update(updates)
        r = self._get("/axis-cgi/param.cgi", params=params)
        r.raise_for_status()

    def stream_profile_add(self) -> Optional[str]:
        """
        Create a new (empty) StreamProfile slot via VAPIX `action=add`.
        Returns the slot id (e.g. 'S2') on success, else None.

        Older Axis firmwares don't pre-allocate StreamProfile.S0..S25 — the
        slot groups only exist after an explicit `add` call. The response
        body looks like 'S2 OK' (or contains '# Error: ...' on failure).
        """
        r = self._post(
            "/axis-cgi/param.cgi",
            data={
                "action": "add",
                "template": "streamprofile",
                "group": "StreamProfile",
            },
        )
        r.raise_for_status()
        body = r.text or ""
        m = re.search(r"\b(S\d+)\s+OK\b", body)
        if not m:
            logger.warning("StreamProfile add unexpected response: %r", body)
            return None
        return m.group(1)

    def ensure_defaultfishpond_profile(self) -> None:
        """Set DefaultFishPond (S0) to motion-friendly H.265 1080p30 if S0 name matches."""
        params = self.param_list("StreamProfile")
        name0 = params.get("root.StreamProfile.S0.Name", "")
        if name0 == "DefaultFishPond":
            self.param_update(
                {
                    "root.StreamProfile.S0.Parameters": "videocodec=h265&resolution=1920x1080&fps=30&videobitratemode=mbr&videomaxbitrate=4500&videokeyframeinterval=60&videocompression=20",
                }
            )
            logger.info("Updated DefaultFishPond profile parameters")

    def find_stream_profile_slot(self, profile_name: str) -> Optional[str]:
        """Return param key prefix e.g. root.StreamProfile.S3 if name found, else None."""
        params = self.param_list("StreamProfile")
        for key, val in params.items():
            m = re.match(r"^root\.StreamProfile\.(S\d+)\.Name$", key)
            if m and val == profile_name:
                return f"root.StreamProfile.{m.group(1)}"
        return None

    def next_free_stream_profile_slot(self) -> Optional[str]:
        """
        Return a slot key prefix for an existing-but-unused profile entry.
        A slot is considered free if its Name is blank OR if Name is the
        Axis-generated placeholder (e.g. 'profile3') AND Parameters is empty
        — both are easy targets to overwrite without trampling user data.
        """
        params = self.param_list("StreamProfile")
        for i in range(26):
            sid = f"S{i}"
            name = params.get(f"root.StreamProfile.{sid}.Name", "").strip()
            prm = params.get(f"root.StreamProfile.{sid}.Parameters", "").strip()
            if not name:
                return f"root.StreamProfile.{sid}"
            if re.fullmatch(r"profile\d+", name) and not prm:
                return f"root.StreamProfile.{sid}"
        return None

    def _ensure_stream_profile(
        self, name: str, description: str, parameters: str
    ) -> Tuple[bool, str]:
        """
        Generic ensure: if a profile named `name` exists, make sure its
        Parameters match `parameters` (and re-apply if not). Otherwise
        allocate a slot — preferring an empty/placeholder slot, falling
        back to VAPIX `action=add` for firmwares that don't pre-allocate
        StreamProfile.Sx groups — and write Name/Description/Parameters.
        """
        existing = self.find_stream_profile_slot(name)
        if existing:
            current = self.param_list(existing.split("root.")[-1])
            cur_params = current.get(f"{existing}.Parameters", "")
            if cur_params == parameters:
                return True, f"{name} profile already present on {existing}"
            self.param_update({f"{existing}.Parameters": parameters})
            return True, f"Refreshed {name} parameters on {existing}"

        slot = self.next_free_stream_profile_slot()
        if not slot:
            new_sid = self.stream_profile_add()
            if not new_sid:
                return False, "Camera refused to allocate a StreamProfile slot"
            slot = f"root.StreamProfile.{new_sid}"

        self.param_update(
            {
                f"{slot}.Name": name,
                f"{slot}.Description": description,
                f"{slot}.Parameters": parameters,
            }
        )
        return True, f"Created {name} on {slot}"

    def ensure_livepreview_profile(self) -> Tuple[bool, str]:
        """Ensure stream profile 'livepreview' exists (720p H.264)."""
        return self._ensure_stream_profile(
            name="livepreview",
            description="Wailoa Cam live WebRTC preview",
            parameters="videocodec=h264&resolution=1280x720&fps=25&videobitratemode=vbr&videokeyframeinterval=50",
        )

    def ensure_youtubelive_profile(self) -> Tuple[bool, str]:
        """
        Ensure stream profile 'youtubelive' exists.

        Targets a YouTube-friendly H.264 1080p30 stream with a 4500
        Kbps MBR cap (well under the boat's 8 Mbps Starlink uplink),
        a 2 s keyframe interval (60 frames at 30 fps) per YouTube's
        low-latency ingest recommendation, and `videocompression=0`
        (lowest compression / highest quality). The low-compression
        knob is important for quiet pond scenes: H.264 produces very
        few bits on static water no matter the cap, so we ask the
        encoder to spend as many bits as it can within the cap to
        keep YouTube's ingest from going below its acceptance
        threshold.

        Note: setting `videobitratemode=cbr` here triggered RTSP 400
        Bad Request on this Axis firmware -- channel-level CBR (set
        via `root.Image.I0.RateControl.Mode`) is the only working
        path for true CBR on this camera, see
        `ensure_channel_cbr()`.
        """
        return self._ensure_stream_profile(
            name="youtubelive",
            description="Wailoa Cam YouTube live H.264 1080p30 4.5 Mbps cap",
            parameters=(
                "videocodec=h264"
                "&resolution=1920x1080"
                "&fps=30"
                "&videobitratemode=mbr"
                "&videomaxbitrate=4500"
                "&videokeyframeinterval=60"
                "&videocompression=0"
            ),
        )
