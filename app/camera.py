"""Axis VAPIX HTTP digest client for PTZ, snapshots, and stream profiles."""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote

import requests
from requests.auth import HTTPDigestAuth

logger = logging.getLogger(__name__)


class AxisCamera:
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
        u = quote(self.user, safe="")
        p = quote(self.password, safe="")
        q = f"?streamprofile={streamprofile}" if streamprofile else ""
        return f"rtsp://{u}:{p}@{self.host}:554/axis-media/media.amp{q}"

    def ptz_position(self) -> Dict[str, Any]:
        r = self._get("/axis-cgi/com/ptz.cgi?query=position")
        r.raise_for_status()
        out: Dict[str, Any] = {}
        for line in r.text.strip().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
        return out

    def ptz_continuous(self, pan: float = 0.0, tilt: float = 0.0, zoom: float = 0.0) -> None:
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

    def ensure_defaultfishpond_profile(self) -> None:
        """Set DefaultFishPond (S0) to H.265 1080p @ 15 fps if S0 name matches."""
        params = self.param_list("StreamProfile")
        name0 = params.get("root.StreamProfile.S0.Name", "")
        if name0 == "DefaultFishPond":
            self.param_update(
                {
                    "root.StreamProfile.S0.Parameters": "videocodec=h265&resolution=1920x1080&fps=15&videobitratemode=vbr&videozprofile=storage",
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
        params = self.param_list("StreamProfile")
        for i in range(26):
            sid = f"S{i}"
            n = params.get(f"root.StreamProfile.{sid}.Name", "")
            if not n.strip():
                return f"root.StreamProfile.{sid}"
        return None

    def ensure_livepreview_profile(self) -> Tuple[bool, str]:
        """
        Ensure stream profile 'livepreview' exists (720p H.264).
        Returns (ok, message).
        """
        if self.find_stream_profile_slot("livepreview"):
            return True, "livepreview profile already present"
        slot = self.next_free_stream_profile_slot()
        if not slot:
            return False, "No free StreamProfile slot"
        self.param_update(
            {
                f"{slot}.Name": "livepreview",
                f"{slot}.Description": "Kaumaui Cam live WebRTC preview",
                f"{slot}.Parameters": "videocodec=h264&resolution=1280x720&fps=25&videobitratemode=vbr&videokeyframeinterval=50",
            }
        )
        return True, f"Created livepreview on {slot}"
