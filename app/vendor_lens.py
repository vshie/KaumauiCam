"""MC800S5_AF / iCamra vendor lens control via ``POST /setPTZCmd``.

Auth and command format reverse-engineered from the camera web UI
(see docs/CAMERA-API.md). Zoom and focus are continuous motors that
run until ``stop``. There is no lens position feedback and no way to
re-enable autofocus via this API (manual focus cmds use ``*AutoOff``).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import requests
from cryptography.hazmat.decrepit.ciphers.algorithms import TripleDES
from cryptography.hazmat.primitives.ciphers import Cipher, modes

logger = logging.getLogger(__name__)

_DES_KEY = b"WebLogin"


def des_hex(text: str) -> str:
    """DES-ECB encrypt ``text`` under ``WebLogin``, zero-pad, return hex.

    An 8-byte TripleDES key is treated as single-DES (matches the stock
    UI's ``des.js``). Factory ``admin`` / ``123456`` →
    ``52851dbd7918bbae`` / ``a17faccd02661e4c``.
    """
    data = text.encode()
    data += b"\x00" * (-len(data) % 8)
    enc = Cipher(TripleDES(_DES_KEY), modes.ECB()).encryptor()
    return (enc.update(data) + enc.finalize()).hex()


class VendorLensCamera:
    """Zoom / focus only — no pan/tilt head on the MC800S5_AF."""

    backend = "vendor"

    def __init__(self, host: str, user: str, password: str, timeout: float = 5.0):
        self.host = host
        self.user = user
        self.password = password
        self.timeout = timeout
        self.base = f"http://{host}".rstrip("/")
        # Credentials are static ciphertext — compute once per instance.
        self._userid = des_hex(user or "")
        self._passwd = des_hex(password or "")

    def _set_ptz_cmd(self, cmd: str, extra: str = "") -> None:
        body = (
            '<?xml version="1.0"?>'
            '<soap:Envelope xmlns:soap="http://www.w3.org/2001/12/soap-envelope">'
            f"<soap:Header>\t<userid>{self._userid}</userid>"
            f"\t<passwd>{self._passwd}</passwd></soap:Header>"
            f"<soap:Body><xml><cmd>{cmd}</cmd>{extra}</xml></soap:Body>"
            "</soap:Envelope>"
        )
        # Content-Type must be form-urlencoded — text/xml is routed to the
        # ONVIF gSOAP handler and fails validation.
        r = requests.post(
            f"{self.base}/setPTZCmd",
            data=body.encode(),
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=self.timeout,
        )
        # Camera returns 202 with an empty body whether or not the motor
        # moved — never treat status alone as proof of motion.
        if r.status_code not in (200, 202):
            r.raise_for_status()
        logger.debug("vendor setPTZCmd %s -> %s", cmd, r.status_code)

    def ptz_position(self) -> Dict[str, Any]:
        # Nothing on this camera reports lens position.
        return {
            "pan": "—",
            "tilt": "—",
            "zoom": "—",
            "autofocus": "unknown",
            "backend": self.backend,
            "note": "Vendor lens has no position feedback",
        }

    def ptz_continuous(
        self,
        pan: float = 0.0,
        tilt: float = 0.0,
        zoom: float = 0.0,
        focus: float = 0.0,
    ) -> None:
        """Start continuous zoom and/or focus. Pan/tilt are ignored."""
        del pan, tilt  # no P/T head
        # Prefer an explicit focus command when both are set (UI sends one axis).
        if focus:
            self._set_ptz_cmd("FocusFarAutoOff" if focus > 0 else "FocusNearAutoOff")
            return
        if zoom:
            self._set_ptz_cmd("zoomtele" if zoom > 0 else "zoomwide")
            return
        self._set_ptz_cmd("stop")

    def ptz_stop(self) -> None:
        self._set_ptz_cmd("stop")

    def ptz_goto_preset(self, name: str) -> None:
        raise RuntimeError(
            f"Vendor lens presets do not move zoom/focus (requested {name!r})"
        )

    def autofocus(self, on: bool = True) -> None:
        if on:
            raise RuntimeError(
                "Vendor API cannot re-enable autofocus; use Focus −/+ "
                "(those commands leave AF off)"
            )
        # No dedicated AF-off without a focus nudge. Manual focus cmds
        # already carry AutoOff; nothing else to send.
        logger.info("vendor autofocus(off): no-op (AF disables on Focus −/+)")

    def focus_near(self) -> None:
        self._set_ptz_cmd("FocusNearAutoOff")

    def focus_far(self) -> None:
        self._set_ptz_cmd("FocusFarAutoOff")
