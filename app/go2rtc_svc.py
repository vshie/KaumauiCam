"""Render go2rtc.yaml and supervise go2rtc process."""

from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
from typing import Optional

import yaml

logger = logging.getLogger(__name__)

GO2RTC_BIN = os.environ.get("GO2RTC_BIN", "/usr/local/bin/go2rtc")
CONFIG_PATH = "/app/data/go2rtc.yaml"


def render_config(rtsp_url_livepreview: str) -> None:
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    cfg = {
        "api": {"listen": "127.0.0.1:1984"},
        "webrtc": {"listen": ":8555"},
        "streams": {"livepreview": [rtsp_url_livepreview]},
    }
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(cfg, f, default_flow_style=False)


class Go2RtcSupervisor:
    def __init__(self) -> None:
        self._proc: Optional[subprocess.Popen] = None
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._lock = threading.Lock()

    def _loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                if not os.path.isfile(CONFIG_PATH):
                    time.sleep(1)
                    continue
                # Do not use PIPE for stderr: go2rtc can be very chatty when RTSP is down;
                # an unread PIPE fills (~64KiB) and blocks the child.
                self._proc = subprocess.Popen(
                    [GO2RTC_BIN, "-config", CONFIG_PATH],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    stdin=subprocess.DEVNULL,
                )
                logger.info("go2rtc started pid=%s", self._proc.pid)
                backoff = 1.0
                while self._proc.poll() is None and not self._stop.is_set():
                    time.sleep(0.5)
                if self._proc.poll() is not None:
                    logger.warning("go2rtc exited code=%s", self._proc.returncode)
            except Exception as e:
                logger.error("go2rtc supervisor: %s", e)
            finally:
                self._proc = None
            if self._stop.is_set():
                break
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(target=self._loop, daemon=True, name="go2rtc-sup")
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            proc = self._proc
            self._proc = None
        if proc and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        if self._thread:
            self._thread.join(timeout=8)
