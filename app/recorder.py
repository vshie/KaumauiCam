"""RTSP -> MPEG-TS during capture, remux to MP4 on stop."""

from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
from datetime import datetime
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)


class Recorder:
    def __init__(self, on_event: Optional[Callable[[str, str], None]] = None):
        self._proc: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._ts_path: Optional[str] = None
        self._mp4_path: Optional[str] = None
        self._on_event = on_event
        self._stderr_lines: List[str] = []

    def is_running(self) -> bool:
        with self._lock:
            return self._proc is not None and self._proc.poll() is None

    def status(self) -> dict:
        with self._lock:
            return {
                "running": self._proc is not None and self._proc.poll() is None,
                "ts_path": self._ts_path,
                "mp4_path": self._mp4_path,
                "stderr_tail": self._stderr_lines[-15:],
            }

    def start(self, rtsp_url: str, dest_dir: str) -> bool:
        with self._lock:
            if self._proc and self._proc.poll() is None:
                return True
            os.makedirs(os.path.join(dest_dir, "in_progress"), exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            base = f"kaumaui-{stamp}"
            self._ts_path = os.path.join(dest_dir, "in_progress", base + ".ts")
            self._mp4_path = os.path.join(dest_dir, base + ".mp4")
            self._stderr_lines = []
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "warning",
                "-rtsp_transport",
                "tcp",
                "-i",
                rtsp_url,
                "-c",
                "copy",
                "-f",
                "mpegts",
                "-y",
                self._ts_path,
            ]
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                text=True,
            )

            def cap_err() -> None:
                assert self._proc and self._proc.stderr
                try:
                    for line in self._proc.stderr:
                        line = line.rstrip()
                        if line:
                            self._stderr_lines.append(line)
                except Exception:
                    pass

            threading.Thread(target=cap_err, daemon=True, name="rec-stderr").start()
            logger.info("Recording started -> %s", self._ts_path)
            if self._on_event:
                self._on_event("start", self._ts_path)
            return True

    def stop(self) -> None:
        with self._lock:
            proc = self._proc
            ts_path = self._ts_path
            mp4_path = self._mp4_path
            self._proc = None
            self._ts_path = None
            self._mp4_path = None
        if not proc:
            return
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
        except Exception as e:
            logger.warning("recorder stop: %s", e)

        if ts_path and mp4_path and os.path.isfile(ts_path):
            try:
                subprocess.run(
                    [
                        "ffmpeg",
                        "-hide_banner",
                        "-loglevel",
                        "warning",
                        "-i",
                        ts_path,
                        "-c",
                        "copy",
                        "-movflags",
                        "+faststart",
                        "-y",
                        mp4_path,
                    ],
                    check=True,
                    timeout=3600,
                )
                os.remove(ts_path)
                logger.info("Remuxed to %s", mp4_path)
                if self._on_event:
                    self._on_event("complete", mp4_path)
            except subprocess.CalledProcessError as e:
                logger.error("Remux failed: %s", e)
                side = ts_path + ".failed"
                try:
                    with open(side, "w") as f:
                        f.write(str(e))
                except OSError:
                    pass
                if self._on_event:
                    self._on_event("error", ts_path)
            except OSError as e:
                logger.error("Remux IO error: %s", e)
        elif ts_path and self._on_event:
            self._on_event("error", ts_path or "")
