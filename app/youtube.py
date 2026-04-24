"""YouTube Live RTMP via ffmpeg; bandwidth from -progress total_size."""

from __future__ import annotations

import logging
import re
import subprocess
import threading
import time
import uuid
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

RTMP_BASE = "rtmp://a.rtmp.youtube.com/live2"


class YouTubeStreamer:
    def __init__(self, on_bytes_delta: Callable[[int, Optional[str]], None]):
        self._on_delta = on_bytes_delta
        self._proc: Optional[subprocess.Popen[str]] = None
        self._reader: Optional[threading.Thread] = None
        self._stderr_reader: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._last_total = 0
        self._session_id: Optional[str] = None
        self._session_start = 0.0
        self._stderr_lines: List[str] = []
        self._stop = threading.Event()

    def is_running(self) -> bool:
        with self._lock:
            return self._proc is not None and self._proc.poll() is None

    def status(self) -> dict:
        with self._lock:
            return {
                "running": self._proc is not None and self._proc.poll() is None,
                "session_id": self._session_id,
                "stderr_tail": self._stderr_lines[-20:],
            }

    def start(self, rtsp_url: str, stream_key: str) -> bool:
        with self._lock:
            if self._proc and self._proc.poll() is None:
                return True
            if not stream_key.strip():
                return False
            self._stop.clear()
            self._last_total = 0
            self._session_id = str(uuid.uuid4())[:8]
            self._session_start = time.time()
            self._stderr_lines = []
            out_url = f"{RTMP_BASE}/{stream_key.strip()}"
            # Axis RTSP is usually video-only, and packets can arrive without
            # PTS. FLV/RTMP requires monotonic PTS + an audio track, so we:
            #   - force wall-clock timestamps on the RTSP input and regenerate
            #     missing PTS (+genpts, +igndts) before stream copy,
            #   - inject a silent AAC track as input 1 so YouTube sees audio,
            #   - disable FLV's duration/filesize header rewrite (live stream).
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "warning",
                "-fflags",
                "+genpts+igndts+nobuffer",
                "-use_wallclock_as_timestamps",
                "1",
                "-rtsp_transport",
                "tcp",
                "-i",
                rtsp_url,
                "-f",
                "lavfi",
                "-i",
                "anullsrc=channel_layout=stereo:sample_rate=44100",
                "-map",
                "0:v:0",
                "-map",
                "1:a:0",
                "-c:v",
                "copy",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-ar",
                "44100",
                "-ac",
                "2",
                "-shortest",
                "-max_muxing_queue_size",
                "1024",
                "-f",
                "flv",
                "-flvflags",
                "no_duration_filesize",
                "-progress",
                "pipe:1",
                "-nostats",
                out_url,
            ]
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                text=True,
                bufsize=1,
            )

            def read_progress() -> None:
                assert self._proc and self._proc.stdout
                total_re = re.compile(r"^total_size=(\d+)$")
                try:
                    for line in self._proc.stdout:
                        if self._stop.is_set():
                            break
                        m = total_re.match(line.strip())
                        if m:
                            total = int(m.group(1))
                            delta = total - self._last_total
                            if delta > 0:
                                self._on_delta(delta, self._session_id)
                            self._last_total = total
                except Exception as e:
                    logger.debug("progress reader: %s", e)

            def read_stderr() -> None:
                assert self._proc and self._proc.stderr
                try:
                    for line in self._proc.stderr:
                        if self._stop.is_set():
                            break
                        line = line.rstrip()
                        if line:
                            self._stderr_lines.append(line)
                            if len(self._stderr_lines) > 200:
                                self._stderr_lines = self._stderr_lines[-200:]
                except Exception as e:
                    logger.debug("stderr reader: %s", e)

            self._reader = threading.Thread(target=read_progress, daemon=True, name="yt-progress")
            self._stderr_reader = threading.Thread(target=read_stderr, daemon=True, name="yt-stderr")
            self._reader.start()
            self._stderr_reader.start()
            logger.info("YouTube ffmpeg started session=%s", self._session_id)
            return True

    def stop(self) -> None:
        with self._lock:
            self._stop.set()
            proc = self._proc
            self._proc = None
        if proc:
            try:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
            except Exception as e:
                logger.warning("stop ffmpeg: %s", e)
        self._last_total = 0
        self._session_id = None
        logger.info("YouTube ffmpeg stopped")
