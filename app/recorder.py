"""RTSP -> 5-minute MP4 chunks via GStreamer.

ffmpeg's RTSP demuxer drops PTS on this Axis HEVC stream, so stream-copy via
ffmpeg produces 0-byte / 44-byte MP4s. GStreamer's rtph265depay recovers
timestamps correctly, so we use it instead.

Rather than splitmuxsink (which buffers a long time before producing its first
playable file), we rotate the pipeline ourselves: every SEGMENT_SECONDS a
supervisor thread sends SIGINT to the running gst-launch so it EOS-finalizes
the current mp4 cleanly, then spawns a fresh pipeline for the next chunk.
Each file therefore has an accurate start-time in its filename and is fully
playable on its own.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import threading
import time
from datetime import datetime
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

SEGMENT_SECONDS = 300


class Recorder:
    def __init__(self, on_event: Optional[Callable[[str, str], None]] = None):
        self._proc: Optional[subprocess.Popen] = None
        self._lock = threading.RLock()
        self._dest_dir: Optional[str] = None
        self._rtsp_url: Optional[str] = None
        self._pattern: Optional[str] = None
        self._current_file: Optional[str] = None
        self._supervisor: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._stderr_reader: Optional[threading.Thread] = None
        self._on_event = on_event
        self._stderr_lines: List[str] = []

    def is_running(self) -> bool:
        with self._lock:
            return (
                self._supervisor is not None
                and self._supervisor.is_alive()
                and not self._stop.is_set()
            )

    def status(self) -> dict:
        with self._lock:
            return {
                "running": self.is_running(),
                "dest_dir": self._dest_dir,
                "segment_pattern": self._pattern,
                "segment_seconds": SEGMENT_SECONDS,
                "current_file": self._current_file,
                "stderr_tail": self._stderr_lines[-15:],
            }

    def _build_cmd(self, out_path: str) -> List[str]:
        # Queue settings are deliberately tuned for *recording* fidelity, not
        # live-display robustness:
        #   - leaky=no: under USB write stalls / mp4mux flush hiccups, apply
        #     backpressure all the way back to rtspsrc instead of silently
        #     dropping the newest frames. We'd rather see an explicit error
        #     than quietly produce a sparser MP4 that's missing training
        #     data. (The previous default of leaky=downstream + silent=true
        #     would absorb up to 30s of stalls as invisible frame loss.)
        #   - max-size-time=2s: enough to absorb normal write jitter; small
        #     enough that real stalls surface fast.
        #   - silent=false: queue overrun/underrun events are logged so the
        #     status endpoint's stderr_tail captures them.
        return [
            "gst-launch-1.0",
            "-e",
            "rtspsrc",
            f"location={self._rtsp_url}",
            "protocols=tcp",
            "latency=5000",
            "retry=5",
            "timeout=5000000",
            "!",
            "rtph265depay",
            "!",
            "h265parse",
            "config-interval=-1",
            "!",
            "queue",
            "max-size-time=2000000000",
            "max-size-bytes=0",
            "max-size-buffers=0",
            "leaky=no",
            "silent=false",
            "!",
            "mp4mux",
            "fragment-duration=5000",
            "streamable=true",
            "!",
            "filesink",
            f"location={out_path}",
            "sync=false",
        ]

    def _run_one_segment(self) -> None:
        assert self._dest_dir and self._rtsp_url
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        out = os.path.join(self._dest_dir, f"kaumaui-{ts}.mp4")
        cmd = self._build_cmd(out)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        with self._lock:
            self._proc = proc
            self._current_file = out
            self._stderr_lines = []

        def cap_err() -> None:
            assert proc.stderr
            try:
                for line in proc.stderr:
                    line = line.rstrip()
                    if not line:
                        continue
                    self._stderr_lines.append(line)
                    if len(self._stderr_lines) > 200:
                        self._stderr_lines = self._stderr_lines[-200:]
            except Exception:
                pass

        err_th = threading.Thread(target=cap_err, daemon=True, name="rec-stderr")
        err_th.start()

        logger.info("Recording segment -> %s", out)
        if self._on_event:
            self._on_event("segment_start", out)

        # Wait for the segment duration OR an explicit stop request.
        # Poll every second so we can react quickly to stop / process death.
        deadline = time.time() + SEGMENT_SECONDS
        while not self._stop.is_set() and time.time() < deadline:
            if proc.poll() is not None:
                logger.warning(
                    "gst-launch exited early rc=%s file=%s", proc.returncode, out
                )
                break
            time.sleep(1.0)

        # Ask gst-launch to EOS + finalize the mp4. With `-e` SIGINT triggers
        # a clean shutdown that writes moov before exiting.
        if proc.poll() is None:
            try:
                proc.send_signal(signal.SIGINT)
            except Exception as e:
                logger.warning("SIGINT to gst-launch failed: %s", e)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                logger.warning("gst-launch did not finalize in 10s; terminating")
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

        err_th.join(timeout=2)
        if self._on_event:
            self._on_event("segment_complete", out)

    def _supervise(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                self._run_one_segment()
                backoff = 1.0
            except Exception as e:
                logger.exception("recorder segment: %s", e)
                # Back off on repeated failures so we don't spin.
                self._stop.wait(timeout=backoff)
                backoff = min(backoff * 2, 30.0)
        with self._lock:
            self._proc = None
            self._current_file = None

    def start(self, rtsp_url: str, dest_dir: str) -> bool:
        with self._lock:
            if self._supervisor and self._supervisor.is_alive():
                return True
            os.makedirs(dest_dir, exist_ok=True)
            self._dest_dir = dest_dir
            self._rtsp_url = rtsp_url
            self._pattern = os.path.join(dest_dir, "kaumaui-YYYYMMDD-HHMMSS.mp4")
            self._stop.clear()
            self._stderr_lines = []
            self._supervisor = threading.Thread(
                target=self._supervise, daemon=True, name="rec-supervisor"
            )
            self._supervisor.start()
        logger.info("Recorder supervisor started (5-min chunks) -> %s", dest_dir)
        if self._on_event:
            self._on_event("start", dest_dir)
        return True

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            proc = self._proc
            sup = self._supervisor
            dest = self._dest_dir
            pat = self._pattern
        if proc and proc.poll() is None:
            try:
                proc.send_signal(signal.SIGINT)
            except Exception:
                pass
        if sup:
            sup.join(timeout=15)
        with self._lock:
            self._proc = None
            self._supervisor = None
            self._dest_dir = None
            self._rtsp_url = None
            self._pattern = None
            self._current_file = None
        logger.info("Recorder supervisor stopped")
        if self._on_event:
            self._on_event("complete", pat or dest or "")
