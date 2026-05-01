"""RTSP -> 5-minute MP4 chunks via ffmpeg.

We use the same RTSP-input recipe the YouTube streamer uses, because it's been
proven to work against this Axis HEVC source:

    -fflags +genpts+igndts -use_wallclock_as_timestamps 1 -rtsp_transport tcp

The Axis stream's RTP timestamps are unreliable (the encoder's "Average bitrate"
mode emits ticks at the configured 30fps clock even when the profile is set to
15fps, so a 5-minute capture would otherwise carry ~10 minutes of PTS span and
play back at half-speed). Forcing wall-clock timestamps on input regenerates
clean, monotonic PTS that match real time, so each segment's reported duration
matches its wall-clock duration.

We rotate segments ourselves rather than using ffmpeg's `-f segment`: every
SEGMENT_SECONDS a supervisor thread sends SIGINT to the running ffmpeg so it
finalizes the current mp4 (writing a moov index for VLC) before spawning a
fresh pipeline for the next chunk. Each file therefore has an accurate
start-time in its filename and is fully indexed/seekable on its own.
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

# Discard segment files that contain less than this many bytes when ffmpeg
# exits. A clean 5-min H.265 1080p15 segment from this Axis stream measures
# >= ~2 MB on the quietest scenes we've recorded; the largest "stub" we've
# observed is the ~9-second tail segment that gets SIGINT'd at a schedule
# slot boundary, which sits at ~300 KB. 1 MB cleanly separates the two and
# is comfortably above any boundary stub we expect to see in practice.
MIN_SEGMENT_BYTES = 1 * 1024 * 1024

# When the supervisor would otherwise start a fresh 5-min ffmpeg pipeline
# but the schedule is going to ask us to stop within this many seconds,
# bail instead. Without this guard, the recorder cheerfully starts a fresh
# segment ~5-9 s before the slot boundary, ffmpeg gets SIGINT'd ~10 s
# later, and we'd be left with a useless 7-10 s mp4 stub on the recordings
# directory.
SEGMENT_TAIL_GUARD_SECS = 30


class Recorder:
    def __init__(
        self,
        on_event: Optional[Callable[[str, str], None]] = None,
        should_continue: Optional[Callable[[], bool]] = None,
    ):
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
        self._should_continue = should_continue
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
        # Stream-copy the RTSP H.265 video to a self-contained MP4. Key flags:
        #   -fflags +genpts+igndts: regenerate PTS, ignore the input's DTS.
        #   -use_wallclock_as_timestamps 1: stamp incoming packets with the
        #     local wall-clock; this is what makes a 5-minute capture produce
        #     a 5-minute file regardless of what the camera's RTP clock claims.
        #     The Axis encoder's "Average bitrate" mode otherwise emits ticks
        #     at the configured 30fps clock even when the profile is set to
        #     15fps, so a 5-min capture would carry ~10 minutes of PTS span
        #     and play back at half-speed.
        #   -rtsp_transport tcp: avoid UDP packet loss; matches Axis defaults.
        #   -an: drop any audio track the camera might enable -- we only want
        #     the H.265 video for training/review data.
        #   -movflags +faststart: shuffle the moov atom to the front on EOS
        #     so VLC sees the index immediately and seeking works without
        #     reading the tail of the file first.
        return [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "warning",
            "-fflags", "+genpts+igndts",
            "-use_wallclock_as_timestamps", "1",
            "-rtsp_transport", "tcp",
            "-i", self._rtsp_url or "",
            "-map", "0:v:0",
            "-an",
            "-c:v", "copy",
            "-movflags", "+faststart",
            "-y",
            out_path,
        ]

    def _run_one_segment(self) -> bool:
        """Run ffmpeg for up to SEGMENT_SECONDS, then EOS-finalize the mp4.

        Returns True if a usable (>= MIN_SEGMENT_BYTES) segment was produced.
        Returns False if ffmpeg died before producing meaningful output --
        in that case the empty/stub mp4 file is deleted before returning so
        the recordings directory doesn't fill up with debris during a camera
        outage. The supervisor uses this signal to apply exponential backoff.
        """
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
                    "ffmpeg exited early rc=%s file=%s", proc.returncode, out
                )
                break
            time.sleep(1.0)

        # Ask ffmpeg to flush the trailer (moov atom) and exit. SIGINT prompts
        # ffmpeg to finish writing the current MP4 cleanly so the file is
        # fully indexed and seekable in VLC.
        if proc.poll() is None:
            try:
                proc.send_signal(signal.SIGINT)
            except Exception as e:
                logger.warning("SIGINT to ffmpeg failed: %s", e)
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                logger.warning("ffmpeg did not finalize in 15s; terminating")
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass

        err_th.join(timeout=2)

        # If the segment is too small to be usable (RTSP failed before any
        # real video reached mp4mux, or the schedule killed the pipeline
        # within seconds of opening it), delete the stub file so it doesn't
        # show up in the recordings list and signal failure to the supervisor.
        try:
            sz = os.path.getsize(out)
            exists = True
        except OSError as e:
            sz = 0
            exists = os.path.exists(out)
            logger.warning("getsize failed for %s: %s (exists=%s)", out, e, exists)
        if sz < MIN_SEGMENT_BYTES:
            logger.warning(
                "Discarding short segment %s (%d bytes < %d threshold, exists=%s)",
                out, sz, MIN_SEGMENT_BYTES, exists,
            )
            if exists:
                try:
                    os.remove(out)
                except OSError as e:
                    logger.error(
                        "Failed to remove short segment %s: %s", out, e
                    )
                else:
                    if os.path.exists(out):
                        # FAT/USB quirk: unlink reported success but the
                        # entry is still visible. Loudly so we notice.
                        logger.error(
                            "Short segment %s still present after os.remove",
                            out,
                        )
                    else:
                        logger.info("Removed short segment %s", out)
            return False

        if self._on_event:
            self._on_event("segment_complete", out)
        return True

    def _supervise(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            # Schedule-aware gate: if the caller's schedule indicates we
            # are about to be told to stop (e.g. the slot boundary is
            # within SEGMENT_TAIL_GUARD_SECS), don't kick off another
            # 5-min ffmpeg only to have it killed within seconds. The
            # main scheduler tick will follow up shortly with .stop().
            if self._should_continue is not None:
                try:
                    cont = bool(self._should_continue())
                except Exception:
                    logger.exception("recorder should_continue() raised")
                    cont = True
                if not cont:
                    logger.info(
                        "Recorder: schedule indicates imminent stop;"
                        " not starting another segment"
                    )
                    break
            try:
                ok = self._run_one_segment()
            except Exception as e:
                logger.exception("recorder segment: %s", e)
                ok = False
            if ok:
                backoff = 1.0
                continue
            # ffmpeg died before producing a usable segment -- usually
            # the camera is unreachable or RTSP is rejecting the profile.
            # Back off so we don't burn through filenames and CPU spinning
            # up empty pipelines once per second.
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
