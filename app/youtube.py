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


SessionEventCb = Callable[[str, str, dict], None]


class YouTubeStreamer:
    def __init__(
        self,
        on_bytes_delta: Callable[[int, Optional[str]], None],
        on_session_event: Optional[SessionEventCb] = None,
    ):
        self._on_delta = on_bytes_delta
        # on_session_event(event_type, session_id, info_dict) is fired on
        # session start and end. main.py wires this to bandwidth.record_session_*
        # so the broadcast history is persisted to SQLite. Optional so unit
        # tests can construct a streamer without a database.
        self._on_session_event = on_session_event
        self._proc: Optional[subprocess.Popen[str]] = None
        self._reader: Optional[threading.Thread] = None
        self._stderr_reader: Optional[threading.Thread] = None
        self._watcher: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._last_total = 0
        self._session_id: Optional[str] = None
        self._session_start = 0.0
        self._last_byte_time = 0.0
        self._stderr_lines: List[str] = []
        self._stop = threading.Event()
        # Per-session dedup so the death watcher and an explicit stop() don't
        # both fire a session-end event (and double-write the SQLite row) if
        # ffmpeg happens to exit at the same instant the user hits Stop. We
        # key by session_id (not a single bool) so a watcher firing for a
        # crashed previous session can't accidentally swallow the end event
        # for a freshly-spawned restart session. Capped to avoid unbounded
        # growth across long uptimes.
        self._fired_sessions: List[str] = []

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

    def seconds_since_last_byte(self) -> float:
        """How long since the last RTMP byte was reported by ffmpeg's
        `-progress` output. Used by the supervisor watchdog to detect a
        wedged ffmpeg pipeline (process alive but bytes flat-lined). Returns
        0.0 when no session has started yet."""
        with self._lock:
            if self._proc is None or self._proc.poll() is not None:
                return 0.0
            ref = self._last_byte_time or self._session_start
            if ref <= 0:
                return 0.0
            return max(0.0, time.time() - ref)

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
            self._last_byte_time = self._session_start
            self._stderr_lines = []
            out_url = f"{RTMP_BASE}/{stream_key.strip()}"
            # Axis RTSP packets often arrive without a PTS, which the FLV/RTMP
            # muxer rejects ("Packet is missing PTS"). +genpts+igndts lets
            # ffmpeg regenerate timestamps from the input frame timing so we
            # can stream-copy H.264 straight through to YouTube.
            #
            # YouTube Live requires an audio track to register a broadcast as
            # live — a video-only RTMP feed is accepted at the network layer
            # (no ffmpeg errors) but the dashboard never shows it. We mix in
            # a silent AAC track from lavfi so YouTube sees a complete A/V
            # stream. anullsrc is essentially free CPU-wise.
            #
            # Things we deliberately do NOT do (each measured to throttle the
            # actual bitrate reaching YouTube on this Pi/Axis combo):
            #   - +nobuffer: drops bursty packets aggressively, kept only ~30%
            #     of frames in testing.
            #   - -use_wallclock_as_timestamps 1: rewriting PTS to wall clock
            #     halved the egress bitrate even with +genpts present.
            #   - -shortest: with two open-ended inputs (RTSP video + lavfi
            #     audio) the timelines drift slightly at startup; -shortest
            #     interpreted that drift as one input ending and dropped
            #     ~90% of video frames before muxing.
            # ffmpeg defaults `thread_queue_size` to 8 packets per input,
            # which is ~270ms at 30fps. A single Starlink/RTMP transient
            # stall easily exceeds that; once an input thread blocks on a
            # full queue, the muxer thread can wedge and bytes stop flowing
            # while the process stays alive.
            #
            # We were running at 1024 packets (~30s headroom) and still
            # tripping "Thread message queue blocking; consider raising the
            # thread_queue_size option (current value: 1024)" warnings in
            # the UI's stderr_tail, which means real RTMP stalls were
            # exceeding 30s before the death watcher would self-heal. Bump
            # to 4096 (~2 min @ 30fps) so the queue can absorb a Starlink
            # outage without dropping packets, and the watcher (below) is
            # the thing that decides when a stalled session is unrecoverable.
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel",
                "warning",
                "-fflags",
                "+genpts+igndts",
                "-rtsp_transport",
                "tcp",
                "-thread_queue_size",
                "4096",
                "-i",
                rtsp_url,
                "-f",
                "lavfi",
                "-thread_queue_size",
                "512",
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
                "64k",
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
                                self._last_byte_time = time.time()
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
            self._watcher = threading.Thread(
                target=self._watch_proc,
                args=(self._proc, self._session_id, self._session_start),
                daemon=True,
                name="yt-watch",
            )
            self._reader.start()
            self._stderr_reader.start()
            self._watcher.start()
            logger.info("YouTube ffmpeg started session=%s", self._session_id)
            if self._on_session_event:
                try:
                    self._on_session_event(
                        "start",
                        self._session_id,
                        {"started_ts": self._session_start},
                    )
                except Exception:
                    logger.exception("on_session_event(start) failed")
            return True

    def _watch_proc(
        self,
        proc: "subprocess.Popen[str]",
        session_id: str,
        started: float,
    ) -> None:
        """Block on ffmpeg, then fire `session_end(end_reason="died")` if the
        process exited on its own. We bind proc/session_id/started as args so
        the watcher reports on the session it was launched for, even if a
        new session has already replaced `self._proc` / `self._session_id`
        by the time it wakes up."""
        try:
            rc = proc.wait()
        except Exception as e:
            logger.debug("yt-watch wait: %s", e)
            return
        # If stop() was the cause, it's already responsible for firing the
        # end event with end_reason="stopped". Don't double-fire.
        if self._stop.is_set():
            return
        last_err = "\n".join(self._stderr_lines[-5:]) if self._stderr_lines else ""
        duration = time.time() - started if started else 0.0
        logger.warning(
            "YouTube ffmpeg died unexpectedly session=%s rc=%s after=%.1fs bytes=%d last_stderr=%r",
            session_id,
            rc,
            duration,
            self._last_total,
            last_err[-300:],
        )
        self._fire_end(
            session_id,
            started,
            "died",
            exit_code=rc,
            last_stderr=last_err[-1000:],
        )

    def _fire_end(
        self,
        session_id: Optional[str],
        started: float,
        end_reason: str,
        exit_code: Optional[int],
        last_stderr: Optional[str],
    ) -> None:
        """Single-shot session end notifier, keyed by session_id. Both the
        death watcher and the explicit stop() path call this; only the first
        call per session_id wins. Using a per-id list (instead of a single
        bool) means a late-firing watcher from a crashed previous session
        can't suppress the end event for a freshly-restarted session."""
        if not session_id:
            return
        with self._lock:
            if session_id in self._fired_sessions:
                return
            self._fired_sessions.append(session_id)
            if len(self._fired_sessions) > 32:
                self._fired_sessions = self._fired_sessions[-32:]
        if self._on_session_event:
            try:
                self._on_session_event(
                    "end",
                    session_id,
                    {
                        "ended_ts": time.time(),
                        "exit_code": exit_code,
                        "end_reason": end_reason,
                        "last_stderr": last_stderr,
                        "duration": max(0.0, time.time() - started) if started else 0.0,
                    },
                )
            except Exception:
                logger.exception("on_session_event(end) failed")

    def stop(self, reason: str = "stopped") -> None:
        """Terminate the current ffmpeg session. `reason` is recorded as the
        session's end_reason so the broadcast history distinguishes user/
        schedule stops ("stopped") from watchdog-forced restarts ("stalled").
        """
        with self._lock:
            self._stop.set()
            proc = self._proc
            sid = self._session_id
            started = self._session_start
            last_total = self._last_total
            last_err = "\n".join(self._stderr_lines[-5:]) if self._stderr_lines else ""
            self._proc = None
        rc: Optional[int] = None
        if proc:
            try:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                rc = proc.returncode
            except Exception as e:
                logger.warning("stop ffmpeg: %s", e)
        duration = time.time() - started if started else 0.0
        log_fn = logger.warning if reason != "stopped" else logger.info
        log_fn(
            "YouTube ffmpeg %s session=%s rc=%s after=%.1fs bytes=%d",
            reason,
            sid,
            rc,
            duration,
            last_total,
        )
        self._fire_end(sid, started, reason, exit_code=rc, last_stderr=last_err[-1000:])
        self._last_total = 0
        self._session_id = None
