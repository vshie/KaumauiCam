"""YouTube Live RTMP via ffmpeg; bandwidth from -progress total_size."""

from __future__ import annotations

import logging
import re
import socket
import subprocess
import threading
import time
import uuid
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

RTMP_BASE = "rtmp://a.rtmp.youtube.com/live2"
RTMP_HOST = "a.rtmp.youtube.com"

# Resilience tuning. The boat is on Starlink, which goes through brief
# (1–30s) blips when the satellite hands off, the dish reorients, or
# upstream DNS hiccups. ffmpeg itself doesn't reconnect RTMP outputs,
# so we rely on the scheduler to respawn — but if we respawn every
# scheduler tick (2s) during a 30s DNS outage we generate 15 noise
# entries in the broadcast history, hammer YouTube's ingest servers,
# and starve actual recovery attempts. The values below trade off
# fast recovery on healthy networks vs. quiet behavior during
# extended outages.
HEALTHY_DURATION_SEC = 30.0  # session that ran this long resets failure counter
HEALTHY_BYTES = 1 * 1024 * 1024  # ...and pushed at least this many bytes
BACKOFF_MIN_SEC = 2.0
BACKOFF_MAX_SEC = 30.0
DNS_CHECK_TIMEOUT_SEC = 3.0


SessionEventCb = Callable[[str, str, dict], None]


def _classify_end(last_stderr: Optional[str]) -> str:
    """Bucket ffmpeg-death stderr into a coarse end_reason for the
    broadcast history UI. Recognised buckets:
      - "dns": Starlink/DNS hiccup, hostname resolution failed
      - "rtmp_broken_pipe": YouTube closed the RTMP socket on us
      - "network": socket-level failure to reach YouTube
      - "rtsp_error": camera RTSP returned an error code
    Unrecognised stderr falls back to "died".
    """
    if not last_stderr:
        return "died"
    s = last_stderr.lower()
    if (
        "failed to resolve hostname" in s
        or "temporary failure in name resolution" in s
        or "name or service not known" in s
    ):
        return "dns"
    if "broken pipe" in s:
        return "rtmp_broken_pipe"
    if (
        "connection refused" in s
        or "connection reset" in s
        or "network is unreachable" in s
        or "no route to host" in s
    ):
        return "network"
    if "server returned 4" in s or "server returned 5" in s:
        return "rtsp_error"
    return "died"


def _dns_ok(host: str = RTMP_HOST, timeout: float = DNS_CHECK_TIMEOUT_SEC) -> bool:
    """Best-effort DNS pre-flight. Runs gethostbyname in a worker
    thread with a hard timeout because Starlink DNS occasionally hangs
    for 30+ seconds during satellite handoff and we don't want the
    scheduler thread to block on resolver retries."""
    result: List[Optional[bool]] = [None]

    def _resolve() -> None:
        try:
            socket.gethostbyname(host)
            result[0] = True
        except Exception:
            result[0] = False

    t = threading.Thread(target=_resolve, daemon=True, name="yt-dns-probe")
    t.start()
    t.join(timeout)
    return result[0] is True


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
        # Starlink-blip backoff state. start() returns False (silently) when
        # `now < _next_attempt_ts`, so the scheduler's 2s tick effectively
        # becomes a 4/8/16/30s tick during sustained outages. The watcher
        # bumps `_consecutive_failures` on a fast/zero-byte death and resets
        # to 0 on a session that ran HEALTHY_DURATION_SEC + pushed
        # HEALTHY_BYTES.
        self._consecutive_failures = 0
        self._next_attempt_ts = 0.0
        # Track DNS-suppression so we throttle warnings instead of logging
        # every 2s during a sustained outage.
        self._last_dns_warn_ts = 0.0

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
        # First do the cheap pre-checks under the lock, then drop the lock
        # for the (potentially blocking) DNS probe so we don't wedge other
        # callers (e.g. /api/stream/status) waiting on the resolver.
        with self._lock:
            if self._proc and self._proc.poll() is None:
                return True
            if not stream_key.strip():
                return False
            now = time.time()
            if now < self._next_attempt_ts:
                # Still in backoff window from a previous failure. The
                # scheduler will keep calling us each tick; we silently
                # decline until backoff expires. Don't log here -- the
                # last failure already logged the next_in= window.
                return False

        # Pre-flight DNS check. On Starlink, RTMP attempts during satellite
        # handoff fail with "Temporary failure in name resolution" within
        # ~5-10s of ffmpeg startup. Each such attempt creates a session
        # row, fires start/end events, and clutters the broadcast history.
        # Probing DNS first lets us skip the spawn entirely and just wait
        # for the network to come back, with no UI noise.
        if not _dns_ok():
            with self._lock:
                self._consecutive_failures += 1
                self._next_attempt_ts = time.time() + self._compute_backoff()
                throttle = time.time() - self._last_dns_warn_ts > 30.0
                if throttle:
                    self._last_dns_warn_ts = time.time()
            if throttle:
                logger.warning(
                    "DNS resolve %s failed; deferring YouTube start"
                    " (consecutive_failures=%d, next_attempt_in=%.1fs)",
                    RTMP_HOST,
                    self._consecutive_failures,
                    max(0.0, self._next_attempt_ts - time.time()),
                )
            return False

        with self._lock:
            # Re-check liveness now that we've reacquired the lock — a
            # concurrent caller may have started a session between the
            # DNS probe and here.
            if self._proc and self._proc.poll() is None:
                return True
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

    def _compute_backoff(self) -> float:
        """Exponential 2s, 4s, 8s, 16s, 30s, 30s, ... capped at
        BACKOFF_MAX_SEC. Caller holds `self._lock`."""
        n = max(0, self._consecutive_failures - 1)
        return min(BACKOFF_MAX_SEC, BACKOFF_MIN_SEC * (2 ** n))

    def _watch_proc(
        self,
        proc: "subprocess.Popen[str]",
        session_id: str,
        started: float,
    ) -> None:
        """Block on ffmpeg, then fire `session_end(end_reason=...)` if the
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
        end_reason = _classify_end(last_err)
        # Update backoff state. A session that survived HEALTHY_DURATION_SEC
        # AND pushed >= HEALTHY_BYTES is treated as proof the network was
        # working; reset so the next attempt fires immediately. Anything
        # shorter or byte-starved bumps the backoff so sustained outages
        # don't generate dozens of broadcast-history rows per minute.
        with self._lock:
            healthy = (
                duration >= HEALTHY_DURATION_SEC
                and self._last_total >= HEALTHY_BYTES
            )
            if healthy:
                self._consecutive_failures = 0
                self._next_attempt_ts = 0.0
                backoff = 0.0
            else:
                self._consecutive_failures += 1
                backoff = self._compute_backoff()
                self._next_attempt_ts = time.time() + backoff
        logger.warning(
            "YouTube ffmpeg died session=%s rc=%s after=%.1fs bytes=%d"
            " reason=%s consecutive_failures=%d next_attempt_in=%.1fs"
            " last_stderr=%r",
            session_id,
            rc,
            duration,
            self._last_total,
            end_reason,
            self._consecutive_failures,
            backoff,
            last_err[-300:],
        )
        self._fire_end(
            session_id,
            started,
            end_reason,
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
            # An explicit stop is not a failure, even if it interrupted a
            # short session — reset the backoff so the next scheduled
            # start fires immediately without inheriting the previous
            # crash counter.
            self._consecutive_failures = 0
            self._next_attempt_ts = 0.0
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
