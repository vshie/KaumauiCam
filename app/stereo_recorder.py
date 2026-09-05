"""MarineSitu C3 (OAK-D) stereo capture supervisor.

Wraps the vendored capture script in ``app/c3record`` -- an unmodified copy of
the standalone C3Record project -- as a long-running subprocess that the
scheduler starts and stops on the record/pause cycle.

Unlike the Axis ``Recorder``, we don't rotate segments ourselves: the script's
``C3VideoWriterManager`` opens a fresh MKV every ``--duration`` seconds and
names it from ``--output_format``, so one process invocation produces a whole
burst worth of short segments. Our job is to launch it, keep an eye on free
space while it writes, SIGINT it at the end of the burst (its handler flushes
the frame queue and finalizes the current MKV), and back off when the camera
can't be reached.

Two things make this camera different from the Axis:

  * It's a network (PoE) DepthAI device, so there's no device node to bind --
    but bringing the pipeline up takes on the order of 10-20 s, which is dead
    time at the start of every burst.
  * A DepthAI device accepts exactly one client at a time. If something else
    on the host has claimed it, our process exits during startup; the backoff
    below keeps that from becoming a hot loop.
"""

from __future__ import annotations

import glob
import logging
import os
import signal
import subprocess
import threading
import time
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# Vendored capture script. Invoked with the interpreter directly rather than
# through `uv`/`-m`: the c3record modules import each other by bare name
# (`from c3_video import ...`), which resolves because Python puts the script's
# own directory at the front of sys.path.
C3RECORD_MAIN = os.path.join(os.path.dirname(__file__), "c3record", "main.py")

# Segment filename prefix. Also the glob we prune stubs with, so it has to be
# distinct from the Axis recorder's `wailoa-` files.
SEGMENT_PREFIX = "stereo-"
SEGMENT_FORMAT = SEGMENT_PREFIX + "%Y%m%d-%H%M%S-%f.mkv"
SEGMENT_GLOB = SEGMENT_PREFIX + "*.mkv"

# A usable segment holds three H.264 tracks; at the default 8 Mbit/s per
# stream even a 1-second sliver clears this comfortably. Anything smaller is
# a file matroskamux opened and never got real data into -- typically the
# segment that was open when we SIGINT'd at the end of a burst.
MIN_SEGMENT_BYTES = 256 * 1024

# How often the supervisor wakes while the child runs, to notice a stop
# request, a dead child, or the disk filling up.
POLL_SECS = 1.0

# Free-space is re-checked every this many seconds while recording. The child
# writes continuously for the whole burst, so unlike the Axis recorder we
# can't rely on a check at segment-start time alone.
SPACE_CHECK_SECS = 5.0

# Grace period after SIGINT. The script joins its writer thread with a 15 s
# timeout before returning from main(), so allow a bit more than that before
# escalating to SIGTERM/SIGKILL.
SIGINT_GRACE_SECS = 25.0

# Backoff bounds for a child that exits without producing anything. Starts
# well above the Axis recorder's 1 s because a failed DepthAI connect already
# burns ~30 s of bootup timeout, and the usual cause (device claimed by
# another process) won't clear in a second.
BACKOFF_START_SECS = 5.0
BACKOFF_MAX_SECS = 60.0

# Progress watchdog. The child does not exit when its writer thread dies --
# the acquisition loop keeps draining the camera quite happily while every
# frame is dropped on the floor, which is exactly what happens when the
# configured encoder element is missing from this host's GStreamer install.
# Without this watchdog the tab would report "Recording" indefinitely while
# writing nothing, so we require a steady stream of new segment files.
#
# STARTUP_GRACE_SECS covers connecting to the camera and uploading the
# pipeline; a healthy PoE C3 takes ~10 s, and depthai's own bootup timeout
# is 30 s. After the first segment lands we only allow one segment period
# plus PROGRESS_GRACE_SECS between files, which also catches a mid-burst
# wedge (camera drops off, GStreamer stalls).
STARTUP_GRACE_SECS = 45.0
PROGRESS_GRACE_SECS = 20.0


class StereoRecorder:
    """Supervises one ``c3record/main.py`` child at a time."""

    def __init__(
        self,
        should_continue: Optional[Callable[[], bool]] = None,
        space_ok: Optional[Callable[[str], Optional[str]]] = None,
    ):
        """
        Args:
            should_continue: consulted before each child launch; return False
                to let the supervisor exit cleanly rather than start a process
                the scheduler is about to tear down.
            space_ok: called with the destination directory while recording;
                return a human-readable reason to stop, or None to keep going.
        """
        self._lock = threading.RLock()
        self._proc: Optional[subprocess.Popen] = None
        self._dest_dir: Optional[str] = None
        self._tunables: Dict[str, Any] = {}
        self._supervisor: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._stderr_lines: List[str] = []
        self._last_error: Optional[str] = None
        self._started_ts: float = 0.0
        self._should_continue = should_continue
        self._space_ok = space_ok

    # --- introspection ----------------------------------------------------

    def is_running(self) -> bool:
        with self._lock:
            return (
                self._supervisor is not None
                and self._supervisor.is_alive()
                and not self._stop.is_set()
            )

    def status(self) -> Dict[str, Any]:
        with self._lock:
            dest = self._dest_dir
            running = self.is_running()
            started = self._started_ts
            err = self._last_error
            tail = self._stderr_lines[-15:]
        return {
            "running": running,
            "dest_dir": dest,
            "segment_pattern": (
                os.path.join(dest, SEGMENT_FORMAT) if dest else None
            ),
            "segment_seconds": self._tunables.get("segment_secs"),
            "current_file": self._newest_segment(dest) if dest else None,
            "uptime_secs": (time.time() - started) if (running and started) else 0.0,
            "last_error": err,
            "stderr_tail": tail,
        }

    def _newest_segment(self, dest_dir: str) -> Optional[str]:
        try:
            paths = glob.glob(os.path.join(dest_dir, SEGMENT_GLOB))
        except OSError:
            return None
        newest: Optional[str] = None
        newest_mtime = -1.0
        for p in paths:
            try:
                m = os.path.getmtime(p)
            except OSError:
                continue
            if m > newest_mtime:
                newest_mtime = m
                newest = p
        return newest

    def _count_segments(self, dest_dir: str) -> int:
        """Number of usable (non-stub) segments currently in ``dest_dir``."""
        n = 0
        try:
            paths = glob.glob(os.path.join(dest_dir, SEGMENT_GLOB))
        except OSError:
            return 0
        for p in paths:
            try:
                if os.path.getsize(p) >= MIN_SEGMENT_BYTES:
                    n += 1
            except OSError:
                continue
        return n

    # --- child process ----------------------------------------------------

    def _build_cmd(self, dest_dir: str) -> List[str]:
        t = self._tunables
        cmd = [
            "python3",
            "-u",
            C3RECORD_MAIN,
            "--output_dir", dest_dir,
            "--output_format", SEGMENT_FORMAT,
            "--duration", str(t["segment_secs"]),
            "--fps", str(t["fps"]),
            "--sync", str(t["sync_ms"]),
            "--mjpeg-quality", str(t["mjpeg_quality"]),
            "--color-resolution", str(t["color_resolution"]),
            "--stereo-resolution", str(t["stereo_resolution"]),
            "--bitrate", str(t["bitrate"]),
            "--gop-size", str(t["gop_size"]),
            "--bitrate-control", str(t["bitrate_control"]),
            "--quantizer", str(t["quantizer"]),
            "--speed-preset", str(t["speed_preset"]),
            "--tune", str(t["tune"]),
            "--encoder", str(t["encoder"]),
        ]
        # The script defaults these to None and only forwards them to the
        # encoder when set, so omit rather than pass an empty value.
        if t.get("quality_min") is not None:
            cmd += ["--quality-min", str(t["quality_min"])]
        if t.get("quality_max") is not None:
            cmd += ["--quality-max", str(t["quality_max"])]
        if t.get("max_bitrate") is not None:
            cmd += ["--max-bitrate", str(t["max_bitrate"])]
        return cmd

    def _prune_stubs(self, dest_dir: str) -> None:
        """Delete sub-threshold MKVs. Only called with no child running, so
        every matching file is closed and safe to judge by size."""
        try:
            paths = glob.glob(os.path.join(dest_dir, SEGMENT_GLOB))
        except OSError:
            return
        for p in paths:
            try:
                if os.path.getsize(p) >= MIN_SEGMENT_BYTES:
                    continue
            except OSError:
                continue
            try:
                os.remove(p)
            except OSError as e:
                logger.warning("stereo: failed to remove stub %s: %s", p, e)
            else:
                logger.info("stereo: removed stub segment %s", p)

    def _run_once(self, dest_dir: str) -> bool:
        """Run one child to completion. True if it produced a usable segment."""
        cmd = self._build_cmd(dest_dir)
        logger.info("stereo: launching %s", " ".join(cmd))
        segments_before = self._count_segments(dest_dir)

        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                text=True,
                bufsize=1,
                env=env,
            )
        except OSError as e:
            with self._lock:
                self._last_error = f"launch failed: {e}"
            logger.exception("stereo: launch failed")
            return False

        with self._lock:
            self._proc = proc
            self._started_ts = time.time()
            self._stderr_lines = []

        err_th = threading.Thread(
            target=self._drain_stderr, args=(proc,), daemon=True, name="stereo-stderr"
        )
        err_th.start()

        space_reason: Optional[str] = None
        stall_reason: Optional[str] = None
        next_space_check = time.time() + SPACE_CHECK_SECS
        try:
            segment_secs = float(self._tunables.get("segment_secs") or 10)
        except (TypeError, ValueError):
            segment_secs = 10.0
        seen_segments = segments_before
        progress_deadline = time.time() + STARTUP_GRACE_SECS + segment_secs
        while not self._stop.is_set():
            if proc.poll() is not None:
                break
            now = time.time()
            if self._space_ok is not None and now >= next_space_check:
                next_space_check = now + SPACE_CHECK_SECS
                try:
                    space_reason = self._space_ok(dest_dir)
                except Exception:
                    logger.exception("stereo: space check raised")
                    space_reason = None
                if space_reason:
                    logger.warning("stereo: stopping, %s", space_reason)
                    with self._lock:
                        self._last_error = space_reason
                    break
            n = self._count_segments(dest_dir)
            if n > seen_segments:
                seen_segments = n
                progress_deadline = now + segment_secs + PROGRESS_GRACE_SECS
            elif now >= progress_deadline:
                stall_reason = self._stall_reason(seen_segments > segments_before)
                logger.warning("stereo: %s", stall_reason)
                with self._lock:
                    self._last_error = stall_reason
                break
            time.sleep(POLL_SECS)

        self._shutdown_child(proc)
        err_th.join(timeout=2)

        rc = proc.returncode
        produced = self._count_segments(dest_dir) > segments_before
        self._prune_stubs(dest_dir)

        if stall_reason:
            # A stalled child is a failure even if it managed a segment or
            # two before wedging -- the caller must back off, not relaunch
            # immediately into the same wall.
            return False
        if not produced and not space_reason:
            tail = "; ".join(self._stderr_lines[-3:]) or f"exit code {rc}"
            hint = ""
            if "No OAK-D device discovered" in tail:
                # A DepthAI device serves one client at a time and drops out
                # of discovery entirely while claimed, so "not found" and
                # "in use" look identical from here. Say so.
                hint = (
                    " -- the camera is either offline or already claimed by"
                    " another process"
                )
            with self._lock:
                self._last_error = f"no video produced ({tail}){hint}"
            logger.warning("stereo: child rc=%s produced no segments", rc)
        elif produced:
            with self._lock:
                if not space_reason:
                    self._last_error = None
        return produced

    def _stall_reason(self, had_progress: bool) -> str:
        """Human-readable diagnosis for a child that stopped producing.

        A missing encoder element is by far the most common cause and the
        least self-evident, so name it explicitly and point at the setting
        the operator can actually change."""
        with self._lock:
            tail = list(self._stderr_lines[-40:])
        encoder = self._tunables.get("encoder")
        for line in reversed(tail):
            if "no element" in line:
                return (
                    f"encoder '{encoder}' is not available on this host "
                    f"({line.strip()}); switch the encoder setting"
                )
        if had_progress:
            return "recording stalled: no new segment files; camera or encoder wedged"
        return (
            "recording stalled: camera connected but no video was written "
            + (f"({tail[-1].strip()})" if tail else "")
        ).strip()

    def _drain_stderr(self, proc: subprocess.Popen) -> None:
        """Mirror the child's log output into our logger and a rolling tail."""
        if proc.stderr is None:
            return
        try:
            for line in proc.stderr:
                line = line.rstrip()
                if not line:
                    continue
                logger.info("c3record: %s", line)
                with self._lock:
                    self._stderr_lines.append(line)
                    if len(self._stderr_lines) > 200:
                        self._stderr_lines = self._stderr_lines[-200:]
        except Exception:
            pass

    def _shutdown_child(self, proc: subprocess.Popen) -> None:
        """SIGINT so the script finalizes its MKV, escalating if it hangs."""
        if proc.poll() is not None:
            return
        try:
            proc.send_signal(signal.SIGINT)
        except Exception as e:
            logger.warning("stereo: SIGINT failed: %s", e)
        try:
            proc.wait(timeout=SIGINT_GRACE_SECS)
            return
        except subprocess.TimeoutExpired:
            logger.warning(
                "stereo: child did not exit %.0fs after SIGINT; terminating",
                SIGINT_GRACE_SECS,
            )
        try:
            proc.terminate()
            proc.wait(timeout=5)
            return
        except Exception:
            pass
        try:
            proc.kill()
            proc.wait(timeout=5)
        except Exception:
            logger.error("stereo: failed to kill child pid=%s", proc.pid)

    # --- supervisor -------------------------------------------------------

    def _supervise(self, dest_dir: str) -> None:
        backoff = BACKOFF_START_SECS
        while not self._stop.is_set():
            if self._should_continue is not None:
                try:
                    cont = bool(self._should_continue())
                except Exception:
                    logger.exception("stereo: should_continue() raised")
                    cont = True
                if not cont:
                    logger.info("stereo: schedule indicates imminent stop; not relaunching")
                    break
            try:
                ok = self._run_once(dest_dir)
            except Exception:
                logger.exception("stereo: run failed")
                ok = False
            if self._stop.is_set():
                break
            if ok:
                # The child exited on its own after writing video -- e.g. the
                # camera heartbeat dropped. Relaunch promptly.
                backoff = BACKOFF_START_SECS
                continue
            self._stop.wait(timeout=backoff)
            backoff = min(backoff * 2, BACKOFF_MAX_SECS)
        with self._lock:
            self._proc = None
            self._started_ts = 0.0

    def start(self, dest_dir: str, tunables: Dict[str, Any]) -> bool:
        with self._lock:
            if self._supervisor and self._supervisor.is_alive():
                return True
            try:
                os.makedirs(dest_dir, exist_ok=True)
            except OSError as e:
                self._last_error = f"cannot create {dest_dir}: {e}"
                logger.error("stereo: %s", self._last_error)
                return False
            self._dest_dir = dest_dir
            self._tunables = dict(tunables)
            self._stop.clear()
            self._stderr_lines = []
            self._supervisor = threading.Thread(
                target=self._supervise,
                args=(dest_dir,),
                daemon=True,
                name="stereo-supervisor",
            )
            self._supervisor.start()
        logger.info("stereo: supervisor started -> %s", dest_dir)
        return True

    def stop(self) -> None:
        self._stop.set()
        with self._lock:
            proc = self._proc
            sup = self._supervisor
        if proc is not None and proc.poll() is None:
            self._shutdown_child(proc)
        if sup is not None:
            sup.join(timeout=SIGINT_GRACE_SECS + 15.0)
        with self._lock:
            self._proc = None
            self._supervisor = None
            self._started_ts = 0.0
        logger.info("stereo: supervisor stopped")
