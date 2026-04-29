#!/usr/bin/env python3
"""Kaumaui Cam BlueOS extension — Flask API + go2rtc proxy + schedulers."""

from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, Tuple

import requests
from flask import Flask, Response, jsonify, request, send_from_directory, send_file, stream_with_context

import bandwidth
import config as cfgmod
from camera import AxisCamera
from go2rtc_svc import Go2RtcSupervisor, render_config
from recorder import Recorder
from scheduler import schedule_now, should_be_on
from usb_storage import (
    get_free_mb,
    get_recording_dir_usb,
    get_status as usb_status,
    is_mounted,
    sd_card_free_gb,
    start_probe,
    try_mount,
)
from youtube import YouTubeStreamer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("kaumaui")

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
GO2RTC_UPSTREAM = "http://127.0.0.1:1984"

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path="/static")

def _on_yt_session_event(event_type: str, session_id: str, info: Dict[str, Any]) -> None:
    """Persist YouTube session lifecycle events to SQLite. Wired into
    YouTubeStreamer so every ffmpeg invocation -- whether ended cleanly by
    schedule, killed by user, or crashed on its own -- leaves a row in
    `yt_sessions` that the UI / /api/stream/sessions can read back."""
    try:
        if event_type == "start":
            bandwidth.record_session_start(session_id, float(info.get("started_ts") or time.time()))
        elif event_type == "end":
            bandwidth.record_session_end(
                session_id,
                float(info.get("ended_ts") or time.time()),
                info.get("exit_code"),
                str(info.get("end_reason") or "ended"),
                info.get("last_stderr"),
            )
    except Exception:
        logger.exception("yt session event %s persist failed", event_type)


youtube_streamer = YouTubeStreamer(
    on_bytes_delta=lambda b, sid: bandwidth.record_delta(b, sid),
    on_session_event=_on_yt_session_event,
)
recorder = Recorder()
go2rtc_sup = Go2RtcSupervisor()

_state_lock = threading.Lock()
_youtube_force = False
_recording_force = False
_youtube_session_start = 0.0
_recording_error: str | None = None
_boot_applied = False

# Scheduler timing. Tick is short so a crashed YouTube ffmpeg gets respawned
# within ~SCHEDULER_TICK_SECS instead of the previous 5s gap (which YouTube
# viewers saw as "stream offline"). STREAM_STALL_SECS is the time a running
# ffmpeg process is allowed to push zero RTMP bytes before the supervisor
# considers it wedged and force-restarts it. 30s is comfortably longer than
# any real RTSP/RTMP handshake (<10s in practice on this Pi/Axis combo).
SCHEDULER_TICK_SECS = 2.0
STREAM_STALL_SECS = 30.0


def _camera() -> AxisCamera:
    c = cfgmod.load()
    return AxisCamera(c["camera_host"], c["camera_user"], c["camera_pass"])


def _recording_dir(cfg: Dict[str, Any]) -> Tuple[str, str]:
    """Return (directory, mode label)."""
    mode = cfg.get("recordings_storage", "auto")
    try_mount()
    usb_mounted = is_mounted()
    if mode == "usb":
        if not usb_mounted:
            raise RuntimeError("USB storage not mounted")
        return get_recording_dir_usb(), "usb"
    if mode == "sd":
        d = os.path.join("/app/data", "recordings")
        os.makedirs(d, exist_ok=True)
        return d, "sd"
    # auto
    if usb_mounted:
        return get_recording_dir_usb(), "usb"
    d = os.path.join("/app/data", "recordings")
    os.makedirs(d, exist_ok=True)
    return d, "sd"


def _can_start_recording(cfg: Dict[str, Any], dest_dir: str, label: str) -> Tuple[bool, str]:
    free_gb = sd_card_free_gb(dest_dir if label == "sd" else "/app/data")
    if label == "sd" or (label == "auto" and dest_dir.startswith("/app/data")):
        if free_gb is not None and free_gb < 10.0:
            return False, f"SD path has only {free_gb:.1f} GB free; need >= 10 GB"
    if label == "usb":
        free_mb = get_free_mb()
        if free_mb is not None and free_mb < 100:
            return False, f"USB low space: {free_mb} MB"
    return True, "ok"


def _apply_boot() -> None:
    """Camera / go2rtc setup; may block on HTTP — run from a daemon thread only."""
    global _boot_applied
    if _boot_applied:
        return
    _boot_applied = True
    try:
        cam = _camera()
        cam.ensure_defaultfishpond_profile()
    except Exception as e:
        logger.warning("boot fishpond profile: %s", e)
    try:
        cam = _camera()
        ok, msg = cam.ensure_youtubelive_profile()
        logger.info("boot youtubelive profile: %s (%s)", msg, ok)
    except Exception as e:
        logger.warning("boot youtubelive profile: %s", e)
    try:
        cam = _camera()
        rtsp = cam.rtsp_url("livepreview")
        render_config(rtsp)
        go2rtc_sup.start()
    except Exception as e:
        logger.warning("boot go2rtc: %s", e)


_EXTENSION_VERSION = "0.3.6"

YOUTUBE_STREAM_PROFILE = "youtubelive"


def _parse_listen_port() -> int:
    raw = os.environ.get("PORT", "6042")
    try:
        p = int(str(raw).strip() or "6042")
    except ValueError:
        return 6042
    if not (1 <= p <= 65535):
        return 6042
    return p


def _listen_ports_to_try() -> list[int]:
    preferred = _parse_listen_port()
    out = [preferred]
    for x in range(6040, 6061):
        if x != preferred:
            out.append(x)
    return out


def _scheduler_loop() -> None:
    global _youtube_force, _recording_force, _youtube_session_start, _recording_error
    while True:
        try:
            cfg = cfgmod.load()

            now = schedule_now()

            # YouTube desired. Within an active window we want continuous
            # presence on YouTube Live, so the scheduler treats this as a
            # supervisor: if ffmpeg has died we restart it, and if ffmpeg is
            # alive but no bytes have flowed to RTMP for STREAM_STALL_SECS
            # we kill it (logged as end_reason="stalled") so the next tick
            # spawns a clean replacement. The `force` flag (set by the
            # "Start now" button) keeps the supervisor active even outside a
            # scheduled slot until the user clicks Stop.
            ys = cfg["youtube_schedule"]
            sched_yt = ys.get("enabled", False) and should_be_on(now, ys)
            with _state_lock:
                force = _youtube_force
            want_yt = force or sched_yt
            key = (cfg.get("youtube_stream_key") or "").strip()
            if want_yt and key:
                if not youtube_streamer.is_running():
                    cam = _camera()
                    rtsp = cam.rtsp_url(YOUTUBE_STREAM_PROFILE)
                    if youtube_streamer.start(rtsp, key):
                        with _state_lock:
                            _youtube_session_start = time.time()
                else:
                    # Stall watchdog: ffmpeg's process can stay alive while
                    # its internal thread queues wedge after a transient
                    # RTMP/RTSP hiccup, leaving bytes flat-lined to YouTube
                    # and no further stderr. If no progress bytes have been
                    # reported for STREAM_STALL_SECS, kill and let the next
                    # iteration restart it from scratch -- recorded as
                    # end_reason="stalled" in the broadcast history so the
                    # UI distinguishes watchdog-forced restarts from clean
                    # schedule/user stops.
                    stalled = youtube_streamer.seconds_since_last_byte()
                    if stalled > STREAM_STALL_SECS:
                        logger.warning(
                            "YouTube stream stalled %.0fs without bytes; force-restart",
                            stalled,
                        )
                        youtube_streamer.stop(reason="stalled")
            else:
                if youtube_streamer.is_running():
                    youtube_streamer.stop()

            # Recordings desired
            rs = cfg["recordings_schedule"]
            sched_rec = rs.get("enabled", False) and should_be_on(now, rs)
            with _state_lock:
                rforce = _recording_force
            want_rec = rforce or sched_rec
            if want_rec:
                if not recorder.is_running():
                    try:
                        dest, label = _recording_dir(cfg)
                    except Exception as e:
                        _recording_error = str(e)
                        time.sleep(5)
                        continue
                    ok, msg = _can_start_recording(cfg, dest, label)
                    if not ok:
                        _recording_error = msg
                        time.sleep(5)
                        continue
                    _recording_error = None
                    cam = _camera()
                    prof = cfg.get("recordings_profile", "DefaultFishPond")
                    rtsp = cam.rtsp_url(prof)
                    if not recorder.start(rtsp, dest):
                        _recording_error = "Recorder failed to start"
            else:
                if recorder.is_running():
                    recorder.stop()
        except Exception as e:
            logger.exception("scheduler: %s", e)
        time.sleep(SCHEDULER_TICK_SECS)


@app.route("/")
def index():
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/register_service")
def register_service():
    """BlueOS helper: sidebar entry + metadata (GET)."""
    return jsonify(
        {
            "name": "Kaumaui Cam",
            "description": "Axis live view, PTZ, YouTube Live scheduling, and local recordings.",
            "icon": "mdi-fish",
            "company": "Blue Robotics",
            "version": _EXTENSION_VERSION,
            "webpage": "https://github.com/vshie/KaumauiCam",
            "api": "https://github.com/vshie/KaumauiCam",
            "new_page": False,
            "works_in_relative_paths": True,
        }
    )


@app.route("/go2rtc/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
def go2rtc_proxy(path: str):
    if request.method == "OPTIONS":
        return Response("", status=204)
    url = f"{GO2RTC_UPSTREAM}/{path}"
    if request.query_string:
        url = url + "?" + request.query_string.decode()
    hop_headers = (
        "content-type",
        "content-length",
        "accept",
        "authorization",
    )
    headers = {}
    for k in hop_headers:
        if k in request.headers:
            headers[k] = request.headers[k]
    body = request.get_data()
    try:
        r = requests.request(
            method=request.method,
            url=url,
            data=body if body else None,
            headers=headers,
            stream=True,
            timeout=120,
        )
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 502

    excluded = {"content-encoding", "transfer-encoding", "connection"}

    def gen():
        for chunk in r.iter_content(chunk_size=65536):
            if chunk:
                yield chunk

    resp = Response(stream_with_context(gen()), status=r.status_code)
    for k, v in r.headers.items():
        if k.lower() not in excluded:
            resp.headers[k] = v
    return resp


@app.route("/api/health")
def health():
    return jsonify({"ok": True, "service": "kaumaui-cam"})


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    if request.method == "GET":
        return jsonify(cfgmod.load())
    data = request.get_json(force=True, silent=True) or {}
    out = cfgmod.update(data)
    if any(k in data for k in ("camera_host", "camera_user", "camera_pass")):
        try:
            cam = _camera()
            render_config(cam.rtsp_url("livepreview"))
            go2rtc_sup.stop()
            go2rtc_sup.start()
        except Exception as e:
            logger.warning("go2rtc reload after config: %s", e)
    return jsonify(out)


@app.route("/api/ptz/position", methods=["GET"])
def ptz_position():
    try:
        return jsonify(_camera().ptz_position())
    except Exception as e:
        # Camera offline is a normal operating state for this extension; return 503 and
        # skip the stack trace so we don't fill the log on every poll.
        return jsonify({"error": str(e), "offline": True}), 503


@app.route("/api/ptz/move", methods=["POST"])
def ptz_move():
    j = request.get_json(force=True, silent=True) or {}
    pan = float(j.get("pan", 0))
    tilt = float(j.get("tilt", 0))
    zoom = float(j.get("zoom", 0))
    try:
        _camera().ptz_continuous(pan, tilt, zoom)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ptz/stop", methods=["POST"])
def ptz_stop():
    try:
        _camera().ptz_stop()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ptz/home", methods=["POST"])
def ptz_home():
    try:
        _camera().ptz_goto_preset("Home")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ptz/autofocus", methods=["POST"])
def ptz_autofocus():
    j = request.get_json(force=True, silent=True) or {}
    on = bool(j.get("on", True))
    try:
        _camera().autofocus(on)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stream/start", methods=["POST"])
def stream_start():
    global _youtube_force, _youtube_session_start
    cfg = cfgmod.load()
    key = (cfg.get("youtube_stream_key") or "").strip()
    if not key:
        return jsonify({"error": "youtube_stream_key empty"}), 400
    with _state_lock:
        _youtube_force = True
    cam = _camera()
    try:
        cam.ensure_youtubelive_profile()
    except Exception as e:
        logger.warning("ensure youtubelive on stream/start: %s", e)
    rtsp = cam.rtsp_url(YOUTUBE_STREAM_PROFILE)
    if youtube_streamer.start(rtsp, key):
        with _state_lock:
            _youtube_session_start = time.time()
        return jsonify({"ok": True, "status": youtube_streamer.status()})
    return jsonify({"error": "failed to start"}), 500


@app.route("/api/stream/stop", methods=["POST"])
def stream_stop():
    global _youtube_force
    with _state_lock:
        _youtube_force = False
    youtube_streamer.stop()
    return jsonify({"ok": True})


@app.route("/api/stream/status", methods=["GET"])
def stream_status():
    cfg = cfgmod.load()
    with _state_lock:
        t0 = _youtube_session_start
        yf = _youtube_force
    st = youtube_streamer.status()
    sess = 0
    if youtube_streamer.is_running() and t0:
        sess = bandwidth.session_sum_since(t0, st.get("session_id"))
    bw = bandwidth.status(cfg.get("bandwidth_overhead_pct", 3), cfg.get("monthly_quota_gb", 0))
    st.update({"bandwidth": bw, "session_bytes": sess, "force": yf})
    return jsonify(st)


@app.route("/api/stream/sessions", methods=["GET"])
def stream_sessions():
    """Recent YouTube broadcast history. Defaults to the last 24h so the
    streaming page can show today's sessions without paginating; pass
    ?limit=N or ?since=<unix-ts> to override."""
    try:
        limit = int(request.args.get("limit", 50))
    except (TypeError, ValueError):
        limit = 50
    since: float | None
    raw_since = request.args.get("since")
    if raw_since is None:
        since = time.time() - 24 * 3600
    else:
        try:
            since = float(raw_since)
        except (TypeError, ValueError):
            since = None
    sessions = bandwidth.recent_sessions(limit=limit, since_ts=since)
    return jsonify({"sessions": sessions, "since": since, "now": time.time()})


@app.route("/api/bandwidth/status", methods=["GET"])
def bw_status():
    cfg = cfgmod.load()
    return jsonify(bandwidth.status(cfg.get("bandwidth_overhead_pct", 3), cfg.get("monthly_quota_gb", 0)))


@app.route("/api/bandwidth/reset", methods=["POST"])
def bw_reset():
    bandwidth.reset_month_manual()
    return jsonify({"ok": True})


@app.route("/api/storage", methods=["GET"])
def storage():
    try_mount()
    u = usb_status()
    u["sd_free_gb"] = sd_card_free_gb("/app/data")
    return jsonify(u)


@app.route("/api/camera/ensure-livepreview", methods=["POST"])
def ensure_livepreview():
    try:
        ok, msg = _camera().ensure_livepreview_profile()
        cam = _camera()
        render_config(cam.rtsp_url("livepreview"))
        return jsonify({"ok": ok, "message": msg})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/camera/ensure-fishpond", methods=["POST"])
def ensure_fishpond():
    try:
        _camera().ensure_defaultfishpond_profile()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/camera/ensure-youtubelive", methods=["POST"])
def ensure_youtubelive():
    try:
        ok, msg = _camera().ensure_youtubelive_profile()
        return jsonify({"ok": ok, "message": msg})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/proxy/snapshot")
def proxy_snapshot():
    try:
        data = _camera().snapshot_jpeg()
        return Response(data, mimetype="image/jpeg")
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/recordings/config", methods=["GET", "POST"])
def rec_config():
    if request.method == "GET":
        cfg = cfgmod.load()
        return jsonify(
            {
                "schedule": cfg.get("recordings_schedule"),
                "storage": cfg.get("recordings_storage"),
                "profile": cfg.get("recordings_profile"),
            }
        )
    j = request.get_json(force=True, silent=True) or {}
    patch: Dict[str, Any] = {}
    if "schedule" in j:
        patch["recordings_schedule"] = j["schedule"]
    if "storage" in j:
        patch["recordings_storage"] = j["storage"]
    if "profile" in j:
        patch["recordings_profile"] = j["profile"]
    return jsonify(cfgmod.update(patch))


@app.route("/api/recordings/start", methods=["POST"])
def rec_start():
    global _recording_force, _recording_error
    cfg = cfgmod.load()
    try:
        dest, label = _recording_dir(cfg)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    ok, msg = _can_start_recording(cfg, dest, label)
    if not ok:
        return jsonify({"error": msg}), 400
    with _state_lock:
        _recording_force = True
    cam = _camera()
    prof = cfg.get("recordings_profile", "DefaultFishPond")
    rtsp = cam.rtsp_url(prof)
    if recorder.start(rtsp, dest):
        _recording_error = None
        return jsonify({"ok": True, "dest": dest, "label": label})
    return jsonify({"error": "start failed"}), 500


@app.route("/api/recordings/stop", methods=["POST"])
def rec_stop():
    global _recording_force
    with _state_lock:
        _recording_force = False
    recorder.stop()
    return jsonify({"ok": True})


@app.route("/api/recordings/status", methods=["GET"])
def rec_status():
    cfg = cfgmod.load()
    with _state_lock:
        rf = _recording_force
        re = _recording_error
    try:
        dest, label = _recording_dir(cfg)
    except Exception as e:
        dest, label = "", str(e)
    st = recorder.status()
    st.update({"dest": dest, "label": label, "error": re, "force": rf})
    return jsonify(st)


@app.route("/api/recordings/list", methods=["GET"])
def rec_list():
    cfg = cfgmod.load()
    try:
        dest, _ = _recording_dir(cfg)
    except Exception:
        dest = os.path.join("/app/data", "recordings")
    # Hide stub files left behind when gst-launch couldn't reach the camera.
    # The recorder is supposed to delete these itself, but a defensive
    # threshold here keeps the UI clean if any slip through (e.g. user
    # killed the container mid-segment, leaving a zero-byte file).
    # The currently-recording segment is exempt -- it's still being written
    # and may legitimately be tiny in its first second or two.
    min_bytes = 100 * 1024
    current = recorder.status().get("current_file") or ""
    current_name = os.path.basename(current) if current else ""
    items = []
    if os.path.isdir(dest):
        for n in sorted(os.listdir(dest), reverse=True):
            if not n.endswith((".ts", ".mp4")):
                continue
            p = os.path.join(dest, n)
            try:
                sz = os.path.getsize(p)
                if sz < min_bytes and n != current_name:
                    continue
                items.append({"name": n, "size": sz, "mtime": os.path.getmtime(p)})
            except OSError:
                pass
    return jsonify({"files": items, "dir": dest})


@app.route("/api/recordings/delete", methods=["POST"])
def rec_delete():
    j = request.get_json(force=True, silent=True) or {}
    name = j.get("name", "")
    if not name or "/" in name or ".." in name:
        return jsonify({"error": "bad name"}), 400
    cfg = cfgmod.load()
    try:
        dest, _ = _recording_dir(cfg)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    path = os.path.join(dest, name)
    if os.path.isfile(path):
        os.remove(path)
        return jsonify({"ok": True})
    return jsonify({"error": "not found"}), 404


@app.route("/api/recordings/cleanup-empty", methods=["POST"])
def rec_cleanup_empty():
    """Delete all stub mp4/ts files smaller than 100 KB. These accumulate
    when the camera RTSP is rejecting the configured profile (each retry
    creates a fresh empty file via gst's filesink before the negotiation
    fails). Safe to call any time; the currently-recording segment is
    skipped by name."""
    min_bytes = 100 * 1024
    cfg = cfgmod.load()
    try:
        dest, _ = _recording_dir(cfg)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    if not os.path.isdir(dest):
        return jsonify({"ok": True, "deleted": 0})
    current = recorder.status().get("current_file") or ""
    current_name = os.path.basename(current) if current else ""
    deleted = 0
    errors: list[str] = []
    for n in os.listdir(dest):
        if not n.endswith((".ts", ".mp4")):
            continue
        if current_name and n == current_name:
            continue
        p = os.path.join(dest, n)
        try:
            if os.path.getsize(p) < min_bytes:
                os.remove(p)
                deleted += 1
        except OSError as e:
            errors.append(f"{n}: {e}")
    return jsonify({"ok": True, "deleted": deleted, "errors": errors})


@app.route("/api/recordings/delete-all", methods=["POST"])
def rec_delete_all():
    cfg = cfgmod.load()
    try:
        dest, _ = _recording_dir(cfg)
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    if not os.path.isdir(dest):
        return jsonify({"ok": True, "deleted": 0, "errors": []})
    # Skip the segment currently being written so we don't fight gstreamer.
    current = recorder.status().get("current_file") or ""
    current_name = os.path.basename(current) if current else ""
    deleted = 0
    errors: list[str] = []
    for n in os.listdir(dest):
        if not n.endswith((".ts", ".mp4")):
            continue
        if current_name and n == current_name:
            continue
        p = os.path.join(dest, n)
        try:
            if os.path.isfile(p):
                os.remove(p)
                deleted += 1
        except OSError as e:
            errors.append(f"{n}: {e}")
    return jsonify({"ok": True, "deleted": deleted, "errors": errors})


@app.route("/api/recordings/download/<name>")
def rec_download(name: str):
    if "/" in name or ".." in name:
        return "bad", 400
    cfg = cfgmod.load()
    try:
        dest, _ = _recording_dir(cfg)
    except Exception as e:
        return str(e), 400
    path = os.path.join(dest, name)
    if os.path.isfile(path):
        return send_file(path, as_attachment=True, download_name=name)
    return "not found", 404


def main() -> None:
    logger.info("Kaumaui Cam starting (camera may be offline; UI comes up first)")
    try:
        bandwidth.init_db()
    except Exception:
        logger.exception("bandwidth.init_db failed")
    try:
        cfgmod.load()
    except Exception:
        logger.exception("config load failed")
    try:
        start_probe()
    except Exception:
        logger.exception("USB probe start failed")
    threading.Thread(target=_scheduler_loop, daemon=True, name="scheduler").start()
    # Defer camera/go2rtc so we bind HTTP before VAPIX/RTSP timeouts (BlueOS health checks).
    threading.Thread(target=_apply_boot, daemon=True, name="boot").start()
    last_err: OSError | None = None
    for port in _listen_ports_to_try():
        try:
            logger.info("Listening on 0.0.0.0:%s", port)
            app.run(host="0.0.0.0", port=port, threaded=True)
            return
        except OSError as e:
            last_err = e
            errno = getattr(e, "errno", None)
            if errno in (48, 98) or "Address already in use" in str(e):
                logger.warning("Bind failed on port %s: %s", port, e)
                continue
            raise
    logger.error("No free port in scan range; last error: %s", last_err)
    raise SystemExit(1) from last_err


if __name__ == "__main__":
    main()
