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
from scheduler import cycle_should_be_on
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

youtube_streamer = YouTubeStreamer(on_bytes_delta=lambda b, sid: bandwidth.record_delta(b, sid))
recorder = Recorder()
go2rtc_sup = Go2RtcSupervisor()

_state_lock = threading.Lock()
_youtube_force = False
_recording_force = False
_youtube_session_start = 0.0
_recording_error: str | None = None
_boot_applied = False


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
        rtsp = cam.rtsp_url("livepreview")
        render_config(rtsp)
        go2rtc_sup.start()
    except Exception as e:
        logger.warning("boot go2rtc: %s", e)


def _scheduler_loop() -> None:
    global _youtube_force, _recording_force, _youtube_session_start, _recording_error
    while True:
        try:
            cfg = cfgmod.load()
            import datetime as dt

            now = dt.datetime.now()

            # YouTube desired
            ys = cfg["youtube_schedule"]
            sched_yt = ys.get("enabled", False) and cycle_should_be_on(
                now,
                ys.get("window_start", "06:00"),
                ys.get("window_stop", "18:00"),
                int(ys.get("interval_min", 60)),
                int(ys.get("duration_min", 20)),
            )
            with _state_lock:
                force = _youtube_force
            want_yt = force or sched_yt
            key = (cfg.get("youtube_stream_key") or "").strip()
            if want_yt and key:
                if not youtube_streamer.is_running():
                    cam = _camera()
                    rtsp = cam.rtsp_url(None)
                    if youtube_streamer.start(rtsp, key):
                        with _state_lock:
                            _youtube_session_start = time.time()
            else:
                if youtube_streamer.is_running():
                    youtube_streamer.stop()

            # Recordings desired
            rs = cfg["recordings_schedule"]
            sched_rec = rs.get("enabled", False) and cycle_should_be_on(
                now,
                rs.get("window_start", "06:00"),
                rs.get("window_stop", "18:00"),
                int(rs.get("interval_min", 60)),
                int(rs.get("duration_min", 30)),
            )
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
        time.sleep(5)


@app.route("/")
def index():
    return send_from_directory(STATIC_DIR, "index.html")


@app.route("/go2rtc/<path:path>", methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
def go2rtc_proxy(path: str):
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
        return jsonify({"error": str(e)}), 500


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
    rtsp = cam.rtsp_url(None)
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
    items = []
    if os.path.isdir(dest):
        for n in sorted(os.listdir(dest), reverse=True):
            if n.endswith(".mp4"):
                p = os.path.join(dest, n)
                try:
                    items.append({"name": n, "size": os.path.getsize(p), "mtime": os.path.getmtime(p)})
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
    bandwidth.init_db()
    cfgmod.load()
    start_probe()
    threading.Thread(target=_scheduler_loop, daemon=True, name="scheduler").start()
    _apply_boot()
    port = int(os.environ.get("PORT", "6030"))
    app.run(host="0.0.0.0", port=port, threaded=True)


if __name__ == "__main__":
    main()
