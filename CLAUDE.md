# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repo and branches

- Remote: `https://github.com/vshie/KaumauiCam`.
- Develop on the **`wailoacam`** branch (Wailoa Cam deployment).
- `main` and the other branches target a different hardware setup (Kaumaui). Don't commit Wailoa work to them or merge between them unless explicitly asked.
- `Wailoa-Cam` is an older branch and `cursor/*` branches are agent work branches. Neither is the development target.

## What this is

A BlueOS extension ("Wailoa Cam") running in Docker on an **amd64** BlueOS host. It's a single Python/Flask process plus a supervised **go2rtc** binary. Features:

- Full-screen live preview.
- Scheduled YouTube Live over RTMP.
- Daytime record/pause cycles for two cameras: mono MP4 plus MarineSitu C3 stereo MKV.
- Starlink link-uptime logging.
- Orca camera `/data` CSV logging.
- HydroVu water-quality scraping.

`README.md` has the feature spec and the API table, but parts of it are stale (it still mentions multi-arch CI and the `vshie/WailoaCam` repo).

## Build / deploy

There's no test suite, linter, or local dev harness. Paths like `/app/data` and `/mnt/usb` are hard-coded, and the app needs `ffmpeg`, `go2rtc`, GStreamer, and a reachable camera, so real testing means building the image and running it on the BlueOS host.

```bash
docker buildx build --platform linux/amd64 -t vshie/wailoa_cam:dev --load .
docker save vshie/wailoa_cam:dev -o wailoa_cam.tar   # BlueOS Extensions → "Load from file"
```

- **amd64 only.** `depthai==2.29.0.0` (stereo capture) ships only manylinux x86_64 wheels. `.github/workflows/deploy.yml` builds and pushes only `linux/amd64` to `<DOCKER_USERNAME>/blueos-wailoa_cam:<branch>`, with `:latest` added for semver tags. It deliberately does *not* use `BlueOS-community/Deploy-BlueOS-Extension`, because that action forces arm builds that fail on the native deps.
- GStreamer, PyGObject, and VA-API come from apt in the Dockerfile, not pip. The image also ships a static FFmpeg 7.x in `/usr/local/bin` because go2rtc 1.9 rejects Ubuntu's FFmpeg 4.4.
- Vue 3 and the DM Sans fonts are downloaded into `app/static/vendor/` at build time so the UI works offline. Don't add runtime CDN references.
- The BlueOS container permissions (host network, privileged, `/app/data` bind) live in the Dockerfile `LABEL permissions` and are mirrored in README. Keep the two in sync.
- Bump `_EXTENSION_VERSION` in `app/main.py` for releases.

Runtime: the entrypoint runs `python3 -u /app/main.py`. The UI/API listens on `PORT` (default 6042) and scans 6040–6060 if that port is busy. go2rtc listens on 127.0.0.1:1984 and is proxied at `/go2rtc/*`.

## Architecture

`app/main.py` holds the Flask routes, global state, and wiring. Each other module runs a background thread started from `main()`, and each start is wrapped in its own try/except so one failure can't block the UI.

- **`_scheduler_loop`** (2 s tick) is the central supervisor. Each tick it reloads config and decides whether each pipeline should run (from the schedule/cycle or the manual `_*_force` flags). It restarts crashed or stalled YouTube ffmpeg (no RTMP bytes for `STREAM_STALL_SECS`) and runs the YouTube-health watchdog; the kickoff/link-down/post-recovery `end_reason` logic is described in README. Stereo gets its own `_tick_stereo`, called first, so the `continue`s in the YouTube and Axis sections can't starve it.
- **`_apply_boot`** runs in its own thread so HTTP binds before slow camera calls (BlueOS health checks). It starts go2rtc on the fixed live feed *first*, then tries the legacy Axis profile setup, which may time out.
- **Camera sources are split:**
  - Live preview and mono recording use the fixed `CAMERA_RTSP_URL` (`rtsp://192.168.0.142:8554/unicast`, hard-coded in main.py, not configurable).
  - YouTube still streams the Axis `youtubelive` profile through `AxisCamera.rtsp_url()` (`camera.py`, VAPIX digest, `camera_host` config).
  - The browser plays go2rtc's fragmented MP4 (`/go2rtc/api/stream.mp4?src=livepreview`) in a `<video>` element. There's no WebRTC negotiation.
- **Config** (`config.py`) is a JSON file at `/app/data/config.json` merged over `DEFAULT_CONFIG`. Modules receive config getters as lambdas and reload them on every cycle, so Settings edits apply without a restart. When adding a setting, add its default to `DEFAULT_CONFIG` and read it at use time.
- **Time**: all scheduling uses fixed HST (`scheduler.SCHEDULE_TIMEZONE`, UTC−10) via `schedule_now()`. YouTube uses 15-minute slots with a weekday filter. The mono (`recordings_cycle`) and stereo (`stereo_cycle`) cycles use record-secs/pause-secs inside a fixed 07:45–18:00 window, which is a deliberate constant.
- **YouTube**: `youtube.py` runs ffmpeg with stream-copied video plus a silent AAC track (YouTube won't go live without audio). Byte deltas go to `bandwidth.py`. `youtube_api.py` is the optional OAuth device-flow mode, managing one broadcast per HST day (`ensure_todays_broadcast` supplies the key). `youtube_monitor.py` scrapes the public channel `/live` page for `isLiveNow`, which detects the "Preparing stream" lockup.
- **ffmpeg flags are empirically tuned.** YouTube input uses only `-fflags +genpts+igndts`; `+nobuffer`, `-use_wallclock_as_timestamps`, and `-shortest` were measured to drop 70–90% of frames. The mono recorder *does* use wall-clock timestamps, to avoid half-speed MP4s. Read the module docstrings before changing flags.
- **Mono recorder** (`recorder.py`): captures RTSP to MPEG-TS and remuxes to MP4. It rotates 5-minute segments itself by sending SIGINT and asks `_recorder_should_continue` before each new segment.
- **Stereo recorder** (`stereo_recorder.py`) supervises `app/c3record/main.py` as a subprocess. That script is an **unmodified vendored copy** of the upstream C3Record project (DepthAI + GStreamer, MKV output), and its modules import each other by bare name. Keep it pristine; put integration changes in `stereo_recorder.py`. The script rotates its own segments, and the supervisor SIGINTs it at the end of a burst. Tunables come from `stereo_tunables` config. DepthAI bootup takes 10–20 s and allows only one client at a time, hence the startup backoff and the free-space floors (`STEREO_*_MIN_FREE_BYTES`).
- **Storage** (`usb_storage.py`): mounts the first removable `sd*` partition at `/mnt/usb` (recordings under `/mnt/usb/WailoaCam/`), falling back to `/app/data`. Free-space guards block new clips. App logs are mirrored to a rotating file on USB, since container logs are lost on power cuts.
- **Persistence**:
  - SQLite `/app/data/state.db` (overridable with `WAILOA_STATE_DB`) is shared by `bandwidth.py`, `link_uptime.py` (8.8.8.8 ping every 10 s), and `youtube_monitor.py`. Each module owns its tables and a lock.
  - `orca.py` appends to `/app/data/orca.csv`. Columns are the camera's `/data` keys converted to snake_case, plus a trailing raw `payload_json`.
  - `hydrovu.py` uses undocumented HydroVu SPA endpoints with a public token. It refreshes hourly into `hydrovu_cache.json` (read by the Live tab) and `hydrovu.csv`. All HydroVu API knowledge lives in that module.
- **Frontend**: a single `app/static/index.html` (Vue 3 global build, no build step) plus `styles.css`.

## Conventions

Modules use `from __future__ import annotations`, a module-level `logger`, and long docstrings/comments explaining field-measured behavior (Starlink blips, Axis/DepthAI quirks). Keep that level of rationale when changing tuned values. Background loops log exceptions rather than dying.
