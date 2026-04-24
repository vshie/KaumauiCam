# Kaumaui Cam — BlueOS extension

BlueOS extension for an Axis PTZ camera (fixed IP **192.168.20.20** by default): WebRTC live preview (`livepreview` 720p H.264 via **go2rtc**), VAPIX PTZ, scheduled **YouTube Live** (H.264 RTMP, bandwidth meter with SQLite persistence), and scheduled **MP4** recordings (default `DefaultFishPond` H.265 profile) to USB or SD.

## Reference

Layout and GitHub Action follow [BlueOS_videorecorder / dropcam](https://github.com/vshie/BlueOS_videorecorder/tree/dropcam).

## GitHub Actions (image publish)

Repository secrets:

- `DOCKER_USERNAME` / `DOCKER_PASSWORD` — Docker Hub (or registry) credentials used by `Deploy-BlueOS-Extension`.

Repository variables (example names from workflow):

- `MY_NAME`, `MY_EMAIL`, `ORG_NAME`, `ORG_EMAIL`

On **push**, the workflow builds **linux/arm64** and **linux/arm/v7** and publishes the extension image.

## After each code change: local `.tar` + `git push`

The image tar is built **on your machine** (not in GitHub Actions). **`kaumaui_cam.tar`** is gitignored and stays local for BlueOS “Load from file”.

```bash
git add -A && git commit -m "your message"   # when you have changes to commit
chmod +x scripts/build-tar-and-push.sh
./scripts/build-tar-and-push.sh              # build arm64 tar + git push current branch
```

Override image tag or output path if needed:

```bash
IMAGE=vshie/kaumaui_cam:dev OUT=kaumaui_cam.tar ./scripts/build-tar-and-push.sh
```

## Local image / tar (manual install on Pi)

From this directory:

```bash
docker buildx build --platform linux/arm64 \
  -t vshie/kaumaui_cam:dev --load .
docker save vshie/kaumaui_cam:dev -o kaumaui_cam.tar
```

`docker save` always produces a **tar** archive; using the **`.tar`** extension matches that. Install **kaumaui_cam.tar** from the BlueOS Extension Manager (“Load from file”).

Optional smaller file (gzip-wrapped tar, still loadable after decompress or wherever your UI accepts it):

```bash
docker save vshie/kaumaui_cam:dev | gzip > kaumaui_cam.tar.gz
```

For multi-arch without `--load`:

```bash
docker buildx build --platform linux/arm64,linux/arm/v7 \
  -t vshie/kaumaui_cam:dev --push .
```

## Runtime (BlueOS)

- **HTTP UI:** port **6030** (host network).
- **WebRTC (go2rtc):** **8555/tcp+udp** on the host (advertised in the extension `permissions` label).
- **Data / SQLite / recordings fallback:** host bind  
  `/usr/blueos/extensions/kaumauicam` → `/app/data`

### Camera

1. Create stream profile **`livepreview`**: 720p H.264 (or use **Create livepreview profile** in the UI / `POST /api/camera/ensure-livepreview`).
2. **DefaultFishPond** is set to H.265 1920×1080 @ 15 fps on boot / via **Apply DefaultFishpond** in Settings.

### YouTube

- Ingest uses **default** RTSP (no `streamprofile`) — **H.264** passthrough; audio is re-encoded to stereo AAC for compatibility.
- **Bandwidth:** `ffmpeg -progress` `total_size` deltas are stored in `/app/data/state.db`. Month total is the sum for the **current calendar month** (resets automatically on the 1st). Optional **+overhead %** in settings.

### Recordings

- Capture: **RTSP → MPEG-TS** while recording, then **remux to MP4** on stop.
- **USB:** first removable **`sd*`** partition mounted at `/mnt/usb/KaumauiCam/recordings`.
- **SD fallback:** `/app/data/recordings` — new clips **blocked** if free space is under **10 GB** on that filesystem.

## Push this repo (you deploy)

Repository: **https://github.com/vshie/KaumauiCam**

```bash
git init
git add .
git commit -m "Initial Kaumaui Cam extension"
git branch -M main
git remote add origin git@github.com:vshie/KaumauiCam.git
git push -u origin main
```

## API summary

| Method | Path | Purpose |
|--------|------|---------|
| GET/POST | `/api/config` | Full config |
| GET | `/api/ptz/position` | PTZ state |
| POST | `/api/ptz/move` | `{pan,tilt,zoom}` continuous |
| POST | `/api/ptz/stop` | Stop motion |
| POST | `/api/ptz/home` | Home preset |
| POST | `/api/ptz/autofocus` | `{on:true/false}` |
| POST | `/api/stream/start` | Start YouTube now |
| POST | `/api/stream/stop` | Stop YouTube |
| GET | `/api/stream/status` | Stream + bandwidth |
| GET/POST | `/api/recordings/config` | Recording schedule / storage / profile |
| POST | `/api/recordings/start` | Force record |
| POST | `/api/recordings/stop` | Stop record |
| GET | `/api/recordings/list` | MP4 list |
| GET | `/api/recordings/download/<name>` | Download |
| POST | `/api/recordings/delete` | `{name}` |
| GET | `/api/bandwidth/status` | Month/day totals |
| POST | `/api/bandwidth/reset` | Manual month reset |
| GET | `/api/storage` | USB mount + SD free GB |
| POST | `/api/camera/ensure-livepreview` | Create profile on camera |
| POST | `/api/camera/ensure-fishpond` | Set DefaultFishPond params |
| * | `/go2rtc/<path>` | Reverse proxy to go2rtc (WebRTC signaling) |

## License

Use and modify for your deployment; attribute Blue Robotics / your org as appropriate.
