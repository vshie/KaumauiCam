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

## Manual installation (BlueOS Extensions Manager)

In **BlueOS → Extensions → Installed → +** (Create Extension), fill in:

| Field | Value |
|---|---|
| Extension Identifier | `br.km` |
| Extension Name | `Kaumaui Cam` |
| Docker image | `vshie/blueos-kaumaui_cam` |
| Docker tag | `main` |

Paste the following into the **Permissions / Original Settings** JSON editor:

```json
{
  "ExposedPorts": {
    "6042/tcp": {},
    "8555/tcp": {},
    "8555/udp": {}
  },
  "HostConfig": {
    "Binds": [
      "/usr/blueos/extensions/kaumauicam:/app/data",
      "/dev:/dev",
      "/run/udev:/run/udev:ro"
    ],
    "NetworkMode": "host",
    "Privileged": true,
    "PortBindings": {
      "6042/tcp": [{ "HostPort": "" }]
    }
  }
}
```

What it grants:

- **ExposedPorts** — `6042/tcp` (web UI / API), `8555/tcp` + `8555/udp` (go2rtc WebRTC/RTSP).
- **Binds**
  - `/usr/blueos/extensions/kaumauicam:/app/data` — persistent storage for SQLite, config, and the SD-fallback recordings folder.
  - `/dev:/dev` — USB camera / removable storage device nodes.
  - `/run/udev:/run/udev:ro` — hot-plug detection for USB drives.
- **NetworkMode: host** — required for go2rtc / WebRTC and mDNS.
- **Privileged: true** — required to mount external exFAT / NTFS USB drives from inside the container.
- **PortBindings** — maps `6042/tcp` to a host port (BlueOS surfaces it in the Extensions UI).

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

- **HTTP UI:** port **6042** by default (host network; `PORT` env overrides). If busy, the app scans **6040–6060**.
- **WebRTC (go2rtc):** **8555/tcp+udp** on the host (advertised in the extension `permissions` label).
- **Data / SQLite / recordings fallback:** host bind  
  `/usr/blueos/extensions/kaumauicam` → `/app/data`

### Camera

1. Create stream profile **`livepreview`**: 720p H.264 (or use **Create livepreview profile** in the UI / `POST /api/camera/ensure-livepreview`).
2. **DefaultFishPond** is set to H.265 1920×1080 @ 15 fps on boot / via **Apply DefaultFishpond** in Settings.
3. **`youtubelive`** is auto-provisioned on boot (or via **Apply youtubelive** in Settings / `POST /api/camera/ensure-youtubelive`): H.264 1920×1080 @ 30 fps, MBR cap 4500 kbps, 2 s GOP, compression=20. This is what the YouTube path streams. On older Axis firmwares the StreamProfile slot groups aren't pre-allocated, so the extension uses VAPIX `action=add` then `action=update` to populate the slot.

### YouTube

- Ingest uses the dedicated **`youtubelive`** stream profile (H.264 1080p30, MBR 4500 kbps). Video is **stream-copied** (no re-encoding on the Pi), and a silent AAC audio track is mixed in from `lavfi anullsrc` because YouTube Live only registers a broadcast as live when an audio track is present. The only video input flag is `-fflags +genpts+igndts` to regenerate the PTS that Axis RTSP packets ship without; we deliberately do **not** use `+nobuffer`, `-use_wallclock_as_timestamps`, or `-shortest` (each was measured to drop ~70–90% of video frames before they reached the muxer).
- **Bandwidth:** `ffmpeg -progress` `total_size` deltas are stored in `/app/data/state.db`. Month total is the sum for the **current calendar month** (resets automatically on the 1st). Optional **+overhead %** in settings.
- **Link uptime:** a background thread pings **8.8.8.8** every 10 s and writes each result (success + RTT) to `link_pings` in `/app/data/state.db`. The Streaming page renders a 24-hour status-bar-style graph (5-min buckets) and a 24h uptime % so transient Starlink outages are visible even though the page itself can't load while the modem is offline. Retention is 30 days.
- **YouTube broadcast health monitor:** with a public channel URL configured (any of `/@handle`, `/@handle/streams`, `/@handle/live`, `/channel/UC…`, or a bare `@handle`) the extension polls the channel's `/live` page every 30 s while a session is supposed to be running and parses YouTube's own `isLiveNow` flag plus the canonical video URL out of the served HTML. Two things this gives you that the encoder-side view can't: (1) confirmation the broadcast is actually live to viewers, with concurrent viewer count and a click-through link to the watch page, and (2) automatic detection of YouTube's "Preparing stream" lockup — where ffmpeg cheerfully reports `running:true` and a healthy byte counter while YouTube never promotes the broadcast. The supervisor watchdog runs in two modes:
  - **Kickoff** (first **`youtube_health_kickoff_grace_secs`**, default **360 s** = 6 min, of a broadcast attempt): be patient with YouTube — fresh ingests routinely take 30–90 s to register and on Starlink we've seen 2–3 min — but bounce immediately with `end_reason="link_down_kickoff"` if the 8.8.8.8 ping monitor reports the link is down. ffmpeg started during a marginal link rarely recovers cleanly, so a fresh attempt with a fresh kickoff window is more reliable than letting the existing one limp along.
  - **Post-kickoff:** tolerate brief Starlink hiccups — ffmpeg can ride them out and YouTube usually reclaims the broadcast on its own once bytes flow again. Once pings have been steady for **`youtube_health_post_link_recovery_secs`** (default **60 s**) after such an outage, if YouTube is still confirmed not-live, the supervisor force-restarts with `end_reason="yt_unhealthy_post_link"` and a fresh kickoff window. With no link issues at all, fall back to the classic trigger: YouTube confirmed not-live for **`youtube_health_unhealthy_grace_secs`** (default 90 s) plus minimum session age **`youtube_health_min_session_age_secs`** (default 60 s) ⇒ `end_reason="yt_unhealthy"`.
The "kickoff" timer is per *broadcast attempt*, not per ffmpeg session — ffmpeg respawns within the same attempt (e.g. an RTMP "broken pipe") keep the original 6 min window so the operator doesn't silently get a fresh kickoff every time RTMP burps. The monitor only works for **public** broadcasts — unlisted/private streams don't appear on the channel `/live` page; for those, a future Data API integration would be required.

### Recordings

- Capture: **RTSP → MPEG-TS** while recording, then **remux to MP4** on stop.
- **USB:** first removable **`sd*`** partition mounted at `/mnt/usb/KaumauiCam/recordings`.
- **SD fallback:** `/app/data/recordings` — new clips **blocked** if free space is under **10 GB** on that filesystem.

### Solar logging (Victron MPPT via on-board ESPHome)

The on-board ESPHome device (default **Fishpond at 192.168.20.66**) exposes a Victron BlueSolar MPPT charger and a couple of relays. The extension polls its per-entity REST API every `solar_interval_secs` (default 60s) and appends one row to **`/app/data/solar.csv`**. The file is **cumulative** (append-only, no rotation) — the operator downloads or deletes it from **Settings → Solar logging (Victron)**.

CSV schema (header is written on first run; `timestamp_iso` is always column 1):

```
timestamp_iso, timestamp_epoch,
battery_voltage_v, battery_current_a, battery_temperature_c,
pv_voltage_v, pv_power_w,
load_output, load_current_a,
yield_today_wh, yield_total_wh, yield_yesterday_wh,
max_power_today_w, max_power_yesterday_w,
charging_mode, mppt_tracking, error,
device_type, serial, firmware
```

Numeric columns are stripped of their unit suffix (e.g. `26.83`, not `26.830 V`) so Excel / pandas parse them as numbers; the unit lives in the column name. Missing entities are written as empty cells rather than aborting the whole row, so a brief network blip on one sensor doesn't blank out the others. The file lives on the bind-mounted `/app/data` (i.e. `/usr/blueos/extensions/kaumauicam/solar.csv` on the host) so it survives container rebuilds.

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
| GET | `/api/link/status` | Starlink ICMP probe state (last reply, 24h uptime %) |
| GET | `/api/link/buckets` | Aggregated uptime buckets for graph (`?window=86400&bucket=300`, or fixed range via `?from=<unix-ts>&to=<unix-ts>` — used by the streaming page's 7am–6pm HST view) |
| GET | `/api/stream/youtube_health` | Latest YouTube channel `/live` poll (state, video URL, viewers, unhealthy timer) |
| POST | `/api/stream/youtube_health/poke` | Wake the monitor for an immediate poll (used after settings save) |
| GET | `/api/stream/youtube_health/history` | Recent YouTube health rows (`?since=<unix-ts>&limit=200`) |
| GET | `/api/storage` | USB mount, used / total / free bytes when mounted, plus SD free GB |
| GET | `/api/solar/status` | Solar logger status: enabled, host, interval, last sample, file size, row count, last error, 5-row preview |
| GET | `/api/solar/sample` | One-shot live poll of the ESPHome device (optional `?host=` override) |
| GET | `/api/solar/download` | Download `solar.csv` (cumulative, header + timestamp_iso first column) |
| POST | `/api/solar/delete` | Wipe `solar.csv` (logging continues, next row appears at the next poll) |
| POST | `/api/solar/poke` | Wake the logger thread immediately (used after Save on the Settings page) |
| POST | `/api/camera/ensure-livepreview` | Create profile on camera |
| POST | `/api/camera/ensure-fishpond` | Set DefaultFishPond params |
| POST | `/api/camera/ensure-youtubelive` | Create/refresh `youtubelive` profile (H.264 1080p30 MBR 4.5 Mbps) |
| * | `/go2rtc/<path>` | Reverse proxy to go2rtc (WebRTC signaling) |

## License

Use and modify for your deployment; attribute Blue Robotics / your org as appropriate.
