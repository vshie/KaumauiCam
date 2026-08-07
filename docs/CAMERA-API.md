# MC800S5_AF camera: lens control API findings

Reverse-engineered from the camera's own web UI and verified against the live
video stream. Written to be handed to another agent working on this device.

## Device

| | |
| --- | --- |
| Firmware | `MC800S5_AF_V0-A-RTMP-H5 V3.4.5.2 build 2025-11-12` |
| Kernel | `Linux 5.10.61 #160 PREEMPT armv7l` |
| Serial | `EF00000006304E40` |
| Address used here | `192.168.1.56`, login `admin` / `123456` |
| Optics | Motorised zoom + focus, no pan/tilt, no iris |
| Video | `rtsp://admin:123456@<ip>:554/stream0` (main), `/stream1` (sub) |

## Ports

| Port | What it is | Useful? |
| --- | --- | --- |
| 80 | Web UI, the vendor XML API, and ONVIF (`/onvif/*`) | Yes — all control lives here |
| 554 | RTSP | Yes — video, and the only way to observe lens state |
| 8000 | "HIK" Hikvision private SDK, binary, auth disabled | Not without the Hikvision SDK; resets on HTTP |
| 8091 | Vendor "Control Protocol", binary, used by their search tool | Not without their SDK; resets on HTTP |
| 12351-12354 | WebSocket video for the HTML5 player (main/sub/replay/third) | Video only, accepts a WS upgrade at `ws://<ip>:12351/`, carries no control |

## Authentication for the vendor API

Each credential is encrypted separately with **DES-ECB under the fixed key
`WebLogin`**, zero-padded to an 8-byte boundary, then hex encoded. No session,
cookie or token is involved — the same two strings are replayed on every
request, so they can be computed once.

```python
from cryptography.hazmat.decrepit.ciphers.algorithms import TripleDES
from cryptography.hazmat.primitives.ciphers import Cipher, modes

def des_hex(text: str) -> str:
    data = text.encode()
    data += b"\x00" * (-len(data) % 8)
    enc = Cipher(TripleDES(b"WebLogin"), modes.ECB()).encryptor()
    return (enc.update(data) + enc.finalize()).hex()
```

A 64-bit key makes TripleDES equivalent to single DES, which is why the
deprecated `TripleDES` primitive is used here. For the factory login this gives
`admin` → `52851dbd7918bbae` and `123456` → `a17faccd02661e4c`.

## Request format

**The content type is the whole trick.** Send
`Content-Type: application/x-www-form-urlencoded`. With `text/xml` the camera
hands the request to its ONVIF gSOAP handler instead, which fails with
`Validation constraint violation: missing root element`. This is the single
reason earlier attempts at this API looked unreachable.

Note the literal tab characters before `<userid>` and `<passwd>`, copied from
the stock UI. Success is **HTTP 202 Accepted with an empty body** — there is no
acknowledgement of whether the lens actually moved.

```python
import urllib.request

def ptz(ip, cmd, extra="", path="/setPTZCmd"):
    body = (
        '<?xml version="1.0"?>'
        '<soap:Envelope xmlns:soap="http://www.w3.org/2001/12/soap-envelope">'
        f'<soap:Header>\t<userid>{des_hex("admin")}</userid>'
        f'\t<passwd>{des_hex("123456")}</passwd></soap:Header>'
        f'<soap:Body><xml><cmd>{cmd}</cmd>{extra}</xml></soap:Body>'
        '</soap:Envelope>'
    )
    req = urllib.request.Request(
        f"http://{ip}{path}", data=body.encode(),
        headers={
            "Content-type": "application/x-www-form-urlencoded",
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    return urllib.request.urlopen(req, timeout=5).status  # 202
```

## Commands

### Lens — `POST /setPTZCmd`

Motion is continuous: the command starts the motor and it runs until `stop`.

| `<cmd>` | Effect | Verified |
| --- | --- | --- |
| `zoomtele` | Zoom in until stopped | Works |
| `zoomwide` | Zoom out until stopped | Works |
| `FocusNearAutoOff` | Focus nearer, **turns autofocus off** | Works |
| `FocusFarAutoOff` | Focus farther, **turns autofocus off** | Works |
| `stop` | Stops whichever lens motor is running | Works |
| `IrisOpenAutoOff` | — | Accepted, no effect |
| `IrisCloseAutoOff` | — | Accepted, no effect |

The `AutoOff` suffix is valuable: it disables autofocus while moving, so manual
focus stays put. Measured to hold for at least 18 seconds idle. Zooming
afterwards can wake autofocus again, and **nothing found so far turns AF back
on** — ONVIF `AutoFocusMode=AUTO` is accepted but does nothing, and the camera's
own config only ever reads its AF flag.

There is **no speed control for the lens**. The web UI's "Speed (1–10)" selector
is element `#ps`, read only by the pan/tilt handler, which sends
`<panspeed>`/`<tiltspeed>`. Adding those fields, or `<speed>`, or `<zoomspeed>`,
to a zoom command changes nothing (measured movement 68, 71, 68, 68, 69 for a
1-second run at speeds 1 and 10). ONVIF velocity is ignored too (0.1 → 67,
1.0 → 67). One fixed speed on every path.

### Pan/tilt — `POST /setPTZCmd`

`<xml><cmd>DIR</cmd><panspeed>N</panspeed><tiltspeed>N</tiltspeed></xml>`, sent
out the serial port. Irrelevant on this unit — no pan/tilt head is attached.

### Presets — `POST /PresetList` (not `/setPTZCmd`)

| Payload | Purpose |
| --- | --- |
| `<xml><cmd>setpreset</cmd><preset>N</preset><flag>1</flag></xml>` | Store preset N |
| `<xml><cmd>callpreset</cmd><preset>N</preset></xml>` | Recall preset N |
| `<xml><cmd>clearpreset</cmd><preset>N</preset></xml>` | Delete preset N |

**These do not move the lens.** They are accepted, and `POST /getPresetList`
afterwards really does list the stored numbers, but recall never restores lens
position. `getPtzConfig` explains why: the camera is set to `PELCO_D` at 2400
baud on COM1, and 1–255 is the PELCO_D preset range, so these are serialised out
to a pan/tilt head that is not connected. Reserved numbers confirm the intent:
92/93 set scan borders, 99 starts scan, 98 cruise, 94 reboots the PTZ, 84 is
"focus restore", 82 "PTZ restore". Verified by saving a preset at a zoomed-in
position, zooming away, recalling, then waiting 10s: the frame was unchanged
from where it had been moved to.

### Other endpoints seen in the web UI

All take the same envelope and content type. `/ipcLogin`, `/WEBLogin`,
`/getUserConfigPwdEntrypt`, `/getSystemVersionInfo`, `/getPtzConfig`,
`/getPresetList`, `/getMediaVideoConfig`, `/getMediaStreamConfig`,
`/setMediaVideoEncodeConfig`, `/getNetworkConfig`, `/setNetworkLANConfig`,
`/getTimeConfig`, `/setForceIdr`, `/set3DYuntaiCoordinate`.

`/ipcLogin` returns a `<SystemFunction>` capability list including `ptz_zoom`,
`ptz_focus`, `ptz_iris`. Treat it as advertising, not proof — `ptz_iris` is
listed on a camera with no iris.

`/set3DYuntaiCoordinate` takes
`<Yuntai3D><Yuntai3DCoordinate StartX="" StartY="" EndX="" EndY="" /></Yuntai3D>`
for drag-a-box positioning. Aimed at pan/tilt; untested here.

## ONVIF, for comparison

Endpoints `/onvif/ptz` and `/onvif/imaging`, profile token `MainStream`, video
source token `VideoSourceMain`. ONVIF authentication is disabled in the camera's
settings, though WSSE headers are harmless.

| Operation | Result |
| --- | --- |
| PTZ `ContinuousMove` (zoom) | Works, velocity ignored |
| Imaging `Move` continuous (focus) | Works, but leaves autofocus on, so it fights you |
| Imaging `SetImagingSettings` exposure | Works — the only usable light control |
| PTZ `AbsoluteMove` | 200 OK, lens never moves |
| PTZ `GetStatus` | Always reports zero for zoom and focus |
| `GotoPreset` | 200 OK, lens never moves |
| `AutoFocusMode=AUTO` | 200 OK, autofocus does not come back |

Prefer the vendor API purely because of `AutoOff`.

## Positioning without feedback

Nothing on this camera reports lens position, and no absolute move works, so the
only reliable reference is a mechanical end-stop. Run to an end-stop, then drive
back for a measured time.

Homing is excellent: three runs to the wide end-stop from scattered positions
produced frames differing by 0.7–0.9 against a scene noise floor of 1.0. All the
error is in the timed drive, and it is small — repeating an identical 1.12s
drive differed by 12.8, while deliberate offsets of 0.2s and 0.5s differed by
23.2 and 31.0, putting the repeat error under 0.1s of travel, roughly 3% of the
zoom range.

| Axis | End-to-end travel |
| --- | --- |
| Zoom, wide ↔ tele | 3.2 s |
| Focus, near ↔ far | 16.0 s |

Focus is slow, so a full two-axis recall takes about 40 seconds. Re-measure with
`calibrate.py` in this repo after a firmware change.

## How to verify anything on this camera

Every command returns 202 whether or not it does something, so **never trust the
response**. Measure the video instead. Grab a frame with
`ffmpeg -rtsp_transport tcp -i <rtsp> -frames:v 1 -q:v 2 out.jpg`, then:

- **Did the lens move?** Mean pixel difference between two frames:
  `[0:v][1:v]blend=all_mode=difference,signalstats` and read `YAVG`. A static
  scene sits around 1–5; a real zoom move reads 40+.
- **Did framing change, ignoring focus?** Apply `boxblur=12` to both frames
  before differencing, so blur changes drop out and only framing survives.
- **Did focus change?** Frame difference is a poor detector — use JPEG size at
  fixed quality as a sharpness proxy, since blur compresses far smaller.
- **Point the camera at something detailed.** Focus measurements against a blank
  wall produce nothing but noise.

Beware two traps that produced wrong conclusions early on: autofocus hunting
raises the apparent noise floor to ~9, which masks real movement if the
threshold is fixed; and a blown-out highlight in frame inflates difference
scores, making a good recall look bad.
