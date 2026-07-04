"""YouTube Data API v3 broadcast lifecycle manager.

The rest of the extension pushes RTMP bytes with ffmpeg to a hardcoded
ingest URL (see ``app/youtube.py``) and scrapes the public ``/live``
page to observe whether YouTube is actually promoting the broadcast
(``app/youtube_monitor.py``). That's enough for a single manually-armed
scheduled broadcast to work once, but not for the "no manual input,
every day" operating mode: a YouTube *scheduled broadcast* is a
one-time event that never recreates itself, so the day after it runs
ffmpeg cheerfully keeps pushing bytes into the ingest, YouTube accepts
them at the network layer, and viewers see nothing.

This module owns the broadcast lifecycle -- the piece that today's
"just paste the stream key" flow does not do:

    liveStreams.insert / list  (find or create a persistent stream key)
    liveBroadcasts.insert       (one broadcast per calendar day, HST)
    liveBroadcasts.bind         (bind broadcast <-> stream)
    ffmpeg -> RTMP -> YouTube   (existing pipeline in app/youtube.py)
    liveBroadcasts.transition   (broadcastStatus=live once stream is active)
    ...
    liveBroadcasts.transition   (broadcastStatus=complete at end of day)

Two design choices worth calling out:

1. **OAuth device flow** ("go to google.com/device, enter this code").
   The Pi is headless and accessed from an operator laptop / phone on a
   different network, so a redirect-based flow can't reach a
   ``http://localhost:xxxx`` redirect on the Pi. The device flow needs
   no redirect at all. It only supports the ``youtube`` scope (NOT
   ``youtube.force-ssl``) -- which is fine, that scope is sufficient
   for ``liveBroadcasts`` and ``liveStreams``.

2. **Per-day broadcast, kept alive across slot gaps.** The scheduler
   in ``main.py`` opens/closes multiple 15-min slots per day. We do
   NOT create/complete a broadcast per slot -- that would produce a
   dozen archived videos per day and a different watch URL each time.
   Instead we insert one broadcast for the day, use ``enableAutoStop=
   false`` so ffmpeg stopping between slots doesn't end it (this is
   the exact YouTube setting that made the operator's tests work), and
   only call ``transition(complete)`` on calendar-date rollover or
   when the last remaining slot of the day is over.

State is split across two JSON files under ``/app/data`` so container
restarts (device loses power without clean shutdown -- see the USB-log
rationale in ``main.py``) resume the same broadcast instead of
insert-ing a duplicate:

    /app/data/youtube_oauth.json     refresh token + cached access token
    /app/data/youtube_broadcast.json today's broadcast id / stream id / key

All I/O is plain ``requests`` -- avoiding the ``google-api-python-
client`` transitive tree keeps the arm/v7 image small and the
dependency surface trivial.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

import requests

from scheduler import SCHEDULE_TIMEZONE

logger = logging.getLogger("kaumaui.youtube-api")

# ---------------------------------------------------------------------------
# Endpoints and constants
# ---------------------------------------------------------------------------

DEVICE_CODE_URL = "https://oauth2.googleapis.com/device/code"
TOKEN_URL = "https://oauth2.googleapis.com/token"
YT_API_BASE = "https://www.googleapis.com/youtube/v3"

# The device flow's scope allowlist excludes ``youtube.force-ssl`` -- only
# the broader ``youtube`` scope (or ``youtube.readonly``) is supported.
# ``youtube`` is sufficient for liveBroadcasts.{insert,bind,transition}
# and liveStreams.{list,insert}.
OAUTH_SCOPE = "https://www.googleapis.com/auth/youtube"

# Grant type for the token-poll leg of the device flow.
DEVICE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:device_code"

# HTTP timeouts. Google's OAuth/API edge is fast; anything over ~15s
# almost always means the Pi's uplink is congested (Starlink handoff)
# or we're being throttled. Keep timeouts short so a scheduler tick
# doesn't block for a full minute during an outage.
HTTP_TIMEOUT_SECS = 15.0

# Access tokens are issued for 3600s. Refresh at ~10 minutes remaining
# so a scheduler tick landing near expiry doesn't race the refresh.
ACCESS_TOKEN_REFRESH_MARGIN_SECS = 600.0

# Device code lifetime is typically 15 minutes; we track ``expires_at``
# per authorization attempt but cap the total poll time as a sanity
# guard.
DEVICE_POLL_MAX_SECS = 30 * 60.0

# Persisted state file paths (override via env for tests).
DEFAULT_DATA_DIR = "/app/data"
OAUTH_STATE_PATH = os.environ.get(
    "KAUMAUI_YT_OAUTH_STATE",
    os.path.join(DEFAULT_DATA_DIR, "youtube_oauth.json"),
)
BROADCAST_STATE_PATH = os.environ.get(
    "KAUMAUI_YT_BROADCAST_STATE",
    os.path.join(DEFAULT_DATA_DIR, "youtube_broadcast.json"),
)

# Title / description we tag persistent liveStreams with so we can find
# and reuse the same stream across days rather than accumulating a fresh
# stream key each time (YouTube quotas ``liveStreams.insert`` more
# expensively than ``list``, and stray keys clutter Studio).
PERSISTENT_STREAM_TITLE = "Kaumaui Cam (auto-managed)"


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class YouTubeApiError(Exception):
    """Raised for any non-recoverable YouTube API failure. Callers in
    the scheduler catch this and fall back to legacy behavior for the
    tick; the error is surfaced via ``status()`` for the UI."""

    def __init__(self, message: str, *, needs_reauth: bool = False) -> None:
        super().__init__(message)
        self.needs_reauth = needs_reauth


# ---------------------------------------------------------------------------
# Module state
# ---------------------------------------------------------------------------

_lock = threading.Lock()          # auth / token state
_bcast_lock = threading.Lock()    # today's broadcast state
_pending_lock = threading.Lock()  # device-flow pending state

# OAuth tokens and derived channel info.
_refresh_token: Optional[str] = None
_access_token: Optional[str] = None
_access_expiry: float = 0.0
_channel_title: Optional[str] = None
_needs_reauth: bool = False
_last_auth_error: Optional[str] = None

# Device-flow pending state: populated by ``start_device_auth`` and
# consumed by a background thread that polls ``TOKEN_URL`` until either
# the user approves, denies, or the code expires.
_pending_auth: Optional[Dict[str, Any]] = None
_pending_thread: Optional[threading.Thread] = None

# Today's broadcast state (mirrors ``BROADCAST_STATE_PATH`` for O(1)
# reads from the scheduler tick). ``date`` is the HST calendar date the
# broadcast was created for; on rollover we complete + clear it.
_broadcast: Optional[Dict[str, Any]] = None

# Provider callable to fetch (client_id, client_secret) from config.
# Wired by ``init()``. Kept as a callable rather than baked in so config
# edits are picked up without restarting the module.
_get_client_creds: Optional[Callable[[], Tuple[str, str]]] = None


# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------


def _atomic_write_json(path: str, payload: Dict[str, Any], *, mode: int = 0o600) -> None:
    """Write ``payload`` to ``path`` atomically. The device loses power
    without clean shutdown; a partial ``json.dump`` would corrupt the
    file and force reauth on next boot. tmp+rename is atomic on POSIX
    and safe on the ext4 host bind mount."""
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.flush()
        try:
            os.fsync(f.fileno())
        except OSError:
            pass
    os.replace(tmp, path)
    try:
        os.chmod(path, mode)
    except OSError:
        # Not fatal -- FAT-mounted USB doesn't honor chmod, but we don't
        # actually write these files there.
        pass


def _read_json(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        return None
    except (OSError, ValueError):
        logger.exception("failed to read %s; ignoring", path)
        return None


def _save_oauth_state() -> None:
    """Persist refresh_token + cached access_token + channel title.
    Access tokens are ephemeral but caching them avoids a re-fetch after
    a container restart while the previous access token is still
    valid."""
    with _lock:
        payload = {
            "refresh_token": _refresh_token,
            "access_token": _access_token,
            "access_expiry": _access_expiry,
            "channel_title": _channel_title,
            "needs_reauth": _needs_reauth,
            "last_auth_error": _last_auth_error,
        }
    _atomic_write_json(OAUTH_STATE_PATH, payload)


def _load_oauth_state() -> None:
    global _refresh_token, _access_token, _access_expiry
    global _channel_title, _needs_reauth, _last_auth_error
    data = _read_json(OAUTH_STATE_PATH)
    if not data:
        return
    with _lock:
        _refresh_token = data.get("refresh_token") or None
        _access_token = data.get("access_token") or None
        try:
            _access_expiry = float(data.get("access_expiry") or 0.0)
        except (TypeError, ValueError):
            _access_expiry = 0.0
        _channel_title = data.get("channel_title") or None
        _needs_reauth = bool(data.get("needs_reauth"))
        _last_auth_error = data.get("last_auth_error") or None


def _save_broadcast_state() -> None:
    with _bcast_lock:
        payload = _broadcast.copy() if _broadcast else {}
    _atomic_write_json(BROADCAST_STATE_PATH, payload, mode=0o644)


def _load_broadcast_state() -> None:
    global _broadcast
    data = _read_json(BROADCAST_STATE_PATH)
    if not data or not data.get("broadcast_id"):
        return
    with _bcast_lock:
        _broadcast = data


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


def _hst_today() -> str:
    """Today's calendar date in HST as an ISO string (YYYY-MM-DD).
    Broadcasts roll over at Honolulu midnight, matching the schedule
    editor's TZ so a slot that crosses local midnight (unlikely for
    this fishpond deployment but supported) still gets a fresh
    broadcast at the calendar boundary."""
    return dt.datetime.now(SCHEDULE_TIMEZONE).date().isoformat()


def _iso_now_utc() -> str:
    """Current UTC time in RFC3339 with a trailing ``Z`` -- the format
    ``liveBroadcasts`` requires for ``scheduledStartTime``."""
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------


def init(get_client_creds: Callable[[], Tuple[str, str]]) -> None:
    """Wire the client-credential provider and load persisted state.
    Called once at startup from ``main.py``. Idempotent."""
    global _get_client_creds
    _get_client_creds = get_client_creds
    _load_oauth_state()
    _load_broadcast_state()


def _client_creds() -> Tuple[str, str]:
    if _get_client_creds is None:
        raise YouTubeApiError("youtube_api.init() not called")
    cid, csec = _get_client_creds()
    cid = (cid or "").strip()
    csec = (csec or "").strip()
    if not cid or not csec:
        raise YouTubeApiError(
            "OAuth client ID / secret not configured; see docs/youtube-api-setup.md"
        )
    return cid, csec


# ---------------------------------------------------------------------------
# Device-code OAuth flow
# ---------------------------------------------------------------------------


def start_device_auth() -> Dict[str, Any]:
    """Kick off the OAuth 2.0 device flow. Returns the user-facing
    ``verification_url`` and ``user_code`` that the UI displays to the
    operator, plus polling metadata. A background thread continues
    polling ``TOKEN_URL`` until the user approves, denies, or the code
    expires."""
    global _pending_auth, _pending_thread
    client_id, _ = _client_creds()

    # Cancel any prior pending flow so a second click doesn't spawn a
    # zombie poller. We don't need to actively kill the old thread -- it
    # observes ``_pending_auth`` on every tick and exits when it sees a
    # different device_code.
    with _pending_lock:
        _pending_auth = None

    r = requests.post(
        DEVICE_CODE_URL,
        data={"client_id": client_id, "scope": OAUTH_SCOPE},
        timeout=HTTP_TIMEOUT_SECS,
    )
    if r.status_code != 200:
        raise YouTubeApiError(
            f"device/code failed: HTTP {r.status_code} {r.text[:200]}"
        )
    data = r.json()
    device_code = data.get("device_code") or ""
    user_code = data.get("user_code") or ""
    verification_url = (
        data.get("verification_url")
        or data.get("verification_uri")
        or "https://www.google.com/device"
    )
    interval = float(data.get("interval") or 5)
    expires_in = float(data.get("expires_in") or 900)
    if not device_code or not user_code:
        raise YouTubeApiError("device/code missing device_code/user_code")

    pending = {
        "device_code": device_code,
        "user_code": user_code,
        "verification_url": verification_url,
        "interval": interval,
        "expires_at": time.time() + min(expires_in, DEVICE_POLL_MAX_SECS),
        "started_at": time.time(),
        "error": None,
    }
    with _pending_lock:
        _pending_auth = pending
        _pending_thread = threading.Thread(
            target=_run_device_poll,
            args=(dict(pending),),
            daemon=True,
            name="yt-device-poll",
        )
        _pending_thread.start()

    logger.info(
        "YouTube OAuth device flow started user_code=%s verification_url=%s",
        user_code,
        verification_url,
    )
    return {
        "user_code": user_code,
        "verification_url": verification_url,
        "expires_in": expires_in,
        "interval": interval,
    }


def _run_device_poll(snapshot: Dict[str, Any]) -> None:
    """Background poller: hits ``TOKEN_URL`` on ``interval`` cadence
    until the operator approves the code, denies, or it expires. On
    success persists the refresh token and fetches the channel title so
    the UI can render 'Connected as <channel>' immediately."""
    global _refresh_token, _access_token, _access_expiry
    global _channel_title, _needs_reauth, _last_auth_error, _pending_auth
    try:
        client_id, client_secret = _client_creds()
    except YouTubeApiError as e:
        with _pending_lock:
            if _pending_auth and _pending_auth.get("device_code") == snapshot["device_code"]:
                _pending_auth = dict(_pending_auth, error=str(e))
        return

    interval = float(snapshot.get("interval") or 5)
    device_code = snapshot["device_code"]

    while True:
        # Check we haven't been superseded by a newer start_device_auth.
        with _pending_lock:
            still_current = (
                _pending_auth is not None
                and _pending_auth.get("device_code") == device_code
            )
        if not still_current:
            return
        if time.time() >= snapshot["expires_at"]:
            with _pending_lock:
                if _pending_auth and _pending_auth.get("device_code") == device_code:
                    _pending_auth = dict(_pending_auth, error="expired")
            logger.warning("YouTube OAuth device code expired without approval")
            return

        try:
            r = requests.post(
                TOKEN_URL,
                data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "device_code": device_code,
                    "grant_type": DEVICE_GRANT_TYPE,
                },
                timeout=HTTP_TIMEOUT_SECS,
            )
        except requests.RequestException as e:
            logger.debug("device token poll fetch error: %s", e)
            time.sleep(interval)
            continue

        try:
            body = r.json()
        except ValueError:
            body = {}

        if r.status_code == 200 and body.get("access_token"):
            new_refresh = body.get("refresh_token")
            new_access = body["access_token"]
            expires_in = float(body.get("expires_in") or 3600)
            with _lock:
                if new_refresh:
                    _refresh_token = new_refresh
                _access_token = new_access
                _access_expiry = time.time() + expires_in
                # Clear any stale needs_reauth flag from a previous
                # failed session -- we're freshly authorized now.
                _needs_reauth = False
                _last_auth_error = None
            _save_oauth_state()
            # Best-effort channel title fetch so the UI can show who's
            # connected. Not fatal if it fails.
            try:
                title = _fetch_channel_title()
                with _lock:
                    _channel_title = title
                _save_oauth_state()
            except Exception:
                logger.exception("channel title fetch failed after auth")
            with _pending_lock:
                _pending_auth = None
            logger.info("YouTube OAuth device flow completed successfully")
            return

        err = str(body.get("error") or "")
        if err == "authorization_pending":
            time.sleep(interval)
            continue
        if err == "slow_down":
            # Google asks us to back off; bump the interval by 5s per
            # the spec and keep going.
            interval += 5
            time.sleep(interval)
            continue
        if err in ("access_denied", "expired_token"):
            with _pending_lock:
                if _pending_auth and _pending_auth.get("device_code") == device_code:
                    _pending_auth = dict(_pending_auth, error=err)
            logger.warning("YouTube OAuth device flow ended: %s", err)
            return
        # Anything else -- surface it and stop.
        msg = err or f"HTTP {r.status_code}"
        with _pending_lock:
            if _pending_auth and _pending_auth.get("device_code") == device_code:
                _pending_auth = dict(_pending_auth, error=msg)
        logger.warning("YouTube OAuth device flow failed: %s (%s)", msg, r.text[:200])
        return


def disconnect() -> None:
    """Wipe all persisted OAuth + broadcast state. Called from the UI
    'Disconnect' button or after a hard ``invalid_grant`` where the
    operator will need to re-consent anyway."""
    global _refresh_token, _access_token, _access_expiry
    global _channel_title, _needs_reauth, _last_auth_error
    global _broadcast, _pending_auth
    with _lock:
        _refresh_token = None
        _access_token = None
        _access_expiry = 0.0
        _channel_title = None
        _needs_reauth = False
        _last_auth_error = None
    with _bcast_lock:
        _broadcast = None
    with _pending_lock:
        _pending_auth = None
    for p in (OAUTH_STATE_PATH, BROADCAST_STATE_PATH):
        try:
            if os.path.isfile(p):
                os.remove(p)
        except OSError:
            logger.exception("failed to remove %s", p)


# ---------------------------------------------------------------------------
# Access-token management
# ---------------------------------------------------------------------------


def _access() -> str:
    """Return a valid access token, refreshing from ``_refresh_token``
    if the cached one is close to expiry or absent. Raises
    ``YouTubeApiError(needs_reauth=True)`` if the refresh token is
    missing or Google has revoked it."""
    with _lock:
        tok = _access_token
        exp = _access_expiry
        rt = _refresh_token
        reauth = _needs_reauth
    if reauth:
        raise YouTubeApiError("YouTube reauthorization required", needs_reauth=True)
    if not rt:
        raise YouTubeApiError("YouTube not connected", needs_reauth=True)
    if tok and time.time() < exp - ACCESS_TOKEN_REFRESH_MARGIN_SECS:
        return tok
    return _refresh_access_token()


def _refresh_access_token() -> str:
    """Exchange the persisted refresh token for a fresh access token.
    Serialised via ``_lock`` so concurrent scheduler / API-endpoint
    callers don't stampede."""
    global _access_token, _access_expiry, _needs_reauth, _last_auth_error
    client_id, client_secret = _client_creds()
    with _lock:
        rt = _refresh_token
    if not rt:
        raise YouTubeApiError("YouTube not connected", needs_reauth=True)
    r = requests.post(
        TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": rt,
            "grant_type": "refresh_token",
        },
        timeout=HTTP_TIMEOUT_SECS,
    )
    try:
        body = r.json()
    except ValueError:
        body = {}
    if r.status_code == 200 and body.get("access_token"):
        with _lock:
            _access_token = body["access_token"]
            _access_expiry = time.time() + float(body.get("expires_in") or 3600)
            _needs_reauth = False
            _last_auth_error = None
        _save_oauth_state()
        return _access_token  # type: ignore[return-value]
    err = str(body.get("error") or f"HTTP {r.status_code}")
    # ``invalid_grant`` almost always means the refresh token was
    # revoked (Google project still in Testing so it expired after
    # 7 days, or the operator revoked from myaccount.google.com). The
    # only recovery is a fresh device-flow authorization, so mark the
    # module unavailable rather than retrying every scheduler tick.
    fatal = err in ("invalid_grant", "invalid_client", "unauthorized_client")
    with _lock:
        _last_auth_error = err
        if fatal:
            _needs_reauth = True
    _save_oauth_state()
    logger.warning(
        "YouTube token refresh failed: %s (%s)%s",
        err,
        r.text[:200],
        " -- reauthorization required" if fatal else "",
    )
    raise YouTubeApiError(f"token refresh failed: {err}", needs_reauth=fatal)


# ---------------------------------------------------------------------------
# YouTube Data API v3 plumbing
# ---------------------------------------------------------------------------


def _api_request(
    method: str,
    path: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Single-shot API call with one automatic 401-refresh retry.

    Returns the parsed JSON body on 2xx. Raises ``YouTubeApiError`` on
    anything else, with ``needs_reauth`` set on 401 after the retry
    also fails (Google occasionally returns 401 with a fresh but
    revoked token if consent was revoked while the token was live)."""
    url = f"{YT_API_BASE}/{path.lstrip('/')}"

    def _do(access: str) -> requests.Response:
        headers = {"Authorization": f"Bearer {access}"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        return requests.request(
            method,
            url,
            params=params,
            headers=headers,
            data=json.dumps(body) if body is not None else None,
            timeout=HTTP_TIMEOUT_SECS,
        )

    tok = _access()
    r = _do(tok)
    if r.status_code == 401:
        # Force a refresh and try once more.
        global _access_expiry
        with _lock:
            _access_expiry = 0.0
        try:
            tok2 = _refresh_access_token()
        except YouTubeApiError:
            raise
        r = _do(tok2)

    try:
        body_out = r.json() if r.content else {}
    except ValueError:
        body_out = {}

    if 200 <= r.status_code < 300:
        return body_out if isinstance(body_out, dict) else {}

    err_msg = ""
    if isinstance(body_out, dict):
        err_msg = (
            (body_out.get("error") or {}).get("message")
            if isinstance(body_out.get("error"), dict)
            else str(body_out.get("error"))
        )
    err_msg = err_msg or r.text[:300]
    if r.status_code == 401:
        raise YouTubeApiError(f"401 unauthorised: {err_msg}", needs_reauth=True)
    raise YouTubeApiError(f"{method} {path} -> HTTP {r.status_code}: {err_msg}")


def _redundant_transition(exc: YouTubeApiError) -> bool:
    """YouTube returns 403 ``redundantTransition`` when we ask it to
    transition a broadcast into a state it's already in (e.g. we set
    ``enableAutoStart=true`` and YouTube already promoted the broadcast
    to ``live`` on its own by the time we call ``transition(live)``).
    That's a success, not a failure."""
    return "redundantTransition" in str(exc) or "redundant" in str(exc).lower()


# ---------------------------------------------------------------------------
# High-level API operations
# ---------------------------------------------------------------------------


def _fetch_channel_title() -> Optional[str]:
    data = _api_request("GET", "channels", params={"part": "snippet", "mine": "true"})
    items = data.get("items") or []
    if not items:
        return None
    snip = items[0].get("snippet") or {}
    return snip.get("title") or None


def ensure_reusable_stream() -> Dict[str, Any]:
    """Return a persistent ``liveStream`` we can reuse across days.

    Reusing one stream instead of inserting a fresh one per day keeps
    the same RTMP ingest key on the box (no config changes on ffmpeg
    restart), and avoids piling up stray stream entries in YouTube
    Studio. We identify our stream by title -- YouTube doesn't return
    ``description`` in ``mine=true`` listings reliably. If nothing
    matches, we ``insert`` a new one."""
    listing = _api_request(
        "GET",
        "liveStreams",
        params={"part": "id,cdn,snippet,status", "mine": "true", "maxResults": 50},
    )
    for item in listing.get("items") or []:
        snip = item.get("snippet") or {}
        if snip.get("title") == PERSISTENT_STREAM_TITLE:
            cdn = item.get("cdn") or {}
            ing = cdn.get("ingestionInfo") or {}
            return {
                "stream_id": item.get("id"),
                "stream_key": ing.get("streamName"),
                "ingestion_address": ing.get("ingestionAddress"),
            }

    logger.info("Creating new persistent liveStream %r", PERSISTENT_STREAM_TITLE)
    created = _api_request(
        "POST",
        "liveStreams",
        params={"part": "id,cdn,snippet,status"},
        body={
            "snippet": {
                "title": PERSISTENT_STREAM_TITLE,
                "description": (
                    "Auto-managed by Kaumaui Cam. Do not delete while the "
                    "extension is running -- it will be reused day-to-day."
                ),
            },
            "cdn": {
                "frameRate": "variable",
                "ingestionType": "rtmp",
                "resolution": "variable",
            },
        },
    )
    cdn = created.get("cdn") or {}
    ing = cdn.get("ingestionInfo") or {}
    return {
        "stream_id": created.get("id"),
        "stream_key": ing.get("streamName"),
        "ingestion_address": ing.get("ingestionAddress"),
    }


def _find_existing_broadcast_today(hst_date: str, title: str) -> Optional[Dict[str, Any]]:
    """Look up an existing not-yet-completed broadcast for today. Used
    on startup when we've lost state (fresh container, empty broadcast
    state file) but a previous container already created one earlier
    today. We match on title -- YouTube's ``mine=true`` listing
    returns all lifecycle states, so we filter to non-terminal ones."""
    data = _api_request(
        "GET",
        "liveBroadcasts",
        params={
            "part": "id,snippet,status,contentDetails",
            "broadcastStatus": "upcoming",
            "mine": "true",
            "maxResults": 25,
        },
    )
    for item in data.get("items") or []:
        snip = item.get("snippet") or {}
        if snip.get("title") == title:
            return item
    data = _api_request(
        "GET",
        "liveBroadcasts",
        params={
            "part": "id,snippet,status,contentDetails",
            "broadcastStatus": "active",
            "mine": "true",
            "maxResults": 25,
        },
    )
    for item in data.get("items") or []:
        snip = item.get("snippet") or {}
        if snip.get("title") == title:
            return item
    return None


def _insert_broadcast(title: str, privacy: str) -> Dict[str, Any]:
    return _api_request(
        "POST",
        "liveBroadcasts",
        params={"part": "id,snippet,status,contentDetails"},
        body={
            "snippet": {
                "title": title,
                "scheduledStartTime": _iso_now_utc(),
                "description": (
                    "Auto-created by Kaumaui Cam. Managed lifecycle: "
                    "start/stop is driven by the extension's schedule."
                ),
            },
            "status": {
                "privacyStatus": privacy,
                "selfDeclaredMadeForKids": False,
            },
            "contentDetails": {
                # enableAutoStart is belt-and-suspenders: our supervisor
                # explicitly transitions to ``live`` once the stream is
                # active, but if the API call fails transiently
                # (Starlink hiccup during the tick), auto-start still
                # gets us live. Redundant transitions are handled.
                "enableAutoStart": True,
                # enableAutoStop=false is the key setting that made the
                # operator's tests work across multiple ffmpeg sessions
                # in a day -- YouTube won't end the broadcast every
                # time bytes stop flowing between schedule slots.
                "enableAutoStop": False,
                "enableDvr": True,
                "enableContentEncryption": False,
                "enableEmbed": True,
                "recordFromStart": True,
                "startWithSlate": False,
            },
        },
    )


def _bind_broadcast(broadcast_id: str, stream_id: str) -> Dict[str, Any]:
    return _api_request(
        "POST",
        "liveBroadcasts/bind",
        params={
            "id": broadcast_id,
            "streamId": stream_id,
            "part": "id,contentDetails,status",
        },
    )


def _transition(broadcast_id: str, status: str) -> Dict[str, Any]:
    return _api_request(
        "POST",
        "liveBroadcasts/transition",
        params={
            "id": broadcast_id,
            "broadcastStatus": status,
            "part": "id,status",
        },
    )


def _get_broadcast(broadcast_id: str) -> Dict[str, Any]:
    data = _api_request(
        "GET",
        "liveBroadcasts",
        params={"id": broadcast_id, "part": "id,status,contentDetails,snippet"},
    )
    items = data.get("items") or []
    return items[0] if items else {}


def _get_stream(stream_id: str) -> Dict[str, Any]:
    data = _api_request(
        "GET",
        "liveStreams",
        params={"id": stream_id, "part": "id,status,cdn,snippet"},
    )
    items = data.get("items") or []
    return items[0] if items else {}


# ---------------------------------------------------------------------------
# Daily broadcast state machine
# ---------------------------------------------------------------------------


def _format_title(template: str, hst_date: str) -> str:
    try:
        return template.format(date=hst_date)
    except (KeyError, IndexError, ValueError):
        return f"Kaumaui Cam - {hst_date}"


def _watch_url(broadcast_id: str) -> str:
    return f"https://www.youtube.com/watch?v={broadcast_id}"


def ensure_todays_broadcast(
    title_template: str = "Kaumaui Cam - {date}",
    privacy: str = "public",
) -> Optional[Dict[str, Any]]:
    """Idempotently ensure a broadcast exists for today's HST date and
    is bound to our reusable stream. Returns a dict with ``stream_key``
    (for ffmpeg) and ``broadcast_id`` / ``watch_url`` (for the UI). The
    caller must handle ``None`` -- meaning we can't set up API mode
    right now -- by falling back to legacy behavior for this tick.
    """
    global _broadcast
    hst = _hst_today()
    with _bcast_lock:
        current = _broadcast

    # If we have state for a previous day, close it out and let the
    # next scheduler tick fall through to the fresh-broadcast path
    # below. Guarding on the calendar date means a container restart
    # after midnight cleanly rolls over.
    if current and current.get("date") and current["date"] != hst:
        try:
            _transition(current["broadcast_id"], "complete")
        except YouTubeApiError as e:
            if not _redundant_transition(e):
                logger.warning(
                    "Auto-complete of yesterday's broadcast failed: %s (continuing)", e
                )
        with _bcast_lock:
            _broadcast = None
        current = None
        _save_broadcast_state()

    if current and current.get("broadcast_id"):
        return dict(current)

    title = _format_title(title_template, hst)
    # Recovery path: fresh container that lost broadcast state, but a
    # broadcast for today already exists on YouTube from a previous
    # container. Reuse it instead of creating a duplicate.
    existing = None
    try:
        existing = _find_existing_broadcast_today(hst, title)
    except YouTubeApiError as e:
        logger.warning("existing-broadcast lookup failed: %s", e)

    stream = ensure_reusable_stream()
    if not stream.get("stream_key") or not stream.get("stream_id"):
        raise YouTubeApiError("liveStreams did not return a stream_key/stream_id")

    if existing:
        broadcast_id = existing.get("id") or ""
        logger.info(
            "Adopting existing broadcast %s for HST date %s", broadcast_id, hst
        )
    else:
        created = _insert_broadcast(title, privacy)
        broadcast_id = created.get("id") or ""
        logger.info("Created broadcast %s (%s) for HST date %s", broadcast_id, title, hst)

    if not broadcast_id:
        raise YouTubeApiError("broadcast insert returned no id")

    try:
        _bind_broadcast(broadcast_id, stream["stream_id"])
    except YouTubeApiError as e:
        # A rebind on an already-bound broadcast is fine; some other
        # binding error is not.
        if "already bound" not in str(e).lower():
            raise

    state = {
        "date": hst,
        "broadcast_id": broadcast_id,
        "stream_id": stream["stream_id"],
        "stream_key": stream["stream_key"],
        "ingestion_address": stream.get("ingestion_address"),
        "title": title,
        "privacy": privacy,
        "is_live": False,
        "watch_url": _watch_url(broadcast_id),
        "created_ts": time.time(),
        "last_error": None,
    }
    with _bcast_lock:
        _broadcast = state
    _save_broadcast_state()
    return dict(state)


def drive_live() -> Optional[str]:
    """Called on every scheduler tick while ffmpeg is running. If the
    broadcast isn't yet live and the bound stream is receiving data,
    transition it to ``live``. Returns the current ``lifeCycleStatus``
    (``ready``, ``testing``, ``live``, ...) for logging/UI, or None if
    no broadcast state exists.

    Idempotent -- if the broadcast is already live (via ``enableAuto
    Start`` or a prior tick), we do nothing beyond noting it. Errors
    are swallowed and recorded on the state dict; the next tick will
    retry."""
    global _broadcast
    with _bcast_lock:
        current = _broadcast.copy() if _broadcast else None
    if not current or not current.get("broadcast_id"):
        return None
    bid = current["broadcast_id"]
    sid = current["stream_id"]
    try:
        bcast = _get_broadcast(bid)
        stream = _get_stream(sid)
    except YouTubeApiError as e:
        _mark_broadcast_error(str(e))
        return None
    lifecycle = ((bcast.get("status") or {}).get("lifeCycleStatus") or "").lower()
    stream_status = ((stream.get("status") or {}).get("streamStatus") or "").lower()
    stream_health = ((stream.get("status") or {}).get("healthStatus") or {}).get("status") or ""

    if lifecycle in ("live",):
        _update_broadcast_fields(
            is_live=True,
            lifecycle=lifecycle,
            stream_status=stream_status,
            stream_health=stream_health,
            last_error=None,
        )
        return lifecycle
    # We can only transition to ``live`` from ``testing`` (or ``ready``
    # on some auto-start-off broadcasts). YouTube only moves to
    # ``testing`` once RTMP data is actually flowing, so we wait until
    # ``streamStatus == active`` before pushing.
    if stream_status != "active":
        _update_broadcast_fields(
            lifecycle=lifecycle,
            stream_status=stream_status,
            stream_health=stream_health,
        )
        return lifecycle
    if lifecycle in ("testing", "ready", "testStarting", "liveStarting"):
        try:
            _transition(bid, "live")
            logger.info(
                "Transitioned broadcast %s to live (was %s, stream=%s health=%s)",
                bid,
                lifecycle,
                stream_status,
                stream_health,
            )
            _update_broadcast_fields(
                is_live=True,
                lifecycle="live",
                stream_status=stream_status,
                stream_health=stream_health,
                last_error=None,
            )
            return "live"
        except YouTubeApiError as e:
            if _redundant_transition(e):
                _update_broadcast_fields(
                    is_live=True,
                    lifecycle="live",
                    stream_status=stream_status,
                    stream_health=stream_health,
                    last_error=None,
                )
                return "live"
            _mark_broadcast_error(str(e))
            return lifecycle
    _update_broadcast_fields(
        lifecycle=lifecycle,
        stream_status=stream_status,
        stream_health=stream_health,
    )
    return lifecycle


def complete_today() -> None:
    """Transition today's broadcast to ``complete`` and clear state.
    Called at end-of-day (no more slots today) or from the manual UI
    stop path when the operator wants to close the archive."""
    global _broadcast
    with _bcast_lock:
        current = _broadcast.copy() if _broadcast else None
    if not current or not current.get("broadcast_id"):
        return
    bid = current["broadcast_id"]
    try:
        _transition(bid, "complete")
        logger.info("Completed broadcast %s", bid)
    except YouTubeApiError as e:
        if _redundant_transition(e):
            logger.info("Broadcast %s already complete", bid)
        else:
            # Log but still clear state -- next day will insert fresh.
            logger.warning("complete transition failed for %s: %s", bid, e)
    with _bcast_lock:
        _broadcast = None
    _save_broadcast_state()


def _mark_broadcast_error(msg: str) -> None:
    global _broadcast
    with _bcast_lock:
        if _broadcast is not None:
            _broadcast = dict(_broadcast, last_error=msg)
    _save_broadcast_state()


def _update_broadcast_fields(**kwargs: Any) -> None:
    global _broadcast
    with _bcast_lock:
        if _broadcast is not None:
            _broadcast = dict(_broadcast, **kwargs)
    _save_broadcast_state()


# ---------------------------------------------------------------------------
# Public accessors for main.py / UI
# ---------------------------------------------------------------------------


def is_connected() -> bool:
    """True when we have a persisted refresh token and haven't been
    revoked. This is the switch the scheduler consults to choose
    between API mode and legacy pasted-key mode."""
    with _lock:
        return bool(_refresh_token) and not _needs_reauth


def needs_reauth() -> bool:
    with _lock:
        return _needs_reauth


def managed_stream_key() -> Optional[str]:
    """Today's stream key if we've set up API mode, else None. Called
    from the scheduler to feed ``YouTubeStreamer.start()``."""
    with _bcast_lock:
        return _broadcast.get("stream_key") if _broadcast else None


def status() -> Dict[str, Any]:
    """OAuth + pending-flow state for the UI to render the Connect
    button / device-code modal / 'Connected as X' badge."""
    with _lock:
        connected = bool(_refresh_token) and not _needs_reauth
        info = {
            "connected": connected,
            "needs_reauth": bool(_needs_reauth),
            "channel_title": _channel_title,
            "last_auth_error": _last_auth_error,
        }
    with _pending_lock:
        info["pending"] = dict(_pending_auth) if _pending_auth else None
    return info


def broadcast_status() -> Dict[str, Any]:
    """Today's broadcast state for the UI (watch URL, lifecycle status,
    stream health, is_live flag)."""
    with _bcast_lock:
        current = dict(_broadcast) if _broadcast else None
    return {"broadcast": current, "hst_today": _hst_today()}
