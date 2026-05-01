"""SQLite-backed YouTube broadcast health monitor.

The on-Pi YouTubeStreamer sees only its own ffmpeg subprocess: bytes
delivered, RTMP socket state, broken pipes, byte stalls. None of those
prove the broadcast is actually live to viewers -- the canonical failure
mode this module exists to catch is YouTube's "Preparing stream" state,
where the RTMP ingest accepts our bytes for an indefinite time but the
broadcast never gets promoted to live (no picture, no preview, no
viewers). We've seen sessions stay in that state for 30+ minutes while
ffmpeg reports `running:true` and a healthy byte counter.

To catch that, we poll the channel's public ``/live`` page on a
configurable cadence and parse YouTube's ``isLiveNow`` flag plus the
canonical video URL out of the served HTML. Two important properties
of this approach:

  - It works for any visibility setting whose live broadcast appears on
    the channel ``/live`` page, i.e. **public** broadcasts. Unlisted and
    private broadcasts will not show up here -- the operator has to set
    their YouTube live default to Public for this to function. (A future
    extension could add the Data API for unlisted/private support and
    richer signals like ``healthStatus`` and ``lifeCycleStatus``.)

  - It uses no API key and no OAuth. The polling cost is one HTTP GET
    per ``POLL_INTERVAL_SECS`` while a stream is supposed to be running,
    and zero GETs while idle, so it doesn't burn quota when we're not
    actually broadcasting.

Polling state is persisted to ``yt_health`` so the UI can show
"YouTube confirmed live at HH:MM" alongside the encoder-side bytes
counter, and ``unhealthy_for_secs()`` exposes the current confirmed-bad
duration so the supervisor watchdog in main.py can force-restart ffmpeg
when YouTube has been showing no picture for too long.
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger("kaumaui.yt-monitor")

DB_PATH = os.environ.get("KAUMAUI_STATE_DB", "/app/data/state.db")

# Polling cadence while a stream is supposed to be running. 30s gives
# us 2 polls per minute, which matters most during the 6-minute kickoff
# window (see _scheduler_loop in main.py) where the operator wants to
# know quickly if YouTube actively terminated a freshly-going-live
# broadcast. One /live fetch is ~250 KB after gzip (~1.1 MB decoded),
# so 120 polls/h ≈ 30 MB/h on the wire -- still negligible vs the
# 4.5 Mbps RTMP push itself. We'd love to stop reading early once
# isLiveNow is seen, but the flag lives in the player-response JSON
# ~60% of the way through the body so streaming early-termination
# doesn't actually save much uncompressed work and adds a fragility
# surface (the offset varies per video). Read the full body and just
# parse it.
POLL_INTERVAL_SECS = 30.0
# A YouTube broadcast typically takes 20-60s after RTMP starts before
# the channel /live page reflects it. Anything below this threshold for
# session age is treated as "still spinning up, no opinion" rather than
# "confirmed not live" so we don't auto-restart against ourselves
# during a normal cold start.
DEFAULT_SPINUP_GRACE_SECS = 60.0
# Per-poll HTTP timeout. The /live page is served over HTTP/2 from
# Google's edge so 95th percentile latency is single-digit seconds even
# on Starlink; 15s is the cliff after which we count the poll as
# errored rather than letting it block the polling thread arbitrarily.
HTTP_TIMEOUT_SECS = 15.0
RETENTION_DAYS = 7

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# State labels written to yt_health.state and surfaced via latest()/recent().
STATE_LIVE = "live"           # YouTube confirms isLiveNow=true on the channel /live page
STATE_NOT_LIVE = "not_live"   # /live page reachable, but not currently broadcasting
STATE_DISABLED = "disabled"   # no channel URL configured -- monitor is parked
STATE_ERROR = "error"         # network / parse failure; treat as "unknown" not "bad"

_lock = threading.Lock()
_thread: Optional[threading.Thread] = None
_thread_started = False
_wakeup = threading.Event()

_state_lock = threading.Lock()
_last_poll_ts: Optional[float] = None
_last_live_ts: Optional[float] = None
_last_state: Optional[str] = None
_last_video_id: Optional[str] = None
_last_video_url: Optional[str] = None
_last_title: Optional[str] = None
_last_viewers: Optional[int] = None
_last_error: Optional[str] = None
_unhealthy_since_ts: Optional[float] = None
_consecutive_unhealthy_polls = 0

_get_channel_url: Optional[Callable[[], str]] = None
_get_streamer_running: Optional[Callable[[], bool]] = None
_get_session_age_secs: Optional[Callable[[], float]] = None


def _conn() -> sqlite3.Connection:
    d = os.path.dirname(DB_PATH)
    if d:
        os.makedirs(d, exist_ok=True)
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    with _lock:
        c = _conn()
        try:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS yt_health (
                    ts REAL NOT NULL,
                    state TEXT NOT NULL,
                    video_id TEXT,
                    video_url TEXT,
                    title TEXT,
                    viewers INTEGER,
                    channel_url TEXT,
                    error TEXT
                )
                """
            )
            c.execute("CREATE INDEX IF NOT EXISTS yt_health_ts_idx ON yt_health(ts)")
            cutoff = time.time() - RETENTION_DAYS * 24 * 3600
            c.execute("DELETE FROM yt_health WHERE ts < ?", (cutoff,))
            c.commit()
        finally:
            c.close()


# Channel-URL normaliser. We accept any of:
#   https://www.youtube.com/@handle
#   https://www.youtube.com/@handle/streams
#   https://www.youtube.com/@handle/live
#   https://www.youtube.com/@handle/videos
#   https://www.youtube.com/channel/UCxxxx
#   https://www.youtube.com/c/legacy
#   https://www.youtube.com/user/legacy
#   bare @handle
# and reduce them to a single ``/<base>/live`` URL we can poll. Anything
# else -- a /watch?v=... URL, a youtu.be short link, garbage -- returns
# None so we record an error rather than fetching something useless.
_CHANNEL_PATH_RE = re.compile(
    r"^(?:https?://)?(?:www\.)?youtube\.com/"
    r"(@[^/?#]+|channel/[^/?#]+|user/[^/?#]+|c/[^/?#]+)/?",
    re.IGNORECASE,
)
_BARE_HANDLE_RE = re.compile(r"^@([\w\.\-]+)$")


def normalize_channel_url(raw: str) -> Optional[str]:
    """Convert any of the accepted channel-URL forms (see module docstring)
    to its canonical ``/live`` poll URL, or return None if unrecognised."""
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None
    m = _CHANNEL_PATH_RE.match(s)
    if m:
        return f"https://www.youtube.com/{m.group(1)}/live"
    m2 = _BARE_HANDLE_RE.match(s)
    if m2:
        return f"https://www.youtube.com/@{m2.group(1)}/live"
    return None


# Compiled signal regexes. We deliberately match against the raw HTML
# rather than json-parsing ``ytInitialPlayerResponse`` -- the JSON blob's
# closing brace is preceded by other matched braces and parsing it
# robustly requires a bracket counter, while a string match for the
# specific signals we care about is bounded and tolerant of YouTube's
# frequent (but small) page-template changes.
_RE_IS_LIVE_NOW = re.compile(rb'"isLiveNow"\s*:\s*(true|false)')
_RE_CANONICAL = re.compile(
    rb'<link\s+rel="canonical"\s+href="(https://www\.youtube\.com/watch\?v=([\w\-]+))"',
    re.IGNORECASE,
)
_RE_CANONICAL_CHANNEL = re.compile(
    rb'<link\s+rel="canonical"\s+href="(https://www\.youtube\.com/(?:channel|@)[^"]+)"',
    re.IGNORECASE,
)
_RE_TITLE_META = re.compile(
    rb'<meta\s+name="title"\s+content="([^"]{1,200})"', re.IGNORECASE
)
_RE_CONCURRENT_VIEWERS = re.compile(rb'"concurrentViewers"\s*:\s*"(\d+)"')


def _fetch_live_page(channel_live_url: str) -> Tuple[bytes, int]:
    """GET the channel /live page with gzip support and return
    ``(body, http_status)``. Empty body + status 0 means the request
    itself failed (network/timeout). Body is the decompressed HTML
    bytes; ``requests`` handles the gzip transparently."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip",
    }
    try:
        r = requests.get(
            channel_live_url,
            headers=headers,
            timeout=HTTP_TIMEOUT_SECS,
            allow_redirects=True,
        )
        return r.content or b"", int(r.status_code)
    except requests.RequestException as e:
        logger.debug("yt-monitor fetch %s: %s", channel_live_url, e)
        return b"", 0


def _parse_live_signals(body: bytes) -> Dict[str, Any]:
    """Pull the live-state signals out of a /live response body. Always
    returns a dict with keys ``state``/``video_id``/``video_url``/
    ``title``/``viewers`` -- anything we couldn't extract is None."""
    out: Dict[str, Any] = {
        "state": STATE_NOT_LIVE,
        "video_id": None,
        "video_url": None,
        "title": None,
        "viewers": None,
    }
    if not body:
        return out
    m_live = _RE_IS_LIVE_NOW.search(body)
    is_live_now = m_live is not None and m_live.group(1) == b"true"
    m_can = _RE_CANONICAL.search(body)
    if m_can:
        out["video_url"] = m_can.group(1).decode("ascii", errors="replace")
        out["video_id"] = m_can.group(2).decode("ascii", errors="replace")
    m_title = _RE_TITLE_META.search(body)
    if m_title:
        try:
            out["title"] = m_title.group(1).decode("utf-8", errors="replace")
        except Exception:
            out["title"] = None
    m_view = _RE_CONCURRENT_VIEWERS.search(body)
    if m_view:
        try:
            out["viewers"] = int(m_view.group(1))
        except (TypeError, ValueError):
            out["viewers"] = None
    # We require BOTH the isLiveNow flag AND a canonical /watch URL --
    # the channel page when not live can sometimes still echo isLiveNow
    # in unrelated player blocks (e.g. a featured "live" recommendation),
    # but the canonical only resolves to /watch?v=... when YouTube is
    # actually serving the live broadcast for that channel.
    if is_live_now and out["video_id"]:
        out["state"] = STATE_LIVE
    return out


def _record(
    ts: float,
    state: str,
    video_id: Optional[str],
    video_url: Optional[str],
    title: Optional[str],
    viewers: Optional[int],
    channel_url: Optional[str],
    error: Optional[str],
) -> None:
    with _lock:
        c = _conn()
        try:
            c.execute(
                "INSERT INTO yt_health (ts, state, video_id, video_url, title, viewers, channel_url, error) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (ts, state, video_id, video_url, title, viewers, channel_url, error),
            )
            c.commit()
        finally:
            c.close()


def _prune(now: float) -> None:
    cutoff = now - RETENTION_DAYS * 24 * 3600
    with _lock:
        c = _conn()
        try:
            c.execute("DELETE FROM yt_health WHERE ts < ?", (cutoff,))
            c.commit()
        finally:
            c.close()


def _update_state(
    ts: float,
    state: str,
    parsed: Dict[str, Any],
    error: Optional[str],
    streamer_running: bool,
    session_age: float,
) -> None:
    """Update the in-memory monitor state from a poll result. The
    ``_unhealthy_since_ts`` book-keeping is intentionally split out from
    the watchdog logic in main.py so that the supervisor can read a
    single scalar without reasoning about session-age windows."""
    global _last_poll_ts, _last_live_ts, _last_state
    global _last_video_id, _last_video_url, _last_title, _last_viewers
    global _last_error, _unhealthy_since_ts, _consecutive_unhealthy_polls
    with _state_lock:
        _last_poll_ts = ts
        _last_state = state
        _last_video_id = parsed.get("video_id")
        _last_video_url = parsed.get("video_url")
        _last_title = parsed.get("title")
        _last_viewers = parsed.get("viewers")
        _last_error = error
        if state == STATE_LIVE:
            _last_live_ts = ts
            _unhealthy_since_ts = None
            _consecutive_unhealthy_polls = 0
            return
        if not streamer_running or session_age < DEFAULT_SPINUP_GRACE_SECS:
            # Either we're not streaming (so "not live" is the correct
            # answer and not a fault), or the encoder just started and
            # YouTube hasn't had time to register the broadcast yet.
            # Don't accumulate an unhealthy window in either case.
            _unhealthy_since_ts = None
            _consecutive_unhealthy_polls = 0
            return
        if state == STATE_NOT_LIVE:
            if _unhealthy_since_ts is None:
                _unhealthy_since_ts = ts
            _consecutive_unhealthy_polls += 1
        elif state == STATE_ERROR:
            # Network/parse errors are NOT proof that YouTube isn't live
            # -- our Pi might just be unreachable from the public internet
            # mid-Starlink-handoff. Don't reset the unhealthy clock either
            # way: if we had a confirmed not-live before this and the
            # error is transient, the next successful poll will pick up
            # where we left off; if we didn't, we won't manufacture one.
            _consecutive_unhealthy_polls += 1


def _poll_once(channel_url: str, streamer_running: bool, session_age: float) -> Dict[str, Any]:
    live_url = normalize_channel_url(channel_url)
    ts = time.time()
    if not live_url:
        _update_state(
            ts,
            STATE_ERROR,
            {"video_id": None, "video_url": None, "title": None, "viewers": None},
            "Channel URL not recognised",
            streamer_running,
            session_age,
        )
        try:
            _record(ts, STATE_ERROR, None, None, None, None, channel_url, "unrecognised channel URL")
        except Exception:
            logger.exception("yt-monitor record failed")
        return {"state": STATE_ERROR, "error": "unrecognised channel URL"}
    body, http = _fetch_live_page(live_url)
    if http == 0:
        _update_state(
            ts,
            STATE_ERROR,
            {"video_id": None, "video_url": None, "title": None, "viewers": None},
            "fetch failed",
            streamer_running,
            session_age,
        )
        try:
            _record(ts, STATE_ERROR, None, None, None, None, channel_url, "fetch failed")
        except Exception:
            logger.exception("yt-monitor record failed")
        return {"state": STATE_ERROR, "error": "fetch failed"}
    if http >= 500:
        _update_state(
            ts,
            STATE_ERROR,
            {"video_id": None, "video_url": None, "title": None, "viewers": None},
            f"HTTP {http}",
            streamer_running,
            session_age,
        )
        try:
            _record(ts, STATE_ERROR, None, None, None, None, channel_url, f"HTTP {http}")
        except Exception:
            logger.exception("yt-monitor record failed")
        return {"state": STATE_ERROR, "error": f"HTTP {http}"}
    parsed = _parse_live_signals(body)
    state = parsed["state"]
    error = None if state in (STATE_LIVE, STATE_NOT_LIVE) else f"unexpected state {state}"
    _update_state(ts, state, parsed, error, streamer_running, session_age)
    try:
        _record(
            ts,
            state,
            parsed["video_id"],
            parsed["video_url"],
            parsed["title"],
            parsed["viewers"],
            channel_url,
            None,
        )
    except Exception:
        logger.exception("yt-monitor record failed")
    return {
        "state": state,
        "video_id": parsed["video_id"],
        "video_url": parsed["video_url"],
        "title": parsed["title"],
        "viewers": parsed["viewers"],
    }


def _set_disabled() -> None:
    """Park the monitor with state=disabled. We still write a row so
    history->latest queries reflect the current configuration, but we
    don't accumulate unhealthy time and we skip the HTTP fetch."""
    global _last_state, _last_poll_ts, _last_error
    global _unhealthy_since_ts, _consecutive_unhealthy_polls
    ts = time.time()
    with _state_lock:
        if _last_state == STATE_DISABLED:
            return
        _last_state = STATE_DISABLED
        _last_poll_ts = ts
        _last_error = None
        _unhealthy_since_ts = None
        _consecutive_unhealthy_polls = 0
    try:
        _record(ts, STATE_DISABLED, None, None, None, None, None, None)
    except Exception:
        logger.exception("yt-monitor record (disabled) failed")


def _poll_loop() -> None:
    last_prune = 0.0
    while True:
        try:
            channel_url = _get_channel_url() if _get_channel_url else ""
            running = bool(_get_streamer_running()) if _get_streamer_running else False
            session_age = float(_get_session_age_secs()) if _get_session_age_secs else 0.0
        except Exception:
            logger.exception("yt-monitor provider read failed")
            channel_url, running, session_age = "", False, 0.0
        normalised = normalize_channel_url(channel_url)
        # Polling cadence policy: if no channel URL configured, park as
        # "disabled" and just sleep -- nothing useful to do. If a URL is
        # configured but ffmpeg isn't running, we still skip the fetch
        # because there's no broadcast to confirm; we just record idle
        # state. Only when both are set do we actually hit YouTube.
        try:
            if not normalised:
                _set_disabled()
            elif not running:
                _update_state(
                    time.time(),
                    STATE_NOT_LIVE,
                    {"video_id": None, "video_url": None, "title": None, "viewers": None},
                    None,
                    False,
                    session_age,
                )
            else:
                try:
                    _poll_once(channel_url, running, session_age)
                except Exception:
                    logger.exception("yt-monitor poll raised")
        except Exception:
            logger.exception("yt-monitor loop body raised")
        now = time.time()
        if now - last_prune > 3600:
            try:
                _prune(now)
            except Exception:
                logger.exception("yt-monitor prune failed")
            last_prune = now
        # Wakeable sleep: a manual /api/stream/youtube_health/poll call
        # or a config-change handler can call ``poke()`` to break us out
        # immediately rather than waiting up to a full POLL_INTERVAL.
        _wakeup.wait(POLL_INTERVAL_SECS)
        _wakeup.clear()


def start(
    get_channel_url: Callable[[], str],
    get_streamer_running: Callable[[], bool],
    get_session_age_secs: Callable[[], float],
) -> None:
    """Idempotent start of the background polling thread. The three
    providers are read on every loop iteration so config edits and
    streamer state changes are picked up without needing to restart
    the monitor."""
    global _thread, _thread_started
    global _get_channel_url, _get_streamer_running, _get_session_age_secs
    _get_channel_url = get_channel_url
    _get_streamer_running = get_streamer_running
    _get_session_age_secs = get_session_age_secs
    if _thread_started:
        return
    _thread_started = True
    _thread = threading.Thread(target=_poll_loop, daemon=True, name="yt-monitor")
    _thread.start()


def poke() -> None:
    """Wake the polling thread immediately (e.g. from a settings save
    handler so the operator sees a fresh result without waiting up to
    POLL_INTERVAL_SECS)."""
    _wakeup.set()


def latest() -> Dict[str, Any]:
    """Most recent observation, suitable for surfacing alongside the
    encoder-side stream status."""
    with _state_lock:
        ts = _last_poll_ts
        last_live = _last_live_ts
        state = _last_state
        vid = _last_video_id
        vurl = _last_video_url
        title = _last_title
        viewers = _last_viewers
        err = _last_error
        unhealthy_since = _unhealthy_since_ts
        unhealthy_polls = _consecutive_unhealthy_polls
    now = time.time()
    return {
        "now": now,
        "last_poll_ts": ts,
        "last_live_ts": last_live,
        "state": state,
        "video_id": vid,
        "video_url": vurl,
        "title": title,
        "viewers": viewers,
        "error": err,
        "unhealthy_since_ts": unhealthy_since,
        "unhealthy_for_secs": (now - unhealthy_since) if unhealthy_since else 0.0,
        "consecutive_unhealthy_polls": unhealthy_polls,
        "poll_interval_secs": POLL_INTERVAL_SECS,
        "spinup_grace_secs": DEFAULT_SPINUP_GRACE_SECS,
    }


def unhealthy_for_secs() -> float:
    """How long YouTube has been confirmed not-live while we expected it
    to be live. Returns 0 when healthy or when we have no opinion (idle,
    spinup grace window, no channel configured). The supervisor watchdog
    in main.py uses this scalar directly to decide when to force-restart
    a wedged ffmpeg session."""
    with _state_lock:
        since = _unhealthy_since_ts
    if since is None:
        return 0.0
    return max(0.0, time.time() - since)


def reset_unhealthy_clock() -> None:
    """Called by main.py immediately after it triggers a yt-unhealthy
    restart so we don't fire the watchdog again until a fresh window
    has accumulated against the new ffmpeg session."""
    global _unhealthy_since_ts, _consecutive_unhealthy_polls
    with _state_lock:
        _unhealthy_since_ts = None
        _consecutive_unhealthy_polls = 0


def recent(
    limit: int = 200, since_ts: Optional[float] = None
) -> List[Dict[str, Any]]:
    """Return the most recent yt_health rows newest-first. Used by the
    optional health-history view in the UI."""
    limit = max(1, min(int(limit), 1000))
    c = _conn()
    try:
        if since_ts is not None:
            rows = c.execute(
                "SELECT ts, state, video_id, video_url, title, viewers, channel_url, error "
                "FROM yt_health WHERE ts >= ? ORDER BY ts DESC LIMIT ?",
                (float(since_ts), limit),
            ).fetchall()
        else:
            rows = c.execute(
                "SELECT ts, state, video_id, video_url, title, viewers, channel_url, error "
                "FROM yt_health ORDER BY ts DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        c.close()
