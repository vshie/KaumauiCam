/* global RTCPeerConnection */

const $ = (id) => document.getElementById(id);

/** Resolve paths for BlueOS extension subpaths (e.g. /extensionv2/kaumauicam/). */
function kmUrl(path) {
  const base = (typeof window.__KM_PREFIX === "string" ? window.__KM_PREFIX : "/").replace(/\/?$/, "/");
  return base + String(path || "").replace(/^\/+/, "");
}

function scheduleHtml(prefix, s) {
  const ws = (s.window_start || "06:00").slice(0, 5);
  const we = (s.window_stop || "18:00").slice(0, 5);
  return `
    <h3>Daily window & cycle</h3>
    <p class="status-line">Inside the window: every <b>interval</b> minutes, stream/record for the first <b>duration</b> minutes (same pattern as the other tab).</p>
    <div class="row">
      <div><label>Window start</label><input type="time" id="${prefix}-wstart" value="${ws}"></div>
      <div><label>Window stop</label><input type="time" id="${prefix}-wstop" value="${we}"></div>
    </div>
    <div class="row">
      <div><label>Interval (min)</label><input type="number" id="${prefix}-interval" min="1" value="${Number(s.interval_min) || 60}"></div>
      <div><label>Duration (min, on)</label><input type="number" id="${prefix}-duration" min="0" value="${Number(s.duration_min) || 20}"></div>
      <div style="flex: 0 0 auto; display: flex; align-items: flex-end; padding-bottom: 0.5rem">
        <label style="display: flex; align-items: center; gap: 0.4rem; cursor: pointer; color: var(--muted); margin: 0">
          <input type="checkbox" id="${prefix}-en" ${s.enabled ? "checked" : ""} /> Schedule on
        </label>
      </div>
    </div>
  `;
}

function readSchedule(prefix) {
  return {
    enabled: $(`${prefix}-en`).checked,
    window_start: toHhMmSs($(`${prefix}-wstart`).value),
    window_stop: toHhMmSs($(`${prefix}-wstop`).value),
    interval_min: parseInt($(`${prefix}-interval`).value, 10) || 60,
    duration_min: parseInt($(`${prefix}-duration`).value, 10) || 0,
  };
}

/** HTML time input "HH:MM" -> "HH:MM:00" for API */
function toHhMmSs(hhmm) {
  const p = (hhmm || "06:00").split(":");
  const h = (p[0] || "0").padStart(2, "0");
  const m = (p[1] || "00").padStart(2, "0");
  return `${h}:${m}:00`;
}

let pc = null;

async function startWebRTC() {
  const vid = $("live-video");
  const st = $("webrtc-status");
  stopWebRTC();
  try {
    pc = new RTCPeerConnection({ iceServers: [] });
    pc.addTransceiver("video", { direction: "recvonly" });
    pc.ontrack = (ev) => {
      if (vid.srcObject !== ev.streams[0]) {
        vid.srcObject = ev.streams[0];
      }
    };
    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    const res = await fetch(kmUrl("/go2rtc/api/webrtc?src=livepreview"), {
      method: "POST",
      body: pc.localDescription.sdp,
      headers: { "Content-Type": "application/sdp" },
    });
    if (!res.ok) {
      const t = await res.text();
      throw new Error(t || res.statusText);
    }
    const answerSdp = await res.text();
    await pc.setRemoteDescription({ type: "answer", sdp: answerSdp });
    st.textContent = "Connected.";
  } catch (e) {
    st.textContent = "WebRTC error: " + (e && e.message ? e.message : String(e));
    stopWebRTC();
  }
}

function stopWebRTC() {
  const vid = $("live-video");
  if (pc) {
    try {
      pc.close();
    } catch (_) {}
    pc = null;
  }
  if (vid.srcObject) {
    vid.srcObject.getTracks().forEach((t) => t.stop());
    vid.srcObject = null;
  }
  $("webrtc-status").textContent = "Disconnected.";
}

async function api(method, path, body) {
  const opt = { method, headers: {} };
  if (body !== undefined) {
    opt.headers["Content-Type"] = "application/json";
    opt.body = JSON.stringify(body);
  }
  const r = await fetch(kmUrl(path), opt);
  const t = await r.text();
  let j = null;
  try {
    j = JSON.parse(t);
  } catch (_) {}
  if (!r.ok) throw new Error((j && j.error) || t || r.statusText);
  return j;
}

async function loadConfig() {
  const c = await api("GET", "/api/config");
  $("cam-host").value = c.camera_host || "";
  $("cam-user").value = c.camera_user || "";
  $("cam-pass").value = c.camera_pass || "";
  $("yt-key").value = c.youtube_stream_key || "";
  $("quota-gb").value = c.monthly_quota_gb ?? 100;
  $("overhead-pct").value = c.bandwidth_overhead_pct ?? 3;
  $("rec-storage").value = c.recordings_storage || "auto";
  $("rec-profile").value = c.recordings_profile || "DefaultFishPond";
  $("schedule-yt").innerHTML = scheduleHtml("yt", c.youtube_schedule || {});
  $("schedule-rec").innerHTML = scheduleHtml("rec", c.recordings_schedule || {});
}

async function pollPtz() {
  try {
    const p = await api("GET", "/api/ptz/position");
    $("ptz-readout").textContent = `pan=${p.pan} tilt=${p.tilt} zoom=${p.zoom} autofocus=${p.autofocus || "?"}`;
  } catch (_) {
    $("ptz-readout").textContent = "PTZ unavailable";
  }
}

function fmtBytes(n) {
  if (n < 1024) return n + " B";
  if (n < 1024 ** 2) return (n / 1024).toFixed(1) + " KB";
  if (n < 1024 ** 3) return (n / 1024 ** 2).toFixed(1) + " MB";
  return (n / 1024 ** 3).toFixed(2) + " GB";
}

async function pollStream() {
  try {
    const s = await api("GET", "/api/stream/status");
    const b = s.bandwidth || {};
    const pct = b.quota_bytes ? Math.min(100, (100 * b.month_bytes_adjusted) / b.quota_bytes) : 0;
    $("bw-bar").style.width = pct + "%";
    let line = `Month (adj.): ${fmtBytes(b.month_bytes_adjusted || 0)}`;
    if (b.quota_bytes) line += ` / ${fmtBytes(b.quota_bytes)} · remaining ${fmtBytes(b.remaining_bytes || 0)}`;
    line += ` · today ${fmtBytes(b.day_bytes_adjusted || 0)}`;
    $("bw-text").textContent = line;
    if (s.running) {
      $("yt-stderr").textContent = "Streaming · session " + fmtBytes(s.session_bytes || 0);
    } else {
      $("yt-stderr").textContent = (s.stderr_tail || []).slice(-5).join(" | ") || "Idle.";
    }
  } catch (_) {}
}

async function pollRec() {
  try {
    const s = await api("GET", "/api/recordings/status");
    $("rec-status").textContent = `${s.running ? "Recording" : "Idle"} · ${s.label || "?"} · ${s.dest || ""} ${s.error ? "ERR: " + s.error : ""}`;
    const list = await api("GET", "/api/recordings/list");
    const tb = $("rec-files");
    tb.innerHTML = "";
    for (const f of list.files || []) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${f.name}</td><td>${fmtBytes(f.size)}</td><td><a href="${kmUrl("/api/recordings/download/" + encodeURIComponent(f.name))}">Download</a> · <button type="button" class="link-del" data-name="${f.name}">Delete</button></td>`;
      tb.appendChild(tr);
    }
    tb.querySelectorAll(".link-del").forEach((btn) => {
      btn.addEventListener("click", async () => {
        if (!confirm("Delete " + btn.dataset.name + "?")) return;
        await api("POST", "/api/recordings/delete", { name: btn.dataset.name });
        pollRec();
      });
    });
  } catch (_) {}
}

async function pollStorage() {
  try {
    const u = await api("GET", "/api/storage");
    const el = $("global-banner");
    if (!u.mounted && $("rec-storage").value === "usb") {
      el.className = "banner warn";
      el.textContent = "USB not mounted — recordings set to USB only will fail.";
    } else {
      el.className = "banner";
      el.textContent = "";
    }
  } catch (_) {}
}

function bindTabs() {
  const nav = document.getElementById("tab-nav");
  if (!nav || nav.dataset.kmTabsBound === "1") return;
  nav.dataset.kmTabsBound = "1";

  function activateTabFromButton(btn) {
    const tab = btn && btn.dataset.tab;
    if (!tab) return;
    nav.querySelectorAll("button[data-tab]").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".tab-panel").forEach((p) => {
      p.classList.remove("active");
      p.hidden = true;
    });
    btn.classList.add("active");
    const panel = document.getElementById("tab-" + tab);
    if (panel) {
      panel.hidden = false;
      panel.classList.add("active");
    }
  }

  /* Capture phase: run before any ancestor stops propagation (BlueOS / iframe shells). */
  nav.addEventListener(
    "click",
    (ev) => {
      const btn = ev.target.closest("button[data-tab]");
      if (!btn || !nav.contains(btn)) return;
      activateTabFromButton(btn);
    },
    true
  );

  /* Keyboard: left/right across tab buttons */
  nav.addEventListener("keydown", (ev) => {
    if (ev.key !== "ArrowLeft" && ev.key !== "ArrowRight") return;
    const tabs = Array.from(nav.querySelectorAll("button[data-tab]"));
    const i = tabs.indexOf(document.activeElement);
    if (i < 0) return;
    ev.preventDefault();
    const next = ev.key === "ArrowRight" ? tabs[Math.min(tabs.length - 1, i + 1)] : tabs[Math.max(0, i - 1)];
    if (next) {
      next.focus();
      activateTabFromButton(next);
    }
  });
}

function bindPtzHold() {
  document.querySelectorAll("[data-move]").forEach((btn) => {
    const [pan, tilt, zoom] = btn.dataset.move.split(",").map(Number);
    const down = () => api("POST", "/api/ptz/move", { pan, tilt, zoom });
    const up = () => api("POST", "/api/ptz/stop", {});
    btn.addEventListener("mousedown", down);
    btn.addEventListener("mouseup", up);
    btn.addEventListener("mouseleave", up);
    btn.addEventListener("touchstart", (e) => {
      e.preventDefault();
      down();
    });
    btn.addEventListener("touchend", up);
  });
  $("btn-ptz-stop").addEventListener("click", () => api("POST", "/api/ptz/stop", {}));
  $("btn-zoom-in").addEventListener("mousedown", () => api("POST", "/api/ptz/move", { pan: 0, tilt: 0, zoom: 30 }));
  $("btn-zoom-in").addEventListener("mouseup", () => api("POST", "/api/ptz/stop", {}));
  $("btn-zoom-out").addEventListener("mousedown", () => api("POST", "/api/ptz/move", { pan: 0, tilt: 0, zoom: -30 }));
  $("btn-zoom-out").addEventListener("mouseup", () => api("POST", "/api/ptz/stop", {}));
  $("btn-home").addEventListener("click", () => api("POST", "/api/ptz/home", {}));
  $("btn-af-on").addEventListener("click", () => api("POST", "/api/ptz/autofocus", { on: true }));
  $("btn-af-off").addEventListener("click", () => api("POST", "/api/ptz/autofocus", { on: false }));
}

async function initKaumauiCam() {
  bindTabs();
  bindPtzHold();
  try {
    await loadConfig();
  } catch (e) {
    $("global-banner").className = "banner err";
    $("global-banner").textContent = "Failed to load config: " + e.message;
  }

  $("btn-webrtc-start").addEventListener("click", startWebRTC);
  $("btn-webrtc-stop").addEventListener("click", stopWebRTC);

  $("btn-yt-save").addEventListener("click", async () => {
    const ys = readSchedule("yt");
    await api("POST", "/api/config", { youtube_stream_key: $("yt-key").value.trim(), youtube_schedule: ys });
    alert("Saved.");
  });
  $("btn-yt-start").addEventListener("click", async () => {
    await api("POST", "/api/config", { youtube_stream_key: $("yt-key").value.trim() });
    await api("POST", "/api/stream/start", {});
  });
  $("btn-yt-stop").addEventListener("click", async () => {
    await api("POST", "/api/stream/stop", {});
  });

  $("btn-rec-save").addEventListener("click", async () => {
    const rs = readSchedule("rec");
    await api("POST", "/api/recordings/config", {
      schedule: rs,
      storage: $("rec-storage").value,
      profile: $("rec-profile").value.trim(),
    });
    alert("Saved.");
  });
  $("btn-rec-start").addEventListener("click", async () => {
    await api("POST", "/api/recordings/config", {
      storage: $("rec-storage").value,
      profile: $("rec-profile").value.trim(),
    });
    await api("POST", "/api/recordings/start", {});
  });
  $("btn-rec-stop").addEventListener("click", async () => {
    await api("POST", "/api/recordings/stop", {});
  });

  $("btn-settings-save").addEventListener("click", async () => {
    await api("POST", "/api/config", {
      camera_host: $("cam-host").value.trim(),
      camera_user: $("cam-user").value.trim(),
      camera_pass: $("cam-pass").value,
      monthly_quota_gb: parseFloat($("quota-gb").value) || 0,
      bandwidth_overhead_pct: parseFloat($("overhead-pct").value) || 0,
    });
    alert("Saved. Reload page if camera IP changed.");
  });
  $("btn-bw-reset").addEventListener("click", async () => {
    if (!confirm("Reset this month's bandwidth counter?")) return;
    await api("POST", "/api/bandwidth/reset", {});
    pollStream();
  });
  $("btn-fishpond").addEventListener("click", async () => {
    await api("POST", "/api/camera/ensure-fishpond", {});
    alert("Applied DefaultFishPond profile parameters on camera.");
  });

  setInterval(pollPtz, 800);
  setInterval(pollStream, 2000);
  setInterval(pollRec, 3000);
  setInterval(pollStorage, 10000);
  pollPtz();
  pollStream();
  pollRec();
  pollStorage();
}

/* app.js is injected at end of body; it often loads after DOMContentLoaded already fired. */
if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", () => void initKaumauiCam());
} else {
  void initKaumauiCam();
}
