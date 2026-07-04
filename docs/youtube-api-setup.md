# YouTube Data API setup (hands-off daily streaming)

This is a one-time Google Cloud Console walkthrough that turns the extension's **API mode** on. Once done, the Pi will create a fresh YouTube broadcast every day, promote it to *live* automatically when bytes are flowing, and close it out at end-of-day — with no manual work in YouTube Studio.

The setup takes ~10 minutes. All the actual authorization then happens inside the extension (click Connect → enter a 6-character code → done).

> **Why is this needed?** Without the Data API, the extension only pushes RTMP bytes to a stream key — it has no way to *promote* a broadcast to "live." That's why a manually-armed YouTube broadcast worked one day and silently sat in "Preparing stream" the next: a YouTube scheduled broadcast is a one-time event that does not recreate itself. See the reference the plan is based on: <https://developers.google.com/youtube/v3/live/broadcasts-and-streams>.

## What you'll need

- A Google account that owns (or manages) the YouTube channel you want to stream to.
- Live streaming enabled on that channel (Studio → Settings → Channel → Feature eligibility → **Live streaming**). If this is the first time live is enabled on the channel, YouTube can take **up to 24 hours** to activate it.
- ~10 minutes at a laptop. The Pi does not need to be reachable from your laptop during setup.

## Step 1 — Create a Google Cloud project

1. Go to <https://console.cloud.google.com/>.
2. Top bar → project picker → **New Project**.
3. Name it something you'll recognise (e.g. `Kaumaui Cam`), organization can be left blank, click **Create**.
4. Make sure that new project is selected in the top bar for the rest of these steps.

## Step 2 — Enable the YouTube Data API v3

1. Left menu → **APIs & Services → Library**.
2. Search **YouTube Data API v3**, click it, then **Enable**.

## Step 3 — Configure the OAuth consent screen

This is the screen the operator sees when they approve the extension.

1. Left menu → **APIs & Services → OAuth consent screen** (or **Branding**, depending on Console version).
2. User type → **External** → Create.
3. App name: `Kaumaui Cam`; support email: your email; developer contact: your email. The other fields can be left blank.
4. **Scopes** → Add or Remove Scopes → search **YouTube Data API v3** → tick `.../auth/youtube` ("Manage your YouTube account") → Update. This is the only scope needed. **Do not** add `youtube.force-ssl` — the device flow doesn't support it.
5. **Test users** — skip; this step is optional and you'll publish next.
6. Save and continue back to the OAuth consent screen dashboard.
7. **Publishing status** → click **Publish app** and confirm.

> **Why publish the app?** Google gives OAuth refresh tokens a 7-day lifetime while the project is in **Testing** — the Pi would silently lose access every week. Publishing to **In production** removes the 7-day expiry. You do **not** need to complete Google's formal verification for personal / <100-user projects; the "Unverified app" warning at first connection is expected.

## Step 4 — Create an OAuth client (TVs and Limited Input)

1. Left menu → **APIs & Services → Credentials**.
2. **Create credentials → OAuth client ID**.
3. Application type → **TVs and Limited Input devices**.
4. Name: `Kaumaui Cam device`.
5. Create → copy the **Client ID** and **Client secret** shown; you'll paste them into the extension in a moment.

> **Why "TVs and Limited Input devices"?** The Pi is a headless device with no browser; the extension uses the OAuth 2.0 device flow ("go to google.com/device and enter a code") which is what this client type is for. A standard Web-application client type won't work — it requires a redirect URI the Pi doesn't have.

## Step 5 — Connect the extension

1. Open the Kaumaui Cam UI.
2. Streaming tab → **YouTube account (API mode)**.
3. Paste the **Client ID** and **Client secret** from step 4, click **Save credentials**.
4. Click **Connect YouTube** — a modal appears with a 6-character code.
5. Click **Open google.com/device** (or navigate there manually if the popup was blocked), enter the code.
6. Sign in with the Google account from step 1. You'll see an **"Unverified app"** warning — this is expected for your own Cloud project. Click **Advanced → Go to Kaumaui Cam (unsafe)**. It is safe: it's *your* project.
7. Approve the requested permission. The extension modal will close and show **Connected as \<channel name>**.
8. Toggle **Use API mode (extension manages broadcasts automatically)** on. Optionally edit the broadcast title template and privacy.
9. **Save API settings**.

That's it — the next time your schedule fires (or you click **Start now**), the extension will insert today's broadcast, bind it to a persistent stream, and transition it to live once RTMP is flowing.

## What happens day to day

- On the **first streaming slot of each HST calendar day**, the extension inserts a fresh broadcast titled with `{date}` substituted, binds it to a reusable `Kaumaui Cam (auto-managed)` liveStream, and starts ffmpeg pushing to that stream's RTMP endpoint.
- On every scheduler tick while ffmpeg is running, the extension calls `liveBroadcasts.transition(broadcastStatus="live")` once the stream is `active`. If YouTube's `enableAutoStart` already promoted it, the redundant transition is treated as success.
- **Between scheduled slots the broadcast stays open** (via `enableAutoStop=false`). This is the same setting that made the operator's manual test on 2026-07-03 work across multiple ffmpeg sessions in a single day — YouTube won't end the broadcast every time bytes stop briefly between slots.
- After the **last slot of the day**, the extension calls `liveBroadcasts.transition(broadcastStatus="complete")` so the archived video finalises with a correct end time.
- The next day, the whole cycle repeats: one broadcast per calendar date, one archived video per day, one persistent watch URL per day.

## Things to watch out for

- **First-time live streaming activation** can take up to 24 hours after enabling on the channel. Test on your own account before the org account so you don't get caught by this on go-live day.
- **Privacy = public** is required if you also want the extension's `/live` health monitor (channel-page scraping) to work. Unlisted/private broadcasts do not appear on the channel `/live` page. All API-mode features work regardless of privacy — the health-monitor is a bonus signal.
- **Do not delete** the `Kaumaui Cam (auto-managed)` liveStream from YouTube Studio while the extension is running. The extension will recreate it if it's missing, but that changes the stream key and produces a brief noisy patch.
- **Revoked tokens.** If you or your org's admin revoke the extension at [myaccount.google.com](https://myaccount.google.com/permissions), the next scheduler tick will see `invalid_grant` and the UI will flip to **Reauth needed**. Click **Connect YouTube** again to re-authorize; today's already-live broadcast is not disturbed.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Modal keeps saying "Waiting for you to approve the code" long after you approved | Client secret typo | Disconnect → re-enter credentials → Connect again |
| `invalid_grant` in the UI after 7 days | OAuth consent screen still in **Testing** | Go back to Step 3 and click **Publish app** |
| `Error: access_denied` in the modal | You clicked "Deny" on the consent screen | Click Connect again |
| `403 quotaExceeded` | You've been hammering the API from another tool with the same client | Wait 24h or request a quota bump |
| Broadcast created but stays `testing` forever | `liveStreams` status is not `active` (encoder not sending recognisable data) | Check the Streaming tab → Bandwidth line shows bytes moving; check camera + Starlink |
| Extension shows "Connected · off" | API mode toggle is not enabled | Turn on **Use API mode** |
| First broadcast title has literal `{date}` in it | Template lost its placeholder | Set title template to `Kaumaui Cam - {date}` and save |

## Where the state lives

Two files on the Pi's persistent volume (`/usr/blueos/extensions/kaumauicam` on the host, `/app/data` inside the container):

- `youtube_oauth.json` — refresh token, cached access token, and channel title. File mode `0600`. Persists across container rebuilds; delete manually only if you want to force reauth.
- `youtube_broadcast.json` — today's broadcast id / stream id / stream key. Rewritten each time the state machine advances; safe to delete (the next tick will recreate as needed).

Neither file is included in Docker builds; they're only ever written to the bind-mounted host volume.
