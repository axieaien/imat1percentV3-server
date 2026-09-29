# I'm at 1% — Live Battery Assistance (self-hosted server)

A real Node.js server for the app: no Firebase, no third-party backend.
Everyone who opens the site — from any device, anywhere — sees the same
live data (accounts, online status, requests, chat).

## What's inside
- `server.js` — Express API + WebSocket server + JSON-file storage (`data.json`, created automatically)
- `public/index.html` — the whole front-end (map, dashboard, requests, "Who To Help", chat, profile)
- Auth: email + password, hashed with bcrypt, sessions via JWT
- Realtime: a WebSocket pushes presence/request/message updates to every connected browser instantly

## Run it locally
```bash
npm install
npm start
```
Then open http://localhost:3000 — that's it, front-end and API are served from the same place.

## Deploy it for free (pick one)

### Render.com
1. Push this folder to a GitHub repo.
2. Render → New → Web Service → connect the repo.
3. Build command: `npm install`  ·  Start command: `npm start`
4. Add an environment variable `JWT_SECRET` set to any long random string (Render can generate one).
5. Deploy. Render gives you a public `https://yourapp.onrender.com` URL — share that.

### Railway.app
1. Push to GitHub, then Railway → New Project → Deploy from GitHub repo.
2. Railway auto-detects Node and runs `npm start`.
3. Add the `JWT_SECRET` variable in the project's Variables tab.
4. Railway gives you a public URL under Settings → Networking.

### Fly.io / any VPS
Any host that runs `node server.js` and exposes `$PORT` works the same way —
`npm install && npm start`, set `JWT_SECRET`, done.

## What's new in v3
- **Map works everywhere**: no fixed service area any more. The map centers on *you*, with a search radius you choose (500 m → 25 km → anywhere). It loads Leaflet from three CDNs in turn, switches tile provider if OpenStreetMap is blocked, and falls back to a dashed line plus "Open in Google/Apple Maps" links if routing is unavailable.
- **Location permission on any device**: pre-permission explainer, Permissions-API check, step-by-step help for a blocked permission on iOS / Android / desktop, HTTPS check, in-app-browser warning, high→low accuracy retry, weak-GPS handling, refresh when the app returns to the foreground, and a **"pin on map" manual fallback** when GPS is unavailable or denied. Sharing resumes automatically only if permission is already granted (no surprise prompts).
- **Forgot password**: emailed single-use link (30 min, stored hashed, all sessions signed out after reset). Configure `RESEND_API_KEY` (+ `MAIL_FROM`) or `SMTP_URL`, and `APP_URL`. With neither, the link is printed in the server log (handy for local testing).
- **Safety**: block/unblock and report users (blocked people vanish from each other's lists, requests and chat).
- `render.yaml` blueprint for one-click Render deploys.

### Environment variables
| Name | Purpose |
|---|---|
| `JWT_SECRET` | signing key (auto-generated if unset, but set it in production) |
| `DATA_DIR` | folder for `data.json` (use a mounted disk) |
| `APP_URL` | public https URL, used in reset emails |
| `RESEND_API_KEY`, `MAIL_FROM` | send reset emails via Resend |
| `SMTP_URL` | alternative: `smtps://user:pass@host:465` |

## What was new in v2
- **Security**: random JWT secret auto-generated if `JWT_SECRET` is unset; rate limiting (auth + API + messages); security headers; strict input validation; 8+ char passwords; no user enumeration on login; change-password (signs out other sessions) and delete-account.
- **Privacy**: locations and presence are memory-only (never written to disk) and expire ~90 s after a user goes quiet; requests are only visible to their owner, the accepting helper, and available helpers (for pending ones).
- **Reliability**: debounced atomic writes flushed on shutdown, corrupt-file backup, presence sweeper, WebSocket ping/pong, coalesced live updates, graceful SIGTERM, `/healthz`, `DATA_DIR` for persistent disks.
- **Features**: request lifecycle (Pending → Accepted → Resolved / Cancelled / auto-Expired after 6 h), one open request per user, unread badges, last-message previews, read receipts, typing indicator, helper push toasts + browser notifications, distance from you, reconnect resync and offline banner, installable PWA (manifest + service worker).
- **Fewer dependencies**: `uuid` removed (uses `crypto.randomUUID`).

## Important notes
- **Persistence**: set `DATA_DIR` to a mounted disk path (e.g. `/var/data`) on Render/Railway/Fly so `data.json` survives redeploys.
- **HTTPS** is required for location sharing; Render/Railway provide it automatically.
- **Scale**: JSON storage suits a class/campus pilot. For more, move `db` to SQLite/Postgres.
- **Still on the wishlist**: token in an HttpOnly cookie, SRI hashes for the Leaflet CDN files, automated tests.

## API summary
| Method | Path | Purpose |
|---|---|---|
| POST | /api/auth/register · /login | `{token,user}` |
| POST | /api/auth/forgot · /reset | request reset email · set new password with token |
| POST | /api/block · /api/report · GET /api/blocked | safety tools |
| GET / PUT / DELETE | /api/me | profile · update · delete account |
| POST | /api/me/password | change password → new token |
| POST | /api/location · /heartbeat · /offline | presence |
| GET | /api/users | others' public profile/presence |
| GET / POST | /api/requests | visible requests · create |
| POST | /api/requests/:id/accept · /resolve · /cancel | lifecycle |
| GET | /api/conversations | last message + unread per person |
| GET / POST | /api/messages | thread (`?with=ID`) · send |
| POST | /api/messages/read | mark thread read |
| GET | /healthz | health check |
| WS | /ws?token=… | `users:update`, `requests:update`, `request:new`, `message:new`, `message:read`, `typing` |
