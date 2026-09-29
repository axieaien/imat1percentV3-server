// I'm at 1% — Live Battery Assistance (v2). Express + WebSocket + JSON-file storage.
"use strict";
const express = require("express"), http = require("http"), path = require("path"), fs = require("fs");
const crypto = require("crypto"), bcrypt = require("bcryptjs"), jwt = require("jsonwebtoken");
const { WebSocketServer } = require("ws");

const PORT = process.env.PORT || 3000;
const DATA_DIR = process.env.DATA_DIR || __dirname;
const DATA_FILE = path.join(DATA_DIR, "data.json");
const STALE_MS = 90e3, REQUEST_TTL = 6 * 3600e3;
const uuid = () => crypto.randomUUID();
fs.mkdirSync(DATA_DIR, { recursive: true });

// JWT secret: env var, else a random one generated once and kept next to the data
const JWT_SECRET = process.env.JWT_SECRET || (() => {
  const f = path.join(DATA_DIR, ".jwt_secret");
  try { return fs.readFileSync(f, "utf8").trim(); } catch (_) {}
  const s = crypto.randomBytes(48).toString("hex");
  try { fs.writeFileSync(f, s, { mode: 0o600 }); } catch (_) {}
  return s;
})();

// ---------- storage: debounced atomic writes, flushed on exit ----------
let db = { users: {}, requests: {}, messages: {}, resets: {}, reports: [] };
try { if (fs.existsSync(DATA_FILE)) db = { ...db, ...JSON.parse(fs.readFileSync(DATA_FILE, "utf8")) }; }
catch (e) { console.error("Could not read data.json:", e.message); try { fs.copyFileSync(DATA_FILE, DATA_FILE + ".corrupt-" + Date.now()); } catch (_) {} }
for (const u of Object.values(db.users)) { u.tv = u.tv || 0; u.blocked = u.blocked || []; delete u.online; delete u.lat; delete u.lng; } // presence & location are memory-only
const presence = new Map(), locations = new Map(); // uid -> lastSeen ms, uid -> {lat,lng}
const byPair = new Map(); // pair -> messages[] (index)
function indexMessages() { byPair.clear(); Object.values(db.messages).sort((a, b) => a.createdAt - b.createdAt).forEach((m) => { if (!byPair.has(m.pair)) byPair.set(m.pair, []); byPair.get(m.pair).push(m); }); }
indexMessages();

let saveTimer = null, writing = false, dirty = false;
function persist() { dirty = true; if (!saveTimer) saveTimer = setTimeout(flush, 400); }
function flush() {
  saveTimer = null; if (writing || !dirty) return; writing = true; dirty = false;
  const tmp = DATA_FILE + ".tmp";
  fs.writeFile(tmp, JSON.stringify(db), (err) => {
    if (err) { console.error("Write failed:", err.message); dirty = true; writing = false; return; }
    fs.rename(tmp, DATA_FILE, () => { writing = false; if (dirty) persist(); });
  });
}
function flushSync() { try { fs.writeFileSync(DATA_FILE + ".tmp", JSON.stringify(db)); fs.renameSync(DATA_FILE + ".tmp", DATA_FILE); } catch (e) { console.error(e.message); } }

// ---------- helpers ----------
const isLive = (id) => Date.now() - (presence.get(id) || 0) < STALE_MS;
const touch = (id) => presence.set(id, Date.now());
const blockedEither = (a, b) => !!(db.users[a]?.blocked.includes(b) || db.users[b]?.blocked.includes(a));
const pairOf = (a, b) => [a, b].sort().join("_");
const num = (v, lo, hi) => typeof v === "number" && Number.isFinite(v) && v >= lo && v <= hi;
const clean = (s, n) => String(s ?? "").replace(/[\u0000-\u001f\u007f]/g, " ").trim().slice(0, n);
function publicUser(u) {
  const live = isLive(u.id), loc = live && locations.get(u.id);
  return { id: u.id, name: u.name, role: u.role, helperAvailable: u.helperAvailable, online: live, lat: loc ? loc.lat : null, lng: loc ? loc.lng : null, updatedAt: presence.get(u.id) || null };
}
const sign = (u) => jwt.sign({ uid: u.id, tv: u.tv }, JWT_SECRET, { expiresIn: "30d" });
function verify(token) {
  const { uid, tv } = jwt.verify(token, JWT_SECRET), u = db.users[uid];
  if (!u || (u.tv || 0) !== (tv || 0)) throw new Error("invalid");
  return u;
}

// ---------- realtime ----------
const app = express(), server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws", maxPayload: 4096 });
const sockets = new Map(); // uid -> Set<ws>
function sendTo(uid, payload) { const m = JSON.stringify(payload); for (const ws of sockets.get(uid) || []) if (ws.readyState === 1) ws.send(m); }
function broadcast(payload) { const m = JSON.stringify(payload); for (const set of sockets.values()) for (const ws of set) if (ws.readyState === 1) ws.send(m); }
let usersTimer = null; // coalesce bursts of presence/location changes into one push
function usersChanged() { if (usersTimer) return; usersTimer = setTimeout(() => { usersTimer = null; broadcast({ type: "users:update" }); }, 700); }

wss.on("connection", (ws, req) => {
  let u; try { u = verify(new URL(req.url, "http://x").searchParams.get("token")); } catch (_) { return ws.close(4001, "bad token"); }
  const set = sockets.get(u.id) || sockets.set(u.id, new Set()).get(u.id);
  if (set.size >= 5) set.values().next().value.close(4002, "too many connections");
  set.add(ws); ws.alive = true; touch(u.id); usersChanged();
  ws.on("pong", () => { ws.alive = true; });
  ws.on("message", (raw) => { // typing indicator relay
    try { const m = JSON.parse(raw); if (m.type === "typing" && db.users[m.to]) sendTo(m.to, { type: "typing", from: u.id }); } catch (_) {}
  });
  ws.on("close", () => { set.delete(ws); if (!set.size) sockets.delete(u.id); });
});
setInterval(() => wss.clients.forEach((ws) => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); }), 30e3).unref();
setInterval(() => { // sweeper: expire stale presence & old pending requests
  let changed = false;
  for (const [id, t] of presence) if (Date.now() - t >= STALE_MS && locations.delete(id)) changed = true;
  for (const r of Object.values(db.requests)) if (r.status === "Pending" && Date.now() - r.createdAt > REQUEST_TTL) { r.status = "Expired"; changed = true; persist(); broadcast({ type: "requests:update" }); }
  if (changed) usersChanged();
}, 30e3).unref();

// ---------- middleware ----------
app.disable("x-powered-by"); app.set("trust proxy", 1);
app.use((req, res, next) => {
  res.set({ "X-Content-Type-Options": "nosniff", "X-Frame-Options": "DENY", "Referrer-Policy": "same-origin", "Permissions-Policy": "geolocation=(self)" });
  if (req.secure) res.set("Strict-Transport-Security", "max-age=15552000");
  next();
});
app.use(express.json({ limit: "10kb" }), express.text({ type: "text/plain", limit: "2kb" }));
const ah = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);
function limiter(max, windowMs, keyFn = (r) => r.ip) {
  const hits = new Map(); setInterval(() => { const n = Date.now(); for (const [k, v] of hits) if (v.reset < n) hits.delete(k); }, windowMs).unref();
  return (req, res, next) => {
    const k = keyFn(req), n = Date.now(); let h = hits.get(k);
    if (!h || h.reset < n) hits.set(k, (h = { c: 0, reset: n + windowMs }));
    if (++h.c > max) return res.set("Retry-After", Math.ceil((h.reset - n) / 1000)).status(429).json({ error: "Too many attempts. Please slow down and try again shortly." });
    next();
  };
}
const authLimit = limiter(15, 15 * 60e3);
function auth(req, res, next) {
  const h = req.headers.authorization || "";
  if (!h.startsWith("Bearer ")) return res.status(401).json({ error: "Not signed in." });
  try { req.user = verify(h.slice(7)); req.uid = req.user.id; next(); }
  catch (_) { res.status(401).json({ error: "Session expired. Please log in again." }); }
}
const apiLimit = limiter(240, 60e3, (r) => r.uid || r.ip);
app.get("/healthz", (req, res) => res.json({ ok: true, users: Object.keys(db.users).length, online: [...presence.keys()].filter(isLive).length }));

// ---------- auth ----------
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;
const DUMMY_HASH = bcrypt.hashSync("dummy-password", 10);
app.post("/api/auth/register", authLimit, ah(async (req, res) => {
  const email = clean(req.body.email, 254).toLowerCase(), password = String(req.body.password || "");
  const name = clean(req.body.name, 60) || email.split("@")[0];
  if (!EMAIL_RE.test(email)) return res.status(400).json({ error: "Please enter a valid email address." });
  if (password.length < 8 || password.length > 72) return res.status(400).json({ error: "Password must be 8–72 characters." });
  if (Object.values(db.users).some((u) => u.email === email)) return res.status(409).json({ error: "That account already exists. Log in instead." });
  const id = uuid();
  db.users[id] = { id, email, passwordHash: await bcrypt.hash(password, 11), name, role: "Student", helperAvailable: false, tv: 0, blocked: [], createdAt: Date.now() };
  touch(id); persist(); usersChanged();
  res.json({ token: sign(db.users[id]), user: publicUser(db.users[id]) });
}));
app.post("/api/auth/login", authLimit, ah(async (req, res) => {
  const email = clean(req.body.email, 254).toLowerCase(), password = String(req.body.password || "").slice(0, 72);
  const u = Object.values(db.users).find((x) => x.email === email);
  const ok = await bcrypt.compare(password, u ? u.passwordHash : DUMMY_HASH); // constant-ish time: no user enumeration
  if (!u || !ok) return res.status(401).json({ error: "Account not found or password is incorrect." });
  touch(u.id); usersChanged();
  res.json({ token: sign(u), user: publicUser(u) });
}));

// ---------- forgot / reset password ----------
// Email delivery: SMTP_URL (uses nodemailer) or RESEND_API_KEY (plain HTTPS). With neither, the link is printed in the server log.
const MAIL_FROM = process.env.MAIL_FROM || "I'm at 1% <onboarding@resend.dev>";
async function sendMail(to, subject, text) {
  try {
    if (process.env.SMTP_URL) { await require("nodemailer").createTransport(process.env.SMTP_URL).sendMail({ from: MAIL_FROM, to, subject, text }); return true; }
    if (process.env.RESEND_API_KEY) {
      const r = await fetch("https://api.resend.com/emails", { method: "POST", headers: { Authorization: "Bearer " + process.env.RESEND_API_KEY, "Content-Type": "application/json" }, body: JSON.stringify({ from: MAIL_FROM, to, subject, text }) });
      if (!r.ok) throw new Error(await r.text()); return true;
    }
  } catch (e) { console.error("Mail failed:", e.message); return false; }
  console.log(`\n[mail not configured] To: ${to}\n${subject}\n${text}\n`); return false;
}
const sha = (t) => crypto.createHash("sha256").update(t).digest("hex");
app.post("/api/auth/forgot", limiter(5, 15 * 60e3), ah(async (req, res) => {
  const email = clean(req.body.email, 254).toLowerCase(), u = Object.values(db.users).find((x) => x.email === email);
  if (u) {
    const t = crypto.randomBytes(32).toString("hex");
    for (const [k, v] of Object.entries(db.resets)) if (v.uid === u.id || v.exp < Date.now()) delete db.resets[k];
    db.resets[sha(t)] = { uid: u.id, exp: Date.now() + 30 * 60e3 }; persist();
    const base = process.env.APP_URL || `${req.protocol}://${req.get("host")}`;
    sendMail(u.email, "Reset your I'm at 1% password", `Hi ${u.name},\n\nReset your password (link valid for 30 minutes):\n${base.replace(/\/$/, "")}/?reset=${t}\n\nIf you didn't ask for this, ignore this email.`);
  }
  res.json({ ok: true, message: "If that email has an account, a reset link is on its way." }); // same answer either way
}));
app.post("/api/auth/reset", authLimit, ah(async (req, res) => {
  const k = sha(String(req.body.token || "")), r = db.resets[k], password = String(req.body.password || "");
  if (!r || r.exp < Date.now() || !db.users[r.uid]) return res.status(400).json({ error: "This reset link is invalid or has expired. Request a new one." });
  if (password.length < 8 || password.length > 72) return res.status(400).json({ error: "Password must be 8–72 characters." });
  const u = db.users[r.uid]; u.passwordHash = await bcrypt.hash(password, 11); u.tv++; delete db.resets[k]; persist();
  res.json({ token: sign(u), user: publicUser(u) });
}));

// ---------- profile / presence ----------
app.use("/api", (req, res, next) => (req.path.startsWith("/auth/") || req.path === "/offline" ? next() : auth(req, res, () => apiLimit(req, res, next))));
const me = (u) => ({ id: u.id, email: u.email, name: u.name, role: u.role, helperAvailable: u.helperAvailable });
app.get("/api/me", (req, res) => res.json(me(req.user)));
app.put("/api/me", (req, res) => {
  const u = req.user, b = req.body;
  if (clean(b.name, 60)) u.name = clean(b.name, 60);
  if (["Student", "Helper", "Both"].includes(b.role)) u.role = b.role;
  if (typeof b.helperAvailable === "boolean") u.helperAvailable = b.helperAvailable;
  touch(u.id); persist(); usersChanged(); res.json(me(u));
});
app.post("/api/me/password", ah(async (req, res) => {
  const u = req.user, next = String(req.body.newPassword || "");
  if (!(await bcrypt.compare(String(req.body.password || ""), u.passwordHash))) return res.status(403).json({ error: "Current password is incorrect." });
  if (next.length < 8 || next.length > 72) return res.status(400).json({ error: "New password must be 8–72 characters." });
  u.passwordHash = await bcrypt.hash(next, 11); u.tv++; persist(); // invalidates all other sessions
  res.json({ token: sign(u) });
}));
app.delete("/api/me", ah(async (req, res) => {
  const u = req.user;
  if (!(await bcrypt.compare(String(req.body.password || ""), u.passwordHash))) return res.status(403).json({ error: "Password is incorrect." });
  delete db.users[u.id]; for (const x of Object.values(db.users)) x.blocked = x.blocked.filter((i) => i !== u.id); presence.delete(u.id); locations.delete(u.id);
  for (const r of Object.values(db.requests)) if (r.userId === u.id) delete db.requests[r.id];
  for (const m of Object.values(db.messages)) if (m.from === u.id || m.to === u.id) delete db.messages[m.id];
  indexMessages(); persist(); usersChanged(); broadcast({ type: "requests:update" });
  for (const ws of sockets.get(u.id) || []) ws.close(4001, "deleted");
  res.json({ ok: true });
}));
app.post("/api/location", (req, res) => {
  const { lat, lng } = req.body;
  if (lat === null && lng === null) locations.delete(req.uid);
  else if (num(lat, -90, 90) && num(lng, -180, 180)) locations.set(req.uid, { lat, lng });
  else return res.status(400).json({ error: "Invalid coordinates." });
  touch(req.uid); usersChanged(); res.json({ ok: true });
});
app.post("/api/heartbeat", (req, res) => { const was = isLive(req.uid); touch(req.uid); if (!was) usersChanged(); res.json({ ok: true }); });
app.post("/api/offline", (req, res) => { // sendBeacon: token in text/plain body (or ?token= for older clients)
  try {
    const raw = typeof req.body === "string" ? req.body : req.body && req.body.token;
    const u = verify(req.query.token || raw);
    presence.delete(u.id); locations.delete(u.id); usersChanged();
  } catch (_) {}
  res.json({ ok: true });
});
app.get("/api/blocked", (req, res) => res.json(req.user.blocked.filter((id) => db.users[id]).map((id) => ({ id, name: db.users[id].name }))));
app.post("/api/block", (req, res) => {
  const id = String(req.body.id || ""); if (!db.users[id] || id === req.uid) return res.status(400).json({ error: "Invalid user." });
  const b = req.user.blocked, i = b.indexOf(id);
  if (req.body.block === false) { if (i >= 0) b.splice(i, 1); } else if (i < 0) b.push(id);
  persist(); usersChanged(); broadcast({ type: "requests:update" }); res.json({ ok: true });
});
app.post("/api/report", limiter(10, 3600e3, (r) => r.uid), (req, res) => {
  const id = String(req.body.id || ""); if (!db.users[id] || id === req.uid) return res.status(400).json({ error: "Invalid user." });
  db.reports.push({ id: uuid(), reporter: req.uid, reported: id, reason: clean(req.body.reason, 500), createdAt: Date.now() }); persist();
  console.log(`[report] ${req.uid} reported ${id}`); res.json({ ok: true });
});
app.get("/api/users", (req, res) => res.json(Object.values(db.users).filter((u) => u.id !== req.uid && !blockedEither(u.id, req.uid)).map(publicUser)));

// ---------- requests ----------
const canSee = (r, u) => !blockedEither(r.userId, u.id) && (r.userId === u.id || r.helperId === u.id || (u.helperAvailable && r.status === "Pending"));
app.get("/api/requests", (req, res) => res.json(Object.values(db.requests).filter((r) => canSee(r, req.user)).sort((a, b) => b.createdAt - a.createdAt).slice(0, 200)));
app.post("/api/requests", (req, res) => {
  const open = Object.values(db.requests).find((r) => r.userId === req.uid && (r.status === "Pending" || r.status === "Accepted"));
  if (open) return res.status(409).json({ error: "You already have an open request. Resolve or cancel it first." });
  const resource = ["Power Bank", "Charger", "Charging Outlet"].includes(req.body.resource) ? req.body.resource : "Power Bank";
  const r = { id: uuid(), userId: req.uid, battery: Math.max(1, Math.min(100, Math.round(Number(req.body.battery)) || 1)), resource, note: clean(req.body.note, 300), status: "Pending", helperId: null, createdAt: Date.now() };
  db.requests[r.id] = r; persist(); broadcast({ type: "requests:update" });
  for (const u of Object.values(db.users)) if (u.helperAvailable && u.id !== req.uid && !blockedEither(u.id, req.uid)) sendTo(u.id, { type: "request:new", name: req.user.name, battery: r.battery, resource });
  res.json(r);
});
function loadReq(req, res) { const r = db.requests[req.params.id]; if (!r || !canSee(r, req.user)) { res.status(404).json({ error: "Request not found." }); return null; } return r; }
app.post("/api/requests/:id/accept", (req, res) => {
  const r = loadReq(req, res); if (!r) return;
  if (r.userId === req.uid) return res.status(400).json({ error: "You can't accept your own request." });
  if (!req.user.helperAvailable) return res.status(403).json({ error: "Turn on “Available as helper” in your profile first." });
  if (r.status !== "Pending") return res.status(409).json({ error: "Someone else already accepted this request." });
  r.status = "Accepted"; r.helperId = req.uid; r.acceptedAt = Date.now();
  addMessage(req.uid, r.userId, `Hi! I can help with your ${r.resource}.`);
  persist(); broadcast({ type: "requests:update" }); res.json(r);
});
const finish = (status) => (req, res) => {
  const r = loadReq(req, res); if (!r) return;
  if (r.userId !== req.uid && !(status === "Resolved" && r.helperId === req.uid)) return res.status(403).json({ error: "Not allowed." });
  if (r.status !== "Pending" && r.status !== "Accepted") return res.status(409).json({ error: `Request is already ${r.status.toLowerCase()}.` });
  r.status = status; r.closedAt = Date.now(); persist(); broadcast({ type: "requests:update" }); res.json(r);
};
app.post("/api/requests/:id/resolve", finish("Resolved"));
app.post("/api/requests/:id/cancel", finish("Cancelled"));

// ---------- messages ----------
function addMessage(from, to, text) {
  const pair = pairOf(from, to), m = { id: uuid(), from, to, pair, text, createdAt: Date.now(), readAt: null };
  db.messages[m.id] = m; if (!byPair.has(pair)) byPair.set(pair, []); byPair.get(pair).push(m);
  sendTo(to, { type: "message:new", pair, from }); sendTo(from, { type: "message:new", pair, from });
  return m;
}
app.get("/api/conversations", (req, res) => { // last message + unread count per person
  const out = [];
  for (const [pair, list] of byPair) {
    if (!pair.split("_").includes(req.uid) || !list.length) continue;
    const last = list[list.length - 1], other = last.from === req.uid ? last.to : last.from;
    out.push({ with: other, last: { text: last.text, from: last.from, createdAt: last.createdAt }, unread: list.filter((m) => m.to === req.uid && !m.readAt).length });
  }
  res.json(out.sort((a, b) => b.last.createdAt - a.last.createdAt));
});
app.get("/api/messages", (req, res) => {
  const w = String(req.query.with || ""); if (!db.users[w]) return res.status(400).json({ error: "Unknown user." });
  res.json((byPair.get(pairOf(req.uid, w)) || []).slice(-200));
});
app.post("/api/messages/read", (req, res) => {
  const w = String(req.body.with || ""), now = Date.now(); let n = 0;
  for (const m of byPair.get(pairOf(req.uid, w)) || []) if (m.to === req.uid && !m.readAt) { m.readAt = now; n++; }
  if (n) { persist(); sendTo(w, { type: "message:read", by: req.uid }); }
  res.json({ ok: true });
});
app.post("/api/messages", limiter(60, 60e3, (r) => r.uid), (req, res) => {
  const to = String(req.body.to || ""), text = clean(req.body.text, 1000);
  if (!db.users[to] || to === req.uid || !text || blockedEither(req.uid, to)) return res.status(400).json({ error: "Invalid message." });
  persist(); res.json(addMessage(req.uid, to, text));
});

// ---------- static / errors ----------
app.use("/api", (req, res) => res.status(404).json({ error: "Not found." }));
app.use(express.static(path.join(__dirname, "public"), { maxAge: "1h", setHeaders: (res, p) => { if (/\.(html|js)$/.test(p)) res.setHeader("Cache-Control", "no-cache"); } }));
app.get("*", (req, res) => res.sendFile(path.join(__dirname, "public", "index.html")));
app.use((err, req, res, next) => { // eslint-disable-line no-unused-vars
  if (err.type === "entity.parse.failed" || err.type === "entity.too.large") return res.status(400).json({ error: "Bad request body." });
  console.error(err); res.status(500).json({ error: "Something went wrong on the server." });
});

server.listen(PORT, () => console.log(`I'm at 1% server listening on http://localhost:${PORT}`));
for (const sig of ["SIGINT", "SIGTERM"]) process.on(sig, () => { flushSync(); wss.close(); server.close(() => process.exit(0)); setTimeout(() => process.exit(0), 3000).unref(); });
