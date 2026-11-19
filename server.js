// 챕터 0 · 함께 걷는 밤 — 의존성 없는 실시간 서버 (Node.js 18+) v2.3
// 실행: node server.js   (PORT 환경변수가 있으면 그 포트, 없으면 3000)
"use strict";
const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = +process.env.MAX_PLAYERS || 60;
const MAX_PAYLOAD = 2048;
const SAY_GAP_MS = 800;
const EMOTE_GAP_MS = 600;
const SPAM_LIMIT = 8, SPAM_WINDOW_MS = 20000, MUTE_MS = 30000;
const WORLD_W = 4200, WORLD_H = 600;
const BAD_WORDS = ["씨발", "시발", "병신", "개새끼", "ㅅㅂ", "ㅂㅅ", "좆", "섹스"];
const GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const EMOTES = ["😊", "😢", "😂", "👍", "🔥", "👏"];

const indexHtml = fs.readFileSync(path.join(__dirname, "index.html"));
const bgmCandidates = ["bgm.mp3", "Library _ Suno - Chrome 2026-11-19 13-35-54.mp3"];
let bgmBuf = null;
for (const name of bgmCandidates) {
  const full = path.join(__dirname, name);
  if (fs.existsSync(full)) {
    bgmBuf = fs.readFileSync(full);
    break;
  }
}

const clients = new Map();
const booths = {
  b1: { id: "b1", x: 1200, ownerId: null, ownerName: null },
  b2: { id: "b2", x: 2700, ownerId: null, ownerName: null }
};
let dirty = false;

const clean = (v, n) => String(v == null ? "" : v)
  .replace(/[\u0000-\u001f\u007f\u200b-\u200f\u202a-\u202e\u2066-\u2069]/g, "").trim().slice(0, n);
const maskBad = (t) => {
  const flat = t.toLowerCase().replace(/[\s.\-_*]/g, "");
  return BAD_WORDS.some((w) => flat.includes(w)) ? null : t;
};
const num = (v, lo, hi, d) => (Number.isFinite(+v) ? Math.max(lo, Math.min(hi, +v)) : d);

function boothSnapshot() {
  return Object.values(booths).map((b) => ({
    id: b.id, x: b.x, ownerId: b.ownerId, ownerName: b.ownerName
  }));
}

function releaseBoothsBy(clientId) {
  let changed = false;
  for (const b of Object.values(booths)) {
    if (b.ownerId === clientId) {
      b.ownerId = null;
      b.ownerName = null;
      changed = true;
    }
  }
  if (changed) broadcast({ t: "booths", booths: boothSnapshot() });
}

// ---------- HTTP ----------
const server = http.createServer((req, res) => {
  const url = (req.url || "/").split("?")[0];
  if (url === "/" || url === "/index.html") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-cache" });
    res.end(indexHtml);
  } else if (url === "/bgm.mp3") {
    if (!bgmBuf) {
      res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
      res.end("bgm not found");
      return;
    }
    res.writeHead(200, {
      "Content-Type": "audio/mpeg",
      "Content-Length": bgmBuf.length,
      "Accept-Ranges": "bytes",
      "Cache-Control": "public, max-age=3600"
    });
    res.end(bgmBuf);
  } else if (url === "/healthz") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok " + clients.size);
  } else {
    res.writeHead(404, { "Content-Type": "text/plain; charset=utf-8" });
    res.end("not found");
  }
});

// ---------- WebSocket (RFC 6455 최소 구현) ----------
function frame(opcode, payload) {
  const body = Buffer.isBuffer(payload) ? payload : Buffer.from(payload);
  const len = body.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | opcode, len]);
  else { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
  return Buffer.concat([head, body]);
}
function send(c, obj) {
  if (c.dead || c.socket.destroyed) return;
  try { c.socket.write(frame(0x1, JSON.stringify(obj))); } catch (e) { drop(c); }
}
function drop(c, code) {
  if (c.dead) return;
  c.dead = true;
  try { if (code) c.socket.write(frame(0x8, Buffer.from([code >> 8, code & 255]))); } catch (e) {}
  try { c.socket.end(); c.socket.destroy(); } catch (e) {}
  const wasJoined = c.joined;
  if (clients.delete(c.id) && wasJoined) {
    releaseBoothsBy(c.id);
    dirty = true;
    broadcast({ t: "leave", id: c.id });
  }
}
function broadcast(obj) { for (const c of clients.values()) if (c.joined) send(c, obj); }

server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  if ((req.url || "").split("?")[0] !== "/ws" || !key || String(req.headers.upgrade).toLowerCase() !== "websocket") {
    socket.end("HTTP/1.1 400 Bad Request\r\n\r\n"); return;
  }
  if (clients.size >= MAX_PLAYERS) { socket.end("HTTP/1.1 503 Service Unavailable\r\n\r\n"); return; }
  const accept = crypto.createHash("sha1").update(key + GUID).digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n");
  socket.setNoDelay(true);

  const c = {
    id: crypto.randomBytes(4).toString("hex"), socket, buf: Buffer.alloc(0),
    joined: false, dead: false, n: "", col: 0, x: 260, y: 0, f: 1, w: 0, sit: -1,
    lastSay: 0, lastEmote: 0, sayTimes: [], mutedUntil: 0, alive: true
  };
  clients.set(c.id, c);
  socket.on("data", (d) => {
    c.buf = Buffer.concat([c.buf, d]);
    if (c.buf.length > MAX_PAYLOAD + 32) return drop(c, 1009);
    let m;
    while (!c.dead && (m = parse(c))) handle(c, m);
  });
  socket.on("error", () => drop(c));
  socket.on("close", () => drop(c));
});

function parse(c) {
  const b = c.buf;
  if (b.length < 2) return null;
  const fin = b[0] & 0x80, op = b[0] & 0x0f, masked = b[1] & 0x80;
  let len = b[1] & 0x7f, off = 2;
  if (len === 126) { if (b.length < 4) return null; len = b.readUInt16BE(2); off = 4; }
  else if (len === 127) { drop(c, 1009); return null; }
  if (!masked || !fin || len > MAX_PAYLOAD) { drop(c, len > MAX_PAYLOAD ? 1009 : 1002); return null; }
  if (b.length < off + 4 + len) return null;
  const mask = b.subarray(off, off + 4), data = Buffer.alloc(len);
  for (let i = 0; i < len; i++) data[i] = b[off + 4 + i] ^ mask[i & 3];
  c.buf = b.subarray(off + 4 + len);
  return { op, data };
}

function handle(c, m) {
  if (m.op === 0x8) return drop(c, 1000);
  if (m.op === 0x9) { try { c.socket.write(frame(0xA, m.data)); } catch (e) {} return; }
  if (m.op === 0xA) { c.alive = true; return; }
  if (m.op !== 0x1) return;
  let o; try { o = JSON.parse(m.data.toString("utf8")); } catch (e) { return; }
  if (!o || typeof o !== "object") return;

  if (o.t === "join" && !c.joined) {
    c.n = maskBad(clean(o.n, 10)) || "나그네" + (100 + Math.floor(Math.random() * 900));
    c.col = num(o.c, 0, 7, 0) | 0;
    c.joined = true; dirty = true;
    send(c, { t: "hello", id: c.id, online: clients.size, booths: boothSnapshot() });
  } else if (!c.joined) {
    return;
  } else if (o.t === "pos") {
    c.x = num(o.x, 0, WORLD_W, c.x);
    c.y = num(o.y, 0, WORLD_H, c.y);
    c.f = o.f === -1 ? -1 : 1;
    c.w = o.w ? 1 : 0;
    c.sit = Number.isInteger(o.sit) ? o.sit : -1;
    dirty = true;
  } else if (o.t === "emote") {
    const now = Date.now();
    if (now - c.lastEmote < EMOTE_GAP_MS) return;
    const idx = Number.isInteger(o.idx) ? o.idx : -1;
    if (idx < 0 || idx >= EMOTES.length) return;
    c.lastEmote = now;
    broadcast({ t: "emote", id: c.id, e: EMOTES[idx] });
  } else if (o.t === "say") {
    const now = Date.now();
    if (now < c.mutedUntil) return send(c, { t: "warn", m: "잠시 쉬어가요. 조금 뒤에 다시 말할 수 있어요." });
    c.sayTimes = c.sayTimes.filter((t) => now - t < SPAM_WINDOW_MS); c.sayTimes.push(now);
    if (c.sayTimes.length > SPAM_LIMIT) { c.mutedUntil = now + MUTE_MS; return send(c, { t: "warn", m: "너무 빨라요. 30초 동안 쉬어가요." }); }
    if (now - c.lastSay < SAY_GAP_MS) return;
    const text = clean(o.m, 40);
    if (!text) return;
    if (/https?:|www\.|\.(com|net|kr|io|me)\b/i.test(text) || maskBad(text) === null)
      return send(c, { t: "warn", m: "보낼 수 없는 말이에요." });
    c.lastSay = now;
    broadcast({ t: "say", id: c.id, m: text });
  } else if (o.t === "booth_toggle") {
    const b = booths[String(o.id || "")];
    if (!b || Math.abs(c.x - b.x) > 120) return;
    if (!b.ownerId) {
      releaseBoothsBy(c.id);
      b.ownerId = c.id;
      b.ownerName = c.n;
      broadcast({ t: "booths", booths: boothSnapshot() });
    } else if (b.ownerId === c.id) {
      b.ownerId = null;
      b.ownerName = null;
      broadcast({ t: "booths", booths: boothSnapshot() });
    }
  } else if (o.t === "booth_msg") {
    const b = booths[String(o.id || "")];
    if (!b || !b.ownerId) return;
    const text = clean(o.m, 120);
    if (!text) return;
    if (/https?:|www\.|\.(com|net|kr|io|me)\b/i.test(text) || maskBad(text) === null)
      return send(c, { t: "warn", m: "보낼 수 없는 말이에요." });
    const payload = { t: "booth_msg", boothId: b.id, fromId: c.id, fromName: c.n, m: text };
    send(c, payload);
    const ownerClient = clients.get(b.ownerId);
    if (ownerClient && ownerClient.id !== c.id) send(ownerClient, payload);
    for (const other of clients.values()) {
      if (other.joined && other.id !== c.id && other.id !== b.ownerId && Math.abs(other.x - b.x) < 160) {
        send(other, payload);
      }
    }
  }
}

setInterval(() => {
  if (!dirty) return;
  dirty = false;
  const p = [];
  for (const c of clients.values()) if (c.joined) p.push([c.id, Math.round(c.x), Math.round(c.y), c.f, c.w, c.n, c.col, c.sit]);
  broadcast({ t: "s", p });
}, 66);

setInterval(() => {
  for (const c of clients.values()) {
    if (!c.alive) { drop(c, 1001); continue; }
    c.alive = false;
    try { c.socket.write(frame(0x9, Buffer.alloc(0))); } catch (e) { drop(c); }
  }
}, 30000);

if (require.main === module) {
  server.listen(PORT, "0.0.0.0", () => console.log("챕터 0 v2.3 서버 실행 중 → 포트 " + PORT));
}
module.exports = { server };
