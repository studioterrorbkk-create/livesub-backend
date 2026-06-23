import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { createClient, LiveTranscriptionEvents } from "@deepgram/sdk";
import fetch from "node-fetch";
import cors from "cors";

const PORT         = process.env.PORT || 3001;
const DEEPGRAM_KEY = process.env.DEEPGRAM_KEY;
const ANTHROPIC_KEY= process.env.ANTHROPIC_KEY;
const FIREBASE_URL = process.env.FIREBASE_URL;
const GATE_PASS    = process.env.GATE_PASS || "kingcobra";

console.log("=== LiveSub Backend Starting ===");
console.log("Deepgram key present:", !!DEEPGRAM_KEY, "length:", DEEPGRAM_KEY?.length);
console.log("Anthropic key present:", !!ANTHROPIC_KEY);
console.log("Firebase URL:", FIREBASE_URL);

// ── FIREBASE (REST) ──
async function firebaseSet(path, data) {
  if (!FIREBASE_URL) return;
  try {
    await fetch(`${FIREBASE_URL}/${path}.json`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data)
    });
  } catch(e) { console.error("Firebase error:", e.message); }
}

async function firebaseGet(path) {
  if (!FIREBASE_URL) return null;
  try {
    const res = await fetch(`${FIREBASE_URL}/${path}.json`);
    return await res.json();
  } catch(e) { return null; }
}

// ── TRANSLATION ──
async function translateWithClaude(thai) {
  const res = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "x-api-key": ANTHROPIC_KEY,
      "anthropic-version": "2023-06-01"
    },
    body: JSON.stringify({
      model: "claude-haiku-4-5-20251001",
      max_tokens: 200,
      system: "You are a live event subtitle translator. The MC speaks Thai at events. Translate Thai speech to natural English subtitles. Handle slang naturally. Return ONLY the English translation, nothing else.",
      messages: [{ role: "user", content: thai }]
    })
  });
  if (!res.ok) {
    const err = await res.json();
    throw new Error(err.error?.message || `HTTP ${res.status}`);
  }
  const data = await res.json();
  return data.content[0].text.trim();
}

// ── EXPRESS ──
const app = express();
app.use(cors({ origin: "*" }));
app.use(express.json());
app.get("/", (req, res) => res.json({ status: "LiveSub v3 running", ts: Date.now() }));
app.get("/health", (req, res) => res.json({ ok: true }));
const server = createServer(app);

// ── ROOM STATE ──
// roomCode -> { ws (host), deepgramLive, lastThai, lastEng, isTranslating }
const rooms = new Map();
// roomCode -> Set<ws> (all clients including audience)
const roomClients = new Map();

function broadcast(roomCode, data) {
  const clients = roomClients.get(roomCode);
  if (!clients) return;
  const msg = JSON.stringify(data);
  clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(msg); });
}

// ── DEEPGRAM ──
function startDeepgram(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) { console.log(`[${roomCode}] Cannot start Deepgram - no room found`); return; }

  console.log(`[${roomCode}] Starting Deepgram... key: ${DEEPGRAM_KEY?.substring(0,8)}...`);

  const dg = createClient(DEEPGRAM_KEY);
  const live = dg.listen.live({
    language: "th",
    model: "nova-2",
    smart_format: true,
    interim_results: true,
    utterance_end_ms: 1000,
    vad_events: true,
    encoding: "linear16",
    sample_rate: 16000,
    channels: 1
  });

  live.on(LiveTranscriptionEvents.Open, () => {
    console.log(`[${roomCode}] Deepgram OPEN`);
    room.deepgramLive = live;
    // Notify host
    if (room.ws && room.ws.readyState === WebSocket.OPEN) {
      room.ws.send(JSON.stringify({ type: "deepgram_ready" }));
    }
    broadcast(roomCode, { type: "status", status: "live" });
  });

  live.on(LiveTranscriptionEvents.Transcript, async (data) => {
    const alt = data.channel?.alternatives?.[0];
    if (!alt || !alt.transcript.trim()) return;

    const thai = alt.transcript.trim();
    const isFinal = data.is_final;

    console.log(`[${roomCode}] ${isFinal ? "FINAL" : "interim"}: ${thai}`);
    room.lastThai = thai;

    // Push Thai immediately
    broadcast(roomCode, { type: "thai_interim", thai, isFinal });
    await firebaseSet(`rooms/${roomCode}/current`, { thai, eng: room.lastEng || "...", ts: Date.now() });

    // Translate final results
    if (isFinal && thai && !room.isTranslating) {
      room.isTranslating = true;
      try {
        const eng = await translateWithClaude(thai);
        room.lastEng = eng;
        room.isTranslating = false;
        console.log(`[${roomCode}] Translation: ${eng}`);
        broadcast(roomCode, { type: "subtitle", thai, eng, isFinal: true });
        await firebaseSet(`rooms/${roomCode}/current`, { thai, eng, ts: Date.now() });
      } catch(e) {
        room.isTranslating = false;
        console.error(`[${roomCode}] Translation error:`, e.message);
        broadcast(roomCode, { type: "subtitle", thai, eng: `[Error: ${e.message}]`, isFinal: true });
      }
    }
  });

  live.on(LiveTranscriptionEvents.UtteranceEnd, async () => {
    if (room.lastThai && !room.isTranslating) {
      room.isTranslating = true;
      try {
        const eng = await translateWithClaude(room.lastThai);
        room.lastEng = eng;
        room.isTranslating = false;
        broadcast(roomCode, { type: "subtitle", thai: room.lastThai, eng, isFinal: true });
        await firebaseSet(`rooms/${roomCode}/current`, { thai: room.lastThai, eng, ts: Date.now() });
      } catch(e) {
        room.isTranslating = false;
      }
    }
  });

  live.on(LiveTranscriptionEvents.Error, (err) => {
    console.error(`[${roomCode}] Deepgram error:`, err);
    room.deepgramLive = null;
    if (room.ws && room.ws.readyState === WebSocket.OPEN) {
      room.ws.send(JSON.stringify({ type: "error", message: "Speech recognition error: " + err }));
    }
  });

  live.on(LiveTranscriptionEvents.Close, () => {
    console.log(`[${roomCode}] Deepgram closed`);
    room.deepgramLive = null;
  });

  // Don't set room.deepgramLive here — set it in the Open event
  return live;
}

// ── WEBSOCKET ──
const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  let roomCode = null;
  let role = null;
  console.log("New WS connection");

  ws.on("message", async (raw) => {
    // Binary = audio data from host mic
    if (Buffer.isBuffer(raw) || raw instanceof ArrayBuffer) {
      if (!roomCode || role !== "host") return;
      const room = rooms.get(roomCode);
      if (!room) return;
      if (room.deepgramLive && room.deepgramLive.getReadyState() === 1) {
        room.deepgramLive.send(raw);
      }
      // Don't log audio packets — too noisy
      return;
    }

    // JSON message
    let rawStr = raw.toString();
    console.log(`[WS RAW] first100chars: ${rawStr.substring(0,100)} | isBuffer: ${Buffer.isBuffer(raw)} | type: ${typeof raw}`);
    let msg;
    try { msg = JSON.parse(rawStr); }
    catch(e) { console.error("JSON parse error:", e.message, "raw:", rawStr.substring(0,50)); return; }

    console.log(`[WS] type=${msg.type} room=${roomCode} role=${role}`);

    switch(msg.type) {

      case "host_join": {
        if (msg.password !== GATE_PASS) {
          ws.send(JSON.stringify({ type: "error", message: "Wrong password" }));
          ws.close(); return;
        }
        roomCode = msg.roomCode;
        role = "host";

        // Create room entry
        rooms.set(roomCode, { ws, deepgramLive: null, lastThai: "", lastEng: "", isTranslating: false });
        if (!roomClients.has(roomCode)) roomClients.set(roomCode, new Set());
        roomClients.get(roomCode).add(ws);

        // Firebase
        await firebaseSet(`rooms/${roomCode}`, {
          status: "waiting",
          design: msg.design || {},
          createdAt: Date.now()
        });

        ws.send(JSON.stringify({ type: "host_joined", roomCode }));
        console.log(`[${roomCode}] Host joined`);
        break;
      }

      case "audience_join": {
        if (msg.password !== GATE_PASS) {
          ws.send(JSON.stringify({ type: "error", message: "Wrong password" }));
          ws.close(); return;
        }
        roomCode = msg.roomCode;
        role = "audience";
        if (!roomClients.has(roomCode)) roomClients.set(roomCode, new Set());
        roomClients.get(roomCode).add(ws);

        await firebaseSet(`rooms/${roomCode}/audience/aud_${Date.now()}`, { joined: Date.now() });

        // Send current room state
        const roomData = await firebaseGet(`rooms/${roomCode}`);
        ws.send(JSON.stringify({ type: "room_state", room: roomData }));
        console.log(`[${roomCode}] Audience joined`);
        break;
      }

      case "start_listening": {
        console.log(`[${roomCode}] start_listening received`);
        const room = rooms.get(roomCode);
        if (!room) {
          console.log(`[${roomCode}] ERROR: room not found! All rooms: ${[...rooms.keys()].join(", ")}`);
          ws.send(JSON.stringify({ type: "error", message: "Room not found. Please refresh and try again." }));
          break;
        }
        // Stop existing Deepgram if any
        if (room.deepgramLive) {
          try { room.deepgramLive.finish(); } catch(e) {}
          room.deepgramLive = null;
        }
        startDeepgram(roomCode);
        break;
      }

      case "stop_listening": {
        console.log(`[${roomCode}] stop_listening received`);
        const room = rooms.get(roomCode);
        if (room?.deepgramLive) {
          try { room.deepgramLive.finish(); } catch(e) {}
          room.deepgramLive = null;
        }
        break;
      }

      case "go_live": {
        console.log(`[${roomCode}] go_live received`);
        await firebaseSet(`rooms/${roomCode}/status`, "live");
        if (msg.design) await firebaseSet(`rooms/${roomCode}/design`, msg.design);
        broadcast(roomCode, { type: "status", status: "live" });
        break;
      }

      case "update_design": {
        if (roomCode && msg.design) {
          await firebaseSet(`rooms/${roomCode}/design`, msg.design);
          broadcast(roomCode, { type: "design_update", design: msg.design });
        }
        break;
      }

      case "end_session": {
        console.log(`[${roomCode}] end_session received`);
        const room = rooms.get(roomCode);
        if (room?.deepgramLive) {
          try { room.deepgramLive.finish(); } catch(e) {}
        }
        await firebaseSet(`rooms/${roomCode}/status`, "ended");
        broadcast(roomCode, { type: "status", status: "ended" });
        rooms.delete(roomCode);
        break;
      }

      default:
        console.log(`[WS] Unknown message type: ${msg.type}`);
    }
  });

  ws.on("close", () => {
    console.log(`WS closed. room=${roomCode} role=${role}`);
    if (roomCode) {
      const clients = roomClients.get(roomCode);
      if (clients) { clients.delete(ws); if (clients.size === 0) roomClients.delete(roomCode); }
      if (role === "host") {
        const room = rooms.get(roomCode);
        if (room?.deepgramLive) { try { room.deepgramLive.finish(); } catch(e) {} }
      }
    }
  });

  ws.on("error", (e) => console.error("WS error:", e.message));
});

server.listen(PORT, () => {
  console.log(`LiveSub backend running on port ${PORT}`);
});
