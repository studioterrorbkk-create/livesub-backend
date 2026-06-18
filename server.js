import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { createClient, LiveTranscriptionEvents } from "@deepgram/sdk";
import fetch from "node-fetch";
import admin from "firebase-admin";
import cors from "cors";

// ── CONFIG ──
const PORT         = process.env.PORT || 3001;
const DEEPGRAM_KEY = process.env.DEEPGRAM_KEY;
const ANTHROPIC_KEY= process.env.ANTHROPIC_KEY;
const FIREBASE_URL = process.env.FIREBASE_URL;
const GATE_PASS    = process.env.GATE_PASS;

// ── FIREBASE INIT ──
if (!admin.apps.length) {
  admin.initializeApp({
    databaseURL: FIREBASE_URL,
    credential: admin.credential.applicationDefault().catch
      ? admin.credential.cert({
          projectId: "liquidmetalpublishing-b0178",
          clientEmail: "firebase-adminsdk@liquidmetalpublishing-b0178.iam.gserviceaccount.com",
          privateKey: "placeholder"
        })
      : admin.credential.applicationDefault()
  });
}

// Use REST API for Firebase instead of admin SDK to avoid cert issues
async function firebaseSet(path, data) {
  try {
    await fetch(`${FIREBASE_URL}/${path}.json`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(data)
    });
  } catch(e) {
    console.error("Firebase write error:", e.message);
  }
}

async function firebaseGet(path) {
  try {
    const res = await fetch(`${FIREBASE_URL}/${path}.json`);
    return await res.json();
  } catch(e) {
    return null;
  }
}

// ── EXPRESS APP ──
const app = express();
app.use(cors({ origin: "*" }));
app.use(express.json());

app.get("/", (req, res) => res.json({ status: "LiveSub backend running", version: "2.0" }));
app.get("/health", (req, res) => res.json({ ok: true }));

const server = createServer(app);

// ── WEBSOCKET SERVER ──
const wss = new WebSocketServer({ server });

// Track active sessions: roomCode -> { deepgramLive, translateQueue, lastThai, lastEng }
const sessions = new Map();

// Translation queue — prevents parallel calls, batches rapid words
class TranslateQueue {
  constructor(roomCode) {
    this.roomCode = roomCode;
    this.pending = null;
    this.timer = null;
    this.running = false;
    this.lastTranslated = "";
  }

  // Schedule translation — debounce 600ms, translate immediately on final
  schedule(text, isFinal) {
    clearTimeout(this.timer);
    this.pending = text;

    if (isFinal) {
      this._run(text, true);
    } else {
      this.timer = setTimeout(() => this._run(text, false), 600);
    }
  }

  async _run(text, isFinal) {
    if (!text || text === this.lastTranslated) return;
    if (this.running && !isFinal) return; // skip interim if already translating

    this.running = true;
    this.lastTranslated = text;

    try {
      const eng = await translateWithClaude(text);
      const session = sessions.get(this.roomCode);
      if (session) {
        session.lastEng = eng;
        // Push to Firebase
        await firebaseSet(`rooms/${this.roomCode}/current`, {
          thai: text,
          eng: eng,
          isFinal,
          ts: Date.now()
        });
        // Broadcast to all connected WS clients for this room
        broadcastToRoom(this.roomCode, {
          type: "subtitle",
          thai: text,
          eng: eng,
          isFinal
        });
      }
    } catch(e) {
      console.error("Translation error:", e.message);
    }

    this.running = false;
    // If new pending arrived while we were running, process it
    if (this.pending && this.pending !== text) {
      const next = this.pending;
      this.pending = null;
      this._run(next, false);
    }
  }
}

// ── CLAUDE TRANSLATION ──
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
      system: "You are a real-time live event subtitle translator. The MC speaks Thai at events, concerts, and shows. Translate Thai speech to natural English subtitles. Handle Thai slang, casual speech, and MC/event language naturally. Keep translations concise and natural — exactly like professional live subtitles. Return ONLY the English translation, nothing else.",
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

// ── DEEPGRAM STREAMING ──
function startDeepgramSession(roomCode, ws) {
  const deepgram = createClient(DEEPGRAM_KEY);
  const queue = new TranslateQueue(roomCode);

  const live = deepgram.listen.live({
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
    console.log(`[${roomCode}] Deepgram connected`);
    ws.send(JSON.stringify({ type: "deepgram_ready" }));
  });

  live.on(LiveTranscriptionEvents.Transcript, async (data) => {
    const alt = data.channel?.alternatives?.[0];
    if (!alt || !alt.transcript.trim()) return;

    const thai = alt.transcript.trim();
    const isFinal = data.is_final;

    console.log(`[${roomCode}] ${isFinal ? "FINAL" : "interim"}: ${thai}`);

    // Update session
    const session = sessions.get(roomCode);
    if (session) session.lastThai = thai;

    // Send Thai immediately to host and audience (no waiting)
    broadcastToRoom(roomCode, {
      type: "thai_interim",
      thai,
      isFinal
    });

    // Also update Firebase with Thai immediately so audience sees it fast
    await firebaseSet(`rooms/${roomCode}/current`, {
      thai,
      eng: session?.lastEng || "...",
      isFinal: false,
      ts: Date.now()
    });

    // Queue translation
    queue.schedule(thai, isFinal);
  });

  live.on(LiveTranscriptionEvents.UtteranceEnd, async (data) => {
    // Force translate any pending interim when utterance ends
    const session = sessions.get(roomCode);
    if (session?.lastThai) {
      queue.schedule(session.lastThai, true);
    }
  });

  live.on(LiveTranscriptionEvents.Error, (err) => {
    console.error(`[${roomCode}] Deepgram error:`, err);
    ws.send(JSON.stringify({ type: "error", message: "Speech recognition error" }));
  });

  live.on(LiveTranscriptionEvents.Close, () => {
    console.log(`[${roomCode}] Deepgram closed`);
  });

  return { live, queue };
}

// ── ROOM BROADCAST ──
const roomClients = new Map(); // roomCode -> Set of WebSocket

function broadcastToRoom(roomCode, data) {
  const clients = roomClients.get(roomCode);
  if (!clients) return;
  const msg = JSON.stringify(data);
  clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  });
}

// ── WEBSOCKET HANDLER ──
wss.on("connection", (ws) => {
  let roomCode = null;
  let role = null; // "host" or "audience"

  ws.on("message", async (raw) => {
    // Check if this is binary audio data
    if (raw instanceof Buffer && roomCode && role === "host") {
      const session = sessions.get(roomCode);
      if (session?.live?.getReadyState() === 1) {
        session.live.send(raw);
      }
      return;
    }

    // Parse JSON message
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch(e) { return; }

    switch(msg.type) {

      case "host_join": {
        if (msg.password !== GATE_PASS) {
          ws.send(JSON.stringify({ type: "error", message: "Wrong password" }));
          ws.close();
          return;
        }
        roomCode = msg.roomCode;
        role = "host";

        // Add to room clients
        if (!roomClients.has(roomCode)) roomClients.set(roomCode, new Set());
        roomClients.get(roomCode).add(ws);

        // Init session
        sessions.set(roomCode, { live: null, queue: null, lastThai: "", lastEng: "" });

        // Set room as waiting in Firebase
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
          ws.close();
          return;
        }
        roomCode = msg.roomCode;
        role = "audience";
        if (!roomClients.has(roomCode)) roomClients.set(roomCode, new Set());
        roomClients.get(roomCode).add(ws);

        // Register audience in Firebase
        await firebaseSet(`rooms/${roomCode}/audience/aud_${Date.now()}`, { joined: Date.now() });

        // Send current room state
        const roomData = await firebaseGet(`rooms/${roomCode}`);
        ws.send(JSON.stringify({ type: "room_state", room: roomData }));
        console.log(`[${roomCode}] Audience joined`);
        break;
      }

      case "start_listening": {
        const session = sessions.get(roomCode);
        if (!session) break;

        // Start Deepgram
        const { live, queue } = startDeepgramSession(roomCode, ws);
        session.live = live;
        session.queue = queue;

        // Set room as live in Firebase
        await firebaseSet(`rooms/${roomCode}/status`, "live");
        broadcastToRoom(roomCode, { type: "status", status: "live" });
        break;
      }

      case "stop_listening": {
        const session = sessions.get(roomCode);
        if (session?.live) {
          session.live.finish();
          session.live = null;
        }
        break;
      }

      case "go_live": {
        await firebaseSet(`rooms/${roomCode}/status`, "live");
        if (msg.design) await firebaseSet(`rooms/${roomCode}/design`, msg.design);
        broadcastToRoom(roomCode, { type: "status", status: "live" });
        break;
      }

      case "update_design": {
        if (roomCode) {
          await firebaseSet(`rooms/${roomCode}/design`, msg.design);
          broadcastToRoom(roomCode, { type: "design_update", design: msg.design });
        }
        break;
      }

      case "end_session": {
        const session = sessions.get(roomCode);
        if (session?.live) { session.live.finish(); }
        await firebaseSet(`rooms/${roomCode}/status`, "ended");
        broadcastToRoom(roomCode, { type: "status", status: "ended" });
        sessions.delete(roomCode);
        break;
      }
    }
  });

  ws.on("close", () => {
    if (roomCode) {
      const clients = roomClients.get(roomCode);
      if (clients) {
        clients.delete(ws);
        if (clients.size === 0) roomClients.delete(roomCode);
      }
      // If host disconnected, stop Deepgram
      if (role === "host") {
        const session = sessions.get(roomCode);
        if (session?.live) session.live.finish();
      }
    }
  });

  ws.on("error", (err) => console.error("WS error:", err.message));
});

// ── START ──
server.listen(PORT, () => {
  console.log(`LiveSub backend running on port ${PORT}`);
});
