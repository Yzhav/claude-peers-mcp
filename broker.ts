#!/usr/bin/env bun
/**
 * claude-peers broker daemon
 *
 * A singleton HTTP server on localhost:7899 backed by SQLite.
 * Tracks all registered Claude Code peers and routes messages between them.
 *
 * Auto-launched by the MCP server if not already running.
 * Run directly: bun broker.ts
 */

import { Database } from "bun:sqlite";
import type {
  RegisterRequest,
  RegisterResponse,
  HeartbeatRequest,
  SetSummaryRequest,
  ListPeersRequest,
  SendMessageRequest,
  PollMessagesRequest,
  PollMessagesResponse,
  Peer,
  Message,
} from "./shared/types.ts";

const PORT = parseInt(process.env.CLAUDE_PEERS_PORT ?? "7899", 10);
const HOME_DIR = process.env.HOME ?? process.env.USERPROFILE ?? ".";
const DB_PATH = process.env.CLAUDE_PEERS_DB ?? `${HOME_DIR}/.claude-peers.db`;

// --- Database setup ---

const db = new Database(DB_PATH);
db.run("PRAGMA journal_mode = WAL");
db.run("PRAGMA busy_timeout = 3000");

db.run(`
  CREATE TABLE IF NOT EXISTS peers (
    id TEXT PRIMARY KEY,
    pid INTEGER NOT NULL,
    ppid INTEGER NOT NULL DEFAULT 0,
    cwd TEXT NOT NULL,
    git_root TEXT,
    tty TEXT,
    summary TEXT NOT NULL DEFAULT '',
    registered_at TEXT NOT NULL,
    last_seen TEXT NOT NULL
  )
`);

// Migrate existing databases: add ppid column if missing
try {
  db.run("ALTER TABLE peers ADD COLUMN ppid INTEGER NOT NULL DEFAULT 0");
} catch {
  // Column already exists — ignore
}

db.run(`
  CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    from_id TEXT NOT NULL,
    to_id TEXT NOT NULL,
    text TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    delivered INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY (from_id) REFERENCES peers(id),
    FOREIGN KEY (to_id) REFERENCES peers(id)
  )
`);

// Cross-platform process existence check
function isProcessAlive(pid: number): boolean {
  if (process.platform === "win32") {
    try {
      const proc = Bun.spawnSync(["tasklist", "/FI", `PID eq ${pid}`, "/NH", "/FO", "CSV"]);
      const output = new TextDecoder().decode(proc.stdout);
      return output.includes(`"${pid}"`);
    } catch {
      return false;
    }
  }
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

// Stale timeout: if no heartbeat for 30s (2x heartbeat interval), consider dead
const STALE_TIMEOUT_MS = 30_000;

// Auto-shutdown: if no peers for this long, broker exits itself
const IDLE_SHUTDOWN_MS = 5 * 60_000; // 5 minutes
let lastPeerSeenAt = Date.now();

// Old message cleanup threshold
const MESSAGE_RETENTION_MS = 60 * 60_000; // 1 hour

// Clean up stale peers: dead PIDs, orphaned processes, or heartbeat timeout
function cleanStalePeers() {
  const peers = db.query("SELECT id, pid, ppid, last_seen FROM peers").all() as {
    id: string;
    pid: number;
    ppid: number;
    last_seen: string;
  }[];
  const now = Date.now();

  for (const peer of peers) {
    const lastSeenAge = now - new Date(peer.last_seen).getTime();
    const pidAlive = isProcessAlive(peer.pid);
    // ppid=0 means legacy registration (no ppid tracked) — skip ppid check
    const ppidAlive = peer.ppid === 0 || isProcessAlive(peer.ppid);

    if (!pidAlive || !ppidAlive || lastSeenAge > STALE_TIMEOUT_MS) {
      db.run("DELETE FROM peers WHERE id = ?", [peer.id]);
      db.run("DELETE FROM messages WHERE to_id = ? AND delivered = 0", [peer.id]);
    }
  }

  // Clean up old delivered messages (prevent DB bloat)
  const cutoff = new Date(now - MESSAGE_RETENTION_MS).toISOString();
  db.run("DELETE FROM messages WHERE delivered = 1 AND sent_at < ?", [cutoff]);

  // Track idle state for auto-shutdown
  const remainingPeers = (db.query("SELECT COUNT(*) as cnt FROM peers").get() as { cnt: number }).cnt;
  if (remainingPeers > 0) {
    lastPeerSeenAt = now;
  } else if (now - lastPeerSeenAt > IDLE_SHUTDOWN_MS) {
    console.error("[claude-peers broker] No peers for 5 minutes — shutting down");
    db.close();
    process.exit(0);
  }
}

cleanStalePeers();

// Periodically clean stale peers (every 15s for faster orphan detection)
setInterval(cleanStalePeers, 15_000);

// --- Prepared statements ---

const insertPeer = db.prepare(`
  INSERT INTO peers (id, pid, ppid, cwd, git_root, tty, summary, registered_at, last_seen)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const updateLastSeen = db.prepare(`
  UPDATE peers SET last_seen = ? WHERE id = ?
`);

const updateSummary = db.prepare(`
  UPDATE peers SET summary = ? WHERE id = ?
`);

const deletePeer = db.prepare(`
  DELETE FROM peers WHERE id = ?
`);

const selectAllPeers = db.prepare(`
  SELECT * FROM peers
`);

const selectPeersByDirectory = db.prepare(`
  SELECT * FROM peers WHERE cwd = ?
`);

const selectPeersByGitRoot = db.prepare(`
  SELECT * FROM peers WHERE git_root = ?
`);

const insertMessage = db.prepare(`
  INSERT INTO messages (from_id, to_id, text, sent_at, delivered)
  VALUES (?, ?, ?, ?, 0)
`);

const selectUndelivered = db.prepare(`
  SELECT * FROM messages WHERE to_id = ? AND delivered = 0 ORDER BY sent_at ASC
`);

const markDelivered = db.prepare(`
  UPDATE messages SET delivered = 1 WHERE id = ?
`);

// --- Generate peer ID ---

function generateId(): string {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let id = "";
  for (let i = 0; i < 8; i++) {
    id += chars[Math.floor(Math.random() * chars.length)];
  }
  return id;
}

// --- Request handlers ---

function handleRegister(body: RegisterRequest): RegisterResponse {
  const id = generateId();
  const now = new Date().toISOString();
  const ppid = body.ppid ?? 0;

  // Remove any existing registration for this PID (re-registration)
  const existingByPid = db.query("SELECT id FROM peers WHERE pid = ?").get(body.pid) as { id: string } | null;
  if (existingByPid) {
    deletePeer.run(existingByPid.id);
  }

  // Remove any existing registration for this PPID (same Claude Code session, duplicate MCP)
  if (ppid !== 0) {
    const existingByPpid = db.query("SELECT id FROM peers WHERE ppid = ?").get(ppid) as { id: string } | null;
    if (existingByPpid) {
      deletePeer.run(existingByPpid.id);
    }
  }

  insertPeer.run(id, body.pid, ppid, body.cwd, body.git_root, body.tty, body.summary, now, now);
  return { id };
}

function handleHeartbeat(body: HeartbeatRequest): void {
  updateLastSeen.run(new Date().toISOString(), body.id);
}

function handleSetSummary(body: SetSummaryRequest): void {
  updateSummary.run(body.summary, body.id);
}

function handleListPeers(body: ListPeersRequest): Peer[] {
  let peers: Peer[];

  switch (body.scope) {
    case "machine":
      peers = selectAllPeers.all() as Peer[];
      break;
    case "directory":
      peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      break;
    case "repo":
      if (body.git_root) {
        peers = selectPeersByGitRoot.all(body.git_root) as Peer[];
      } else {
        // No git root, fall back to directory
        peers = selectPeersByDirectory.all(body.cwd) as Peer[];
      }
      break;
    default:
      peers = selectAllPeers.all() as Peer[];
  }

  // Exclude the requesting peer
  if (body.exclude_id) {
    peers = peers.filter((p) => p.id !== body.exclude_id);
  }

  // Verify each peer's process is still alive (pid + ppid check)
  return peers.filter((p) => {
    const pidAlive = isProcessAlive(p.pid);
    const ppidAlive = p.ppid === 0 || isProcessAlive(p.ppid);
    if (pidAlive && ppidAlive) {
      return true;
    }
    // Clean up dead/orphaned peer
    deletePeer.run(p.id);
    return false;
  });
}

function handleSendMessage(body: SendMessageRequest): { ok: boolean; error?: string } {
  // Verify target exists
  const target = db.query("SELECT id FROM peers WHERE id = ?").get(body.to_id) as { id: string } | null;
  if (!target) {
    return { ok: false, error: `Peer ${body.to_id} not found` };
  }

  insertMessage.run(body.from_id, body.to_id, body.text, new Date().toISOString());
  return { ok: true };
}

function handlePollMessages(body: PollMessagesRequest): PollMessagesResponse {
  const messages = selectUndelivered.all(body.id) as Message[];

  // Mark them as delivered
  for (const msg of messages) {
    markDelivered.run(msg.id);
  }

  return { messages };
}

function handleUnregister(body: { id: string }): void {
  deletePeer.run(body.id);
}

// --- HTTP Server ---

Bun.serve({
  port: PORT,
  hostname: "127.0.0.1",
  async fetch(req) {
    const url = new URL(req.url);
    const path = url.pathname;

    if (req.method !== "POST") {
      if (path === "/health") {
        return Response.json({ status: "ok", peers: (selectAllPeers.all() as Peer[]).length });
      }
      return new Response("claude-peers broker", { status: 200 });
    }

    try {
      const body = await req.json();

      switch (path) {
        case "/register":
          return Response.json(handleRegister(body as RegisterRequest));
        case "/heartbeat":
          handleHeartbeat(body as HeartbeatRequest);
          return Response.json({ ok: true });
        case "/set-summary":
          handleSetSummary(body as SetSummaryRequest);
          return Response.json({ ok: true });
        case "/list-peers":
          return Response.json(handleListPeers(body as ListPeersRequest));
        case "/send-message":
          return Response.json(handleSendMessage(body as SendMessageRequest));
        case "/poll-messages":
          return Response.json(handlePollMessages(body as PollMessagesRequest));
        case "/unregister":
          handleUnregister(body as { id: string });
          return Response.json({ ok: true });
        default:
          return Response.json({ error: "not found" }, { status: 404 });
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      return Response.json({ error: msg }, { status: 500 });
    }
  },
});

console.error(`[claude-peers broker] listening on 127.0.0.1:${PORT} (db: ${DB_PATH})`);
