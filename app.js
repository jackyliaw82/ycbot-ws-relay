/**
 * ycbot-ws-relay — WebSocket relay between vm-bots and Binance fstream.
 *
 * Architecture (v1.1.0+, combined-streams):
 *  - One persistent combined-streams upstream WebSocket per relay process to
 *    `wss://fstream.binance.com/stream`. Streams are subscribed/unsubscribed
 *    dynamically via JSON SUBSCRIBE / UNSUBSCRIBE messages on that single
 *    connection. This matches Binance's documented best practice
 *    ("Combined streams are an efficient way to subscribe to multiple data
 *    streams through a single WebSocket connection") and stays well below
 *    the 300-connections-per-IP-per-5-min cap.
 *  - User-data WS (per-user listenKey) is still 1:1 — each user gets their own
 *    upstream — but routed via the relay IP to keep user-VM IPs out of
 *    Binance's view.
 *  - Bots fan out via the relay: many bots subscribed to the same stream share
 *    one upstream subscription.
 *
 * Connection-management posture:
 *  - Upstream stays alive for 23.5h, then pre-emptively reconnects to dodge
 *    Binance's 24h forced-disconnect cliff.
 *  - On close, exponential backoff: 1s → 2s → 4s → 8s → 16s → 32s → 60s →
 *    2m → 5m → 30m, capped. Reset on a productive reconnect (first DATA frame).
 *  - Hard rate cap: max 30 connect attempts per 5 minutes (10× safety margin
 *    under Binance's documented 300/5min cap).
 *  - Server pings every 30s; auto-pong handled by `ws` library.
 *
 * URL format (unchanged from v1.0.x — bot side requires no changes):
 *  - `ws://<relay-host>:<port>/ws/<stream>` where <stream> is either a Binance
 *    market stream (contains '@', e.g. "solusdt@markPrice@1s") or a user-data
 *    listenKey (no '@'). Fan-out happens server-side; bots see raw Binance
 *    event frames just like a direct connection.
 *
 * References:
 *  - https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams
 *  - https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams
 *  - https://www.binance.com/en/academy/articles/what-are-binance-websocket-limits
 */

import { WebSocketServer, WebSocket } from 'ws';
import http from 'http';
import { timingSafeEqual } from 'crypto';

const PORT = Number(process.env.PORT || 8080);

// Optional shared-secret auth for client connections. When set, clients must
// include `?token=<RELAY_AUTH_TOKEN>` on the connection URL or get 1008.
// When unset, the relay logs a security warning at startup and accepts all
// connections — useful for transition deploys, NOT recommended for production.
const RELAY_AUTH_TOKEN = process.env.RELAY_AUTH_TOKEN || '';

// Base URL for fstream — host only, no /ws or /stream suffix; we append the right
// one per use case. Override via env for testnet (e.g. wss://stream.binancefuture.com).
const BINANCE_WS_BASE_HOST = process.env.BINANCE_WS_BASE_HOST || 'wss://fstream.binance.com';
// Combined-streams endpoint: opens with no streams, manages subscriptions via JSON.
const COMBINED_STREAM_URL = `${BINANCE_WS_BASE_HOST}/stream`;
// Per-listenKey user-data endpoint pattern.
const userDataUpstreamUrl = (listenKey) => `${BINANCE_WS_BASE_HOST}/ws/${listenKey}`;

// Heartbeat
const PING_INTERVAL_MS = 30_000;

// Single combined-streams upstream lifecycle
const UPSTREAM_FIRST_MESSAGE_TIMEOUT_MS = 8_000;
const UPSTREAM_RECONNECT_BASE_MS = 1_000;
const UPSTREAM_RECONNECT_MAX_MS = 30 * 60 * 1000; // 30 min cap on exponential backoff
// Hard rate cap on connect attempts — 10× safety margin under Binance's documented
// 300/5min/IP cap.
const UPSTREAM_MAX_ATTEMPTS_PER_5MIN = 30;
const UPSTREAM_RATE_WINDOW_MS = 5 * 60 * 1000;
// Pre-emptive reconnect 30 min before Binance's 24h forced-disconnect.
const UPSTREAM_PREEMPTIVE_RECONNECT_MS = (24 * 60 - 30) * 60 * 1000; // 23h30m
// Stale-warning threshold for chatty streams. Doesn't trigger reconnect — just
// surfaces in logs in case Binance has paused a specific stream.
const CHATTY_STREAM_STALE_WARNING_MS = 60 * 1000;
const STALE_CHECK_INTERVAL_MS = 30 * 1000;

// Log level: 'debug' | 'info' | 'warn' | 'error'
const LOG_LEVEL = (process.env.LOG_LEVEL || 'info').toLowerCase();
const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };
function log(level, ...args) {
  if ((LEVELS[level] ?? 1) < (LEVELS[LOG_LEVEL] ?? 1)) return;
  const ts = new Date().toISOString();
  console.log(`${ts} [${level.toUpperCase()}]`, ...args);
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

function isListenKey(stream) {
  // Binance market streams always contain '@'. Listen keys are ~64-char alnum.
  return !stream.includes('@');
}

// Constant-time string compare. Falls back to false on length mismatch
// (timingSafeEqual throws on different-length buffers).
function timingSafeStringEqual(a, b) {
  const ab = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ab.length !== bb.length) return false;
  return timingSafeEqual(ab, bb);
}

// Extract `?token=...` from a request URL. Returns null if missing.
function extractTokenFromUrl(reqUrl) {
  if (!reqUrl) return null;
  const qIdx = reqUrl.indexOf('?');
  if (qIdx < 0) return null;
  const params = new URLSearchParams(reqUrl.slice(qIdx + 1));
  return params.get('token');
}

// "Chatty" streams that should be receiving messages frequently. Used only for
// stale-warning logging — not for auto-recycling, since Binance pausing a single
// stream doesn't mean the connection itself is broken.
function isChattyStream(stream) {
  return /(@markPrice|@ticker|@kline|@depth|@aggTrade)/i.test(stream);
}

// ─── Combined-streams market upstream ────────────────────────────────────────

const market = {
  ws: null,
  alive: false,
  openedAt: null,
  firstDataAt: null, // first DATA frame (envelope with `stream`), not subscribe-acks
  // Map<stream, Set<ClientWs>> — reverse index of who wants what
  streamSubscribers: new Map(),
  // Map<stream, lastMessageAtMs>
  streamLastMessageAt: new Map(),
  // Set<stream> — streams we've already warned about going stale (avoid spam)
  staleWarned: new Set(),
  // Sliding window of connect-attempt timestamps for the rate limiter
  recentAttemptTimestamps: [],
  reconnectAttempts: 0,
  reconnectTimeout: null,
  pingInterval: null,
  firstMessageWatchdog: null,
  preemptiveReconnectTimeout: null,
  nextRequestId: 1,
  pendingRequests: new Map(), // id → { method, streams, sentAt }
};

function nextBackoffMs(attempts) {
  // 1s, 2s, 4s, 8s, 16s, 32s, 60s, 2m, 5m, 30m, capped
  return Math.min(UPSTREAM_RECONNECT_MAX_MS, UPSTREAM_RECONNECT_BASE_MS * Math.pow(2, attempts - 1));
}

function applyRateLimit(baseDelay) {
  // Prune timestamps outside the 5-min window.
  const cutoff = Date.now() - UPSTREAM_RATE_WINDOW_MS;
  while (market.recentAttemptTimestamps.length && market.recentAttemptTimestamps[0] < cutoff) {
    market.recentAttemptTimestamps.shift();
  }
  if (market.recentAttemptTimestamps.length >= UPSTREAM_MAX_ATTEMPTS_PER_5MIN) {
    // At the cap — must wait until the oldest entry falls out of the window.
    const oldestExpiresAt = market.recentAttemptTimestamps[0] + UPSTREAM_RATE_WINDOW_MS;
    const requiredWait = oldestExpiresAt - Date.now() + 1_000; // +1s buffer
    const finalDelay = Math.max(baseDelay, requiredWait);
    log('warn', `connect rate cap hit (${market.recentAttemptTimestamps.length}/${UPSTREAM_MAX_ATTEMPTS_PER_5MIN} attempts in last 5min). Forcing wait of ${Math.round(finalDelay / 1000)}s.`);
    return finalDelay;
  }
  return baseDelay;
}

function hasAnySubscribers() {
  for (const subs of market.streamSubscribers.values()) {
    if (subs.size > 0) return true;
  }
  return false;
}

function streamsWithSubscribers() {
  const result = [];
  for (const [stream, subs] of market.streamSubscribers) {
    if (subs.size > 0) result.push(stream);
  }
  return result;
}

function connectMarketUpstream() {
  if (market.ws && market.ws.readyState !== WebSocket.CLOSED) {
    log('debug', 'connectMarketUpstream skipped: ws not closed');
    return;
  }
  if (!hasAnySubscribers()) {
    log('debug', 'connectMarketUpstream skipped: no subscribers');
    return;
  }
  if (market.reconnectTimeout) {
    clearTimeout(market.reconnectTimeout);
    market.reconnectTimeout = null;
  }

  market.recentAttemptTimestamps.push(Date.now());

  log('info', `market upstream connecting (attempt ${market.reconnectAttempts + 1}, ${streamsWithSubscribers().length} streams pending)`);
  const ws = new WebSocket(COMBINED_STREAM_URL);
  market.ws = ws;
  market.alive = true;
  market.openedAt = null;
  market.firstDataAt = null;

  ws.on('open', () => {
    market.openedAt = Date.now();
    log('info', `market upstream open`);

    // Re-subscribe everything that has active subscribers (handles reconnect cleanly).
    const toSubscribe = streamsWithSubscribers();
    if (toSubscribe.length > 0) {
      sendSubscribe(toSubscribe);
    }

    // Heartbeat — Binance also pings us every 3 min; the `ws` library auto-pongs.
    if (market.pingInterval) clearInterval(market.pingInterval);
    market.pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        try { ws.ping(); } catch (_) { /* ignore */ }
      }
    }, PING_INTERVAL_MS);

    // First-DATA-frame watchdog. Catches silent-stuck handshakes where Binance
    // accepts the connection but never pushes data (the throttle pattern).
    // Only fires once per fresh connection; subsequent silence is the bot's
    // problem to detect.
    if (market.firstMessageWatchdog) clearTimeout(market.firstMessageWatchdog);
    if (toSubscribe.length > 0) {
      market.firstMessageWatchdog = setTimeout(() => {
        if (!market.firstDataAt && ws.readyState === WebSocket.OPEN) {
          log('warn', `market upstream first-data timeout after ${UPSTREAM_FIRST_MESSAGE_TIMEOUT_MS}ms — closing for retry`);
          try { ws.close(4001, 'first-data timeout'); } catch (_) { /* ignore */ }
        }
      }, UPSTREAM_FIRST_MESSAGE_TIMEOUT_MS);
    }

    // Pre-emptive reconnect at 23h30m to avoid Binance's 24h forced-disconnect.
    if (market.preemptiveReconnectTimeout) clearTimeout(market.preemptiveReconnectTimeout);
    market.preemptiveReconnectTimeout = setTimeout(() => {
      log('info', `market upstream pre-emptive reconnect (23h30m elapsed)`);
      try { ws.close(1000, 'pre-emptive 24h refresh'); } catch (_) { /* ignore */ }
    }, UPSTREAM_PREEMPTIVE_RECONNECT_MS);
  });

  ws.on('message', (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      log('warn', `market upstream non-JSON message: ${e.message}`);
      return;
    }

    // Subscribe / Unsubscribe ack: { result: null, id: N }
    if (msg && typeof msg === 'object' && 'result' in msg && 'id' in msg) {
      const ctx = market.pendingRequests.get(msg.id);
      if (ctx) {
        log('info', `${ctx.method} ack id=${msg.id} streams=[${ctx.streams.join(', ')}]`);
        market.pendingRequests.delete(msg.id);
      } else {
        log('debug', `unexpected ack id=${msg.id}`);
      }
      return;
    }

    // Combined-stream data frame: { stream, data }
    if (msg && typeof msg === 'object' && msg.stream && msg.data) {
      const stream = msg.stream;
      market.streamLastMessageAt.set(stream, Date.now());
      market.staleWarned.delete(stream); // cleared — stream is healthy again

      // First DATA frame (not ack) — clear watchdog, reset backoff.
      if (!market.firstDataAt) {
        market.firstDataAt = Date.now();
        const latency = market.openedAt ? market.firstDataAt - market.openedAt : null;
        log('info', `market upstream first-data latency=${latency != null ? `${latency}ms` : 'unknown'}`);
        if (market.firstMessageWatchdog) {
          clearTimeout(market.firstMessageWatchdog);
          market.firstMessageWatchdog = null;
        }
        market.reconnectAttempts = 0;
      }

      // Fan out the UNWRAPPED data (bots expect raw Binance event frames, no envelope).
      const subs = market.streamSubscribers.get(stream);
      if (subs && subs.size > 0) {
        const unwrappedFrame = JSON.stringify(msg.data);
        for (const client of subs) {
          if (client.readyState === WebSocket.OPEN) {
            try { client.send(unwrappedFrame); } catch (_) { /* per-client send error — ignore */ }
          }
        }
      }
      return;
    }

    // Single-stream-shaped frame (only if BINANCE_WS_BASE_HOST is overridden to a
    // non-combined endpoint, or some Binance edge case). Nothing to fan out without
    // a stream identifier — log and drop.
    log('debug', `market upstream unrecognized frame shape: ${data.toString().slice(0, 200)}`);
  });

  ws.on('error', (err) => {
    log('warn', `market upstream error: ${err.message}`);
  });

  ws.on('close', (code, reason) => {
    log('info', `market upstream close: code=${code} reason=${reason || 'none'}`);
    market.alive = false;
    market.firstDataAt = null;
    market.openedAt = null;
    if (market.pingInterval) {
      clearInterval(market.pingInterval);
      market.pingInterval = null;
    }
    if (market.firstMessageWatchdog) {
      clearTimeout(market.firstMessageWatchdog);
      market.firstMessageWatchdog = null;
    }
    if (market.preemptiveReconnectTimeout) {
      clearTimeout(market.preemptiveReconnectTimeout);
      market.preemptiveReconnectTimeout = null;
    }
    // Pending request map is per-connection; drop any unacked requests.
    market.pendingRequests.clear();

    if (!hasAnySubscribers()) {
      log('info', 'market upstream not reconnecting — no subscribers');
      return;
    }

    market.reconnectAttempts++;
    const baseDelay = nextBackoffMs(market.reconnectAttempts);
    const finalDelay = applyRateLimit(baseDelay);
    log('info', `market upstream reconnect scheduled in ${Math.round(finalDelay / 1000)}s (attempt ${market.reconnectAttempts})`);
    market.reconnectTimeout = setTimeout(connectMarketUpstream, finalDelay);
  });
}

function ensureMarketUpstream() {
  if (market.ws && (market.ws.readyState === WebSocket.OPEN || market.ws.readyState === WebSocket.CONNECTING)) {
    return;
  }
  // If a reconnect is already pending (in exponential backoff after a recent
  // close), let it run on its scheduled timer — DO NOT bypass to a fresh
  // immediate connect just because a new subscriber arrived. Bypassing would
  // defeat the backoff during a Binance throttle and could hammer connections
  // per incoming subscriber. The pending reconnect will pick up the latest
  // streamSubscribers state on its next on('open') automatically.
  if (market.reconnectTimeout) {
    return;
  }
  // True fresh start (no current ws, no pending reconnect): startup, post
  // graceful shutdown of an idle connection, etc.
  market.reconnectAttempts = 0;
  connectMarketUpstream();
}

function sendRequest(method, streams) {
  if (!market.ws || market.ws.readyState !== WebSocket.OPEN) {
    // Will be handled on next on('open') re-subscribe pass
    log('debug', `defer ${method} — upstream not open; will resend on reconnect`);
    return;
  }
  const id = market.nextRequestId++;
  const payload = { method, params: streams, id };
  try {
    market.ws.send(JSON.stringify(payload));
    market.pendingRequests.set(id, { method, streams: streams.slice(), sentAt: Date.now() });
    log('info', `${method} id=${id} streams=[${streams.join(', ')}]`);
  } catch (e) {
    log('warn', `${method} failed to send: ${e.message}`);
  }
}

function sendSubscribe(streams) {
  if (streams.length > 0) sendRequest('SUBSCRIBE', streams);
}

function sendUnsubscribe(streams) {
  if (streams.length > 0) sendRequest('UNSUBSCRIBE', streams);
}

function addClientToStream(client, stream) {
  let subs = market.streamSubscribers.get(stream);
  const isFirstSubscriber = !subs || subs.size === 0;
  if (!subs) {
    subs = new Set();
    market.streamSubscribers.set(stream, subs);
  }
  subs.add(client);
  log('info', `client+ ${stream} subs=${subs.size}`);

  ensureMarketUpstream();

  // Send SUBSCRIBE only if upstream is currently OPEN. If it's connecting/reconnecting,
  // the on('open') re-subscribe pass picks up the latest streamSubscribers state.
  if (isFirstSubscriber && market.ws && market.ws.readyState === WebSocket.OPEN) {
    sendSubscribe([stream]);
  }
}

function removeClientFromStream(client, stream) {
  const subs = market.streamSubscribers.get(stream);
  if (!subs) return;
  subs.delete(client);
  log('info', `client- ${stream} subs=${subs.size}`);
  if (subs.size === 0) {
    market.streamSubscribers.delete(stream);
    market.streamLastMessageAt.delete(stream);
    market.staleWarned.delete(stream);
    if (market.ws && market.ws.readyState === WebSocket.OPEN) {
      sendUnsubscribe([stream]);
    }
  }
}

function handleMarketSubscriber(clientWs, stream) {
  addClientToStream(clientWs, stream);

  clientWs.on('close', () => {
    removeClientFromStream(clientWs, stream);
  });

  clientWs.on('error', (err) => {
    log('warn', `client error on ${stream}: ${err.message}`);
  });
}

// Stale-stream watchdog (informational only — does NOT auto-recycle the upstream).
// Logs once per silence event so we have visibility without log spam during a
// real Binance throttle.
setInterval(() => {
  const now = Date.now();
  for (const [stream, subs] of market.streamSubscribers) {
    if (subs.size === 0) continue;
    if (!isChattyStream(stream)) continue;
    const lastAt = market.streamLastMessageAt.get(stream);
    if (!lastAt) continue;
    const silentMs = now - lastAt;
    if (silentMs > CHATTY_STREAM_STALE_WARNING_MS && !market.staleWarned.has(stream)) {
      log('warn', `chatty stream ${stream} silent for ${Math.round(silentMs / 1000)}s — Binance may have paused this specific stream (connection itself is fine)`);
      market.staleWarned.add(stream);
    }
  }
}, STALE_CHECK_INTERVAL_MS).unref();

// ─── User-data proxy (1:1, unchanged from v1.0.x) ────────────────────────────

function handleUserDataProxy(clientWs, listenKey) {
  const shortKey = listenKey.slice(0, 8);
  const upstreamUrl = userDataUpstreamUrl(listenKey);
  log('info', `userdata+ ${shortKey}…`);

  const upstreamWs = new WebSocket(upstreamUrl);
  let upstreamPingInterval = null;
  let closed = false;

  const shutdown = (reason) => {
    if (closed) return;
    closed = true;
    if (upstreamPingInterval) clearInterval(upstreamPingInterval);
    try { if (upstreamWs.readyState !== WebSocket.CLOSED) upstreamWs.close(); } catch (_) { /* ignore */ }
    try { if (clientWs.readyState !== WebSocket.CLOSED) clientWs.close(); } catch (_) { /* ignore */ }
    log('info', `userdata- ${shortKey}… reason=${reason}`);
  };

  upstreamWs.on('open', () => {
    log('info', `userdata upstream open: ${shortKey}…`);
    upstreamPingInterval = setInterval(() => {
      if (upstreamWs.readyState === WebSocket.OPEN) {
        try { upstreamWs.ping(); } catch (_) { /* ignore */ }
      }
    }, PING_INTERVAL_MS);
  });

  upstreamWs.on('message', (data) => {
    if (clientWs.readyState === WebSocket.OPEN) {
      try { clientWs.send(data); } catch (_) { /* ignore */ }
    }
  });

  upstreamWs.on('close', () => shutdown('upstream-close'));
  upstreamWs.on('error', (err) => log('warn', `userdata upstream error ${shortKey}: ${err.message}`));

  clientWs.on('close', () => shutdown('client-close'));
  clientWs.on('error', (err) => log('warn', `userdata client error ${shortKey}: ${err.message}`));
}

// ─── HTTP server (/health) ────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    const body = {
      status: 'ok',
      uptime: process.uptime(),
      marketUpstream: {
        connected: market.alive && market.ws?.readyState === WebSocket.OPEN,
        openedAt: market.openedAt,
        firstDataLatencyMs: market.openedAt && market.firstDataAt ? market.firstDataAt - market.openedAt : null,
        reconnectAttempts: market.reconnectAttempts,
        recentConnectAttempts5min: market.recentAttemptTimestamps.length,
        connectAttemptCap5min: UPSTREAM_MAX_ATTEMPTS_PER_5MIN,
      },
      streams: Array.from(market.streamSubscribers.entries()).map(([stream, subs]) => {
        const lastAt = market.streamLastMessageAt.get(stream) || null;
        return {
          stream,
          subs: subs.size,
          lastMessageAt: lastAt,
          silentMs: lastAt ? Date.now() - lastAt : null,
        };
      }),
      totalMarketSubscribers: Array.from(market.streamSubscribers.values()).reduce((acc, s) => acc + s.size, 0),
    };
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(body, null, 2));
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not Found');
});

// ─── WebSocket server ─────────────────────────────────────────────────────────

const wss = new WebSocketServer({ server });

wss.on('connection', (ws, req) => {
  // Auth check (only enforced when RELAY_AUTH_TOKEN is configured).
  if (RELAY_AUTH_TOKEN) {
    const provided = extractTokenFromUrl(req.url);
    if (!provided || !timingSafeStringEqual(provided, RELAY_AUTH_TOKEN)) {
      const safePath = (req.url || '').split('?')[0];
      log('warn', `rejecting client: bad/missing token on ${safePath}`);
      ws.close(1008, 'Unauthorized');
      return;
    }
  }

  const match = (req.url || '').match(/^\/ws\/([^/?#]+)/);
  if (!match) {
    log('warn', `rejecting client with bad path: ${req.url}`);
    ws.close(1008, 'Invalid path');
    return;
  }
  const target = match[1];

  if (isListenKey(target)) {
    handleUserDataProxy(ws, target);
  } else {
    handleMarketSubscriber(ws, target);
  }
});

// ─── Startup + graceful shutdown ──────────────────────────────────────────────

server.listen(PORT, () => {
  log('info', `ycbot-ws-relay listening on port ${PORT}, combined-streams=${COMBINED_STREAM_URL}`);
  if (RELAY_AUTH_TOKEN) {
    log('info', `auth: RELAY_AUTH_TOKEN configured (clients must include ?token=...)`);
  } else {
    log('warn', `[SECURITY] RELAY_AUTH_TOKEN not set — accepting all client connections without auth. Set RELAY_AUTH_TOKEN env to enable.`);
  }
});

function shutdown(signal) {
  log('info', `received ${signal}, shutting down...`);
  // Close the market upstream cleanly.
  if (market.reconnectTimeout) clearTimeout(market.reconnectTimeout);
  if (market.pingInterval) clearInterval(market.pingInterval);
  if (market.firstMessageWatchdog) clearTimeout(market.firstMessageWatchdog);
  if (market.preemptiveReconnectTimeout) clearTimeout(market.preemptiveReconnectTimeout);
  if (market.ws && market.ws.readyState !== WebSocket.CLOSED) {
    try { market.ws.close(1001, 'Relay shutting down'); } catch (_) { /* ignore */ }
  }
  // Close all client connections.
  for (const subs of market.streamSubscribers.values()) {
    for (const client of subs) {
      if (client.readyState === WebSocket.OPEN) {
        try { client.close(1001, 'Relay shutting down'); } catch (_) { /* ignore */ }
      }
    }
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 5_000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
