/**
 * ycbot-ws-relay — WebSocket relay between vm-bots and Binance fstream.
 *
 * Goals:
 *  - Remove user VM IPs from Binance's view entirely. Only the relay's static IP
 *    appears to Binance, keeping per-user VM reputation out of the WAF equation.
 *  - Fan out public market-data streams (markPrice, ticker, forceOrder) from ONE
 *    upstream connection per (symbol, stream) to N subscribed vm-bots. Saves
 *    upstream connections at scale — 30 bots on SOLUSDT = 1 upstream, not 30.
 *  - Pass through user-data WS (per-user listenKey) 1:1. Cannot be fanned out —
 *    each user's events are distinct — but routing via the relay IP still gives
 *    the reputation benefit.
 *
 * URL format:
 *  - ws://<relay-host>:<port>/ws/<stream>
 *    where <stream> is either a Binance market stream (contains '@', e.g.
 *    "solusdt@markPrice@1s", "btcusdt@ticker", "solusdt@forceOrder") or a
 *    user-data listenKey (64-char alphanumeric, no '@').
 *
 * Per-IP connection notes:
 *  - Binance allows up to 1024 simultaneous connections per source IP. Relay
 *    fans out market data so typical load is one upstream per active symbol,
 *    well under the limit.
 *  - User-data is 1:1, so the relay opens 1 upstream per active user.
 */

import { WebSocketServer, WebSocket } from 'ws';
import http from 'http';

const PORT = Number(process.env.PORT || 8080);
const BINANCE_WS_BASE = process.env.BINANCE_WS_BASE || 'wss://fstream.binance.com/ws';
const PING_INTERVAL_MS = 30_000;
const UPSTREAM_RECONNECT_BASE_MS = 1_000;
const UPSTREAM_RECONNECT_MAX_MS = 60_000;
const UPSTREAM_MAX_RECONNECT_ATTEMPTS = 25;
const UPSTREAM_IDLE_CLOSE_MS = 30_000; // close upstream when no subscribers for this long
// If Binance accepts the WS handshake but never pushes a message (silent throttle / bad
// stream name / IP rep), close the WS after this long so the close-handler reconnect
// kicks in. Without this, a silent upstream sits stuck forever (alive=true, never delivers).
// 8s gives ~9x margin over the slowest observed healthy cold-start (~920ms).
const UPSTREAM_FIRST_MESSAGE_TIMEOUT_MS = 8_000;

// Log level: 'debug' | 'info' | 'warn' | 'error'
const LOG_LEVEL = (process.env.LOG_LEVEL || 'info').toLowerCase();
const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 };
function log(level, ...args) {
  if ((LEVELS[level] ?? 1) < (LEVELS[LOG_LEVEL] ?? 1)) return;
  const ts = new Date().toISOString();
  console.log(`${ts} [${level.toUpperCase()}]`, ...args);
}

// ─── Market-data upstream manager ─────────────────────────────────────────────
// Key: stream string (e.g. "solusdt@markPrice@1s")
// Value: { ws, subscribers: Set<ClientWs>, reconnectAttempts, reconnectTimeout,
//          pingInterval, idleCloseTimeout, alive }
const marketUpstreams = new Map();

function isListenKey(stream) {
  // Binance market streams always contain '@'. Listen keys are ~64-char alnum.
  // Treat anything without '@' as a listen key candidate.
  return !stream.includes('@');
}

function connectUpstream(upstream) {
  const url = `${BINANCE_WS_BASE}/${upstream.stream}`;
  log('info', `upstream connecting: ${upstream.stream}`);
  const ws = new WebSocket(url);
  upstream.ws = ws;
  upstream.alive = true;

  ws.on('open', () => {
    upstream.openedAt = Date.now();
    upstream.firstMessageAt = null;
    log('info', `upstream open: ${upstream.stream} (${upstream.subscribers.size} subs)`);
    if (upstream.pingInterval) clearInterval(upstream.pingInterval);
    upstream.pingInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) {
        try { ws.ping(); } catch (_) { /* ignore */ }
      }
    }, PING_INTERVAL_MS);
    // Watchdog: if no message arrives within the timeout, close so close-handler reconnects.
    if (upstream.firstMessageWatchdog) clearTimeout(upstream.firstMessageWatchdog);
    upstream.firstMessageWatchdog = setTimeout(() => {
      if (!upstream.firstMessageAt && ws.readyState === WebSocket.OPEN) {
        log('warn', `upstream first-message timeout: ${upstream.stream} after ${UPSTREAM_FIRST_MESSAGE_TIMEOUT_MS}ms — closing for retry`);
        try { ws.close(4001, 'first-message timeout'); } catch (_) { /* ignore */ }
      }
    }, UPSTREAM_FIRST_MESSAGE_TIMEOUT_MS);
  });

  ws.on('message', (data) => {
    if (!upstream.firstMessageAt) {
      upstream.firstMessageAt = Date.now();
      const latency = upstream.openedAt ? upstream.firstMessageAt - upstream.openedAt : null;
      log('info', `upstream first-message: ${upstream.stream} latency=${latency != null ? `${latency}ms` : 'unknown'}`);
      if (upstream.firstMessageWatchdog) {
        clearTimeout(upstream.firstMessageWatchdog);
        upstream.firstMessageWatchdog = null;
      }
      // Reset backoff only on a *productive* connection — receiving a real message.
      // Resetting on WS-open instead would prevent backoff growth across silent-close cycles.
      upstream.reconnectAttempts = 0;
    }
    // Fan-out: forward raw frame to all subscribed clients
    for (const client of upstream.subscribers) {
      if (client.readyState === WebSocket.OPEN) {
        try { client.send(data); } catch (_) { /* ignore per-client send errors */ }
      }
    }
  });

  ws.on('error', (err) => {
    log('warn', `upstream error: ${upstream.stream}: ${err.message}`);
  });

  ws.on('close', (code, reason) => {
    log('info', `upstream close: ${upstream.stream} code=${code} reason=${reason || 'none'} subs=${upstream.subscribers.size}`);
    if (upstream.pingInterval) {
      clearInterval(upstream.pingInterval);
      upstream.pingInterval = null;
    }
    if (upstream.firstMessageWatchdog) {
      clearTimeout(upstream.firstMessageWatchdog);
      upstream.firstMessageWatchdog = null;
    }
    upstream.alive = false;

    if (upstream.subscribers.size === 0) {
      // No subscribers — don't bother reconnecting
      marketUpstreams.delete(upstream.stream);
      return;
    }

    // Reconnect with exponential backoff
    upstream.reconnectAttempts++;
    if (upstream.reconnectAttempts > UPSTREAM_MAX_RECONNECT_ATTEMPTS) {
      log('error', `upstream max reconnect attempts: ${upstream.stream}`);
      // Notify clients so they can reconnect or fall back
      for (const client of upstream.subscribers) {
        if (client.readyState === WebSocket.OPEN) {
          client.close(1011, 'Upstream max reconnect reached');
        }
      }
      marketUpstreams.delete(upstream.stream);
      return;
    }
    const delay = Math.min(
      UPSTREAM_RECONNECT_MAX_MS,
      UPSTREAM_RECONNECT_BASE_MS * Math.pow(2, upstream.reconnectAttempts - 1)
    );
    upstream.reconnectTimeout = setTimeout(() => connectUpstream(upstream), delay);
  });
}

function getOrCreateUpstream(stream) {
  let upstream = marketUpstreams.get(stream);
  if (upstream) {
    // If an idle-close was pending, cancel it now that we have a new subscriber
    if (upstream.idleCloseTimeout) {
      clearTimeout(upstream.idleCloseTimeout);
      upstream.idleCloseTimeout = null;
    }
    return upstream;
  }
  upstream = {
    stream,
    ws: null,
    subscribers: new Set(),
    reconnectAttempts: 0,
    reconnectTimeout: null,
    pingInterval: null,
    idleCloseTimeout: null,
    firstMessageWatchdog: null,
    alive: false,
    openedAt: null,
    firstMessageAt: null,
  };
  marketUpstreams.set(stream, upstream);
  connectUpstream(upstream);
  return upstream;
}

function handleMarketSubscriber(clientWs, stream) {
  const upstream = getOrCreateUpstream(stream);
  upstream.subscribers.add(clientWs);
  log('info', `client+ ${stream} subs=${upstream.subscribers.size}`);

  clientWs.on('close', () => {
    upstream.subscribers.delete(clientWs);
    log('info', `client- ${stream} subs=${upstream.subscribers.size}`);
    if (upstream.subscribers.size === 0) {
      // Schedule idle close — avoids churn if another client re-subscribes immediately
      if (upstream.idleCloseTimeout) clearTimeout(upstream.idleCloseTimeout);
      upstream.idleCloseTimeout = setTimeout(() => {
        if (upstream.subscribers.size === 0) {
          log('info', `upstream idle-close: ${stream}`);
          if (upstream.reconnectTimeout) clearTimeout(upstream.reconnectTimeout);
          if (upstream.pingInterval) clearInterval(upstream.pingInterval);
          if (upstream.firstMessageWatchdog) clearTimeout(upstream.firstMessageWatchdog);
          if (upstream.ws && upstream.ws.readyState !== WebSocket.CLOSED) {
            try { upstream.ws.close(); } catch (_) { /* ignore */ }
          }
          marketUpstreams.delete(stream);
        }
      }, UPSTREAM_IDLE_CLOSE_MS);
    }
  });

  clientWs.on('error', (err) => {
    log('warn', `client error on ${stream}: ${err.message}`);
  });
}

// ─── User-data proxy (1:1) ────────────────────────────────────────────────────

function handleUserDataProxy(clientWs, listenKey) {
  const shortKey = listenKey.slice(0, 8);
  const upstreamUrl = `${BINANCE_WS_BASE}/${listenKey}`;
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

// ─── HTTP server (/health + /metrics) ─────────────────────────────────────────

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    const body = {
      status: 'ok',
      uptime: process.uptime(),
      marketUpstreams: marketUpstreams.size,
      totalMarketSubscribers: Array.from(marketUpstreams.values()).reduce(
        (acc, u) => acc + u.subscribers.size,
        0
      ),
      streams: Array.from(marketUpstreams.entries()).map(([stream, u]) => ({
        stream,
        subs: u.subscribers.size,
        alive: u.alive,
        reconnectAttempts: u.reconnectAttempts,
        firstMessageLatencyMs: u.openedAt && u.firstMessageAt ? u.firstMessageAt - u.openedAt : null,
      })),
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
  // URL must be /ws/<stream_or_listenKey>
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
  log('info', `ycbot-ws-relay listening on port ${PORT}, upstream=${BINANCE_WS_BASE}`);
});

function shutdown(signal) {
  log('info', `received ${signal}, shutting down...`);
  for (const upstream of marketUpstreams.values()) {
    if (upstream.reconnectTimeout) clearTimeout(upstream.reconnectTimeout);
    if (upstream.pingInterval) clearInterval(upstream.pingInterval);
    if (upstream.idleCloseTimeout) clearTimeout(upstream.idleCloseTimeout);
    if (upstream.firstMessageWatchdog) clearTimeout(upstream.firstMessageWatchdog);
    if (upstream.ws && upstream.ws.readyState !== WebSocket.CLOSED) {
      try { upstream.ws.close(); } catch (_) { /* ignore */ }
    }
    for (const client of upstream.subscribers) {
      if (client.readyState === WebSocket.OPEN) {
        try { client.close(1001, 'Relay shutting down'); } catch (_) { /* ignore */ }
      }
    }
  }
  server.close(() => process.exit(0));
  // Hard exit if shutdown hangs
  setTimeout(() => process.exit(1), 5_000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
