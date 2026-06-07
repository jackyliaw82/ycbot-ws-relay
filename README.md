# ycbot-ws-relay

WebSocket relay between vm-bots and Binance fstream. Removes user VM IPs from Binance's view entirely — only the relay's dedicated static IP talks to Binance. Fans out public market-data streams so N bots on the same symbol share ONE upstream connection.

## Architecture (v1.1.0+)

```
[user vm-bots]  ──WS──▶  [ycbot-ws-relay]  ──single combined-streams WS──▶  Binance fstream
                         (single static IP)
```

- **Market data** (`@markPrice@1s`, `@ticker`, `@forceOrder`, `@kline_*`, `@depth`, …): one persistent **combined-streams** upstream to `wss://fstream.binance.com/stream`. Streams are added/removed dynamically via `SUBSCRIBE` / `UNSUBSCRIBE` JSON messages on that single connection. All subscribed clients across all streams share this one upstream — typical relay load is **one upstream per relay process**, not one per stream. Matches Binance's documented best practice and stays well below the 300 connections/IP/5min cap.
- **User data** (listenKey-based): 1:1 pass-through to `wss://fstream.binance.com/ws/<listenKey>` — cannot be fanned out (each user's events are distinct), but routing via the relay IP still hides the user-VM IP.

### Connection-management posture

- **Pre-emptive 23h30m reconnect** to dodge Binance's 24h forced-disconnect cliff.
- **Exponential backoff** on close: 1s → 2s → 4s → 8s → 16s → 32s → 60s → 2m → 5m → 30m, capped. Reset on the first DATA frame after a fresh open.
- **Hard rate cap**: 30 connect attempts per 5 minutes (10× safety margin under Binance's documented 300/5min/IP cap). When the cap is hit, backoff is forced longer until the sliding window clears.
- **First-DATA-frame watchdog** (8s): catches the silent-stuck handshake pattern where Binance accepts the WS but never pushes data.
- **Per-stream stale-warning** for chatty streams (`@markPrice`, `@ticker`, `@kline`, `@depth`, `@aggTrade`): logs a warning if a specific stream goes silent for >60s but does NOT auto-recycle the upstream — that's a Binance-side stream-pause, not a connection failure.

## URL format

Clients connect to: `ws://<relay-host>:<port>/ws/<stream-or-listenKey>?token=<PER_USER_TOKEN>`

- Market stream: anything containing `@` (e.g. `solusdt@markPrice@1s`, `btcusdt@forceOrder`)
- User-data listenKey: no `@` (e.g. `3HBljNoVsw9NB5iDU5OJI8HyDMNJbTFmMlAmq4G38mXqkwBHQYIIq5JlWv2FXGPl`)
- `?token=` is **required** — valid tokens come from the Firestore collection `relay_auth_tokens`. Backend writes one doc per VM at provision time; relay loads them at startup and live-updates via `onSnapshot`. Clients without a matching token are closed with WS code 1008 (Unauthorized).

The relay detects which is which automatically.

## Endpoints

- `GET /health` — JSON status with upstream connection state, recent-attempt count + cap, and per-stream subscriber counts + lastMessageAt.

## Environment variables

| Var | Default | Purpose |
|-----|---------|---------|
| `PORT` | `8080` | TCP port the relay listens on |
| `BINANCE_WS_BASE_HOST` | `wss://fstream.binance.com` | Upstream Binance host (no `/ws` or `/stream` suffix — the relay appends the right path per channel) |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `FIREBASE_PROJECT_ID` | `ycbot-6f336` | Firestore project for the `relay_auth_tokens` collection. Relay uses Application Default Credentials — the VM's attached service account must have `roles/datastore.user` on this project. Backend (ycbot-ai backend-service) writes one token doc per VM at provision time; relay loads them at startup and live-updates via `onSnapshot`. If init fails, relay exits (no fallback to permissive mode). |

### Cold-start handling

The relay opens its single combined-streams upstream lazily on the first subscriber. A first-DATA-frame watchdog (8s) closes any upstream that completes the handshake + SUBSCRIBE ack but never delivers actual market data — the close-handler then reconnects with exponential backoff. Pre-warming the upstream before the bot subscribes is the responsibility of the bot itself (`POST /ai-reversal/prepare-symbol` opens a discard-only WS to the relay for the user's currently-selected symbol so the upstream is hot when a strategy actually starts). The `marketUpstream.firstDataLatencyMs` field on `/health` shows the real cold-start cost.

## Deploy on a new GCP e2-micro VM (one-time setup)

Run these commands **from your laptop** (cmd.exe or PowerShell, whichever you have gcloud in):

### 1. Reserve a dedicated static IP for the relay

```cmd
gcloud compute addresses create ycbot-ws-relay-ip --region=asia-southeast1
```

Check the IP:

```cmd
gcloud compute addresses describe ycbot-ws-relay-ip --region=asia-southeast1 --format="get(address)"
```

Note the IP — let's call it `<RELAY_IP>`.

### 2. Create the e2-micro VM

```cmd
gcloud compute instances create ycbot-ws-relay ^
  --zone=asia-southeast1-b ^
  --machine-type=e2-micro ^
  --image-family=debian-12 ^
  --image-project=debian-cloud ^
  --address=<RELAY_IP> ^
  --tags=ycbot-ws-relay
```

(In PowerShell, replace the `^` with backticks `` ` `` for line continuation, or put it all on one line.)

### 3. Open port 8080 (ingress firewall rule)

```cmd
gcloud compute firewall-rules create allow-ycbot-ws-relay ^
  --network=default ^
  --direction=INGRESS ^
  --action=ALLOW ^
  --rules=tcp:8080 ^
  --target-tags=ycbot-ws-relay ^
  --source-ranges=0.0.0.0/0
```

For tighter security, narrow `--source-ranges` to just your user VM IP(s). For early rollout with a few VMs, `0.0.0.0/0` is acceptable — there's no auth on the relay so the worst case is a random client opening a stream to Binance, which is public market data anyway. Harden with auth + IP allowlist before wider use.

### 4. SSH in, install Node + PM2, clone the repo

```bash
gcloud compute ssh ycbot-ws-relay --zone=asia-southeast1-b
```

Once on the VM:

```bash
# Install Node.js 20
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs

# Install PM2 globally
sudo npm install -g pm2

# Clone this repo (or just copy the ycbot-ws-relay/ directory onto the VM)
# Option A: if you make ycbot-ws-relay its own repo
sudo git clone https://github.com/jackyliaw82/ycbot-ws-relay.git /opt/ycbot-ws-relay

# Option B: if it lives in ycbot-ai monorepo, sparse checkout or scp the directory
# Simplest: on laptop, `gcloud compute scp --recurse --zone=asia-southeast1-b ycbot-ws-relay ycbot-ws-relay:/tmp/`
# then on VM: sudo mv /tmp/ycbot-ws-relay /opt/

cd /opt/ycbot-ws-relay
sudo npm install --production
```

### 5. Start under PM2 and enable startup

```bash
sudo pm2 start ecosystem.config.cjs
sudo pm2 save
sudo pm2 startup systemd -u root --hp /root
# ^ follow the instruction it prints, typically a sudo env ... pm2 startup ... command
```

Refer to the "follow the instruction it prints.., please refer below:
- My earlier "copy the sudo env line" instruction is outdated — PM2 v6 now executes the systemd setup itself when it's already running as root. That's exactly what happened for you:
[PM2] [-] Executing: systemctl enable pm2-root...
Created symlink /etc/systemd/system/multi-user.target.wants/pm2-root.service → /etc/systemd/system/pm2-root.service.
[PM2] [v] Command successfully executed.
PM2 already installed and enabled the systemd service. No manual command needed. ****The relay will now auto-start on VM reboot.****

### 6. Verify

From your laptop:

```bash
curl http://<RELAY_IP>:8080/health
```

Expected: JSON `{ "status": "ok", "marketUpstreams": 0, "totalMarketSubscribers": 0, ... }`

Test with `wscat`:

```bash
wscat -c "ws://<RELAY_IP>:8080/ws/btcusdt@markPrice@1s"
```

You should see `markPriceUpdate` messages flowing within ~1 second.

## Point a vm-bot at the relay

On the user VM (e.g. `vm-user-dkcvtsffjovvdzhlmrnpljhak3b3`):

```bash
# Edit ecosystem config to add RELAY_WS_URL env var
sudo nano /opt/vm-bot/ecosystem.config.cjs
```

Add to the `env` block (or wherever env vars live):

```js
env: {
  // ...existing env vars...
  RELAY_WS_URL: 'ws://<RELAY_IP>:8080/ws',
},
```

Then restart:

```bash
sudo pm2 restart ycbot --update-env
sudo pm2 logs ycbot --lines 30
```

Look for `[DIAG] connectRealtimeWebSocket called. ... url=ws://<RELAY_IP>:8080/ws/solusdt@markPrice@1s` — URL now points at the relay instead of fstream.binance.com.

## Rollout

1. Deploy relay and smoke-test with `wscat`.
2. Point admin's own vm-bot at the relay. Run a strategy on a cheap symbol for 30 min. Compare behavior to a direct-Binance vm-bot (identical tick rate, no stalls).
3. Firewall-block test: on the admin's vm-bot VM, block the relay's IP. Confirm Phase 1 REST fallback engages. Unblock — WS recovers via relay.
4. Env-flag-gated rollout: one user VM at a time.
5. When all vm-bots point at relay, remove direct-Binance egress from user VMs (optional — further isolation).

## Failure modes + mitigations

| Failure | Effect | Mitigation |
|---|---|---|
| Relay crashes | All vm-bots see WS close → REST fallback triggers on each | PM2 autorestart; monitor `/health`; bot trading continues |
| Relay's static IP gets silently throttled by Binance for a specific stream | First-DATA watchdog catches it within 8s; backoff kicks in; eventually de-flagged or operator cycles the IP | Exponential backoff + 30/5min rate cap prevents self-inflicted re-throttle. IP-cycle runbook below as last resort. |
| Relay's static IP gets fully banned by Binance | All upstreams fail; bots see silent stall → REST fallback | Release/re-reserve a new static IP (runbook below) |
| Binance's 24h forced disconnect | Pre-emptive 23h30m reconnect handles it gracefully — re-subscribes all active streams on the new connection with no data gap | Built-in to the upstream lifecycle (v1.1.0+) |
| e2-micro VM out of memory | PM2 `max_memory_restart: 200M` in ecosystem config recycles it | Monitor memory usage; upsize to e2-small if real need |
| Relay can't reach Binance (network outage) | Upstream fails to open, backoff escalates up to 30 min | Watchdog + REST fallback handles the bot side; relay reconnects when network restored |

### IP-cycle runbook (when an IP gets flagged despite v1.1.0 mitigations)

```bash
gcloud compute addresses create ycbot-ws-relay-ip-v2 --region=asia-southeast1
gcloud compute addresses describe ycbot-ws-relay-ip-v2 --region=asia-southeast1 --format='value(address)'
gcloud compute instances delete-access-config ycbot-ws-relay --zone=asia-southeast1-b --access-config-name='External NAT'
gcloud compute instances add-access-config ycbot-ws-relay --zone=asia-southeast1-b --address=<NEW_IP> --access-config-name='External NAT'
# After 24h of stable operation, release the old reservation:
# gcloud compute addresses delete ycbot-ws-relay-ip --region=asia-southeast1
```

Then update `RELAY_WS_URL` in `vm-bot/ecosystem.config.cjs` to point at the new IP, push, and `git pull && pm2 restart ycbot --update-env` on each user VM.

## Not yet implemented (future work)

- TLS (`wss://`) — requires a cert. Not needed for within-GCP traffic; needed if exposing to untrusted networks.
- Auth header / API key — relay is currently open. Add a token check in `wss.on('connection')` before fanout.
- Multi-region HA — single relay is SPOF; second relay in a different region would give redundancy.
- Metrics — Prometheus `/metrics` endpoint would replace hand-rolled `/health` for proper observability.
