# ycbot-ws-relay

WebSocket relay between vm-bots and Binance fstream. Removes user VM IPs from Binance's view entirely — only the relay's dedicated static IP talks to Binance. Also fans out public market-data streams so N bots on the same symbol share ONE upstream connection.

## Architecture

```
[user vm-bots]  ──WS──▶  [ycbot-ws-relay]  ──WS──▶  Binance fstream
                         (single static IP)
```

- **Market data** (`@markPrice@1s`, `@ticker`, `@forceOrder`): one upstream per `(symbol, stream)`, multiplexed to N subscribed bots.
- **User data** (listenKey-based): 1:1 pass-through (cannot be fanned out — each user has distinct events).

## URL format

Clients connect to: `ws://<relay-host>:<port>/ws/<stream-or-listenKey>`

- Market stream: anything containing `@` (e.g. `solusdt@markPrice@1s`, `btcusdt@forceOrder`)
- User-data listenKey: no `@` (e.g. `3HBljNoVsw9NB5iDU5OJI8HyDMNJbTFmMlAmq4G38mXqkwBHQYIIq5JlWv2FXGPl`)

The relay detects which is which automatically.

## Endpoints

- `GET /health` — JSON status with upstream count, subscriber totals, per-stream stats.

## Environment variables

| Var | Default | Purpose |
|-----|---------|---------|
| `PORT` | `8080` | TCP port the relay listens on |
| `BINANCE_WS_BASE` | `wss://fstream.binance.com/ws` | Upstream Binance WS base URL |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |

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
| Relay crashes | All vm-bots see WS close → Phase 1 REST fallback triggers on each | PM2 autorestart; monitor `/health`; bot trading continues |
| Relay's static IP gets banned by Binance | All upstreams fail; bots see silent stall → fallback | Release/re-reserve a new static IP (same runbook as user VM IP swap) |
| e2-micro VM out of memory | PM2 `max_memory_restart: 200M` in ecosystem config recycles it | Monitor memory usage; upsize to e2-small if real need |
| Relay can't reach Binance | Upstreams fail to open, bots stall | Watchdog + REST fallback handles the bot side |

## Not yet implemented (future work)

- TLS (`wss://`) — requires a cert. Not needed for within-GCP traffic; needed if exposing to untrusted networks.
- Auth header / API key — relay is currently open. Add a token check in `wss.on('connection')` before fanout.
- Multi-region HA — single relay is SPOF; second relay in a different region would give redundancy.
- Metrics — Prometheus `/metrics` endpoint would replace hand-rolled `/health` for proper observability.
