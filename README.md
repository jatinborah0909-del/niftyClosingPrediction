# Nifty 50 Closing Price Estimator — Railway Deployment

## Repo layout

```
your-repo/
├── nifty_close_bridge.py   Flask app + Kite WebSocket bridge
├── requirements.txt
├── Procfile
├── railway.json
├── README.md
└── static/
    └── index.html          Dashboard (served at your Railway public URL)
```

---

## How it works

| Time | Mode | Method |
|------|------|--------|
| 9:15 AM – 3:00 PM | **LTP** | Live price proxy — `Σ(w_i × ltp_i / prev_i) × prev_nifty` |
| 3:00 PM – 3:30 PM | **VWAP** | Mirrors NSE's official settlement method exactly |
| After 3:30 PM | **VWAP** | Final accumulated VWAP — this is the projected official close |

---

## Railway setup (one-time, ~5 minutes)

### 1. Push to GitHub
```bash
git init && git add . && git commit -m "init"
git remote add origin https://github.com/you/nifty-close-estimator.git
git push -u origin main
```

### 2. Create Railway project
Railway → **New Project** → **Deploy from GitHub** → select your repo.

> No PostgreSQL needed — this app is stateless (all data is in-memory).

### 3. Set environment variables
Railway → your service → **Variables** tab → add these two:

| Variable | Value | Notes |
|----------|-------|-------|
| `KITE_API_KEY` | your Zerodha API key | from kite.trade developer console |
| `KITE_ACCESS_TOKEN` | today's access token | **must be updated daily** |

> `PORT` is injected by Railway automatically — **do NOT set it**.

### 4. Generate a public domain
Railway → **Settings** → **Networking** → **Generate Domain**

Your dashboard is now live at `https://your-service.up.railway.app/`

---

## Daily routine

Every trading day you need to refresh the access token:

```bash
# Option A: Update via Railway dashboard
# Railway → Variables → KITE_ACCESS_TOKEN → paste today's token → Save
# Railway auto-redeploys (~30 seconds)

# Option B: Use Railway CLI
railway variables set KITE_ACCESS_TOKEN=your_new_token
```

Trigger this before 9:15 AM for uninterrupted market coverage.

---

## API endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Dashboard HTML |
| `GET /close-estimate` | Projected close + all 50 stocks breakdown + history + flagged stocks |
| `GET /status` | Health check (used by Railway healthcheck probe) — includes resolution + flagged counts |
| `GET /tokens` | Inspect resolved instrument tokens — confirms every symbol mapped correctly at startup |

---

## Token resolution (automatic — no manual step needed)

On every startup, the app calls `kite.instruments("NSE")` and resolves each of
the 50 symbols in `NIFTY50_WEIGHTS` to its live instrument token. There are
**no hand-typed tokens in the code** — this is what `/tokens` lets you verify.

If a symbol can't be resolved (company renamed, delisted, NSE rebalance),
check the Railway logs for a `[resolve_tokens] WARN: ... missing` line and
update the symbol name in `NIFTY50_WEIGHTS`.

### Sanity-band protection
Any stock whose `price / prev_close` ratio falls outside **±15%** is automatically
excluded from the projection and listed in `flagged_stocks` (via `/status`) or
`flagged` (via `/close-estimate`). This catches the failure mode where a bad
token pairs a price with the wrong stock's previous close, which previously
produced impossible jumps like +43% in a single day.

---

## Updating constituent weights

Nifty 50 weights change quarterly (NSE rebalances every March, June, Sep, Dec).
Update the `NIFTY50_WEIGHTS` dict in `nifty_close_bridge.py` from the NSE factsheet:
https://www.niftyindices.com/indices/equity/broad-based-indices/NIFTY-50

Tokens never need manual updates — they're resolved live from Kite on every
startup. Just keep the symbol names current (NSE `tradingsymbol` values).
