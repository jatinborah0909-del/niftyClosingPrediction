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
| `GET /close-estimate` | Projected close + all 50 stocks breakdown + history |
| `GET /status` | Health check (used by Railway healthcheck probe) |

---

## Updating constituent weights

Nifty 50 weights change quarterly (NSE rebalances every March, June, Sep, Dec).
Update the `NIFTY50` dict in `nifty_close_bridge.py` from the NSE factsheet:
https://www.niftyindices.com/indices/equity/broad-based-indices/NIFTY-50

To verify instrument tokens run:
```python
from kiteconnect import KiteConnect
kite = KiteConnect(api_key="..."); kite.set_access_token("...")
inst = {i['tradingsymbol']: i['instrument_token'] for i in kite.instruments("NSE")}
print(inst["RELIANCE"])  # verify any symbol
```
