"""
Nifty 50 Closing Price Estimator — Railway Edition
====================================================
Mirrors NSE's official closing methodology:
  • Streams all 50 Nifty constituent stocks via Kite WebSocket (MODE_FULL)
  • Accumulates per-stock VWAP during settlement window (3:00–3:30 PM IST)
  • Projects closing index = Σ(weight_i × vwap_i / prev_close_i) × prev_nifty_close
  • Before 3 PM: uses LTP as a live proxy

Flask serves the dashboard at GET / and JSON APIs at /close-estimate etc.
No CORS needed — same-origin since Flask serves the HTML too.

Environment variables (set in Railway → Variables tab):
  KITE_API_KEY        Zerodha API key (required)
  KITE_ACCESS_TOKEN   Daily access token (required, refresh each morning)
  PORT                Injected by Railway automatically — do NOT set manually

File layout expected by Railway:
  nifty_close_bridge.py   ← this file
  static/
    index.html             ← dashboard (served at /)
  requirements.txt
  Procfile
  railway.json
"""

import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from kiteconnect import KiteTicker, KiteConnect

# ── ENV CONFIG ────────────────────────────────────────────────────────────────

API_KEY      = os.environ.get("KITE_API_KEY")
ACCESS_TOKEN = os.environ.get("KITE_ACCESS_TOKEN")
PORT         = int(os.environ.get("PORT", 8080))

# Settlement window: 3:00 PM – 3:30 PM IST
SETTLEMENT_START = (15, 0)
SETTLEMENT_END   = (15, 30)

# How often to retry WebSocket reconnect (seconds)
WS_RECONNECT_DELAY = 10

# ── VALIDATE ──────────────────────────────────────────────────────────────────

def _check_env():
    missing = []
    if not API_KEY:      missing.append("KITE_API_KEY")
    if not ACCESS_TOKEN: missing.append("KITE_ACCESS_TOKEN")
    if missing:
        print(f"[FATAL] Missing required environment variables: {', '.join(missing)}")
        print("        Set them in Railway → your service → Variables tab.")
        sys.exit(1)

# ── NIFTY 50 CONSTITUENTS ─────────────────────────────────────────────────────
# Token  = Kite NSE instrument token (run verify_tokens.py to confirm/update)
# Weight = approximate free-float index weight (%) — update quarterly from NSE factsheet
#
# Weights intentionally sum to ~100%. Missing weight is filled with ratio=1.0
# (no-move assumption) so the estimate degrades gracefully if ticks are absent.

NIFTY50 = {
    738561:  {"symbol": "RELIANCE",   "weight": 9.30},
    341249:  {"symbol": "HDFCBANK",   "weight": 8.20},
    1270529: {"symbol": "ICICIBANK",  "weight": 7.30},
    2953217: {"symbol": "INFY",       "weight": 5.80},
    779521:  {"symbol": "TCS",        "weight": 4.90},
    3001089: {"symbol": "BHARTIARTL", "weight": 3.90},
    442369:  {"symbol": "ITC",        "weight": 3.60},
    4267265: {"symbol": "HINDUNILVR", "weight": 2.90},
    356865:  {"symbol": "LT",         "weight": 2.80},
    582:     {"symbol": "SBIN",       "weight": 2.70},
    2815745: {"symbol": "BAJFINANCE", "weight": 2.60},
    4574849: {"symbol": "M&M",        "weight": 2.30},
    1363969: {"symbol": "KOTAKBANK",  "weight": 2.40},
    60417:   {"symbol": "AXISBANK",   "weight": 2.20},
    2865:    {"symbol": "MARUTI",     "weight": 2.10},
    3926273: {"symbol": "SUNPHARMA",  "weight": 2.00},
    1901249: {"symbol": "TATAMOTORS", "weight": 1.90},
    3812801: {"symbol": "WIPRO",      "weight": 1.80},
    895745:  {"symbol": "ULTRACEMCO", "weight": 1.70},
    519937:  {"symbol": "ASIANPAINT", "weight": 1.60},
    4343553: {"symbol": "POWERGRID",  "weight": 1.50},
    1346049: {"symbol": "NTPC",       "weight": 1.50},
    1901217: {"symbol": "ONGC",       "weight": 1.40},
    2977281: {"symbol": "JSWSTEEL",   "weight": 1.40},
    3465:    {"symbol": "TATASTEEL",  "weight": 1.30},
    8192:    {"symbol": "HCLTECH",    "weight": 1.30},
    3098049: {"symbol": "ADANIENT",   "weight": 1.20},
    2714625: {"symbol": "ADANIPORTS", "weight": 1.20},
    1750:    {"symbol": "BAJAJFINSV", "weight": 1.10},
    505:     {"symbol": "EICHERMOT",  "weight": 1.10},
    4538561: {"symbol": "GRASIM",     "weight": 1.00},
    2815233: {"symbol": "TITAN",      "weight": 1.00},
    348929:  {"symbol": "TECHM",      "weight": 0.95},
    884737:  {"symbol": "TATACONSUM", "weight": 0.90},
    2763265: {"symbol": "HINDALCO",   "weight": 0.90},
    1510401: {"symbol": "INDUSINDBK", "weight": 0.90},
    2865793: {"symbol": "DRREDDY",    "weight": 0.85},
    969473:  {"symbol": "COALINDIA",  "weight": 0.85},
    5215745: {"symbol": "BEL",        "weight": 0.80},
    3834113: {"symbol": "CIPLA",      "weight": 0.80},
    2740353: {"symbol": "BRITANNIA",  "weight": 0.75},
    2061393: {"symbol": "DIVISLAB",   "weight": 0.75},
    1102337: {"symbol": "NESTLEIND",  "weight": 0.70},
    3263489: {"symbol": "BAJAJ-AUTO", "weight": 0.70},
    4592385: {"symbol": "SHRIRAMFIN", "weight": 0.65},
    3674721: {"symbol": "HEROMOTOCO", "weight": 0.65},
    2796033: {"symbol": "SBILIFE",    "weight": 0.60},
    3524673: {"symbol": "TRENT",      "weight": 0.60},
    4775425: {"symbol": "HDFCLIFE",   "weight": 0.55},
    225537:  {"symbol": "APOLLOHOSP", "weight": 0.55},
}

NIFTY_INDEX_TOKEN = 256265  # NSE:NIFTY 50 — fixed, never changes

# ── FLASK ─────────────────────────────────────────────────────────────────────

app = Flask(__name__, static_folder="static")
CORS(app)

# ── SHARED STATE ──────────────────────────────────────────────────────────────

_lock = threading.Lock()

# Initialise per-stock state
stock = {
    token: {
        "symbol":     meta["symbol"],
        "weight":     meta["weight"],
        "prev_close": None,
        "ltp":        None,
        "cum_pv":     0.0,   # Σ(price × volume) inside settlement window
        "cum_vol":    0.0,   # Σ(volume) inside settlement window
        "vwap":       None,  # running VWAP (valid only after first tick in window)
        "in_window":  False, # True once we received ≥1 tick during settlement
        "tick_count": 0,
    }
    for token, meta in NIFTY50.items()
}

nifty = {
    "ltp":        None,
    "prev_close": None,
    "ltp_time":   None,
}

ws_state = {
    "connected":    False,
    "last_tick_ts": None,
    "error":        None,
}

# Running projected-close history for the sparkline (last 120 data points)
proj_history = []
HISTORY_MAX  = 120

# ── HELPERS ───────────────────────────────────────────────────────────────────

def _in_settlement(now: datetime) -> bool:
    sh, sm = SETTLEMENT_START
    eh, em = SETTLEMENT_END
    after_start = (now.hour > sh) or (now.hour == sh and now.minute >= sm)
    before_end  = (now.hour < eh) or (now.hour == eh and now.minute <  em)
    return after_start and before_end


def _compute_projection(use_vwap: bool) -> dict:
    """
    Project Nifty 50 closing price.

    Formula (weight-based approximation of NSE's free-float market-cap method):
        index = Σ_i (w_i / 100 × price_i / prev_close_i) × prev_nifty_close

    For any stock without a price, we assume ratio = 1.0 (no-move).
    This means the projection is still meaningful even at market open when
    only a few stocks have ticked in.
    """
    weighted_ratio    = 0.0
    weight_with_data  = 0.0
    stocks_ready      = 0
    details           = []

    for token, s in stock.items():
        price = (s["vwap"] if use_vwap else None) or s["ltp"]
        prev  = s["prev_close"]

        row = {
            "symbol":     s["symbol"],
            "weight":     s["weight"],
            "prev_close": prev,
            "ltp":        s["ltp"],
            "vwap":       s["vwap"],
            "in_window":  s["in_window"],
            "tick_count": s["tick_count"],
        }

        if price and prev and prev > 0:
            ratio = price / prev
            weighted_ratio   += (s["weight"] / 100.0) * ratio
            weight_with_data += s["weight"]
            stocks_ready     += 1
            row["ratio"]      = round(ratio, 6)
        else:
            # no-move assumption for missing stocks
            weighted_ratio   += (s["weight"] / 100.0) * 1.0
            row["ratio"]      = None

        details.append(row)

    projected = None
    if nifty["prev_close"] and weighted_ratio > 0:
        projected = round(weighted_ratio * nifty["prev_close"], 2)

    return {
        "projected_close":  projected,
        "prev_nifty_close": nifty["prev_close"],
        "nifty_spot_ltp":   nifty["ltp"],
        "stocks_ready":     stocks_ready,
        "stocks_total":     len(stock),
        "weight_with_data": round(weight_with_data, 2),
        "in_settlement":    _in_settlement(datetime.now()),
        "mode":             "VWAP" if use_vwap else "LTP",
        "stocks":           sorted(details, key=lambda x: -x["weight"]),
    }

# ── PREV-CLOSE FETCH ──────────────────────────────────────────────────────────

def fetch_prev_closes(kite_client: KiteConnect):
    """
    Fetch T-1 close for all 50 stocks + Nifty index via kite.quote().
    Called once at startup. Retries every 30 s until all data is present.
    """
    nse_symbols = [f"NSE:{s['symbol']}" for s in NIFTY50.values()]
    symbols_all = nse_symbols + ["NSE:NIFTY 50"]

    while True:
        try:
            print(f"[prev_close] Fetching quotes for {len(symbols_all)} instruments...")
            quotes = kite_client.quote(symbols_all)

            with _lock:
                for token, s in stock.items():
                    key = f"NSE:{s['symbol']}"
                    if key in quotes:
                        pc = float(quotes[key]["ohlc"]["close"])
                        s["prev_close"] = pc
                    else:
                        print(f"[prev_close] WARN: {key} missing from quote response")

                if "NSE:NIFTY 50" in quotes:
                    nifty["prev_close"] = float(quotes["NSE:NIFTY 50"]["ohlc"]["close"])

            ready = sum(1 for s in stock.values() if s["prev_close"] is not None)
            print(f"[prev_close] {ready}/{len(stock)} stocks ready | "
                  f"Nifty prev_close = {nifty['prev_close']}")
            return

        except Exception as e:
            print(f"[prev_close] Error: {e} — retrying in 30 s...")
            time.sleep(30)

# ── WEBSOCKET ──────────────────────────────────────────────────────────────────

def _build_ticker() -> KiteTicker:
    ticker = KiteTicker(API_KEY, ACCESS_TOKEN)
    all_tokens = list(stock.keys()) + [NIFTY_INDEX_TOKEN]

    def on_connect(ws, response):
        ws.subscribe(all_tokens)
        ws.set_mode(ws.MODE_FULL, all_tokens)   # FULL gives volume_traded
        with _lock:
            ws_state["connected"] = True
            ws_state["error"]     = None
        print(f"[ws] Connected — subscribed {len(all_tokens)} tokens")

    def on_ticks(ws, ticks):
        now       = datetime.now()
        in_window = _in_settlement(now)

        with _lock:
            ws_state["last_tick_ts"] = now.strftime("%H:%M:%S")

            for tick in ticks:
                token = tick["instrument_token"]
                ltp   = tick.get("last_price")
                vol   = tick.get("volume_traded") or 0

                if token == NIFTY_INDEX_TOKEN:
                    nifty["ltp"]      = ltp
                    nifty["ltp_time"] = now.strftime("%H:%M:%S")
                    continue

                if token not in stock:
                    continue

                s = stock[token]
                s["ltp"]       = ltp
                s["tick_count"] += 1

                # VWAP accumulation only during 3:00–3:30 PM
                if in_window and ltp and vol > 0:
                    s["in_window"] = True
                    s["cum_pv"]   += ltp * vol
                    s["cum_vol"]  += vol
                    s["vwap"]      = s["cum_pv"] / s["cum_vol"]

    def on_close(ws, code, reason):
        with _lock:
            ws_state["connected"] = False
        print(f"[ws] Closed: {code} {reason} — will reconnect...")

    def on_error(ws, code, reason):
        with _lock:
            ws_state["connected"] = False
            ws_state["error"]     = str(reason)
        print(f"[ws] Error: {code} {reason}")

    ticker.on_connect = on_connect
    ticker.on_ticks   = on_ticks
    ticker.on_close   = on_close
    ticker.on_error   = on_error
    return ticker


def start_websocket():
    """Start ticker in a background thread with auto-reconnect."""
    def _run():
        while True:
            try:
                ticker = _build_ticker()
                ticker.connect(threaded=False)   # blocks until closed
            except Exception as e:
                print(f"[ws] Exception: {e}")
            print(f"[ws] Reconnecting in {WS_RECONNECT_DELAY} s...")
            time.sleep(WS_RECONNECT_DELAY)

    t = threading.Thread(target=_run, daemon=True)
    t.start()


# ── HISTORY UPDATER ───────────────────────────────────────────────────────────

def _history_loop():
    """Append a projected-close snapshot every 5 s for the sparkline chart."""
    while True:
        time.sleep(5)
        now = datetime.now()
        in_window = _in_settlement(now)
        with _lock:
            result = _compute_projection(use_vwap=in_window)
            if result["projected_close"] is not None:
                proj_history.append({
                    "t": now.strftime("%H:%M:%S"),
                    "v": result["projected_close"],
                })
                if len(proj_history) > HISTORY_MAX:
                    proj_history.pop(0)

# ── FLASK ROUTES ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Serve the dashboard HTML."""
    return send_from_directory("static", "index.html")


@app.route("/close-estimate")
def close_estimate():
    """
    Primary polling endpoint.
    Returns projected Nifty close + per-stock breakdown.
    Mode switches automatically: LTP before 3 PM, VWAP from 3:00–3:30 PM.
    """
    now       = datetime.now()
    in_window = _in_settlement(now)

    with _lock:
        data    = _compute_projection(use_vwap=in_window)
        history = list(proj_history)
        ws_ok   = ws_state["connected"]

    data["timestamp"]    = now.strftime("%H:%M:%S")
    data["ws_connected"] = ws_ok
    data["history"]      = history
    return jsonify(data)


@app.route("/status")
def status():
    """Health check — useful for Railway's healthcheck probe."""
    with _lock:
        ready = sum(1 for s in stock.values() if s["prev_close"] is not None)
        ticks = sum(s["tick_count"] for s in stock.values())
        ws_ok = ws_state["connected"]
        last  = ws_state["last_tick_ts"]
        err   = ws_state["error"]

    return jsonify({
        "ok":                    ws_ok,
        "ws_connected":          ws_ok,
        "last_tick":             last,
        "ws_error":              err,
        "stocks_prev_close":     ready,
        "stocks_total":          len(stock),
        "total_ticks_received":  ticks,
        "nifty_ltp":             nifty["ltp"],
        "nifty_prev_close":      nifty["prev_close"],
        "timestamp":             datetime.now().strftime("%H:%M:%S"),
    })


# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    _check_env()

    kite_client = KiteConnect(api_key=API_KEY)
    kite_client.set_access_token(ACCESS_TOKEN)

    print("=" * 60)
    print("  Nifty 50 Closing Price Estimator  —  Railway Edition")
    print("=" * 60)
    print(f"  Port           : {PORT}")
    print(f"  Constituents   : {len(NIFTY50)} stocks")
    print(f"  Settlement win : {SETTLEMENT_START[0]:02d}:{SETTLEMENT_START[1]:02d} "
          f"– {SETTLEMENT_END[0]:02d}:{SETTLEMENT_END[1]:02d} IST")

    # 1. Fetch T-1 closes (blocks until done)
    fetch_prev_closes(kite_client)

    # 2. Start WebSocket in background thread
    print("[main] Starting Kite WebSocket...")
    start_websocket()

    # 3. Start history accumulator
    threading.Thread(target=_history_loop, daemon=True).start()

    # 4. Serve Flask (Railway expects the process to bind to $PORT)
    print(f"[main] Flask listening on 0.0.0.0:{PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
