"""
Nifty 50 Closing Price Estimator — Railway Edition
====================================================
Mirrors NSE's official closing methodology:
  • Resolves all 50 constituent instrument tokens live from Kite at startup
    (no hand-typed tokens — eliminates stale/wrong-token bugs)
  • Streams all 50 Nifty constituent stocks via Kite WebSocket (MODE_FULL)
  • Accumulates per-stock VWAP during settlement window (3:00–3:30 PM IST)
  • Projects closing index = Σ(weight_i × vwap_i / prev_close_i) × prev_nifty_close
  • Before 3 PM: uses LTP as a live proxy
  • Flags (and excludes) any stock whose price/prev_close ratio falls outside
    a ±15% sanity band — almost always a sign of a bad token/price pairing,
    not a real single-day move

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
# Symbol + weight only — NO hand-typed tokens. Tokens are resolved at startup
# directly from kite.instruments("NSE"), which is the only reliable source.
# (An earlier version of this file hand-typed instrument tokens from memory;
#  several were wrong/stale, causing missing stocks and a corrupted projection.
#  Resolving tokens live from Kite eliminates that failure mode entirely.)
#
# Weight = approximate free-float index weight (%) — update quarterly from the
# NSE factsheet: https://www.niftyindices.com/indices/equity/broad-based-indices/NIFTY-50
#
# Weights intentionally sum to ~100%. Missing weight is filled with ratio=1.0
# (no-move assumption) so the estimate degrades gracefully if ticks are absent.

NIFTY50_WEIGHTS = {
    "RELIANCE":   9.30,
    "HDFCBANK":   8.20,
    "ICICIBANK":  7.30,
    "INFY":       5.80,
    "TCS":        4.90,
    "BHARTIARTL": 3.90,
    "ITC":        3.60,
    "HINDUNILVR": 2.90,
    "LT":         2.80,
    "SBIN":       2.70,
    "BAJFINANCE": 2.60,
    "M&M":        2.30,
    "KOTAKBANK":  2.40,
    "AXISBANK":   2.20,
    "MARUTI":     2.10,
    "SUNPHARMA":  2.00,
    "TATAMOTORS": 1.90,
    "WIPRO":      1.80,
    "ULTRACEMCO": 1.70,
    "ASIANPAINT": 1.60,
    "POWERGRID":  1.50,
    "NTPC":       1.50,
    "ONGC":       1.40,
    "JSWSTEEL":   1.40,
    "TATASTEEL":  1.30,
    "HCLTECH":    1.30,
    "ADANIENT":   1.20,
    "ADANIPORTS": 1.20,
    "BAJAJFINSV": 1.10,
    "EICHERMOT":  1.10,
    "GRASIM":     1.00,
    "TITAN":      1.00,
    "TECHM":      0.95,
    "TATACONSUM": 0.90,
    "HINDALCO":   0.90,
    "INDUSINDBK": 0.90,
    "DRREDDY":    0.85,
    "COALINDIA":  0.85,
    "BEL":        0.80,
    "CIPLA":      0.80,
    "BRITANNIA":  0.75,
    "DIVISLAB":   0.75,
    "NESTLEIND":  0.70,
    "BAJAJ-AUTO": 0.70,
    "SHRIRAMFIN": 0.65,
    "HEROMOTOCO": 0.65,
    "SBILIFE":    0.60,
    "TRENT":      0.60,
    "HDFCLIFE":   0.55,
    "APOLLOHOSP": 0.55,
}

NIFTY_INDEX_SYMBOL = "NIFTY 50"   # NSE:NIFTY 50 — fixed index name, used to resolve its token too

# Populated at startup by resolve_tokens(). Do not hand-edit.
NIFTY50 = {}             # token -> {"symbol": ..., "weight": ...}
NIFTY_INDEX_TOKEN = None # resolved at startup
RATIO_SANITY_BAND = (0.85, 1.15)  # flag any stock whose price/prev_close ratio falls outside ±15%


def resolve_tokens(kite_client: KiteConnect) -> dict:
    """
    Resolve instrument tokens for all NIFTY50_WEIGHTS symbols (+ the index itself)
    directly from Kite's live NSE instrument dump. This replaces any hand-typed
    token list and is the single source of truth for tokens going forward.

    Returns the resolved {token: {"symbol", "weight"}} dict and also sets the
    module-level NIFTY50 / NIFTY_INDEX_TOKEN globals.

    Retries every 30 s on failure (e.g. network blip at startup).
    """
    global NIFTY50, NIFTY_INDEX_TOKEN

    while True:
        try:
            print("[resolve_tokens] Fetching NSE instrument dump from Kite...")
            instruments = kite_client.instruments("NSE")
            sym_to_token = {
                i["tradingsymbol"]: i["instrument_token"]
                for i in instruments
                if i["segment"] == "NSE"
            }
            break
        except Exception as e:
            print(f"[resolve_tokens] Error fetching instruments: {e} — retrying in 30 s...")
            time.sleep(30)

    resolved = {}
    missing  = []
    total_weight_resolved = 0.0

    for sym, wt in NIFTY50_WEIGHTS.items():
        token = sym_to_token.get(sym)
        if token is None:
            missing.append(sym)
            continue
        resolved[token] = {"symbol": sym, "weight": wt}
        total_weight_resolved += wt

    index_token = sym_to_token.get(NIFTY_INDEX_SYMBOL)
    if index_token is None:
        # Fallback: NSE:NIFTY 50 index token is well-known and stable.
        # Kite's instruments("NSE") dump doesn't always include index instruments
        # under the equity segment filter above, so this fallback is expected/normal.
        index_token = 256265
        print(f"[resolve_tokens] WARN: '{NIFTY_INDEX_SYMBOL}' not found in NSE equity "
              f"segment dump — using known-stable index token {index_token} instead.")

    print(f"[resolve_tokens] Resolved {len(resolved)}/{len(NIFTY50_WEIGHTS)} stocks "
          f"({total_weight_resolved:.2f}% weight)")
    if missing:
        print(f"[resolve_tokens] WARN: could not resolve tradingsymbol(s) — check for "
              f"renames/delisting: {missing}")

    NIFTY50            = resolved
    NIFTY_INDEX_TOKEN   = index_token

    _normalize_weights()
    return resolved


def _normalize_weights():
    """
    Rescale all weights in NIFTY50 so they sum to exactly 100%.

    This matters a lot: the projection formula computes
        weighted_ratio = Σ (weight_i / 100) × ratio_i
    which is only correct if Σ weight_i == 100. In practice it never is exactly
    100 — NIFTY50_WEIGHTS itself sums to ~100.45%, and any symbol that fails to
    resolve (renamed/delisted) shrinks the total further. Without normalization,
    every unresolved or excluded stock's weight effectively vanishes instead of
    being treated as "no move", which silently biases the projection — this is
    exactly what caused a -1.0% projected move while the real stocks averaged
    +0.46% and the actual Nifty spot was +0.30%.
    """
    global NIFTY50
    total = sum(meta["weight"] for meta in NIFTY50.values())
    if total <= 0:
        return
    if abs(total - 100.0) < 1e-9:
        return
    for meta in NIFTY50.values():
        meta["weight"] = meta["weight"] * 100.0 / total
    print(f"[resolve_tokens] Normalized weights: raw total was {total:.2f}% -> rescaled to 100.00%")

# ── FLASK ─────────────────────────────────────────────────────────────────────

app = Flask(__name__, static_folder="static")
CORS(app)

# ── SHARED STATE ──────────────────────────────────────────────────────────────

_lock = threading.Lock()

# Populated by init_stock_state() after resolve_tokens() runs at startup.
# (Must happen AFTER token resolution — NIFTY50 is empty until then.)
stock = {}

def init_stock_state():
    """Build the per-stock live-state dict. Call once, after resolve_tokens()."""
    global stock
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
    flagged           = []   # stocks whose ratio fell outside the sanity band
    details           = []

    lo, hi = RATIO_SANITY_BAND

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

            if ratio < lo or ratio > hi:
                # Single-day move outside ±15% is virtually always a bad token/price
                # pairing, not a real market move. Exclude it from the projection
                # rather than letting it silently distort the index.
                flagged.append({
                    "symbol": s["symbol"], "ratio": round(ratio, 4),
                    "price": price, "prev_close": prev,
                })
                weighted_ratio += (s["weight"] / 100.0) * 1.0   # no-move fallback
                row["ratio"]    = round(ratio, 6)
                row["flagged"]  = True
            else:
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
        "flagged":          flagged,
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
        ready    = sum(1 for s in stock.values() if s["prev_close"] is not None)
        ticks    = sum(s["tick_count"] for s in stock.values())
        ws_ok    = ws_state["connected"]
        last     = ws_state["last_tick_ts"]
        err      = ws_state["error"]
        proj     = _compute_projection(use_vwap=_in_settlement(datetime.now()))
        flagged  = proj["flagged"]

    return jsonify({
        "ok":                    ws_ok,
        "ws_connected":          ws_ok,
        "last_tick":             last,
        "ws_error":              err,
        "stocks_resolved":       len(NIFTY50),
        "stocks_expected":       len(NIFTY50_WEIGHTS),
        "stocks_prev_close":     ready,
        "stocks_total":          len(stock),
        "total_ticks_received":  ticks,
        "nifty_ltp":             nifty["ltp"],
        "nifty_prev_close":      nifty["prev_close"],
        "flagged_stocks":        flagged,
        "timestamp":             datetime.now().strftime("%H:%M:%S"),
    })


@app.route("/tokens")
def tokens():
    """
    Inspect resolved instrument tokens. Useful for verifying that every Nifty 50
    symbol resolved to a real Kite token, with no hand-typed guesses involved.
    """
    missing = [sym for sym in NIFTY50_WEIGHTS if sym not in {s["symbol"] for s in NIFTY50.values()}]
    return jsonify({
        "index_token":      NIFTY_INDEX_TOKEN,
        "resolved_count":   len(NIFTY50),
        "expected_count":   len(NIFTY50_WEIGHTS),
        "missing_symbols":  missing,
        "tokens": [
            {"token": token, "symbol": meta["symbol"], "weight": meta["weight"]}
            for token, meta in sorted(NIFTY50.items(), key=lambda x: -x[1]["weight"])
        ],
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
    print(f"  Settlement win : {SETTLEMENT_START[0]:02d}:{SETTLEMENT_START[1]:02d} "
          f"– {SETTLEMENT_END[0]:02d}:{SETTLEMENT_END[1]:02d} IST")

    # 1. Resolve instrument tokens live from Kite — no hand-typed tokens, ever.
    resolve_tokens(kite_client)
    init_stock_state()
    print(f"  Constituents   : {len(NIFTY50)} stocks resolved "
          f"(of {len(NIFTY50_WEIGHTS)} expected)")

    # 2. Fetch T-1 closes (blocks until done)
    fetch_prev_closes(kite_client)

    # 3. Start WebSocket in background thread
    print("[main] Starting Kite WebSocket...")
    start_websocket()

    # 4. Start history accumulator
    threading.Thread(target=_history_loop, daemon=True).start()

    # 5. Serve Flask (Railway expects the process to bind to $PORT)
    print(f"[main] Flask listening on 0.0.0.0:{PORT}")
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
