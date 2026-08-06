from __future__ import annotations

import os
import secrets
import threading
import time
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
import requests

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(_REPO_ROOT, ".env"))

from .notify import send_email, send_twilio_sms
from .contact_store import load_contact, save_contact
from .rithmic_executor import RithmicExecutor
from .challenge_manager import ChallengeManager
from .paper_engine import PaperEngine
from .copy_router import CopyRouter
from .signals import (
    SYMBOLS,
    OHLC_INTERVAL_MINUTES,
    backtest_symbol,
    fetch_candles_live,
    detect_kz_signals,
    detect_orb_signals,
    detect_pine_ashl_signals,
    detect_pine_lrny_signals,
    is_asia_pine,
    is_london_kz,
    is_ny_kz,
    is_ny_open,
    is_london_pine,
    is_ny_pine,
    is_asia_scalp_trade,
)

ET = ZoneInfo("America/New_York")
app = Flask(__name__)

# ── Basic auth (only active when DASH_PASSWORD is set) ─────────────────────
DASH_USER = os.environ.get("DASH_USER", "morgan")
DASH_PASSWORD = os.environ.get("DASH_PASSWORD", "").strip()


@app.before_request
def _require_basic_auth():
    if not DASH_PASSWORD:
        return None
    auth = request.authorization
    if auth and auth.username == DASH_USER and secrets.compare_digest(
        auth.password or "", DASH_PASSWORD
    ):
        return None
    return (
        "Authentication required",
        401,
        {"WWW-Authenticate": 'Basic realm="Morgan Futures"'},
    )

# ── Live trading (Rithmic) ─────────────────────────────────────────────────
LIVE_TRADING_ENABLED = os.environ.get("LIVE_TRADING_ENABLED", "0").strip() == "1"
TRADE_RISK_DOLLARS = float(os.environ.get("TRADE_RISK_DOLLARS", "200"))

# Approved strategy+symbol combos for live execution (≥50% backtest win rate)
_APPROVED_LIVE: set[tuple[str, str]] = {
    ("RTY=F", "ORB"), ("ES=F",  "ORB"), ("MES=F", "ORB"),
    ("YM=F",  "ORB"), ("MYM=F", "ORB"),
    ("YM=F",  "LRNY"), ("MYM=F", "LRNY"),
}

_rithmic = RithmicExecutor()
_challenge = ChallengeManager()


def _rithmic_fill_callback(trade: dict) -> None:
    """Invoked by RithmicExecutor when a live trade closes (SL or TP hit)."""
    pnl = float(trade.get("pnl_usd", 0.0))
    _challenge.record_trade(pnl)
    log(
        f"[LIVE] Closed {trade.get('yahoo_sym')} {trade.get('exit_reason')} "
        f"pnl=${pnl:.2f}",
        "EXECUTED",
    )

# Runtime live-trading override (None = use env var; True/False = UI toggle)
_live_override: bool | None = None


def _is_live_enabled() -> bool:
    return _live_override if _live_override is not None else LIVE_TRADING_ENABLED

DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "..", "dashboard")

SCAN_INTERVAL_SEC = float(os.environ.get("SCAN_INTERVAL_SEC", "60"))
STALE_WINDOW_SEC = int(os.environ.get("SIGNAL_STALE_WINDOW_SEC", "300"))
SIGNAL_RR_DEFAULT = float(os.environ.get("SIGNAL_RR_DEFAULT", "1.0"))

DEFAULT_ENABLED = os.environ.get("SIGNALS_ENABLED", "1").strip().lower() in ("1", "true", "yes")

# Ranker (backtests) - used by dashboard symbol rankings
RANKER_ENABLED = os.environ.get("RANKER_ENABLED", "1").strip().lower() in ("1", "true", "yes")
RANKER_INTERVAL_SEC = float(os.environ.get("RANKER_INTERVAL_SEC", "900"))  # 15 min
BACKTEST_DAYS_BACK = int(os.environ.get("BACKTEST_DAYS_BACK", "30"))
# Default low enough that intraday sources (e.g. Yahoo 5m) can qualify symbols
# without requiring user-provided multi-month CSV history.
BACKTEST_MIN_CANDLES = int(os.environ.get("BACKTEST_MIN_CANDLES", "300"))

state: dict = {
    "signals_enabled": DEFAULT_ENABLED,
    "trading_enabled": DEFAULT_ENABLED,
    "scanner_status": "INITIALIZING",
    "last_scan": None,
    "last_updated": datetime.now(timezone.utc).isoformat(),
    "recent_signals": [],
    "decision_log": [],
    "diagnostics": {},
}

_fired: dict[str, float] = {}

_rankings_lock = threading.Lock()
_rankings: dict = {}
_ranker_last_run_utc: str | None = None

# =====================
# PAPER TRADING (SIMULATED)
# =====================

CONTRACT_SPECS: dict[str, dict] = {
    # Equity indices
    "ES": {"tick_size": 0.25, "tick_value": 12.50},
    "NQ": {"tick_size": 0.25, "tick_value": 5.00},
    "YM": {"tick_size": 1.0, "tick_value": 5.00},
    "RTY": {"tick_size": 0.10, "tick_value": 5.00},
    "MES": {"tick_size": 0.25, "tick_value": 1.25},
    "MNQ": {"tick_size": 0.25, "tick_value": 0.50},
    "MYM": {"tick_size": 1.0, "tick_value": 0.50},
    # Energy
    "CL": {"tick_size": 0.01, "tick_value": 10.00},
    # Metals
    "GC": {"tick_size": 0.10, "tick_value": 10.00},
    "MGC": {"tick_size": 0.10, "tick_value": 1.00},
    # Rates
    "ZB": {"tick_size": 0.03125, "tick_value": 31.25},
    "ZN": {"tick_size": 0.015625, "tick_value": 15.625},
}

# ── Paper desk ─────────────────────────────────────────────────────────────
# The simulated venue lives in bot/paper_engine.py: real order types
# (MARKET / LIMIT / STOP / STOP_LIMIT / TRAILING_STOP), brackets, OCO groups,
# per-contract commission and configurable slippage — the two things
# TradingView's paper trading does not model at all.
#
# price_fn is a lambda because _mark_price is defined further down this module;
# the engine resolves it lazily on every tick.
paper = PaperEngine(
    starting_balance=float(os.environ.get("PAPER_START_BALANCE", "50000")),
    commission_per_contract=float(os.environ.get("PAPER_COMMISSION", "2.50")),
    slippage_ticks=float(os.environ.get("PAPER_SLIPPAGE_TICKS", "0")),
    price_fn=lambda s: _mark_price(s),
)

# ── Copy trading ───────────────────────────────────────────────────────────
# Fans the trader's OWN signals across the trader's OWN prop-firm accounts,
# through a pre-trade compliance gate (trailing DD, daily loss, max size,
# no-hedge, consistency). It never mirrors anyone else's signals — doing so
# would cross out of the self-executing 17 CFR 4.14(a)(9) exemption.
copy_router = CopyRouter(executor=_rithmic, paper_engine=paper)


def _now_day_id_et() -> int:
    n = datetime.now(ET)
    return n.year * 10000 + n.month * 100 + n.day


_mark_cache_lock = threading.Lock()
_mark_cache: dict[str, dict] = {}




def _mark_price_polygon(symbol: str) -> float | None:
    sym = str(symbol or '').strip().upper()
    if not sym:
        return None
    api_key = str(os.environ.get('POLYGON_API_KEY') or '').strip()
    if not api_key:
        return None
    try:
        contract = _polygon_contract_from_base(sym)
    except Exception:
        return None
    url = f"{_POLYGON_BASE}/futures/vX/aggs/{contract}"
    params = {"resolution": "1min", "limit": 5, "sort": "window_start.desc", "apiKey": api_key}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            return None
        j = r.json() or {}
        rows = j.get('results') or []
        for row in rows:
            c = row.get('close')
            if c is None:
                continue
            return float(c)
    except Exception:
        return None
    return None
def _mark_price_quick(symbol: str) -> float | None:
    """
    Fast mark price fetch via Yahoo chart endpoint.

    We avoid calling yfinance from request handlers because it can stall
    for 20-60s due to rate limiting / HTML responses.
    """
    sym = str(symbol or "").strip()
    if not sym:
        return None
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
    params = {"range": "1d", "interval": f"{int(OHLC_INTERVAL_MINUTES)}m", "includePrePost": "false"}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        r = requests.get(url, params=params, headers=headers, timeout=8)
        if r.status_code != 200:
            return None
        data = r.json()
        res = (((data or {}).get("chart") or {}).get("result") or [None])[0] or {}
        q = (((res.get("indicators") or {}).get("quote") or [None])[0]) or {}
        closes = q.get("close") or []
        # pick last non-null close
        for v in reversed(closes):
            if v is None:
                continue
            return float(v)
        return None
    except Exception:
        return None


def _mark_price(symbol: str) -> float | None:
    sym = str(symbol or "").strip()
    if not sym:
        return None
    now = time.time()
    with _mark_cache_lock:
        cached = _mark_cache.get(sym) or {}
        ts = float(cached.get("ts") or 0.0)
        px = cached.get("px")
        if px is not None and (now - ts) < float(os.environ.get("MARK_CACHE_TTL_SEC", "3.0")):
            try:
                return float(px)
            except Exception:
                pass

    px = _mark_price_polygon(sym) if '=' not in sym else None
    if px is None:
        px = _mark_price_quick(sym)
    if px is None:
        # last resort (slower): existing live candles helper
        try:
            candles = fetch_candles_live(sym, interval=OHLC_INTERVAL_MINUTES, limit=3) or []
            if candles:
                px = float(candles[-1]["close"])
        except Exception:
            px = None
    if px is None:
        return None
    with _mark_cache_lock:
        _mark_cache[sym] = {"ts": now, "px": float(px)}
    return float(px)


def _spec(symbol: str) -> dict:
    return CONTRACT_SPECS.get(symbol, {"tick_size": 0.25, "tick_value": 1.0})


def _contract_base(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    return s[:-2] if s.endswith("=F") else s


def _pnl_usd(symbol: str, side: str, entry: float, mark: float, qty: int) -> float:
    sp = _spec(_contract_base(symbol))
    tick_size = float(sp.get("tick_size") or 0.25)
    tick_value = float(sp.get("tick_value") or 1.0)
    if tick_size <= 0:
        tick_size = 0.25
    ticks = (mark - entry) / tick_size
    if str(side).upper() == "SELL":
        ticks = -ticks
    return float(ticks) * tick_value * int(qty)


def _drain_paper_events(events: list) -> None:
    """Mirror engine events into the decision log + challenge metrics.

    The engine is deliberately self-contained (no imports from this module), so
    the side effects the old inline paper code did on close — challenge
    bookkeeping and the [SIM] log line — are re-attached here.
    """
    for ev in events or []:
        try:
            if ev.get("type") != "CLOSE":
                continue
            t = ev.get("trade") or {}
            net = float(t.get("net_pnl") or 0.0)
            log(
                f"[SIM] Closed {t.get('symbol')} {t.get('side')} x{t.get('qty')} "
                f"{t.get('reason')} net=${net}",
                "EXECUTED",
            )
            # Keep Lucid/challenge metrics in sync with SIM trades, as before.
            _challenge.record_trade(net)
        except Exception:
            continue


def paper_loop() -> None:
    """Drive the simulated venue off live marks.

    tick_all() pulls a price for every symbol with a working order or an open
    position and returns the resulting fill/close/trigger events.
    """
    while True:
        try:
            _drain_paper_events(paper.tick_all())
        except Exception as e:
            log(f"paper loop error: {e}", "ERROR")
        time.sleep(float(os.environ.get("PAPER_TICK_SEC", "2.0")))


def log(message: str, level: str = "INFO") -> None:
    state.setdefault("decision_log", [])
    state["decision_log"].insert(0, {
        "time": datetime.now(timezone.utc).isoformat(),
        "message": str(message),
        "decision": str(level),
    })
    state["decision_log"] = state["decision_log"][:300]
    print(f"[{level}] {message}")


def _is_weekday_session(now_et: datetime) -> bool:
    return now_et.weekday() <= 4


def _active_windows(now_et: datetime) -> bool:
    return (
        is_asia_pine(now_et)
        or is_london_kz(now_et)
        or is_ny_kz(now_et)
        or is_ny_open(now_et)
        or is_london_pine(now_et)
        or is_ny_pine(now_et)
        or is_asia_scalp_trade(now_et)
    )


def _cleanup_fired(ttl_sec: int = 4 * 3600) -> None:
    cutoff = time.time() - ttl_sec
    for k in [k for k, ts in _fired.items() if ts < cutoff]:
        _fired.pop(k, None)


def _format_email(sig: dict) -> tuple[str, str]:
    sym = sig.get("symbol")
    st = sig.get("type")
    side = sig.get("direction")
    entry = sig.get("entry")
    sl = sig.get("stop_loss")
    tp = sig.get("take_profit")
    ts_et = sig.get("time_et")

    subject = f"Morgan Futures Signal: {st} {side} {sym}"
    body = (
        f"Signal-only alert (NO auto-trading)\n\n"
        f"Symbol: {sym}\n"
        f"Type:   {st}\n"
        f"Side:   {side}\n"
        f"Time:   {ts_et} ET\n\n"
        f"Entry:  {entry}\n"
        f"SL:     {sl}\n"
        f"TP:     {tp}\n\n"
        f"Notes: This system sends trade ideas/signals only."
    )
    return subject, body


def _format_sms(sig: dict) -> str:
    return (
        f"MorganFutures {sig.get('type')} {sig.get('direction')} {sig.get('symbol')} "
        f"E:{sig.get('entry')} SL:{sig.get('stop_loss')} TP:{sig.get('take_profit')} "
        f"{sig.get('time_et')} ET"
    )


def _notify(sig: dict) -> None:
    if not load_contact() and not os.environ.get("SIGNAL_EMAIL_TO", "").strip() and not os.environ.get("SIGNAL_SMS_TO", "").strip():
        log("No contact configured; skipping notifications.", "IGNORED")
        return
    subject, body = _format_email(sig)
    em_err = send_email(subject, body)
    if em_err:
        log(f"Email not sent: {em_err}", "WARN")

    if os.environ.get("TWILIO_ACCOUNT_SID", "").strip():
        sms_err = send_twilio_sms(_format_sms(sig))
        if sms_err:
            log(f"SMS not sent: {sms_err}", "WARN")


def _add_take_profit(sig: dict) -> dict:
    try:
        entry = float(sig.get("entry"))
        sl = float(sig.get("stop_loss"))
        rr = float(sig.get("rr") or SIGNAL_RR_DEFAULT)
        rk = abs(entry - sl)
        if rk <= 0:
            return sig
        if str(sig.get("direction")).upper() == "BUY":
            tp = entry + rk * rr
        else:
            tp = entry - rk * rr
        sig["take_profit"] = round(tp, 6)
    except Exception:
        pass
    return sig


def _empty_leg() -> dict:
    return {"winrate": 0.0, "expectancy": 0.0, "recent_expectancy": 0.0, "total": 0}


def _placeholder_rankings() -> dict:
    out: dict = {}
    for sym in SYMBOLS:
        z = _empty_leg()
        out[sym] = {"KZ": z, "ORB": z, "ASHL": z, "LRNY": z, "score": 0.0}
    return out


def _ranker_log(message: str, level: str = "INFO") -> None:
    state.setdefault("ranker_log", [])
    state["ranker_log"].insert(
        0,
        {
            "time": datetime.now(timezone.utc).isoformat(),
            "message": str(message),
            "decision": str(level),
        },
    )
    state["ranker_log"] = state["ranker_log"][:300]
    print(f"[RANKER:{level}] {message}")


def _run_backtests_once() -> None:
    global _rankings, _ranker_last_run_utc
    started = time.time()
    _ranker_log(
        f"Backtests starting ({len(SYMBOLS)} symbols, min_candles={BACKTEST_MIN_CANDLES}, days_back={BACKTEST_DAYS_BACK})",
        "SYSTEM",
    )

    results: dict = {}
    ok = skipped = 0

    for sym in SYMBOLS:
        try:
            r = backtest_symbol(
                sym,
                interval=OHLC_INTERVAL_MINUTES,
                days_back=BACKTEST_DAYS_BACK,
                rr_target_kz=1.0,
                rr_target_orb=1.0,
                rr_target_ashl=1.0,
                rr_target_lrny=1.0,
                min_candles=BACKTEST_MIN_CANDLES,
            )
            if isinstance(r, dict) and r.get("_skip"):
                skipped += 1
                _ranker_log(f"{sym}: SKIP — {r.get('detail')}", "IGNORED")
                # still publish zeros so UI can show it but it will be "ignored"
                results[sym] = {"KZ": _empty_leg(), "ORB": _empty_leg(), "ASHL": _empty_leg(), "LRNY": _empty_leg(), "score": 0.0}
                continue

            ok += 1
            results[sym] = {
                "KZ": r.get("KZ") or _empty_leg(),
                "ORB": r.get("ORB") or _empty_leg(),
                "ASHL": r.get("ASHL") or _empty_leg(),
                "LRNY": r.get("LRNY") or _empty_leg(),
                "score": float(r.get("score") or 0.0),
            }
            _ranker_log(
                f"{sym}: KZ {results[sym]['KZ'].get('winrate',0)}%/{results[sym]['KZ'].get('expectancy',0)}R "
                f"ORB {results[sym]['ORB'].get('winrate',0)}%/{results[sym]['ORB'].get('expectancy',0)}R "
                f"ASHL {results[sym]['ASHL'].get('winrate',0)}%/{results[sym]['ASHL'].get('expectancy',0)}R "
                f"LRNY {results[sym]['LRNY'].get('winrate',0)}%/{results[sym]['LRNY'].get('expectancy',0)}R",
                "EXECUTED",
            )
        except Exception as e:
            _ranker_log(f"{sym}: ERROR — {e}", "ERROR")
            results[sym] = {"KZ": _empty_leg(), "ORB": _empty_leg(), "ASHL": _empty_leg(), "LRNY": _empty_leg(), "score": 0.0}

    with _rankings_lock:
        _rankings = results
        _ranker_last_run_utc = datetime.now(timezone.utc).isoformat()

    elapsed = time.time() - started
    _ranker_log(f"Backtests done: ok={ok} skipped={skipped} in {elapsed:.1f}s", "SYSTEM")


def ranker_loop() -> None:
    state["ranker_status"] = "RUNNING" if RANKER_ENABLED else "DISABLED"
    if not RANKER_ENABLED:
        return
    # run once quickly on boot
    time.sleep(1.0)
    while True:
        try:
            _run_backtests_once()
        except Exception as e:
            _ranker_log(f"Ranker loop error: {e}", "ERROR")
        time.sleep(max(30.0, float(RANKER_INTERVAL_SEC)))


def _maybe_execute_live(sig: dict) -> None:
    """
    Gate checks before sending a live order to Rithmic:
    1. Strategy must be in the approved set (≥50% backtest win rate)
    2. Challenge rules must allow more trading (DD / daily cap / target)
    3. No existing open position on the same instrument
    4. Position sized to TRADE_RISK_DOLLARS (~$200)
    """
    sym = sig.get("symbol", "")
    strategy = str(sig.get("type", "")).upper()

    if (sym, strategy) not in _APPROVED_LIVE:
        return

    allowed, reason = _challenge.can_trade()
    if not allowed:
        log(f"[LIVE] Trade blocked — {reason}", "WARN")
        return

    if _rithmic.has_open_position(sym):
        log(f"[LIVE] Skipping {sym} — position already open", "WARN")
        return

    if not _rithmic.is_connected():
        log("[LIVE] Rithmic not connected — order skipped", "WARN")
        return

    entry = float(sig.get("entry") or 0)
    sl = float(sig.get("stop_loss") or 0)
    tp = float(sig.get("take_profit") or 0)
    if not entry or not sl or not tp:
        return

    spec = CONTRACT_SPECS.get(sym.replace("=F", ""), {"tick_size": 0.25, "tick_value": 1.0})
    tick_size = float(spec.get("tick_size") or 0.25)
    tick_value = float(spec.get("tick_value") or 1.0)
    stop_ticks = abs(entry - sl) / tick_size
    if stop_ticks <= 0:
        return

    risk_per_contract = stop_ticks * tick_value
    qty = max(1, int(TRADE_RISK_DOLLARS / risk_per_contract))
    qty = min(qty, 3)  # hard cap: never more than 3 contracts on a challenge

    basket_id = _rithmic.place_bracket_order(
        yahoo_sym=sym,
        side=str(sig.get("direction", "BUY")).upper(),
        qty=qty,
        entry=entry,
        sl=sl,
        tp=tp,
    )
    if basket_id:
        log(
            f"[LIVE] Order placed: {strategy} {sig.get('direction')} {qty}x{sym} "
            f"E={entry} SL={sl} TP={tp} basket={basket_id}",
            "EXECUTED",
        )


def scanner_loop() -> None:
    while True:
        try:
            if not (state.get("signals_enabled", True) and state.get("trading_enabled", True)):
                state["scanner_status"] = "PAUSED"
                time.sleep(2)
                continue

            now_et = datetime.now(ET)
            if not _is_weekday_session(now_et):
                state["scanner_status"] = "WEEKEND (OFF)"
                time.sleep(30)
                continue

            if not _active_windows(now_et):
                state["scanner_status"] = "WAITING FOR SESSION"
                time.sleep(15)
                continue

            state["scanner_status"] = "SCANNING"
            _cleanup_fired()

            for sym in SYMBOLS:
                candles = fetch_candles_live(sym, interval=OHLC_INTERVAL_MINUTES, limit=260)
                if not candles or len(candles) < 80:
                    continue

                # Every detector sizes its sweep threshold off the contract's real
                # tick, so `symbol` has to be passed — without it they fall back to
                # a 1bp-of-price pseudo-tick, which is a fraction of a ZB tick and
                # many times an NQ tick.
                signals_by_type: dict[str, list] = {}
                if is_london_kz(now_et) or is_ny_kz(now_et):
                    signals_by_type["KZ"] = detect_kz_signals(candles, symbol=sym) or []
                if is_ny_open(now_et):
                    signals_by_type["ORB"] = detect_orb_signals(candles, symbol=sym) or []
                if is_london_pine(now_et):
                    signals_by_type["ASHL"] = detect_pine_ashl_signals(candles, symbol=sym) or []
                # LRNY now carries the Asia scalp, which trades 00:00-05:00 ET.
                # Gating it on the old 08:00-11:00 NY window meant it could never
                # fire live — the detector's own window had already closed.
                if is_asia_scalp_trade(now_et):
                    signals_by_type["LRNY"] = detect_pine_lrny_signals(candles, symbol=sym) or []

                new_sigs: list = []
                for arr in signals_by_type.values():
                    new_sigs.extend(arr)

                now_ts = int(time.time())
                for sig in new_sigs:
                    try:
                        sig_ts = int(sig.get("time") or 0)
                    except Exception:
                        continue
                    age = now_ts - sig_ts
                    if age < 0 or age > STALE_WINDOW_SEC:
                        continue

                    sig["symbol"] = sym
                    sig["time_et"] = datetime.fromtimestamp(sig_ts, tz=ET).strftime("%Y-%m-%d %H:%M")
                    sig = _add_take_profit(sig)

                    key = f"{sym}_{sig.get('type')}_{sig.get('direction')}_{sig_ts}"
                    if key in _fired:
                        continue
                    _fired[key] = time.time()

                    state["recent_signals"].insert(0, sig)
                    state["recent_signals"] = state["recent_signals"][:200]

                    log(
                        f"[SIGNAL] {sig.get('type')} {sig.get('direction')} {sym} "
                        f"entry={sig.get('entry')} sl={sig.get('stop_loss')}",
                        "SIGNAL",
                    )
                    _notify(sig)
                    if _is_live_enabled():
                        _maybe_execute_live(sig)

                time.sleep(0.25)

            state["last_scan"] = datetime.now(timezone.utc).isoformat()
            state["last_updated"] = datetime.now(timezone.utc).isoformat()

        except Exception as e:
            log(f"Scanner error: {e}", "ERROR")
            time.sleep(5)

        time.sleep(SCAN_INTERVAL_SEC)


_background_lock = threading.Lock()
_background_started = False


def start_background_tasks() -> None:
    global _background_started
    with _background_lock:
        if _background_started:
            return
        threading.Thread(target=scanner_loop, daemon=True).start()
        threading.Thread(target=ranker_loop, daemon=True).start()
        threading.Thread(target=paper_loop, daemon=True).start()
        _background_started = True
        log("Signals scanner started.", "SYSTEM")
        if RANKER_ENABLED:
            _ranker_log("Ranker thread started.", "SYSTEM")
        else:
            _ranker_log("Ranker disabled (set RANKER_ENABLED=1 to enable).", "SYSTEM")

        if LIVE_TRADING_ENABLED:
            _rithmic.start()
            _rithmic.on_fill(_rithmic_fill_callback)
            _rithmic.connect(
                user=os.environ.get("RITHMIC_USER", ""),
                password=os.environ.get("RITHMIC_PASSWORD", ""),
                system_name=os.environ.get("RITHMIC_SYSTEM_NAME", ""),
                gateway_uri=os.environ.get("RITHMIC_GATEWAY_URI", ""),
            )
            log("Rithmic live trading enabled — connecting...", "SYSTEM")
        else:
            log("Live trading DISABLED (set LIVE_TRADING_ENABLED=1 to enable).", "SYSTEM")


@app.before_request
def _lazy_start_background() -> None:
    start_background_tasks()


@app.route("/")
def dashboard():
    # Ensure dashboard updates propagate immediately (Railway/CDN + browser can cache aggressively).
    resp = send_from_directory(DASHBOARD_DIR, "index.html", max_age=0)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


# ── Mobile app, web build ──────────────────────────────────────────────────
# The Expo web export of the Morgan Futures app (tabs, Trade Desk, prop-firm
# rules). Served from this origin on purpose: the app's API client uses
# same-origin requests on web, so hosting it here sidesteps CORS entirely.
# The dashboard at "/" is the operator view; this is the product.
WEBAPP_DIR = os.path.join(os.path.dirname(__file__), "..", "webapp")


def _webapp_index():
    resp = send_from_directory(WEBAPP_DIR, "index.html", max_age=0)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


@app.route("/app")
@app.route("/app/")
def webapp_root():
    return _webapp_index()


@app.route("/app/<path:subpath>")
def webapp_route(subpath: str):
    # Expo Router owns client-side routing, so any unknown path under /app must
    # fall back to index.html rather than 404 — otherwise a refresh on a deep
    # link breaks.
    candidate = os.path.join(WEBAPP_DIR, subpath)
    if os.path.isfile(candidate):
        return send_from_directory(WEBAPP_DIR, subpath)
    return _webapp_index()


# The export references its bundle and assets by absolute path, so these have
# to live at the root rather than under /app.
@app.route("/_expo/<path:subpath>")
def webapp_expo_assets(subpath: str):
    return send_from_directory(os.path.join(WEBAPP_DIR, "_expo"), subpath)


@app.route("/assets/<path:subpath>")
def webapp_assets(subpath: str):
    return send_from_directory(os.path.join(WEBAPP_DIR, "assets"), subpath)


@app.route("/favicon.ico")
def webapp_favicon():
    return send_from_directory(WEBAPP_DIR, "favicon.ico")


@app.route("/health")
def health():
    return jsonify({"ok": True})


@app.route("/api/state")
def get_state():
    now_et = datetime.now(ET)
    diag = {
        "now_et": now_et.strftime("%Y-%m-%d %H:%M:%S"),
        "weekday_et": now_et.strftime("%a"),
        "sessions": {
            "asia": is_asia_pine(now_et),
            "london_kz": is_london_kz(now_et),
            "ny_kz": is_ny_kz(now_et),
            "ny_open": is_ny_open(now_et),
            "london_pine": is_london_pine(now_et),
            "ny_pine": is_ny_pine(now_et),
        },
        "symbols": list(SYMBOLS),
        "stale_window_sec": STALE_WINDOW_SEC,
        "scan_interval_sec": SCAN_INTERVAL_SEC,
        "signal_mode": True,
        "kraken_balance_ok": True,
        "kraken_balance_error": None,
        "min_winrate": 0,
        "min_expectancy": 0,
        # UI tiering thresholds (used in dashboard tierForLeg)
        "watchlist_min_expectancy": float(os.environ.get("WATCHLIST_MIN_EXPECTANCY", "0.15")),
        "base_min_expectancy": float(os.environ.get("BASE_MIN_EXPECTANCY", "0.20")),
        "base_min_winrate": float(os.environ.get("BASE_MIN_WINRATE", "30")),
        # Futures systems often have fewer samples per contract/session; default to lower minimums.
        "base_min_trades": float(os.environ.get("BASE_MIN_TRADES", "10")),
        "elite_min_expectancy": float(os.environ.get("ELITE_MIN_EXPECTANCY", "1.00")),
        "elite_min_winrate": float(os.environ.get("ELITE_MIN_WINRATE", "30")),
        "elite_min_trades": float(os.environ.get("ELITE_MIN_TRADES", "5")),
        "super_min_expectancy": float(os.environ.get("SUPER_MIN_EXPECTANCY", "1.00")),
        "super_min_winrate": float(os.environ.get("SUPER_MIN_WINRATE", "60")),
        "super_min_trades": float(os.environ.get("SUPER_MIN_TRADES", "10")),
    }

    base = {
        "balance": 0.0,
        "daily_start_balance": 0.0,
        "start_balance": 0.0,
        "daily_pnl": 0.0,
        "trades": [],
        "open_positions": [],
        "symbol_rankings": _placeholder_rankings(),
        "ranker_log": state.get("ranker_log", []),
        "ranker_status": state.get("ranker_status", "DISABLED"),
        "session_stats": {
            "KZ": {"wins": 0, "losses": 0, "total": 0, "pnl_r": 0.0},
            "ORB": {"wins": 0, "losses": 0, "total": 0, "pnl_r": 0.0},
            "ASHL": {"wins": 0, "losses": 0, "total": 0, "pnl_r": 0.0},
            "LRNY": {"wins": 0, "losses": 0, "total": 0, "pnl_r": 0.0},
        },
        "qualifying_for_trade": len(SYMBOLS),
    }

    # Main dashboard metrics are powered by /api/state.
    # When live trading is disabled, surface internal paper engine balances/positions here
    # so "SIM" test signals visibly change Balance / P&L / Positions in the top row.
    try:
        ps = paper.state()
        bal = float(ps.get("balance") or 0.0)
        d0 = float(ps.get("day_start_balance") or bal)
        base["balance"] = bal
        base["daily_start_balance"] = d0
        base["start_balance"] = float(ps.get("starting_balance") or 0.0) or d0 or bal
        base["daily_pnl"] = float(ps.get("day_pnl") or round(bal - d0, 2))
        base["equity"] = float(ps.get("equity") or bal)
        base["open_positions"] = list(ps.get("positions") or [])
        base["trades"] = list(ps.get("trades") or [])[:500]
    except Exception:
        pass

    out = {**base, **state}
    with _rankings_lock:
        if _rankings:
            out["symbol_rankings"] = _rankings
            out["ranker_last_run_utc"] = _ranker_last_run_utc
    out["diagnostics"] = {**diag, **(out.get("diagnostics") or {})}
    out["last_updated"] = datetime.now(timezone.utc).isoformat()
    return jsonify(out)


@app.route("/api/rankings")
def rankings():
    with _rankings_lock:
        if _rankings:
            return jsonify(_rankings)
    # If ranker hasn't produced results yet, return placeholders for now.
    return jsonify(_placeholder_rankings())


@app.route("/api/toggle", methods=["POST"])
def toggle():
    v = not bool(state.get("signals_enabled", True))
    state["signals_enabled"] = v
    state["trading_enabled"] = v
    return jsonify({"signals_enabled": v, "trading_enabled": v})


@app.route("/api/chat", methods=["POST"])
def chat():
    data = request.get_json(force=True, silent=True) or {}
    msg = str(data.get("message") or "").strip()

    with _rankings_lock:
        rankings_snapshot = dict(_rankings) if isinstance(_rankings, dict) else {}

    def _fmt_leg(leg: dict) -> str:
        if not isinstance(leg, dict):
            return "—"
        wr = leg.get("winrate", 0)
        ex = leg.get("expectancy", 0)
        rx = leg.get("recent_expectancy", ex)
        t = leg.get("total", 0)
        try:
            return f"{wr}% · exp {float(ex):.2f}R (recent {float(rx):.2f}R) · {int(t)} trades"
        except Exception:
            return f"{wr}% · exp {ex}R · {t} trades"

    def _best_symbol(rankings: dict) -> tuple[str | None, dict | None]:
        best_sym = None
        best_leg = None
        best_score = None
        for sym, r in (rankings or {}).items():
            if not isinstance(r, dict):
                continue
            legs = []
            for k in ("KZ", "ORB", "ASHL", "LRNY"):
                leg = r.get(k)
                if isinstance(leg, dict):
                    legs.append((k, leg))
            for k, leg in legs:
                try:
                    ex = float(leg.get("expectancy") or 0.0)
                    rx = float(leg.get("recent_expectancy") or ex)
                    t = int(leg.get("total") or 0)
                except Exception:
                    continue
                score = (rx, ex, t)
                if best_score is None or score > best_score:
                    best_score = score
                    best_sym = str(sym)
                    best_leg = {"leg": k, **leg}
        return best_sym, best_leg

    def _symbol_row(sym: str, r: dict) -> str:
        if not isinstance(r, dict):
            return f"{sym}: —"
        legs = []
        for k in ("KZ", "ORB", "ASHL", "LRNY"):
            if isinstance(r.get(k), dict):
                legs.append(f"{k}({_fmt_leg(r.get(k))})")
        return f"{sym}: " + (" | ".join(legs) if legs else "—")

    # Safety: never claim we can open/close trades.
    if not msg:
        return jsonify(
            {
                "response": (
                    "I can analyze the bot’s data (rankings/backtests, signals, logs, trades) and answer questions. "
                    "I cannot open/close trades.\n\n"
                    "Try: “why are symbols ignored?”, “top 5 symbols”, “ES stats”, “last ranker run”, “recent signals”."
                )
            }
        )

    q = msg.lower()

    # Quick health / status
    if any(x in q for x in ("status", "health", "running", "online")):
        return jsonify(
            {
                "response": (
                    f"Scanner: {state.get('scanner_status','—')}\n"
                    f"Ranker: {state.get('ranker_status','—')}\n"
                    f"Last scan: {state.get('last_scan') or '—'}\n"
                    f"Backtests symbols: {len(rankings_snapshot) if rankings_snapshot else 0}/{len(SYMBOLS)}"
                )
            }
        )

    # Explain ignored / no trades
    if "ignored" in q or "watchlist" in q or "not testing" in q or "0 backtest" in q:
        min_c = int(os.environ.get("BACKTEST_MIN_CANDLES", str(BACKTEST_MIN_CANDLES)))
        days = int(os.environ.get("BACKTEST_DAYS_BACK", str(BACKTEST_DAYS_BACK)))
        yf_period = os.environ.get("YF_INTRADAY_PERIOD", "30d")
        return jsonify(
            {
                "response": (
                    "Most “ignored” here means the backtester has **0 usable backtest trades** (or the leg stats don’t meet your tiers), "
                    "usually because the data fetch returned too few 5m candles.\n\n"
                    f"Current settings: BACKTEST_MIN_CANDLES={min_c}, BACKTEST_DAYS_BACK={days}, YF_INTRADAY_PERIOD={yf_period}.\n"
                    "If you want, tell me which symbol (e.g. ES, NQ, GC) and I’ll summarize its KZ/ORB/ASHL/LRNY legs."
                )
            }
        )

    # Top symbols
    if "top" in q or "best" in q or "rank" in q:
        if not rankings_snapshot:
            return jsonify({"response": "No backtest rankings yet (ranker hasn’t produced data)."})
        # pick by score field if present, else by best leg expectancy
        items = []
        for sym, r in rankings_snapshot.items():
            if not isinstance(r, dict):
                continue
            try:
                score = float(r.get("score") or 0.0)
            except Exception:
                score = 0.0
            items.append((score, str(sym), r))
        items.sort(reverse=True, key=lambda x: x[0])
        out = []
        for score, sym, r in items[:5]:
            out.append(f"- {sym} (score {score:.2f})")
        best_sym, best_leg = _best_symbol(rankings_snapshot)
        best_line = ""
        if best_sym and best_leg:
            best_line = f"\n\nBest single leg: {best_sym} {best_leg.get('leg')} — {_fmt_leg(best_leg)}"
        return jsonify({"response": "Top symbols:\n" + "\n".join(out) + best_line})

    # Per-symbol query (match by base like "ES" for "ES=F")
    bases = {s.replace("=F", ""): s for s in SYMBOLS}
    hit = None
    for base, full in bases.items():
        if base.lower() in q:
            hit = full
            break
    if hit and rankings_snapshot and hit in rankings_snapshot:
        return jsonify({"response": _symbol_row(hit, rankings_snapshot.get(hit) or {})})

    # Recent logs / signals
    if "signal" in q:
        rec = list(state.get("recent_signals") or [])[:5]
        if not rec:
            return jsonify({"response": "No recent signals recorded yet."})
        lines = []
        for s in rec:
            lines.append(
                f"- {s.get('time_et','—')} ET · {s.get('type','—')} {s.get('direction','—')} {s.get('symbol','—')} "
                f"(entry {s.get('entry','—')}, SL {s.get('stop_loss','—')})"
            )
        return jsonify({"response": "Recent signals:\n" + "\n".join(lines)})

    if "log" in q:
        logs = list(state.get("ranker_log") or [])[:8]
        if not logs:
            return jsonify({"response": "No ranker log entries yet."})
        lines = []
        for l in logs:
            lines.append(f"- {l.get('decision','—')}: {l.get('message','—')}")
        return jsonify({"response": "Ranker log (latest):\n" + "\n".join(lines)})

    # Default help
    return jsonify(
        {
            "response": (
                "Ask me about: top symbols, a symbol’s stats (ES/NQ/GC/CL/etc), ignored reasons, recent signals, ranker log, status. "
                "I can only analyze; I can’t execute trades."
            )
        }
    )


@app.route("/api/test_notify", methods=["POST"])
def test_notify():
    data = request.get_json(force=True, silent=True) or {}
    sym = data.get("symbol") or "ES=F"
    sig = {
        "symbol": sym,
        "type": "TEST",
        "direction": "BUY",
        "entry": 0,
        "stop_loss": 0,
        "take_profit": 0,
        "time_et": datetime.now(ET).strftime("%Y-%m-%d %H:%M"),
        "time": int(time.time()),
    }
    _notify(sig)
    return jsonify({"ok": True})


def _is_valid_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    local, _, domain = email.partition("@")
    return bool(local.strip()) and "." in domain and " " not in email


def _phone_to_e164(phone_raw: str) -> str | None:
    if not phone_raw:
        return None
    s = "".join(ch for ch in str(phone_raw).strip() if ch.isdigit() or ch == "+")
    if s.startswith("+"):
        digits = "".join(ch for ch in s if ch.isdigit())
        if len(digits) == 11 and digits.startswith("1"):
            return f"+{digits}"
        return None
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None


@app.route("/api/contact", methods=["GET"])
def get_contact():
    c = load_contact()
    if not c:
        return jsonify({"email": "", "phone_e164": ""})
    return jsonify({"email": c.email, "phone_e164": c.phone_e164})


@app.route("/api/contact", methods=["POST"])
def set_contact():
    data = request.get_json(force=True, silent=True) or {}
    email = str(data.get("email") or "").strip()
    phone = str(data.get("phone") or "").strip()
    if not _is_valid_email(email):
        return jsonify({"ok": False, "error": "invalid_email"}), 400
    e164 = _phone_to_e164(phone)
    if not e164:
        return jsonify({"ok": False, "error": "invalid_phone"}), 400
    c = save_contact(email=email, phone_e164=e164)
    return jsonify({"ok": True, "email": c.email, "phone_e164": c.phone_e164})


@app.route("/api/challenge")
def challenge_state():
    """Current Lucid challenge progress: balance, drawdown, consistency, status."""
    summary = _challenge.status_summary()
    summary["rithmic_connected"] = _rithmic.is_connected()
    summary["live_enabled"] = _is_live_enabled()

    # Enrich open positions with current mark price + unrealized P&L
    positions = []
    for pos in _rithmic.get_positions().values():
        p = dict(pos)
        yahoo_sym = p.get("yahoo_sym", "")
        fill_price = float(p.get("fill_price") or p.get("entry", 0))
        side = str(p.get("side", "BUY")).upper()
        qty = int(p.get("qty", 1))
        mark = _mark_price(yahoo_sym) if yahoo_sym else None
        if mark is not None:
            p["mark_price"] = round(mark, 4)
            p["unrealized_pnl_usd"] = round(
                _pnl_usd(yahoo_sym.replace("=F", ""), side, fill_price, mark, qty), 2
            )
        else:
            p["mark_price"] = None
            p["unrealized_pnl_usd"] = None
        positions.append(p)

    summary["open_positions"] = positions
    return jsonify(summary)


@app.route("/api/challenge/reset", methods=["POST"])
def challenge_reset():
    """Reset challenge state to starting conditions. Admin/testing use only."""
    _challenge.reset()
    return jsonify({"ok": True, "message": "Challenge state reset to starting conditions"})


@app.route("/api/live/toggle", methods=["POST"])
def live_toggle():
    """Toggle live trading on/off at runtime without restarting the server."""
    global _live_override
    _live_override = not _is_live_enabled()
    if _live_override:
        # Ensure Rithmic is started and connected when switching to live
        if not _rithmic.is_connected():
            _rithmic.start()
            _rithmic.on_fill(_rithmic_fill_callback)
            _rithmic.connect(
                user=os.environ.get("RITHMIC_USER", ""),
                password=os.environ.get("RITHMIC_PASSWORD", ""),
                system_name=os.environ.get("RITHMIC_SYSTEM_NAME", ""),
                gateway_uri=os.environ.get("RITHMIC_GATEWAY_URI", ""),
            )
        log("Live trading ENABLED via UI toggle", "SYSTEM")
    else:
        log("Live trading DISABLED via UI toggle (SIM mode)", "SYSTEM")
    return jsonify({"ok": True, "live_enabled": _is_live_enabled()})


@app.route("/api/signal/test", methods=["POST"])
def fire_test_signal():
    """
    Inject a fake signal into the system to verify the Rithmic connection.
    In SIM mode: logs only. In LIVE mode: sends a real bracket order to Rithmic.
    Body params: symbol, strategy, direction, entry, sl, tp
    """
    data = request.get_json(force=True, silent=True) or {}
    sym = str(data.get("symbol") or "RTY=F").strip()
    strategy = str(data.get("strategy") or "ORB").strip().upper()
    direction = str(data.get("direction") or "BUY").strip().upper()

    if sym not in SYMBOLS:
        return jsonify({"ok": False, "error": f"Unknown symbol: {sym}"}), 400
    if direction not in ("BUY", "SELL"):
        return jsonify({"ok": False, "error": "direction must be BUY or SELL"}), 400

    # Try to get entry from request; auto-calculate from mark price if not provided
    try:
        entry = float(data.get("entry") or 0)
    except Exception:
        entry = 0.0
    try:
        sl_val = float(data.get("sl") or 0)
    except Exception:
        sl_val = 0.0
    try:
        tp_val = float(data.get("tp") or 0)
    except Exception:
        tp_val = 0.0

    if not entry:
        mark = _mark_price(sym)
        if mark is None:
            return jsonify({"ok": False, "error": "no_market_data — cannot auto-fill entry price"}), 503
        spec = _spec(sym.replace("=F", ""))
        tick_size = float(spec.get("tick_size") or 0.25)
        entry = round(float(mark), 4)
        if direction == "BUY":
            sl_val = round(entry - 40 * tick_size, 4)
            tp_val = round(entry + 66 * tick_size, 4)   # ~1.65R
        else:
            sl_val = round(entry + 40 * tick_size, 4)
            tp_val = round(entry - 66 * tick_size, 4)

    rr = abs(tp_val - entry) / abs(entry - sl_val) if abs(entry - sl_val) > 0 else 1.0
    tqty = max(1, min(int(data.get("qty") or 1), 50))
    sig = {
        # Stable id, minted here at the source. The copy router dedupes fan-outs
        # on (signal_id, account_id), so a redelivered signal must carry the same
        # id or it would double-fill across every connected account.
        "id": uuid.uuid4().hex,
        "symbol": sym,
        "type": strategy,
        "direction": direction,
        "side": direction,
        "qty": tqty,
        "entry": entry,
        "stop_loss": sl_val,
        "take_profit": tp_val,
        "sl": sl_val,
        "tp": tp_val,
        "strategy": strategy,
        "rr": round(rr, 2),
        "time": int(time.time()),
        "ts": time.time(),
        "time_et": datetime.now(ET).strftime("%Y-%m-%d %H:%M"),
        "test_signal": True,
    }

    # Always surface in the UI signal feed
    state["recent_signals"].insert(0, sig)
    state["recent_signals"] = state["recent_signals"][:200]
    log(f"[TEST] {strategy} {direction} {sym} E={entry} SL={sl_val} TP={tp_val}", "SIGNAL")

    mode = "LIVE" if _is_live_enabled() else "SIM"
    paper_pos: dict | None = None
    if _is_live_enabled():
        _maybe_execute_live(sig)
    else:
        # Mirror into the paper engine so P&L / SL / TP can be exercised without Rithmic.
        try:
            res = paper.place_order({
                "symbol": sym,
                "side": direction,
                "qty": tqty,
                "type": "MARKET",
                "bracket": {"sl_price": sl_val, "tp_price": tp_val},
                "tag": strategy,
            })
            paper_pos = res.get("position")
            _drain_paper_events(res.get("events") or [])
        except Exception as e:
            log(f"test signal paper mirror failed: {e}", "ERROR")

    # Fan out to the trader's other prop-firm accounts, compliance-gated.
    copy_results: list = []
    try:
        fan = copy_router.route_signal(sig)
        copy_results = fan.get("results") or []
    except Exception as e:
        log(f"copy fan-out failed: {e}", "ERROR")

    return jsonify({
        "ok": True,
        "mode": mode,
        "signal": sig,
        "paper_position": paper_pos,
        "copy_results": copy_results,
    })


@app.route("/api/paper/state")
def paper_state():
    out = paper.state()
    out["symbols"] = list(SYMBOLS)
    return jsonify(out)


@app.route("/api/paper/order", methods=["POST"])
def paper_order():
    """Place a paper order.

    Accepts the full ticket (type / limit_price / stop_price / trail_ticks /
    tif / bracket) and still accepts the old flat {stop_loss, take_profit}
    payload, which the engine converts into a bracket.
    """
    data = request.get_json(force=True, silent=True) or {}
    sym = str(data.get("symbol") or "").strip()
    side = str(data.get("side") or "BUY").strip().upper()

    if sym not in SYMBOLS:
        return jsonify({"ok": False, "error": "unknown_symbol"}), 400
    if side not in ("BUY", "SELL"):
        return jsonify({"ok": False, "error": "bad_side"}), 400

    payload = dict(data)
    payload["symbol"] = sym
    payload["side"] = side
    payload["qty"] = max(1, min(int(data.get("qty") or 1), 50))

    res = paper.place_order(payload)
    _drain_paper_events(res.get("events") or [])
    return jsonify(res), (200 if res.get("ok") else 400)


@app.route("/api/paper/close", methods=["POST"])
def paper_close():
    data = request.get_json(force=True, silent=True) or {}
    pos_id = str(data.get("id") or "").strip()
    if not pos_id:
        return jsonify({"ok": False, "error": "missing_id"}), 400

    qty = data.get("qty")
    res = paper.close_position(pos_id, float(qty) if qty is not None else None)
    _drain_paper_events(res.get("events") or [])
    if not res.get("ok"):
        return jsonify(res), 404
    # Legacy shape: the mobile client reads {trade}.
    return jsonify(res)


@app.route("/api/paper/order/cancel", methods=["POST"])
def paper_order_cancel():
    data = request.get_json(force=True, silent=True) or {}
    oid = str(data.get("id") or "").strip()
    if not oid:
        return jsonify({"ok": False, "error": "missing_id"}), 400
    res = paper.cancel_order(oid)
    return jsonify(res), (200 if res.get("ok") else 404)


@app.route("/api/paper/order/modify", methods=["POST"])
def paper_order_modify():
    data = request.get_json(force=True, silent=True) or {}
    oid = str(data.get("id") or "").strip()
    if not oid:
        return jsonify({"ok": False, "error": "missing_id"}), 400
    res = paper.modify_order(oid, {k: v for k, v in data.items() if k != "id"})
    return jsonify(res), (200 if res.get("ok") else 400)


@app.route("/api/paper/cancel_all", methods=["POST"])
def paper_cancel_all():
    data = request.get_json(force=True, silent=True) or {}
    sym = str(data.get("symbol") or "").strip() or None
    return jsonify(paper.cancel_all(sym))


@app.route("/api/paper/flatten", methods=["POST"])
def paper_flatten():
    res = paper.flatten_all()
    _drain_paper_events(res.get("events") or [])
    return jsonify(res)


@app.route("/api/paper/reset", methods=["POST"])
def paper_reset():
    data = request.get_json(force=True, silent=True) or {}
    bal = data.get("balance")
    return jsonify(paper.reset(float(bal) if bal is not None else None))


@app.route("/api/paper/config", methods=["POST"])
def paper_config():
    data = request.get_json(force=True, silent=True) or {}
    data.pop("price_fn", None)  # never settable over HTTP
    res = paper.configure(**data)
    return jsonify(res), (200 if res.get("ok") else 400)


# =====================
# COPY TRADING (the trader's own signals -> the trader's own prop accounts)
# =====================


@app.route("/api/copy/state")
def copy_state():
    return jsonify(copy_router.state())


@app.route("/api/copy/toggle", methods=["POST"])
def copy_toggle():
    data = request.get_json(force=True, silent=True) or {}
    return jsonify(copy_router.set_enabled(bool(data.get("enabled"))))


@app.route("/api/copy/accounts", methods=["POST"])
def copy_add_account():
    data = request.get_json(force=True, silent=True) or {}
    res = copy_router.add_account(data)
    return jsonify(res), (200 if res.get("ok", True) else 400)


@app.route("/api/copy/accounts/<account_id>", methods=["PATCH", "DELETE"])
def copy_account(account_id: str):
    if request.method == "DELETE":
        res = copy_router.remove_account(account_id)
    else:
        res = copy_router.update_account(account_id, request.get_json(force=True, silent=True) or {})
    return jsonify(res), (200 if res.get("ok", True) else 404)


@app.route("/api/copy/leader", methods=["POST"])
def copy_leader():
    data = request.get_json(force=True, silent=True) or {}
    res = copy_router.set_leader(str(data.get("account_id") or ""))
    return jsonify(res), (200 if res.get("ok", True) else 404)


@app.route("/api/copy/evaluate", methods=["POST"])
def copy_evaluate():
    """Dry run: return the compliance verdicts without routing anything."""
    data = request.get_json(force=True, silent=True) or {}
    return jsonify({"ok": True, "results": copy_router.evaluate(data)})


@app.route("/api/copy/kill", methods=["POST"])
def copy_kill():
    data = request.get_json(force=True, silent=True) or {}
    return jsonify(copy_router.kill_switch(bool(data.get("on"))))


@app.route("/api/copy/flatten", methods=["POST"])
def copy_flatten():
    data = request.get_json(force=True, silent=True) or {}
    acct = str(data.get("account_id") or "").strip() or None
    return jsonify(copy_router.flatten_all(acct))


# =====================
# MARKET DATA (Polygon Futures)
# =====================

_POLYGON_BASE = "https://api.polygon.io"

_MONTH_CODE = {
    1: "F",
    2: "G",
    3: "H",
    4: "J",
    5: "K",
    6: "M",
    7: "N",
    8: "Q",
    9: "U",
    10: "V",
    11: "X",
    12: "Z",
}

_QUARTERLY_MONTHS = (3, 6, 9, 12)


def _next_cycle_month(now_et: datetime, cycle_months: tuple[int, ...]) -> tuple[int, int]:
    """Return (month, year) for the next active contract month in a cycle."""
    m = int(now_et.month)
    y = int(now_et.year)
    # Simple roll heuristic: after mid-month, prefer next cycle month.
    if int(now_et.day) >= 16:
        m += 1
        if m == 13:
            m = 1
            y += 1

    for cm in cycle_months:
        if cm >= m:
            return cm, y
    return int(cycle_months[0]), y + 1


def _polygon_contract_from_base(base: str) -> str:
    """Map a base symbol (ES, NQ, CL, GC, etc) to a Polygon futures contract ticker."""
    b = str(base or "").strip().upper()
    if not b:
        raise ValueError("missing base")

    now_et = datetime.now(ET)

    # Indices + rates tend to be quarterly.
    if b in {"ES", "NQ", "YM", "RTY", "ZB", "ZN", "MES", "MNQ", "MYM"}:
        month, year = _next_cycle_month(now_et, _QUARTERLY_MONTHS)
    else:
        # Metals / energy: allow monthly.
        month, year = _next_cycle_month(now_et, tuple(range(1, 13)))

    code = _MONTH_CODE[int(month)]
    yy = str(year)[-1]
    return f"{b}{code}{yy}"


def _tf_to_polygon_resolution(tf: str) -> str:
    t = str(tf or "5m").strip().lower()
    if t in ("1m", "1min", "1minute"):
        return "1min"
    if t in ("5m", "5min", "5minute"):
        return "5min"
    if t in ("30m", "30min", "30minute"):
        return "30min"
    if t in ("1h", "60m", "1hour"):
        return "1hour"
    # default
    return "5min"


def _ns_to_epoch_seconds(v: int) -> int:
    # Polygon futures uses nanoseconds in window_start.
    # Guard in case of ms.
    if v > 10_000_000_000_000:  # > 1e13
        return int(v // 1_000_000_000)
    return int(v // 1000)


@app.route("/api/market/candles")
def api_market_candles():
    base = str(request.args.get("symbol") or "").strip().upper()
    tf = str(request.args.get("tf") or "5m").strip().lower()
    limit = int(request.args.get("limit") or 600)
    limit = max(50, min(limit, 5000))
    if not base:
        return jsonify({"ok": False, "error": "missing_symbol"}), 400
    cache = app.config.setdefault("_yahoo_candles_cache", {})
    ttl_sec = float(os.environ.get("CANDLES_CACHE_TTL_SEC", "20"))
    key = (base, tf, limit)
    import time as _time
    now = _time.time()
    hit = cache.get(key)
    if hit and (now - float(hit.get("t") or 0)) < ttl_sec:
        return jsonify(hit.get("v"))

    def _yahoo_chart_params(x: str) -> tuple[str, str]:
        """Map UI timeframe to Yahoo `interval` + `range` (best-effort; Yahoo has no true tick tape)."""
        x = str(x or "5m").strip().lower()
        # Intraday minutes (Yahoo: 1m,2m,5m,15m,30m,60m,90m — others map to nearest).
        if x in ("1m", "1min", "1minute"):
            return "1m", os.environ.get("YF_RANGE_1M", "5d")
        if x in ("2m", "2min"):
            return "2m", os.environ.get("YF_RANGE_2M", "30d")
        if x in ("3m", "3min", "4m", "4min"):
            return "5m", os.environ.get("YF_RANGE_5M", "60d")
        if x in ("5m", "5min", "5minute"):
            return "5m", os.environ.get("YF_RANGE_5M", "60d")
        if x in ("10m", "10min"):
            return "15m", os.environ.get("YF_RANGE_15M", "60d")
        if x in ("15m", "15min", "15minute"):
            return "15m", os.environ.get("YF_RANGE_15M", "60d")
        if x in ("20m", "20min"):
            return "30m", os.environ.get("YF_RANGE_30M", "60d")
        if x in ("30m", "30min", "30minute"):
            return "30m", os.environ.get("YF_RANGE_30M", "60d")
        if x in ("45m", "45min"):
            return "60m", os.environ.get("YF_RANGE_1H", "730d")
        # Hours
        if x in ("1h", "60m", "1hour"):
            return "60m", os.environ.get("YF_RANGE_1H", "730d")
        if x in ("90m", "90min", "1h30m"):
            return "90m", os.environ.get("YF_RANGE_90M", "730d")
        if x in ("2h", "120m", "2hour", "3h", "180m", "4h", "240m", "4hour", "5h", "300m"):
            # Yahoo has no 2–5h bar; hourly bars — zoom on chart for “feel”.
            return "1h", os.environ.get("YF_RANGE_MULTIH", "2y")
        # Days / weeks / months / years (aggregated OHLC, not ticks)
        if x in ("1d", "1day", "d", "day"):
            return "1d", os.environ.get("YF_RANGE_1D", "5y")
        if x in ("5d", "5day"):
            return "5d", os.environ.get("YF_RANGE_5D", "5y")
        if x in ("1w", "1wk", "week", "weekly"):
            return "1wk", os.environ.get("YF_RANGE_1W", "5y")
        if x in ("1mo", "1mth", "1month", "monthly"):
            return "1mo", os.environ.get("YF_RANGE_1MO", "max")
        if x in ("3mo", "3mth", "quarter"):
            return "3mo", os.environ.get("YF_RANGE_3MO", "max")
        if x in ("6mo", "6mth"):
            return "3mo", os.environ.get("YF_RANGE_6MO", "max")
        if x in ("1y", "12mo", "1yr"):
            return "1mo", os.environ.get("YF_RANGE_1Y", "10y")
        if x in ("2y", "24mo"):
            return "1mo", os.environ.get("YF_RANGE_2Y", "max")
        if x in ("5y", "5yr"):
            return "3mo", os.environ.get("YF_RANGE_5Y", "max")
        if x in ("10y", "10yr", "max"):
            return "3mo", "max"
        return "5m", os.environ.get("YF_RANGE_5M", "60d")

    def tf_to_interval(x: str) -> str:
        return _yahoo_chart_params(x)[0]

    def tf_to_range(x: str) -> str:
        return _yahoo_chart_params(x)[1]

    def candidates(sym: str) -> list[str]:
        sym = str(sym or "").strip().upper()
        out: list[str] = []

        def push(v: str) -> None:
            v = str(v or "").strip().upper()
            if v and v not in out:
                out.append(v)

        push(sym)
        if sym.endswith("=F"):
            push(sym[:-2])
        else:
            push(sym + "=F")
        return out

    def fetch_chart(sym: str) -> list[dict]:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
        params = {
            "range": tf_to_range(tf),
            "interval": tf_to_interval(tf),
            "includePrePost": "false",
            "events": "div|split|earn",
        }
        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"}
        import requests as _requests

        r = _requests.get(url, params=params, headers=headers, timeout=20)
        r.raise_for_status()
        j = r.json() or {}
        res0 = (((j.get("chart") or {}).get("result") or [None])[0]) or {}
        ts = res0.get("timestamp") or []
        ind = (((res0.get("indicators") or {}).get("quote") or [None])[0]) or {}
        o = ind.get("open") or []
        hi = ind.get("high") or []
        lo = ind.get("low") or []
        cl = ind.get("close") or []
        vol = ind.get("volume") or []
        out: list[dict] = []
        for i, t0 in enumerate(ts):
            try:
                oo, hh, ll, cc = o[i], hi[i], lo[i], cl[i]
                if oo is None or hh is None or ll is None or cc is None:
                    continue
                out.append(
                    {
                        "time": int(t0),
                        "open": float(oo),
                        "high": float(hh),
                        "low": float(ll),
                        "close": float(cc),
                        "volume": float(vol[i]) if i < len(vol) and vol[i] is not None else 0.0,
                    }
                )
            except Exception:
                continue
        return out

    last = None
    candles: list[dict] = []
    resolved = None
    for sym in candidates(base):
        try:
            rows = fetch_chart(sym)
            if rows:
                candles = rows[-limit:]
                resolved = sym
                break
            last = "empty"
        except Exception as e:
            last = str(e)

    if not candles:
        return jsonify({"ok": False, "error": "no_data", "details": last, "symbol": base, "tf": tf}), 502

    out = {"ok": True, "symbol": base, "resolved": resolved, "tf": tf, "candles": candles}
    cache[key] = {"t": now, "v": out}
    return jsonify(out)
if __name__ == "__main__":
    start_background_tasks()
    # Threaded dev server so background ranker/paper loops don't starve HTTP handlers.
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=False, threaded=True)
