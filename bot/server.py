from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(_REPO_ROOT, ".env"))

from .notify import send_email, send_twilio_sms
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
)

ET = ZoneInfo("America/New_York")
app = Flask(__name__)

DASHBOARD_DIR = os.path.join(os.path.dirname(__file__), "..", "dashboard")

SCAN_INTERVAL_SEC = float(os.environ.get("SCAN_INTERVAL_SEC", "60"))
STALE_WINDOW_SEC = int(os.environ.get("SIGNAL_STALE_WINDOW_SEC", "300"))
SIGNAL_RR_DEFAULT = float(os.environ.get("SIGNAL_RR_DEFAULT", "1.0"))

DEFAULT_ENABLED = os.environ.get("SIGNALS_ENABLED", "1").strip().lower() in ("1", "true", "yes")

# Ranker (backtests) - used by dashboard symbol rankings
RANKER_ENABLED = os.environ.get("RANKER_ENABLED", "1").strip().lower() in ("1", "true", "yes")
RANKER_INTERVAL_SEC = float(os.environ.get("RANKER_INTERVAL_SEC", "900"))  # 15 min
BACKTEST_DAYS_BACK = int(os.environ.get("BACKTEST_DAYS_BACK", "183"))
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

                signals_by_type: dict[str, list] = {}
                if is_london_kz(now_et) or is_ny_kz(now_et):
                    signals_by_type["KZ"] = detect_kz_signals(candles) or []
                if is_ny_open(now_et):
                    signals_by_type["ORB"] = detect_orb_signals(candles) or []
                if is_london_pine(now_et):
                    signals_by_type["ASHL"] = detect_pine_ashl_signals(candles) or []
                if is_ny_pine(now_et):
                    signals_by_type["LRNY"] = detect_pine_lrny_signals(candles) or []

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
        _background_started = True
        log("Signals scanner started.", "SYSTEM")
        if RANKER_ENABLED:
            _ranker_log("Ranker thread started.", "SYSTEM")
        else:
            _ranker_log("Ranker disabled (set RANKER_ENABLED=1 to enable).", "SYSTEM")


@app.before_request
def _lazy_start_background() -> None:
    start_background_tasks()


@app.route("/")
def dashboard():
    return send_from_directory(DASHBOARD_DIR, "index.html")


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
        "base_min_trades": float(os.environ.get("BASE_MIN_TRADES", "20")),
        "elite_min_expectancy": float(os.environ.get("ELITE_MIN_EXPECTANCY", "1.00")),
        "elite_min_winrate": float(os.environ.get("ELITE_MIN_WINRATE", "30")),
        "elite_min_trades": float(os.environ.get("ELITE_MIN_TRADES", "10")),
        "super_min_expectancy": float(os.environ.get("SUPER_MIN_EXPECTANCY", "1.00")),
        "super_min_winrate": float(os.environ.get("SUPER_MIN_WINRATE", "60")),
        "super_min_trades": float(os.environ.get("SUPER_MIN_TRADES", "20")),
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
    return jsonify({"response": "Assistant chat is disabled in Morgan Futures signal-only mode."})


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


if __name__ == "__main__":
    start_background_tasks()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=False)
