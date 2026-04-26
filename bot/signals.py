"""
signals.py v4
Fixes:
1. Paginated fetch with gap detection + real candle count validation
2. ATR averaged across session not single bar
3. Same-bar conflict resolved by open proximity
4. Adaptive swing lookback based on volatility
5. Volume confirmation on MSS bar
"""

import os
import json
import time
import statistics
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import yfinance as yf
import pandas as pd
import requests

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(_REPO_ROOT, ".env"))

from .history_loader import load_symbol_csv_5m, ensure_symbol_history_5m

ET = ZoneInfo("America/New_York")

def _flatten_yf_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [str(c[0]) if isinstance(c, tuple) else str(c) for c in df.columns]
    return df


def _yf_float(row, col: str) -> float:
    try:
        v = row[col] if col in row.index else row.get(col)
        if hasattr(v, "item"):
            v = v.item()
        return float(v)
    except Exception:
        return float("nan")

KRAKEN_BASE = "https://api.kraken.com"

# Kraken public OHLC only allows 1,5,15,30,60,240,1440,10080,21600 — interval=10 returns EGeneral:Invalid arguments.
OHLC_INTERVAL_MINUTES = 5

# Kraken REST OHLC returns at most ~720 candles (most recent only); `since` cannot
# paginate further back. See https://docs.kraken.com/api/docs/rest-api/get-ohlc-data
KRAKEN_OHLC_MAX_BARS = 720

# Optional: trim local CSV to last N calendar days of 5m bars (~6 months default).
def _local_tail_bar_count(days: int, interval: int = OHLC_INTERVAL_MINUTES) -> int:
    bars_per_day = (24 * 60) // interval
    return max(bars_per_day, int(days) * bars_per_day)


# Fallback when Kraken AssetPairs discovery fails (see server ``BOT_SYMBOLS`` override).
# CME futures universe (mini + micro where available) using Yahoo Finance futures tickers.
# These are SIGNALS ONLY (no auto-trading).
SYMBOLS = [
    # Equity index futures
    "ES=F",   # E-mini S&P 500
    "MES=F",  # Micro E-mini S&P 500
    "NQ=F",   # E-mini Nasdaq-100
    "MNQ=F",  # Micro E-mini Nasdaq-100
    "RTY=F",  # E-mini Russell 2000
    "M2K=F",  # Micro E-mini Russell 2000
    # Metals
    "GC=F",   # Gold
    "MGC=F",  # Micro Gold
    "SI=F",   # Silver
    "SIL=F",  # Micro Silver (Yahoo listing dependent)
    "HG=F",   # Copper
    # Energy
    "CL=F",   # Crude Oil
    "MCL=F",  # Micro Crude Oil
]

_env_syms = os.environ.get("BOT_SYMBOLS", "").strip()
if _env_syms:
    SYMBOLS = [s.strip() for s in _env_syms.split(",") if s.strip()]

# =====================
# DATA FETCHING ? FIX 1 — FIX 1
# =====================

def fetch_ohlc_interval(symbol, interval=OHLC_INTERVAL_MINUTES, retries=3):
    '''Fetch OHLC using Yahoo Finance intraday bars.

    Returns (candles, coverage_days, has_gaps). Best-effort gap detection.
    '''
    def _yahoo_chart(period: str) -> pd.DataFrame | None:
        """
        Fetch intraday OHLC via Yahoo's public chart endpoint.

        This avoids some common `yfinance` JSON decode failures (rate-limit / HTML responses).
        """
        sym = str(symbol)
        intv = f"{int(interval)}m"
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
        params = {
            "range": period,
            "interval": intv,
            "includePrePost": "false",
            "events": "div|split|earn",
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
        r = requests.get(url, params=params, headers=headers, timeout=30)
        if r.status_code != 200:
            return None
        try:
            data = r.json()
        except Exception:
            return None

        chart = (data or {}).get("chart") or {}
        err = chart.get("error")
        if err:
            return None
        res = (chart.get("result") or [None])[0] or {}
        ts = res.get("timestamp") or []
        ind = ((res.get("indicators") or {}).get("quote") or [None])[0] or {}
        if not ts or not ind:
            return None
        opens = ind.get("open") or []
        highs = ind.get("high") or []
        lows = ind.get("low") or []
        closes = ind.get("close") or []
        vols = ind.get("volume") or []
        rows = []
        for i, t in enumerate(ts):
            try:
                o = float(opens[i])
                hi = float(highs[i])
                lo = float(lows[i])
                cl = float(closes[i])
                vol = float(vols[i]) if i < len(vols) and vols[i] is not None else 0.0
            except Exception:
                continue
            rows.append((int(t), o, hi, lo, cl, vol))
        if not rows:
            return None

        df = pd.DataFrame(rows, columns=["time", "Open", "High", "Low", "Close", "Volume"])
        # `time` is epoch seconds already; we keep it but also create an index like yfinance would.
        df["Datetime"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert(ET)
        df = df.set_index("Datetime")
        return df[["Open", "High", "Low", "Close", "Volume"]]

    last_err = None
    yf_interval = f"{int(interval)}m"
    # yfinance allows limited intraday depth per interval (5m typically up to ~60d).
    # Use a longer default window so backtests actually have enough bars.
    yf_period = os.environ.get("YF_INTRADAY_PERIOD", "30d").strip() or "30d"
    for attempt in range(retries):
        try:
            # 1) Prefer direct Yahoo chart API.
            df = _yahoo_chart(yf_period)
            if df is None or getattr(df, "empty", False):
                # 2) Fall back to yfinance.
                df = yf.download(
                    tickers=str(symbol),
                    period=yf_period,
                    interval=yf_interval,
                    progress=False,
                    auto_adjust=False,
                    prepost=False,
                    threads=False,
                )
            # Fallback path: sometimes `download` fails with transient JSON decode errors.
            if df is None or getattr(df, "empty", False):
                try:
                    df = yf.Ticker(str(symbol)).history(
                        period=yf_period,
                        interval=yf_interval,
                        auto_adjust=False,
                        prepost=False,
                    )
                except Exception:
                    pass
            if df is None or df.empty:
                return [], 0, True

            df = _flatten_yf_columns(df)

            out = []
            for ts, row in df.iterrows():
                try:
                    dt = ts.to_pydatetime()
                    # yfinance often returns tz-naive timestamps that are *already*
                    # in the market's local timezone. Treating them as UTC shifts
                    # sessions (Asia/London/NY windows) and produces 0 signals.
                    # For our futures session logic we standardize to NY time.
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=ET)
                    dt_et = dt.astimezone(ET)
                    t0 = int(dt_et.timestamp())
                    o = _yf_float(row, "Open")
                    hi = _yf_float(row, "High")
                    lo = _yf_float(row, "Low")
                    cl = _yf_float(row, "Close")
                    vol = _yf_float(row, "Volume")
                    if not all(map(lambda x: x == x, (o, hi, lo, cl))):
                        continue
                    out.append({
                        "time": t0,
                        "open": o,
                        "high": hi,
                        "low": lo,
                        "close": cl,
                        "volume": vol if vol == vol else 0.0,
                    })
                except Exception:
                    continue

            out.sort(key=lambda x: x["time"])
            if len(out) < 50:
                return [], 0, True

            oldest = out[0]["time"]
            newest = out[-1]["time"]
            coverage_days = (newest - oldest) / 86400.0
            interval_sec = int(interval) * 60
            expected = int((newest - oldest) / interval_sec) if newest > oldest else len(out)
            actual = len(out)
            gap_pct = 1.0 - (actual / max(1, expected))
            has_gaps = gap_pct > 0.15
            return out, round(coverage_days, 1), has_gaps
        except Exception as e:
            last_err = str(e)
            # Yahoo will occasionally rate-limit/captcha. Back off harder.
            time.sleep(1.5 * (attempt + 1))

    print(f"Fetch error {symbol} [{interval}m] after {retries} tries: {last_err}")
    return [], 0, True

def fetch_candles_paginated(symbol, interval=OHLC_INTERVAL_MINUTES, days_back=90):
    """
    Fetch Kraken OHLC (5m by default). REST caps ~720 bars (~2.5d on 5m).

    ``days_back`` is desired depth for logging only; Kraken cannot return more.

    Returns (candles, coverage_days, has_gaps).
    """
    return fetch_ohlc_interval(symbol, interval=interval, retries=4)


def fetch_daily_ohlc(symbol, retries=3):
    """Daily bars used for higher-timeframe context."""
    try:
        df = yf.download(
            tickers=str(symbol),
            period="2y",
            interval="1d",
            progress=False,
            auto_adjust=False,
            prepost=False,
            threads=False,
        )
        if df is None or df.empty:
            return [], 0, True
        df = _flatten_yf_columns(df)
        out = []
        for ts, row in df.iterrows():
            try:
                dt = ts.to_pydatetime()
                # Same tz-naive handling as intraday: prefer NY time for consistency.
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ET)
                dt_et = dt.astimezone(ET)
                t0 = int(dt_et.timestamp())
                o = _yf_float(row, "Open")
                hi = _yf_float(row, "High")
                lo = _yf_float(row, "Low")
                cl = _yf_float(row, "Close")
                vol = _yf_float(row, "Volume")
                if not all(map(lambda x: x == x, (o, hi, lo, cl))):
                    continue
                out.append({
                    "time": t0,
                    "open": o,
                    "high": hi,
                    "low": lo,
                    "close": cl,
                    "volume": vol if vol == vol else 0.0,
                })
            except Exception:
                continue
        out.sort(key=lambda x: x["time"])
        if len(out) < 50:
            return [], 0, True
        oldest, newest = out[0]["time"], out[-1]["time"]
        return out, round((newest - oldest) / 86400.0, 1), False
    except Exception:
        return [], 0, True


def compute_daily_context(daily_candles, lookback_days=180):
    """
    Long-horizon stats from 1d OHLC (REST-limited but multi-month).
    ``lookback_days`` uses the last N daily bars, capped by available data.
    """
    if not daily_candles or len(daily_candles) < 14:
        return None

    subset = daily_candles[-min(lookback_days, len(daily_candles)) :]
    closes = [c["close"] for c in subset]
    if not closes or closes[0] <= 0:
        return None

    t0, t1 = subset[0]["time"], subset[-1]["time"]
    cal_days = max(1, (t1 - t0) / 86400)
    ret_pct = (closes[-1] - closes[0]) / closes[0] * 100.0

    rets = []
    for i in range(1, len(closes)):
        if closes[i - 1] > 0:
            rets.append((closes[i] - closes[i - 1]) / closes[i - 1])

    vol = round(statistics.pstdev(rets) * 100, 3) if len(rets) > 2 else 0.0
    trend_component = max(-1.0, min(1.0, ret_pct / 40.0))

    return {
        "daily_bars_used": len(subset),
        "calendar_days": round(cal_days, 1),
        "return_lookback_pct": round(ret_pct, 2),
        "daily_vol_pct": vol,
        "trend_component": round(trend_component, 3),
    }


def fetch_candles_live(symbol, interval=OHLC_INTERVAL_MINUTES, limit=300):
    """Fetch recent candles for live scanning."""
    candles, _, _ = fetch_ohlc_interval(symbol, interval=interval, retries=2)
    return (candles or [])[-limit:]


# =====================
# SESSION HELPERS
# =====================

def candle_dt(c):
    return datetime.fromtimestamp(c["time"], tz=ET)

def in_range(dt, sh, sm, eh, em):
    s = sh * 60 + sm
    e = eh * 60 + em
    n = dt.hour * 60 + dt.minute
    if e <= s:
        return n >= s or n < e
    return s <= n < e

def is_asia(dt):      return in_range(dt, 20, 0,  3, 0)
def is_london_kz(dt): return in_range(dt,  2, 0,  5, 0)
def is_ny_kz(dt):     return in_range(dt,  8,30, 11, 0)
def is_london(dt):    return in_range(dt,  2, 0,  5, 0)
def is_ny_open(dt):   return in_range(dt,  9,30, 12, 0)


# =====================
# PINE PRESET SESSIONS (Forex/Metals, NY timezone)
# Matches TradingView preset: Asia 1900-0300, London 0300-0600, NY 0800-1100.
# =====================

def is_asia_pine(dt):
    return in_range(dt, 19, 0, 3, 0)


def is_london_pine(dt):
    return in_range(dt, 3, 0, 6, 0)


def is_ny_pine(dt):
    return in_range(dt, 8, 0, 11, 0)


def pine_trading_day_id(dt: datetime, use_rollover: bool = True, rollover_hour: int = 17) -> int:
    """
    FX-style "trading day": after rollover_hour NY, calendar date shifts forward by 1 day
    (same as Pine ``tradingDayId`` with rollover).
    """
    rh = int(os.environ.get("PINE_ROLLOVER_HOUR_NY", str(rollover_hour)))
    use = os.environ.get("PINE_USE_ROLLOVER_DAY", "1").strip().lower() not in ("0", "false", "no")
    t = dt
    if use and t.hour >= rh:
        t = t + timedelta(days=1)
    return t.year * 10000 + t.month * 100 + t.day


def _pine_prev_swing_high_close(candles, idx: int, lookback: int) -> float | None:
    """Pine ``ta.highest(high[1], lookback)``: max high on bars [idx-lookback, idx-1]."""
    if idx < 1:
        return None
    start = max(0, idx - lookback)
    return max(candles[j]["high"] for j in range(start, idx))


def _detect_pine_long_only(
    candles,
    signal_type: str,
    is_ref_bar,
    is_trade_bar,
    ref_label: str,
):
    """
    Long-only sweep + MSS + reclaim, one signal per Pine trading day.
    ``is_ref_bar`` / ``is_trade_bar`` take NY ``datetime`` for the candle.
    """
    signals = []
    if len(candles) < 40:
        return signals

    swing_lb = max(2, min(int(os.environ.get("PINE_SWING_LOOKBACK", "8")), 50))
    min_sweep_ticks = max(0, int(os.environ.get("PINE_MIN_SWEEP_TICKS", "2")))
    sl_buf_ticks = max(0, int(os.environ.get("PINE_SL_BUFFER_TICKS", "2")))
    max_min_after = max(5, min(int(os.environ.get("PINE_MAX_MINUTES_AFTER", "120")), 600))
    interval = int(OHLC_INTERVAL_MINUTES)
    max_bars_after = max(1, (max_min_after + interval - 1) // interval)

    mintick = candles[-1]["close"] * 0.0001
    sweep_thresh = min_sweep_ticks * mintick
    sl_buf = sl_buf_ticks * mintick

    ref_low = ref_high = None
    prev_day_id = None
    swept_sell = False
    sweep_low = None
    prev_in_trade = False
    win_start_ts = None
    win_start_bar_idx = None
    signaled_today = False

    for i in range(20, len(candles)):
        c = candles[i]
        dt = candle_dt(c)
        day_id = pine_trading_day_id(dt)

        if prev_day_id is not None and day_id != prev_day_id:
            ref_low = ref_high = None
            signaled_today = False
            swept_sell = False
            sweep_low = None
            win_start_ts = None
            win_start_bar_idx = None
        prev_day_id = day_id

        if is_ref_bar(dt):
            ref_low = min(ref_low, c["low"]) if ref_low is not None else c["low"]
            ref_high = max(ref_high, c["high"]) if ref_high is not None else c["high"]

        in_trade = is_trade_bar(dt)
        trade_start = in_trade and not prev_in_trade
        if trade_start:
            swept_sell = False
            sweep_low = None
            win_start_ts = c["time"]
            win_start_bar_idx = i

        allow = False
        if in_trade and win_start_ts is not None and win_start_bar_idx is not None:
            elapsed_min = (c["time"] - win_start_ts) / 60.0
            bars_after = i - win_start_bar_idx
            allow = elapsed_min <= max_min_after and bars_after <= max_bars_after

        if allow and ref_low is not None:
            if c["low"] < (ref_low - sweep_thresh):
                if not swept_sell:
                    swept_sell = True
                    sweep_low = c["low"]
                else:
                    sweep_low = min(sweep_low, c["low"])

        prev_sh = _pine_prev_swing_high_close(candles, i, swing_lb)
        mss_up = swept_sell and prev_sh is not None and c["close"] > prev_sh
        reclaim = swept_sell and ref_low is not None and c["close"] > ref_low
        long_setup = allow and mss_up and reclaim and (not signaled_today)

        if long_setup:
            sl = (sweep_low - sl_buf) if sweep_low is not None else (ref_low - sl_buf)
            signals.append(
                {
                    "type": signal_type,
                    "direction": "BUY",
                    "bar_index": i,
                    "entry": c["close"],
                    "stop_loss": round(sl, 8),
                    "time": c["time"],
                    "dt": dt.isoformat(),
                    "session": ref_label,
                    "lookback": swing_lb,
                }
            )
            signaled_today = True
            swept_sell = False
            sweep_low = None

        prev_in_trade = in_trade

    return signals


def detect_pine_ashl_signals(candles):
    """Asia range (Pine 19:00–03:00 NY) + London trade window (03:00–06:00 NY), long only."""
    return _detect_pine_long_only(
        candles,
        "ASHL",
        is_asia_pine,
        is_london_pine,
        "ASHL",
    )


def detect_pine_lrny_signals(candles):
    """London range (03:00–06:00 NY) + NY trade window (08:00–11:00 NY), long only."""
    return _detect_pine_long_only(
        candles,
        "LRNY",
        is_london_pine,
        is_ny_pine,
        "LRNY",
    )


# =====================
# ATR — FIX 2: session-averaged
# =====================

def calc_atr_session(candles, idx, session_fn, length=14):
    """
    ATR averaged across bars in the current session window,
    capped at `length` bars. Falls back to standard ATR if
    not enough session bars.
    """
    # Collect recent bars in same session
    session_bars = []
    dt_idx = candle_dt(candles[idx])
    for j in range(max(0, idx - 60), idx + 1):
        if session_fn(candle_dt(candles[j])):
            session_bars.append(j)

    use_bars = session_bars[-length:] if len(session_bars) >= 3 else list(range(max(0, idx - length), idx + 1))

    trs = []
    for i in use_bars:
        if i == 0:
            continue
        h  = candles[i]["high"]
        l  = candles[i]["low"]
        pc = candles[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))

    if not trs:
        return None
    return sum(trs) / len(trs)


# =====================
# ADAPTIVE SWING LOOKBACK — FIX 4
# =====================

def adaptive_lookback(candles, idx, base=8, min_lb=4, max_lb=20):
    """
    Scale swing lookback by recent volatility.
    High volatility → more lookback (capture real structure).
    Low volatility  → less lookback (tighter signals).
    """
    if idx < 20:
        return base

    recent_ranges = [candles[j]["high"] - candles[j]["low"]
                     for j in range(idx - 20, idx)]
    avg_range = sum(recent_ranges) / len(recent_ranges)
    price     = candles[idx]["close"]
    vol_pct   = avg_range / price  # range as % of price

    if vol_pct > 0.003:    # high vol
        lb = min(base + 6, max_lb)
    elif vol_pct < 0.001:  # low vol
        lb = max(base - 3, min_lb)
    else:
        lb = base

    return lb


def swing_high(candles, idx, lookback):
    start = max(0, idx - lookback)
    return max(candles[j]["high"] for j in range(start, idx))

def swing_low(candles, idx, lookback):
    start = max(0, idx - lookback)
    return min(candles[j]["low"] for j in range(start, idx))


# =====================
# VOLUME CONFIRMATION — FIX 5
# =====================

def volume_confirms(candles, idx, lookback=20):
    """
    MSS bar volume must be above average of recent bars.
    Returns True if volume is at least 1.2x the rolling average.
    """
    if idx < lookback:
        return True  # not enough data — don't filter

    avg_vol = sum(candles[j]["volume"] for j in range(idx - lookback, idx)) / lookback
    bar_vol = candles[idx]["volume"]

    if avg_vol <= 0:
        return True

    return bar_vol >= avg_vol * 1.2


# =====================
# SAME-BAR CONFLICT — FIX 3
# =====================

def resolve_same_bar(bar, entry, sl, tp, direction):
    """
    When a bar hits both SL and TP, use open price proximity
    to determine which was likely hit first.
    """
    open_to_sl = abs(bar["open"] - sl)
    open_to_tp = abs(bar["open"] - tp)

    if open_to_sl < open_to_tp:
        return "LOSS", sl
    else:
        return "WIN", tp


# =====================
# KZ SIGNAL DETECTION
# =====================

def detect_kz_signals(candles, atr_mult=0.20, sl_atr_mult=0.10, min_sweep_ticks=0):
    signals = []
    if len(candles) < 30:
        return signals

    mintick = candles[-1]["close"] * 0.0001

    asia_high = asia_low = None
    prev_day  = None
    swept_sell = swept_buy = False
    sweep_low  = sweep_high = None
    prev_in_kill = False
    session_fn = lambda dt: is_london_kz(dt) or is_ny_kz(dt)

    for i in range(15, len(candles)):
        c  = candles[i]
        dt = candle_dt(c)
        day = dt.date()

        if prev_day is not None and day != prev_day:
            asia_high = asia_low = None

        prev_day = day

        if is_asia(dt):
            asia_high = max(asia_high, c["high"]) if asia_high is not None else c["high"]
            asia_low  = min(asia_low,  c["low"])  if asia_low  is not None else c["low"]

        in_kill    = is_london_kz(dt) or is_ny_kz(dt)
        kill_start = in_kill and not prev_in_kill

        if kill_start:
            swept_sell = swept_buy = False
            sweep_low  = sweep_high = None

        a_valid = (asia_high is not None and asia_low is not None
                   and asia_high > asia_low)

        if in_kill and a_valid:
            # FIX 2: session-averaged ATR
            atr = calc_atr_session(candles, i, session_fn)
            if atr is None:
                prev_in_kill = in_kill
                continue

            sweep_thresh = atr * atr_mult + (min_sweep_ticks * mintick)
            sl_buf       = atr * sl_atr_mult

            # FIX 4: adaptive lookback
            lb = adaptive_lookback(candles, i)

            # BUY SETUP
            if c["low"] < (asia_low - sweep_thresh):
                if not swept_sell:
                    swept_sell = True
                    sweep_low  = c["low"]
                else:
                    sweep_low = min(sweep_low, c["low"])

            if swept_sell:
                prev_sh = swing_high(candles, i, lb)
                mss_up  = c["high"] > prev_sh
                reclaim = c["close"] > asia_low

                # FIX 5: volume confirmation
                if mss_up and reclaim and volume_confirms(candles, i):
                    sl = (sweep_low - sl_buf) if sweep_low is not None else (asia_low - sl_buf)
                    signals.append({
                        "type":      "KZ",
                        "direction": "BUY",
                        "bar_index": i,
                        "entry":     c["close"],
                        "stop_loss": round(sl, 8),
                        "time":      c["time"],
                        "dt":        dt.isoformat(),
                        "session":   "LONDON" if is_london_kz(dt) else "NY",
                        "atr":       round(atr, 8),
                        "lookback":  lb,
                        "vol_ratio": round(c["volume"] / max(
                            sum(candles[j]["volume"] for j in range(i-20,i))/20, 0.001), 2),
                    })
                    swept_sell = False
                    sweep_low  = None

            # SELL SETUP
            if c["high"] > (asia_high + sweep_thresh):
                if not swept_buy:
                    swept_buy  = True
                    sweep_high = c["high"]
                else:
                    sweep_high = max(sweep_high, c["high"])

            if swept_buy:
                prev_sl  = swing_low(candles, i, lb)
                mss_down = c["low"] < prev_sl
                reclaim  = c["close"] < asia_high

                if mss_down and reclaim and volume_confirms(candles, i):
                    sl = (sweep_high + sl_buf) if sweep_high is not None else (asia_high + sl_buf)
                    signals.append({
                        "type":      "KZ",
                        "direction": "SELL",
                        "bar_index": i,
                        "entry":     c["close"],
                        "stop_loss": round(sl, 8),
                        "time":      c["time"],
                        "dt":        dt.isoformat(),
                        "session":   "LONDON" if is_london_kz(dt) else "NY",
                        "atr":       round(atr, 8),
                        "lookback":  lb,
                    })
                    swept_buy  = False
                    sweep_high = None

        prev_in_kill = in_kill

    return signals


# =====================
# ORB SIGNAL DETECTION
# =====================

def detect_orb_signals(candles, min_sweep_ticks=2, sl_buffer_ticks=2, max_bars_after_open=90):
    signals = []
    if len(candles) < 30:
        return signals

    mintick = candles[-1]["close"] * 0.0001

    london_low  = london_high = None
    prev_day    = None
    ny_bar_count = 0
    prev_in_ny  = False
    swept_sell  = swept_buy = False
    sweep_low   = sweep_high = None
    signaled_today = False

    for i in range(15, len(candles)):
        c  = candles[i]
        dt = candle_dt(c)
        day = dt.date()

        if prev_day is not None and day != prev_day:
            london_low = london_high = None
            signaled_today = False

        prev_day = day

        if is_london(dt):
            london_low  = min(london_low,  c["low"])  if london_low  is not None else c["low"]
            london_high = max(london_high, c["high"]) if london_high is not None else c["high"]

        in_ny    = is_ny_open(dt)
        ny_start = in_ny and not prev_in_ny

        if ny_start:
            ny_bar_count = 0
            swept_sell = swept_buy = False
            sweep_low  = sweep_high = None
        elif in_ny:
            ny_bar_count += 1

        allow = in_ny and ny_bar_count <= max_bars_after_open

        if allow and london_low is not None and london_high is not None:
            sweep_thresh = min_sweep_ticks * mintick
            sl_buf       = sl_buffer_ticks * mintick

            # FIX 4: adaptive lookback
            lb = adaptive_lookback(candles, i)

            # BUY
            if c["low"] < (london_low - sweep_thresh):
                if not swept_sell:
                    swept_sell = True
                    sweep_low  = c["low"]
                else:
                    sweep_low = min(sweep_low, c["low"])

            if swept_sell and not signaled_today:
                prev_sh = swing_high(candles, i, lb)
                mss_up  = c["close"] > prev_sh
                reclaim = c["close"] > london_low

                # FIX 5: volume confirmation
                if mss_up and reclaim and volume_confirms(candles, i):
                    sl = (sweep_low - sl_buf) if sweep_low is not None else (london_low - sl_buf)
                    signals.append({
                        "type":        "ORB",
                        "direction":   "BUY",
                        "bar_index":   i,
                        "entry":       c["close"],
                        "stop_loss":   round(sl, 8),
                        "time":        c["time"],
                        "dt":          dt.isoformat(),
                        "session":     "NY",
                        "lookback":    lb,
                        "london_low":  london_low,
                        "london_high": london_high,
                    })
                    signaled_today = True
                    swept_sell = False
                    sweep_low  = None

            # SELL
            if c["high"] > (london_high + sweep_thresh):
                if not swept_buy:
                    swept_buy  = True
                    sweep_high = c["high"]
                else:
                    sweep_high = max(sweep_high, c["high"])

            if swept_buy and not signaled_today:
                prev_sl  = swing_low(candles, i, lb)
                mss_down = c["close"] < prev_sl
                reclaim  = c["close"] < london_high

                if mss_down and reclaim and volume_confirms(candles, i):
                    sl = (sweep_high + sl_buf) if sweep_high is not None else (london_high + sl_buf)
                    signals.append({
                        "type":        "ORB",
                        "direction":   "SELL",
                        "bar_index":   i,
                        "entry":       c["close"],
                        "stop_loss":   round(sl, 8),
                        "time":        c["time"],
                        "dt":          dt.isoformat(),
                        "session":     "NY",
                        "lookback":    lb,
                        "london_low":  london_low,
                        "london_high": london_high,
                    })
                    signaled_today = True
                    swept_buy  = False
                    sweep_high = None

        prev_in_ny = in_ny

    return signals


# =====================
# REAL BAR-BY-BAR BACKTEST — FIX 3
# =====================

def simulate_trade(candles, signal, rr_target=2.0, max_bars=150):
    """
    Walk forward bar by bar.
    FIX 3: Same-bar conflict resolved by open proximity not defaulting to loss.
    """
    bi    = signal["bar_index"]
    entry = signal["entry"]
    sl    = signal["stop_loss"]
    direction = signal["direction"]

    risk = abs(entry - sl)
    if risk <= 0:
        return None

    tp = (entry + risk * rr_target) if direction == "BUY" else (entry - risk * rr_target)

    for j in range(bi + 1, min(bi + max_bars + 1, len(candles))):
        bar = candles[j]

        if direction == "BUY":
            sl_hit = bar["low"]  <= sl
            tp_hit = bar["high"] >= tp

            if sl_hit and tp_hit:
                # FIX 3: resolve by open proximity
                outcome, exit_p = resolve_same_bar(bar, entry, sl, tp, direction)
                pnl_r = rr_target if outcome == "WIN" else -1.0
                return {"outcome": outcome, "exit_price": exit_p,
                        "exit_reason": f"{outcome} (same bar)", "bars_held": j - bi,
                        "pnl_r": pnl_r, "rr_target": rr_target}
            elif sl_hit:
                return {"outcome": "LOSS", "exit_price": sl,
                        "exit_reason": "STOP LOSS", "bars_held": j - bi,
                        "pnl_r": -1.0, "rr_target": rr_target}
            elif tp_hit:
                return {"outcome": "WIN", "exit_price": tp,
                        "exit_reason": "TAKE PROFIT", "bars_held": j - bi,
                        "pnl_r": rr_target, "rr_target": rr_target}

        else:  # SELL
            sl_hit = bar["high"] >= sl
            tp_hit = bar["low"]  <= tp

            if sl_hit and tp_hit:
                outcome, exit_p = resolve_same_bar(bar, entry, sl, tp, direction)
                pnl_r = rr_target if outcome == "WIN" else -1.0
                return {"outcome": outcome, "exit_price": exit_p,
                        "exit_reason": f"{outcome} (same bar)", "bars_held": j - bi,
                        "pnl_r": pnl_r, "rr_target": rr_target}
            elif sl_hit:
                return {"outcome": "LOSS", "exit_price": sl,
                        "exit_reason": "STOP LOSS", "bars_held": j - bi,
                        "pnl_r": -1.0, "rr_target": rr_target}
            elif tp_hit:
                return {"outcome": "WIN", "exit_price": tp,
                        "exit_reason": "TAKE PROFIT", "bars_held": j - bi,
                        "pnl_r": rr_target, "rr_target": rr_target}

    # Expired — use actual close price for real PnL
    last = candles[min(bi + max_bars, len(candles)-1)]["close"]
    pnl_r = ((last - entry) / risk) if direction == "BUY" else ((entry - last) / risk)
    return {
        "outcome":     "WIN" if pnl_r > 0 else "LOSS",
        "exit_price":  last,
        "exit_reason": "EXPIRED",
        "bars_held":   max_bars,
        "pnl_r":       round(pnl_r, 3),
        "rr_target":   rr_target,
    }


# =====================
# RR optimization (history path + grid on same bar engine as live)
# =====================


def _optimized_rr_cache_path() -> str:
    raw = os.environ.get("OPTIMIZED_RR_CACHE", "").strip()
    if raw:
        return raw if os.path.isabs(raw) else os.path.normpath(os.path.join(_REPO_ROOT, raw))
    return os.path.join(_REPO_ROOT, "data", "optimized_rr.json")


def load_optimized_rr_cache() -> dict:
    path = _optimized_rr_cache_path()
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_optimized_rr_cache_row(symbol: str, optimal: dict) -> None:
    """Merge per-symbol optimal RR map into JSON (survives restarts; path is repo-local)."""
    if not symbol or not optimal:
        return
    path = _optimized_rr_cache_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        data = load_optimized_rr_cache()
        prev = data.get(symbol, {}) if isinstance(data.get(symbol), dict) else {}
        prev.update(optimal)
        data[symbol] = prev
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass


def mfe_r_before_stop(candles, signal, max_bars: int = 150) -> float:
    """
    Max favorable excursion in R multiples from entry, bar-by-bar, until full SL
    level is touched (path-based; no TP). BUY: favorable = high above entry.
    """
    bi = int(signal["bar_index"])
    entry = float(signal["entry"])
    sl = float(signal["stop_loss"])
    direction = signal["direction"]
    risk = abs(entry - sl)
    if risk <= 0:
        return 0.0
    mfe = 0.0
    end = min(bi + max_bars + 1, len(candles))
    for j in range(bi + 1, end):
        bar = candles[j]
        if direction == "BUY":
            if bar["low"] <= sl:
                break
            mfe = max(mfe, (bar["high"] - entry) / risk)
        else:
            if bar["high"] >= sl:
                break
            mfe = max(mfe, (entry - bar["low"]) / risk)
    return float(mfe)


def _batch_stats(candles, signals, rr_target: float, max_bars: int = 150):
    wins = losses = 0
    r_sum = 0.0
    bars_sum = 0
    for sig in signals:
        out = simulate_trade(candles, sig, rr_target=float(rr_target), max_bars=max_bars)
        if out is None:
            continue
        if out["outcome"] == "WIN":
            wins += 1
        else:
            losses += 1
        r_sum += float(out["pnl_r"])
        bars_sum += int(out["bars_held"])
    total = wins + losses
    if total == 0:
        return None
    winrate = round(100.0 * wins / total, 1)
    rr = float(rr_target)
    expectancy = round((winrate / 100.0 * rr) - ((1 - winrate / 100.0) * 1.0), 3)
    avg_r = round(r_sum / total, 3)
    avg_bars = round(bars_sum / total) if total else 0
    return {
        "wins": wins,
        "losses": losses,
        "total": total,
        "winrate": winrate,
        "expectancy": expectancy,
        "avg_r": avg_r,
        "avg_bars": avg_bars,
    }


def _rr_candidate_grid() -> list:
    step = float(os.environ.get("RR_GRID_STEP", "0.05"))
    rmax = float(os.environ.get("RR_GRID_MAX", "3.5"))
    step = max(0.01, min(step, 0.5))
    rmax = max(0.3, min(rmax, 8.0))
    out = []
    x = 0.15
    while x <= rmax + 1e-9:
        out.append(round(x, 4))
        x += step
    return out


def optimize_rr_for_signals(
    candles,
    signals,
    default_rr: float,
    leg_name: str = "KZ",
    max_bars: int = 150,
) -> tuple:
    """
    Pick RR (R multiple for TP) from historical bar path using the same simulate_trade
    engine. Targets win rate in [RR_OPT_TARGET_LO, RR_OPT_TARGET_HI] (percent) when
    possible; otherwise best expectancy, then closeness to band mid.

    Returns (chosen_rr, meta_dict).
    """
    # Enforce a minimum RR floor so the optimizer can't pick tiny TP targets
    # that inflate win-rate but produce negative expectancy.
    rr_floor = float(os.environ.get("RR_MIN_FLOOR", "1.0"))
    rr_floor = max(0.1, min(rr_floor, 8.0))
    # Default target band tuned for "high hit-rate" configs.
    lo = float(os.environ.get("RR_OPT_TARGET_LO", "66"))
    hi = float(os.environ.get("RR_OPT_TARGET_HI", "70"))
    lo_f = max(0.0, min(lo, 100.0)) / 100.0
    hi_f = max(0.0, min(hi, 100.0)) / 100.0
    if lo_f > hi_f:
        lo_f, hi_f = hi_f, lo_f
    mid = 0.5 * (lo_f + hi_f)

    min_tr = int(os.environ.get("RR_OPT_MIN_TRADES", "12"))
    min_tr = max(3, min(min_tr, 500))

    if not signals:
        return float(default_rr), {"method": "default", "reason": "no_signals"}

    mfes = [mfe_r_before_stop(candles, s, max_bars=max_bars) for s in signals]
    mfes_pos = [m for m in mfes if m > 0.05]
    extras = []
    if mfes_pos:
        extras.append(statistics.median(mfes_pos))
        if len(mfes_pos) >= 4:
            xs = sorted(mfes_pos)
            extras.append(xs[len(xs) // 4])
            extras.append(xs[(3 * len(xs)) // 4])

    cand = sorted(set(_rr_candidate_grid() + [round(x, 4) for x in extras if 0.12 <= x <= 7.0]))
    cand = [x for x in cand if x >= rr_floor]
    hint = float(default_rr)
    if hint > 0:
        cand = sorted(set([round(hint, 4)] + cand))
    cand = [x for x in cand if x >= rr_floor]

    best = None
    best_key = None

    def consider(rr, st, min_need: int):
        nonlocal best, best_key
        if st is None or st["total"] < min_need:
            return
        wr = st["winrate"] / 100.0
        in_band = lo_f <= wr <= hi_f
        key = (
            1 if in_band else 0,
            st["expectancy"],
            -abs(wr - mid),
            st["total"],
            -abs(rr - hint),
            rr,
        )
        if best_key is None or key > best_key:
            best_key = key
            best = (rr, st)

    for rr in cand:
        st = _batch_stats(candles, signals, rr, max_bars=max_bars)
        consider(rr, st, min_tr)

    if best is None:
        # Relax min trades so short histories still get an RR
        for relax in (max(5, min_tr // 2), 5, 3):
            for rr in cand:
                st = _batch_stats(candles, signals, rr, max_bars=max_bars)
                consider(rr, st, relax)
            if best is not None:
                break

    if best is None:
        return float(default_rr), {"method": "default", "reason": "insufficient_fills", "leg": leg_name}

    rr_chosen, st = best
    wr = st["winrate"] / 100.0
    meta = {
        "method": "path_grid",
        "leg": leg_name,
        "rr": float(rr_chosen),
        "winrate": st["winrate"],
        "expectancy": st["expectancy"],
        "trades": st["total"],
        "target_lo": lo,
        "target_hi": hi,
        "in_target_band": bool(lo_f <= wr <= hi_f),
        "mfe_median": round(statistics.median(mfes_pos), 4) if mfes_pos else None,
        "rr_floor": float(rr_floor),
    }
    return float(rr_chosen), meta


def backtest_symbol(
    symbol,
    interval=OHLC_INTERVAL_MINUTES,
    days_back=90,
    rr_target_kz=1.0,
    rr_target_orb=1.0,
    rr_target_ashl=1.0,
    rr_target_lrny=1.0,
    min_candles=100,
):
    """Full backtest with gap detection and real simulation.

    Intraday path:
      - If ``HISTORY_CSV_DIR`` contains ``{symbol}.csv`` with enough 5m rows, those
        bars are used (tail window ``BACKTEST_LOCAL_TAIL_DAYS``, default ~6 months).
      - Else Kraken REST 5m (~720 bars, ~2.5d).

    Long context (always when Kraken reachable):
      - Parallel 1d OHLC (~720 daily bars) for multi-month return/vol metadata.

    On success returns stats dict. On skip (insufficient history) returns
    {\"_skip\": True, ...} so callers can log before deploy.

    ``min_candles`` requests a minimum bar count. Kraken REST 5m cannot exceed ~720
    bars unless ``HISTORY_CSV_DIR`` provides longer 5m series.
    """
    print(f"Backtesting {symbol}...")
    need_req = max(30, int(min_candles))
    kraken_cap = max(30, KRAKEN_OHLC_MAX_BARS - 10)

    history_source = "kraken_rest_5m"
    tail_days = int(os.environ.get("BACKTEST_LOCAL_TAIL_DAYS", "183"))
    max_local = _local_tail_bar_count(tail_days, interval)

    loaded = load_symbol_csv_5m(symbol)
    if loaded:
        raw_local, csv_path = loaded
        use = raw_local[-max_local:] if len(raw_local) > max_local else raw_local
        # Prefer CSV when it meets the requested depth, or when it beats Kraken's REST cap
        # (do not treat min(need_req, 710) as the CSV gate — that accepts 800 bars when 50k were requested).
        if len(use) >= need_req:
            candles = use
            coverage = (candles[-1]["time"] - candles[0]["time"]) / 86400 if len(candles) > 1 else 0.0
            has_gaps = False
            history_source = f"csv:{csv_path}"
            need = need_req
            print(f"  {symbol}: {len(candles)} candles from CSV / ~{coverage:.1f}d / gaps={has_gaps}")
        elif len(use) > kraken_cap:
            # Some repos contain shallow CSVs (~Kraken REST depth). If the user requested
            # deep history (need_req > Kraken cap), try auto-fetch to replace the shallow CSV.
            if need_req > kraken_cap and len(use) < need_req:
                try:
                    ensure_symbol_history_5m(symbol, days=int(days_back), min_rows=int(need_req))
                except Exception:
                    pass
                loaded2 = load_symbol_csv_5m(symbol)
                if loaded2:
                    raw2, csv2 = loaded2
                    use2 = raw2[-max_local:] if len(raw2) > max_local else raw2
                    if len(use2) > len(use):
                        raw_local, csv_path = raw2, csv2
                        use = use2

            candles = use
            coverage = (candles[-1]["time"] - candles[0]["time"]) / 86400 if len(candles) > 1 else 0.0
            has_gaps = False
            history_source = f"csv:{csv_path}"
            need = min(need_req, len(candles))
            if need < need_req:
                print(
                    f"  {symbol}: CSV {len(candles)} bars < BACKTEST_MIN_CANDLES={need_req}; "
                    f"using best available (need={need})"
                )
            print(f"  {symbol}: {len(candles)} candles from CSV / ~{coverage:.1f}d / gaps={has_gaps}")
        else:
            candles, coverage, has_gaps = fetch_candles_paginated(
                symbol, interval=interval, days_back=days_back
            )
            need = min(need_req, kraken_cap)
            print(
                f"  {symbol}: CSV only {len(use)} bars (need {need_req} or >{kraken_cap} for CSV); "
                f"Kraken REST (need={need})"
            )
    else:
        # No local history. Try auto-fetching deep 5m history (Coinbase public) into cache,
        # then reload. This makes it practical to qualify >=8 symbols without manually curating CSVs.
        if interval == OHLC_INTERVAL_MINUTES:
            ensure_symbol_history_5m(symbol, days=int(days_back), min_rows=int(need_req))
            loaded2 = load_symbol_csv_5m(symbol)
            if loaded2:
                raw_local, csv_path = loaded2
                use = raw_local[-max_local:] if len(raw_local) > max_local else raw_local
                candles = use
                coverage = (candles[-1]["time"] - candles[0]["time"]) / 86400 if len(candles) > 1 else 0.0
                has_gaps = False
                history_source = f"csv:{csv_path}"
                need = min(need_req, len(candles))
            else:
                candles, coverage, has_gaps = fetch_candles_paginated(
                    symbol, interval=interval, days_back=days_back
                )
                need = min(need_req, kraken_cap)
        else:
            candles, coverage, has_gaps = fetch_candles_paginated(
                symbol, interval=interval, days_back=days_back
            )
            need = min(need_req, kraken_cap)
        # Don't spam warnings during long runs; only warn when no CSV history is available.
        if need_req > kraken_cap:
            print(
                f"  {symbol}: requested BACKTEST_MIN_CANDLES={need_req} but Kraken 5m REST caps ~{kraken_cap}. "
                f"Using {need} bars. For true multi-month 5m edges, ensure `{symbol}.csv` exists in HISTORY_CSV_DIR."
            )

    daily_candles, daily_cov, daily_gaps = fetch_daily_ohlc(symbol)
    daily_ctx = compute_daily_context(daily_candles) if daily_candles else None

    if len(candles) < need:
        detail = (
            f"insufficient history: {len(candles)} intraday bars "
            f"(need {need}+), ~{coverage}d coverage ({history_source})"
        )
        print(f"  {symbol}: SKIP — {detail}")
        return {
            "_skip": True,
            "symbol": symbol,
            "detail": detail,
            "candles": len(candles),
            "coverage_days": coverage,
            "days_back_requested": days_back,
            "min_candles": need,
            "history_source": history_source,
            "daily_context": daily_ctx,
        }

    if not history_source.startswith("csv"):
        print(f"  {symbol}: {len(candles)} candles / {coverage}d / gaps={has_gaps}")

    kz_sigs   = detect_kz_signals(candles)
    orb_sigs  = detect_orb_signals(candles)
    ashl_sigs = detect_pine_ashl_signals(candles)
    lrny_sigs = detect_pine_lrny_signals(candles)

    auto_rr = os.environ.get("AUTO_RR_OPTIMIZE", "1").strip().lower() in ("1", "true", "yes")
    cache_row = load_optimized_rr_cache().get(symbol, {})
    if not isinstance(cache_row, dict):
        cache_row = {}

    def score(signals, rr_target):
        if not signals:
            return {"total": 0, "wins": 0, "losses": 0, "winrate": 0.0,
                    "avg_r": 0.0, "avg_bars": 0, "expectancy": 0.0,
                    "recent_expectancy": 0.0,
                    "signals": []}

        wins = losses = 0
        r_sum = bars_sum = 0
        pnl_series = []

        for sig in signals:
            out = simulate_trade(candles, sig, rr_target=rr_target)
            if out is None:
                continue
            sig["backtest"] = out
            if out["outcome"] == "WIN":
                wins += 1
            else:
                losses += 1
            r_sum    += out["pnl_r"]
            bars_sum += out["bars_held"]
            pnl_series.append(float(out["pnl_r"]))

        total = wins + losses
        if total == 0:
            return {"total": 0, "wins": 0, "losses": 0, "winrate": 0.0,
                    "avg_r": 0.0, "avg_bars": 0, "expectancy": 0.0,
                    "recent_expectancy": 0.0,
                    "signals": signals}

        winrate    = round(wins / total * 100, 1)
        avg_r      = round(r_sum / total, 3)
        avg_bars   = round(bars_sum / total)
        expectancy = round((winrate/100 * rr_target) - ((1 - winrate/100) * 1), 3)
        # Recent expectancy proxy: average pnl_r over last N trades (more regime-adaptive).
        # This is not perfect but tracks the realized edge better than winrate alone.
        recent_n = int(os.environ.get("RECENT_WINDOW_TRADES", "60"))
        tail = pnl_series[-max(1, min(recent_n, len(pnl_series))):]
        recent_expectancy = round(sum(tail) / max(1, len(tail)), 3) if tail else 0.0

        return {"total": total, "wins": wins, "losses": losses,
                "winrate": winrate, "avg_r": avg_r, "avg_bars": avg_bars,
                "expectancy": expectancy, "recent_expectancy": recent_expectancy,
                "signals": signals}

    def resolve_rr(leg: str, sigs, fallback_rr: float) -> tuple:
        fb = float(fallback_rr)
        cached = cache_row.get(leg)
        try:
            cached_f = float(cached) if cached is not None else None
        except (TypeError, ValueError):
            cached_f = None
        if auto_rr and sigs:
            rr_guess = cached_f if (cached_f and 0.1 <= cached_f <= 10.0) else fb
            rr_pick, meta = optimize_rr_for_signals(candles, sigs, rr_guess, leg_name=leg)
            return rr_pick, meta
        rr_use = cached_f if (cached_f and 0.1 <= cached_f <= 10.0) else fb
        return rr_use, {"method": "fixed_or_cache", "rr": rr_use, "leg": leg}

    rr_kz, meta_kz = resolve_rr("KZ", kz_sigs, rr_target_kz)
    rr_orb, meta_orb = resolve_rr("ORB", orb_sigs, rr_target_orb)
    rr_ashl, meta_ashl = resolve_rr("ASHL", ashl_sigs, rr_target_ashl)
    rr_lrny, meta_lrny = resolve_rr("LRNY", lrny_sigs, rr_target_lrny)

    kz   = score(kz_sigs, rr_target=float(rr_kz))
    orb  = score(orb_sigs, rr_target=float(rr_orb))
    ashl = score(ashl_sigs, rr_target=float(rr_ashl))
    lrny = score(lrny_sigs, rr_target=float(rr_lrny))

    kz["optimal_rr"] = float(rr_kz)
    kz["rr_optimization"] = meta_kz
    orb["optimal_rr"] = float(rr_orb)
    orb["rr_optimization"] = meta_orb
    ashl["optimal_rr"] = float(rr_ashl)
    ashl["rr_optimization"] = meta_ashl
    lrny["optimal_rr"] = float(rr_lrny)
    lrny["rr_optimization"] = meta_lrny

    optimal_rr = {
        "KZ": float(rr_kz),
        "ORB": float(rr_orb),
        "ASHL": float(rr_ashl),
        "LRNY": float(rr_lrny),
    }
    save_optimized_rr_cache_row(symbol, optimal_rr)

    composite = (
        (kz["winrate"] + orb["winrate"] + ashl["winrate"] + lrny["winrate"]) * 0.075
        + (kz["expectancy"] + orb["expectancy"] + ashl["expectancy"] + lrny["expectancy"]) * 10.0
    )
    if daily_ctx and daily_ctx.get("trend_component") is not None:
        composite += 4.0 * daily_ctx["trend_component"]

    daily_bundle = {
        "kraken_daily_bars": len(daily_candles) if daily_candles else 0,
        "kraken_daily_span_days": daily_cov,
        "daily_gaps_flag": daily_gaps,
        "stats": daily_ctx,
    }

    return {
        "symbol":    symbol,
        "interval":  interval,
        "days_back": coverage,
        "has_gaps":  has_gaps,
        "candles":   len(candles),
        "history_source": history_source,
        "daily_history": daily_bundle,
        "optimal_rr": optimal_rr,
        "KZ":        kz,
        "ORB":       orb,
        "ASHL":      ashl,
        "LRNY":      lrny,
        "score":     round(composite, 2),
    }
