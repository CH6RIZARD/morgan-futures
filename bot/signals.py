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


# =====================
# LOOKBACK WINDOW (explicit + configurable)
# =====================

# Default backtest lookback in calendar days. Overridable per-call
# (``backtest_symbol(..., lookback_days=N)``) or by env ``BACKTEST_LOOKBACK_DAYS``.
DEFAULT_BACKTEST_LOOKBACK_DAYS = 30

# How far back Yahoo will actually serve intraday bars, per interval (calendar days).
# 1m is capped near a week; 2m-90m near two months; hourly is multi-year.
YF_INTRADAY_MAX_DAYS = {1: 7, 2: 60, 5: 60, 15: 60, 30: 60, 60: 730, 90: 60}

_PERIOD_UNIT_DAYS = {"d": 1, "wk": 7, "mo": 30, "y": 365}


def _parse_period_days(period: str) -> int | None:
    """Turn a Yahoo-style period string ('30d', '1mo', '2y') into calendar days."""
    p = (period or "").strip().lower()
    if not p:
        return None
    for unit in ("mo", "wk", "d", "y"):
        if p.endswith(unit):
            head = p[: -len(unit)].strip()
            try:
                return max(1, int(round(float(head) * _PERIOD_UNIT_DAYS[unit])))
            except ValueError:
                return None
    return None


def resolve_lookback_days(explicit=None) -> int:
    """
    Resolve the backtest lookback window, in calendar days.

    Precedence: explicit argument > ``BACKTEST_LOOKBACK_DAYS`` env >
    legacy ``YF_INTRADAY_PERIOD`` env > ``DEFAULT_BACKTEST_LOOKBACK_DAYS``.
    """
    if explicit is not None:
        try:
            v = int(float(explicit))
            if v > 0:
                return v
        except (TypeError, ValueError):
            pass

    raw = os.environ.get("BACKTEST_LOOKBACK_DAYS", "").strip()
    if raw:
        try:
            v = int(float(raw))
            if v > 0:
                return v
        except ValueError:
            pass

    legacy = _parse_period_days(os.environ.get("YF_INTRADAY_PERIOD", ""))
    if legacy:
        return legacy

    return DEFAULT_BACKTEST_LOOKBACK_DAYS


def provider_max_intraday_days(interval: int = OHLC_INTERVAL_MINUTES) -> int:
    """Max calendar days the intraday provider will serve for ``interval``."""
    raw = os.environ.get("YF_MAX_INTRADAY_DAYS", "").strip()
    if raw:
        try:
            v = int(float(raw))
            if v > 0:
                return v
        except ValueError:
            pass
    return YF_INTRADAY_MAX_DAYS.get(int(interval), 60)


def candle_window_info(candles, requested_days=None, source: str | None = None) -> dict:
    """
    Describe the window a candle list ACTUALLY covers.

    Always returns ``bars`` / ``first_bar_utc`` / ``last_bar_utc`` / ``days_covered``
    so callers can render "30d requested / 7d available" instead of silently
    presenting a short window as if it were the full one.
    """
    bars = len(candles or [])
    first_ts = candles[0]["time"] if bars else None
    last_ts = candles[-1]["time"] if bars else None
    days_covered = round((last_ts - first_ts) / 86400.0, 2) if bars > 1 else 0.0

    info = {
        "bars": bars,
        "first_bar_utc": (
            datetime.fromtimestamp(first_ts, tz=timezone.utc).isoformat() if first_ts else None
        ),
        "last_bar_utc": (
            datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat() if last_ts else None
        ),
        "days_covered": days_covered,
    }

    if requested_days is not None:
        req = int(requested_days)
        info["lookback_days_requested"] = req
        info["lookback_days_available"] = days_covered
        info["window_truncated"] = bool(days_covered + 0.5 < req)
        note = f"{req}d requested / {days_covered:g}d available"
        if source:
            note += f" ({source})"
        info["window_note"] = note
    if source:
        info["window_source"] = source
    return info


def window_is_adequate(candles, requested_days, min_fraction: float | None = None) -> bool:
    """True when ``candles`` cover enough of the requested window AND end recently."""
    if not candles or len(candles) < 2:
        return False
    if min_fraction is None:
        try:
            min_fraction = float(os.environ.get("BACKTEST_MIN_COVERAGE_FRAC", "0.8"))
        except ValueError:
            min_fraction = 0.8
    min_fraction = max(0.05, min(min_fraction, 1.0))

    covered = (candles[-1]["time"] - candles[0]["time"]) / 86400.0
    if covered < float(requested_days) * min_fraction:
        return False

    try:
        max_stale_h = float(os.environ.get("BACKTEST_MAX_CACHE_STALE_HOURS", "48"))
    except ValueError:
        max_stale_h = 48.0
    age_h = (time.time() - candles[-1]["time"]) / 3600.0
    return age_h <= max_stale_h


# =====================
# CONTRACT TICK SIZES
# =====================
# Minimum price increment per contract. Mirrors server.py CONTRACT_SPECS, the
# same way copy_router.CONTRACT_SPECS and rithmic_executor._CONTRACT_SPECS do.
#
# Detector parameters named ``*_ticks`` (min_sweep_ticks, sl_buffer_ticks) used
# to be multiplied by ``candles[-1]["close"] * 0.0001`` — one basis point of
# price, which is NOT a tick. Measured against the real specs that pseudo-tick
# is 0.35x a ZB tick and 11.8x an NQ tick, so "2 ticks" silently meant 0.7 ticks
# on the 30-year bond and 23.6 ticks on the Nasdaq. Same knob, different filter
# on every instrument.
CONTRACT_TICK_SIZE: dict[str, float] = {
    # Equity indices
    "ES": 0.25, "NQ": 0.25, "YM": 1.0, "RTY": 0.10,
    "MES": 0.25, "MNQ": 0.25, "MYM": 1.0,
    # Energy
    "CL": 0.01,
    # Metals
    "GC": 0.10, "MGC": 0.10, "SI": 0.005, "HG": 0.0005,
    # Rates
    "ZB": 0.03125, "ZN": 0.015625,
}


def contract_base(symbol: str) -> str:
    """``"MGC=F"`` / ``"MGCZ5"`` -> ``"MGC"``. Longest known root wins."""
    s = (symbol or "").upper().strip()
    if s.endswith("=F"):
        s = s[:-2]
    if s in CONTRACT_TICK_SIZE:
        return s
    for root in sorted(CONTRACT_TICK_SIZE, key=len, reverse=True):
        if s.startswith(root):
            return root
    return s


def tick_size_for(symbol: str | None, candles=None) -> float | None:
    """Real minimum tick for ``symbol``.

    Falls back to the legacy 1bp-of-price heuristic only when the symbol is
    unknown, so instruments we have no spec for behave exactly as before.
    """
    ts = CONTRACT_TICK_SIZE.get(contract_base(symbol)) if symbol else None
    if ts:
        return float(ts)
    if candles:
        return float(candles[-1]["close"]) * 0.0001
    return None


# How long a frozen reference range stays usable. A killzone is at most ~6h
# after the Asia session that built it; anything older means the session was
# missing entirely (holiday / data gap) and the stale range must not be traded.
REFERENCE_RANGE_MAX_AGE_SEC = 18 * 3600


# Fallback when Kraken AssetPairs discovery fails (see server ``BOT_SYMBOLS`` override).
# CME futures universe (mini + micro where available) using Yahoo Finance futures tickers.
# These are SIGNALS ONLY (no auto-trading).
SYMBOLS = [
    "ES=F",
    "NQ=F",
    "YM=F",

    # Energy
    "CL=F",

    # Metals
    "GC=F",  # Gold
    "SI=F",  # Silver
    "HG=F",  # Copper

    # Minis/micros
    "MGC=F",
    "MYM=F",
    "MES=F",
    "MNQ=F",
    "RTY=F",

    # Rates
    "ZB=F",
]

_env_syms = os.environ.get("BOT_SYMBOLS", "").strip()
if _env_syms:
    SYMBOLS = [s.strip().upper() for s in _env_syms.split(',') if s.strip()]

# =====================
# DATA FETCHING ? FIX 1 — FIX 1
# =====================

def fetch_ohlc_interval(symbol, interval=OHLC_INTERVAL_MINUTES, retries=3, days_back=None):
    '''Fetch OHLC using Yahoo Finance intraday bars.

    ``days_back`` is the calendar-day lookback to assemble (default resolves via
    ``resolve_lookback_days``). Requests longer than the provider's per-request
    intraday cap are paged into chunks and merged; whatever the provider will
    not serve is simply absent from the result (the caller reports the real
    window rather than pretending the short window was the requested one).

    Returns (candles, coverage_days, has_gaps). Best-effort gap detection.
    '''
    def _yahoo_chart(period: str | None = None, start: int | None = None,
                     end: int | None = None) -> pd.DataFrame | None:
        """
        Fetch intraday OHLC via Yahoo's public chart endpoint.

        Either ``period`` (range string) or a ``start``/``end`` epoch-second window.
        This avoids some common `yfinance` JSON decode failures (rate-limit / HTML responses).
        """
        sym = str(symbol)
        intv = f"{int(interval)}m"
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
        params = {
            "interval": intv,
            "includePrePost": "false",
            "events": "div|split|earn",
        }
        if start is not None and end is not None:
            params["period1"] = int(start)
            params["period2"] = int(end)
        else:
            params["range"] = period
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

    yf_interval = f"{int(interval)}m"
    requested_days = resolve_lookback_days(days_back)
    chunk_days = max(1, provider_max_intraday_days(interval))

    # Page the request: one chunk when the window fits the provider cap, several
    # (newest first) when the caller asks for more than one request can return.
    end_ts = int(time.time())
    floor_ts = end_ts - requested_days * 86400
    windows = []
    cur_end = end_ts
    while cur_end > floor_ts and len(windows) < 24:
        cur_start = max(floor_ts, cur_end - chunk_days * 86400)
        windows.append((cur_start, cur_end))
        if cur_start <= floor_ts:
            break
        cur_end = cur_start

    def _rows_from_df(df) -> list:
        rows = []
        if df is None or getattr(df, "empty", True):
            return rows
        df = _flatten_yf_columns(df)
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
                rows.append({
                    "time": t0,
                    "open": o,
                    "high": hi,
                    "low": lo,
                    "close": cl,
                    "volume": vol if vol == vol else 0.0,
                })
            except Exception:
                continue
        return rows

    last_err = None
    merged: dict[int, dict] = {}
    empty_streak = 0

    for w_idx, (w_start, w_end) in enumerate(windows):
        got = 0
        for attempt in range(max(1, retries)):
            try:
                df = _yahoo_chart(start=w_start, end=w_end)
                # Only the newest window is worth the noisy yfinance fallback; older
                # windows come back empty because the provider caps intraday depth.
                if (df is None or getattr(df, "empty", False)) and w_idx == 0:
                    # Fall back to yfinance for this window.
                    span_days = max(1, int(round((w_end - w_start) / 86400.0)))
                    try:
                        df = yf.download(
                            tickers=str(symbol),
                            start=datetime.fromtimestamp(w_start, tz=timezone.utc),
                            end=datetime.fromtimestamp(w_end, tz=timezone.utc),
                            interval=yf_interval,
                            progress=False,
                            auto_adjust=False,
                            prepost=False,
                            threads=False,
                        )
                    except Exception:
                        df = None
                    if df is None or getattr(df, "empty", True):
                        try:
                            df = yf.Ticker(str(symbol)).history(
                                period=f"{span_days}d",
                                interval=yf_interval,
                                auto_adjust=False,
                                prepost=False,
                            )
                        except Exception:
                            df = None

                rows = _rows_from_df(df)
                for c in rows:
                    if w_start - 86400 <= c["time"] <= w_end + 86400:
                        merged[c["time"]] = c
                got = len(rows)
                break
            except Exception as e:
                last_err = str(e)
                # Yahoo will occasionally rate-limit/captcha. Back off harder.
                time.sleep(1.5 * (attempt + 1))

        if got == 0:
            empty_streak += 1
            # Provider has stopped serving history this far back — stop paging
            # rather than burning requests on windows it will never fill.
            if empty_streak >= 2:
                break
        else:
            empty_streak = 0

    out = [merged[t] for t in sorted(merged)]

    if len(out) < 50:
        if last_err:
            print(f"Fetch error {symbol} [{interval}m] after {retries} tries: {last_err}")
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

def fetch_candles_paginated(symbol, interval=OHLC_INTERVAL_MINUTES, days_back=None):
    """
    Fetch intraday OHLC covering ``days_back`` calendar days (paged as needed).

    ``days_back`` is now honoured end-to-end: it selects the fetch window instead
    of being logged and discarded. ``None`` resolves via ``resolve_lookback_days``.

    Returns (candles, coverage_days, has_gaps).
    """
    return fetch_ohlc_interval(symbol, interval=interval, retries=4, days_back=days_back)


def fetch_daily_ohlc(symbol, retries=3):
    """Daily bars used for higher-timeframe context."""
    def _yahoo_daily() -> pd.DataFrame | None:
        sym = str(symbol)
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
        params = {
            "range": "2y",
            "interval": "1d",
            "includePrePost": "false",
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
        except Exception:
            return None
        if r.status_code != 200:
            return None
        try:
            data = r.json()
        except Exception:
            return None
        chart = (data or {}).get("chart") or {}
        if chart.get("error"):
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
        df["Datetime"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert(ET)
        df = df.set_index("Datetime")
        return df[["Open", "High", "Low", "Close", "Volume"]]

    df = _yahoo_daily()
    if df is None or getattr(df, "empty", False):
        # fallback (best-effort)
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
        except Exception:
            df = None

    if df is None or getattr(df, "empty", False):
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
            out.append(
                {
                    "time": t0,
                    "open": o,
                    "high": hi,
                    "low": lo,
                    "close": cl,
                    "volume": vol if vol == vol else 0.0,
                }
            )
        except Exception:
            continue
    out.sort(key=lambda x: x["time"])
    if len(out) < 50:
        return [], 0, True
    oldest, newest = out[0]["time"], out[-1]["time"]
    return out, round((newest - oldest) / 86400.0, 1), False


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
    symbol=None,
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
    # The trade window is the SESSION — nothing else.
    #
    # This used to carry a hard-coded 120-minute clock started at the first bar
    # of the trade window, on top of the session predicate. ASHL's trade window
    # is ``is_london_pine`` = 03:00-06:00 ET (180 minutes), which is what this
    # detector's own docstring says, what the TradingView preset that
    # ``is_london_pine`` documents says, and what the live scanner gates on
    # (``bot/server.py`` runs ``detect_pine_ashl_signals`` for as long as
    # ``is_london_pine(now_et)``). The 120-minute constant silently overrode all
    # three and discarded the last third of every session: 11 of every 36 London
    # bars, 451 bars per symbol over a 60-day window. The scanner spent 05:00-06:00
    # ET every day calling a detector that had already decided it would not
    # answer. Two gates encoded the same concept and disagreed; the undocumented
    # magic number lost.
    #
    # ``PINE_MAX_MINUTES_AFTER`` still works if it is explicitly set, so an
    # operator who wants a shorter entry window can still have one. Unset means
    # "the whole session", not "120 minutes".
    _mma_env = os.environ.get("PINE_MAX_MINUTES_AFTER", "").strip()
    max_min_after = max(5, min(int(_mma_env), 600)) if _mma_env else None
    interval = int(OHLC_INTERVAL_MINUTES)
    max_bars_after = (
        max(1, (max_min_after + interval - 1) // interval)
        if max_min_after is not None else None
    )

    mintick = tick_size_for(symbol, candles)
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
            allow = True
            if max_min_after is not None:
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


def detect_pine_ashl_signals(candles, symbol=None):
    """Asia range (Pine 19:00–03:00 NY) + London trade window (03:00–06:00 NY), long only."""
    return _detect_pine_long_only(
        candles,
        "ASHL",
        is_asia_pine,
        is_london_pine,
        "ASHL",
        symbol=symbol,
    )


# =====================
# ASIA LIQUIDITY-TRAP SCALP  (ships under the "LRNY" key)
# =====================
# Port of the Pine v6 script the owner actually trades ("asia scalp short" —
# it emits BOTH directions despite the title). This REPLACED the old
# London-range / NY-trade long-only leg. The dict key stays "LRNY" because the
# API and UI consume it; the strategy behind the key is what changed.
#
#   rangeSess 1800-0000 ET (Asia accumulation, crosses midnight, ends at 00:00)
#   tradeSess 0000-0500 ET
#   swingLen 5 (pivot lookback AND lookforward), minSweepTicks 3, EMA filter off.

ASIA_SCALP_RANGE_SESSION = (18, 0, 0, 0)   # 1800-0000
ASIA_SCALP_TRADE_SESSION = (0, 0, 5, 0)    # 0000-0500


def is_asia_scalp_range(dt, session=ASIA_SCALP_RANGE_SESSION):
    """Pine ``time in "1800-0000"`` — 18:00 up to but not including 00:00 ET."""
    return in_range(dt, *session)


def is_asia_scalp_trade(dt, session=ASIA_SCALP_TRADE_SESSION):
    """Pine ``time in "0000-0500"`` — 00:00 up to but not including 05:00 ET."""
    return in_range(dt, *session)


def _pivot_high(candles, p: int, left: int, right: int) -> float | None:
    """``ta.pivothigh(high, left, right)`` evaluated for candidate bar ``p``.

    Strictly greater than every high in [p-left, p+right]. The caller is
    responsible for only asking once bar ``p+right`` exists — that is the bar on
    which Pine confirms the pivot, and the earliest a live chart could know it.
    """
    if p - left < 0 or p + right >= len(candles):
        return None
    hp = candles[p]["high"]
    for j in range(p - left, p + right + 1):
        if j != p and candles[j]["high"] >= hp:
            return None
    return hp


def _pivot_low(candles, p: int, left: int, right: int) -> float | None:
    """``ta.pivotlow(low, left, right)`` for candidate bar ``p`` (strict)."""
    if p - left < 0 or p + right >= len(candles):
        return None
    lp = candles[p]["low"]
    for j in range(p - left, p + right + 1):
        if j != p and candles[j]["low"] <= lp:
            return None
    return lp


def _ema_series(values, length: int):
    """``ta.ema`` — na until ``length`` bars exist, seeded with the SMA."""
    out: list[float | None] = [None] * len(values)
    if length <= 0 or len(values) < length:
        return out
    alpha = 2.0 / (length + 1.0)
    prev = sum(values[:length]) / length
    out[length - 1] = prev
    for i in range(length, len(values)):
        prev = alpha * values[i] + (1.0 - alpha) * prev
        out[i] = prev
    return out


def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(int(os.environ.get(name, str(default))), hi))
    except (TypeError, ValueError):
        return default


def detect_pine_asia_scalp_signals(
    candles,
    symbol=None,
    signal_type: str = "LRNY",
    swing_len: int | None = None,
    min_sweep_ticks: int | None = None,
    use_ema_filter: bool | None = None,
    ema_len: int | None = None,
    range_session=ASIA_SCALP_RANGE_SESSION,
    trade_session=ASIA_SCALP_TRADE_SESSION,
    sl_buffer_ticks: int | None = None,
):
    """Asia liquidity-trap scalp — long AND short. Faithful port of the Pine v6.

    Entry/stop follow the same convention as every other leg in this module:
    entry is the signal bar's close, the stop sits a tick buffer beyond the
    sweep extreme (the signal bar's high for shorts, its low for longs), and the
    target is derived by ``simulate_trade`` from the leg's RR. The Pine
    indicator defines no exits, so nothing new is invented here.
    """
    signals: list[dict] = []
    if not candles:
        return signals

    swing_len = _env_int("ASIA_SCALP_SWING_LEN", 5, 1, 50) if swing_len is None else int(swing_len)
    min_sweep_ticks = (
        _env_int("ASIA_SCALP_MIN_SWEEP_TICKS", 3, 0, 500)
        if min_sweep_ticks is None else int(min_sweep_ticks)
    )
    sl_buffer_ticks = (
        _env_int("ASIA_SCALP_SL_BUFFER_TICKS", 2, 0, 500)
        if sl_buffer_ticks is None else int(sl_buffer_ticks)
    )
    ema_len = _env_int("ASIA_SCALP_EMA_LEN", 50, 1, 1000) if ema_len is None else int(ema_len)
    if use_ema_filter is None:
        # Pine default is OFF. Opt-in only.
        use_ema_filter = os.environ.get("ASIA_SCALP_USE_EMA", "0").strip().lower() in ("1", "true", "yes")

    if len(candles) < swing_len * 2 + 2:
        return signals

    mintick = tick_size_for(symbol, candles)
    if not mintick:
        return signals
    min_sweep = min_sweep_ticks * mintick
    sl_buf = sl_buffer_ticks * mintick

    ema = _ema_series([c["close"] for c in candles], ema_len) if use_ema_filter else [None] * len(candles)

    sess_high = sess_low = None      # range the LIVE 18:00-00:00 session is building
    last_high = last_low = None      # frozen when that session ENDS (at 00:00)
    last_ph = last_pl = None         # persist across days until replaced
    prev_in_range = False

    for i, c in enumerate(candles):
        dt = candle_dt(c)
        in_rng = is_asia_scalp_range(dt, range_session)
        in_trade = is_asia_scalp_trade(dt, trade_session)

        # --- Asia range: reset on the session's FIRST bar, not on the calendar
        # date. 18:00-00:00 crosses midnight; a date-keyed reset would throw
        # away every bar before 00:00 and freeze a range built from nothing.
        if in_rng and not prev_in_range:
            sess_high, sess_low = c["high"], c["low"]
        elif in_rng:
            sess_high = max(sess_high, c["high"])
            sess_low = min(sess_low, c["low"])

        # Freeze the levels the moment the range session ends (00:00), which is
        # the same instant the trade session opens.
        if (not in_rng) and prev_in_range:
            last_high, last_low = sess_high, sess_low

        prev_in_range = in_rng

        # --- Pivots. ta.pivothigh(high, N, N) is confirmed N bars AFTER the
        # pivot bar, so bar i can only ever learn about bar i-N. Reading the
        # pivot any earlier is lookahead.
        p = i - swing_len
        if p >= 0:
            ph = _pivot_high(candles, p, swing_len, swing_len)
            if ph is not None:
                last_ph = ph
            pl = _pivot_low(candles, p, swing_len, swing_len)
            if pl is not None:
                last_pl = pl

        if not in_trade:
            continue

        close, open_ = c["close"], c["open"]
        bullish = close > open_
        bearish = close < open_
        e = ema[i]
        ema_short_ok = (not use_ema_filter) or (e is not None and close < e)
        ema_long_ok = (not use_ema_filter) or (e is not None and close > e)

        rng = (last_high - last_low) if (last_high is not None and last_low is not None) else None

        # SHORT
        sweep_up = last_ph is not None and c["high"] > last_ph + min_sweep
        reject_down = last_ph is not None and close < last_ph
        # NOTE: premium is measured UP from lastLow — lastLow + 0.6*range — which
        # is what the Pine says. It is not lastHigh - 0.4*range.
        in_premium = rng is not None and close > (last_low + 0.6 * rng)
        if ema_short_ok and sweep_up and reject_down and bearish and in_premium:
            sl = c["high"] + sl_buf
            signals.append({
                "type": signal_type,
                "direction": "SELL",
                "bar_index": i,
                "entry": close,
                "stop_loss": round(sl, 8),
                "time": c["time"],
                "dt": dt.isoformat(),
                "session": "ASIA_SCALP",
                "lookback": swing_len,
                "trading_day": pine_trading_day_id(dt),
                "asia_high": last_high,
                "asia_low": last_low,
                "swept_level": last_ph,
            })

        # LONG
        sweep_down = last_pl is not None and c["low"] < last_pl - min_sweep
        reject_up = last_pl is not None and close > last_pl
        in_discount = rng is not None and close < (last_low + 0.4 * rng)
        if ema_long_ok and sweep_down and reject_up and bullish and in_discount:
            sl = c["low"] - sl_buf
            signals.append({
                "type": signal_type,
                "direction": "BUY",
                "bar_index": i,
                "entry": close,
                "stop_loss": round(sl, 8),
                "time": c["time"],
                "dt": dt.isoformat(),
                "session": "ASIA_SCALP",
                "lookback": swing_len,
                "trading_day": pine_trading_day_id(dt),
                "asia_high": last_high,
                "asia_low": last_low,
                "swept_level": last_pl,
            })

    signals.sort(key=lambda s: s["bar_index"])
    return signals


def detect_pine_lrny_signals(candles, symbol=None, **kwargs):
    """The "LRNY" leg — now the Asia liquidity-trap scalp (18:00-00:00 range,
    00:00-05:00 ET trade window, both directions).

    The old London-range/NY-window long-only strategy this key used to hold is
    no longer traded; the key name is preserved because the API and UI consume it.
    """
    return detect_pine_asia_scalp_signals(candles, symbol=symbol, signal_type="LRNY", **kwargs)


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

_LB_BASELINE_BARS = 480  # ~2 sessions of 5m bars


def adaptive_lookback(candles, idx, base=8, min_lb=4, max_lb=20):
    """
    Scale swing lookback by recent volatility RELATIVE TO THIS INSTRUMENT'S OWN
    recent norm.

    High volatility → more lookback (capture real structure).
    Low volatility  → less lookback (tighter signals).

    This used to compare ``avg_range / price`` against the absolute constants
    0.003 and 0.001. A 5-minute bar range as a fraction of price is not a
    universal constant — it is an asset class's realised intraday volatility.
    Equity index futures sit near 0.0005 and were therefore permanently
    classified "low volatility" (lb=5), while gold and silver sit near 0.001
    and 0.002 and were permanently "normal/high" (lb=8). Measured over a 30-day
    window the result was a per-instrument constant, not an adaptation: ES
    picked lb=5 on 87% of bars, SI picked lb=8 on 80%. The knob discriminated
    between instruments instead of between volatility regimes.

    Comparing the short window against the same instrument's longer baseline is
    dimensionless, so "high volatility" now means high *for this instrument*.
    """
    if idx < 20:
        return base

    short_ranges = [candles[j]["high"] - candles[j]["low"]
                    for j in range(idx - 20, idx)]
    short_avg = sum(short_ranges) / len(short_ranges)

    b_start = max(0, idx - _LB_BASELINE_BARS)
    if idx - b_start < 60:
        return base  # not enough history to know what normal looks like
    base_ranges = [candles[j]["high"] - candles[j]["low"]
                   for j in range(b_start, idx)]
    base_avg = sum(base_ranges) / len(base_ranges)
    if base_avg <= 0:
        return base

    vol_ratio = short_avg / base_avg  # >1 = livelier than its own norm

    if vol_ratio > 1.35:    # high vol for this instrument
        lb = min(base + 6, max_lb)
    elif vol_ratio < 0.75:  # low vol for this instrument
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

def detect_kz_signals(candles, atr_mult=0.20, sl_atr_mult=0.10, min_sweep_ticks=0, symbol=None):
    signals = []
    if len(candles) < 30:
        return signals

    mintick = tick_size_for(symbol, candles)

    asia_high = asia_low = None       # range the LIVE Asia session is building
    asia_last_ts = None
    ref_high = ref_low = None         # range frozen when the killzone opened
    ref_ts = None
    prev_in_asia = False
    swept_sell = swept_buy = False
    sweep_low  = sweep_high = None
    prev_in_kill = False
    session_fn = lambda dt: is_london_kz(dt) or is_ny_kz(dt)

    for i in range(15, len(candles)):
        c  = candles[i]
        dt = candle_dt(c)

        in_asia = is_asia(dt)

        # ``is_asia`` is 20:00-03:00 ET — it deliberately crosses midnight, and
        # ``in_range`` has explicit wrap handling for exactly that. Resetting the
        # accumulator on the CALENDAR date threw away every bar before 00:00,
        # which is 57% of the session, so the "Asia range" the detector actually
        # swept was a 00:00-03:00 range. Reset when the session itself starts.
        # ASHL/LRNY already get this right via ``pine_trading_day_id``.
        if in_asia and not prev_in_asia:
            asia_high = asia_low = None

        if in_asia:
            asia_high = max(asia_high, c["high"]) if asia_high is not None else c["high"]
            asia_low  = min(asia_low,  c["low"])  if asia_low  is not None else c["low"]
            asia_last_ts = c["time"]

        in_kill    = is_london_kz(dt) or is_ny_kz(dt)
        kill_start = in_kill and not prev_in_kill

        if kill_start:
            swept_sell = swept_buy = False
            sweep_low  = sweep_high = None
            # Freeze the reference range at the open of the killzone. is_asia
            # (20:00-03:00) OVERLAPS is_london_kz (02:00-05:00) by a full hour,
            # and the range was extended by the same bar that was then tested
            # against it — so ``low < asia_low - sweep_thresh`` was
            # unsatisfiable for the first 12 bars of every London killzone. You
            # cannot sweep a level that moves to accommodate you.
            ref_high, ref_low, ref_ts = asia_high, asia_low, asia_last_ts

        # A frozen range is only tradeable if the Asia session that built it is
        # the one immediately preceding this killzone (guards holidays / gaps).
        ref_fresh = (
            ref_ts is not None
            and 0 <= (c["time"] - ref_ts) <= REFERENCE_RANGE_MAX_AGE_SEC
        )
        asia_high_r, asia_low_r = ref_high, ref_low
        a_valid = (asia_high_r is not None and asia_low_r is not None
                   and asia_high_r > asia_low_r and ref_fresh)

        if in_kill and a_valid:
            # FIX 2: session-averaged ATR
            atr = calc_atr_session(candles, i, session_fn)
            if atr is None:
                prev_in_kill = in_kill
                prev_in_asia = in_asia
                continue

            sweep_thresh = atr * atr_mult + (min_sweep_ticks * mintick)
            sl_buf       = atr * sl_atr_mult

            # FIX 4: adaptive lookback
            lb = adaptive_lookback(candles, i)

            # BUY SETUP
            if c["low"] < (asia_low_r - sweep_thresh):
                if not swept_sell:
                    swept_sell = True
                    sweep_low  = c["low"]
                else:
                    sweep_low = min(sweep_low, c["low"])

            if swept_sell:
                prev_sh = swing_high(candles, i, lb)
                mss_up  = c["high"] > prev_sh
                reclaim = c["close"] > asia_low_r

                # FIX 5: volume confirmation
                if mss_up and reclaim and volume_confirms(candles, i):
                    sl = (sweep_low - sl_buf) if sweep_low is not None else (asia_low_r - sl_buf)
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
            if c["high"] > (asia_high_r + sweep_thresh):
                if not swept_buy:
                    swept_buy  = True
                    sweep_high = c["high"]
                else:
                    sweep_high = max(sweep_high, c["high"])

            if swept_buy:
                prev_sl  = swing_low(candles, i, lb)
                mss_down = c["low"] < prev_sl
                reclaim  = c["close"] < asia_high_r

                if mss_down and reclaim and volume_confirms(candles, i):
                    sl = (sweep_high + sl_buf) if sweep_high is not None else (asia_high_r + sl_buf)
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
        prev_in_asia = in_asia

    return signals


# =====================
# ORB SIGNAL DETECTION
# =====================

def detect_orb_signals(candles, min_sweep_ticks=2, sl_buffer_ticks=2, max_bars_after_open=90,
                       symbol=None):
    """London range (02:00-05:00 ET) swept during the NY open window, both directions.

    Two limits in here were measured against 60 days of live 5m futures data
    (13 symbols, ~11.4k bars each) rather than assumed:

    * ``max_bars_after_open=90`` is unreachable. ``is_ny_open`` is 09:30-12:00,
      which is 30 five-minute bars, so ``ny_bar_count`` tops out at 29 and the
      gate discarded exactly 0 bars on every symbol. It is inert, not a filter.
    * ``signaled_today`` is a genuine one-signal-per-calendar-day cap and it does
      bind: over the same window it suppressed 44 already-qualified signals
      across the universe (ES 35->28, NQ 26->20, RTY 29->23; CL only 9->8).
      18 of those 44 were the OPPOSITE direction to the day's first signal —
      i.e. a fade of the London high blocked by an earlier fade of the London
      low, two different setups against two different levels. ``detect_kz_signals``,
      which is the same sweep/MSS/reclaim/volume machine, carries no such cap.
      This is left AS IS because it is a plausible risk rule and nothing in the
      repo contradicts it; it is documented here so the count is explainable.

    The dominant reason this leg is selective is neither: on CL 35 of 41 days
    sweep the London range but only 10 ever produce a close beyond the trailing
    swing (803 bar-level MSS rejections). That part is market structure.
    """
    signals = []
    if len(candles) < 30:
        return signals

    mintick = tick_size_for(symbol, candles)

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
    avg_r = round(r_sum / total, 3)
    avg_bars = round(bars_sum / total) if total else 0
    # Realized, not projected — see the note in backtest_symbol.score(). The RR
    # grid search below ranks on this, otherwise it maximises a fiction and
    # keeps picking the largest target that holds win-rate in band while the
    # trades themselves expire short of it.
    return {
        "wins": wins,
        "losses": losses,
        "total": total,
        "winrate": winrate,
        "expectancy": avg_r,
        "projected_expectancy": round((winrate / 100.0 * rr) - ((1 - winrate / 100.0) * 1.0), 3),
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
        "projected_expectancy": st.get("projected_expectancy"),
        "objective": "realized_mean_r",
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
    days_back=None,
    rr_target_kz=1.0,
    rr_target_orb=1.0,
    rr_target_ashl=1.0,
    rr_target_lrny=1.0,
    min_candles=100,
    lookback_days=None,
):
    """Full backtest with gap detection and real simulation.

    Window:
      ``lookback_days`` (or legacy ``days_back``, or env ``BACKTEST_LOOKBACK_DAYS``,
      default 30 calendar days) selects how much history is assembled. Every
      candidate source is scored on the window it ACTUALLY covers — a cached CSV
      is only preferred when it spans most of the requested window and is recent.
      A short/stale cache no longer wins over a fresh multi-week fetch.

    Intraday sources (best real coverage wins):
      - ``{symbol}.csv`` from ``HISTORY_CSV_DIR`` / the auto-fetch cache
        (tail window ``BACKTEST_LOCAL_TAIL_DAYS``, default ~6 months)
      - live intraday fetch, paged across the requested window

    Long context:
      - Parallel 1d OHLC for multi-month return/vol metadata.

    On success returns stats dict. On skip (insufficient history) returns
    {\"_skip\": True, ...} so callers can log before deploy.

    The result always carries the true window (``bars``, ``first_bar_utc``,
    ``last_bar_utc``, ``days_covered``, ``window_note``) so the UI can show
    "30d requested / 7d available" rather than implying the full window.
    """
    print(f"Backtesting {symbol}...")
    need_req = max(30, int(min_candles))
    kraken_cap = max(30, KRAKEN_OHLC_MAX_BARS - 10)

    lookback = resolve_lookback_days(lookback_days if lookback_days is not None else days_back)
    provider_cap_days = provider_max_intraday_days(interval)

    tail_days = int(os.environ.get("BACKTEST_LOCAL_TAIL_DAYS", "183"))
    max_local = _local_tail_bar_count(max(tail_days, lookback), interval)

    def _coverage(rows) -> float:
        if not rows or len(rows) < 2:
            return 0.0
        return (rows[-1]["time"] - rows[0]["time"]) / 86400.0

    # ---- candidate sources -------------------------------------------------
    candidates = []  # (candles, source_label, has_gaps)

    def _add_csv(loaded_pair) -> list:
        if not loaded_pair:
            return []
        raw_local, csv_path = loaded_pair
        use = raw_local[-max_local:] if len(raw_local) > max_local else raw_local
        candidates.append((use, f"csv:{csv_path}", False))
        return use

    csv_rows = _add_csv(load_symbol_csv_5m(symbol))

    if not window_is_adequate(csv_rows, lookback):
        if csv_rows:
            print(
                f"  {symbol}: cached CSV covers only ~{_coverage(csv_rows):.1f}d "
                f"({len(csv_rows)} bars) of the {lookback}d window - refreshing"
            )
        if interval == OHLC_INTERVAL_MINUTES:
            try:
                ensure_symbol_history_5m(symbol, days=int(lookback), min_rows=int(need_req))
            except Exception:
                pass
            _add_csv(load_symbol_csv_5m(symbol))

        best_csv = max((_coverage(c) for c, _, _ in candidates), default=0.0)
        if not any(window_is_adequate(c, lookback) for c, _, _ in candidates):
            live, live_cov, live_gaps = fetch_candles_paginated(
                symbol, interval=interval, days_back=lookback
            )
            if live:
                candidates.append((live, f"yahoo_intraday_{int(interval)}m", live_gaps))
                if best_csv > 0:
                    print(
                        f"  {symbol}: live fetch {len(live)} bars / ~{live_cov}d "
                        f"beats cache (~{best_csv:.1f}d)"
                    )

    if not candidates:
        candidates.append(([], "none", True))

    # Pick the source that genuinely covers the most history (ties → more bars).
    candles, history_source, has_gaps = max(
        candidates, key=lambda t: (round(_coverage(t[0]), 2), len(t[0]))
    )
    coverage = round(_coverage(candles), 2)
    # Never skip a source that is deeper than the REST cap just because an
    # aspirational BACKTEST_MIN_CANDLES was set (matches the previous CSV branch).
    need = (
        need_req
        if len(candles) >= need_req
        else min(need_req, max(kraken_cap, len(candles)))
    )

    window = candle_window_info(candles, requested_days=lookback, source=history_source)
    window["provider_max_days"] = provider_cap_days
    window["interval_minutes"] = int(interval)
    if window.get("window_truncated"):
        reason = (
            f"provider caps {int(interval)}m history near {provider_cap_days}d"
            if coverage <= provider_cap_days + 1
            else "provider returned no bars older than this"
        )
        print(f"  {symbol}: WINDOW SHORT - {window['window_note']} ({reason})")

    if candles and history_source.startswith("csv"):
        print(f"  {symbol}: {len(candles)} candles from CSV / ~{coverage:.1f}d / gaps={has_gaps}")

    daily_candles, daily_cov, daily_gaps = fetch_daily_ohlc(symbol)
    daily_ctx = compute_daily_context(daily_candles) if daily_candles else None

    if len(candles) < need:
        detail = (
            f"insufficient history: {len(candles)} intraday bars "
            f"(need {need}+), ~{coverage}d coverage ({history_source})"
        )
        print(f"  {symbol}: SKIP — {detail}")
        skipped = {
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
        skipped.update(window)
        return skipped

    if not history_source.startswith("csv"):
        print(f"  {symbol}: {len(candles)} candles / {coverage}d / gaps={has_gaps}")

    kz_sigs   = detect_kz_signals(candles, symbol=symbol)
    orb_sigs  = detect_orb_signals(candles, symbol=symbol)
    ashl_sigs = detect_pine_ashl_signals(candles, symbol=symbol)
    lrny_sigs = detect_pine_lrny_signals(candles, symbol=symbol)

    auto_rr = os.environ.get("AUTO_RR_OPTIMIZE", "1").strip().lower() in ("1", "true", "yes")
    cache_row = load_optimized_rr_cache().get(symbol, {})
    if not isinstance(cache_row, dict):
        cache_row = {}

    def score(signals, rr_target):
        if not signals:
            return {"total": 0, "wins": 0, "losses": 0, "winrate": 0.0,
                    "avg_r": 0.0, "avg_bars": 0, "expectancy": 0.0,
                    "projected_expectancy": 0.0,
                    "recent_expectancy": 0.0, "recent_window_trades": 0,
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
                    "projected_expectancy": 0.0,
                    "recent_expectancy": 0.0, "recent_window_trades": 0,
                    "signals": signals}

        winrate    = round(wins / total * 100, 1)
        avg_r      = round(r_sum / total, 3)
        avg_bars   = round(bars_sum / total)

        # ``expectancy`` is the REALIZED mean R of the trades this leg actually
        # produced — the same pnl_r that every signal carries in
        # ``signal["backtest"]["pnl_r"]``.
        #
        # It used to be ``winrate * rr_target - (1 - winrate)``, which projects
        # that every winner is paid the full RR target. Most winners exit
        # "EXPIRED" far short of target, so that formula advertised a forecast as
        # a backtest result — 15 of 52 live legs showed a positive expectancy
        # while their own shipped trades lost money. The projection is kept under
        # ``projected_expectancy`` because it is still what a fixed-RR plan would
        # earn IF every target filled.
        expectancy = avg_r
        projected_expectancy = round((winrate/100 * rr_target) - ((1 - winrate/100) * 1), 3)

        # Genuinely recent: the tail must be a strict subset for the label to
        # mean anything. The old default of 60 exceeded every leg's trade count
        # (max 47), so recent_expectancy equalled avg_r verbatim on all 52 legs.
        recent_n = int(os.environ.get("RECENT_WINDOW_TRADES", "20"))
        recent_n = max(3, recent_n)
        used = max(1, min(recent_n, len(pnl_series)))
        tail = pnl_series[-used:]
        recent_expectancy = round(sum(tail) / len(tail), 3) if tail else 0.0

        return {"total": total, "wins": wins, "losses": losses,
                "winrate": winrate, "avg_r": avg_r, "avg_bars": avg_bars,
                "expectancy": expectancy,
                "projected_expectancy": projected_expectancy,
                "recent_expectancy": recent_expectancy,
                "recent_window_trades": len(tail),
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

    result = {
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
        # Total simulated trades across all legs — what "trade count" means in the UI.
        "trades":    int(kz["total"] + orb["total"] + ashl["total"] + lrny["total"]),
    }
    # Additive only: existing keys above are untouched, the real window is appended.
    result.update(window)
    return result
