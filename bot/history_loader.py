"""
Optional long 5m OHLC for backtests beyond Kraken REST's 720-candle cap.

Set HISTORY_CSV_DIR to a directory containing ``{SYMBOL}.csv`` (e.g. ``XBTUSD.csv``).
Use header row with columns: time,open,high,low,close,volume
(aliases: timestamp / ts for time; vol for volume). ``time`` = Unix seconds at bar open.
"""

from __future__ import annotations

import csv
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

_COINBASE = "https://api.exchange.coinbase.com/products"
_GRANULARITY_SEC = 300  # 5m
_MAX_PER_REQ = 300
_WINDOW_SEC = (_MAX_PER_REQ - 1) * _GRANULARITY_SEC
_BASE_REMAP = {"XBT": "BTC"}


def _history_csv_dir() -> str:
    """Resolve HISTORY_CSV_DIR relative to repo root when not absolute."""
    raw = os.environ.get("HISTORY_CSV_DIR", "").strip()
    if not raw:
        # Sensible default for deployments: if the repo contains `history_csv/`,
        # use it even when HISTORY_CSV_DIR isn't set.
        fallback = os.path.join(_REPO_ROOT, "history_csv")
        return os.path.normpath(fallback) if os.path.isdir(fallback) else ""
    if os.path.isabs(raw):
        return os.path.normpath(raw)
    return os.path.normpath(os.path.join(_REPO_ROOT, raw))


def _pick_col(fieldnames: List[str], options: Tuple[str, ...]) -> Optional[str]:
    low = {f.lower().strip(): f for f in fieldnames}
    for o in options:
        if o in low:
            return low[o]
    return None


def load_symbol_csv_5m(symbol: str) -> Optional[Tuple[List[Dict[str, Any]], str]]:
    """
    Load ``{HISTORY_CSV_DIR}/{symbol}.csv`` as 5m candles.

    Returns (candles, path) or None if missing/invalid.
    """
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return None

    base = _history_csv_dir()
    path = os.path.join(base, f"{symbol}.csv") if base else ""

    # If not present in HISTORY_CSV_DIR, also check the auto-fetch cache directory.
    if (not path) or (not os.path.isfile(path)):
        cache_dir = _auto_cache_dir()
        cache_path = os.path.join(cache_dir, f"{symbol}.csv")
        if os.path.isfile(cache_path):
            path = cache_path
        else:
            return None

    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return None
        fn = list(reader.fieldnames)
        c_time = _pick_col(fn, ("time", "unix", "unix_ts", "timestamp", "ts"))
        c_open = _pick_col(fn, ("open",))
        c_high = _pick_col(fn, ("high",))
        c_low = _pick_col(fn, ("low",))
        c_close = _pick_col(fn, ("close",))
        c_vol = _pick_col(fn, ("volume", "vol"))
        if not all([c_time, c_open, c_high, c_low, c_close, c_vol]):
            return None

        candles: List[Dict[str, Any]] = []
        for row in reader:
            if not row:
                continue
            try:
                ts = int(float((row.get(c_time) or "").strip()))
                o = float((row.get(c_open) or "").strip())
                hi = float((row.get(c_high) or "").strip())
                lo = float((row.get(c_low) or "").strip())
                cl = float((row.get(c_close) or "").strip())
                vol = float((row.get(c_vol) or "0").strip() or 0)
            except (TypeError, ValueError, AttributeError):
                continue
            candles.append(
                {
                    "time": ts,
                    "open": o,
                    "high": hi,
                    "low": lo,
                    "close": cl,
                    "volume": vol,
                }
            )

    if len(candles) < 50:
        return None

    candles.sort(key=lambda x: x["time"])
    by_ts: Dict[int, Dict[str, Any]] = {}
    for c in candles:
        by_ts[c["time"]] = c
    merged = list(by_ts.values())
    merged.sort(key=lambda x: x["time"])
    return merged, path


def _auto_cache_dir() -> str:
    """
    Writable cache directory for fetched 5m history.
    Railway ephemeral FS is fine; it only needs to exist for the running container.
    """
    d = os.environ.get("HISTORY_CACHE_DIR", "").strip()
    if not d:
        d = os.path.join(_REPO_ROOT, "data", "candles")
    if not os.path.isabs(d):
        d = os.path.join(_REPO_ROOT, d)
    os.makedirs(d, exist_ok=True)
    return os.path.normpath(d)


def _coinbase_products() -> set[str]:
    r = requests.get(_COINBASE, timeout=45, headers={"User-Agent": "trading-bot-v4-history/1.0"})
    r.raise_for_status()
    rows = r.json()
    out: set[str] = set()
    for p in rows:
        pid = p.get("id")
        if isinstance(pid, str) and pid:
            out.add(pid.upper())
    return out


def _kraken_alt_to_coinbase_product(kraken_sym: str, products: set[str]) -> Optional[str]:
    s = (kraken_sym or "").strip().upper()
    if not s.endswith("USD") or len(s) <= 3:
        return None
    base = s[:-3]
    base = _BASE_REMAP.get(base, base)
    pid = f"{base}-USD"
    return pid if pid in products else None


def _fetch_coinbase_candles_page(product: str, start: int, end: int) -> List[List[Any]]:
    r = requests.get(
        f"{_COINBASE}/{product}/candles",
        params={"granularity": _GRANULARITY_SEC, "start": start, "end": end},
        timeout=45,
        headers={"User-Agent": "trading-bot-v4-history/1.0"},
    )
    r.raise_for_status()
    return r.json()


def ensure_symbol_history_5m(symbol: str, days: int = 183, min_rows: int = 30_000) -> Optional[str]:
    """
    Ensure we have a deep 5m CSV for ``symbol`` by fetching from Coinbase Exchange (public).

    Returns the path to the cached CSV, or None if the symbol can't be fetched/mapped.

    Controlled by env:
      - AUTO_HISTORY_FETCH (default "1"): enable/disable fetching
      - HISTORY_CACHE_DIR: where cached CSVs are written (default: data/candles)
    """
    if os.environ.get("AUTO_HISTORY_FETCH", "1").strip().lower() in ("0", "false", "no"):
        return None

    symbol = (symbol or "").strip().upper()
    if not symbol:
        return None

    cache_dir = _auto_cache_dir()
    path = os.path.join(cache_dir, f"{symbol}.csv")

    # If we already have enough rows, don't refetch.
    existing = load_symbol_csv_5m(symbol)
    if existing and len(existing[0]) >= int(min_rows):
        return existing[1]
    if os.path.isfile(path):
        # If cache file exists, try reading it directly even if HISTORY_CSV_DIR points elsewhere.
        prev = os.environ.get("HISTORY_CSV_DIR")
        try:
            os.environ["HISTORY_CSV_DIR"] = cache_dir
            cached = load_symbol_csv_5m(symbol)
            if cached and len(cached[0]) >= int(min_rows):
                return cached[1]
        finally:
            if prev is None:
                os.environ.pop("HISTORY_CSV_DIR", None)
            else:
                os.environ["HISTORY_CSV_DIR"] = prev

    # Fetch from Coinbase
    try:
        products = _coinbase_products()
        product = _kraken_alt_to_coinbase_product(symbol, products)
        if not product:
            return None

        end_ts = int(time.time())
        start_limit = end_ts - int(days * 86400)

        by_t: Dict[int, Dict[str, Any]] = {}
        window_end = end_ts
        pause = float(os.environ.get("HISTORY_FETCH_PAUSE_SEC", "0.12"))

        while window_end > start_limit:
            window_start = max(window_end - _WINDOW_SEC, start_limit)
            chunk = _fetch_coinbase_candles_page(product, window_start, window_end)
            # Coinbase candle row: [time, low, high, open, close, volume]
            for row in chunk:
                try:
                    t0 = int(row[0])
                    low, high, o, c, vol = float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])
                except (TypeError, ValueError, IndexError):
                    continue
                by_t[t0] = {"time": t0, "open": o, "high": high, "low": low, "close": c, "volume": vol}

            window_end = window_start - _GRANULARITY_SEC
            time.sleep(pause)

        candles = [by_t[t] for t in sorted(by_t) if t >= start_limit]
        if len(candles) < 50:
            return None

        os.makedirs(cache_dir, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["time", "open", "high", "low", "close", "volume"])
            w.writeheader()
            w.writerows(candles)
        return path
    except Exception:
        return None
