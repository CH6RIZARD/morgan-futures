"""
Tests for the backtest data-window: lookback resolution, honest window reporting,
and the cache-freshness rule that used to pin backtests to "yesterday".

Run: ./.venv/Scripts/python.exe -m pytest bot/test_backtest_window.py -q
"""

import csv
import os
import time

import pytest

from bot import history_loader as HL
from bot import signals as S


# ---------------------------------------------------------------- helpers

def make_candles(n, interval_min=5, end_ts=None, price=100.0):
    """n synthetic bars ending `end_ts` (default: now)."""
    end_ts = int(end_ts if end_ts is not None else time.time())
    step = interval_min * 60
    out = []
    for i in range(n):
        t = end_ts - (n - 1 - i) * step
        p = price + (i % 7) * 0.25
        out.append({
            "time": t, "open": p, "high": p + 0.5, "low": p - 0.5,
            "close": p + 0.1, "volume": 1000.0,
        })
    return out


BARS_PER_DAY_5M = (24 * 60) // 5  # 288


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for k in (
        "BACKTEST_LOOKBACK_DAYS", "YF_INTRADAY_PERIOD", "YF_MAX_INTRADAY_DAYS",
        "BACKTEST_MIN_COVERAGE_FRAC", "BACKTEST_MAX_CACHE_STALE_HOURS",
        "HISTORY_MIN_COVERAGE_FRAC", "HISTORY_MAX_STALE_HOURS",
    ):
        monkeypatch.delenv(k, raising=False)


# ---------------------------------------------------------------- lookback

def test_default_lookback_is_30_days():
    assert S.resolve_lookback_days() == 30
    assert S.DEFAULT_BACKTEST_LOOKBACK_DAYS == 30


def test_explicit_argument_beats_env(monkeypatch):
    monkeypatch.setenv("BACKTEST_LOOKBACK_DAYS", "45")
    assert S.resolve_lookback_days(7) == 7
    assert S.resolve_lookback_days() == 45


def test_legacy_period_env_still_honoured(monkeypatch):
    monkeypatch.setenv("YF_INTRADAY_PERIOD", "60d")
    assert S.resolve_lookback_days() == 60
    monkeypatch.setenv("YF_INTRADAY_PERIOD", "1mo")
    assert S.resolve_lookback_days() == 30


@pytest.mark.parametrize("raw,days", [("30d", 30), ("5d", 5), ("1mo", 30),
                                      ("2y", 730), ("1wk", 7), ("", None),
                                      ("garbage", None)])
def test_period_parsing(raw, days):
    assert S._parse_period_days(raw) == days


def test_provider_cap_lookup(monkeypatch):
    assert S.provider_max_intraday_days(5) == 60
    assert S.provider_max_intraday_days(1) == 7
    monkeypatch.setenv("YF_MAX_INTRADAY_DAYS", "15")
    assert S.provider_max_intraday_days(5) == 15


# ---------------------------------------------------------------- window info

def test_window_info_reports_real_span():
    candles = make_candles(BARS_PER_DAY_5M * 10)  # 10 days
    info = S.candle_window_info(candles, requested_days=30, source="test")
    assert info["bars"] == BARS_PER_DAY_5M * 10
    assert 9.9 <= info["days_covered"] <= 10.0
    assert info["lookback_days_requested"] == 30
    assert info["window_truncated"] is True
    assert "30d requested" in info["window_note"]
    assert info["first_bar_utc"] < info["last_bar_utc"]


def test_window_info_not_truncated_when_full():
    info = S.candle_window_info(make_candles(BARS_PER_DAY_5M * 30), requested_days=30)
    assert info["window_truncated"] is False


def test_window_info_handles_empty():
    info = S.candle_window_info([], requested_days=30)
    assert info["bars"] == 0
    assert info["first_bar_utc"] is None
    assert info["days_covered"] == 0.0


# ---------------------------------------------------------------- adequacy

def test_short_window_is_not_adequate():
    """The reported bug: 300 bars (~1 day) must not satisfy a 30-day request."""
    assert S.window_is_adequate(make_candles(300), 30) is False


def test_full_window_is_adequate():
    assert S.window_is_adequate(make_candles(BARS_PER_DAY_5M * 30), 30) is True


def test_stale_window_is_not_adequate():
    old_end = time.time() - 10 * 86400
    candles = make_candles(BARS_PER_DAY_5M * 30, end_ts=old_end)
    assert S.window_is_adequate(candles, 30) is False


def test_staleness_tolerance_configurable(monkeypatch):
    old_end = time.time() - 10 * 86400
    candles = make_candles(BARS_PER_DAY_5M * 30, end_ts=old_end)
    monkeypatch.setenv("BACKTEST_MAX_CACHE_STALE_HOURS", "500")
    assert S.window_is_adequate(candles, 30) is True


# ---------------------------------------------------------------- cache rule

def test_row_count_alone_no_longer_validates_cache():
    """300 rows >= min_rows but only ~1 day: must be refetched, not reused."""
    assert HL.cache_is_fresh_enough(make_candles(300), days=30, min_rows=300) is False


def test_deep_recent_cache_is_reused():
    assert HL.cache_is_fresh_enough(
        make_candles(BARS_PER_DAY_5M * 30), days=30, min_rows=300
    ) is True


def test_stale_deep_cache_is_rejected():
    candles = make_candles(BARS_PER_DAY_5M * 30, end_ts=time.time() - 30 * 86400)
    assert HL.cache_is_fresh_enough(candles, days=30, min_rows=300) is False


def test_csv_window_info():
    info = HL.csv_window_info(make_candles(BARS_PER_DAY_5M * 5))
    assert 4.9 <= info["days_covered"] <= 5.0
    assert info["age_hours"] < 1.0


# ------------------------------------------------- backtest_symbol (offline)

def _write_csv(path, candles):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["time", "open", "high", "low", "close", "volume"])
        w.writeheader()
        w.writerows(candles)


@pytest.fixture
def offline(monkeypatch, tmp_path):
    """No network: no live fetch, no daily context, no auto history fetch."""
    monkeypatch.setenv("HISTORY_CSV_DIR", str(tmp_path))
    monkeypatch.setenv("HISTORY_CACHE_DIR", str(tmp_path / "cache"))
    monkeypatch.setenv("AUTO_HISTORY_FETCH", "0")
    monkeypatch.setenv("AUTO_RR_OPTIMIZE", "0")
    monkeypatch.setenv("OPTIMIZED_RR_CACHE", str(tmp_path / "rr.json"))
    monkeypatch.setattr(S, "fetch_daily_ohlc", lambda *a, **k: ([], 0, True))
    monkeypatch.setattr(S, "fetch_candles_paginated", lambda *a, **k: ([], 0, True))
    return tmp_path


def test_backtest_reports_true_window_from_csv(offline):
    candles = make_candles(BARS_PER_DAY_5M * 30)
    _write_csv(str(offline / "TEST=F.csv"), candles)

    r = S.backtest_symbol("TEST=F", lookback_days=30, min_candles=300)

    assert not r.get("_skip")
    # Added fields
    assert r["bars"] == len(candles)
    assert 29.9 <= r["days_covered"] <= 30.0
    assert r["lookback_days_requested"] == 30
    assert r["window_truncated"] is False
    assert r["first_bar_utc"] and r["last_bar_utc"]
    # Existing shape preserved for the dashboard / /api/rankings
    for key in ("symbol", "interval", "days_back", "has_gaps", "candles",
                "history_source", "daily_history", "optimal_rr", "score"):
        assert key in r
    for leg in ("KZ", "ORB", "ASHL", "LRNY"):
        assert set(("total", "wins", "losses", "winrate", "expectancy")) <= set(r[leg])
    assert r["candles"] == r["bars"]


def test_short_cache_is_flagged_not_silently_accepted(offline):
    """A 1-day CSV against a 30-day request must surface as truncated."""
    _write_csv(str(offline / "TEST=F.csv"), make_candles(300))

    r = S.backtest_symbol("TEST=F", lookback_days=30, min_candles=100)

    window = r if not r.get("_skip") else r
    assert window["window_truncated"] is True
    assert window["days_covered"] < 2
    assert "30d requested" in window["window_note"]


def test_lookback_argument_changes_requested_window(offline):
    _write_csv(str(offline / "TEST=F.csv"), make_candles(BARS_PER_DAY_5M * 30))

    r7 = S.backtest_symbol("TEST=F", lookback_days=7, min_candles=300)
    r90 = S.backtest_symbol("TEST=F", lookback_days=90, min_candles=300)

    assert r7["lookback_days_requested"] == 7
    assert r7["window_truncated"] is False       # 30d of data covers a 7d ask
    assert r90["lookback_days_requested"] == 90
    assert r90["window_truncated"] is True       # 30d of data cannot cover 90d


def test_env_lookback_used_when_no_argument(offline, monkeypatch):
    monkeypatch.setenv("BACKTEST_LOOKBACK_DAYS", "90")
    _write_csv(str(offline / "TEST=F.csv"), make_candles(BARS_PER_DAY_5M * 30))

    r = S.backtest_symbol("TEST=F", min_candles=300)
    assert r["lookback_days_requested"] == 90
    assert r["window_truncated"] is True


def test_skip_result_also_carries_window(offline):
    _write_csv(str(offline / "TEST=F.csv"), make_candles(60))

    r = S.backtest_symbol("TEST=F", lookback_days=30, min_candles=300)
    assert r.get("_skip") is True
    assert r["bars"] == 60
    assert r["window_truncated"] is True
    assert r["lookback_days_requested"] == 30


def test_trades_field_matches_leg_totals(offline):
    _write_csv(str(offline / "TEST=F.csv"), make_candles(BARS_PER_DAY_5M * 30))
    r = S.backtest_symbol("TEST=F", lookback_days=30, min_candles=300)
    assert r["trades"] == sum(r[l]["total"] for l in ("KZ", "ORB", "ASHL", "LRNY"))
