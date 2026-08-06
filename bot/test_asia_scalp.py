"""Load-bearing tests for the "LRNY" leg after it was replaced by the Asia
liquidity-trap scalp (Pine v6 "asia scalp short", which emits both directions).

Every test here fails against the previous implementation, which was a
long-only London-range (03:00-06:00 ET) / NY-window (08:00-11:00 ET) detector
built on ``ta.highest(high[1], n)`` — a TRAILING max with no lookforward.

What each test pins:

* the range session is 18:00-00:00 ET and CROSSES MIDNIGHT — a calendar-date
  reset would discard it entirely;
* levels freeze exactly when the range session ends, which is when the trade
  window opens;
* ``ta.pivothigh(high, 5, 5)`` is confirmed FIVE BARS AFTER the pivot bar, so a
  level that never becomes a confirmed pivot must never be swept (a trailing
  max would happily trade it) — and no signal may depend on any bar after
  itself;
* ``minSweep`` is 3 REAL ticks, not 3 basis points of price;
* premium/discount gates are measured from lastLow;
* shorts require a bearish close, longs a bullish one.
"""
import unittest
from datetime import datetime, timedelta

from bot import signals as S

ET = S.ET

# ES: 0.25 tick -> minSweep = 3 * 0.25 = 0.75, sl buffer = 2 * 0.25 = 0.50
SYM = "ES=F"
TICK = 0.25
MIN_SWEEP = 3 * TICK
SL_BUF = 2 * TICK

# A Tuesday well clear of any DST transition; EST for the whole tape.
DAY0 = datetime(2026, 2, 10, 18, 0, tzinfo=ET)


class _Tape:
    """Sequential 5m bars starting at 18:00 ET."""

    def __init__(self, start=DAY0):
        self.t = start
        self.bars = []

    def add(self, o, h, l, c, n=1, v=100.0):
        for _ in range(n):
            self.bars.append({
                "time": int(self.t.timestamp()),
                "open": o, "high": h, "low": l, "close": c, "volume": v,
            })
            self.t += timedelta(minutes=5)
        return self

    @property
    def i(self):
        return len(self.bars)


def _base(n):
    """Quiet bar: high 105 / low 100. Every one is identical, so the strict
    pivot comparison finds no pivot among them."""
    return dict(o=102.0, h=105.0, l=100.0, c=102.0, n=n)


def _short_tape(sweep=(109.0, 111.0, 107.0, 108.0), pre_trade_base=0, tail=20):
    """Asia 18:00-23:55 with a single pivot high at 110, then a trade window."""
    t = _Tape()
    t.add(**_base(10))                       # bars 0-9
    t.add(106.0, 110.0, 105.0, 106.0)        # bar 10: pivot high 110 (confirms at 15)
    t.add(**_base(61))                       # bars 11-71, through 23:55
    assert t.i == 72 and t.t.hour == 0 and t.t.minute == 0
    if pre_trade_base:
        t.add(**_base(pre_trade_base))
    sweep_idx = t.i
    o, h, l, c = sweep
    t.add(o, h, l, c)
    t.add(**_base(tail))
    return t.bars, sweep_idx


class AsiaScalpShort(unittest.TestCase):
    def test_short_fires_on_first_trade_bar_after_midnight(self):
        bars, idx = _short_tape()
        sigs = S.detect_pine_lrny_signals(bars, symbol=SYM)

        self.assertEqual(len(sigs), 1, "exactly one sweep bar qualifies")
        s = sigs[0]
        self.assertEqual(s["type"], "LRNY")          # key name is contractual
        self.assertEqual(s["direction"], "SELL")
        self.assertEqual(s["bar_index"], idx)
        self.assertEqual(s["entry"], 108.0)
        # Stop convention matches every other leg: sweep extreme + tick buffer.
        self.assertAlmostEqual(s["stop_loss"], 111.0 + SL_BUF, places=8)
        # 00:00 ET, not the old 08:00-11:00 NY window.
        self.assertEqual(S.candle_dt(bars[idx]).hour, 0)

    def test_range_crosses_midnight_and_freezes_at_the_boundary(self):
        """The high that defines the range is set at 18:50, before midnight. A
        calendar-date reset would throw those bars away and leave no range."""
        bars, _ = _short_tape()
        s = S.detect_pine_lrny_signals(bars, symbol=SYM)[0]
        self.assertEqual(s["asia_high"], 110.0)
        self.assertEqual(s["asia_low"], 100.0)
        self.assertEqual(S.candle_dt(bars[10]).hour, 18)

    def test_sweep_threshold_is_three_real_ticks_not_basis_points(self):
        """3 ticks on ES is 0.75. Three basis points of a 110 price is 0.033 —
        a 0.50 overshoot must NOT qualify, and 1.00 must."""
        self.assertEqual(S.tick_size_for(SYM), TICK)

        under, _ = _short_tape(sweep=(109.0, 110.0 + 0.50, 107.0, 108.0))
        self.assertEqual(S.detect_pine_lrny_signals(under, symbol=SYM), [])

        over, _ = _short_tape(sweep=(109.0, 110.0 + 1.00, 107.0, 108.0))
        self.assertEqual(len(S.detect_pine_lrny_signals(over, symbol=SYM)), 1)

    def test_premium_gate_measured_from_last_low(self):
        """range 100-110 -> premium starts at lastLow + 0.6*range = 106."""
        below, _ = _short_tape(sweep=(109.0, 111.0, 105.0, 105.9))
        self.assertEqual(S.detect_pine_lrny_signals(below, symbol=SYM), [])

        above, _ = _short_tape(sweep=(109.0, 111.0, 105.0, 106.1))
        self.assertEqual(len(S.detect_pine_lrny_signals(above, symbol=SYM)), 1)

    def test_short_requires_a_bearish_close(self):
        bullish, _ = _short_tape(sweep=(107.0, 111.0, 107.0, 108.0))  # close > open
        self.assertEqual(S.detect_pine_lrny_signals(bullish, symbol=SYM), [])

        bearish, _ = _short_tape(sweep=(109.0, 111.0, 107.0, 108.0))
        self.assertEqual(len(S.detect_pine_lrny_signals(bearish, symbol=SYM)), 1)

    def test_short_requires_close_back_under_the_swept_pivot(self):
        no_reject, _ = _short_tape(sweep=(111.5, 112.0, 110.5, 110.5))  # close > 110
        self.assertEqual(S.detect_pine_lrny_signals(no_reject, symbol=SYM), [])

    def test_signals_only_inside_the_0000_0500_window(self):
        # 60 quiet trade bars (00:00-04:55) then the sweep at 05:00 — outside.
        late, idx = _short_tape(pre_trade_base=60)
        self.assertEqual(S.candle_dt(late[idx]).hour, 5)
        self.assertEqual(S.detect_pine_lrny_signals(late, symbol=SYM), [])

        early, idx = _short_tape(pre_trade_base=36)  # 03:00
        self.assertEqual(S.candle_dt(early[idx]).hour, 3)
        self.assertEqual(len(S.detect_pine_lrny_signals(early, symbol=SYM)), 1)


class AsiaScalpLong(unittest.TestCase):
    """The script is titled "short" but emits both directions."""

    def _tape(self, sweep):
        t = _Tape()
        t.add(**_base(10))
        t.add(99.0, 100.0, 95.0, 99.0)   # bar 10: pivot low 95 (confirms at 15)
        t.add(**_base(61))
        assert t.i == 72
        idx = t.i
        t.add(*sweep)
        t.add(**_base(20))
        return t.bars, idx

    def test_long_fires_on_a_discount_sweep(self):
        # range 95-105 -> discount below lastLow + 0.4*range = 99
        bars, idx = self._tape((96.0, 99.0, 94.0, 98.0))
        sigs = S.detect_pine_lrny_signals(bars, symbol=SYM)
        self.assertEqual(len(sigs), 1)
        s = sigs[0]
        self.assertEqual(s["direction"], "BUY")
        self.assertEqual(s["bar_index"], idx)
        self.assertEqual(s["entry"], 98.0)
        self.assertAlmostEqual(s["stop_loss"], 94.0 - SL_BUF, places=8)

    def test_long_requires_a_bullish_close(self):
        bars, _ = self._tape((98.5, 99.0, 94.0, 98.0))  # close < open
        self.assertEqual(S.detect_pine_lrny_signals(bars, symbol=SYM), [])

    def test_long_needs_three_ticks_below_the_pivot_low(self):
        under, _ = self._tape((96.0, 99.0, 95.0 - 0.50, 98.0))
        self.assertEqual(S.detect_pine_lrny_signals(under, symbol=SYM), [])
        over, _ = self._tape((96.0, 99.0, 95.0 - 1.00, 98.0))
        self.assertEqual(len(S.detect_pine_lrny_signals(over, symbol=SYM)), 1)

    def test_long_blocked_outside_discount(self):
        bars, _ = self._tape((96.0, 99.5, 94.0, 99.1))  # close above 99
        self.assertEqual(S.detect_pine_lrny_signals(bars, symbol=SYM), [])


class PivotConfirmationLag(unittest.TestCase):
    """``ta.pivothigh(high, 5, 5)`` cannot be known until five bars later."""

    def test_pivot_helper_needs_the_lookforward_bars(self):
        t = _Tape()
        t.add(**_base(6))
        t.add(106.0, 110.0, 105.0, 106.0)   # candidate at index 6
        t.add(**_base(4))                   # only 4 right bars so far
        self.assertIsNone(S._pivot_high(t.bars, 6, 5, 5))
        t.add(**_base(1))                   # 5th right bar exists
        self.assertEqual(S._pivot_high(t.bars, 6, 5, 5), 110.0)

    def test_unconfirmed_level_is_not_tradeable(self):
        """A high at 23:40 is swept at 00:00, three bars before it could have
        confirmed — so it never becomes a pivot at all and must not be traded.
        A trailing-max port ("highest high of the last N bars") fires here."""
        def tape(sweep_at):
            t = _Tape()
            t.add(**_base(68))
            t.add(106.0, 110.0, 105.0, 106.0)     # bar 68 = 23:40 candidate
            t.add(**_base(3))                     # bars 69-71
            assert t.i == 72
            t.add(**_base(sweep_at))              # quiet trade bars
            idx = t.i
            t.add(109.0, 111.0, 107.0, 108.0)     # sweep
            t.add(**_base(20))
            return t.bars, idx

        early, _ = tape(0)     # sweep at bar 72, pivot would confirm at 73
        self.assertEqual(S.detect_pine_lrny_signals(early, symbol=SYM), [])

        late, idx = tape(2)    # sweep at bar 74, pivot confirmed at 73
        sigs = S.detect_pine_lrny_signals(late, symbol=SYM)
        self.assertEqual(len(sigs), 1)
        self.assertEqual(sigs[0]["bar_index"], idx)

    def test_monotonic_tape_has_no_pivots_and_no_signals(self):
        """Strictly rising highs and lows contain no confirmed pivot in either
        direction, so nothing can be swept. A trailing max would sweep the
        running high on the first trade bar."""
        t = _Tape()
        for k in range(72):
            base = 100.0 + k * 0.1
            t.add(base + 0.5, base + 1.0, base, base + 0.5)
        t.add(112.0, 115.0, 110.0, 111.0)   # would sweep the trailing max
        t.add(**_base(20))
        self.assertEqual(S.detect_pine_lrny_signals(t.bars, symbol=SYM), [])

    def test_no_signal_depends_on_a_later_bar(self):
        """Prefix stability: truncating the tape at the signal bar must still
        produce that signal. Anything else is lookahead."""
        bars, idx = _short_tape()
        full = S.detect_pine_lrny_signals(bars, symbol=SYM)
        self.assertTrue(full)
        for s in full:
            prefix = S.detect_pine_lrny_signals(bars[: s["bar_index"] + 1], symbol=SYM)
            self.assertIn(
                s["bar_index"], [p["bar_index"] for p in prefix],
                "signal disappeared when future bars were removed -> lookahead",
            )


class Parameterisation(unittest.TestCase):
    def test_pine_defaults(self):
        """Defaults must be the Pine defaults — nothing tuned."""
        bars, _ = _short_tape()
        default = S.detect_pine_lrny_signals(bars, symbol=SYM)
        explicit = S.detect_pine_asia_scalp_signals(
            bars, symbol=SYM,
            swing_len=5, min_sweep_ticks=3, use_ema_filter=False, ema_len=50,
            range_session=(18, 0, 0, 0), trade_session=(0, 0, 5, 0),
        )
        self.assertEqual(
            [(s["bar_index"], s["direction"]) for s in default],
            [(s["bar_index"], s["direction"]) for s in explicit],
        )

    def test_sessions_are_configurable(self):
        bars, idx = _short_tape(pre_trade_base=60)  # sweep at 05:00
        self.assertEqual(S.detect_pine_lrny_signals(bars, symbol=SYM), [])
        widened = S.detect_pine_asia_scalp_signals(
            bars, symbol=SYM, trade_session=(0, 0, 6, 0)
        )
        self.assertEqual([s["bar_index"] for s in widened], [idx])

    def test_ema_filter_is_off_by_default_and_can_gate_when_on(self):
        bars, _ = _short_tape()
        self.assertEqual(len(S.detect_pine_lrny_signals(bars, symbol=SYM)), 1)
        # Price at the sweep (108) is above a 50-EMA anchored near 102, so the
        # short is blocked once the filter is switched on.
        gated = S.detect_pine_asia_scalp_signals(
            bars, symbol=SYM, use_ema_filter=True, ema_len=50
        )
        self.assertEqual(gated, [])

    def test_swing_length_is_configurable(self):
        bars, idx = _short_tape()
        self.assertEqual(
            [s["bar_index"] for s in
             S.detect_pine_asia_scalp_signals(bars, symbol=SYM, swing_len=3)],
            [idx],
        )

    def test_session_predicates(self):
        d = lambda h, m: datetime(2026, 2, 10, h, m, tzinfo=ET)
        self.assertTrue(S.is_asia_scalp_range(d(18, 0)))
        self.assertTrue(S.is_asia_scalp_range(d(23, 55)))
        self.assertFalse(S.is_asia_scalp_range(d(0, 0)))     # ends AT midnight
        self.assertFalse(S.is_asia_scalp_range(d(17, 55)))
        self.assertTrue(S.is_asia_scalp_trade(d(0, 0)))      # opens AT midnight
        self.assertTrue(S.is_asia_scalp_trade(d(4, 55)))
        self.assertFalse(S.is_asia_scalp_trade(d(5, 0)))


class BacktestContract(unittest.TestCase):
    def test_signals_simulate_with_the_shared_engine(self):
        """Entry/stop/target convention is unchanged: entry = close, stop from
        the detector, target derived by simulate_trade from the leg RR."""
        bars, idx = _short_tape()
        s = S.detect_pine_lrny_signals(bars, symbol=SYM)[0]
        out = S.simulate_trade(bars, s, rr_target=2.0)
        self.assertIsNotNone(out)
        self.assertIn(out["outcome"], ("WIN", "LOSS"))
        self.assertEqual(out["rr_target"], 2.0)


if __name__ == "__main__":
    unittest.main()
