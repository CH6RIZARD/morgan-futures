"""Regression tests for the instrument-scale and session-boundary defects in
bot/signals.py.

Each test pins one behaviour that was measurably wrong on live 5m futures data:

* ``adaptive_lookback`` compared a bar range expressed as a FRACTION OF PRICE
  against absolute constants, so it graded instruments instead of regimes.
* ``detect_kz_signals`` reset the Asia accumulator on the CALENDAR date, which
  discarded the 20:00-24:00 half of a session that deliberately crosses
  midnight.
* ``is_asia`` (20:00-03:00) overlaps ``is_london_kz`` (02:00-05:00), and the
  range was extended by the very bar being tested against it, so no sweep could
  register during the first hour of any London killzone.
* ``*_ticks`` parameters were multiplied by 1bp of price rather than the
  contract's real tick.
* Published expectancy projected a full RR payout instead of measuring what the
  trades actually returned.
"""
import unittest
from datetime import datetime, timedelta

from bot import signals as S


ET = S.ET


def _ts(y, mo, d, h, mi):
    return int(datetime(y, mo, d, h, mi, tzinfo=ET).timestamp())


def _bar(t, o, h, l, c, v=100.0):
    return {"time": t, "open": o, "high": h, "low": l, "close": c, "volume": v}


def _flat_series(start_dt, n, price=100.0, spread=0.2, vol=100.0):
    """n consecutive 5m bars of quiet two-sided noise around `price`."""
    out = []
    t = int(start_dt.timestamp())
    for _ in range(n):
        out.append(_bar(t, price, price + spread, price - spread, price, vol))
        t += 300
    return out


class TestAdaptiveLookbackScaleInvariance(unittest.TestCase):
    """A lookback must not depend on the instrument's price level or tick."""

    def _steady(self, price, bar_range):
        """400 bars of perfectly steady volatility — no regime change at all."""
        bars = []
        t = _ts(2026, 7, 7, 0, 0)
        for _ in range(400):
            bars.append(_bar(t, price, price + bar_range, price - bar_range,
                             price, 100.0))
            t += 300
        return bars

    def _series(self, scale):
        bars = []
        t = _ts(2026, 7, 7, 0, 0)
        for i in range(400):
            base = 100.0 * scale
            rng = (0.1 if i < 300 else 0.4) * scale  # calm, then livelier
            bars.append(_bar(t, base, base + rng, base - rng, base, 100.0))
            t += 300
        return bars

    def test_same_regime_different_asset_class_gives_same_lookback(self):
        """The real defect: bar range as a FRACTION OF PRICE is an asset-class
        property, not a regime. An equity index sits near 0.06% per 5m bar and a
        metal near 0.10%; both are perfectly ordinary for their own instrument
        and must not be graded differently."""
        index_like = self._steady(5800.0, 1.75)   # ~0.06% of price per bar
        metal_like = self._steady(4300.0, 2.25)   # ~0.105% of price per bar
        for idx in (100, 250, 399):
            self.assertEqual(
                S.adaptive_lookback(index_like, idx),
                S.adaptive_lookback(metal_like, idx),
                f"at bar {idx} two instruments in identically steady regimes "
                f"got different lookbacks — volatility is being graded on an "
                f"absolute fraction of price, so the knob discriminates "
                f"between instruments instead of between regimes",
            )

    def test_lookback_is_invariant_to_price_scale(self):
        cheap = self._series(1.0)        # a ~100-priced contract
        rich = self._series(58.0)        # same shape, ~5800-priced contract
        for idx in (100, 250, 350, 399):
            self.assertEqual(
                S.adaptive_lookback(cheap, idx),
                S.adaptive_lookback(rich, idx),
                f"lookback changed with price level at bar {idx} — the "
                f"threshold is being read as an absolute fraction of price",
            )

    def test_lookback_still_reacts_to_a_real_regime_change(self):
        bars = self._series(1.0)
        calm = S.adaptive_lookback(bars, 290)
        lively = S.adaptive_lookback(bars, 399)
        self.assertLess(calm, lively, "lookback no longer adapts to volatility")


class TestTickSizes(unittest.TestCase):
    def test_real_contract_ticks(self):
        self.assertEqual(S.tick_size_for("ES=F"), 0.25)
        self.assertEqual(S.tick_size_for("GC=F"), 0.10)
        self.assertEqual(S.tick_size_for("MGC=F"), 0.10)
        self.assertEqual(S.tick_size_for("ZB=F"), 0.03125)

    def test_micro_root_not_shadowed_by_parent(self):
        # "MES" must not resolve via "ES", "MGC" must not resolve via "GC".
        self.assertEqual(S.contract_base("MES=F"), "MES")
        self.assertEqual(S.contract_base("MGC=F"), "MGC")
        self.assertEqual(S.contract_base("MNQ=F"), "MNQ")

    def test_matches_the_execution_side_spec_table(self):
        from bot.paper_engine import CONTRACT_SPECS
        for sym, spec in CONTRACT_SPECS.items():
            if sym in S.CONTRACT_TICK_SIZE:
                self.assertEqual(
                    S.CONTRACT_TICK_SIZE[sym], spec["tick_size"],
                    f"{sym} tick disagrees with the execution-side spec",
                )

    def test_unknown_symbol_falls_back_to_legacy_heuristic(self):
        bars = [_bar(0, 100, 100, 100, 100.0)]
        self.assertAlmostEqual(S.tick_size_for("WTF=F", bars), 0.01)
        self.assertIsNone(S.tick_size_for(None))


class TestAsiaSessionCrossesMidnight(unittest.TestCase):
    """The Asia window is 20:00-03:00 ET; the pre-midnight half must count."""

    def _build(self):
        # 18:00 ET Jul 7 -> 11:00 ET Jul 8, quiet 5m bars at 100.
        bars = _flat_series(datetime(2026, 7, 7, 18, 0, tzinfo=ET), 205)
        by_t = {b["time"]: b for b in bars}
        # Genuine Asia low, set BEFORE midnight.
        pre = by_t[_ts(2026, 7, 7, 21, 0)]
        pre["low"] = 95.0
        pre["close"] = 99.5
        return bars, by_t

    def test_pre_midnight_extreme_is_part_of_the_range(self):
        bars, by_t = self._build()
        # During the NY killzone, dip to 97 — below the post-midnight low (99.8)
        # but comfortably ABOVE the true Asia low of 95. This is not a sweep.
        hit = by_t[_ts(2026, 7, 8, 9, 0)]
        hit["low"] = 97.0
        hit["close"] = 99.9
        nxt = by_t[_ts(2026, 7, 8, 9, 5)]
        nxt["high"] = 105.0
        nxt["close"] = 101.0
        nxt["volume"] = 5000.0

        sigs = [s for s in S.detect_kz_signals(bars, symbol="ES=F")
                if s["direction"] == "BUY"]
        self.assertEqual(
            sigs, [],
            "a dip to 97 was treated as sweeping an Asia low of 95 — the "
            "pre-midnight half of the session is being discarded",
        )

    def test_a_true_break_of_the_pre_midnight_low_still_sweeps(self):
        bars, by_t = self._build()
        hit = by_t[_ts(2026, 7, 8, 9, 0)]
        hit["low"] = 93.0            # genuinely below the real Asia low of 95
        hit["close"] = 94.0
        nxt = by_t[_ts(2026, 7, 8, 9, 5)]
        nxt["high"] = 105.0
        nxt["close"] = 101.0         # reclaim above 95
        nxt["volume"] = 5000.0

        sigs = [s for s in S.detect_kz_signals(bars, symbol="ES=F")
                if s["direction"] == "BUY"]
        self.assertTrue(sigs, "a real sweep of the Asia low produced no signal")


class TestKillzoneOverlapDoesNotBlockSweeps(unittest.TestCase):
    """02:00-03:00 ET is inside BOTH is_asia and is_london_kz."""

    def test_sweep_can_register_in_the_first_hour_of_london(self):
        bars = _flat_series(datetime(2026, 7, 7, 18, 0, tzinfo=ET), 110)
        by_t = {b["time"]: b for b in bars}
        # Establish the Asia range before London opens.
        by_t[_ts(2026, 7, 7, 20, 30)]["low"] = 99.0
        by_t[_ts(2026, 7, 7, 22, 0)]["high"] = 101.0

        # 02:05 — inside the overlap. Sweep below the frozen 99.0.
        sweep = by_t[_ts(2026, 7, 8, 2, 5)]
        sweep.update(high=99.5, low=97.0, close=98.0)
        # 02:10 — MSS up + reclaim on strong volume.
        trig = by_t[_ts(2026, 7, 8, 2, 10)]
        trig.update(high=102.0, low=97.9, close=100.0, volume=5000.0)

        sigs = S.detect_kz_signals(bars, symbol="ES=F")
        london = [s for s in sigs
                  if s["direction"] == "BUY" and s["session"] == "LONDON"]
        self.assertTrue(
            london,
            "no sweep could register during the Asia/London overlap hour — "
            "the reference range is still moving with the bar under test",
        )
        self.assertEqual(datetime.fromisoformat(london[0]["dt"]).hour, 2)

    def test_reference_range_is_frozen_not_extended(self):
        """A killzone bar making a new extreme must not redefine the level."""
        bars = _flat_series(datetime(2026, 7, 7, 18, 0, tzinfo=ET), 110)
        by_t = {b["time"]: b for b in bars}
        by_t[_ts(2026, 7, 7, 20, 30)]["low"] = 99.0
        sweep = by_t[_ts(2026, 7, 8, 2, 5)]
        sweep.update(high=99.5, low=97.0, close=98.0)
        trig = by_t[_ts(2026, 7, 8, 2, 10)]
        trig.update(high=102.0, low=97.9, close=100.0, volume=5000.0)

        sig = [s for s in S.detect_kz_signals(bars, symbol="ES=F")
               if s["direction"] == "BUY"][0]
        # Stop sits below the sweep low, not below some later-extended range.
        self.assertLess(sig["stop_loss"], 97.0)


class TestStaleReferenceRange(unittest.TestCase):
    def test_killzone_without_a_preceding_asia_session_is_skipped(self):
        # Start at 04:00 ET so the 02:00 London killzone has already passed and
        # no Asia session exists anywhere in the series before the NY killzone.
        bars = _flat_series(datetime(2026, 7, 8, 4, 0, tzinfo=ET), 100)
        by_t = {b["time"]: b for b in bars}
        by_t[_ts(2026, 7, 8, 9, 0)].update(low=90.0, close=95.0)
        by_t[_ts(2026, 7, 8, 9, 5)].update(high=110.0, close=105.0, volume=5000.0)
        self.assertEqual(
            S.detect_kz_signals(bars, symbol="ES=F"), [],
            "traded a killzone with no Asia range to reference",
        )


class TestExpectancyIsRealized(unittest.TestCase):
    """Published expectancy must measure trades, not forecast them."""

    def _signals_and_candles(self):
        bars = _flat_series(datetime(2026, 7, 7, 9, 0, tzinfo=ET), 200)
        # Drift up so some trades win and some expire mid-flight.
        for i, b in enumerate(bars):
            drift = i * 0.01
            for k in ("open", "high", "low", "close"):
                b[k] += drift
        sigs = [{"type": "T", "direction": "BUY", "bar_index": i,
                 "entry": bars[i]["close"], "stop_loss": bars[i]["close"] - 1.0,
                 "time": bars[i]["time"]}
                for i in range(20, 60, 4)]
        return bars, sigs

    def test_batch_stats_expectancy_equals_mean_realized_r(self):
        bars, sigs = self._signals_and_candles()
        st = S._batch_stats(bars, sigs, rr_target=2.0)
        self.assertIsNotNone(st)
        self.assertEqual(st["expectancy"], st["avg_r"],
                         "expectancy is not the realized mean R")

    def test_projection_is_kept_but_separate(self):
        bars, sigs = self._signals_and_candles()
        st = S._batch_stats(bars, sigs, rr_target=2.0)
        wr = st["winrate"] / 100.0
        self.assertAlmostEqual(st["projected_expectancy"],
                               round(wr * 2.0 - (1 - wr), 3), places=3)

    def test_optimizer_reports_the_objective_it_used(self):
        bars, sigs = self._signals_and_candles()
        _rr, meta = S.optimize_rr_for_signals(bars, sigs, 1.0, leg_name="T")
        if meta.get("method") == "path_grid":
            self.assertEqual(meta.get("objective"), "realized_mean_r")


if __name__ == "__main__":
    unittest.main(verbosity=2)
