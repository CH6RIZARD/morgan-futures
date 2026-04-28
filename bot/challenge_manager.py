"""
Lucid PropFirm challenge state tracker and trading guard.

Lucid 50K Flex Evaluation rules enforced here:
    Profit target:    $3,000
    Max drawdown:     $2,000 (trailing from peak balance)
    Consistency:      Largest single day profit / Total profit ≤ 50%
                      (Lucid cushion: up to ~52% allowed for 50K)

All state is persisted to data/challenge_state.json so it survives restarts.
Thread-safe via RLock.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
log = logging.getLogger(__name__)

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
_STATE_FILE = os.path.join(_DATA_DIR, "challenge_state.json")


class ChallengeManager:
    # Read from env so these can be tweaked per account without code changes
    MAX_DD: float = float(os.environ.get("CHALLENGE_MAX_DD", "2000"))
    PROFIT_TARGET: float = float(os.environ.get("CHALLENGE_PROFIT_TARGET", "3000"))
    # Daily profit cap: keep any single day well under 50% of the target.
    # $1,400 / $3,000 = 46.7% — safely under the 52% Lucid cushion.
    DAILY_CAP: float = float(os.environ.get("CHALLENGE_DAILY_CAP_USD", "1400"))
    MAX_TRADES_PER_DAY: int = int(os.environ.get("MAX_TRADES_PER_DAY", "5"))
    STARTING_BALANCE: float = float(os.environ.get("PAPER_START_BALANCE", "50000"))
    # Lucid consistency cushion: largest day / total ≤ 0.52 (vs strict 0.50)
    CONSISTENCY_LIMIT: float = 0.52

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._state = self._load()

    # ── Public API ────────────────────────────────────────────────────────

    def can_trade(self) -> tuple[bool, str]:
        """
        Returns (allowed: bool, reason: str).
        Call this before placing any live order. If False, do not trade.
        """
        with self._lock:
            s = self._state
            status = s.get("status", "active")

            if status == "passed":
                return False, "Challenge already passed — withdraw or start funded account"
            if status == "busted":
                return False, "Drawdown limit hit — account busted"

            dd = self._drawdown(s)
            if dd >= self.MAX_DD:
                s["status"] = "busted"
                self._save()
                log.error(f"BUSTED: drawdown ${dd:.2f} exceeded ${self.MAX_DD}")
                return False, f"Max drawdown hit (${dd:.0f})"

            total = s["total_realized_pnl"]
            if total >= self.PROFIT_TARGET:
                # Target hit — check if consistency allows stopping
                self._check_pass(s)
                if s["status"] == "passed":
                    self._save()
                    return False, "Challenge passed!"
                # Consistency not yet met — continue but with very small sizes
                return True, f"Target hit but consistency needs work (continue trading)"

            today = _today_key()
            trades_today = s.get("trades_today", {}).get(today, 0)
            if trades_today >= self.MAX_TRADES_PER_DAY:
                return False, f"Max {self.MAX_TRADES_PER_DAY} trades/day reached"

            today_pnl = s.get("daily_pnl", {}).get(today, 0.0)
            if today_pnl >= self.DAILY_CAP:
                return False, f"Daily profit cap hit (${today_pnl:.0f}/${self.DAILY_CAP:.0f})"

            return True, "OK"

    def record_trade(self, pnl_usd: float) -> None:
        """
        Record a completed trade's realized P&L.
        Call this inside the Rithmic fill callback on every SL/TP close.
        """
        with self._lock:
            s = self._state
            today = _today_key()

            s["total_realized_pnl"] = round(s["total_realized_pnl"] + pnl_usd, 2)
            s["current_balance"] = round(self.STARTING_BALANCE + s["total_realized_pnl"], 2)
            s["peak_balance"] = max(s["peak_balance"], s["current_balance"])

            daily = s.setdefault("daily_pnl", {})
            daily[today] = round(daily.get(today, 0.0) + pnl_usd, 2)

            trade_counts = s.setdefault("trades_today", {})
            trade_counts[today] = trade_counts.get(today, 0) + 1

            s["last_updated"] = datetime.now(timezone.utc).isoformat()

            # Auto-check if challenge can be marked passed
            if s["total_realized_pnl"] >= self.PROFIT_TARGET:
                self._check_pass(s)

            self._save()

            dd = self._drawdown(s)
            largest_day = max(s.get("daily_pnl", {}).values(), default=0.0)
            total = s["total_realized_pnl"]
            consistency_pct = (largest_day / total * 100) if total > 0 else 0.0
            log.info(
                f"[CHALLENGE] trade pnl=${pnl_usd:.2f} | total=${total:.2f} "
                f"| DD=${dd:.2f} | consistency={consistency_pct:.1f}% | status={s['status']}"
            )

    def status_summary(self) -> dict:
        """Return full challenge status for the /api/challenge endpoint."""
        with self._lock:
            s = self._state
            today = _today_key()
            total = s["total_realized_pnl"]
            daily_pnl_map = s.get("daily_pnl", {})
            largest_day = max(daily_pnl_map.values(), default=0.0)
            consistency_pct = (largest_day / total * 100) if total > 0 else 0.0
            dd = self._drawdown(s)

            return {
                "status": s.get("status", "active"),
                "total_realized_pnl": round(total, 2),
                "profit_target": self.PROFIT_TARGET,
                "pct_to_target": round(min(total / self.PROFIT_TARGET * 100, 100.0), 1),
                "remaining_to_target": round(max(0.0, self.PROFIT_TARGET - total), 2),
                "current_balance": round(s["current_balance"], 2),
                "peak_balance": round(s["peak_balance"], 2),
                "drawdown": round(dd, 2),
                "drawdown_remaining": round(max(0.0, self.MAX_DD - dd), 2),
                "max_drawdown": self.MAX_DD,
                "daily_pnl_today": round(daily_pnl_map.get(today, 0.0), 2),
                "daily_cap": self.DAILY_CAP,
                "daily_remaining": round(max(0.0, self.DAILY_CAP - daily_pnl_map.get(today, 0.0)), 2),
                "largest_single_day_pnl": round(largest_day, 2),
                "consistency_pct": round(consistency_pct, 1),
                "consistency_limit_pct": round(self.CONSISTENCY_LIMIT * 100, 1),
                "consistency_ok": consistency_pct <= self.CONSISTENCY_LIMIT * 100,
                "trades_today": s.get("trades_today", {}).get(today, 0),
                "max_trades_per_day": self.MAX_TRADES_PER_DAY,
                "daily_pnl_history": {k: round(v, 2) for k, v in sorted(daily_pnl_map.items())},
                "last_updated": s.get("last_updated"),
                "live_trading_enabled": os.environ.get("LIVE_TRADING_ENABLED", "0") == "1",
            }

    def reset(self) -> None:
        """
        Reset challenge state back to starting conditions.
        Admin use only — do NOT call during a real challenge.
        """
        with self._lock:
            self._state = self._default_state()
            self._save()
        log.warning("Challenge state RESET to starting conditions")

    # ── Internal ──────────────────────────────────────────────────────────

    @staticmethod
    def _drawdown(s: dict) -> float:
        return max(0.0, float(s.get("peak_balance", 0)) - float(s.get("current_balance", 0)))

    def _check_pass(self, s: dict) -> None:
        """Mark status='passed' if profit target AND consistency are met."""
        total = s["total_realized_pnl"]
        if total < self.PROFIT_TARGET:
            return
        daily_pnl_map = s.get("daily_pnl", {})
        largest_day = max(daily_pnl_map.values(), default=0.0)
        if total > 0 and largest_day / total <= self.CONSISTENCY_LIMIT:
            s["status"] = "passed"
            log.info(
                f"CHALLENGE PASSED! Total=${total:.2f}, "
                f"largest_day=${largest_day:.2f} ({largest_day/total*100:.1f}%)"
            )
        else:
            log.info(
                f"Target hit but consistency not met: "
                f"largest_day=${largest_day:.2f} / total=${total:.2f} = "
                f"{largest_day/total*100:.1f}% (limit={self.CONSISTENCY_LIMIT*100:.0f}%). "
                f"Continue trading to dilute ratio."
            )

    def _load(self) -> dict:
        os.makedirs(_DATA_DIR, exist_ok=True)
        try:
            with open(_STATE_FILE) as f:
                data = json.load(f)
            # Validate required keys
            for key in ("status", "total_realized_pnl", "peak_balance", "current_balance"):
                if key not in data:
                    raise ValueError(f"Missing key: {key}")
            return data
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            return self._default_state()

    def _save(self) -> None:
        os.makedirs(_DATA_DIR, exist_ok=True)
        try:
            with open(_STATE_FILE, "w") as f:
                json.dump(self._state, f, indent=2)
        except Exception as e:
            log.error(f"Failed to save challenge state: {e}")

    def _default_state(self) -> dict:
        return {
            "status": "active",
            "total_realized_pnl": 0.0,
            "peak_balance": self.STARTING_BALANCE,
            "current_balance": self.STARTING_BALANCE,
            "daily_pnl": {},
            "trades_today": {},
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }


def _today_key() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d")
