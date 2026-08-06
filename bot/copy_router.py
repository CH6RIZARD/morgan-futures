"""
Multi-account prop-firm COPY ROUTER (self-copy / trade replication).

This is a SELF-COPY tool: it fans the *user's own* signals out across the
*user's own* prop-firm accounts (Apex, Topstep, MyFundedFutures, TradeDay,
Lucid, Tradeify, Take Profit Trader, Alpha Futures...). It never mirrors a
third party's signals, and it is never operated for compensation on behalf of
someone else. Nothing in here subscribes to an external trader.

The whole point of the module is the PRE-TRADE COMPLIANCE GATE: before a
single contract is routed to a follower account, every firm rule that could
blow that account is evaluated, and the order is either allowed, resized down,
or blocked — per account, independently, so one account's rule violation never
stops the rest of the fan-out.

Design notes:
  * Pure standard library. No network calls (routing is delegated to injected
    `executor` / `paper_engine` objects).
  * Thread-safe via a single threading.RLock, same as challenge_manager.py.
  * Account registry persisted to data/copy_accounts.json with the atomic
    tmp+os.replace pattern from contact_store.py.
  * The firm rulebook (FIRM_RULES) is plain data — prop firm rules change
    constantly, so editing a dict must be all it takes. Field names are kept
    consistent with apps/mobile/utils/propFirms.ts (FirmRule).
"""
from __future__ import annotations

import json
import os
import threading
import uuid
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# ── Contract specs (mirrors server.py CONTRACT_SPECS / rithmic_executor) ───

CONTRACT_SPECS: dict[str, dict] = {
    "ES":  {"tick_size": 0.25,    "tick_value": 12.50},
    "MES": {"tick_size": 0.25,    "tick_value": 1.25},
    "NQ":  {"tick_size": 0.25,    "tick_value": 5.00},
    "MNQ": {"tick_size": 0.25,    "tick_value": 0.50},
    "YM":  {"tick_size": 1.0,     "tick_value": 5.00},
    "MYM": {"tick_size": 1.0,     "tick_value": 0.50},
    "RTY": {"tick_size": 0.10,    "tick_value": 5.00},
    "M2K": {"tick_size": 0.10,    "tick_value": 0.50},
    "GC":  {"tick_size": 0.10,    "tick_value": 10.00},
    "MGC": {"tick_size": 0.10,    "tick_value": 1.00},
    "CL":  {"tick_size": 0.01,    "tick_value": 10.00},
    "MCL": {"tick_size": 0.01,    "tick_value": 1.00},
    "ZB":  {"tick_size": 0.03125, "tick_value": 31.25},
}

# mini ↔ micro. Used both for symbol_map defaults and for correlation grouping
# (NQ and MNQ are the same underlying for the purposes of a no-hedge rule).
MINI_TO_MICRO: dict[str, str] = {
    "ES": "MES", "NQ": "MNQ", "GC": "MGC", "YM": "MYM", "RTY": "M2K", "CL": "MCL",
}
MICRO_TO_MINI: dict[str, str] = {v: k for k, v in MINI_TO_MICRO.items()}

# Correlated instrument groups — treated as the SAME underlying by the
# no-hedge rule. Extend as needed (firms police correlated hedging too).
CORRELATION_GROUPS: dict[str, str] = {}
for _mini, _micro in MINI_TO_MICRO.items():
    CORRELATION_GROUPS[_mini] = _mini
    CORRELATION_GROUPS[_micro] = _mini


# ── Firm rulebook ──────────────────────────────────────────────────────────
# trailing_type: "static"    — drawdown floor never moves off starting balance
#                "eod"       — floor trails the end-of-day closing balance peak
#                "intraday"  — floor trails real-time peak equity (unrealized
#                              counts, so the floor ratchets up during the day)
# Field names mirror apps/mobile/utils/propFirms.ts where they overlap.

FIRM_RULES: dict[str, dict] = {
    "apex": {
        "id": "apex",
        "name": "Apex",
        "full_name": "Apex Trader Funding",
        "platform": "Rithmic",
        "trailing_type": "eod",
        "trailing_drawdown": True,
        "daily_loss_limit": None,          # Apex has no hard daily loss limit
        "consistency_limit": 50.0,         # pct of total profit in one day
        "hedging_allowed": False,
        "max_accounts": 20,
        "min_trading_days": 7,
        "default_plan": "50K",
        "plans": {
            "25K":  {"balance": 25000,  "profit_target": 1500, "max_drawdown": 1500, "max_contracts": 4},
            "50K":  {"balance": 50000,  "profit_target": 3000, "max_drawdown": 2500, "max_contracts": 10},
            "100K": {"balance": 100000, "profit_target": 6000, "max_drawdown": 3000, "max_contracts": 14},
            "150K": {"balance": 150000, "profit_target": 9000, "max_drawdown": 5000, "max_contracts": 17},
        },
        "note": "EOD trailing drawdown, bracket orders required, 30% consistency on payouts",
    },
    "topstep": {
        "id": "topstep",
        "name": "Topstep",
        "full_name": "Topstep",
        "platform": "Rithmic",
        "trailing_type": "eod",
        "trailing_drawdown": True,
        "daily_loss_limit": 1000.0,
        "consistency_limit": 50.0,
        "hedging_allowed": True,
        "max_accounts": 5,
        "min_trading_days": 5,
        "default_plan": "50K",
        "plans": {
            "50K":  {"balance": 50000,  "profit_target": 3000, "max_drawdown": 2000, "max_contracts": 5,  "daily_loss_limit": 1000.0},
            "100K": {"balance": 100000, "profit_target": 6000, "max_drawdown": 3000, "max_contracts": 10, "daily_loss_limit": 2000.0},
            "150K": {"balance": 150000, "profit_target": 9000, "max_drawdown": 4500, "max_contracts": 15, "daily_loss_limit": 3000.0},
        },
        "note": "EOD trailing drawdown, day-trade only, flat by 3:10pm ET",
    },
    "mff": {
        "id": "mff",
        "name": "MFF",
        "full_name": "My Funded Futures",
        "platform": "Rithmic",
        "trailing_type": "intraday",
        "trailing_drawdown": True,
        "daily_loss_limit": 1000.0,
        "consistency_limit": 40.0,
        "hedging_allowed": False,
        "max_accounts": 5,
        "min_trading_days": 3,
        "default_plan": "50K",
        "plans": {
            "50K":  {"balance": 50000,  "profit_target": 3000, "max_drawdown": 2000, "max_contracts": 5,  "daily_loss_limit": 1000.0},
            "100K": {"balance": 100000, "profit_target": 6000, "max_drawdown": 3000, "max_contracts": 10, "daily_loss_limit": 2000.0},
            "150K": {"balance": 150000, "profit_target": 9000, "max_drawdown": 4500, "max_contracts": 15, "daily_loss_limit": 3000.0},
        },
        "note": "Intraday trailing drawdown on Starter — floor ratchets on unrealized peak",
    },
    "tradeday": {
        "id": "tradeday",
        "name": "TradeDay",
        "full_name": "TradeDay",
        "platform": "Rithmic",
        "trailing_type": "eod",
        "trailing_drawdown": False,
        "daily_loss_limit": 2000.0,
        "consistency_limit": None,
        "hedging_allowed": True,
        "max_accounts": 10,
        "min_trading_days": 5,
        "default_plan": "50K",
        "plans": {
            "50K":  {"balance": 50000,  "profit_target": 3000, "max_drawdown": 2000, "max_contracts": 5,  "daily_loss_limit": 2000.0},
            "100K": {"balance": 100000, "profit_target": 6000, "max_drawdown": 3000, "max_contracts": 10, "daily_loss_limit": 3000.0},
        },
        "note": "Static-style max loss on funded, daily loss limit measured on floating equity",
    },
    "lucid": {
        "id": "lucid",
        "name": "Lucid",
        "full_name": "Lucid Trading (Flex)",
        "platform": "Rithmic",
        "trailing_type": "intraday",
        "trailing_drawdown": True,
        "daily_loss_limit": None,
        "consistency_limit": 50.0,
        "hedging_allowed": True,
        "max_accounts": 10,
        "min_trading_days": 0,
        "default_plan": "50K Flex",
        "plans": {
            "50K Flex":  {"balance": 50000,  "profit_target": 3000, "max_drawdown": 2000, "max_contracts": 5},
            "100K Flex": {"balance": 100000, "profit_target": 6000, "max_drawdown": 3000, "max_contracts": 10},
        },
        "note": "Trailing from peak balance; ~52% consistency cushion (see challenge_manager.py)",
    },
    "tradeify": {
        "id": "tradeify",
        "name": "Tradeify",
        "full_name": "Tradeify",
        "platform": "Tradovate",
        "trailing_type": "eod",
        "trailing_drawdown": True,
        "daily_loss_limit": 1250.0,
        "consistency_limit": 20.0,
        "hedging_allowed": True,
        "max_accounts": 5,
        "min_trading_days": 5,
        "default_plan": "50K Advanced",
        "plans": {
            "50K Advanced":  {"balance": 50000,  "profit_target": 2000, "max_drawdown": 2000, "max_contracts": 5,  "daily_loss_limit": 1250.0},
            "100K Advanced": {"balance": 100000, "profit_target": 4000, "max_drawdown": 3000, "max_contracts": 10, "daily_loss_limit": 2500.0},
        },
        "note": "Straight-to-funded Advanced plans carry a tight 20% consistency rule",
    },
    "tpt": {
        "id": "tpt",
        "name": "TPT",
        "full_name": "Take Profit Trader",
        "platform": "TopstepX",
        "trailing_type": "eod",
        "trailing_drawdown": True,
        "daily_loss_limit": 1000.0,
        "consistency_limit": 50.0,
        "hedging_allowed": True,
        "max_accounts": 5,
        "min_trading_days": 5,
        "default_plan": "50K",
        "plans": {
            "50K":  {"balance": 50000,  "profit_target": 3000, "max_drawdown": 2000, "max_contracts": 5,  "daily_loss_limit": 1000.0},
            "100K": {"balance": 100000, "profit_target": 6000, "max_drawdown": 3000, "max_contracts": 10, "daily_loss_limit": 2000.0},
        },
        "note": "Hard 4:59pm ET flat requirement — see the default 16:58-17:02 blackout window",
    },
    "alpha": {
        "id": "alpha",
        "name": "Alpha",
        "full_name": "Alpha Futures",
        "platform": "Rithmic",
        "trailing_type": "eod",
        "trailing_drawdown": True,
        "daily_loss_limit": 1250.0,
        "consistency_limit": 50.0,
        "hedging_allowed": False,
        "max_accounts": 5,
        "min_trading_days": 5,
        "default_plan": "50K",
        "plans": {
            "50K":  {"balance": 50000,  "profit_target": 3000, "max_drawdown": 2000, "max_contracts": 5,  "daily_loss_limit": 1250.0},
            "100K": {"balance": 100000, "profit_target": 6000, "max_drawdown": 3000, "max_contracts": 10, "daily_loss_limit": 2500.0},
        },
        "note": "No hedging across accounts, news-trading restrictions on evaluation",
    },
}

PLATFORMS = ("Rithmic", "Tradovate", "TopstepX", "NinjaTrader")

DEFAULT_CONFIG: dict = {
    # Master switch — "Copy signals to my accounts". Opt-in: a user who has
    # just added accounts must turn this on before anything is routed. This is
    # a normal off state, distinct from kill_switch (emergency halt + flatten).
    "enabled": False,
    "kill_switch": False,
    # Session boundary — CME/ET day roll. Daily loss limits reset here.
    "session_reset_hour_et": 17,
    # Blackout windows, ET, HH:MM. days = ISO weekday numbers (1=Mon..7=Sun);
    # empty/absent means every day.
    "blackout_windows": [
        {"label": "TPT/CME daily close", "start": "16:58", "end": "17:02", "days": [1, 2, 3, 4, 5]},
        {"label": "News buffer (8:30 ET data)", "start": "08:29", "end": "08:31", "days": [1, 2, 3, 4, 5]},
        {"label": "News buffer (FOMC 14:00 ET)", "start": "13:59", "end": "14:02", "days": [1, 2, 3]},
    ],
    # Consistency: warn only by default; flip to True to also cut size.
    "throttle_on_consistency": False,
    "consistency_throttle_qty": 1,
    # Quarantine an account after this many consecutive routing failures.
    "max_consecutive_failures": 3,
    "audit_limit": 500,
    "fanout_limit": 50,
}

_SEVERITY_INFO = "info"
_SEVERITY_WARN = "warn"
_SEVERITY_BLOCK = "block"


# ── Small helpers ──────────────────────────────────────────────────────────


def _base(symbol: str) -> str:
    """'MES=F' -> 'MES'; 'mes' -> 'MES'."""
    s = str(symbol or "").strip().upper()
    if s.endswith("=F"):
        s = s[:-2]
    return s


def _yahooish(base: str, template: str) -> str:
    """Re-apply the '=F' suffix if the source symbol carried one."""
    return f"{base}=F" if str(template or "").strip().upper().endswith("=F") else base


def _underlying(symbol: str) -> str:
    """Correlation key — NQ and MNQ collapse to 'NQ'."""
    b = _base(symbol)
    return CORRELATION_GROUPS.get(b, b)


def _spec(symbol: str) -> dict:
    return CONTRACT_SPECS.get(_base(symbol), {"tick_size": 0.25, "tick_value": 1.0})


def _risk_per_contract(symbol: str, entry: float, sl: float) -> float:
    """Worst-case dollars at risk per contract if the stop is hit."""
    sp = _spec(symbol)
    tick_size = float(sp.get("tick_size") or 0.25) or 0.25
    tick_value = float(sp.get("tick_value") or 1.0)
    if not entry or not sl:
        return 0.0
    return abs(float(entry) - float(sl)) / tick_size * tick_value


def _hhmm_to_minutes(raw: str) -> int:
    try:
        hh, mm = str(raw).split(":")
        return int(hh) * 60 + int(mm)
    except Exception:
        return -1


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _reason(rule: str, severity: str, message: str) -> dict:
    return {"rule": rule, "severity": severity, "message": message}


# ── CopyRouter ─────────────────────────────────────────────────────────────


class CopyRouter:
    """
    Fans the user's own signals across the user's own prop-firm accounts,
    applying a per-account pre-trade compliance gate first.

    Wiring (done by the caller, not here):
        router = CopyRouter(executor=_rithmic, paper_engine=_paper_engine)
        verdicts = router.evaluate(sig)     # dry run, routes nothing
        result   = router.route_signal(sig) # gate + fan out
    """

    def __init__(self, store_path=None, executor=None, paper_engine=None) -> None:
        self._lock = threading.RLock()
        self._path = self._resolve_path(store_path)
        self._executor = executor
        self._paper = paper_engine
        # Injectable clock so blackout windows and session rolls are testable.
        self._clock = lambda: datetime.now(ET)
        self._audit: list[dict] = []
        self._fanouts: list[dict] = []
        self._idempotency: dict[str, dict] = {}
        self._accounts: dict[str, dict] = {}
        self._leader_id: str | None = None
        self._config: dict = dict(DEFAULT_CONFIG)
        self._config["blackout_windows"] = [dict(w) for w in DEFAULT_CONFIG["blackout_windows"]]
        self._load()

    # ── Persistence ───────────────────────────────────────────────────────

    @staticmethod
    def _resolve_path(store_path) -> str:
        raw = str(store_path or os.environ.get("COPY_ACCOUNTS_PATH", "")).strip()
        if raw:
            return raw if os.path.isabs(raw) else os.path.normpath(os.path.join(_REPO_ROOT, raw))
        os.makedirs(os.path.join(_REPO_ROOT, "data"), exist_ok=True)
        return os.path.join(_REPO_ROOT, "data", "copy_accounts.json")

    def _load(self) -> None:
        if not os.path.isfile(self._path):
            return
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return
            for acct in data.get("accounts") or []:
                if isinstance(acct, dict) and acct.get("id"):
                    self._accounts[str(acct["id"])] = self._normalize(acct)
            leader = data.get("leader_id")
            self._leader_id = str(leader) if leader else None
            cfg = data.get("config")
            if isinstance(cfg, dict):
                self._config.update(cfg)
        except Exception:
            # A corrupt store must never stop the server from booting.
            self._accounts = {}
            self._leader_id = None

    def _save(self) -> None:
        payload = {
            "leader_id": self._leader_id,
            "accounts": list(self._accounts.values()),
            "config": self._config,
            "saved_at": _now_utc_iso(),
        }
        try:
            os.makedirs(os.path.dirname(self._path), exist_ok=True)
            tmp = f"{self._path}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            os.replace(tmp, self._path)
        except Exception as e:
            self._log(f"copy_accounts save failed: {e}", "ERROR")

    # ── Logging / audit ───────────────────────────────────────────────────

    def _log(self, message: str, level: str = "INFO") -> None:
        entry = {"time": _now_utc_iso(), "message": str(message), "decision": str(level)}
        self._audit.insert(0, entry)
        del self._audit[int(self._config.get("audit_limit", 500)):]
        print(f"[{level}] [COPY] {message}")

    def audit_log(self, limit: int = 100) -> list:
        with self._lock:
            return [dict(e) for e in self._audit[: max(0, int(limit))]]

    # ── Account normalization ─────────────────────────────────────────────

    def _normalize(self, payload: dict) -> dict:
        firm_id = str(payload.get("firm_id") or "apex").strip().lower()
        firm = FIRM_RULES.get(firm_id, FIRM_RULES["apex"])
        plan = str(payload.get("plan") or firm.get("default_plan") or "").strip()
        plan_cfg = self._plan_cfg(firm_id, plan)

        start = payload.get("starting_balance")
        start = float(start) if start not in (None, "") else float(plan_cfg.get("balance", 50000))
        balance = payload.get("balance")
        balance = float(balance) if balance not in (None, "") else start

        platform = str(payload.get("platform") or firm.get("platform") or "Rithmic").strip()
        if platform not in PLATFORMS:
            platform = firm.get("platform", "Rithmic")

        mode = str(payload.get("mode") or "paper").strip().lower()
        if mode not in ("paper", "live"):
            mode = "paper"

        smap_raw = payload.get("symbol_map") or {}
        symbol_map = {}
        if isinstance(smap_raw, dict):
            for k, v in smap_raw.items():
                symbol_map[_base(k)] = _base(v)

        trailing_dd = payload.get("trailing_dd")
        trailing_dd = float(trailing_dd) if trailing_dd not in (None, "") else float(
            plan_cfg.get("max_drawdown", 2500)
        )

        max_qty = payload.get("max_qty")
        max_qty = int(max_qty) if max_qty not in (None, "") else int(plan_cfg.get("max_contracts", 5))

        acct = {
            "id": str(payload.get("id") or uuid.uuid4().hex[:12]),
            "label": str(payload.get("label") or f"{firm.get('name', firm_id)} {plan}").strip(),
            "firm_id": firm_id,
            "plan": plan,
            "platform": platform,
            "mode": mode,
            "enabled": bool(payload.get("enabled", True)),
            "is_leader": bool(payload.get("is_leader", False)),
            "multiplier": float(payload.get("multiplier", 1.0)),
            "max_qty": max(0, max_qty),
            "symbol_map": symbol_map,
            "starting_balance": round(start, 2),
            "balance": round(balance, 2),
            "day_start_balance": round(float(payload.get("day_start_balance", balance)), 2),
            "peak_balance": round(float(payload.get("peak_balance", max(start, balance))), 2),
            # peak_equity tracks *floating* peak — only intraday-trailing firms use it.
            "peak_equity": round(float(payload.get("peak_equity", max(start, balance))), 2),
            "trailing_dd": round(trailing_dd, 2),
            "day_pnl": round(float(payload.get("day_pnl", 0.0)), 2),
            "positions": list(payload.get("positions") or []),
            "created_at": str(payload.get("created_at") or _now_utc_iso()),
            # Bookkeeping the gate needs but the caller never sets by hand.
            "daily_pnl": dict(payload.get("daily_pnl") or {}),
            "session_day": payload.get("session_day"),
            "consecutive_failures": int(payload.get("consecutive_failures", 0) or 0),
            "quarantined": bool(payload.get("quarantined", False)),
            "last_error": payload.get("last_error"),
        }
        return acct

    @staticmethod
    def _plan_cfg(firm_id: str, plan: str) -> dict:
        firm = FIRM_RULES.get(str(firm_id).lower(), FIRM_RULES["apex"])
        plans = firm.get("plans") or {}
        cfg = plans.get(plan) or plans.get(firm.get("default_plan")) or {}
        merged = {
            "balance": 50000,
            "profit_target": 3000,
            "max_drawdown": 2500,
            "max_contracts": 5,
            "daily_loss_limit": firm.get("daily_loss_limit"),
        }
        merged.update(cfg)
        if "daily_loss_limit" not in cfg:
            merged["daily_loss_limit"] = firm.get("daily_loss_limit")
        return merged

    # ── CRUD ──────────────────────────────────────────────────────────────

    def list_accounts(self) -> list:
        with self._lock:
            for acct in self._accounts.values():
                self._roll_session(acct)
            return [dict(a) for a in self._accounts.values()]

    def add_account(self, payload: dict) -> dict:
        with self._lock:
            acct = self._normalize(payload or {})
            self._accounts[acct["id"]] = acct
            if acct["is_leader"] or self._leader_id is None:
                self._set_leader_locked(acct["id"])
            self._save()
            self._log(f"account added {acct['label']} ({acct['firm_id']}/{acct['mode']})", "SYSTEM")
            return dict(acct)

    def update_account(self, account_id: str, payload: dict) -> dict:
        with self._lock:
            acct = self._accounts.get(str(account_id))
            if not acct:
                return {"ok": False, "error": "unknown_account", "account_id": account_id}
            merged = dict(acct)
            merged.update(payload or {})
            merged["id"] = acct["id"]
            updated = self._normalize(merged)
            self._accounts[updated["id"]] = updated
            if updated["is_leader"]:
                self._set_leader_locked(updated["id"])
            self._save()
            self._log(f"account updated {updated['label']}", "SYSTEM")
            return dict(updated)

    def remove_account(self, account_id: str) -> dict:
        with self._lock:
            acct = self._accounts.pop(str(account_id), None)
            if not acct:
                return {"ok": False, "error": "unknown_account", "account_id": account_id}
            if self._leader_id == str(account_id):
                self._leader_id = next(iter(self._accounts), None)
                if self._leader_id:
                    self._accounts[self._leader_id]["is_leader"] = True
            self._save()
            self._log(f"account removed {acct['label']}", "SYSTEM")
            return {"ok": True, "removed": account_id, "leader_id": self._leader_id}

    def set_leader(self, account_id: str) -> dict:
        with self._lock:
            if str(account_id) not in self._accounts:
                return {"ok": False, "error": "unknown_account", "account_id": account_id}
            self._set_leader_locked(str(account_id))
            self._save()
            return {"ok": True, "leader_id": self._leader_id}

    def _set_leader_locked(self, account_id: str) -> None:
        self._leader_id = str(account_id)
        for aid, acct in self._accounts.items():
            acct["is_leader"] = aid == self._leader_id

    # ── State / rulebook ──────────────────────────────────────────────────

    def firm_rules(self) -> list:
        return [json.loads(json.dumps(f)) for f in FIRM_RULES.values()]

    def state(self) -> dict:
        with self._lock:
            return {
                # Mirrored at the top level for the UI toggles; config keeps
                # them too so anything reading config.kill_switch still works.
                "enabled": bool(self._config.get("enabled", False)),
                "kill_switch": bool(self._config.get("kill_switch", False)),
                "leader_id": self._leader_id,
                "accounts": self.list_accounts(),
                "recent_fanouts": [dict(f) for f in self._fanouts],
                "firms": self.firm_rules(),
                "config": json.loads(json.dumps(self._config)),
            }

    def kill_switch(self, on: bool) -> dict:
        with self._lock:
            self._config["kill_switch"] = bool(on)
            self._save()
            self._log(f"KILL SWITCH {'ENGAGED — all routing halted' if on else 'released'}", "WARN")
            return {"ok": True, "kill_switch": bool(on)}

    def set_enabled(self, on: bool) -> dict:
        """Master copy switch. Off is a normal state — see kill_switch for the
        emergency halt (which the UI pairs with a flatten)."""
        with self._lock:
            self._config["enabled"] = bool(on)
            self._save()
            self._log(f"copy trading {'ENABLED' if on else 'disabled'} (master switch)", "SYSTEM")
            return {"ok": True, "enabled": bool(on), "kill_switch": bool(self._config.get("kill_switch"))}

    def set_config(self, patch: dict) -> dict:
        """
        Patch config (enabled, blackout windows, thresholds...).

        Unknown keys are REJECTED, not silently swallowed — a typo'd or
        undeclared key used to vanish without a trace, which is exactly how a
        master switch ends up doing nothing. Returns
        {ok, applied, unknown_keys, config}.
        """
        with self._lock:
            patch = dict(patch or {})
            unknown = sorted(k for k in patch if k not in DEFAULT_CONFIG)
            applied = {k: v for k, v in patch.items() if k in DEFAULT_CONFIG}
            self._config.update(applied)
            if unknown:
                self._log(f"set_config ignored unknown key(s): {', '.join(unknown)}", "WARN")
            self._save()
            return {
                "ok": not unknown,
                "applied": json.loads(json.dumps(applied)),
                "unknown_keys": unknown,
                "config": json.loads(json.dumps(self._config)),
            }

    # ── Session / equity maths ────────────────────────────────────────────

    def _now(self) -> datetime:
        try:
            return self._clock()
        except Exception:
            return datetime.now(ET)

    def _session_day(self, now_et: datetime) -> str:
        """Session id rolls at the configured ET boundary (default 5pm)."""
        hour = int(self._config.get("session_reset_hour_et", 17))
        ordinal = now_et.date().toordinal()
        if now_et.hour >= hour:
            ordinal += 1  # past the close we are already in the next session
        return datetime.fromordinal(ordinal).strftime("%Y-%m-%d")

    def _roll_session(self, acct: dict) -> None:
        """Reset daily counters when the ET session boundary is crossed."""
        sid = self._session_day(self._now())
        if acct.get("session_day") == sid:
            return
        prev = acct.get("session_day")
        if prev:
            acct.setdefault("daily_pnl", {})[prev] = round(float(acct.get("day_pnl", 0.0)), 2)
        acct["session_day"] = sid
        acct["day_start_balance"] = round(float(acct.get("balance", 0.0)), 2)
        acct["day_pnl"] = 0.0
        # EOD-trailing firms only ratchet their floor at the session close.
        acct["peak_balance"] = round(max(float(acct.get("peak_balance", 0.0)), float(acct.get("balance", 0.0))), 2)

    @staticmethod
    def _unrealized(acct: dict) -> float:
        total = 0.0
        for pos in acct.get("positions") or []:
            try:
                total += float(pos.get("unrealized_pnl_usd") or 0.0)
            except Exception:
                continue
        return total

    def _equity(self, acct: dict) -> float:
        """Floating equity — realized balance plus open-position P&L."""
        return float(acct.get("balance", 0.0)) + self._unrealized(acct)

    def _dd_floor(self, acct: dict) -> float:
        """Current drawdown floor for this account, per its firm's trail type."""
        firm = FIRM_RULES.get(acct.get("firm_id"), FIRM_RULES["apex"])
        ttype = firm.get("trailing_type", "static")
        dd = float(acct.get("trailing_dd", 0.0))
        start = float(acct.get("starting_balance", 0.0))
        if ttype == "static" or not firm.get("trailing_drawdown", True):
            return start - dd
        if ttype == "intraday":
            # Real-time trailing: the floor ratchets on peak *equity*, so an
            # unrealized winner that gives back gains can still bust you.
            peak = max(float(acct.get("peak_equity", start)), self._equity(acct), start)
            return peak - dd
        # "eod": floor trails the highest end-of-day closing balance only.
        peak = max(float(acct.get("peak_balance", start)), start)
        return peak - dd

    def _daily_loss_limit(self, acct: dict) -> float | None:
        plan_cfg = self._plan_cfg(acct.get("firm_id", "apex"), acct.get("plan", ""))
        dll = plan_cfg.get("daily_loss_limit")
        return float(dll) if dll not in (None, "") else None

    def _consistency_pct(self, acct: dict) -> tuple[float, float, float]:
        """(pct, largest_day_profit, total_profit) using realized daily P&L."""
        days = dict(acct.get("daily_pnl") or {})
        sid = acct.get("session_day")
        if sid:
            days[sid] = float(acct.get("day_pnl", 0.0))
        total = float(acct.get("balance", 0.0)) - float(acct.get("starting_balance", 0.0))
        largest = max([v for v in days.values()] or [0.0])
        pct = (largest / total * 100.0) if total > 0 else 0.0
        return pct, largest, total

    # ── Symbol mapping ────────────────────────────────────────────────────

    def _map_symbol(self, acct: dict, symbol: str) -> str:
        """Apply the account's symbol_map (mini↔micro), preserving '=F'."""
        src = _base(symbol)
        dst = (acct.get("symbol_map") or {}).get(src)
        if not dst:
            return str(symbol)
        return _yahooish(dst, symbol)

    # ── Blackout windows ──────────────────────────────────────────────────

    def _in_blackout(self, now_et: datetime) -> dict | None:
        minutes = now_et.hour * 60 + now_et.minute
        iso_dow = now_et.isoweekday()
        for w in self._config.get("blackout_windows") or []:
            days = w.get("days")
            if days and iso_dow not in list(days):
                continue
            start = _hhmm_to_minutes(w.get("start", ""))
            end = _hhmm_to_minutes(w.get("end", ""))
            if start < 0 or end < 0:
                continue
            inside = start <= minutes <= end if start <= end else (minutes >= start or minutes <= end)
            if inside:
                return dict(w)
        return None

    # ── THE COMPLIANCE GATE ───────────────────────────────────────────────

    def _verdict(self, acct: dict, signal: dict, routing: bool = False) -> dict:
        """
        Pre-trade gate for one follower account. Evaluates every rule in order
        and returns the structured verdict; nothing is routed here.

        `routing` is False for evaluate() (dry-run preview) and True for
        route_signal(). The only difference is the master `enabled` switch:
        with copying switched off the preview still shows what *would* happen,
        but an actual fan-out is blocked.
        """
        firm = FIRM_RULES.get(acct.get("firm_id"), FIRM_RULES["apex"])
        reasons: list[dict] = []
        now_et = self._now()
        self._roll_session(acct)

        side = str(signal.get("side") or signal.get("direction") or "BUY").upper()
        raw_qty = signal.get("qty")
        try:
            base_qty = int(raw_qty) if raw_qty not in (None, "") else 1
        except Exception:
            base_qty = 1
        base_qty = max(1, base_qty)
        original_qty = max(1, int(round(base_qty * float(acct.get("multiplier", 1.0)))))
        qty = original_qty
        decision = "ALLOW"

        symbol = self._map_symbol(acct, signal.get("symbol") or "")
        if _base(symbol) != _base(signal.get("symbol") or ""):
            reasons.append(_reason(
                "symbol_map", _SEVERITY_INFO,
                f"{_base(signal.get('symbol') or '')} copied as {_base(symbol)} on this account",
            ))

        entry = float(signal.get("entry") or 0.0)
        sl = float(signal.get("sl") or signal.get("stop_loss") or 0.0)
        per_contract = _risk_per_contract(symbol, entry, sl)

        def block(rule: str, message: str) -> None:
            nonlocal decision, qty
            reasons.append(_reason(rule, _SEVERITY_BLOCK, message))
            decision = "BLOCK"
            qty = 0

        def resize(rule: str, new_qty: int, message: str) -> None:
            nonlocal decision, qty
            qty = max(0, int(new_qty))
            reasons.append(_reason(rule, _SEVERITY_WARN, message))
            if qty <= 0:
                decision = "BLOCK"
            elif decision != "BLOCK":
                decision = "RESIZE"

        # 0a. Global emergency halt.
        if self._config.get("kill_switch"):
            block("kill_switch", "Global kill switch engaged — all routing halted")

        # 0b. Master copy switch (normal off state, not an emergency).
        if not self._config.get("enabled", False):
            if routing:
                if decision != "BLOCK":
                    block("disabled", "Copy trading is switched off — turn on 'Copy signals to my accounts'")
            else:
                reasons.append(_reason(
                    "disabled", _SEVERITY_INFO,
                    "Copy trading is switched off — preview only, nothing would be routed",
                ))

        # 1. Enabled + connected.
        if decision != "BLOCK":
            if not acct.get("enabled", True):
                block("enabled", "Account disabled")
            elif acct.get("quarantined"):
                block("quarantine", f"Account quarantined after repeated failures: {acct.get('last_error')}")
            elif not self._is_connected(acct):
                block("connection", f"No {acct.get('mode')} target connected for {acct.get('platform')}")

        # 2. Trailing drawdown buffer (EOD vs intraday floors differ).
        floor = self._dd_floor(acct)
        equity = self._equity(acct)
        dd_remaining = round(equity - floor, 2)
        if decision != "BLOCK":
            if dd_remaining <= 0:
                block("trailing_dd", f"Trailing drawdown exhausted (equity {equity:.2f} <= floor {floor:.2f})")
            elif per_contract > 0 and per_contract * qty > dd_remaining:
                affordable = int(dd_remaining // per_contract)
                if affordable < 1:
                    block(
                        "trailing_dd",
                        f"Stop risk ${per_contract:.2f}/contract exceeds ${dd_remaining:.2f} "
                        f"of {firm.get('trailing_type')} trailing buffer",
                    )
                else:
                    resize(
                        "trailing_dd", affordable,
                        f"Resized {qty}->{affordable} to stay inside ${dd_remaining:.2f} trailing buffer",
                    )

        # 3. Daily loss limit, measured on floating equity, ET session reset.
        dll = self._daily_loss_limit(acct)
        daily_remaining = None
        if dll is not None:
            day_move = equity - float(acct.get("day_start_balance", equity))
            daily_remaining = round(dll + min(0.0, day_move), 2)
            if decision != "BLOCK":
                if daily_remaining <= 0:
                    block("daily_loss", f"Daily loss limit hit (${abs(day_move):.2f} / ${dll:.2f})")
                elif per_contract > 0 and per_contract * qty > daily_remaining:
                    affordable = int(daily_remaining // per_contract)
                    if affordable < 1:
                        block("daily_loss", f"Stop risk exceeds ${daily_remaining:.2f} left on the daily loss limit")
                    else:
                        resize(
                            "daily_loss", affordable,
                            f"Resized {qty}->{affordable} to stay inside ${daily_remaining:.2f} daily loss buffer",
                        )

        # 4. Max position size for this account's tier — resize, never reject.
        plan_cfg = self._plan_cfg(acct.get("firm_id", "apex"), acct.get("plan", ""))
        tier_max = int(plan_cfg.get("max_contracts", 5))
        acct_max = int(acct.get("max_qty", tier_max))
        cap = max(0, min(tier_max, acct_max))
        open_qty = sum(
            int(p.get("qty") or 0)
            for p in (acct.get("positions") or [])
            if _underlying(p.get("symbol", "")) == _underlying(symbol)
        )
        room = max(0, cap - open_qty)
        if decision != "BLOCK" and qty > room:
            if room < 1:
                block("max_qty", f"Max {cap} contracts already open on {_underlying(symbol)}")
            else:
                resize("max_qty", room, f"Resized {qty}->{room} for {cap}-contract cap ({acct.get('plan')})")

        # 5. Same-direction / no-hedge rule across the whole household.
        if decision != "BLOCK" and not firm.get("hedging_allowed", True):
            opposing = self._opposing_positions(symbol, side, exclude_account=None)
            if opposing:
                where = ", ".join(f"{o['label']}:{o['symbol']} {o['side']}" for o in opposing[:3])
                block(
                    "no_hedge",
                    f"{firm.get('name')} forbids opposing positions on {_underlying(symbol)} — held by {where}",
                )

        # 6. Consistency rule — warn, optionally throttle.
        climit = firm.get("consistency_limit")
        if climit is not None:
            pct, largest, total = self._consistency_pct(acct)
            if total > 0 and pct > float(climit):
                reasons.append(_reason(
                    "consistency", _SEVERITY_WARN,
                    f"Best day ${largest:.2f} is {pct:.1f}% of ${total:.2f} profit "
                    f"(limit {float(climit):.0f}%) — payout at risk",
                ))
                if decision != "BLOCK" and self._config.get("throttle_on_consistency"):
                    throttle = int(self._config.get("consistency_throttle_qty", 1))
                    if qty > throttle:
                        resize("consistency", throttle, f"Throttled {qty}->{throttle} to dilute the consistency ratio")

        # 7. Max accounts per household for this firm.
        if decision != "BLOCK":
            max_accts = int(firm.get("max_accounts", 999))
            siblings = sorted(
                [a for a in self._accounts.values() if a.get("firm_id") == acct.get("firm_id") and a.get("enabled")],
                key=lambda a: str(a.get("created_at") or ""),
            )
            ids = [a["id"] for a in siblings]
            if acct["id"] in ids and ids.index(acct["id"]) >= max_accts:
                block(
                    "max_accounts",
                    f"{firm.get('name')} allows {max_accts} accounts per household — this is #{ids.index(acct['id']) + 1}",
                )

        # 8. Session / news blackout window.
        if decision != "BLOCK":
            window = self._in_blackout(now_et)
            if window:
                block(
                    "blackout",
                    f"Blackout {window.get('label')} {window.get('start')}-{window.get('end')} ET",
                )

        if decision == "ALLOW" and not reasons:
            reasons.append(_reason("ok", _SEVERITY_INFO, "All firm rules clear"))

        return {
            "account_id": acct["id"],
            "label": acct.get("label"),
            "firm_id": acct.get("firm_id"),
            "mode": acct.get("mode"),
            "symbol": symbol,
            "side": side,
            "decision": decision,
            "qty": int(qty),
            "original_qty": int(original_qty),
            "reasons": reasons,
            "buffer": {
                "trailing_dd_remaining": dd_remaining,
                "daily_loss_remaining": daily_remaining,
                "max_qty": int(cap),
            },
        }

    def _opposing_positions(self, symbol: str, side: str, exclude_account: str | None) -> list:
        """Every open position across the household that opposes `side`."""
        want = _underlying(symbol)
        opposite = "SELL" if str(side).upper() == "BUY" else "BUY"
        out = []
        for acct in self._accounts.values():
            if exclude_account and acct["id"] == exclude_account:
                continue
            for pos in acct.get("positions") or []:
                if _underlying(pos.get("symbol", "")) != want:
                    continue
                if str(pos.get("side", "")).upper() == opposite:
                    out.append({
                        "account_id": acct["id"],
                        "label": acct.get("label"),
                        "symbol": _base(pos.get("symbol", "")),
                        "side": str(pos.get("side", "")).upper(),
                        "qty": int(pos.get("qty") or 0),
                    })
        return out

    def _is_connected(self, acct: dict) -> bool:
        target = self._paper if acct.get("mode") == "paper" else self._executor
        if target is None:
            return False
        checker = getattr(target, "is_connected", None)
        if callable(checker):
            try:
                return bool(checker())
            except Exception:
                return False
        return True

    # ── Public: evaluate / route ──────────────────────────────────────────

    def evaluate(self, signal: dict) -> list:
        """Dry run — returns verdicts for every account, routes nothing."""
        with self._lock:
            sig = self._normalize_signal(signal)
            return [self._verdict(acct, sig, routing=False) for acct in self._followers()]

    def _followers(self) -> list:
        """Accounts that receive the fan-out (everything but the leader)."""
        return [a for a in self._accounts.values() if a["id"] != self._leader_id]

    @staticmethod
    def _normalize_signal(signal: dict) -> dict:
        """
        Accept both the documented copy-router signal shape and the raw dict
        /api/signal/test builds (`type`/`direction`/`stop_loss`/`take_profit`).
        """
        s = dict(signal or {})
        sid = s.get("id") or s.get("signal_id") or uuid.uuid4().hex[:12]
        return {
            "id": str(sid),
            "symbol": s.get("symbol") or "",
            "side": str(s.get("side") or s.get("direction") or "BUY").upper(),
            "qty": s.get("qty") or 1,
            "entry": float(s.get("entry") or 0.0),
            "sl": float(s.get("sl") if s.get("sl") not in (None, "") else (s.get("stop_loss") or 0.0)),
            "tp": float(s.get("tp") if s.get("tp") not in (None, "") else (s.get("take_profit") or 0.0)),
            "strategy": str(s.get("strategy") or s.get("type") or "").upper(),
            "ts": s.get("ts") or s.get("time") or _now_utc_iso(),
        }

    def route_signal(self, signal: dict) -> dict:
        """Gate every follower account, then fan out to the ones that pass."""
        with self._lock:
            sig = self._normalize_signal(signal)
            signal_id = sig["id"]
            results: list[dict] = []

            for acct in list(self._followers()):
                verdict = self._verdict(acct, sig, routing=True)
                key = f"{signal_id}::{acct['id']}"

                prior = self._idempotency.get(key)
                if prior is not None:
                    dup = dict(prior)
                    dup["duplicate"] = True
                    dup["routed"] = False
                    dup["error"] = "duplicate_suppressed"
                    results.append(dup)
                    self._log(f"duplicate suppressed {signal_id} -> {acct['label']}", "WARN")
                    continue

                result = dict(verdict)
                result["signal_id"] = signal_id
                result["routed"] = False
                result["order_id"] = None
                result["error"] = None
                result["duplicate"] = False

                attempted = False
                if verdict["decision"] == "BLOCK" or verdict["qty"] < 1:
                    result["error"] = "blocked"
                    self._log(
                        f"BLOCK {acct['label']} {sig['symbol']} — "
                        f"{'; '.join(r['message'] for r in verdict['reasons'] if r['severity'] == _SEVERITY_BLOCK)}",
                        "WARN",
                    )
                else:
                    # One failing account must never cascade into the others.
                    try:
                        attempted = True
                        order_id = self._dispatch(acct, verdict, sig)
                        if order_id:
                            result["routed"] = True
                            result["order_id"] = str(order_id)
                            acct["consecutive_failures"] = 0
                            acct["last_error"] = None
                            self._log(
                                f"{verdict['decision']} {acct['label']} {verdict['side']} "
                                f"{verdict['qty']}x{_base(verdict['symbol'])} order={order_id}",
                                "EXECUTED",
                            )
                        else:
                            result["error"] = "no_order_id_returned"
                            self._register_failure(acct, result["error"])
                    except Exception as e:
                        result["error"] = f"{type(e).__name__}: {e}"
                        self._register_failure(acct, result["error"])
                        self._log(f"route failed {acct['label']}: {result['error']}", "ERROR")

                # Only an order that actually reached an engine claims the
                # idempotency key. A gate BLOCK must not poison the key — the
                # same signal may be redelivered after the user flips the
                # master switch on, and that delivery has to route.
                if attempted:
                    self._idempotency[key] = dict(result)
                results.append(result)

            fanout = {
                "signal_id": signal_id,
                "symbol": sig["symbol"],
                "side": sig["side"],
                "strategy": sig["strategy"],
                "time": _now_utc_iso(),
                "routed": sum(1 for r in results if r.get("routed")),
                "blocked": sum(1 for r in results if r.get("decision") == "BLOCK"),
                "resized": sum(1 for r in results if r.get("decision") == "RESIZE"),
                "results": [dict(r) for r in results],
            }
            self._fanouts.insert(0, fanout)
            del self._fanouts[int(self._config.get("fanout_limit", 50)):]
            self._save()

            return {
                "ok": not self._config.get("kill_switch") and any(r.get("routed") for r in results),
                "signal_id": signal_id,
                "results": results,
            }

    def _register_failure(self, acct: dict, error: str) -> None:
        acct["consecutive_failures"] = int(acct.get("consecutive_failures", 0)) + 1
        acct["last_error"] = str(error)
        limit = int(self._config.get("max_consecutive_failures", 3))
        if acct["consecutive_failures"] >= limit and not acct.get("quarantined"):
            acct["quarantined"] = True
            self._log(f"QUARANTINED {acct['label']} after {limit} consecutive failures", "ERROR")

    def _dispatch(self, acct: dict, verdict: dict, sig: dict) -> str | None:
        """Hand the sized order to the paper engine or the live executor."""
        payload = {
            "account_id": acct["id"],
            "symbol": verdict["symbol"],
            "side": verdict["side"],
            "qty": int(verdict["qty"]),
            "entry": sig["entry"],
            "sl": sig["sl"],
            "tp": sig["tp"],
            "strategy": sig["strategy"],
            "signal_id": sig["id"],
            "mode": acct.get("mode"),
        }
        if acct.get("mode") == "paper":
            if self._paper is None:
                raise RuntimeError("paper_engine not injected")
            res = self._paper.place_order(payload) or {}
            if res.get("ok") is False:
                raise RuntimeError(str(res.get("error") or "paper_engine_rejected"))
            order = res.get("order") if isinstance(res.get("order"), dict) else {}
            return (
                res.get("order_id")
                or res.get("id")
                or res.get("basket_id")
                or order.get("id")
                or order.get("order_id")
            )
        if self._executor is None:
            raise RuntimeError("executor not injected")
        return self._executor.place_bracket_order(
            yahoo_sym=verdict["symbol"],
            side=verdict["side"],
            qty=int(verdict["qty"]),
            entry=sig["entry"],
            sl=sig["sl"],
            tp=sig["tp"],
        )

    # ── Fills / reconciliation ────────────────────────────────────────────

    def on_fill(self, account_id: str, fill: dict) -> None:
        """
        Reconcile a fill from either engine. `fill` accepts:
            {symbol, side, qty, price|fill_price, action: "OPEN"|"CLOSE",
             pnl_usd, unrealized_pnl_usd}
        """
        with self._lock:
            acct = self._accounts.get(str(account_id))
            if not acct:
                self._log(f"fill for unknown account {account_id}", "WARN")
                return
            self._roll_session(acct)
            f = dict(fill or {})
            action = str(f.get("action") or ("CLOSE" if "pnl_usd" in f else "OPEN")).upper()
            symbol = str(f.get("symbol") or "")
            side = str(f.get("side") or "BUY").upper()
            try:
                qty = int(f.get("qty") or 0)
            except Exception:
                qty = 0
            price = float(f.get("price") or f.get("fill_price") or 0.0)

            positions = list(acct.get("positions") or [])
            if action == "OPEN":
                positions.append({
                    "id": str(f.get("id") or uuid.uuid4().hex[:12]),
                    "symbol": symbol,
                    "side": side,
                    "qty": qty,
                    "entry_price": price,
                    "unrealized_pnl_usd": float(f.get("unrealized_pnl_usd") or 0.0),
                    "opened_at": _now_utc_iso(),
                })
            else:
                pnl = float(f.get("pnl_usd") or 0.0)
                remaining = qty
                kept = []
                for pos in positions:
                    if remaining > 0 and _base(pos.get("symbol", "")) == _base(symbol):
                        held = int(pos.get("qty") or 0)
                        if held <= remaining:
                            remaining -= held
                            continue
                        pos = dict(pos)
                        pos["qty"] = held - remaining
                        remaining = 0
                    kept.append(pos)
                positions = kept
                acct["balance"] = round(float(acct.get("balance", 0.0)) + pnl, 2)
                acct["day_pnl"] = round(float(acct.get("day_pnl", 0.0)) + pnl, 2)
                acct.setdefault("daily_pnl", {})[acct.get("session_day")] = acct["day_pnl"]

            acct["positions"] = positions
            equity = self._equity(acct)
            acct["peak_equity"] = round(max(float(acct.get("peak_equity", equity)), equity), 2)
            acct["peak_balance"] = round(max(float(acct.get("peak_balance", 0.0)), float(acct.get("balance", 0.0))), 2)
            self._save()
            self._log(
                f"fill {acct['label']} {action} {side} {qty}x{_base(symbol)} @ {price} "
                f"bal={acct['balance']:.2f} day={acct['day_pnl']:.2f}",
                "EXECUTED",
            )

    def mark_position(self, account_id: str, symbol: str, unrealized_pnl_usd: float) -> None:
        """Push a mark-to-market update so intraday trailing floors ratchet."""
        with self._lock:
            acct = self._accounts.get(str(account_id))
            if not acct:
                return
            for pos in acct.get("positions") or []:
                if _base(pos.get("symbol", "")) == _base(symbol):
                    pos["unrealized_pnl_usd"] = float(unrealized_pnl_usd)
            equity = self._equity(acct)
            acct["peak_equity"] = round(max(float(acct.get("peak_equity", equity)), equity), 2)

    # ── Flatten ───────────────────────────────────────────────────────────

    def flatten_all(self, account_id: str | None = None) -> dict:
        """Close every open position on one account, or on all of them."""
        with self._lock:
            targets = (
                [self._accounts[str(account_id)]] if account_id and str(account_id) in self._accounts
                else list(self._accounts.values()) if not account_id
                else []
            )
            if account_id and not targets:
                return {"ok": False, "error": "unknown_account", "account_id": account_id}

            closed = 0
            errors: list[dict] = []
            for acct in targets:
                for pos in list(acct.get("positions") or []):
                    flat_side = "SELL" if str(pos.get("side", "BUY")).upper() == "BUY" else "BUY"
                    try:
                        self._dispatch(
                            acct,
                            {"symbol": pos.get("symbol", ""), "side": flat_side, "qty": int(pos.get("qty") or 0)},
                            {"entry": 0.0, "sl": 0.0, "tp": 0.0, "strategy": "FLATTEN",
                             "id": f"flat-{uuid.uuid4().hex[:8]}"},
                        )
                    except Exception as e:
                        errors.append({"account_id": acct["id"], "error": f"{type(e).__name__}: {e}"})
                    closed += 1
                acct["positions"] = []
            self._save()
            self._log(f"FLATTEN {'ALL' if not account_id else account_id} — {closed} position(s)", "WARN")
            return {"ok": not errors, "closed": closed, "accounts": [a["id"] for a in targets], "errors": errors}
