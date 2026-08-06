"""
Simulated futures execution engine (paper trading).

This module is intentionally self-contained: pure standard library, no network
calls, no Flask imports. ``server.py`` owns market data and simply injects a
``price_fn(symbol) -> float | None`` callback, then drives the engine from its
background loop via :meth:`PaperEngine.tick_all`.

What it models that the previous toy engine did not:
  * Real order types — MARKET / LIMIT / STOP / STOP_LIMIT / TRAILING_STOP,
    plus brackets (entry + TP + SL) and OCO groups.
  * A fill model with CONFIGURABLE slippage (in ticks) and per-contract
    commission. TradingView's paper trading models neither, so a strategy that
    looks green there can be red here — which is the point.
  * Correct futures maths: per-symbol tick size / tick value / multiplier so
    P&L lands in real USD, plus a simple initial-margin table.
  * Netting positions per symbol (adds average the entry, opposing quantity
    reduces and can flip), realized + unrealized P&L, ET daily rollover.

Everything is dict-based so ``jsonify(engine.state())`` just works, and every
public method is guarded by a single re-entrant lock because Flask handlers and
the background loop hit the engine concurrently.
"""

from __future__ import annotations

import math
import threading
import uuid
from datetime import datetime, timedelta, timezone, tzinfo
from zoneinfo import ZoneInfo


class _FallbackEastern(tzinfo):
    """US Eastern with the post-2007 DST rules.

    Only used when the IANA database is unavailable (``tzdata`` missing on a
    bare Windows interpreter). The engine only needs ET to decide which
    calendar day a tick belongs to, so this approximation is fine.
    """

    @staticmethod
    def _second_sunday_march(year: int) -> datetime:
        d = datetime(year, 3, 8)
        return d + timedelta(days=(6 - d.weekday()) % 7)

    @staticmethod
    def _first_sunday_november(year: int) -> datetime:
        d = datetime(year, 11, 1)
        return d + timedelta(days=(6 - d.weekday()) % 7)

    def _is_dst(self, dt: datetime) -> bool:
        naive = dt.replace(tzinfo=None)
        start = self._second_sunday_march(naive.year) + timedelta(hours=2)
        end = self._first_sunday_november(naive.year) + timedelta(hours=2)
        return start <= naive < end

    def utcoffset(self, dt):
        return timedelta(hours=-4 if dt and self._is_dst(dt) else -5)

    def dst(self, dt):
        return timedelta(hours=1) if dt and self._is_dst(dt) else timedelta(0)

    def tzname(self, dt):
        return "EDT" if dt and self._is_dst(dt) else "EST"


try:
    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - only hit when tzdata is absent
    ET = _FallbackEastern()

# =====================
# CONTRACT SPECIFICATIONS
# =====================
#
# ``tick_value`` is always ``tick_size * multiplier`` — it is spelled out
# anyway so the table can be eyeballed against the exchange spec sheet.
# ``margin`` is a rough initial margin per contract (USD); it only drives the
# ``margin_used`` display, it never blocks an order.
#
# Bases mirror ``bot/signals.py::SYMBOLS`` and the mobile app's
# ``apps/mobile/utils/symbols.ts::SPECS``, extended with the micros and the
# rest of the CME complex the UI can display.

CONTRACT_SPECS: dict[str, dict] = {
    # ── Equity indices ────────────────────────────────────────────────────
    "ES":  {"tick_size": 0.25,        "tick_value": 12.50,  "multiplier": 50.0,      "margin": 15000.0, "name": "S&P 500 E-mini"},
    "MES": {"tick_size": 0.25,        "tick_value": 1.25,   "multiplier": 5.0,       "margin": 1500.0,  "name": "Micro S&P 500"},
    "NQ":  {"tick_size": 0.25,        "tick_value": 5.00,   "multiplier": 20.0,      "margin": 25000.0, "name": "Nasdaq 100 E-mini"},
    "MNQ": {"tick_size": 0.25,        "tick_value": 0.50,   "multiplier": 2.0,       "margin": 2500.0,  "name": "Micro Nasdaq 100"},
    "YM":  {"tick_size": 1.0,         "tick_value": 5.00,   "multiplier": 5.0,       "margin": 10000.0, "name": "Dow E-mini"},
    "MYM": {"tick_size": 1.0,         "tick_value": 0.50,   "multiplier": 0.5,       "margin": 1000.0,  "name": "Micro Dow"},
    "RTY": {"tick_size": 0.10,        "tick_value": 5.00,   "multiplier": 50.0,      "margin": 8000.0,  "name": "Russell 2000 E-mini"},
    "M2K": {"tick_size": 0.10,        "tick_value": 0.50,   "multiplier": 5.0,       "margin": 800.0,   "name": "Micro Russell 2000"},

    # ── Metals ────────────────────────────────────────────────────────────
    "GC":  {"tick_size": 0.10,        "tick_value": 10.00,  "multiplier": 100.0,     "margin": 12000.0, "name": "Gold"},
    "MGC": {"tick_size": 0.10,        "tick_value": 1.00,   "multiplier": 10.0,      "margin": 1200.0,  "name": "Micro Gold"},
    "SI":  {"tick_size": 0.005,       "tick_value": 25.00,  "multiplier": 5000.0,    "margin": 18000.0, "name": "Silver"},
    "SIL": {"tick_size": 0.005,       "tick_value": 5.00,   "multiplier": 1000.0,    "margin": 3600.0,  "name": "Micro Silver"},
    "HG":  {"tick_size": 0.0005,      "tick_value": 12.50,  "multiplier": 25000.0,   "margin": 6000.0,  "name": "Copper"},
    "MHG": {"tick_size": 0.0005,      "tick_value": 1.25,   "multiplier": 2500.0,    "margin": 600.0,   "name": "Micro Copper"},
    "PL":  {"tick_size": 0.10,        "tick_value": 5.00,   "multiplier": 50.0,      "margin": 5500.0,  "name": "Platinum"},

    # ── Energy ────────────────────────────────────────────────────────────
    "CL":  {"tick_size": 0.01,        "tick_value": 10.00,  "multiplier": 1000.0,    "margin": 7000.0,  "name": "Crude Oil WTI"},
    "MCL": {"tick_size": 0.01,        "tick_value": 1.00,   "multiplier": 100.0,     "margin": 700.0,   "name": "Micro Crude Oil"},
    "QM":  {"tick_size": 0.025,       "tick_value": 12.50,  "multiplier": 500.0,     "margin": 3500.0,  "name": "E-mini Crude Oil"},
    "NG":  {"tick_size": 0.001,       "tick_value": 10.00,  "multiplier": 10000.0,   "margin": 4000.0,  "name": "Natural Gas"},
    "MNG": {"tick_size": 0.001,       "tick_value": 2.50,   "multiplier": 2500.0,    "margin": 1000.0,  "name": "Micro Natural Gas"},
    "RB":  {"tick_size": 0.0001,      "tick_value": 4.20,   "multiplier": 42000.0,   "margin": 7500.0,  "name": "RBOB Gasoline"},
    "HO":  {"tick_size": 0.0001,      "tick_value": 4.20,   "multiplier": 42000.0,   "margin": 7500.0,  "name": "Heating Oil"},

    # ── Rates ─────────────────────────────────────────────────────────────
    "ZB":  {"tick_size": 0.03125,     "tick_value": 31.25,  "multiplier": 1000.0,    "margin": 4500.0,  "name": "30-Yr T-Bond"},
    "ZN":  {"tick_size": 0.015625,    "tick_value": 15.625, "multiplier": 1000.0,    "margin": 2200.0,  "name": "10-Yr T-Note"},
    "ZF":  {"tick_size": 0.0078125,   "tick_value": 7.8125, "multiplier": 1000.0,    "margin": 1400.0,  "name": "5-Yr T-Note"},
    "ZT":  {"tick_size": 0.00390625,  "tick_value": 7.8125, "multiplier": 2000.0,    "margin": 900.0,   "name": "2-Yr T-Note"},

    # ── FX ────────────────────────────────────────────────────────────────
    "6E":  {"tick_size": 0.00005,     "tick_value": 6.25,   "multiplier": 125000.0,  "margin": 2800.0,  "name": "Euro FX"},
    "6B":  {"tick_size": 0.0001,      "tick_value": 6.25,   "multiplier": 62500.0,   "margin": 2300.0,  "name": "British Pound"},
    "6J":  {"tick_size": 0.0000005,   "tick_value": 6.25,   "multiplier": 12500000.0, "margin": 3500.0, "name": "Japanese Yen"},
    "6C":  {"tick_size": 0.00005,     "tick_value": 5.00,   "multiplier": 100000.0,  "margin": 1500.0,  "name": "Canadian Dollar"},
    "6A":  {"tick_size": 0.00005,     "tick_value": 5.00,   "multiplier": 100000.0,  "margin": 1900.0,  "name": "Australian Dollar"},

    # ── Ags ───────────────────────────────────────────────────────────────
    "ZC":  {"tick_size": 0.25,        "tick_value": 12.50,  "multiplier": 50.0,      "margin": 1800.0,  "name": "Corn"},
    "ZW":  {"tick_size": 0.25,        "tick_value": 12.50,  "multiplier": 50.0,      "margin": 2500.0,  "name": "Wheat"},
    "ZS":  {"tick_size": 0.25,        "tick_value": 12.50,  "multiplier": 50.0,      "margin": 3000.0,  "name": "Soybeans"},

    # ── Crypto ────────────────────────────────────────────────────────────
    "BTC": {"tick_size": 5.0,         "tick_value": 25.00,  "multiplier": 5.0,       "margin": 90000.0, "name": "Bitcoin"},
    "MBT": {"tick_size": 5.0,         "tick_value": 0.50,   "multiplier": 0.1,       "margin": 1800.0,  "name": "Micro Bitcoin"},
    "ETH": {"tick_size": 0.50,        "tick_value": 25.00,  "multiplier": 50.0,      "margin": 20000.0, "name": "Ether"},
}

# Sane fallback for anything not in the table: 1 point == $1 per contract.
DEFAULT_SPEC: dict = {
    "tick_size": 0.25,
    "tick_value": 1.00,
    "multiplier": 4.0,
    "margin": 1000.0,
    "name": "Unknown",
}

ORDER_TYPES = ("MARKET", "LIMIT", "STOP", "STOP_LIMIT", "TRAILING_STOP")
TIFS = ("GTC", "DAY", "IOC")
SIDES = ("BUY", "SELL")

MAX_LOG = 400
MAX_TRADES = 2000
MAX_ORDERS = 1000


# =====================
# HELPERS
# =====================

def contract_base(symbol: str) -> str:
    """``"MGC=F"`` -> ``"MGC"``. Also tolerates ``"ESZ5"``-ish and ``"ES/USD"``."""
    s = str(symbol or "").strip().upper()
    if s.endswith("=F"):
        s = s[:-2]
    for sep in ("/", " ", ":"):
        if sep in s:
            s = s.split(sep)[0]
    return s


def spec_for(symbol: str) -> dict:
    """Contract spec for a symbol, falling back to :data:`DEFAULT_SPEC`."""
    base = contract_base(symbol)
    sp = CONTRACT_SPECS.get(base)
    if sp is None:
        return dict(DEFAULT_SPEC)
    return dict(sp)


def _num(v, default=None):
    """Lenient float coercion — the mobile app posts strings and empty strings."""
    if v is None:
        return default
    if isinstance(v, bool):
        return default
    try:
        if isinstance(v, str) and not v.strip():
            return default
        f = float(v)
    except Exception:
        return default
    if not math.isfinite(f):
        return default
    return f


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _day_id_et(ts: float | None = None) -> int:
    """Integer YYYYMMDD in America/New_York — the daily P&L rollover boundary."""
    if ts is None:
        n = datetime.now(ET)
    else:
        n = datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone(ET)
    return n.year * 10000 + n.month * 100 + n.day


def _r6(v) -> float:
    return round(float(v), 6)


def _r2(v) -> float:
    return round(float(v), 2)


def _qty_out(q: float):
    """Contracts are whole in practice; emit ints so the UI formats cleanly."""
    f = float(q)
    return int(round(f)) if abs(f - round(f)) < 1e-9 else round(f, 4)


def _public(d: dict) -> dict:
    """Copy without the engine's private ``_``-prefixed book-keeping fields."""
    return {k: v for k, v in d.items() if not k.startswith("_")}


# =====================
# ENGINE
# =====================

class PaperEngine:
    """Thread-safe simulated futures broker.

    Prices only ever enter through :meth:`on_tick` (push) or ``price_fn``
    (pull, via :meth:`tick_all`) — the engine itself never touches the network.
    """

    def __init__(
        self,
        starting_balance: float = 100000.0,
        commission_per_contract: float = 2.50,
        slippage_ticks: float = 0.0,
        price_fn=None,
    ) -> None:
        self._lock = threading.RLock()
        self.price_fn = price_fn

        self._starting_balance = float(_num(starting_balance, 100000.0) or 100000.0)
        self._commission = max(0.0, float(_num(commission_per_contract, 2.50) or 0.0))
        self._slippage_ticks = max(0.0, float(_num(slippage_ticks, 0.0) or 0.0))
        self._enabled = True

        self._seq = 0                      # monotonic order sequencing (FIFO fills)
        self._reset_books(self._starting_balance)

    # ── internal bookkeeping ────────────────────────────────────────────

    def _reset_books(self, balance: float) -> None:
        """Wipe all state. Caller must hold the lock (or be ``__init__``)."""
        self._balance = float(balance)
        self._realized = 0.0               # net of every commission paid so far
        self._commissions = 0.0
        self._orders: dict[str, dict] = {}
        self._positions: dict[str, dict] = {}      # position_id -> position
        self._by_symbol: dict[str, str] = {}       # symbol -> position_id (netting)
        self._trades: list[dict] = []              # newest first
        self._log: list[dict] = []
        self._last_price: dict[str, float] = {}
        self._day_id = _day_id_et()
        self._day_start_balance = float(balance)

    def _log_msg(self, msg: str, level: str = "INFO") -> None:
        """Same shape as ``server.py::_paper_log`` so the dashboard renders it."""
        self._log.insert(0, {"time": _utcnow(), "decision": str(level), "message": str(msg)})
        del self._log[MAX_LOG:]

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    # ── pricing ─────────────────────────────────────────────────────────

    def _mark(self, symbol: str) -> float | None:
        """Best available mark: injected ``price_fn`` first, then last tick."""
        sym = str(symbol or "").strip()
        if not sym:
            return None
        px = None
        if callable(self.price_fn):
            try:
                px = _num(self.price_fn(sym))
            except Exception as e:  # a broken price feed must never kill a request
                self._log_msg(f"price_fn({sym}) failed: {e}", "ERROR")
                px = None
        if px is None:
            px = self._last_price.get(sym)
        if px is None:
            return None
        self._last_price[sym] = float(px)
        return float(px)

    @staticmethod
    def _signed(side: str, qty: float) -> float:
        return float(qty) if str(side).upper() == "BUY" else -float(qty)

    @staticmethod
    def _opposite(side: str) -> str:
        return "SELL" if str(side).upper() == "BUY" else "BUY"

    def _pnl_usd(self, symbol: str, side: str, entry: float, exit_px: float, qty: float) -> float:
        """P&L in USD for ``qty`` contracts, tick-rounded to kill FP drift."""
        sp = spec_for(symbol)
        tick_size = float(sp.get("tick_size") or 0.25) or 0.25
        tick_value = float(sp.get("tick_value") or 1.0)
        ticks = round((float(exit_px) - float(entry)) / tick_size, 8)
        if str(side).upper() == "SELL":
            ticks = -ticks
        return round(ticks * tick_value * float(qty), 6)

    # =====================
    # PUBLIC API
    # =====================

    def state(self) -> dict:
        """Full engine snapshot — this is what ``/api/paper/state`` returns."""
        with self._lock:
            self._refresh_marks()
            open_pnl = sum(float(p.get("unrealized_pnl") or 0.0) for p in self._positions.values())
            equity = self._balance + open_pnl
            return {
                "enabled": bool(self._enabled),
                "starting_balance": _r2(self._starting_balance),
                "balance": _r2(self._balance),
                "equity": _r2(equity),
                "open_pnl": _r2(open_pnl),
                "realized_pnl": _r2(self._realized),
                "day_pnl": _r2(equity - self._day_start_balance),
                "day_start_balance": _r2(self._day_start_balance),
                "commissions": _r2(self._commissions),
                "margin_used": _r2(self._margin_used()),
                "positions": [_public(p) for p in self._sorted_positions()],
                "orders": [_public(o) for o in self._sorted_orders()],
                "trades": [dict(t) for t in self._trades[:500]],
                "performance": self._performance(),
                "log": [dict(x) for x in self._log[:200]],
                "config": self._config(),
                # Legacy keys the existing dashboard / mobile screens still read.
                "daily_start_balance": _r2(self._day_start_balance),
                "daily_pnl": _r2(equity - self._day_start_balance),
                "contract_specs": {k: dict(v) for k, v in CONTRACT_SPECS.items()},
            }

    def place_order(self, payload: dict) -> dict:
        """Validate + accept an order. Returns ``{ok, order, position?, error?}``.

        MARKET orders fill immediately when a price is available (so the HTTP
        caller gets a position back straight away, like the old engine did);
        everything else rests until a tick trips it.
        """
        with self._lock:
            try:
                order = self._build_order(payload or {})
            except ValueError as e:
                return {"ok": False, "error": str(e)}

            self._orders[order["id"]] = order
            self._trim_orders()
            self._log_msg(
                f"ORDER {order['type']} {order['side']} {order['symbol']} x{_qty_out(order['qty'])}"
                + (f" lmt={order['limit_price']}" if order.get("limit_price") is not None else "")
                + (f" stp={order['stop_price']}" if order.get("stop_price") is not None else "")
                + f" tif={order['tif']}",
                "ORDER",
            )

            events: list[dict] = []
            price = self._mark(order["symbol"])
            if price is not None:
                self._maybe_rollover()
                # Give the resting book a chance to act on the current print.
                events.extend(self._process_symbol(order["symbol"], float(price), fresh_ids={order["id"]}))
                if order["status"] == "WORKING":
                    events.extend(self._evaluate_order(order, float(price), set()))
            elif order["type"] == "MARKET" and order["tif"] == "IOC":
                self._cancel_order_obj(order, "NO_MARKET_DATA")

            pos = self._by_symbol.get(order["symbol"])
            out: dict = {"ok": True, "order": _public(order), "events": events}
            if pos and pos in self._positions:
                out["position"] = _public(self._positions[pos])
            if order["status"] == "REJECTED":
                out["ok"] = False
                out["error"] = order.get("reject_reason") or "rejected"
            return out

    def cancel_order(self, order_id: str) -> dict:
        with self._lock:
            o = self._orders.get(str(order_id or ""))
            if o is None:
                return {"ok": False, "error": "order_not_found"}
            if o["status"] != "WORKING":
                return {"ok": False, "error": f"order_{o['status'].lower()}", "order": _public(o)}
            self._cancel_order_obj(o, "MANUAL")
            return {"ok": True, "order": _public(o)}

    def modify_order(self, order_id: str, payload: dict) -> dict:
        """Amend price / qty / trail on a working order (cancel-replace in place)."""
        with self._lock:
            o = self._orders.get(str(order_id or ""))
            if o is None:
                return {"ok": False, "error": "order_not_found"}
            if o["status"] != "WORKING":
                return {"ok": False, "error": f"order_{o['status'].lower()}", "order": _public(o)}

            p = payload or {}
            if "qty" in p:
                q = _num(p.get("qty"))
                if q is None or q <= 0:
                    return {"ok": False, "error": "bad_qty"}
                if q < o["filled_qty"]:
                    return {"ok": False, "error": "qty_below_filled"}
                o["qty"] = float(q)
                o["remaining_qty"] = float(q) - float(o["filled_qty"])
            for key, field in (("limit_price", "limit_price"), ("price", "limit_price"),
                               ("stop_price", "stop_price")):
                if key in p:
                    o[field] = _num(p.get(key))
            if "trail_ticks" in p:
                t = _num(p.get("trail_ticks"), 0.0) or 0.0
                o["trail_ticks"] = max(0.0, float(t))
                o["trail_anchor"] = None      # re-anchor so the new distance takes effect
                o["trail_stop"] = None
            if "tif" in p:
                tif = str(p.get("tif") or "GTC").upper()
                if tif in TIFS:
                    o["tif"] = tif
            if o["type"] in ("LIMIT",) and o["limit_price"] is None:
                return {"ok": False, "error": "limit_price_required"}
            if o["type"] in ("STOP", "STOP_LIMIT") and o["stop_price"] is None:
                return {"ok": False, "error": "stop_price_required"}

            o["updated_at"] = _utcnow()
            self._log_msg(
                f"MODIFY {o['id'][:8]} {o['symbol']} qty={_qty_out(o['qty'])} "
                f"lmt={o.get('limit_price')} stp={o.get('stop_price')}",
                "ORDER",
            )
            self._sync_position_brackets(o["symbol"])
            return {"ok": True, "order": _public(o)}

    def cancel_all(self, symbol: str | None = None) -> dict:
        with self._lock:
            sym = str(symbol).strip() if symbol else None
            killed = []
            for o in list(self._orders.values()):
                if o["status"] != "WORKING":
                    continue
                if sym and o["symbol"] != sym:
                    continue
                self._cancel_order_obj(o, "CANCEL_ALL")
                killed.append(_public(o))
            return {"ok": True, "canceled": len(killed), "orders": killed}

    def close_position(self, position_id: str, qty: float | None = None) -> dict:
        """Market-close all (or part) of a position at the current mark."""
        with self._lock:
            pos = self._positions.get(str(position_id or ""))
            if pos is None:
                return {"ok": False, "error": "position_not_found"}
            price = self._mark(pos["symbol"])
            if price is None:
                price = _num(pos.get("mark_price")) or _num(pos.get("entry_price"))
            if price is None:
                return {"ok": False, "error": "no_market_data"}

            want = _num(qty, None)
            avail = abs(float(pos["_qty"]))
            close_qty = avail if want is None else min(abs(float(want)), avail)
            if close_qty <= 0:
                return {"ok": False, "error": "bad_qty"}

            side = self._opposite(pos["side"])
            fill_px = self._slip(pos["symbol"], side, float(price))
            trades = self._apply_fill(pos["symbol"], side, close_qty, fill_px, "MANUAL", tag=pos.get("tag"))
            out = {"ok": True, "trades": trades}
            if trades:
                out["trade"] = trades[0]
            if position_id in self._positions:
                out["position"] = _public(self._positions[position_id])
            return out

    def flatten_all(self) -> dict:
        """Cancel every working order, then market-close every position."""
        with self._lock:
            self.cancel_all()
            trades: list[dict] = []
            for pid in list(self._positions.keys()):
                res = self.close_position(pid)
                trades.extend(res.get("trades") or [])
            self._log_msg(f"FLATTEN ALL — {len(trades)} position(s) closed", "EXECUTED")
            return {"ok": True, "trades": trades, "closed": len(trades)}

    def reset(self, balance: float | None = None) -> dict:
        """Wipe positions/orders/trades and restart the account."""
        with self._lock:
            bal = _num(balance, None)
            if bal is None:
                bal = self._starting_balance
            self._starting_balance = float(bal)
            self._reset_books(float(bal))
            self._log_msg(f"RESET — account restarted at ${float(bal):,.2f}", "EXECUTED")
            return {"ok": True, "state": self.state()}

    def configure(self, **kwargs) -> dict:
        """Update commission / slippage / starting balance / enabled at runtime."""
        with self._lock:
            if "commission_per_contract" in kwargs:
                v = _num(kwargs.get("commission_per_contract"))
                if v is None or v < 0:
                    return {"ok": False, "error": "bad_commission"}
                self._commission = float(v)
            if "slippage_ticks" in kwargs:
                v = _num(kwargs.get("slippage_ticks"))
                if v is None or v < 0:
                    return {"ok": False, "error": "bad_slippage"}
                self._slippage_ticks = float(v)
            if "starting_balance" in kwargs:
                v = _num(kwargs.get("starting_balance"))
                if v is None or v <= 0:
                    return {"ok": False, "error": "bad_starting_balance"}
                # Treat as a deposit/withdrawal so realized P&L stays meaningful.
                delta = float(v) - self._starting_balance
                self._starting_balance = float(v)
                self._balance += delta
                self._day_start_balance += delta
            if "enabled" in kwargs:
                self._enabled = bool(kwargs.get("enabled"))
            if "price_fn" in kwargs and callable(kwargs.get("price_fn")):
                self.price_fn = kwargs.get("price_fn")
            self._log_msg(
                f"CONFIG commission=${self._commission} slippage={self._slippage_ticks}t "
                f"start=${self._starting_balance:,.2f} enabled={self._enabled}",
                "INFO",
            )
            return {"ok": True, "config": self._config()}

    def on_tick(self, symbol: str, price: float, ts: float | None = None) -> list:
        """Feed one print in. Returns the list of events it caused."""
        with self._lock:
            px = _num(price)
            sym = str(symbol or "").strip()
            if not sym or px is None or px <= 0:
                return []
            self._last_price[sym] = float(px)
            events = self._maybe_rollover(ts)
            events.extend(self._process_symbol(sym, float(px)))
            self._mark_position(sym, float(px))
            return events

    def tick_all(self) -> list:
        """Pull a price for every symbol we care about and process it."""
        with self._lock:
            if not self._enabled:
                return []
            symbols = set(self._by_symbol.keys())
            symbols.update(o["symbol"] for o in self._orders.values() if o["status"] == "WORKING")
            events = self._maybe_rollover()
        for sym in sorted(symbols):
            px = None
            if callable(self.price_fn):
                try:
                    px = _num(self.price_fn(sym))
                except Exception as e:
                    with self._lock:
                        self._log_msg(f"price_fn({sym}) failed: {e}", "ERROR")
                    px = None
            if px is None or px <= 0:
                continue
            events.extend(self.on_tick(sym, float(px)))
        return events

    def performance(self) -> dict:
        with self._lock:
            return self._performance()

    # =====================
    # ORDER CONSTRUCTION
    # =====================

    def _build_order(self, p: dict) -> dict:
        symbol = str(p.get("symbol") or "").strip()
        if not symbol:
            raise ValueError("missing_symbol")
        side = str(p.get("side") or "BUY").strip().upper()
        if side in ("LONG", "BUY_TO_OPEN"):
            side = "BUY"
        if side in ("SHORT", "SELL_TO_OPEN"):
            side = "SELL"
        if side not in SIDES:
            raise ValueError("bad_side")

        qty = _num(p.get("qty"), 1.0)
        if qty is None or qty <= 0:
            raise ValueError("bad_qty")

        otype = str(p.get("type") or "MARKET").strip().upper().replace("-", "_").replace(" ", "_")
        if otype in ("STOPLIMIT", "STOP_LMT"):
            otype = "STOP_LIMIT"
        if otype in ("TRAIL", "TRAILING", "TRAILING_STOP_MARKET"):
            otype = "TRAILING_STOP"
        if otype not in ORDER_TYPES:
            raise ValueError("bad_order_type")

        tif = str(p.get("tif") or "GTC").strip().upper()
        if tif not in TIFS:
            tif = "GTC"

        limit_price = _num(p.get("limit_price"))
        stop_price = _num(p.get("stop_price"))
        trail_ticks = _num(p.get("trail_ticks"), 0.0) or 0.0

        if otype == "LIMIT" and limit_price is None:
            raise ValueError("limit_price_required")
        if otype in ("STOP", "STOP_LIMIT") and stop_price is None:
            raise ValueError("stop_price_required")
        if otype == "STOP_LIMIT" and limit_price is None:
            limit_price = stop_price      # classic default: limit == stop
        if otype == "TRAILING_STOP" and trail_ticks <= 0:
            raise ValueError("trail_ticks_required")

        bracket = p.get("bracket") if isinstance(p.get("bracket"), dict) else None
        # Legacy flat form used by the old /api/paper/order handler.
        legacy_sl = _num(p.get("stop_loss"), None)
        legacy_tp = _num(p.get("take_profit"), None)
        if bracket is None and (legacy_sl is not None or legacy_tp is not None):
            bracket = {}
            if legacy_sl is not None:
                bracket["sl_price"] = legacy_sl
            if legacy_tp is not None:
                bracket["tp_price"] = legacy_tp

        sp = spec_for(symbol)
        now = _utcnow()
        return {
            "id": uuid.uuid4().hex,
            "seq": self._next_seq(),
            "symbol": symbol,
            "base": contract_base(symbol),
            "side": side,
            "qty": float(qty),
            "filled_qty": 0.0,
            "remaining_qty": float(qty),
            "type": otype,
            "status": "WORKING",
            "limit_price": _r6(limit_price) if limit_price is not None else None,
            "stop_price": _r6(stop_price) if stop_price is not None else None,
            "trail_ticks": float(trail_ticks) if trail_ticks > 0 else None,
            "trail_anchor": None,
            "trail_stop": None,
            "triggered": False,
            "tif": tif,
            "oco_group": str(p.get("oco_group")) if p.get("oco_group") else None,
            "reduce_only": bool(p.get("reduce_only")),
            "tag": str(p.get("tag")) if p.get("tag") else None,
            "bracket": dict(bracket) if bracket else None,
            "parent_id": str(p.get("parent_id")) if p.get("parent_id") else None,
            "role": str(p.get("role") or "ENTRY").upper(),
            "avg_fill_price": None,
            "created_at": now,
            "updated_at": now,
            "filled_at": None,
            "day_id_et": self._day_id,
            "cancel_reason": None,
            "reject_reason": None,
            "contract": {"tick_size": sp["tick_size"], "tick_value": sp["tick_value"],
                         "multiplier": sp["multiplier"]},
        }

    def _trim_orders(self) -> None:
        """Keep the terminal-order history bounded without dropping live work."""
        if len(self._orders) <= MAX_ORDERS:
            return
        dead = [o for o in self._orders.values() if o["status"] != "WORKING"]
        dead.sort(key=lambda o: o["seq"])
        for o in dead[: max(0, len(self._orders) - MAX_ORDERS)]:
            self._orders.pop(o["id"], None)

    def _cancel_order_obj(self, o: dict, reason: str) -> None:
        o["status"] = "CANCELED"
        o["cancel_reason"] = str(reason)
        o["remaining_qty"] = 0.0
        o["updated_at"] = _utcnow()
        self._log_msg(f"CANCEL {o['type']} {o['side']} {o['symbol']} ({reason})", "ORDER")
        self._sync_position_brackets(o["symbol"])

    # =====================
    # TICK PROCESSING
    # =====================

    def _maybe_rollover(self, ts: float | None = None) -> list:
        """ET calendar-day rollover: rebase day P&L and expire DAY orders."""
        day = _day_id_et(ts)
        if day == self._day_id:
            return []
        prev = self._day_id
        self._day_id = day
        open_pnl = sum(float(p.get("unrealized_pnl") or 0.0) for p in self._positions.values())
        self._day_start_balance = self._balance + open_pnl
        expired = 0
        for o in list(self._orders.values()):
            if o["status"] == "WORKING" and o["tif"] == "DAY":
                self._cancel_order_obj(o, "DAY_EXPIRED")
                expired += 1
        self._log_msg(
            f"DAY ROLLOVER {prev} -> {day} — day start ${self._day_start_balance:,.2f}, "
            f"{expired} DAY order(s) expired",
            "INFO",
        )
        return [{"type": "ROLLOVER", "day_id_et": day, "prev_day_id_et": prev,
                 "day_start_balance": _r2(self._day_start_balance), "expired_orders": expired}]

    def _process_symbol(self, symbol: str, price: float, fresh_ids: set | None = None) -> list:
        """Walk the resting book for one symbol against a single print."""
        fresh = set(fresh_ids or ())
        events: list[dict] = []
        # FIFO by arrival; re-read status each pass since fills cancel siblings.
        book = sorted(
            [o for o in self._orders.values() if o["symbol"] == symbol],
            key=lambda o: o["seq"],
        )
        for o in book:
            if o["status"] != "WORKING" or o["id"] in fresh:
                continue
            events.extend(self._evaluate_order(o, price, fresh))
        # Trailing stops move on every print, so re-mirror sl/tp onto the position.
        self._sync_position_brackets(symbol)
        self._mark_position(symbol, price)
        return events

    def _evaluate_order(self, o: dict, price: float, fresh: set) -> list:
        """Trigger/fill a single working order against ``price``."""
        events: list[dict] = []
        otype = o["type"]
        side = o["side"]

        # 1. Trailing stops ratchet BEFORE we test for a trigger — they only
        #    ever move in the favourable direction, never loosen.
        if otype == "TRAILING_STOP":
            self._ratchet_trail(o, price)

        # 2. Trigger tests.
        triggered = False
        if otype == "MARKET":
            triggered = True
        elif otype == "LIMIT":
            triggered = (price <= o["limit_price"]) if side == "BUY" else (price >= o["limit_price"])
        elif otype in ("STOP", "STOP_LIMIT"):
            if not o["triggered"]:
                hit = (price >= o["stop_price"]) if side == "BUY" else (price <= o["stop_price"])
                if hit:
                    o["triggered"] = True
                    o["updated_at"] = _utcnow()
                    events.append({"type": "TRIGGER", "order_id": o["id"], "symbol": o["symbol"],
                                   "price": _r6(price), "order_type": otype})
                    self._log_msg(f"TRIGGER {otype} {side} {o['symbol']} @ {_r6(price)}", "ORDER")
                    if otype == "STOP_LIMIT":
                        # Becomes a resting limit; do not fill on the trigger tick.
                        return events
            if o["triggered"]:
                if otype == "STOP":
                    triggered = True
                else:
                    triggered = (price <= o["limit_price"]) if side == "BUY" else (price >= o["limit_price"])
        elif otype == "TRAILING_STOP":
            stop = o.get("trail_stop")
            if stop is not None:
                triggered = (price >= stop) if side == "BUY" else (price <= stop)
                if triggered:
                    o["triggered"] = True

        if not triggered:
            if o["tif"] == "IOC":
                self._cancel_order_obj(o, "IOC_UNFILLED")
            return events

        # 3. Fill.
        events.extend(self._fill_order(o, price, fresh))
        return events

    def _ratchet_trail(self, o: dict, price: float) -> None:
        """Anchor on the best price seen; the stop follows but never retreats."""
        dist = float(o.get("trail_ticks") or 0.0) * float(spec_for(o["symbol"])["tick_size"])
        if dist <= 0:
            return
        if o["side"] == "SELL":            # protects a long: anchor = highest print
            anchor = price if o.get("trail_anchor") is None else max(float(o["trail_anchor"]), price)
            o["trail_anchor"] = _r6(anchor)
            new_stop = _r6(anchor - dist)
            cur = o.get("trail_stop")
            o["trail_stop"] = new_stop if cur is None else _r6(max(float(cur), new_stop))
        else:                               # protects a short: anchor = lowest print
            anchor = price if o.get("trail_anchor") is None else min(float(o["trail_anchor"]), price)
            o["trail_anchor"] = _r6(anchor)
            new_stop = _r6(anchor + dist)
            cur = o.get("trail_stop")
            o["trail_stop"] = new_stop if cur is None else _r6(min(float(cur), new_stop))
        o["stop_price"] = o["trail_stop"]
        o["updated_at"] = _utcnow()

    def _slip(self, symbol: str, side: str, price: float) -> float:
        """Adverse slippage in ticks: buys pay up, sells get hit down."""
        if self._slippage_ticks <= 0:
            return _r6(price)
        tick = float(spec_for(symbol)["tick_size"])
        adj = self._slippage_ticks * tick
        return _r6(price + adj if str(side).upper() == "BUY" else price - adj)

    def _fill_price_for(self, o: dict, price: float) -> float:
        """Where the order actually prints.

        LIMIT (and a triggered STOP_LIMIT) fill *at the limit or better* — a gap
        through the level fills at the better market price and slippage can
        never push the fill past the limit. Marketable orders eat the slippage.
        """
        side = o["side"]
        resting_limit = o["type"] == "LIMIT" or (o["type"] == "STOP_LIMIT" and o["triggered"])
        if resting_limit and o.get("limit_price") is not None:
            lim = float(o["limit_price"])
            base = min(lim, price) if side == "BUY" else max(lim, price)
            slipped = self._slip(o["symbol"], side, base)
            return _r6(min(slipped, lim) if side == "BUY" else max(slipped, lim))
        return self._slip(o["symbol"], side, price)

    def _fill_order(self, o: dict, price: float, fresh: set) -> list:
        events: list[dict] = []
        qty = float(o["remaining_qty"])

        # reduce_only never opens or flips — clamp to the resting position.
        if o.get("reduce_only"):
            avail = 0.0
            pid = self._by_symbol.get(o["symbol"])
            if pid:
                pos = self._positions[pid]
                if self._signed(o["side"], 1.0) * float(pos["_qty"]) < 0:
                    avail = abs(float(pos["_qty"]))
            qty = min(qty, avail)
            if qty <= 0:
                self._cancel_order_obj(o, "REDUCE_ONLY_FLAT")
                return events

        fill_px = self._fill_price_for(o, price)
        # Blotter reason: a trailing exit reads as TRAILING even in the SL slot.
        if o["type"] == "TRAILING_STOP":
            reason = "TRAILING"
        else:
            reason = {"TP": "TP", "SL": "SL"}.get(o.get("role"), "ORDER")

        trades = self._apply_fill(o["symbol"], o["side"], qty, fill_px, reason, tag=o.get("tag"),
                                  order_id=o["id"])

        o["filled_qty"] = float(o["filled_qty"]) + qty
        o["remaining_qty"] = max(0.0, float(o["qty"]) - float(o["filled_qty"]))
        o["avg_fill_price"] = _r6(fill_px)
        o["filled_at"] = _utcnow()
        o["updated_at"] = o["filled_at"]
        o["status"] = "FILLED" if o["remaining_qty"] <= 1e-9 else "PARTIAL"
        self._log_msg(
            f"FILL {o['side']} {o['symbol']} x{_qty_out(qty)} @ {fill_px} "
            f"({o['type']}/{reason}) comm=${_r2(self._commission * qty)}",
            "EXECUTED",
        )
        events.append({"type": "FILL", "order_id": o["id"], "symbol": o["symbol"], "side": o["side"],
                       "qty": _qty_out(qty), "price": _r6(fill_px), "order_type": o["type"],
                       "reason": reason})
        for t in trades:
            events.append({"type": "CLOSE", "symbol": t["symbol"], "trade": t,
                           "net_pnl": t["net_pnl"], "reason": t["reason"]})

        # OCO: a fill kills every sibling in the group.
        if o.get("oco_group"):
            for sib in list(self._orders.values()):
                if sib["id"] != o["id"] and sib["status"] == "WORKING" \
                        and sib.get("oco_group") == o["oco_group"]:
                    self._cancel_order_obj(sib, "OCO")
                    events.append({"type": "CANCEL", "order_id": sib["id"], "symbol": sib["symbol"],
                                   "reason": "OCO"})

        # Attach the bracket once the entry actually prints.
        if o.get("bracket") and o.get("role") == "ENTRY":
            for child in self._attach_bracket(o, fill_px, qty):
                fresh.add(child["id"])
                events.append({"type": "ORDER", "order_id": child["id"], "symbol": child["symbol"],
                               "role": child["role"]})

        # Flat symbol -> no protective orders should linger.
        if o["symbol"] not in self._by_symbol:
            for other in list(self._orders.values()):
                if other["symbol"] == o["symbol"] and other["status"] == "WORKING" \
                        and other.get("reduce_only"):
                    self._cancel_order_obj(other, "POSITION_CLOSED")
                    events.append({"type": "CANCEL", "order_id": other["id"],
                                   "symbol": other["symbol"], "reason": "POSITION_CLOSED"})

        self._sync_position_brackets(o["symbol"])
        return events

    def _attach_bracket(self, entry: dict, fill_px: float, qty: float) -> list[dict]:
        """Create the OCO'd TP (limit) + SL (stop) children for a filled entry."""
        br = entry.get("bracket") or {}
        tick = float(spec_for(entry["symbol"])["tick_size"])
        long_side = entry["side"] == "BUY"
        exit_side = self._opposite(entry["side"])
        group = entry.get("oco_group") or f"br-{entry['id'][:12]}"

        tp = _num(br.get("tp_price"))
        if tp is None:
            t = _num(br.get("tp_ticks"))
            if t is not None and t > 0:
                tp = fill_px + t * tick if long_side else fill_px - t * tick
        sl = _num(br.get("sl_price"))
        if sl is None:
            t = _num(br.get("sl_ticks"))
            if t is not None and t > 0:
                sl = fill_px - t * tick if long_side else fill_px + t * tick
        trail = _num(br.get("trail_ticks")) or _num(entry.get("trail_ticks"))

        children: list[dict] = []
        common = {"symbol": entry["symbol"], "side": exit_side, "qty": qty, "tif": "GTC",
                  "oco_group": group, "reduce_only": True, "tag": entry.get("tag"),
                  "parent_id": entry["id"]}
        if tp is not None:
            c = self._build_order({**common, "type": "LIMIT", "limit_price": _r6(tp), "role": "TP"})
            self._orders[c["id"]] = c
            children.append(c)
        if sl is not None:
            c = self._build_order({**common, "type": "STOP", "stop_price": _r6(sl), "role": "SL"})
            self._orders[c["id"]] = c
            children.append(c)
        if trail and trail > 0 and sl is None:
            c = self._build_order({**common, "type": "TRAILING_STOP", "trail_ticks": trail,
                                   "role": "SL"})
            c["trail_anchor"] = _r6(fill_px)
            c["trail_stop"] = _r6(fill_px - trail * tick) if long_side else _r6(fill_px + trail * tick)
            c["stop_price"] = c["trail_stop"]
            self._orders[c["id"]] = c
            children.append(c)

        if children:
            self._log_msg(
                f"BRACKET {entry['symbol']} tp={_r6(tp) if tp is not None else None} "
                f"sl={_r6(sl) if sl is not None else None} group={group}",
                "ORDER",
            )
            self._sync_position_brackets(entry["symbol"])
        return children

    # =====================
    # POSITIONS / NETTING
    # =====================

    def _apply_fill(self, symbol: str, side: str, qty: float, price: float, reason: str,
                    tag=None, order_id=None) -> list[dict]:
        """Net ``qty`` into the symbol's position. Returns any trades booked.

        Adds average the entry; opposing quantity reduces (realizing P&L) and,
        if it exceeds the resting size, flips into a brand-new position.
        """
        qty = float(qty)
        if qty <= 0:
            return []

        commission = _r2(self._commission * qty)
        self._commissions = _r2(self._commissions + commission)
        self._balance = _r2(self._balance - commission)
        self._realized = _r2(self._realized - commission)

        trades: list[dict] = []
        signed = self._signed(side, qty)
        pid = self._by_symbol.get(symbol)
        pos = self._positions.get(pid) if pid else None

        if pos is None:
            self._open_position(symbol, side, qty, price, commission, tag)
            return trades

        cur = float(pos["_qty"])
        if (cur > 0 and signed > 0) or (cur < 0 and signed < 0):
            # ── ADD: weighted-average entry ────────────────────────────────
            new_qty = abs(cur) + qty
            pos["_entry"] = _r6((float(pos["_entry"]) * abs(cur) + price * qty) / new_qty)
            pos["_qty"] = new_qty if cur > 0 else -new_qty
            pos["_commission"] = _r2(float(pos["_commission"]) + commission)
            self._decorate_position(pos, price)
            self._log_msg(
                f"ADD {symbol} {pos['side']} x{_qty_out(qty)} @ {price} -> "
                f"x{_qty_out(abs(pos['_qty']))} avg {pos['_entry']}",
                "EXECUTED",
            )
            return trades

        # ── REDUCE / CLOSE / FLIP ──────────────────────────────────────────
        closing = min(qty, abs(cur))
        entry_comm_share = _r2(float(pos["_commission"]) * (closing / abs(cur)))
        exit_comm_share = _r2(commission * (closing / qty))
        trade = self._book_trade(pos, closing, price, reason,
                                 entry_comm_share + exit_comm_share, order_id)
        trades.append(trade)

        pos["_commission"] = _r2(float(pos["_commission"]) - entry_comm_share)
        remaining = abs(cur) - closing
        if remaining <= 1e-9:
            self._positions.pop(pos["id"], None)
            if self._by_symbol.get(symbol) == pos["id"]:
                self._by_symbol.pop(symbol, None)
            flip_qty = qty - closing
            if flip_qty > 1e-9:
                self._open_position(symbol, side, flip_qty, price,
                                    _r2(commission - exit_comm_share), tag)
                self._log_msg(f"FLIP {symbol} -> {side} x{_qty_out(flip_qty)} @ {price}", "EXECUTED")
        else:
            pos["_qty"] = remaining if cur > 0 else -remaining
            self._decorate_position(pos, price)
        return trades

    def _open_position(self, symbol: str, side: str, qty: float, price: float,
                       commission: float, tag=None) -> dict:
        sp = spec_for(symbol)
        now = _utcnow()
        pos: dict = {
            # ── keys the existing mobile UI reads (do not rename) ─────────
            "id": uuid.uuid4().hex,
            "symbol": symbol,
            "side": str(side).upper(),
            "qty": _qty_out(qty),
            "entry_price": _r6(price),
            "sl": None,
            "tp": None,
            "pnl": 0.0,
            # ── engine fields ─────────────────────────────────────────────
            "base": contract_base(symbol),
            "avg_price": _r6(price),
            "mark_price": _r6(price),
            "unrealized_pnl": 0.0,
            "unrealized_pnl_usd": 0.0,      # legacy alias
            "realized_pnl": 0.0,
            "commission": _r2(commission),
            "stop_loss": None,              # legacy alias of sl
            "take_profit": None,            # legacy alias of tp
            "trail_stop": None,
            "trail_ticks": None,
            "margin": _r2(float(sp["margin"]) * qty),
            "status": "OPEN",
            "opened_at": now,
            "updated_at": now,
            "tag": str(tag) if tag else None,
            "contract": {"tick_size": sp["tick_size"], "tick_value": sp["tick_value"],
                         "multiplier": sp["multiplier"]},
            # private, stripped from nothing but never relied on by the UI
            "_qty": self._signed(side, qty),
            "_entry": _r6(price),
            "_commission": _r2(commission),
            "_initial_sl": None,
        }
        self._positions[pos["id"]] = pos
        self._by_symbol[symbol] = pos["id"]
        self._log_msg(f"OPEN {symbol} {pos['side']} x{_qty_out(qty)} @ {pos['entry_price']}",
                      "EXECUTED")
        return pos

    def _book_trade(self, pos: dict, qty: float, exit_px: float, reason: str,
                    commission: float, order_id=None) -> dict:
        gross = self._pnl_usd(pos["symbol"], pos["side"], float(pos["_entry"]), float(exit_px), qty)
        net = _r2(gross - commission)
        self._balance = _r2(self._balance + gross)
        self._realized = _r2(self._realized + gross)
        pos["realized_pnl"] = _r2(float(pos.get("realized_pnl") or 0.0) + net)

        # R multiple against the stop that was in force when the trade opened.
        r_mult = None
        risk_ref = pos.get("_initial_sl")
        if risk_ref is not None:
            risk = abs(self._pnl_usd(pos["symbol"], pos["side"], float(pos["_entry"]),
                                     float(risk_ref), qty))
            if risk > 1e-9:
                r_mult = round(gross / risk, 3)

        opened_at = pos.get("opened_at")
        closed_at = _utcnow()
        try:
            dur = (datetime.fromisoformat(closed_at) - datetime.fromisoformat(opened_at)).total_seconds()
        except Exception:
            dur = 0.0

        trade: dict = {
            "id": uuid.uuid4().hex,
            "position_id": pos["id"],
            "order_id": order_id,
            "symbol": pos["symbol"],
            "base": pos["base"],
            "side": pos["side"],
            "qty": _qty_out(qty),
            "entry_price": _r6(pos["_entry"]),
            "exit_price": _r6(exit_px),
            "gross_pnl": _r2(gross),
            "commission": _r2(commission),
            "net_pnl": net,
            "r_multiple": r_mult,
            "reason": str(reason),
            "opened_at": opened_at,
            "closed_at": closed_at,
            "duration_sec": round(float(dur), 3),
            "tag": pos.get("tag"),
            "status": "CLOSED",
            # legacy aliases so old dashboard/mobile readers keep working
            "pnl": net,
            "pnl_usd": net,
            "realized_pnl_usd": net,
            "exit_reason": str(reason),
            "entry": _r6(pos["_entry"]),
        }
        self._trades.insert(0, trade)
        del self._trades[MAX_TRADES:]
        self._log_msg(
            f"CLOSED {pos['symbol']} {pos['side']} x{_qty_out(qty)} @ {_r6(exit_px)} ({reason}) "
            f"gross=${_r2(gross)} comm=${_r2(commission)} net=${net}",
            "EXECUTED",
        )
        return trade

    def _decorate_position(self, pos: dict, mark: float | None) -> None:
        """Refresh the public/UI-facing fields from the private book fields."""
        qty = abs(float(pos["_qty"]))
        pos["side"] = "BUY" if float(pos["_qty"]) > 0 else "SELL"
        pos["qty"] = _qty_out(qty)
        pos["entry_price"] = _r6(pos["_entry"])
        pos["avg_price"] = _r6(pos["_entry"])
        pos["commission"] = _r2(pos["_commission"])
        pos["margin"] = _r2(float(spec_for(pos["symbol"])["margin"]) * qty)
        if mark is not None:
            pos["mark_price"] = _r6(mark)
            upnl = _r2(self._pnl_usd(pos["symbol"], pos["side"], float(pos["_entry"]),
                                     float(mark), qty))
            pos["unrealized_pnl"] = upnl
            pos["unrealized_pnl_usd"] = upnl
            pos["pnl"] = upnl
        pos["updated_at"] = _utcnow()

    def _mark_position(self, symbol: str, price: float) -> None:
        pid = self._by_symbol.get(symbol)
        if pid and pid in self._positions:
            self._decorate_position(self._positions[pid], float(price))

    def _refresh_marks(self) -> None:
        """Re-price every open position (best effort; caller holds the lock)."""
        for pos in list(self._positions.values()):
            px = self._mark(pos["symbol"])
            if px is not None:
                self._decorate_position(pos, float(px))

    def _sync_position_brackets(self, symbol: str) -> None:
        """Mirror live protective orders onto ``pos.sl`` / ``pos.tp`` for the UI."""
        pid = self._by_symbol.get(symbol)
        if not pid or pid not in self._positions:
            return
        pos = self._positions[pid]
        sl = tp = trail_stop = trail_ticks = None
        for o in self._orders.values():
            if o["symbol"] != symbol or o["status"] != "WORKING" or not o.get("reduce_only"):
                continue
            if o["role"] == "TP" and o.get("limit_price") is not None:
                tp = float(o["limit_price"])
            elif o["role"] == "SL":
                if o["type"] == "TRAILING_STOP":
                    trail_stop = o.get("trail_stop")
                    trail_ticks = o.get("trail_ticks")
                    if trail_stop is not None:
                        sl = float(trail_stop)
                elif o.get("stop_price") is not None:
                    sl = float(o["stop_price"])
        pos["sl"] = _r6(sl) if sl is not None else None
        pos["tp"] = _r6(tp) if tp is not None else None
        pos["stop_loss"] = pos["sl"]
        pos["take_profit"] = pos["tp"]
        pos["trail_stop"] = _r6(trail_stop) if trail_stop is not None else None
        pos["trail_ticks"] = trail_ticks
        if pos.get("_initial_sl") is None and sl is not None:
            pos["_initial_sl"] = _r6(sl)
        pos["updated_at"] = _utcnow()

    # =====================
    # REPORTING
    # =====================

    def _config(self) -> dict:
        return {
            "enabled": bool(self._enabled),
            "starting_balance": _r2(self._starting_balance),
            "commission_per_contract": _r2(self._commission),
            "slippage_ticks": float(self._slippage_ticks),
            "has_price_fn": bool(callable(self.price_fn)),
        }

    def _margin_used(self) -> float:
        return sum(float(p.get("margin") or 0.0) for p in self._positions.values())

    def _sorted_positions(self) -> list[dict]:
        return sorted(self._positions.values(), key=lambda p: p.get("opened_at") or "", reverse=True)

    def _sorted_orders(self) -> list[dict]:
        # Working orders first (that's what a trader acts on), then newest.
        return sorted(
            self._orders.values(),
            key=lambda o: (0 if o["status"] == "WORKING" else 1, -int(o["seq"])),
        )

    def _performance(self) -> dict:
        trades = self._trades
        n = len(trades)
        nets = [float(t["net_pnl"]) for t in trades]
        wins = [x for x in nets if x > 0]
        losses = [x for x in nets if x < 0]
        gross_profit = _r2(sum(wins))
        gross_loss = _r2(abs(sum(losses)))
        win_rate = round(len(wins) / n, 4) if n else 0.0
        avg_win = _r2(sum(wins) / len(wins)) if wins else 0.0
        avg_loss = _r2(sum(losses) / len(losses)) if losses else 0.0
        expectancy = _r2(sum(nets) / n) if n else 0.0

        # Undefined (no losing trade yet) is None rather than a fake number.
        if gross_loss > 0:
            profit_factor = round(gross_profit / gross_loss, 4)
        elif gross_profit > 0:
            profit_factor = None
        else:
            profit_factor = 0.0

        # Max drawdown across the trade-by-trade equity curve (oldest -> newest).
        equity = self._starting_balance
        peak = equity
        max_dd = 0.0
        for t in reversed(trades):
            equity += float(t["net_pnl"])
            peak = max(peak, equity)
            max_dd = max(max_dd, peak - equity)
        max_dd_pct = round((max_dd / peak) * 100.0, 4) if peak > 0 else 0.0

        # "Sharpe-ish": mean/stdev of per-trade net P&L (unitless, not annualized).
        sharpe = 0.0
        if n > 1:
            mean = sum(nets) / n
            var = sum((x - mean) ** 2 for x in nets) / (n - 1)
            sd = math.sqrt(var)
            if sd > 1e-12:
                sharpe = round(mean / sd, 4)

        rs = [float(t["r_multiple"]) for t in trades if t.get("r_multiple") is not None]

        return {
            "total_trades": n,
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": win_rate,
            "net_pnl": _r2(sum(nets)),
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "profit_factor": profit_factor,
            "avg_win": avg_win,
            "avg_loss": avg_loss,
            "expectancy": expectancy,
            "largest_win": _r2(max(wins)) if wins else 0.0,
            "largest_loss": _r2(min(losses)) if losses else 0.0,
            "max_drawdown": _r2(max_dd),
            "max_drawdown_pct": max_dd_pct,
            "sharpe": sharpe,
            "avg_r": round(sum(rs) / len(rs), 3) if rs else None,
            "total_commissions": _r2(self._commissions),
        }


__all__ = ["PaperEngine", "CONTRACT_SPECS", "DEFAULT_SPEC", "spec_for", "contract_base"]
