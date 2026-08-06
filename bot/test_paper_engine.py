"""
Plain-python asserts for :mod:`bot.paper_engine`.

Deliberately dependency-free so it runs anywhere the server runs:

    python -m bot.test_paper_engine
"""

from __future__ import annotations

from .paper_engine import PaperEngine, spec_for, contract_base

_PASSED: list[str] = []


def check(name: str, fn) -> None:
    fn()
    _PASSED.append(name)
    print(f"  ok  {name}")


def approx(a, b, tol=1e-6) -> bool:
    return abs(float(a) - float(b)) <= tol


def engine(**kw) -> PaperEngine:
    """Engine with no price feed — tests push prices via ``on_tick``."""
    kw.setdefault("starting_balance", 100000.0)
    kw.setdefault("commission_per_contract", 0.0)
    kw.setdefault("slippage_ticks", 0.0)
    return PaperEngine(**kw)


# =====================
# SPECS / P&L MATHS
# =====================

def test_specs():
    assert contract_base("MGC=F") == "MGC"
    assert contract_base("es=f") == "ES"
    assert approx(spec_for("MGC=F")["tick_size"], 0.1)
    assert approx(spec_for("MGC=F")["tick_value"], 1.00)
    assert approx(spec_for("ES=F")["tick_size"], 0.25)
    assert approx(spec_for("ES=F")["tick_value"], 12.50)
    # unknown symbol -> fallback, never a KeyError
    assert spec_for("ZZZ=F")["tick_value"] == 1.00


def test_pnl_micro_mgc():
    """MGC: tick 0.1 == $1.00. A +$5.00 move on 2 lots = 50 ticks = $50."""
    e = engine()
    e.on_tick("MGC=F", 2400.0)
    e.place_order({"symbol": "MGC=F", "side": "BUY", "qty": 2, "type": "MARKET"})
    pos = e.state()["positions"][0]
    assert approx(pos["entry_price"], 2400.0)
    e.on_tick("MGC=F", 2405.0)
    pos = e.state()["positions"][0]
    assert approx(pos["pnl"], 100.0), pos["pnl"]              # 50 ticks * $1 * 2
    assert approx(pos["unrealized_pnl"], 100.0)
    e.close_position(pos["id"])
    st = e.state()
    assert approx(st["realized_pnl"], 100.0), st["realized_pnl"]
    assert approx(st["balance"], 100100.0)
    assert approx(st["trades"][0]["gross_pnl"], 100.0)


def test_pnl_mini_es():
    """ES: tick 0.25 == $12.50. A 4-point down move short 1 lot = +$200."""
    e = engine()
    e.on_tick("ES=F", 5000.0)
    e.place_order({"symbol": "ES=F", "side": "SELL", "qty": 1, "type": "MARKET"})
    e.on_tick("ES=F", 4996.0)
    pos = e.state()["positions"][0]
    assert approx(pos["pnl"], 200.0), pos["pnl"]              # 16 ticks * $12.50
    e.close_position(pos["id"])
    assert approx(e.state()["balance"], 100200.0)


# =====================
# ORDER TYPES / FILL MODEL
# =====================

def test_limit_fills_only_at_or_through():
    e = engine()
    e.on_tick("ES=F", 5000.0)
    r = e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "LIMIT",
                       "limit_price": 4990.0})
    assert r["ok"] and r["order"]["status"] == "WORKING"
    e.on_tick("ES=F", 4995.0)                                  # above limit: no fill
    assert e.state()["positions"] == []
    e.on_tick("ES=F", 4990.25)                                 # still above
    assert e.state()["positions"] == []
    e.on_tick("ES=F", 4990.0)                                  # touch: fills at limit
    pos = e.state()["positions"][0]
    assert approx(pos["entry_price"], 4990.0)
    assert e.state()["orders"][0]["status"] == "FILLED"


def test_limit_fills_better_on_gap():
    e = engine()
    e.on_tick("ES=F", 5000.0)
    e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "LIMIT",
                   "limit_price": 4990.0})
    e.on_tick("ES=F", 4980.0)                                  # gapped through
    pos = e.state()["positions"][0]
    assert approx(pos["entry_price"], 4980.0), pos["entry_price"]


def test_sell_limit():
    e = engine()
    e.on_tick("MGC=F", 2400.0)
    e.place_order({"symbol": "MGC=F", "side": "SELL", "qty": 1, "type": "LIMIT",
                   "limit_price": 2410.0})
    e.on_tick("MGC=F", 2405.0)
    assert e.state()["positions"] == []
    e.on_tick("MGC=F", 2412.0)
    assert approx(e.state()["positions"][0]["entry_price"], 2412.0)


def test_stop_triggers_then_fills_market():
    e = engine()
    e.on_tick("ES=F", 5000.0)
    e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "STOP",
                   "stop_price": 5010.0})
    e.on_tick("ES=F", 5005.0)
    assert e.state()["positions"] == []
    evs = e.on_tick("ES=F", 5012.0)                            # through the stop
    assert any(x["type"] == "TRIGGER" for x in evs)
    assert any(x["type"] == "FILL" for x in evs)
    assert approx(e.state()["positions"][0]["entry_price"], 5012.0)


def test_stop_limit_rests_after_trigger():
    e = engine()
    e.on_tick("ES=F", 5000.0)
    e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "STOP_LIMIT",
                   "stop_price": 5010.0, "limit_price": 5011.0})
    evs = e.on_tick("ES=F", 5020.0)                            # trigger, but > limit
    assert any(x["type"] == "TRIGGER" for x in evs)
    assert e.state()["positions"] == [], "stop-limit must not fill above its limit"
    e.on_tick("ES=F", 5011.0)                                  # back to the limit
    assert approx(e.state()["positions"][0]["entry_price"], 5011.0)


def test_commission_and_slippage_applied():
    """1 tick of slippage each way + $2.50/side on a flat MGC round-turn."""
    e = engine(commission_per_contract=2.50, slippage_ticks=1.0)
    e.on_tick("MGC=F", 2400.0)
    e.place_order({"symbol": "MGC=F", "side": "BUY", "qty": 1, "type": "MARKET"})
    pos = e.state()["positions"][0]
    assert approx(pos["entry_price"], 2400.1), pos["entry_price"]   # paid up 1 tick
    e.on_tick("MGC=F", 2400.0)
    res = e.close_position(pos["id"])
    t = res["trade"]
    assert approx(t["exit_price"], 2399.9), t["exit_price"]         # sold down 1 tick
    assert approx(t["gross_pnl"], -2.0), t["gross_pnl"]             # 2 ticks * $1
    assert approx(t["commission"], 5.0), t["commission"]            # both sides
    assert approx(t["net_pnl"], -7.0), t["net_pnl"]
    st = e.state()
    assert approx(st["commissions"], 5.0)
    assert approx(st["balance"], 100000.0 - 7.0), st["balance"]
    assert approx(st["realized_pnl"], -7.0)


# =====================
# BRACKETS / OCO / TRAILING
# =====================

def test_bracket_tp_exit():
    e = engine()
    e.on_tick("ES=F", 5000.0)
    r = e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "MARKET",
                       "bracket": {"tp_ticks": 20, "sl_ticks": 10}})
    assert r["ok"]
    st = e.state()
    pos = st["positions"][0]
    assert approx(pos["tp"], 5005.0), pos["tp"]                # 20 * 0.25
    assert approx(pos["sl"], 4997.5), pos["sl"]
    assert approx(pos["take_profit"], 5005.0)                  # legacy alias present
    working = [o for o in st["orders"] if o["status"] == "WORKING"]
    assert len(working) == 2 and working[0]["oco_group"] == working[1]["oco_group"]

    e.on_tick("ES=F", 5005.0)
    st = e.state()
    assert st["positions"] == []
    t = st["trades"][0]
    assert t["reason"] == "TP" and approx(t["gross_pnl"], 250.0), t
    assert approx(t["r_multiple"], 2.0), t["r_multiple"]        # 20t reward / 10t risk
    assert not [o for o in st["orders"] if o["status"] == "WORKING"]


def test_bracket_sl_exit_and_oco_cancel():
    e = engine()
    e.on_tick("ES=F", 5000.0)
    e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "MARKET",
                   "bracket": {"tp_price": 5010.0, "sl_price": 4995.0}})
    e.on_tick("ES=F", 4994.0)                                  # gap through the stop
    st = e.state()
    assert st["positions"] == []
    t = st["trades"][0]
    assert t["reason"] == "SL"
    assert approx(t["exit_price"], 4994.0)
    assert approx(t["gross_pnl"], -300.0), t["gross_pnl"]      # 24 ticks * 12.50
    tp = [o for o in st["orders"] if o["role"] == "TP"][0]
    assert tp["status"] == "CANCELED" and tp["cancel_reason"] == "OCO", tp


def test_oco_group_siblings():
    """Two independent entries in one OCO group: the first fill kills the other."""
    e = engine()
    e.on_tick("ES=F", 5000.0)
    a = e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "STOP",
                       "stop_price": 5010.0, "oco_group": "breakout"})["order"]
    b = e.place_order({"symbol": "ES=F", "side": "SELL", "qty": 1, "type": "STOP",
                       "stop_price": 4990.0, "oco_group": "breakout"})["order"]
    e.on_tick("ES=F", 5011.0)
    orders = {o["id"]: o for o in e.state()["orders"]}
    assert orders[a["id"]]["status"] == "FILLED"
    assert orders[b["id"]]["status"] == "CANCELED"
    assert orders[b["id"]]["cancel_reason"] == "OCO"


def test_trailing_stop_ratchets_never_loosens():
    e = engine()
    e.on_tick("MGC=F", 2400.0)
    e.place_order({"symbol": "MGC=F", "side": "BUY", "qty": 1, "type": "MARKET",
                   "bracket": {"trail_ticks": 20}})            # 20 ticks == $2.00
    trail = [o for o in e.state()["orders"] if o["role"] == "SL"][0]
    assert approx(trail["trail_stop"], 2398.0), trail["trail_stop"]

    e.on_tick("MGC=F", 2405.0)
    trail = [o for o in e.state()["orders"] if o["role"] == "SL"][0]
    assert approx(trail["trail_stop"], 2403.0)
    assert approx(e.state()["positions"][0]["trail_stop"], 2403.0)

    e.on_tick("MGC=F", 2404.0)                                 # pullback: must NOT loosen
    trail = [o for o in e.state()["orders"] if o["role"] == "SL"][0]
    assert approx(trail["trail_stop"], 2403.0), trail["trail_stop"]

    e.on_tick("MGC=F", 2408.0)                                 # new high: ratchets up
    trail = [o for o in e.state()["orders"] if o["role"] == "SL"][0]
    assert approx(trail["trail_stop"], 2406.0)

    e.on_tick("MGC=F", 2405.9)                                 # takes out the trail
    st = e.state()
    assert st["positions"] == []
    t = st["trades"][0]
    assert t["reason"] == "TRAILING", t["reason"]
    assert approx(t["gross_pnl"], 59.0), t["gross_pnl"]        # 2405.9-2400 = 59 ticks


def test_trailing_stop_short_side():
    e = engine()
    e.on_tick("MGC=F", 2400.0)
    e.place_order({"symbol": "MGC=F", "side": "SELL", "qty": 1, "type": "TRAILING_STOP",
                   "trail_ticks": 10})                          # standalone buy-stop trail
    o = e.state()["orders"][0]
    assert o["side"] == "SELL" and approx(o["trail_stop"], 2399.0)
    e.on_tick("MGC=F", 2410.0)
    o = e.state()["orders"][0]
    assert approx(o["trail_stop"], 2409.0)


# =====================
# NETTING
# =====================

def test_average_down_entry_math():
    e = engine()
    e.on_tick("ES=F", 5000.0)
    e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 2, "type": "MARKET"})
    e.on_tick("ES=F", 4990.0)
    e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 2, "type": "MARKET"})
    pos = e.state()["positions"][0]
    assert pos["qty"] == 4
    assert approx(pos["entry_price"], 4995.0), pos["entry_price"]
    assert approx(pos["avg_price"], 4995.0)
    # 4 lots, -5.00 from avg = -20 ticks * 12.50 * 4
    assert approx(pos["pnl"], -1000.0), pos["pnl"]


def test_partial_reduce():
    e = engine()
    e.on_tick("MGC=F", 2400.0)
    e.place_order({"symbol": "MGC=F", "side": "BUY", "qty": 3, "type": "MARKET"})
    e.on_tick("MGC=F", 2410.0)
    e.place_order({"symbol": "MGC=F", "side": "SELL", "qty": 1, "type": "MARKET"})
    st = e.state()
    pos = st["positions"][0]
    assert pos["qty"] == 2 and pos["side"] == "BUY"
    assert approx(pos["entry_price"], 2400.0)                  # entry unchanged
    assert approx(st["trades"][0]["gross_pnl"], 100.0)         # 100 ticks * $1 * 1


def test_position_flip():
    e = engine()
    e.on_tick("MGC=F", 2400.0)
    e.place_order({"symbol": "MGC=F", "side": "BUY", "qty": 2, "type": "MARKET"})
    e.on_tick("MGC=F", 2405.0)
    e.place_order({"symbol": "MGC=F", "side": "SELL", "qty": 5, "type": "MARKET"})
    st = e.state()
    assert len(st["positions"]) == 1
    pos = st["positions"][0]
    assert pos["side"] == "SELL" and pos["qty"] == 3, pos
    assert approx(pos["entry_price"], 2405.0)
    assert len(st["trades"]) == 1
    assert approx(st["trades"][0]["gross_pnl"], 100.0)         # 50 ticks * $1 * 2
    assert approx(st["realized_pnl"], 100.0)


def test_reduce_only_never_flips():
    e = engine()
    e.on_tick("ES=F", 5000.0)
    e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "MARKET"})
    e.place_order({"symbol": "ES=F", "side": "SELL", "qty": 5, "type": "MARKET",
                   "reduce_only": True})
    st = e.state()
    assert st["positions"] == []
    assert st["trades"][0]["qty"] == 1


# =====================
# WORKING-ORDER MANAGEMENT
# =====================

def test_cancel_modify_and_cancel_all():
    e = engine()
    e.on_tick("ES=F", 5000.0)
    o = e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "LIMIT",
                       "limit_price": 4900.0})["order"]
    m = e.modify_order(o["id"], {"limit_price": 4950.0, "qty": 3})
    assert m["ok"] and approx(m["order"]["limit_price"], 4950.0) and m["order"]["qty"] == 3.0
    e.on_tick("ES=F", 4960.0)
    assert e.state()["positions"] == []
    c = e.cancel_order(o["id"])
    assert c["ok"] and c["order"]["status"] == "CANCELED"
    assert not e.cancel_order(o["id"])["ok"]

    e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "LIMIT",
                   "limit_price": 4900.0})
    e.place_order({"symbol": "MGC=F", "side": "BUY", "qty": 1, "type": "LIMIT",
                   "limit_price": 2000.0})
    assert e.cancel_all("ES=F")["canceled"] == 1
    assert len([x for x in e.state()["orders"] if x["status"] == "WORKING"]) == 1
    assert e.cancel_all()["canceled"] == 1


def test_flatten_all():
    e = engine()
    e.on_tick("ES=F", 5000.0)
    e.on_tick("MGC=F", 2400.0)
    e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "MARKET"})
    e.place_order({"symbol": "MGC=F", "side": "SELL", "qty": 1, "type": "MARKET"})
    e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "LIMIT",
                   "limit_price": 4000.0})
    res = e.flatten_all()
    assert res["closed"] == 2, res
    st = e.state()
    assert st["positions"] == []
    assert not [o for o in st["orders"] if o["status"] == "WORKING"]


def test_rejections():
    e = engine()
    assert e.place_order({"symbol": "", "side": "BUY", "qty": 1})["error"] == "missing_symbol"
    assert e.place_order({"symbol": "ES=F", "side": "X", "qty": 1})["error"] == "bad_side"
    assert e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 0})["error"] == "bad_qty"
    assert e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1,
                          "type": "LIMIT"})["error"] == "limit_price_required"
    assert e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1,
                          "type": "STOP"})["error"] == "stop_price_required"
    assert e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1,
                          "type": "FOO"})["error"] == "bad_order_type"
    assert not e.cancel_order("nope")["ok"]
    assert not e.modify_order("nope", {})["ok"]
    assert not e.close_position("nope")["ok"]


# =====================
# ACCOUNT / DAY ROLLOVER / RESET
# =====================

def test_day_rollover():
    e = engine()
    e.on_tick("MGC=F", 2400.0)
    e.place_order({"symbol": "MGC=F", "side": "BUY", "qty": 1, "type": "MARKET"})
    e.on_tick("MGC=F", 2450.0)
    e.close_position(e.state()["positions"][0]["id"])
    st = e.state()
    assert approx(st["day_pnl"], 500.0), st["day_pnl"]         # 500 ticks * $1

    # A DAY order must not survive the boundary.
    d = e.place_order({"symbol": "MGC=F", "side": "BUY", "qty": 1, "type": "LIMIT",
                       "limit_price": 2000.0, "tif": "DAY"})["order"]
    e._day_id = 19700101                                       # force the rollover
    evs = e.on_tick("MGC=F", 2450.0)
    assert any(x["type"] == "ROLLOVER" for x in evs), evs
    st = e.state()
    assert approx(st["day_start_balance"], 100500.0), st["day_start_balance"]
    assert approx(st["day_pnl"], 0.0), st["day_pnl"]
    assert approx(st["realized_pnl"], 500.0)                   # lifetime is untouched
    orders = {o["id"]: o for o in st["orders"]}
    assert orders[d["id"]]["status"] == "CANCELED"
    assert orders[d["id"]]["cancel_reason"] == "DAY_EXPIRED"


def test_reset():
    e = engine(commission_per_contract=2.50)
    e.on_tick("ES=F", 5000.0)
    e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "MARKET"})
    e.on_tick("ES=F", 5010.0)
    e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "LIMIT",
                   "limit_price": 4000.0})
    r = e.reset(25000.0)
    assert r["ok"]
    st = r["state"]
    assert approx(st["balance"], 25000.0) and approx(st["starting_balance"], 25000.0)
    assert st["positions"] == [] and st["orders"] == [] and st["trades"] == []
    assert approx(st["realized_pnl"], 0.0) and approx(st["commissions"], 0.0)
    assert approx(st["equity"], 25000.0) and approx(st["day_pnl"], 0.0)


def test_configure():
    e = engine()
    c = e.configure(commission_per_contract=4.0, slippage_ticks=2.0, enabled=False)
    assert c["ok"]
    assert approx(c["config"]["commission_per_contract"], 4.0)
    assert approx(c["config"]["slippage_ticks"], 2.0)
    assert c["config"]["enabled"] is False
    assert not e.configure(commission_per_contract=-1)["ok"]
    assert e.tick_all() == []                                  # disabled => no work
    assert e.configure(starting_balance=50000.0)["ok"]
    assert approx(e.state()["balance"], 50000.0)


def test_equity_margin_and_state_shape():
    e = engine()
    e.on_tick("MGC=F", 2400.0)
    e.place_order({"symbol": "MGC=F", "side": "BUY", "qty": 2, "type": "MARKET"})
    e.on_tick("MGC=F", 2410.0)
    st = e.state()
    for k in ("enabled", "starting_balance", "balance", "equity", "open_pnl", "realized_pnl",
              "day_pnl", "day_start_balance", "commissions", "margin_used", "positions",
              "orders", "trades", "performance", "log", "config"):
        assert k in st, f"state() missing {k}"
    assert approx(st["open_pnl"], 200.0)
    assert approx(st["equity"], 100200.0)
    assert approx(st["margin_used"], 2400.0)                   # MGC 1200 * 2
    pos = st["positions"][0]
    for k in ("id", "symbol", "side", "qty", "entry_price", "sl", "tp", "pnl"):
        assert k in pos, f"position missing backward-compatible key {k}"
    assert not [k for k in pos if k.startswith("_")], "private fields leaked into state()"


def test_tick_all_uses_price_fn():
    feed = {"MGC=F": 2400.0}
    e = PaperEngine(starting_balance=100000.0, commission_per_contract=0.0,
                    slippage_ticks=0.0, price_fn=lambda s: feed.get(s))
    e.place_order({"symbol": "MGC=F", "side": "BUY", "qty": 1, "type": "MARKET",
                   "bracket": {"tp_price": 2410.0}})
    assert approx(e.state()["positions"][0]["entry_price"], 2400.0)
    feed["MGC=F"] = 2410.0
    evs = e.tick_all()
    assert any(x["type"] == "CLOSE" for x in evs), evs
    assert e.state()["positions"] == []
    assert approx(e.state()["trades"][0]["gross_pnl"], 100.0)


def test_price_fn_exception_is_contained():
    def boom(_sym):
        raise RuntimeError("feed down")
    e = PaperEngine(price_fn=boom)
    r = e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1, "type": "MARKET"})
    assert r["ok"] and r["order"]["status"] == "WORKING"        # rests until a price shows
    assert e.tick_all() == []
    e.on_tick("ES=F", 5000.0)
    assert e.state()["positions"][0]["qty"] == 1


# =====================
# BLOTTER / PERFORMANCE
# =====================

def test_trade_blotter_fields():
    e = engine(commission_per_contract=2.50)
    e.on_tick("MGC=F", 2400.0)
    e.place_order({"symbol": "MGC=F", "side": "BUY", "qty": 1, "type": "MARKET",
                   "bracket": {"sl_ticks": 10, "tp_ticks": 30}, "tag": "ORB"})
    e.on_tick("MGC=F", 2403.0)
    t = e.state()["trades"][0]
    for k in ("id", "symbol", "side", "qty", "entry_price", "exit_price", "gross_pnl",
              "commission", "net_pnl", "r_multiple", "reason", "opened_at", "closed_at",
              "duration_sec", "tag"):
        assert k in t, f"trade missing {k}"
    assert t["reason"] == "TP" and t["tag"] == "ORB"
    assert approx(t["gross_pnl"], 30.0) and approx(t["commission"], 5.0)
    assert approx(t["net_pnl"], 25.0)
    assert approx(t["r_multiple"], 3.0)
    assert approx(t["pnl"], t["net_pnl"]) and approx(t["pnl_usd"], t["net_pnl"])
    assert t["duration_sec"] >= 0.0


def test_performance_summary():
    e = engine()
    # MGC tick 0.1 == $1, so 100 points == 1000 ticks == $1000.
    # Sequence: +$1000, -$500, +$2000.
    for entry, exit_px in ((2400.0, 2500.0), (2400.0, 2350.0), (2400.0, 2600.0)):
        e.on_tick("MGC=F", entry)
        e.place_order({"symbol": "MGC=F", "side": "BUY", "qty": 1, "type": "MARKET"})
        e.on_tick("MGC=F", exit_px)
        e.close_position(e.state()["positions"][0]["id"])
    p = e.performance()
    assert p["total_trades"] == 3
    assert p["wins"] == 2 and p["losses"] == 1
    assert approx(p["win_rate"], 2 / 3, 1e-4)
    assert approx(p["net_pnl"], 2500.0)
    assert approx(p["gross_profit"], 3000.0) and approx(p["gross_loss"], 500.0)
    assert approx(p["profit_factor"], 6.0)
    assert approx(p["avg_win"], 1500.0) and approx(p["avg_loss"], -500.0)
    assert approx(p["expectancy"], round(2500.0 / 3, 2), 0.01)
    assert approx(p["largest_win"], 2000.0) and approx(p["largest_loss"], -500.0)
    assert approx(p["max_drawdown"], 500.0)                    # 101000 -> 100500 dip
    assert p["sharpe"] != 0.0
    assert approx(p["total_commissions"], 0.0)


def test_performance_empty():
    p = engine().performance()
    assert p["total_trades"] == 0 and approx(p["net_pnl"], 0.0)
    assert approx(p["max_drawdown"], 0.0) and approx(p["profit_factor"], 0.0)


def test_legacy_flat_bracket_payload():
    """The old /api/paper/order body (stop_loss/take_profit) still works."""
    e = engine()
    e.on_tick("ES=F", 5000.0)
    r = e.place_order({"symbol": "ES=F", "side": "BUY", "qty": 1,
                       "stop_loss": 4990.0, "take_profit": 5020.0})
    assert r["ok"] and "position" in r
    pos = r["position"]
    assert approx(pos["sl"], 4990.0) and approx(pos["tp"], 5020.0)
    assert approx(pos["stop_loss"], 4990.0) and approx(pos["take_profit"], 5020.0)


def test_thread_safety_smoke():
    import threading as _t
    e = engine()
    e.on_tick("MGC=F", 2400.0)
    errs: list = []

    def worker(i):
        try:
            for n in range(40):
                side = "BUY" if (i + n) % 2 == 0 else "SELL"
                e.place_order({"symbol": "MGC=F", "side": side, "qty": 1, "type": "MARKET"})
                e.on_tick("MGC=F", 2400.0 + (n % 7))
                e.state()
        except Exception as ex:      # noqa: BLE001 - surfaced via errs
            errs.append(ex)

    threads = [_t.Thread(target=worker, args=(i,)) for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errs, errs[:3]
    e.flatten_all()
    st = e.state()
    assert st["positions"] == []
    assert approx(st["balance"], 100000.0 + st["realized_pnl"], 0.01)


def main() -> int:
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("test_") and callable(f)]
    print(f"bot.paper_engine — running {len(tests)} tests")
    for name, fn in tests:
        check(name, fn)
    print(f"\nAll {len(_PASSED)} tests passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
