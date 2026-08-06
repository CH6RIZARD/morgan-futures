"""
Tests for bot/copy_router.py — the multi-account prop-firm copy router.

Pure stdlib, no network, no pytest dependency:

    python -m bot.test_copy_router

Every test builds its own CopyRouter against a throwaway JSON store and pins
the clock so blackout windows and ET session rolls are deterministic.
"""
from __future__ import annotations

import os
import shutil
import tempfile
import traceback
from datetime import datetime

from .copy_router import ET, FIRM_RULES, CopyRouter

# Tuesday 2026-03-10, 10:15 ET — inside RTH, outside every default blackout.
FIXED_NOW = datetime(2026, 3, 10, 10, 15, tzinfo=ET)
SESSION_DAY = "2026-03-10"

_TMP: list[str] = []


# ── Fakes ──────────────────────────────────────────────────────────────────


class FakePaper:
    """Stands in for the paper engine: place_order(payload) -> dict."""

    def __init__(self, fail_accounts=None, fail_times=99) -> None:
        self.calls: list[dict] = []
        self.fail_accounts = set(fail_accounts or [])
        self.fail_times = fail_times
        self._failed: dict[str, int] = {}
        self.n = 0

    def is_connected(self) -> bool:
        return True

    def place_order(self, payload: dict) -> dict:
        aid = payload.get("account_id")
        if aid in self.fail_accounts and self._failed.get(aid, 0) < self.fail_times:
            self._failed[aid] = self._failed.get(aid, 0) + 1
            raise RuntimeError("paper engine exploded")
        self.calls.append(dict(payload))
        self.n += 1
        return {"order_id": f"paper-{self.n}"}


class FakePaperEngineShape:
    """bot/paper_engine.py's real shape: {ok, order: {id, ...}, position?}."""

    def __init__(self, ok=True) -> None:
        self.calls: list[dict] = []
        self.ok = ok

    def place_order(self, payload: dict) -> dict:
        self.calls.append(dict(payload))
        if not self.ok:
            return {"ok": False, "error": "unknown symbol"}
        return {"ok": True, "order": {"id": f"ord-{len(self.calls)}", "status": "FILLED"}}


class FakeExecutor:
    """Stands in for RithmicExecutor.place_bracket_order(...) -> basket_id."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def is_connected(self) -> bool:
        return True

    def place_bracket_order(self, yahoo_sym, side, qty, entry, sl, tp):
        self.calls.append({"symbol": yahoo_sym, "side": side, "qty": qty, "entry": entry, "sl": sl, "tp": tp})
        return f"basket-{len(self.calls)}"


# ── Fixtures ───────────────────────────────────────────────────────────────


def new_router(paper=None, executor=None) -> CopyRouter:
    d = tempfile.mkdtemp(prefix="copyrouter-")
    _TMP.append(d)
    r = CopyRouter(
        store_path=os.path.join(d, "copy_accounts.json"),
        executor=executor if executor is not None else FakeExecutor(),
        paper_engine=paper if paper is not None else FakePaper(),
    )
    r._clock = lambda: FIXED_NOW
    r.set_enabled(True)   # master switch defaults OFF — fan-out is opt-in
    # Leader: the account whose signals get copied. Never receives fan-out.
    r.add_account({
        "id": "leader",
        "label": "Leader (own account)",
        "firm_id": "lucid",
        "mode": "paper",
        "is_leader": True,
        "session_day": SESSION_DAY,
    })
    return r


def healthy(account_id: str, firm_id: str = "apex", **kw) -> dict:
    """A follower account with clean books at the pinned session day."""
    payload = {
        "id": account_id,
        "label": account_id.upper(),
        "firm_id": firm_id,
        "mode": "paper",
        "session_day": SESSION_DAY,
        "starting_balance": 50000.0,
        "balance": 50000.0,
        "day_start_balance": 50000.0,
        "peak_balance": 50000.0,
        "peak_equity": 50000.0,
    }
    payload.update(kw)
    return payload


def signal(**kw) -> dict:
    """MES risk is $25/contract at these prices — small enough not to resize."""
    sig = {
        "id": "sig-1",
        "symbol": "MES=F",
        "side": "BUY",
        "qty": 1,
        "entry": 5000.0,
        "sl": 4995.0,
        "tp": 5015.0,
        "strategy": "ORB",
        "ts": 1772000000,
    }
    sig.update(kw)
    return sig


def verdict_for(verdicts: list, account_id: str) -> dict:
    return next(v for v in verdicts if v["account_id"] == account_id)


def rules(verdict: dict) -> set:
    return {r["rule"] for r in verdict["reasons"]}


# ── Tests ──────────────────────────────────────────────────────────────────


def test_resize_on_max_qty():
    r = new_router()
    r.add_account(healthy("a1", "apex", max_qty=2))
    v = verdict_for(r.evaluate(signal(qty=5)), "a1")
    assert v["decision"] == "RESIZE", v
    assert v["original_qty"] == 5, v
    assert v["qty"] == 2, v
    assert "max_qty" in rules(v), v
    assert v["buffer"]["max_qty"] == 2, v


def test_multiplier_scales_then_caps():
    r = new_router()
    r.add_account(healthy("a1", "apex", multiplier=3.0, max_qty=10))
    v = verdict_for(r.evaluate(signal(qty=2)), "a1")
    assert v["original_qty"] == 6 and v["qty"] == 6 and v["decision"] == "ALLOW", v


def test_block_on_exhausted_trailing_dd():
    r = new_router()
    # Apex = EOD trailing: floor = peak_balance(50000) - 2500 = 47500.
    r.add_account(healthy("a1", "apex", balance=47500.0, trailing_dd=2500.0))
    v = verdict_for(r.evaluate(signal()), "a1")
    assert v["decision"] == "BLOCK", v
    assert "trailing_dd" in rules(v), v
    assert v["buffer"]["trailing_dd_remaining"] <= 0, v


def test_intraday_trailing_floor_ratchets_above_eod():
    """Same books, different firm: the intraday floor is higher (tighter)."""
    r = new_router()
    r.add_account(healthy("eod", "apex", balance=51000.0, peak_balance=50000.0,
                          peak_equity=52000.0, trailing_dd=2500.0))
    r.add_account(healthy("intra", "mff", balance=51000.0, peak_balance=50000.0,
                          peak_equity=52000.0, trailing_dd=2000.0))
    vs = r.evaluate(signal())
    eod = verdict_for(vs, "eod")["buffer"]["trailing_dd_remaining"]
    intra = verdict_for(vs, "intra")["buffer"]["trailing_dd_remaining"]
    # EOD:   51000 - (50000 - 2500) = 3500
    # Intra: 51000 - (52000 - 2000) = 1000  (floor ratcheted on unrealized peak)
    assert eod == 3500.0, eod
    assert intra == 1000.0, intra
    assert intra < eod


def test_block_on_daily_loss_limit():
    r = new_router()
    # Topstep 50K: $1,000 daily loss limit, already down exactly $1,000.
    r.add_account(healthy("a1", "topstep", plan="50K", balance=49000.0,
                          day_start_balance=50000.0, trailing_dd=2000.0))
    v = verdict_for(r.evaluate(signal()), "a1")
    assert v["decision"] == "BLOCK", v
    assert "daily_loss" in rules(v), v
    assert v["buffer"]["daily_loss_remaining"] == 0.0, v


def test_daily_loss_measured_on_floating_equity():
    """An open loser counts against the daily limit before it is realized."""
    r = new_router()
    r.add_account(healthy(
        "a1", "topstep", plan="50K", trailing_dd=2000.0,
        positions=[{"symbol": "MES=F", "side": "BUY", "qty": 1, "unrealized_pnl_usd": -1000.0}],
    ))
    v = verdict_for(r.evaluate(signal()), "a1")
    assert v["decision"] == "BLOCK" and "daily_loss" in rules(v), v


def test_hedge_block_treats_nq_and_mnq_as_one_underlying():
    r = new_router()
    # Apex forbids hedging. This account is short MNQ; a long NQ signal is a hedge.
    r.add_account(healthy(
        "a1", "apex",
        positions=[{"symbol": "MNQ=F", "side": "SELL", "qty": 1, "unrealized_pnl_usd": 0.0}],
    ))
    r.add_account(healthy("a2", "tradeday"))  # hedging allowed -> not blocked
    vs = r.evaluate(signal(symbol="NQ=F", side="BUY"))
    a1 = verdict_for(vs, "a1")
    a2 = verdict_for(vs, "a2")
    assert a1["decision"] == "BLOCK" and "no_hedge" in rules(a1), a1
    assert a2["decision"] != "BLOCK" or "no_hedge" not in rules(a2), a2


def test_consistency_warning_and_optional_throttle():
    r = new_router()
    # $1,000 total profit, $900 of it made in one day = 90% > Apex's 50%.
    r.add_account(healthy("a1", "apex", balance=51000.0, peak_balance=51000.0,
                          daily_pnl={"2026-03-09": 900.0}, max_qty=10))
    v = verdict_for(r.evaluate(signal(qty=4)), "a1")
    assert v["decision"] in ("ALLOW", "RESIZE"), v   # warn only by default
    assert "consistency" in rules(v), v
    assert v["qty"] == 4, v

    r.set_config({"throttle_on_consistency": True, "consistency_throttle_qty": 1})
    v2 = verdict_for(r.evaluate(signal(qty=4)), "a1")
    assert v2["decision"] == "RESIZE" and v2["qty"] == 1, v2


def test_max_accounts_per_household():
    r = new_router()
    limit = FIRM_RULES["topstep"]["max_accounts"]      # 5
    for i in range(limit + 2):
        r.add_account(healthy(f"ts{i}", "topstep", plan="50K", trailing_dd=2000.0,
                              created_at=f"2026-01-{i + 1:02d}T00:00:00+00:00"))
    vs = r.evaluate(signal())
    ok = [v for v in vs if "max_accounts" not in rules(v)]
    blocked = [v for v in vs if "max_accounts" in rules(v)]
    assert len(ok) == limit, [v["account_id"] for v in ok]
    assert len(blocked) == 2, [v["account_id"] for v in blocked]
    assert all(v["decision"] == "BLOCK" for v in blocked)


def test_blackout_window_blocks():
    r = new_router()
    r.add_account(healthy("a1", "tpt", plan="50K", trailing_dd=2000.0))
    r._clock = lambda: datetime(2026, 3, 10, 16, 59, tzinfo=ET)   # TPT 5pm close
    v = verdict_for(r.evaluate(signal()), "a1")
    assert v["decision"] == "BLOCK" and "blackout" in rules(v), v


def test_mini_to_micro_symbol_mapping():
    r = new_router()
    paper = FakePaper()
    r._paper = paper
    r.add_account(healthy("small", "apex", symbol_map={"ES": "MES", "NQ": "MNQ"}, max_qty=10))
    r.add_account(healthy("big", "tradeday", max_qty=10))
    vs = r.evaluate(signal(symbol="ES=F", qty=2))
    small = verdict_for(vs, "small")
    big = verdict_for(vs, "big")
    assert small["symbol"] == "MES=F", small
    assert "symbol_map" in rules(small), small
    assert big["symbol"] == "ES=F", big

    r.route_signal(signal(symbol="ES=F", qty=2))
    routed = {c["account_id"]: c["symbol"] for c in paper.calls}
    assert routed == {"small": "MES=F", "big": "ES=F"}, routed


def test_real_paper_engine_result_shape():
    """Accepts bot/paper_engine.py's {ok, order:{id}} and its rejections."""
    eng = FakePaperEngineShape()
    r = new_router(paper=eng)
    r.add_account(healthy("a1", "apex", max_qty=5))
    res = r.route_signal(signal(id="pe-1"))
    assert res["results"][0]["routed"] is True, res
    assert res["results"][0]["order_id"] == "ord-1", res

    r2 = new_router(paper=FakePaperEngineShape(ok=False))
    r2.add_account(healthy("a1", "apex", max_qty=5))
    bad = r2.route_signal(signal(id="pe-2"))["results"][0]
    assert bad["routed"] is False and "unknown symbol" in str(bad["error"]), bad


def test_idempotent_double_route():
    paper = FakePaper()
    r = new_router(paper=paper)
    r.add_account(healthy("a1", "apex", max_qty=5))
    first = r.route_signal(signal(id="dupe-1"))
    second = r.route_signal(signal(id="dupe-1"))
    assert first["results"][0]["routed"] is True, first
    assert second["results"][0]["routed"] is False, second
    assert second["results"][0]["duplicate"] is True, second
    assert second["results"][0]["error"] == "duplicate_suppressed", second
    assert len(paper.calls) == 1, paper.calls
    # A genuinely new signal id still routes.
    third = r.route_signal(signal(id="dupe-2"))
    assert third["results"][0]["routed"] is True and len(paper.calls) == 2


def test_one_failing_account_does_not_cascade():
    paper = FakePaper(fail_accounts=["bad"])
    r = new_router(paper=paper)
    r.add_account(healthy("bad", "apex", max_qty=5))
    r.add_account(healthy("good1", "topstep", plan="50K", trailing_dd=2000.0, max_qty=5))
    r.add_account(healthy("good2", "tradeday", plan="50K", trailing_dd=2000.0, max_qty=5))

    res = r.route_signal(signal(id="s-1"))
    by_id = {x["account_id"]: x for x in res["results"]}
    assert by_id["bad"]["routed"] is False and by_id["bad"]["error"], by_id["bad"]
    assert by_id["good1"]["routed"] is True and by_id["good2"]["routed"] is True, by_id
    assert res["ok"] is True

    # Quarantine kicks in after max_consecutive_failures and stops re-trying.
    limit = r.state()["config"]["max_consecutive_failures"]
    for i in range(2, limit + 2):
        r.route_signal(signal(id=f"s-{i}"))
    bad = next(a for a in r.list_accounts() if a["id"] == "bad")
    assert bad["quarantined"] is True, bad
    last = r.route_signal(signal(id="s-final"))
    bad_v = next(x for x in last["results"] if x["account_id"] == "bad")
    assert bad_v["decision"] == "BLOCK" and "quarantine" in rules(bad_v), bad_v
    others = [x for x in last["results"] if x["account_id"] != "bad"]
    assert all(x["routed"] for x in others), others


def test_master_switch_defaults_off_and_blocks_fanout():
    paper = FakePaper()
    r = new_router(paper=paper)
    r.add_account(healthy("a1", "apex", max_qty=5))
    r.set_enabled(False)

    res = r.route_signal(signal(id="off-1"))
    v = res["results"][0]
    assert res["ok"] is False, res
    assert v["decision"] == "BLOCK" and "disabled" in rules(v), v
    assert paper.calls == [], paper.calls

    # A blocked delivery must not burn the idempotency key: the same signal
    # redelivered after the user switches copying on still routes.
    r.set_enabled(True)
    again = r.route_signal(signal(id="off-1"))["results"][0]
    assert again["routed"] is True and again.get("duplicate") is False, again
    assert len(paper.calls) == 1, paper.calls


def test_evaluate_still_previews_while_disabled():
    r = new_router()
    r.add_account(healthy("a1", "apex", max_qty=2))
    r.set_enabled(False)
    vs = r.evaluate(signal(qty=5))
    v = verdict_for(vs, "a1")
    assert len(vs) == 1, vs
    # Preview still shows the real sizing decision, not a blanket block.
    assert v["decision"] == "RESIZE" and v["qty"] == 2, v
    assert "disabled" in rules(v), v
    assert next(x for x in v["reasons"] if x["rule"] == "disabled")["severity"] == "info", v


def test_enabled_and_kill_switch_are_distinct():
    r = new_router()
    r.add_account(healthy("a1", "apex", max_qty=5))

    r.set_enabled(True)
    r.kill_switch(True)
    v = r.route_signal(signal(id="d-1"))["results"][0]
    assert "kill_switch" in rules(v) and v["decision"] == "BLOCK", v
    st = r.state()
    assert st["enabled"] is True and st["kill_switch"] is True, st

    r.kill_switch(False)
    r.set_enabled(False)
    v2 = r.route_signal(signal(id="d-2"))["results"][0]
    assert "disabled" in rules(v2) and "kill_switch" not in rules(v2), v2
    st2 = r.state()
    assert st2["enabled"] is False and st2["kill_switch"] is False, st2


def test_state_surfaces_switches_at_top_level():
    r = new_router()
    st = r.state()
    assert set(st.keys()) == {"enabled", "kill_switch", "leader_id", "accounts",
                              "recent_fanouts", "firms", "config"}, st.keys()
    # Mirrored, not moved — config still carries them.
    assert st["enabled"] == st["config"]["enabled"] is True, st
    assert st["kill_switch"] == st["config"]["kill_switch"] is False, st
    r.kill_switch(True)
    r.set_enabled(False)
    st2 = r.state()
    assert st2["kill_switch"] is True and st2["config"]["kill_switch"] is True, st2
    assert st2["enabled"] is False and st2["config"]["enabled"] is False, st2


def test_set_config_reports_unknown_keys():
    r = new_router()
    ok = r.set_config({"enabled": False, "session_reset_hour_et": 18})
    assert ok["ok"] is True and ok["unknown_keys"] == [], ok
    assert ok["applied"] == {"enabled": False, "session_reset_hour_et": 18}, ok
    assert r.state()["enabled"] is False and r.state()["config"]["session_reset_hour_et"] == 18

    bad = r.set_config({"enabld": True, "nonsense": 1, "kill_switch": True})
    assert bad["ok"] is False, bad
    assert bad["unknown_keys"] == ["enabld", "nonsense"], bad
    assert bad["applied"] == {"kill_switch": True}, bad
    assert "enabld" not in bad["config"], bad
    assert r.state()["kill_switch"] is True


def test_kill_switch_halts_everything():
    paper = FakePaper()
    r = new_router(paper=paper)
    r.add_account(healthy("a1", "apex", max_qty=5))
    r.add_account(healthy("a2", "tradeday", plan="50K", trailing_dd=2000.0, max_qty=5))
    assert r.kill_switch(True)["kill_switch"] is True

    res = r.route_signal(signal(id="ks-1"))
    assert res["ok"] is False, res
    assert all(x["decision"] == "BLOCK" for x in res["results"]), res
    assert all("kill_switch" in rules(x) for x in res["results"]), res
    assert paper.calls == [], paper.calls

    r.kill_switch(False)
    res2 = r.route_signal(signal(id="ks-2"))
    assert res2["ok"] is True and len(paper.calls) == 2, res2


def test_missing_target_never_crashes():
    r = new_router()
    r._paper = None                                  # paper engine not wired yet
    r.add_account(healthy("a1", "apex", mode="paper", max_qty=5))
    res = r.route_signal(signal(id="nt-1"))
    v = res["results"][0]
    assert v["decision"] == "BLOCK" and "connection" in rules(v), v
    assert v["routed"] is False and res["ok"] is False


def test_live_mode_routes_through_executor():
    ex = FakeExecutor()
    r = new_router(executor=ex)
    r.add_account(healthy("live1", "apex", mode="live", max_qty=5))
    res = r.route_signal(signal(id="live-1", qty=2))
    assert res["results"][0]["routed"] is True, res
    assert ex.calls[0]["qty"] == 2 and ex.calls[0]["symbol"] == "MES=F", ex.calls
    assert res["results"][0]["order_id"] == "basket-1", res


def test_server_signal_shape_is_accepted():
    """/api/signal/test emits type/direction/stop_loss/take_profit — accept it."""
    paper = FakePaper()
    r = new_router(paper=paper)
    r.add_account(healthy("a1", "apex", max_qty=5))
    raw = {"symbol": "MES=F", "type": "ORB", "direction": "SELL", "entry": 5000.0,
           "stop_loss": 5005.0, "take_profit": 4985.0, "time": 1772000000, "test_signal": True}
    res = r.route_signal(raw)
    assert res["results"][0]["side"] == "SELL", res
    assert paper.calls[0]["strategy"] == "ORB" and paper.calls[0]["sl"] == 5005.0, paper.calls


def test_on_fill_updates_books_and_flatten_clears_them():
    paper = FakePaper()
    r = new_router(paper=paper)
    r.add_account(healthy("a1", "apex", max_qty=5))
    r.on_fill("a1", {"action": "OPEN", "symbol": "MES=F", "side": "BUY", "qty": 2, "price": 5000.0})
    a = next(x for x in r.list_accounts() if x["id"] == "a1")
    assert len(a["positions"]) == 1 and a["positions"][0]["qty"] == 2, a

    r.mark_position("a1", "MES=F", 400.0)
    a = next(x for x in r.list_accounts() if x["id"] == "a1")
    assert a["peak_equity"] == 50400.0, a

    r.on_fill("a1", {"action": "CLOSE", "symbol": "MES=F", "side": "SELL", "qty": 2,
                     "price": 5010.0, "pnl_usd": 400.0})
    a = next(x for x in r.list_accounts() if x["id"] == "a1")
    assert a["balance"] == 50400.0 and a["day_pnl"] == 400.0 and a["positions"] == [], a
    assert a["peak_balance"] == 50400.0, a

    r.on_fill("a1", {"action": "OPEN", "symbol": "MES=F", "side": "BUY", "qty": 1, "price": 5001.0})
    out = r.flatten_all("a1")
    a = next(x for x in r.list_accounts() if x["id"] == "a1")
    assert out["ok"] is True and out["closed"] == 1 and a["positions"] == [], (out, a)
    assert paper.calls[-1]["side"] == "SELL" and paper.calls[-1]["strategy"] == "FLATTEN", paper.calls[-1]
    # Unknown account is reported, never raised.
    assert r.flatten_all("nope")["ok"] is False


def test_crud_persistence_and_state_shape():
    d = tempfile.mkdtemp(prefix="copyrouter-persist-")
    _TMP.append(d)
    path = os.path.join(d, "copy_accounts.json")
    paper = FakePaper()

    r = CopyRouter(store_path=path, executor=FakeExecutor(), paper_engine=paper)
    r._clock = lambda: FIXED_NOW
    r.add_account({"id": "leader", "label": "Leader", "firm_id": "lucid", "is_leader": True,
                   "session_day": SESSION_DAY})
    r.add_account(healthy("a1", "apex", max_qty=4))
    r.update_account("a1", {"max_qty": 7, "label": "Apex #1"})
    assert r.update_account("ghost", {})["error"] == "unknown_account"
    assert r.remove_account("ghost")["error"] == "unknown_account"

    r2 = CopyRouter(store_path=path, executor=FakeExecutor(), paper_engine=paper)
    r2._clock = lambda: FIXED_NOW
    st = r2.state()
    assert set(st.keys()) == {"enabled", "kill_switch", "leader_id", "accounts",
                              "recent_fanouts", "firms", "config"}, st.keys()
    assert st["enabled"] is False, st          # opt-in survives a reload
    assert st["leader_id"] == "leader", st
    a1 = next(a for a in st["accounts"] if a["id"] == "a1")
    assert a1["max_qty"] == 7 and a1["label"] == "Apex #1", a1
    for key in ("id", "label", "firm_id", "plan", "platform", "mode", "enabled", "is_leader",
                "multiplier", "max_qty", "symbol_map", "starting_balance", "balance",
                "day_start_balance", "peak_balance", "trailing_dd", "day_pnl", "positions",
                "created_at"):
        assert key in a1, key
    assert r2.set_leader("a1")["leader_id"] == "a1"
    assert r2.remove_account("a1")["ok"] is True


def test_firm_rulebook_covers_every_firm():
    ids = {f["id"] for f in CopyRouter(store_path=os.path.join(tempfile.mkdtemp(), "x.json")).firm_rules()}
    assert {"apex", "topstep", "mff", "tradeday", "lucid", "tradeify", "tpt", "alpha"} <= ids, ids
    for fid, firm in FIRM_RULES.items():
        assert firm["trailing_type"] in ("static", "eod", "intraday"), fid
        for key in ("daily_loss_limit", "consistency_limit", "hedging_allowed", "max_accounts",
                    "platform", "min_trading_days", "trailing_drawdown", "note", "plans"):
            assert key in firm, (fid, key)
        assert firm["default_plan"] in firm["plans"], fid
    # Stays consistent with apps/mobile/utils/propFirms.ts (FirmRule 50K rows).
    assert FIRM_RULES["apex"]["plans"]["50K"]["max_drawdown"] == 2500
    assert FIRM_RULES["topstep"]["plans"]["50K"]["max_drawdown"] == 2000
    assert FIRM_RULES["mff"]["plans"]["50K"]["daily_loss_limit"] == 1000.0
    assert FIRM_RULES["tradeday"]["plans"]["50K"]["daily_loss_limit"] == 2000.0
    assert FIRM_RULES["apex"]["max_accounts"] == 20


def test_recent_fanouts_and_audit_log():
    r = new_router()
    r.add_account(healthy("a1", "apex", max_qty=5))
    r.route_signal(signal(id="f-1"))
    st = r.state()
    fan = st["recent_fanouts"][0]
    assert fan["signal_id"] == "f-1" and fan["routed"] == 1, fan
    assert fan["results"][0]["account_id"] == "a1", fan
    assert len(r.audit_log()) > 0


# ── Runner ─────────────────────────────────────────────────────────────────


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    print(f"copy_router: running {len(tests)} tests\n")
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
        except Exception:
            failed += 1
            print(f"  FAIL  {t.__name__}")
            traceback.print_exc()
    for d in _TMP:
        shutil.rmtree(d, ignore_errors=True)
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
