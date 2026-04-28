"""
Rithmic R|API+ live order executor for Morgan Futures bot.

Connects to Lucid PropFirm's Rithmic gateway via WebSocket + protobuf.
Implements bracket orders: market entry → SL (stop-market) + TP (limit) on fill.

Environment variables required:
    RITHMIC_USER          Lucid/Rithmic username
    RITHMIC_PASSWORD      Lucid/Rithmic password
    RITHMIC_SYSTEM_NAME   System name from Lucid dashboard (e.g. "Rithmic Paper Trading")
    RITHMIC_GATEWAY_URI   WSS URI from Lucid (e.g. wss://rituz00100.rithmic.com:443)
    RITHMIC_ACCOUNT_ID    Account ID shown in Lucid platform

Optional:
    RITHMIC_APP_NAME      Default "MorganFutures"
    RITHMIC_APP_VERSION   Default "1.0"
    RITHMIC_DEBUG         Set to "1" to log all raw protobuf messages

Field numbers are from R|API+ protobuf definitions (community-documented).
If fills are not received, enable RITHMIC_DEBUG=1 and check that template_id
values in incoming messages match the TID constants below.
"""
from __future__ import annotations

import asyncio
import logging
import os
import ssl
import struct
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Callable

log = logging.getLogger(__name__)

# ── Minimal protobuf encoder/decoder ──────────────────────────────────────


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        bits = n & 0x7F
        n >>= 7
        out.append(bits | (0x80 if n else 0))
        if not n:
            break
    return bytes(out)


def _pf_int(field: int, value: int) -> bytes:
    """Encode varint field (int32/uint32/enum)."""
    return _varint((field << 3) | 0) + _varint(value)


def _pf_str(field: int, s: str) -> bytes:
    """Encode string (length-delimited) field."""
    b = s.encode("utf-8")
    return _varint((field << 3) | 2) + _varint(len(b)) + b


def _pf_double(field: int, v: float) -> bytes:
    """Encode double (64-bit IEEE 754) field."""
    return _varint((field << 3) | 1) + struct.pack("<d", v)


def _rithmic_frame(payload: bytes) -> bytes:
    """Rithmic wire format: 4-byte big-endian length prefix + protobuf payload."""
    return struct.pack(">I", len(payload)) + payload


def _parse_proto(data: bytes) -> dict:
    """
    Minimal protobuf parser. Returns {field_number: value}.
    Handles varints, 64-bit doubles, and UTF-8 strings (length-delimited fields).
    Repeated fields: last value wins (we only care about status codes and prices).
    """
    result: dict = {}
    i = 0
    while i < len(data):
        try:
            tag = 0
            shift = 0
            while True:
                b = data[i]; i += 1
                tag |= (b & 0x7F) << shift
                if not (b & 0x80):
                    break
                shift += 7
            field = tag >> 3
            wire = tag & 7
            if wire == 0:  # varint
                v = 0; shift = 0
                while True:
                    b = data[i]; i += 1
                    v |= (b & 0x7F) << shift
                    if not (b & 0x80):
                        break
                    shift += 7
                result[field] = v
            elif wire == 1:  # 64-bit
                result[field] = struct.unpack_from("<d", data, i)[0]
                i += 8
            elif wire == 2:  # length-delimited
                length = 0; shift = 0
                while True:
                    b = data[i]; i += 1
                    length |= (b & 0x7F) << shift
                    if not (b & 0x80):
                        break
                    shift += 7
                raw = data[i:i + length]; i += length
                try:
                    result[field] = raw.decode("utf-8")
                except Exception:
                    result[field] = raw
            elif wire == 5:  # 32-bit float
                result[field] = struct.unpack_from("<f", data, i)[0]
                i += 4
            else:
                break  # unknown wire type — stop parsing
        except (IndexError, struct.error):
            break
    return result


# ── Template IDs and field constants ──────────────────────────────────────

# Field 154467 is the template_id field in all Rithmic R|API+ protobuf messages
TID_FIELD = 154467

# Field 132766 = rp_code (return code in responses, "0" = success)
RP_CODE_FIELD = 132766

# Order update status codes (field 9 in ResponseOrderUpdate)
class OrderStatus:
    NEW = 1
    MODIFY_PENDING = 2
    MODIFIED = 3
    CANCEL_PENDING = 4
    CANCELED = 5
    FILLED = 6
    PARTIAL_FILL = 7
    REJECTED = 8


class TID:
    LOGIN_REQ = 10
    LOGIN_RESP = 11
    LOGOUT_REQ = 12
    LOGOUT_RESP = 13
    HEARTBEAT_REQ = 18
    HEARTBEAT_RESP = 19
    NEW_ORDER_REQ = 312
    NEW_ORDER_RESP = 313
    CANCEL_ORDER_REQ = 316
    CANCEL_ORDER_RESP = 317
    ORDER_UPDATE = 350


# ── Contract specs (matches server.py CONTRACT_SPECS) ─────────────────────

_CONTRACT_SPECS: dict[str, dict] = {
    "RTY=F": {"tick_size": 0.10, "tick_value": 5.00,    "exchange": "CME"},
    "ES=F":  {"tick_size": 0.25, "tick_value": 12.50,   "exchange": "CME"},
    "YM=F":  {"tick_size": 1.0,  "tick_value": 5.00,    "exchange": "CBOT"},
    "MES=F": {"tick_size": 0.25, "tick_value": 1.25,    "exchange": "CME"},
    "MYM=F": {"tick_size": 1.0,  "tick_value": 0.50,    "exchange": "CBOT"},
    "ZB=F":  {"tick_size": 0.03125, "tick_value": 31.25,"exchange": "CBOT"},
    "NQ=F":  {"tick_size": 0.25, "tick_value": 5.00,    "exchange": "CME"},
    "MNQ=F": {"tick_size": 0.25, "tick_value": 0.50,    "exchange": "CME"},
    "GC=F":  {"tick_size": 0.10, "tick_value": 10.00,   "exchange": "COMEX"},
    "CL=F":  {"tick_size": 0.01, "tick_value": 10.00,   "exchange": "NYMEX"},
}


def _contract_month_code() -> str:
    """Return the current front-month code, e.g. 'M5' for June 2025."""
    now = datetime.now(timezone.utc)
    year_digit = now.year % 10
    # Equity futures roll quarterly: H(Mar), M(Jun), U(Sep), Z(Dec)
    if now.month <= 3:
        letter = "H"
    elif now.month <= 6:
        letter = "M"
    elif now.month <= 9:
        letter = "U"
    else:
        letter = "Z"
    return f"{letter}{year_digit}"


def _rithmic_symbol(yahoo_sym: str) -> tuple[str, str]:
    """Return (rithmic_contract_symbol, exchange) for a Yahoo Finance futures ticker."""
    month = _contract_month_code()
    spec = _CONTRACT_SPECS.get(yahoo_sym)
    exchange = spec["exchange"] if spec else "CME"
    base = yahoo_sym.replace("=F", "")
    return f"{base}{month}", exchange


# ── RithmicExecutor ────────────────────────────────────────────────────────


class RithmicExecutor:
    """
    Live order executor connecting to Rithmic R|API+ via WebSocket.

    Lifecycle:
        1. executor.start()           — start background event loop thread
        2. executor.connect(...)      — schedule async connection (non-blocking)
        3. executor.on_fill(cb)       — register fill/close callback
        4. executor.place_bracket_order(...)  — fire and forget
    """

    def __init__(self) -> None:
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ws = None
        self._connected = False
        self._fcm_id: str = ""
        self._ib_id: str = ""
        self._account_id: str = os.environ.get("RITHMIC_ACCOUNT_ID", "")
        self._fill_callbacks: list[Callable[[dict], None]] = []
        self._open_orders: dict[str, dict] = {}   # basket_id -> order info
        self._open_positions: dict[str, dict] = {}  # yahoo_sym -> position
        self._lock = threading.RLock()
        self._debug = os.environ.get("RITHMIC_DEBUG", "0") == "1"
        self._reconnect = True
        # Connection params, filled in by connect()
        self._user = ""
        self._password = ""
        self._system_name = ""
        self._gateway_uri = ""
        self._app_name = os.environ.get("RITHMIC_APP_NAME", "MorganFutures")
        self._app_version = os.environ.get("RITHMIC_APP_VERSION", "1.0")

    # ── Public API ────────────────────────────────────────────────────────

    def start(self) -> None:
        """Start the background asyncio event loop thread. Call once at startup."""
        t = threading.Thread(target=self._run_event_loop, daemon=True, name="rithmic-loop")
        t.start()
        for _ in range(50):
            if self._loop is not None:
                return
            time.sleep(0.1)

    def connect(
        self,
        user: str,
        password: str,
        system_name: str,
        gateway_uri: str,
    ) -> None:
        """
        Schedule async connection to the Rithmic gateway (non-blocking).
        Connection runs in the background; check is_connected() for status.
        """
        self._user = user
        self._password = password
        self._system_name = system_name
        self._gateway_uri = gateway_uri
        if self._loop:
            asyncio.run_coroutine_threadsafe(self._connection_loop(), self._loop)
        else:
            log.error("Call start() before connect()")

    def on_fill(self, callback: Callable[[dict], None]) -> None:
        """
        Register a callback invoked on every trade close (TP or SL hit).
        Callback receives a dict with: yahoo_sym, side, qty, entry, exit_price,
        exit_reason ('TP'|'SL'), pnl_usd, basket_id.
        """
        self._fill_callbacks.append(callback)

    def place_bracket_order(
        self,
        yahoo_sym: str,
        side: str,
        qty: int,
        entry: float,
        sl: float,
        tp: float,
    ) -> str | None:
        """
        Place a bracket order: market entry + stop-loss + take-profit.
        SL and TP orders are sent after the entry fill is confirmed.
        Returns basket_id for tracking, or None if not connected.
        """
        if not self._connected:
            log.warning("Rithmic not connected — cannot place order")
            return None

        basket_id = uuid.uuid4().hex[:16]
        rithmic_sym, exchange = _rithmic_symbol(yahoo_sym)
        order_info = {
            "basket_id": basket_id,
            "yahoo_sym": yahoo_sym,
            "rithmic_sym": rithmic_sym,
            "exchange": exchange,
            "side": side.upper(),
            "qty": qty,
            "entry": entry,
            "sl": sl,
            "tp": tp,
            "status": "PENDING",
            "placed_at": datetime.now(timezone.utc).isoformat(),
        }
        with self._lock:
            self._open_orders[basket_id] = order_info

        asyncio.run_coroutine_threadsafe(
            self._send_market_entry(rithmic_sym, exchange, side.upper(), qty, basket_id),
            self._loop,
        )
        log.info(
            f"[ORDER] {side.upper()} {qty}x{rithmic_sym} basket={basket_id} "
            f"SL={sl} TP={tp}"
        )
        return basket_id

    def cancel_order(self, basket_id: str) -> None:
        """Cancel a pending order by basket_id."""
        with self._lock:
            order = self._open_orders.get(basket_id)
        if not order or not self._connected or not self._loop:
            return
        asyncio.run_coroutine_threadsafe(
            self._send_cancel(basket_id, order["rithmic_sym"], order["exchange"]),
            self._loop,
        )

    def has_open_position(self, yahoo_sym: str) -> bool:
        """Return True if there is already an open position on this instrument."""
        with self._lock:
            return yahoo_sym in self._open_positions

    def get_positions(self) -> dict:
        with self._lock:
            return dict(self._open_positions)

    def is_connected(self) -> bool:
        return self._connected

    # ── Internal: event loop ──────────────────────────────────────────────

    def _run_event_loop(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    # ── Internal: connection management ───────────────────────────────────

    async def _connection_loop(self) -> None:
        """Persistent WebSocket connection with auto-reconnect."""
        try:
            import websockets
        except ImportError:
            log.error("websockets package not installed. Run: pip install websockets")
            return

        while self._reconnect:
            try:
                ssl_ctx = ssl.create_default_context()
                log.info(f"Connecting to Rithmic gateway: {self._gateway_uri}")
                async with websockets.connect(
                    self._gateway_uri,
                    ssl=ssl_ctx,
                    ping_interval=20,
                    ping_timeout=10,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    if not await self._login(ws):
                        log.error("Rithmic login failed — check credentials and system_name")
                        self._ws = None
                        await asyncio.sleep(30)
                        continue

                    self._connected = True
                    log.info("Rithmic: connected and authenticated")

                    async for raw in ws:
                        if isinstance(raw, bytes) and len(raw) >= 4:
                            payload = raw[4:]  # strip 4-byte length prefix
                            if self._debug:
                                log.debug(f"[RX raw] {payload.hex()[:120]}")
                            await self._handle_message(payload)

            except Exception as e:
                log.warning(f"Rithmic connection dropped: {e}")
                self._connected = False
                self._ws = None
                await asyncio.sleep(5)

    async def _login(self, ws) -> bool:
        """Send RequestLogin (template 10) and parse ResponseLogin (template 11)."""
        msg = (
            _pf_int(TID_FIELD, TID.LOGIN_REQ)
            + _pf_str(2, self._user)
            + _pf_str(12, self._password)
            + _pf_str(3, self._system_name)
            + _pf_str(4, self._app_name)
            + _pf_str(5, self._app_version)
            + _pf_int(8, 2)  # login_type: 2 = ORDER_PLANT
        )
        await ws.send(_rithmic_frame(msg))
        if self._debug:
            log.debug(f"[TX] RequestLogin user={self._user} system={self._system_name}")

        try:
            for _ in range(15):  # allow a few non-login messages before timeout
                raw = await asyncio.wait_for(ws.recv(), timeout=15.0)
                if not isinstance(raw, bytes) or len(raw) < 4:
                    continue
                fields = _parse_proto(raw[4:])
                if self._debug:
                    log.debug(f"[RX] {fields}")
                if fields.get(TID_FIELD) == TID.LOGIN_RESP:
                    rp = str(fields.get(RP_CODE_FIELD, ""))
                    if rp.startswith("0"):
                        self._fcm_id = str(fields.get(300, ""))
                        self._ib_id = str(fields.get(301, ""))
                        if not self._account_id:
                            self._account_id = str(fields.get(16, ""))
                        return True
                    log.error(f"Rithmic login refused — rp_code: {rp!r}")
                    return False
        except asyncio.TimeoutError:
            log.error("Rithmic login timeout (15s)")
        return False

    # ── Internal: order placement ─────────────────────────────────────────

    async def _send_market_entry(
        self, rithmic_sym: str, exchange: str, side: str, qty: int, basket_id: str
    ) -> None:
        if not self._ws:
            return
        side_code = 1 if side == "BUY" else 2
        msg = (
            _pf_int(TID_FIELD, TID.NEW_ORDER_REQ)
            + _pf_str(154013, self._fcm_id)
            + _pf_str(154014, self._ib_id)
            + _pf_str(16, self._account_id)
            + _pf_str(110, rithmic_sym)
            + _pf_str(111, exchange)
            + _pf_int(112, qty)
            + _pf_int(121, 2)   # price_type: 2 = MARKET
            + _pf_int(113, side_code)
            + _pf_int(120, 1)   # duration: 1 = DAY
            + _pf_str(132001, basket_id)
        )
        await self._ws.send(_rithmic_frame(msg))
        if self._debug:
            log.debug(f"[TX] NewOrder MKT {side} {qty}x{rithmic_sym} basket={basket_id}")

    async def _send_sl_tp(
        self,
        rithmic_sym: str,
        exchange: str,
        close_side: str,
        qty: int,
        sl: float,
        tp: float,
        basket_id: str,
    ) -> None:
        """Place SL (stop-market) and TP (limit) orders after entry fills."""
        if not self._ws:
            return
        close_code = 1 if close_side == "BUY" else 2

        # Stop-loss: stop-market order
        sl_msg = (
            _pf_int(TID_FIELD, TID.NEW_ORDER_REQ)
            + _pf_str(154013, self._fcm_id)
            + _pf_str(154014, self._ib_id)
            + _pf_str(16, self._account_id)
            + _pf_str(110, rithmic_sym)
            + _pf_str(111, exchange)
            + _pf_int(112, qty)
            + _pf_int(121, 3)           # price_type: 3 = STOP_MARKET
            + _pf_double(110301, sl)    # trigger_price
            + _pf_int(113, close_code)
            + _pf_int(120, 2)           # duration: 2 = GTC
            + _pf_str(132001, basket_id + "_SL")
        )
        await self._ws.send(_rithmic_frame(sl_msg))

        # Take-profit: limit order
        tp_msg = (
            _pf_int(TID_FIELD, TID.NEW_ORDER_REQ)
            + _pf_str(154013, self._fcm_id)
            + _pf_str(154014, self._ib_id)
            + _pf_str(16, self._account_id)
            + _pf_str(110, rithmic_sym)
            + _pf_str(111, exchange)
            + _pf_int(112, qty)
            + _pf_int(121, 1)           # price_type: 1 = LIMIT
            + _pf_double(110300, tp)    # limit price
            + _pf_int(113, close_code)
            + _pf_int(120, 2)           # duration: 2 = GTC
            + _pf_str(132001, basket_id + "_TP")
        )
        await self._ws.send(_rithmic_frame(tp_msg))
        if self._debug:
            log.debug(f"[TX] SL+TP placed for basket={basket_id} SL={sl} TP={tp}")

    async def _send_cancel(self, basket_id: str, rithmic_sym: str, exchange: str) -> None:
        if not self._ws:
            return
        msg = (
            _pf_int(TID_FIELD, TID.CANCEL_ORDER_REQ)
            + _pf_str(154013, self._fcm_id)
            + _pf_str(154014, self._ib_id)
            + _pf_str(16, self._account_id)
            + _pf_str(110, rithmic_sym)
            + _pf_str(111, exchange)
            + _pf_str(132001, basket_id)
        )
        await self._ws.send(_rithmic_frame(msg))

    # ── Internal: message handling ────────────────────────────────────────

    async def _handle_message(self, data: bytes) -> None:
        fields = _parse_proto(data)
        if self._debug:
            log.debug(f"[RX parsed] {fields}")

        tid = fields.get(TID_FIELD)
        if tid == TID.HEARTBEAT_REQ:
            hb = _pf_int(TID_FIELD, TID.HEARTBEAT_RESP)
            if self._ws:
                await self._ws.send(_rithmic_frame(hb))
        elif tid == TID.ORDER_UPDATE:
            await self._handle_order_update(fields)

    async def _handle_order_update(self, fields: dict) -> None:
        """
        Process ResponseOrderUpdate (template 350).

        Key fields (approximate — enable RITHMIC_DEBUG=1 to inspect actual field numbers):
            132001: basket_id (client order ref)
            9:      status code (see OrderStatus class)
            110300: fill price
            112:    fill quantity
        """
        basket_id = str(fields.get(132001, ""))
        status = int(fields.get(9, 0))
        fill_price = float(fields.get(110300, 0.0) or 0.0)
        fill_qty = int(fields.get(112, 0) or 0)

        log.info(f"[ORDER_UPDATE] basket={basket_id} status={status} fill_px={fill_price} qty={fill_qty}")

        # Resolve to root basket (strip _SL / _TP suffix)
        root_basket = basket_id.replace("_SL", "").replace("_TP", "")

        with self._lock:
            order = self._open_orders.get(root_basket)
        if not order:
            return

        if status == OrderStatus.FILLED and fill_price > 0:
            if basket_id == root_basket:
                # Entry fill → place SL + TP
                entry_side = order["side"]
                close_side = "SELL" if entry_side == "BUY" else "BUY"
                with self._lock:
                    self._open_positions[order["yahoo_sym"]] = {
                        **order,
                        "fill_price": fill_price,
                        "fill_qty": fill_qty,
                        "status": "OPEN",
                        "opened_at": datetime.now(timezone.utc).isoformat(),
                    }
                asyncio.ensure_future(
                    self._send_sl_tp(
                        order["rithmic_sym"],
                        order["exchange"],
                        close_side,
                        order["qty"],
                        order["sl"],
                        order["tp"],
                        root_basket,
                    )
                )
                log.info(
                    f"[FILL] Entry {entry_side} {fill_qty}x{order['rithmic_sym']} "
                    f"@ {fill_price} — SL/TP orders queued"
                )

            elif "_SL" in basket_id or "_TP" in basket_id:
                # Exit fill (SL or TP hit)
                with self._lock:
                    pos = self._open_positions.pop(order["yahoo_sym"], None)
                    self._open_orders.pop(root_basket, None)
                if pos:
                    self._fire_fill_callbacks(pos, fill_price, basket_id)

        elif status == OrderStatus.PARTIAL_FILL and fill_price > 0:
            # Track partial fills — full handling can be extended if needed
            log.info(f"[PARTIAL_FILL] basket={basket_id} qty={fill_qty} px={fill_price}")

        elif status == OrderStatus.REJECTED:
            log.error(f"[REJECTED] basket={basket_id} — fields: {fields}")
            with self._lock:
                order = self._open_orders.pop(root_basket, None)
                if order:
                    self._open_positions.pop(order.get("yahoo_sym", ""), None)

        elif status == OrderStatus.CANCELED:
            log.info(f"[CANCELED] basket={basket_id}")
            with self._lock:
                self._open_orders.pop(root_basket, None)

    def _fire_fill_callbacks(self, pos: dict, exit_price: float, basket_id: str) -> None:
        """Calculate P&L and invoke registered fill callbacks."""
        yahoo_sym = pos["yahoo_sym"]
        spec = _CONTRACT_SPECS.get(yahoo_sym, {"tick_size": 0.25, "tick_value": 1.0})
        tick_size = float(spec["tick_size"])
        tick_value = float(spec["tick_value"])
        entry = float(pos.get("fill_price") or pos.get("entry", 0))
        side = str(pos.get("side", "BUY")).upper()
        qty = int(pos.get("qty", 1))

        ticks = (exit_price - entry) / tick_size
        if side == "SELL":
            ticks = -ticks
        pnl_usd = round(ticks * tick_value * qty, 2)
        reason = "TP" if "_TP" in basket_id else "SL"

        trade = {
            **pos,
            "exit_price": exit_price,
            "exit_reason": reason,
            "pnl_usd": pnl_usd,
            "closed_at": datetime.now(timezone.utc).isoformat(),
        }
        log.info(f"[CLOSED] {yahoo_sym} {reason} pnl=${pnl_usd:.2f}")
        for cb in self._fill_callbacks:
            try:
                cb(trade)
            except Exception as e:
                log.error(f"Fill callback error: {e}")
