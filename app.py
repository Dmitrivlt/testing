# app.py — Binance USDT-M Futures EMA Touch Trader (v1.8.6)
# Новое в v1.8.6:
# - Автовыбор монеты Топ-1 по росту за 24ч среди РЕАЛЬНО доступных USDT-M PERPETUAL (status=TRADING).
# - Если поле "Symbols" пустое, включается auto-pick: бот берёт Топ-1; если Топ-1 меняется и позиций нет — перегружается на новый символ.
# - Фолбэк при любой ошибке авто-пика: CYBERUSDT.
#
# Из v1.8.5:
# - Убраны PnL 1/7/30 дней; добавлен только PnL за сегодня (UTC), только REALIZED_PNL (точный).
# - PnL Today (R) в UI как бейдж: >0 зелёный, <0 красный, 0 серый.
# - Улучшены перевороты: сначала закрытие, микропаузa, повторный fetch_account, попытка открыть прежним объёмом.
# - Точные проверки minNotional/minQty; корректное уменьшение количества; в hedge-режиме reduceOnly НЕ отправляем.
# - По умолчанию DEFAULT_LEVERAGE=10; MID_GAP_THRESHOLD берётся из env (по умолчанию 1%).
#
# Зависимости:  pip install fastapi uvicorn httpx websockets

import os, time, math, json, asyncio, random, hmac, hashlib
import datetime as dt
from typing import Optional, Dict, List, Tuple, Any

import httpx, websockets
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

# ---------- Binance Futures (USDT-M) ----------
BINANCE_API = "https://fapi.binance.com"
WS_PUBLIC   = "wss://fstream.binance.com/ws"

API_KEY        = os.getenv("BINANCE_API_KEY", "")
API_SECRET_RAW = os.getenv("BINANCE_API_SECRET", "")
API_SECRET     = API_SECRET_RAW.encode() if API_SECRET_RAW else b""

HTTP_TIMEOUT   = 10.0
TIME_DRIFT_MS  = 0
TIME_SYNC_SEC  = 300

# ---------- Strategy / risk defaults ----------
DEFAULT_INTERVAL     = "1m"
DEFAULT_LEVERAGE     = 1   # по умолчанию 1х
LEVERAGE_CAP         = 50
UTILIZATION          = 1.00
FEE_BUFFER_RATE      = 0.001
WARMUP_LIMIT         = 1200
MID_GAP_THRESHOLD    = float(os.getenv("MID_GAP_THRESHOLD", "0.01"))  # 1%

DEFAULT_ON_CLOSE_ONLY = False   # OFF = Touch (intrabar)
DEFAULT_MODE = "BOTH"           # BOTH | LONG

AUTO_FALLBACK_SYMBOL = "CYBERUSDT"  # если авто-пик не удался

NATIVE_INTERVALS = {"1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d","3d","1w","1M"}
ALL_INTERVALS    = ["1s"] + sorted(
    list(NATIVE_INTERVALS),
    key=lambda x: ["m","h","d","w","M"].index(x[-1]) * 1000 + int(x[:-1])
)

def now_ms() -> int:
    return int(time.time()*1000)

# ---------- Backoff helpers ----------
async def _get_with_backoff(http: httpx.AsyncClient, url: str, *,
                            params: Optional[dict]=None, headers: Optional[dict]=None,
                            max_tries: int=6) -> httpx.Response:
    delay = 0.8
    for _ in range(max_tries):
        try:
            r = await http.get(url, params=params, headers=headers)
            if r.status_code in (418, 429):
                ra = r.headers.get("Retry-After")
                wait = float(ra) if (ra and ra.isdigit()) else delay
                await asyncio.sleep(min(wait + random.uniform(0,0.4), 30.0))
                delay = min(delay * 2.0, 8.0)
                continue
            r.raise_for_status()
            return r
        except httpx.TransportError:
            await asyncio.sleep(0.6 + random.uniform(0,0.4))
    r = await http.get(url, params=params, headers=headers)
    r.raise_for_status()
    return r

def _sign(params: Dict[str, str]) -> List[Tuple[str,str]]:
    items = sorted([(k, str(v)) for k, v in params.items()], key=lambda x: x[0])
    q = "&".join([f"{k}={v}" for k, v in items])
    sig = hmac.new(API_SECRET, q.encode(), hashlib.sha256).hexdigest()
    items.append(("signature", sig))
    return items

async def _signed_get_with_backoff(http: httpx.AsyncClient, url: str, *,
                                   params: Dict[str,str], max_tries: int=6) -> httpx.Response:
    headers = {"X-MBX-APIKEY": API_KEY}
    delay = 0.8
    for _ in range(max_tries):
        try:
            qp = _sign(params)
            r = await http.get(url, params=qp, headers=headers)
            if r.status_code in (418, 429):
                ra = r.headers.get("Retry-After")
                wait = float(ra) if (ra and ra.isdigit()) else delay
                await asyncio.sleep(min(wait + random.uniform(0,0.4), 30.0))
                delay = min(delay * 2.0, 8.0)
                continue
            r.raise_for_status()
            return r
        except httpx.TransportError:
            await asyncio.sleep(0.6 + random.uniform(0,0.4))
    qp = _sign(params)
    r = await http.get(url, params=qp, headers=headers)
    if r.status_code not in (418, 429):
        r.raise_for_status()
    return r

# ---------- TV-like EMA ----------
def tv_ema_series(closes: List[float], length: int) -> List[Optional[float]]:
    """
    Полный эквивалент pandas ewm(adjust=False) для EMA:
      - seed = первый close ряда (а не SMA первых N баров)
      - EMA_t = alpha*close_t + (1-alpha)*EMA_{t-1}, где alpha=2/(length+1)
      - значения считаются на каждом элементе ряда (как ewm), 
        но использовать будем только последний (как у 'Васи' в on_bar_close)
    """
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    if length < 1 or n == 0:
        return out

    alpha = 2.0 / (length + 1.0)

    # seed строго = первый close (эквивалент ewm(..., adjust=False))
    prev = float(closes[0])
    out[0] = prev

    for i in range(1, n):
        c = float(closes[i])
        prev = alpha * c + (1.0 - alpha) * prev
        out[i] = prev
    return out

def tv_ema_next(prev: float, close_price: float, length: int) -> float:
    alpha = 2.0/(length+1.0)
    return alpha*close_price + (1.0-alpha)*prev

# ---------- Futures client (hedge-aware) ----------
class FuturesClient:
    def __init__(self, symbol: str, http: httpx.AsyncClient):
        self.symbol = symbol.upper()
        self.http   = http
        # filters
        self.tick: Optional[float] = None
        self.step: Optional[float] = None
        self.min_qty: float = 0.0
        self.max_qty: float = 0.0
        self.min_notional: float = 5.0
        # account
        self.available_usdt: float = 0.0
        self.leverage: int = DEFAULT_LEVERAGE
        self.dual_side: bool = False  # Hedge mode?
        # one-way net position (BOTH)
        self.net_pos: float = 0.0
        self.entry_px: float = 0.0
        # hedge split
        self.long_pos: float = 0.0
        self.short_pos: float = 0.0
        self.long_entry: float = 0.0
        self.short_entry: float = 0.0

    async def sync_time(self):
        global TIME_DRIFT_MS
        try:
            r = await _get_with_backoff(self.http, f"{BINANCE_API}/fapi/v1/time")
            server_ms = int(r.json()["serverTime"])
            local_ms  = now_ms()
            TIME_DRIFT_MS = server_ms - local_ms
        except Exception as e:
            print("time sync:", e)

    async def fetch_filters(self):
        r = await _get_with_backoff(self.http, f"{BINANCE_API}/fapi/v1/exchangeInfo",
                                    params={"symbol": self.symbol})
        data = r.json()
        syms = {s["symbol"]: s for s in data.get("symbols", [])}
        if self.symbol not in syms:
            raise RuntimeError(f"{self.symbol}: not in futures exchangeInfo")
        sym = syms[self.symbol]
        self.tick = self.step = None
        self.min_qty = self.max_qty = 0.0
        self.min_notional = 5.0
        for f in sym.get("filters", []):
            t = f.get("filterType")
            if t == "PRICE_FILTER":
                self.tick = float(f["tickSize"])
            elif t == "LOT_SIZE":
                self.step = float(f["stepSize"])
                self.min_qty = float(f.get("minQty","0"))
                self.max_qty = float(f.get("maxQty","0"))
            elif t in ("MIN_NOTIONAL","NOTIONAL"):
                self.min_notional = float(f.get("minNotional", f.get("notional","5")))
        if not self.tick or not self.step:
            raise RuntimeError(f"{self.symbol}: tick/step not found")

    async def fetch_dual_side(self):
        try:
            ts = now_ms() + TIME_DRIFT_MS
            r = await _signed_get_with_backoff(
                self.http,
                f"{BINANCE_API}/fapi/v1/positionSide/dual",
                params={"timestamp": ts, "recvWindow": 5000}
            )
            if r.status_code in (418, 429):
                self.dual_side = False
                return
            self.dual_side = bool(r.json().get("dualSidePosition", False))
        except Exception as e:
            self.dual_side = False
            print("dualSide error:", e)

    async def fetch_account(self):
        ts = now_ms() + TIME_DRIFT_MS
        qp = _sign({"timestamp": ts, "recvWindow": 5000})
        r = await self.http.get(
            f"{BINANCE_API}/fapi/v2/account",
            params=qp,
            headers={"X-MBX-APIKEY": API_KEY},
        )
        r.raise_for_status()
        acc = r.json()

        self.available_usdt = 0.0
        self.net_pos = 0.0
        self.entry_px = 0.0
        self.long_pos = self.short_pos = 0.0
        self.long_entry = self.short_entry = 0.0

        for a in acc.get("assets", []):
            if a.get("asset") == "USDT":
                self.available_usdt = float(a.get("availableBalance", "0"))
                break

        for p in acc.get("positions", []):
            if p.get("symbol") != self.symbol:
                continue
            side = p.get("positionSide", "BOTH")
            amt  = float(p.get("positionAmt", "0"))
            ent  = float(p.get("entryPrice", "0"))
            try:
                self.leverage = int(float(p.get("leverage", self.leverage)))
            except Exception:
                pass

            if side == "BOTH":
                self.net_pos  = amt
                self.entry_px = ent
            elif side == "LONG":
                self.long_pos   = amt
                self.long_entry = ent
            elif side == "SHORT":
                self.short_pos   = abs(amt)
                self.short_entry = ent

        if not self.dual_side:
            if self.net_pos > 0:
                self.long_pos, self.short_pos = self.net_pos, 0.0
                self.long_entry, self.short_entry = self.entry_px, 0.0
            elif self.net_pos < 0:
                self.long_pos, self.short_pos = 0.0, abs(self.net_pos)
                self.long_entry, self.short_entry = 0.0, self.entry_px

    async def ensure_leverage(self, target: int):
        target = max(1, min(int(target), LEVERAGE_CAP))
        if self.leverage == target:
            return
        ts = now_ms() + TIME_DRIFT_MS
        qp = _sign({"symbol": self.symbol, "leverage": target, "timestamp": ts, "recvWindow": 5000})
        r = await self.http.post(
            f"{BINANCE_API}/fapi/v1/leverage",
            params=qp,
            headers={"X-MBX-APIKEY": API_KEY}
        )
        r.raise_for_status()
        try:
            self.leverage = int(r.json().get("leverage", target))
        except Exception:
            self.leverage = target

    # ----- qty helpers -----
    def _round_qty_floor(self, qty: float) -> float:
        step = self.step or 0.0
        if step <= 0:
            return qty
        prec = max(0, int(round(-math.log10(step))))
        q = math.floor(qty / step) * step
        return float(f"{q:.{prec}f}")

    def _round_qty_ceil(self, qty: float) -> float:
        step = self.step or 0.0
        if step <= 0:
            return qty
        prec = max(0, int(round(-math.log10(step))))
        q = math.ceil(qty / step) * step
        return float(f"{q:.{prec}f}")

    def min_qty_for_notional(self, price: float) -> float:
        """Минимум, чтобы удовлетворить и minNotional, и minQty."""
        need_by_notional = self._round_qty_ceil(self.min_notional / max(price, 1e-12))
        return max(self.min_qty, need_by_notional)

    def max_affordable_qty(self, price: float) -> float:
        """Максимум, исходя из доступного нотионала (учитываем буфер)."""
        notional_cap = max(0.0, self.available_usdt - self.available_usdt*FEE_BUFFER_RATE) * self.leverage * UTILIZATION
        q = self._round_qty_floor(notional_cap / max(price, 1e-12))
        if self.max_qty > 0:
            q = min(q, self.max_qty)
        return max(0.0, q)

    def compute_order_qty(self, ref_price: float) -> Optional[float]:
        price = max(ref_price, 1e-12)
        need = self.min_qty_for_notional(price)
        cap  = self.max_affordable_qty(price)
        if cap < need - 1e-12:
            return None
        qty = max(need, cap)
        return self._round_qty_floor(qty)

    async def fetch_last_price(self) -> Optional[float]:
        try:
            r = await _get_with_backoff(self.http, f"{BINANCE_API}/fapi/v1/ticker/price",
                                        params={"symbol": self.symbol})
            return float(r.json().get("price"))
        except Exception:
            return None

    async def place_market(self, side: str, qty: float, *,
                           reduce_only: bool=False,
                           position_side: Optional[str]=None) -> Tuple[Optional[int], Optional[str]]:
        """
        Универсальный маркет-ордер.
        ВНИМАНИЕ: в hedge-режиме reduceOnly НЕ отправляем (иначе -1106).
        """
        ts = now_ms() + TIME_DRIFT_MS
        params = {
            "symbol": self.symbol, "side": side, "type": "MARKET",
            "quantity": f"{qty}",
            "timestamp": ts, "recvWindow": 5000
        }

        if self.dual_side:
            if position_side is None:
                position_side = "LONG" if side == "BUY" else "SHORT"
            params["positionSide"] = position_side
        else:
            if reduce_only:
                params["reduceOnly"] = "true"

        try:
            r = await self.http.post(
                f"{BINANCE_API}/fapi/v1/order",
                params=_sign(params),
                headers={"X-MBX-APIKEY": API_KEY}
            )
            r.raise_for_status()
            j = r.json()
            oid = int(j.get("orderId") or 0)
            return (oid if oid > 0 else None, None)
        except httpx.HTTPStatusError as e:
            try:
                return (None, e.response.text)
            except Exception:
                return (None, str(e))
        except Exception as e:
            return (None, str(e))

    async def place_market_smart(
        self,
        side: str,
        ref_price: float,
        *,
        reduce_only: bool = False,
        position_side: Optional[str] = None,
        qty_override: Optional[float] = None,
        open_prefer_qty: Optional[float] = None
    ) -> Tuple[Optional[int], Optional[str]]:
        """
        Маркет с ретраями.
        - Для reduce_only берём фактический размер позиции (или qty_override).
        - Для открытия сначала пробуем open_prefer_qty (например, размер только что закрытой позиции),
          если не проходит — уменьшаем ступенями, потом fallback к compute_order_qty().
        """
        price = ref_price if ref_price > 0 else (await self.fetch_last_price() or 0.0)

        await self.fetch_account()
        before_net   = self.net_pos
        before_long  = self.long_pos
        before_short = self.short_pos

        if qty_override is not None:
            qty = self._round_qty_floor(max(0.0, qty_override))
        elif reduce_only:
            if self.dual_side:
                if position_side is None:
                    position_side = "LONG" if side == "SELL" else "SHORT"
                base = self.long_pos if position_side == "LONG" else self.short_pos
                qty = self._round_qty_floor(abs(base))
            else:
                qty = self._round_qty_floor(abs(self.net_pos))
            if qty <= 0:
                return (None, "nothing to close")
        else:
            if open_prefer_qty and open_prefer_qty > 0:
                need = self.min_qty_for_notional(price)
                cap  = self.max_affordable_qty(price)
                q0   = self._round_qty_floor(open_prefer_qty)
                qty  = max(need, min(q0, cap))
                if qty < need - 1e-12:
                    qty = None
            else:
                qty = self.compute_order_qty(price)

        if not qty:
            return (None, "minNotional/minQty check failed")

        last_err = None

        def changed_ok() -> bool:
            if self.dual_side:
                if reduce_only:
                    if (position_side or "").upper() == "LONG":
                        return self.long_pos <= max(before_long - 1e-12, 0.0)
                    else:
                        return self.short_pos <= max(before_short - 1e-12, 0.0)
                else:
                    if (position_side or "").upper() == "LONG":
                        return self.long_pos > before_long + 1e-12
                    else:
                        return self.short_pos > before_short + 1e-12
            else:
                if reduce_only:
                    return abs(self.net_pos) <= max(abs(before_net) - 1e-12, 0.0)
                else:
                    return (self.net_pos > before_net + 1e-12) if side == "BUY" else (self.net_pos < before_net - 1e-12)

        for attempt in range(8):
            oid, err = await self.place_market(
                side,
                qty,
                reduce_only=reduce_only,
                position_side=position_side
            )

            await self.fetch_account()
            if oid and changed_ok():
                return (oid, None)

            last_err = (err or "unknown error")

            if reduce_only:
                break

            need = self.min_qty_for_notional(price)
            cap  = self.max_affordable_qty(price)
            if cap < need - 1e-12:
                qty = None
                break

            next_q = min(self._round_qty_floor(qty * 0.85), cap)
            if next_q < need:
                next_q = need
            if abs(next_q - qty) < (self.step or 1e-12):
                break
            qty = next_q
            await asyncio.sleep(0.12 * (attempt + 1))

        return (None, last_err or "failed after retries")

# ---------- 1-second aggregator ----------
class SecAggregator:
    def __init__(self):
        self.sec: Optional[int] = None
        self.o: Optional[float] = None
        self.h: Optional[float] = None
        self.l: Optional[float] = None
        self.c: Optional[float] = None
        self.v: float = 0.0

    def add_trade(self, price: float, qty: float, ts_ms: int) -> Optional[Dict[str, float]]:
        s = ts_ms // 1000
        closed: Optional[Dict[str, float]] = None
        if self.sec is None:
            self.sec = s; self.o=self.h=self.l=self.c=price; self.v=qty
            return None
        if s == self.sec:
            self.h = max(self.h, price)
            self.l = min(self.l, price)
            self.c = price
            self.v += qty
            return None
        closed = {"t": float(self.sec*1000), "o": self.o, "h": self.h, "l": self.l, "c": self.c, "v": self.v}
        self.sec = s; self.o=self.h=self.l=self.c=price; self.v=qty
        return closed

# ---------- EMA Touch Runner ----------
class EMARunner:
    def __init__(self, symbol: str, interval: str, leverage: int,
                 mid_filter: bool, mode: str, on_close_only: bool,
                 http: httpx.AsyncClient):
        self.symbol = symbol.upper()
        self.interval = interval
        self.target_leverage = max(1, min(leverage, LEVERAGE_CAP))
        self.mid_filter = bool(mid_filter)
        self.mode = "LONG" if str(mode).upper().startswith("LONG") else "BOTH"
        self.on_close_only = bool(on_close_only)
        self.http = http

        self.c = FuturesClient(self.symbol, http)

        self.closes: List[float] = []
        self.ema50: Optional[float]  = None
        self.ema100: Optional[float] = None
        self.ema200: Optional[float] = None

        self.prev_tick_price: Optional[float] = None

        self.running = False
        self._tasks: List[asyncio.Task] = []

        # UI
        self.last_signal: Optional[str] = None
        self.last_action: Optional[str] = None
        # lastActionTs можно оставить в статусе, если нужно
        self.last_action_ts: Optional[int] = None
        self.last_error: Optional[str] = None
        self.source: str = "kline"  # "kline" | "aggTrade_1s"
        self.bot_state: str = "Waiting"  # Waiting / LONG / SHORT

        # one-shot per bar per EMA
        self.last_bar_key: Optional[str] = None
        self.fired_50_this_bar: bool = False
        self.fired_200_this_bar: bool = False

        # PnL today (realized only)
        self.pnl_today_realized: float = 0.0

    # ---------- WARMUP ----------
    async def _warmup_native(self):
        try:
            r = await _get_with_backoff(
                self.http, f"{BINANCE_API}/fapi/v1/klines",
                params={"symbol": self.symbol, "interval": self.interval, "limit": WARMUP_LIMIT}
            )
            data = r.json()
            closes = [float(x[4]) for x in data]
            if not closes:
                return
            self.closes = closes
            e50 = tv_ema_series(closes, 50)
            e100= tv_ema_series(closes, 100)
            e200= tv_ema_series(closes, 200)
            self.ema50  = e50[-1]
            self.ema100 = e100[-1]
            self.ema200 = e200[-1]
        except Exception as e:
            self.last_error = f"warmup_native: {e}"

    async def _warmup_1s(self):
        try:
            r = await _get_with_backoff(
                self.http, f"{BINANCE_API}/fapi/v1/aggTrades",
                params={"symbol": self.symbol, "limit": 1000}
            )
        except Exception as e:
            self.last_error = f"warmup_1s: {e}"
            return
        trades = r.json()
        aggr: Dict[int, Dict[str, float]] = {}
        for t in trades:
            ts = int(t["T"]) // 1000
            p  = float(t["p"])
            q  = float(t["q"])
            if ts not in aggr:
                aggr[ts] = {"o": p, "h": p, "l": p, "c": p, "v": q}
            else:
                aggr[ts]["h"] = max(aggr[ts]["h"], p)
                aggr[ts]["l"] = min(aggr[ts]["l"], p)
                aggr[ts]["c"] = p
                aggr[ts]["v"] += q
        if not aggr:
            return
        closes = [aggr[s]["c"] for s in sorted(aggr.keys())]
        self.closes = closes
        e50 = tv_ema_series(closes, 50)
        e100= tv_ema_series(closes, 100)
        e200= tv_ema_series(closes, 200)
        self.ema50  = e50[-1]
        self.ema100 = e100[-1]
        self.ema200 = e200[-1]

    # ---------- EMA update only on bar close ----------
    async def _on_bar_close(self, close_price: float, bar_key: str):
        self.closes.append(close_price)
        if len(self.closes) > WARMUP_LIMIT:
            self.closes = self.closes[-WARMUP_LIMIT:]

        if self.ema50 is None:
            self.ema50 = tv_ema_series(self.closes, 50)[-1]
        else:
            self.ema50 = tv_ema_next(self.ema50, close_price, 50)

        if self.ema100 is None:
            self.ema100 = tv_ema_series(self.closes, 100)[-1]
        else:
            self.ema100 = tv_ema_next(self.ema100, close_price, 100)

        if self.ema200 is None:
            self.ema200 = tv_ema_series(self.closes, 200)[-1]
        else:
            self.ema200 = tv_ema_next(self.ema200, close_price, 200)

        self.last_bar_key = bar_key
        self.fired_50_this_bar = False
        self.fired_200_this_bar = False

    # ---------- Touch detector (интрабар) ----------
    def _touch_signal_on_price(self, price: float, bar_key: Optional[str]) -> Optional[str]:
        if self.ema50 is None or self.ema200 is None:
            return None
        prev = self.prev_tick_price if self.prev_tick_price is not None else price
        self.prev_tick_price = price

        sig: Optional[str] = None
        same_bar = (bar_key is not None and bar_key == self.last_bar_key)

        if not (same_bar and self.fired_50_this_bar):
            up   = prev < self.ema50 <= price
            down = prev > self.ema50 >= price
            if up or down:
                sig = "LONG" if up else "SHORT"
                self.fired_50_this_bar = True

        if sig is None and not (same_bar and self.fired_200_this_bar):
            up   = prev < self.ema200 <= price
            down = prev > self.ema200 >= price
            if up or down:
                sig = "LONG" if up else "SHORT"
                self.fired_200_this_bar = True

        if sig and self.mid_filter and self.ema200:
            gap = abs((self.ema50 or 0) - self.ema200) / max(1e-12, abs(self.ema200))
            if gap < MID_GAP_THRESHOLD:
                sig = None

        if sig == "SHORT" and self.mode == "LONG":
            return "SHORT_EXIT_ONLY"
        return sig

    # ---------- Локальное обновление статуса ----------
    def _refresh_state_nowait(self):
        try:
            min_qty = max(self.c.min_qty, 0.0)
            if self.c.dual_side:
                if self.c.long_pos > min_qty:
                    self.bot_state = "LONG"
                elif self.c.short_pos > min_qty:
                    self.bot_state = "SHORT"
                else:
                    self.bot_state = "Waiting"
            else:
                pos = self.c.net_pos
                self.bot_state = "Waiting" if abs(pos) < 1e-12 else ("LONG" if pos > 0 else "SHORT")
        except Exception:
            self.bot_state = "Waiting"

    # ---------- ДЕЙСТВИЯ ПО СИГНАЛУ (flip-логика) ----------
    async def _act_on_signal(self, sig: Optional[str], ref_price: float):
        try:
            await self.c.fetch_account()
            await self.c.ensure_leverage(self.target_leverage)

            min_qty = max(self.c.min_qty, 0.0)
            if self.c.dual_side:
                if sig == "LONG":
                    if self.c.long_pos > min_qty:
                        self.last_action = "HOLD: already LONG"; self.last_action_ts = now_ms()
                        self._refresh_state_nowait()
                    else:
                        closed_qty = 0.0
                        if self.c.short_pos > min_qty:
                            q = self.c._round_qty_floor(self.c.short_pos)
                            _, err = await self.c.place_market_smart(
                                "BUY", ref_price, reduce_only=True, position_side="SHORT", qty_override=q
                            )
                            await self.c.fetch_account()
                            if err:
                                self.last_action = f"EXIT SHORT FAILED ({err})"; self.last_action_ts = now_ms(); return
                            self.last_action = "EXIT SHORT ok"; self.last_action_ts = now_ms()
                            closed_qty = q
                            await asyncio.sleep(0.25)
                            await self.c.fetch_account()

                        _, err = await self.c.place_market_smart(
                            "BUY", ref_price, reduce_only=False, position_side="LONG",
                            open_prefer_qty=(closed_qty if closed_qty>0 else None)
                        )
                        await self.c.fetch_account()
                        self.last_action = "ENTER LONG ok" if err is None and self.c.long_pos > min_qty else f"ENTER LONG FAILED ({err})"
                        self.last_action_ts = now_ms()

                elif sig == "SHORT":
                    if self.c.short_pos > min_qty:
                        self.last_action = "HOLD: already SHORT"; self.last_action_ts = now_ms()
                        self._refresh_state_nowait()
                    else:
                        closed_qty = 0.0
                        if self.c.long_pos > min_qty:
                            q = self.c._round_qty_floor(self.c.long_pos)
                            _, err = await self.c.place_market_smart(
                                "SELL", ref_price, reduce_only=True, position_side="LONG", qty_override=q
                            )
                            await self.c.fetch_account()
                            if err:
                                self.last_action = f"EXIT LONG FAILED ({err})"; self.last_action_ts = now_ms(); return
                            self.last_action = "EXIT LONG ok"; self.last_action_ts = now_ms()
                            closed_qty = q
                            await asyncio.sleep(0.25)
                            await self.c.fetch_account()

                        _, err = await self.c.place_market_smart(
                            "SELL", ref_price, reduce_only=False, position_side="SHORT",
                            open_prefer_qty=(closed_qty if closed_qty>0 else None)
                        )
                        await self.c.fetch_account()
                        self.last_action = "ENTER SHORT ok" if err is None and self.c.short_pos > min_qty else f"ENTER SHORT FAILED ({err})"
                        self.last_action_ts = now_ms()

                elif sig == "SHORT_EXIT_ONLY":
                    if self.c.long_pos > min_qty:
                        q = self.c._round_qty_floor(self.c.long_pos)
                        _, err = await self.c.place_market_smart(
                            "SELL", ref_price, reduce_only=True, position_side="LONG", qty_override=q
                        )
                        await self.c.fetch_account()
                        self.last_action = "EXIT LONG ok" if err is None and self.c.long_pos <= 1e-12 else f"EXIT LONG FAILED ({err})"
                        self.last_action_ts = now_ms()

            else:
                pos = self.c.net_pos
                if sig == "LONG":
                    if pos > min_qty:
                        self.last_action = "HOLD: already LONG"; self.last_action_ts = now_ms()
                        self._refresh_state_nowait()
                    else:
                        closed_qty = 0.0
                        if pos < -min_qty:
                            q = self.c._round_qty_floor(abs(pos))
                            _, err = await self.c.place_market_smart("BUY", ref_price, reduce_only=True, qty_override=q)
                            await self.c.fetch_account()
                            if err:
                                self.last_action = f"EXIT SHORT FAILED ({err})"; self.last_action_ts = now_ms(); return
                            self.last_action = "EXIT SHORT ok"; self.last_action_ts = now_ms()
                            closed_qty = q
                            await asyncio.sleep(0.25)
                            await self.c.fetch_account()

                        _, err = await self.c.place_market_smart(
                            "BUY", ref_price, reduce_only=False, open_prefer_qty=(closed_qty if closed_qty>0 else None)
                        )
                        await self.c.fetch_account()
                        self.last_action = "ENTER LONG ok" if err is None and self.c.net_pos > min_qty else f"ENTER LONG FAILED ({err})"
                        self.last_action_ts = now_ms()

                elif sig == "SHORT":
                    if pos < -min_qty:
                        self.last_action = "HOLD: already SHORT"; self.last_action_ts = now_ms()
                        self._refresh_state_nowait()
                    else:
                        closed_qty = 0.0
                        if pos > min_qty:
                            q = self.c._round_qty_floor(abs(pos))
                            _, err = await self.c.place_market_smart("SELL", ref_price, reduce_only=True, qty_override=q)
                            await self.c.fetch_account()
                            if err:
                                self.last_action = f"EXIT LONG FAILED ({err})"; self.last_action_ts = now_ms(); return
                            self.last_action = "EXIT LONG ok"; self.last_action_ts = now_ms()
                            closed_qty = q
                            await asyncio.sleep(0.25)
                            await self.c.fetch_account()

                        _, err = await self.c.place_market_smart(
                            "SELL", ref_price, reduce_only=False, open_prefer_qty=(closed_qty if closed_qty>0 else None)
                        )
                        await self.c.fetch_account()
                        self.last_action = "ENTER SHORT ok" if err is None and self.c.net_pos < -min_qty else f"ENTER SHORT FAILED ({err})"
                        self.last_action_ts = now_ms()

                elif sig == "SHORT_EXIT_ONLY":
                    if pos > min_qty:
                        q = self.c._round_qty_floor(abs(pos))
                        _, err = await self.c.place_market_smart("SELL", ref_price, reduce_only=True, qty_override=q)
                        await self.c.fetch_account()
                        self.last_action = "EXIT LONG ok" if err is None and self.c.net_pos <= 1e-12 else f"EXIT LONG FAILED ({err})"
                        self.last_action_ts = now_ms()

            await self.c.fetch_account()
            self._refresh_state_nowait()

        except Exception as e:
            self.last_error = f"act: {e}"

    async def _time_sync_loop(self):
        while self.running:
            await self.c.sync_time()
            await asyncio.sleep(TIME_SYNC_SEC)

    async def _kline_loop(self):
        url = f"{WS_PUBLIC}/{self.symbol.lower()}@kline_{self.interval}"
        self.source = "kline"
        while self.running:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    async for raw in ws:
                        if not self.running: break
                        try:
                            e = json.loads(raw)
                            k = e.get("k") or {}
                            close = float(k.get("c"))
                            is_closed = bool(k.get("x", False))
                            bar_key = f"{k.get('t')}_{k.get('T')}"

                            if is_closed:
                                await self._on_bar_close(close, bar_key)

                            sig: Optional[str] = None
                            if self.on_close_only:
                                if is_closed:
                                    sig = self._touch_signal_on_price(close, bar_key)
                            else:
                                sig = self._touch_signal_on_price(close, bar_key if not is_closed else None)

                            self.last_signal = sig
                            if sig:
                                await self._act_on_signal(sig, close)

                        except Exception as ex:
                            self.last_error = f"kline parse: {ex}"
            except Exception as ex:
                self.last_error = f"ws kline: {ex}"
                await asyncio.sleep(1.0)

    async def _aggtrade_1s_loop(self):
        url = f"{WS_PUBLIC}/{self.symbol.lower()}@aggTrade"
        self.source = "aggTrade_1s"
        ag = SecAggregator()
        while self.running:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    async for raw in ws:
                        if not self.running: break
                        try:
                            e = json.loads(raw)
                            p = float(e["p"]); q = float(e["q"]); ts = int(e["T"])
                            closed = ag.add_trade(p, q, ts)
                            if closed:
                                c = float(closed["c"])
                                bar_key = str(int(closed["t"]))
                                await self._on_bar_close(c, bar_key)
                                sig = self._touch_signal_on_price(c, None if self.on_close_only else bar_key)
                                self.last_signal = sig
                                if sig:
                                    await self._act_on_signal(sig, c)
                        except Exception as ex:
                            self.last_error = f"agg parse: {ex}"
            except Exception as ex:
                self.last_error = f"ws agg: {ex}"
                await asyncio.sleep(1.0)

    async def run(self):
        self.running = True
        try:
            await self.c.sync_time()
            await self.c.fetch_filters()
            await self.c.fetch_dual_side()
            await self.c.fetch_account()
            await self.c.ensure_leverage(self.target_leverage)
            if self.interval == "1s":
                await self._warmup_1s()
            else:
                await self._warmup_native()
            self._refresh_state_nowait()
        except Exception as e:
            self.last_error = f"init: {e}"

        self._tasks = [asyncio.create_task(self._time_sync_loop())]
        if self.interval == "1s":
            self._tasks.append(asyncio.create_task(self._aggtrade_1s_loop()))
        else:
            self._tasks.append(asyncio.create_task(self._kline_loop()))

    async def stop(self):
        self.running = False
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

    def status(self) -> dict:
        return {
            "symbol": self.symbol,
            "interval": self.interval,
            "source": self.source,
            "leverage": self.target_leverage,
            "hedgeMode": self.c.dual_side,
            "availableUSDT": self.c.available_usdt,
            "netPos": self.c.net_pos,
            "entryPx": self.c.entry_px,
            "ema50": self.ema50,
            "ema100": self.ema100,
            "ema200": self.ema200,
            "midFilter": self.mid_filter,
            "mode": self.mode,
            "onCloseOnly": self.on_close_only,
            "botState": self.bot_state,
            "lastSignal": self.last_signal,
            "lastAction": self.last_action,
            "lastActionTs": self.last_action_ts,
            "minNotional": self.c.min_notional,
            "minQty": self.c.min_qty,
            "pnlToday": self.pnl_today_realized,
            "lastError": self.last_error
        }

# ---------- Manager ----------
class Manager:
    def __init__(self):
        self.http = httpx.AsyncClient(timeout=HTTP_TIMEOUT)
        self.runners: Dict[str, EMARunner] = {}
        self.running = False
        self.cur_interval = DEFAULT_INTERVAL
        self.cur_leverage = DEFAULT_LEVERAGE
        self.cur_mid_filter = False
        self.cur_mode = DEFAULT_MODE
        self.cur_on_close_only = DEFAULT_ON_CLOSE_ONLY
        self._tasks: List[asyncio.Task] = []
        # reconcile/external
        self.dual_side: bool = False
        self.external: Dict[str, Dict[str, Any]] = {}

        # --- Автопик ---
        self.auto_pick: bool = False
        self.auto_current_symbol: Optional[str] = None
        self.auto_current_pct: Optional[float] = None

        # --- КЭШ валидных USDT-M PERPETUAL символов (обновляется раз в 5 минут)
        self._valid_syms_cache: set[str] = set()
        self._valid_syms_cached_at_ms: int = 0
        self._valid_syms_ttl_ms: int = 5 * 60 * 1000

    async def _valid_usdtm_perp_symbols(self) -> set:
        """Список реально торгуемых USDT-M PERPETUAL символов на фьючерсах Binance."""
        now = now_ms()
        if self._valid_syms_cache and (now - self._valid_syms_cached_at_ms) < self._valid_syms_ttl_ms:
            return self._valid_syms_cache
        try:
            r = await _get_with_backoff(self.http, f"{BINANCE_API}/fapi/v1/exchangeInfo")
            data = r.json()
            valid = {
                s["symbol"]
                for s in data.get("symbols", [])
                if s.get("quoteAsset") == "USDT"
                and s.get("contractType") == "PERPETUAL"
                and s.get("status") == "TRADING"
            }
            if valid:
                self._valid_syms_cache = valid
                self._valid_syms_cached_at_ms = now
                return valid
        except Exception:
            pass
        return self._valid_syms_cache

    async def pick_top1_symbol(self) -> Tuple[str, float]:
        """
        Берём максимум priceChangePercent среди РЕАЛЬНО доступных USDT-M PERPETUAL.
        Если ничего не нашли/ошибка — фолбэк AUTO_FALLBACK_SYMBOL.
        """
        try:
            valid = await self._valid_usdtm_perp_symbols()
            if not valid:
                return (AUTO_FALLBACK_SYMBOL, 0.0)

            r = await _get_with_backoff(self.http, f"{BINANCE_API}/fapi/v1/ticker/24hr")
            arr = r.json()

            best_sym = None
            best_pct = None
            for it in arr:
                sym = (it.get("symbol") or "").upper()
                if sym not in valid:
                    continue
                try:
                    pct = float(it.get("priceChangePercent") or 0.0)
                except Exception:
                    continue
                if (best_pct is None) or (pct > best_pct):
                    best_sym, best_pct = sym, pct

            if best_sym:
                return best_sym, float(best_pct or 0.0)

        except Exception:
            pass

        return (AUTO_FALLBACK_SYMBOL, 0.0)

    async def start(self, symbols: List[str], interval: str, leverage: int,
                    mid_filter: bool, mode: str, on_close_only: bool):
        if self.running:
            await self.stop()
        if interval not in ALL_INTERVALS:
            raise ValueError(f"Unsupported interval: {interval}")
        self.cur_interval = interval
        self.cur_leverage = max(1, min(int(leverage), LEVERAGE_CAP))
        self.cur_mid_filter = bool(mid_filter)
        self.cur_mode = "LONG" if str(mode).upper().startswith("LONG") else "BOTH"
        self.cur_on_close_only = bool(on_close_only)

        # --- обработка auto-pick ---
        syms_clean = [s.strip().upper() for s in (symbols or []) if s and s.strip()]
        self.auto_pick = (len(syms_clean) == 0)
        if self.auto_pick:
            top_sym, top_pct = await self.pick_top1_symbol()
            self.auto_current_symbol = top_sym
            self.auto_current_pct = top_pct
            syms_clean = [top_sym]
        else:
            self.auto_current_symbol = None
            self.auto_current_pct = None

        self.runners.clear()
        for s in syms_clean:
            r = EMARunner(s, self.cur_interval, self.cur_leverage,
                          self.cur_mid_filter, self.cur_mode, self.cur_on_close_only, self.http)
            self.runners[s] = r
            try:
                await r.run()
            except Exception as e:
                r.last_error = f"start: {e}"
        self.running = True
        self._tasks.append(asyncio.create_task(self._income_loop()))
        self._tasks.append(asyncio.create_task(self._reconcile_loop()))
        self._tasks.append(asyncio.create_task(self._auto_pick_loop()))

    async def stop(self):
        # закрываем позиции у воркеров
        for r in list(self.runners.values()):
            try:
                await r.c.fetch_account()
                if r.c.dual_side:
                    if r.c.long_pos > r.c.min_qty:
                        q = r.c._round_qty_floor(r.c.long_pos)
                        await r.c.place_market_smart(
                            "SELL",
                            r.c.entry_px or (await r.c.fetch_last_price() or 0.0),
                            reduce_only=True,
                            position_side="LONG",
                            qty_override=q
                        )
                    if r.c.short_pos > r.c.min_qty:
                        q = r.c._round_qty_floor(r.c.short_pos)
                        await r.c.place_market_smart(
                            "BUY",
                            r.c.entry_px or (await r.c.fetch_last_price() or 0.0),
                            reduce_only=True,
                            position_side="SHORT",
                            qty_override=q
                        )
                else:
                    pos = r.c.net_pos
                    if abs(pos) >= max(r.c.min_qty, 0):
                        q = r.c._round_qty_floor(abs(pos))
                        side = "SELL" if pos > 0 else "BUY"
                        ref_px = r.c.entry_px or (await r.c.fetch_last_price() or 0.0)
                        await r.c.place_market_smart(side, ref_px, reduce_only=True, qty_override=q)
                        await r.c.fetch_account()
            except Exception as e:
                print("stop close error:", e)
        for r in list(self.runners.values()):
            await r.stop()
        self.runners.clear()
        self.external.clear()
        self.running = False
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

    async def restart(self, symbols: List[str], interval: str, leverage: int,
                      mid_filter: bool, mode: str, on_close_only: bool):
        await self.stop()
        await self.start(symbols, interval, leverage, mid_filter, mode, on_close_only)

    async def _income_loop(self):
        while self.running:
            try:
                await self.refresh_income()
            except Exception as e:
                print("income loop:", e)
            await asyncio.sleep(60)

    async def refresh_income(self):
        """
        Считаем только реальный PnL за СЕГОДНЯ (UTC), по каждому активному символу.
        Используем /fapi/v1/income с incomeType=REALIZED_PNL и пагинацией по startTime.
        """
        end_ms = now_ms() + TIME_DRIFT_MS  # серверное время
        utc_now = dt.datetime.fromtimestamp(end_ms/1000.0, tz=dt.timezone.utc)
        today0 = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_ms = int(today0.timestamp()*1000)

        for sym, r in self.runners.items():
            try:
                total = 0.0
                start = today_start_ms
                last_seen_time = -1
                while start <= end_ms:
                    params = {
                        "timestamp": end_ms,
                        "recvWindow": 5000,
                        "startTime": start,
                        "endTime": end_ms,
                        "symbol": sym,
                        "incomeType": "REALIZED_PNL",
                        "limit": 1000
                    }
                    resp = await _signed_get_with_backoff(self.http, f"{BINANCE_API}/fapi/v1/income", params=params)
                    arr = resp.json()
                    if not isinstance(arr, list) or not arr:
                        break

                    max_t = start
                    for it in arr:
                        t = int(it.get("time", 0))
                        typ = (it.get("incomeType") or "").upper()
                        if t >= today_start_ms and typ == "REALIZED_PNL":
                            total += float(it.get("income", "0"))
                        if t > max_t:
                            max_t = t

                    if max_t <= last_seen_time:
                        break
                    last_seen_time = max_t

                    if len(arr) < 1000:
                        break
                    start = max_t + 1

                r.pnl_today_realized = total

            except Exception as e:
                r.last_error = f"income: {e}"

    async def _reconcile_loop(self):
        """Синхронизируемся с биржей и показываем внешние позиции."""
        while self.running:
            try:
                ts = now_ms() + TIME_DRIFT_MS
                # dualSide
                try:
                    rds = await _signed_get_with_backoff(self.http, f"{BINANCE_API}/fapi/v1/positionSide/dual",
                                                         params={"timestamp": ts, "recvWindow": 5000})
                    self.dual_side = bool(rds.json().get("dualSidePosition", False))
                except Exception:
                    pass

                # account snapshot
                qp = _sign({"timestamp": ts, "recvWindow": 5000})
                acc_r = await self.http.get(f"{BINANCE_API}/fapi/v2/account", params=qp, headers={"X-MBX-APIKEY": API_KEY})
                acc_r.raise_for_status()
                acc = acc_r.json()

                avail_usdt = 0.0
                for a in acc.get("assets", []):
                    if a.get("asset") == "USDT":
                        avail_usdt = float(a.get("availableBalance", "0"))
                        break

                # внешние позиции (символы, которых нет среди активных воркеров)
                ext: Dict[str, Dict[str, Any]] = {}
                tmp: Dict[str, Dict[str, float]] = {}
                for p in acc.get("positions", []):
                    sym = p.get("symbol", "")
                    if sym in self.runners:
                        continue
                    side = p.get("positionSide", "BOTH")
                    amt  = float(p.get("positionAmt", "0"))
                    ent  = float(p.get("entryPrice", "0"))
                    d = tmp.get(sym) or {"net":0.0, "entry":0.0, "long":0.0, "short":0.0, "long_entry":0.0, "short_entry":0.0}
                    if side == "BOTH":
                        d["net"] = amt; d["entry"] = ent
                    elif side == "LONG":
                        d["long"] = amt; d["long_entry"] = ent
                    elif side == "SHORT":
                        d["short"] = abs(amt); d["short_entry"] = ent
                    tmp[sym] = d

                for sym, d in tmp.items():
                    has_pos = abs(d["net"])>1e-12 or d["long"]>1e-12 or d["short"]>1e-12
                    if not has_pos: continue
                    if self.dual_side and (d["long"]>1e-12 or d["short"]>1e-12):
                        state = "External HEDGE" if (d["long"]>1e-12 and d["short"]>1e-12) else ("External LONG" if d["long"]>1e-12 else "External SHORT")
                        pos_text = f"L:{d['long']:.6f} / S:{d['short']:.6f}"
                        entry_text = f"L:{d['long_entry']:.6f} / S:{d['short_entry']:.6f}"
                    else:
                        state = "External LONG" if d["net"]>0 else "External SHORT"
                        pos_text = f"{d['net']:.6f}"
                        entry_text = f"{d['entry']:.6f}"

                    ext[sym] = {
                        "symbol": sym, "interval": self.cur_interval, "source": "external",
                        "leverage": self.cur_leverage, "mode": "-", "midFilter": False, "lastSignal": None,
                        "botState": state, "availableUSDT": avail_usdt,
                        "netPos": pos_text, "entryPx": entry_text,
                        "ema50": None, "ema100": None, "ema200": None,
                        "pnlToday": 0.0,
                        "lastAction": "External position", "lastError": None
                    }
                self.external = ext

                # актуализируем аккаунт у активных воркеров + обновляем их bot_state
                for r in self.runners.values():
                    try:
                        await r.c.fetch_account()
                        r._refresh_state_nowait()
                    except Exception:
                        pass

            except Exception as e:
                print("reconcile loop:", e)
            await asyncio.sleep(3)

    async def _auto_pick_loop(self):
        """Если включён автопик (поле Symbols пустое при старте):
           - каждые 30 сек берём Топ-1 по 24h % среди валидных USDT-M PERP.
           - если у бота НЕТ открытых позиций (по всем воркерам) и Топ-1 изменился — перезапускаем на новый символ.
        """
        while self.running:
            try:
                if self.auto_pick:
                    # Есть ли открытые позиции? Если есть — ничего не меняем.
                    any_open = False
                    for r in self.runners.values():
                        try:
                            await r.c.fetch_account()
                            minq = max(r.c.min_qty, 0.0)
                            if r.c.dual_side:
                                if r.c.long_pos > minq or r.c.short_pos > minq:
                                    any_open = True
                                    break
                            else:
                                if abs(r.c.net_pos) > minq:
                                    any_open = True
                                    break
                        except Exception:
                            pass

                    if not any_open:
                        top_sym, top_pct = await self.pick_top1_symbol()
                        if top_sym and (top_sym not in self.runners):
                            # перезапуск на новый Топ-1
                            await self.restart([top_sym], self.cur_interval, self.cur_leverage,
                                               self.cur_mid_filter, self.cur_mode, self.cur_on_close_only)
                            self.auto_pick = True
                            self.auto_current_symbol = top_sym
                            self.auto_current_pct = top_pct
                            # после restart этот loop перезапустится заново внутри start()
                            return
                        else:
                            self.auto_current_symbol = top_sym
                            self.auto_current_pct = top_pct
            except Exception as e:
                print("auto-pick loop:", e)
            await asyncio.sleep(30)

    def status(self):
        return {
            "running": self.running,
            "params": {
                "interval": self.cur_interval,
                "leverage": self.cur_leverage,
                "mid_filter": self.cur_mid_filter,
                "mode": self.cur_mode,
                "on_close_only": self.cur_on_close_only
            },
            "workers": [r.status() for r in self.runners.values()],
            "external": list(self.external.values()),
            "auto": {
                "enabled": self.auto_pick,
                "currentTop": self.auto_current_symbol,
                "currentTopPct": self.auto_current_pct
            }
        }

MANAGER = Manager()

# ---------- UI ----------
TF_OPTIONS = "".join([f"<option{' selected' if tf==DEFAULT_INTERVAL else ''}>{tf}</option>" for tf in ALL_INTERVALS])

HTML_TEMPLATE = """
<!doctype html><html><head><meta charset="utf-8"/>
<title>Volontyr — Bot</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
:root {
  --bg:#0f172a; --card:#111827; --muted:#94a3b8; --text:#e5e7eb; --border:#1f2937;
  --green:#22c55e; --red:#ef4444; --blue:#3b82f6; --amber:#f59e0b; --gray:#6b7280;
}
* { box-sizing:border-box; }
body { margin:0; background:linear-gradient(180deg,#0b1220 0%,#0f172a 100%); color:var(--text); font-family: ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Arial; }
.container { max-width:1200px; margin:24px auto; padding:0 16px; }
h2 { font-weight:600; letter-spacing:.2px; margin:8px 0 16px; }
.card { background:var(--card); border:1px solid var(--border); border-radius:16px; padding:16px; box-shadow:0 8px 30px rgba(0,0,0,.25); }
.row { display:flex; flex-wrap:wrap; gap:12px; align-items:center; margin:10px 0; }
label.title { color:var(--muted); font-size:13px; margin-right:8px; }
input[type=text], select, input[type=number] { background:#0b1020; color:var(--text); border:1px solid var(--border); border-radius:10px; padding:10px 12px; }
button.primary { background:linear-gradient(90deg,#2563eb,#3b82f6); color:#fff; border:none; border-radius:12px; padding:10px 14px; font-weight:600; cursor:pointer; box-shadow:0 6px 18px rgba(59,130,246,.35); }
button.secondary { background:#111827; color:#e5e7eb; border:1px solid var(--border); border-radius:12px; padding:10px 14px; }
button.danger { background:linear-gradient(90deg,#ef4444,#f43f5e); color:#fff; border:none; border-radius:12px; padding:10px 14px; }
.pill { display:inline-flex; align-items:center; gap:8px; padding:6px 10px; border-radius:999px; font-size:12px; }
.on { background:rgba(34,197,94,.15); color:#86efac; border:1px solid rgba(34,197,94,.4); }
.off{ background:rgba(239,68,68,.15); color:#fca5a5; border:1px solid rgba(239,68,68,.35); }

.table { width:100%; border-collapse:separate; border-spacing:0 8px; }
.table thead th { text-align:left; font-size:12px; color:var(--muted); font-weight:500; padding:8px 10px; }
.table tbody tr { background:#0b1020; border:1px solid var(--border); border-radius:12px; overflow:hidden; }
.table tbody td { padding:10px; border-bottom:1px solid var(--border); font-size:14px; }
.table tbody tr:last-child td { border-bottom:none; }

.status-badge { padding:4px 10px; border-radius:999px; font-size:12px; font-weight:600; }
.waiting { background:#1f2937; color:#cbd5e1; }
.long    { background:rgba(34,197,94,.2); color:#86efac; }
.short   { background:rgba(239,68,68,.2); color:#fca5a5; }

/* iOS-style switches */
.switch { position:relative; width:54px; height:30px; background:#374151; border-radius:999px; transition:background .2s ease; border:1px solid #4b5563; cursor:pointer; }
.switch input { display:none; }
.switch .thumb { position:absolute; top:2px; left:3px; width:24px; height:24px; background:#fff; border-radius:50%; transition:left .2s ease; box-shadow: 0 2px 8px rgba(0,0,0,.35); }
.switch.on { background:#16a34a; border-color:#16a34a; }
.switch.on .thumb { left:27px; }

.grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(220px,1fr)); gap:12px; }
.small { color:var(--muted); font-size:12px; }
.kpi { display:flex; gap:8px; align-items:center; }
.kpi b { font-size:16px; }
</style>
</head>
<body>
<div class="container">
  <h2>EMA Touch — Binance Futures</h2>
  <div class="card">
    <div class="grid">
      <div>
        <label class="title">Symbols (comma)</label><br/>
        <input id="sym" type="text" value="" placeholder="choose a coin"/>
        <div class="small">Оставь пустым — бот выберет Топ-1 24h % автоматически (USDT-M PERP). Фолбэк: CYBERUSDT.</div>
      </div>
      <div>
        <label class="title">Timeframe</label><br/>
        <select id="tf">%%TF_OPTIONS%%</select>
      </div>
      <div>
        <label class="title">Leverage</label><br/>
        <input id="lev" type="number" value="1" min="1" max="50" step="1"/>
      </div>
      <div>
        <label class="title">Mode</label><br/>
        <select id="mode">
          <option selected>Both</option>
          <option>Long only</option>
        </select>
      </div>
      <div>
        <label class="title">Mid filter</label><br/>
        <div id="mid" class="switch on"><input type="checkbox"/><div class="thumb"></div></div>
      </div>
      <div>
        <label class="title">Signal: Close-only</label><br/>
        <div id="closeonly" class="switch"><input type="checkbox"/><div class="thumb"></div></div>
        <div class="small">OFF = Touch (intrabar)</div>
      </div>
    </div>

    <div class="row" style="margin-top:12px;">
      <button id="btnStart" class="primary" onclick="start()">Start</button>
      <button class="danger"  onclick="stop()">Stop</button>
      <button class="secondary" onclick="restart()">Restart</button>
      <span id="flag" class="pill off">Stopped</span>
    </div>
  </div>

  <div style="height:16px"></div>

  <div class="card">
    <table class="table">
      <thead>
        <tr>
          <th>Symbol</th><th>TF</th><th>Source</th><th>Lev</th><th>Mode</th><th>Mid</th><th>Signal</th><th>Bot</th>
          <th>Avail USDT</th><th>Pos</th><th>Entry</th>
          <th>EMA50</th><th>EMA100</th><th>EMA200</th>
          <th>PnL Today (R)</th><th>Last Action</th><th>Error</th>
        </tr>
      </thead>
      <tbody id="tbody"></tbody>
    </table>
    <div class="small" style="margin-top:8px;">
      Реализованный PnL считается за календарные сутки UTC. Положительный — зелёный, отрицательный — красный.
    </div>
  </div>
</div>

<script>
function toggleInit(id){
  const el=document.getElementById(id);
  el.addEventListener('click', ()=>{ el.classList.toggle('on'); });
  return el;
}
const mid = toggleInit('mid');
const closeonly = toggleInit('closeonly');

function symbols(){
  return document.getElementById('sym').value
    .split(',')
    .map(s=>s.trim())
    .filter(Boolean); // если пусто — [] => авто-пик на сервере
}
function params(){
  return {
    interval: document.getElementById('tf').value,
    leverage: parseInt(document.getElementById('lev').value),
    mid_filter: mid.classList.contains('on'),
    on_close_only: closeonly.classList.contains('on'),
    mode: document.getElementById('mode').value
  };
}
async function post(path, body){
  const r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})});
  if(!r.ok){
    let msg = await r.text(); try{ msg = JSON.parse(msg).error || msg; }catch(_){}
    throw new Error(msg);
  }
  return r.json();
}
async function get(path){ const r=await fetch(path); if(!r.ok) throw new Error(await r.text()); return r.json(); }

async function start(){ try{ const p=params(); await post('/api/start',{symbols:symbols(), ...p}); await refresh(); }catch(e){ alert('Start: '+e.message); } }
async function stop(){ try{ await post('/api/stop',{}); await refresh(); }catch(e){ alert('Stop: '+e.message); } }
async function restart(){ try{ const p=params(); await post('/api/restart',{symbols:symbols(), ...p}); await refresh(); }catch(e){ alert('Restart: '+e.message); } }

function td(v){
  const d=document.createElement('td');
  if(v==null) v='';
  if(typeof v==='number'){
    d.textContent = Math.abs(v) >= 0.0001 ? v.toFixed(6) : v.toString();
  } else { d.textContent = v; }
  return d;
}
function stateBadge(state){
  const span=document.createElement('span'); span.className='status-badge';
  if(state==='LONG'){ span.classList.add('long');  span.textContent='LONG'; }
  else if(state==='SHORT'){ span.classList.add('short'); span.textContent='SHORT'; }
  else { span.classList.add('waiting'); span.textContent=state || 'Waiting'; }
  return span;
}
function sigText(sig){
  if(!sig) return '';
  if(sig==='SHORT_EXIT_ONLY') return 'SHORT (exit only)';
  return sig;
}
function pnlCell(v){
  const tdEl=document.createElement('td');
  const b=document.createElement('span');
  b.className='status-badge';
  let num = (typeof v==='number') ? v : parseFloat(v||0) || 0;
  b.textContent = Math.abs(num) >= 0.0001 ? num.toFixed(6) : num.toString();
  if(num > 0) b.classList.add('long');
  else if(num < 0) b.classList.add('short');
  else b.classList.add('waiting');
  tdEl.appendChild(b);
  return tdEl;
}
function pushRow(tb, w){
  const tr=document.createElement('tr');
  tr.appendChild(td(w.symbol));
  tr.appendChild(td(w.interval));
  tr.appendChild(td(w.source||'')); 
  tr.appendChild(td(w.leverage));
  tr.appendChild(td(w.mode||'-'));
  tr.appendChild(td(w.midFilter?'ON':'OFF'));
  tr.appendChild(td(sigText(w.lastSignal)));
  const stateCell = document.createElement('td'); stateCell.appendChild(stateBadge(w.botState)); tr.appendChild(stateCell);
  tr.appendChild(td(w.availableUSDT));
  tr.appendChild(td(w.netPos));
  tr.appendChild(td(w.entryPx));
  tr.appendChild(td(w.ema50));
  tr.appendChild(td(w.ema100));
  tr.appendChild(td(w.ema200));
  tr.appendChild(pnlCell(w.pnlToday));
  tr.appendChild(td(w.lastAction||'')); 
  tr.appendChild(td(w.lastError||''));
  tb.appendChild(tr);
}
async function refresh(){
  const st=await get('/api/status');
  const flag=document.getElementById('flag');
  flag.textContent = st.running?'Running':'Stopped';
  flag.className = 'pill ' + (st.running?'on':'off');

  const tb=document.getElementById('tbody'); tb.innerHTML='';
  (st.workers||[]).forEach(w=> pushRow(tb, w));
  (st.external||[]).forEach(w=> pushRow(tb, w));
}
async function loop(){ try{ await refresh(); const st=await get('/api/status'); setTimeout(loop, st.running?1200:3000);}catch(e){ console.error(e); setTimeout(loop,3000);} }
window.addEventListener('load', loop);
window.addEventListener('keydown', (e)=>{ if(e.key==='Enter'){ document.getElementById('btnStart').click(); } });
</script>
</body></html>
"""

HTML = HTML_TEMPLATE.replace("%%TF_OPTIONS%%", TF_OPTIONS)

# ---------- FastAPI ----------
app = FastAPI()

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/api/status")
async def api_status():
    return JSONResponse(MANAGER.status())

@app.post("/api/start")
async def api_start(req: Request):
    if not API_KEY or not API_SECRET:
        return JSONResponse({"error":"Missing BINANCE_API_KEY/SECRET"}, status_code=400)
    body = await req.json()
    syms = body.get("symbols", [])  # пусто => auto-pick
    interval = str(body.get("interval", DEFAULT_INTERVAL))
    leverage = int(body.get("leverage", DEFAULT_LEVERAGE))
    mid_filter = bool(body.get("mid_filter", False))
    mode = str(body.get("mode", DEFAULT_MODE))
    on_close_only = bool(body.get("on_close_only", DEFAULT_ON_CLOSE_ONLY))
    try:
        await MANAGER.start(syms, interval, leverage, mid_filter, mode, on_close_only)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

@app.post("/api/stop")
async def api_stop():
    await MANAGER.stop()
    return JSONResponse({"ok": True})

@app.post("/api/restart")
async def api_restart(req: Request):
    if not API_KEY or not API_SECRET:
        return JSONResponse({"error":"Missing BINANCE_API_KEY/SECRET"}, status_code=400)
    body = await req.json()
    syms = body.get("symbols", [])  # пусто => auto-pick
    interval = str(body.get("interval", DEFAULT_INTERVAL))
    leverage = int(body.get("leverage", DEFAULT_LEVERAGE))
    mid_filter = bool(body.get("mid_filter", False))
    mode = str(body.get("mode", DEFAULT_MODE))
    on_close_only = bool(body.get("on_close_only", DEFAULT_ON_CLOSE_ONLY))
    try:
        await MANAGER.restart(syms, interval, leverage, mid_filter, mode, on_close_only)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

MANAGER  # keep linter happy

if __name__ == "__main__":
    import uvicorn, os
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)
