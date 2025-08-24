import os, time, math, json, asyncio, random, hmac, hashlib
import datetime as dt
from typing import Optional, Dict, List, Tuple, Any
import httpx, websockets
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

# ---------- Binance Futures (USDT-M) ----------
BINANCE_API = "https://fapi.binance.com"
WS_PUBLIC = "wss://fstream.binance.com/ws"

API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET_RAW = os.getenv("BINANCE_API_SECRET", "")
API_SECRET = API_SECRET_RAW.encode() if API_SECRET_RAW else b""

HTTP_TIMEOUT = 10.0
TIME_DRIFT_MS = 0
TIME_SYNC_SEC = 300

# ---------- Strategy / risk defaults ----------
DEFAULT_INTERVAL = "1m"
DEFAULT_LEVERAGE = 1
LEVERAGE_CAP = 50
UTILIZATION = 0.50
FEE_BUFFER_RATE = 0.001
WARMUP_LIMIT = 1200

# Pine: confirmOnClose = false по умолчанию
DEFAULT_ON_CLOSE_ONLY = False
# Pine: oneSignalPerBar = true
DEFAULT_ONE_SIGNAL_PER_BAR = True
# Pine: Min EMA spread % (по умолчанию 1.0)
DEFAULT_SPREAD_MIN_PCT = 1.0

DEFAULT_MODE = "BOTH"
AUTO_FALLBACK_SYMBOL = "CYBERUSDT"

# === EMA/SL defaults (Pine parity) ===
FAST_LEN_DEFAULT = 50
SLOW_LEN_DEFAULT = 200

DEFAULT_USE_SL    = True
DEFAULT_SL_MODE   = "Percent"   # "Percent" | "ATR"
DEFAULT_SL_PCT    = 10.0        # <= по твоей просьбе 10% по умолчанию
DEFAULT_ATR_LEN   = 14
DEFAULT_ATR_MULT  = 2.0

NATIVE_INTERVALS = {"1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d","3d","1w","1M"}
ALL_INTERVALS = ["1s"] + sorted(
    list(NATIVE_INTERVALS),
    key=lambda x: ["m","h","d","w","M"].index(x[-1]) * 1000 + int(x[:-1])
)

def now_ms() -> int:
    return int(time.time()*1000)

# ---------- Helpers: price / ohlc ----------
def _kline_row_close(row: list) -> float:
    return float(row[4])

def _kline_tick_close(k: dict) -> float:
    return float(k.get("c", 0.0))

def _kline_tick_high(k: dict, fallback: float) -> float:
    try: return float(k.get("h", fallback))
    except: return fallback

def _kline_tick_low(k: dict, fallback: float) -> float:
    try: return float(k.get("l", fallback))
    except: return fallback

# ---------- Backoff helpers ----------
async def _get_with_backoff(http: httpx.AsyncClient, url: str, *, params: Optional[dict]=None, headers: Optional[dict]=None, max_tries: int=6) -> httpx.Response:
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

async def _signed_get_with_backoff(http: httpx.AsyncClient, url: str, *, params: Dict[str,str], max_tries: int=6) -> httpx.Response:
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

# ---------- EMA (как в TradingView: seed = SMA(N), alpha=2/(N+1)) ----------
def tv_ema_series(closes: List[float], length: int) -> List[Optional[float]]:
    n = len(closes)
    out: List[Optional[float]] = [None] * n
    if length < 1 or n == 0 or n < length:
        return out
    alpha = 2.0 / (length + 1.0)
    seed = sum(float(x) for x in closes[:length]) / float(length)
    out[length-1] = float(seed)
    prev = float(seed)
    for i in range(length, n):
        c = float(closes[i])
        prev = alpha * c + (1.0 - alpha) * prev
        out[i] = prev
    return out

def tv_ema_next(prev: float, close_price: float, length: int) -> float:
    alpha = 2.0/(length+1.0)
    return alpha*close_price + (1.0-alpha)*prev

# ---------- ATR (Wilder RMA) ----------
def true_range(h: float, l: float, prev_close: Optional[float]) -> float:
    if prev_close is None:
        return float(abs(h - l))
    return float(max(h - l, abs(h - prev_close), abs(l - prev_close)))

def wilder_rma_series(values: List[float], length: int) -> List[Optional[float]]:
    n = len(values)
    out: List[Optional[float]] = [None]*n
    if length < 1 or n == 0 or n < length:
        return out
    seed = sum(values[:length]) / float(length)
    out[length-1] = seed
    prev = seed
    for i in range(length, n):
        prev = (prev*(length-1) + values[i]) / float(length)
        out[i] = prev
    return out

def wilder_rma_next(prev: float, value: float, length: int) -> float:
    return (prev*(length-1) + value) / float(length)

# ---------- Futures client (hedge-aware) ----------
class FuturesClient:
    def __init__(self, symbol: str, http: httpx.AsyncClient):
        self.symbol = symbol.upper()
        self.http = http
        # filters
        self.tick: Optional[float] = None
        self.step: Optional[float] = None
        self.min_qty: float = 0.0
        self.max_qty: float = 0.0
        self.min_notional: float = 5.0
        # account
        self.available_usdt: float = 0.0
        self.leverage: int = DEFAULT_LEVERAGE
        self.dual_side: bool = False
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
            local_ms = now_ms()
            TIME_DRIFT_MS = server_ms - local_ms
        except Exception as e:
            print("time sync:", e)

    async def fetch_filters(self):
        r = await _get_with_backoff(self.http, f"{BINANCE_API}/fapi/v1/exchangeInfo", params={"symbol": self.symbol})
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
                self.http, f"{BINANCE_API}/fapi/v1/positionSide/dual",
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
            amt = float(p.get("positionAmt", "0"))
            ent = float(p.get("entryPrice", "0"))
            try:
                self.leverage = int(float(p.get("leverage", self.leverage)))
            except Exception:
                pass
            if side == "BOTH":
                self.net_pos = amt
                self.entry_px = ent
            elif side == "LONG":
                self.long_pos = amt
                self.long_entry = ent
            elif side == "SHORT":
                self.short_pos = abs(amt)
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
        need_by_notional = self._round_qty_ceil(self.min_notional / max(price, 1e-12))
        return max(self.min_qty, need_by_notional)

    def max_affordable_qty(self, price: float) -> float:
        notional_cap = max(0.0, self.available_usdt - self.available_usdt*FEE_BUFFER_RATE) * self.leverage * UTILIZATION
        q = self._round_qty_floor(notional_cap / max(price, 1e-12))
        if self.max_qty > 0:
            q = min(q, self.max_qty)
        return max(0.0, q)

    def compute_order_qty(self, ref_price: float) -> Optional[float]:
        price = max(ref_price, 1e-12)
        need = self.min_qty_for_notional(price)
        cap = self.max_affordable_qty(price)
        if cap < need - 1e-12:
            return None
        qty = max(need, cap)
        return self._round_qty_floor(qty)

    async def fetch_last_price(self) -> Optional[float]:
        try:
            r = await _get_with_backoff(self.http, f"{BINANCE_API}/fapi/v1/ticker/price", params={"symbol": self.symbol})
            return float(r.json().get("price"))
        except Exception:
            return None

    async def place_market(self, side: str, qty: float, *, reduce_only: bool=False, position_side: Optional[str]=None) -> Tuple[Optional[int], Optional[str]]:
        ts = now_ms() + TIME_DRIFT_MS
        params = {
            "symbol": self.symbol,
            "side": side,
            "type": "MARKET",
            "quantity": f"{qty}",
            "timestamp": ts,
            "recvWindow": 5000
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
        price = ref_price if ref_price > 0 else (await self.fetch_last_price() or 0.0)
        await self.fetch_account()
        before_net = self.net_pos
        before_long = self.long_pos
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
                cap = self.max_affordable_qty(price)
                q0 = self._round_qty_floor(open_prefer_qty)
                qty = max(need, min(q0, cap))
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
                side, qty,
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
            cap = self.max_affordable_qty(price)
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

# ---------- EMA Cross Runner (pine-эквивалент) ----------
class EMARunner:
    def __init__(self, symbol: str, interval: str, leverage: int,
                 spread_min_pct: float, one_signal_per_bar: bool, on_close_only: bool,
                 fast_len: int, slow_len: int,
                 use_sl: bool, sl_mode: str, sl_pct: float, atr_len: int, atr_mult: float,
                 http: httpx.AsyncClient):
        self.symbol = symbol.upper()
        self.interval = interval
        self.target_leverage = max(1, min(leverage, LEVERAGE_CAP))
        self.spread_min_pct = float(spread_min_pct)
        self.one_signal_per_bar = bool(one_signal_per_bar)
        self.on_close_only = bool(on_close_only)
        self.http = http

        # --- Pine: fast/slow с гарантией fast < slow
        i_fast = max(1, int(fast_len))
        i_slow = max(2, int(slow_len))
        self.fast_len = min(i_fast, i_slow - 1)
        self.slow_len = max(i_slow, i_fast + 1)

        # --- SL / ATR настройки
        self.use_sl   = bool(use_sl)
        self.sl_mode  = "ATR" if str(sl_mode).strip().upper() == "ATR" else "Percent"
        self.sl_pct   = max(0.0, float(sl_pct))
        self.atr_len  = max(1, int(atr_len))
        self.atr_mult = max(0.0, float(atr_mult))

        self.c = FuturesClient(self.symbol, http)

        self.closes: List[float] = []
        # для UI совместимости: ema50=fast, ema200=slow
        self.ema50: Optional[float] = None   # fast
        self.ema100: Optional[float] = None  # не используется
        self.ema200: Optional[float] = None  # slow

        # интрабар EMA альфы под длины fast/slow
        self.alpha50  = 2.0/(self.fast_len+1.0)
        self.alpha200 = 2.0/(self.slow_len+1.0)
        self.prev_tick_ema50: Optional[float] = None
        self.prev_tick_ema200: Optional[float] = None

        # ATR (Wilder RMA)
        self.atr: Optional[float] = None           # ATR последнего закрытого бара
        self.atr_tick: Optional[float] = None      # интрабар оценка ATR текущего бара
        self.prev_close: Optional[float] = None    # предыдущий Close
        self.cur_bar_high: Optional[float] = None  # текущий High бара
        self.cur_bar_low: Optional[float] = None   # текущий Low бара

        self.running = False
        self._tasks: List[asyncio.Task] = []

        # UI/status
        self.last_signal: Optional[str] = None
        self.last_action: Optional[str] = None
        self.last_action_ts: Optional[int] = None
        self.last_error: Optional[str] = None
        self.source: str = "kline"
        self.bot_state: str = "Waiting"

        # по-барный контроль
        self.last_bar_key: Optional[str] = None
        self.fired_entry_this_bar: bool = False

        # pine-подобное pending: 1 / -1 / 0
        self.pending: int = 0

        self.pnl_today_realized: float = 0.0

    # ---------- WARMUP ----------
    async def _warmup_native(self):
        try:
            r = await _get_with_backoff(
                self.http, f"{BINANCE_API}/fapi/v1/klines",
                params={"symbol": self.symbol, "interval": self.interval, "limit": WARMUP_LIMIT}
            )
            data = r.json()
            if not data:
                return
            closes = [float(x[4]) for x in data]
            highs  = [float(x[2]) for x in data]
            lows   = [float(x[3]) for x in data]

            self.closes = closes

            # EMA fast/slow
            e_fast = tv_ema_series(closes, self.fast_len)
            e_slow = tv_ema_series(closes, self.slow_len)
            self.ema50  = e_fast[-1]
            self.ema100 = None
            self.ema200 = e_slow[-1]

            # ATR (Wilder RMA)
            tr_list: List[float] = []
            prev_c = None
            for i in range(len(closes)):
                tr_list.append(true_range(highs[i], lows[i], prev_c))
                prev_c = closes[i]
            atr_series = wilder_rma_series(tr_list, self.atr_len)
            self.atr = atr_series[-1]
            self.prev_close = closes[-1]
            self.cur_bar_high = highs[-1]
            self.cur_bar_low  = lows[-1]
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
        else:
            trades = r.json()
            aggr: Dict[int, Dict[str, float]] = {}
            for t in trades:
                ts = int(t["T"]) // 1000
                p = float(t["p"])
                q = float(t["q"])
                if ts not in aggr:
                    aggr[ts] = {"o": p, "h": p, "l": p, "c": p, "v": q}
                else:
                    aggr[ts]["h"] = max(aggr[ts]["h"], p)
                    aggr[ts]["l"] = min(aggr[ts]["l"], p)
                    aggr[ts]["c"] = p
                    aggr[ts]["v"] += q
            if not aggr:
                return
            secs = sorted(aggr.keys())
            closes = [aggr[s]["c"] for s in secs]
            highs  = [aggr[s]["h"] for s in secs]
            lows   = [aggr[s]["l"] for s in secs]
            self.closes = closes

            e_fast = tv_ema_series(closes, self.fast_len)
            e_slow = tv_ema_series(closes, self.slow_len)
            self.ema50  = e_fast[-1]
            self.ema100 = None
            self.ema200 = e_slow[-1]

            tr_list: List[float] = []
            prev_c = None
            for i in range(len(closes)):
                tr_list.append(true_range(highs[i], lows[i], prev_c))
                prev_c = closes[i]
            atr_series = wilder_rma_series(tr_list, self.atr_len)
            self.atr = atr_series[-1]
            self.prev_close = closes[-1]
            self.cur_bar_high = highs[-1]
            self.cur_bar_low  = lows[-1]

    # ---------- закрытие бара ----------
    async def _on_bar_close(self, o: float, h: float, l: float, c: float, bar_key: str):
        self.closes.append(c)
        if len(self.closes) > WARMUP_LIMIT:
            self.closes = self.closes[-WARMUP_LIMIT:]

        # EMA обновляем на закрытии
        self.ema50  = tv_ema_next(self.ema50,  c, self.fast_len)  if self.ema50  is not None else tv_ema_series(self.closes, self.fast_len)[-1]
        self.ema100 = None
        self.ema200 = tv_ema_next(self.ema200, c, self.slow_len) if self.ema200 is not None else tv_ema_series(self.closes, self.slow_len)[-1]

        # ATR: TR по закрытому бару, rma-next
        if self.prev_close is not None:
            tr_close = true_range(h, l, self.prev_close)
            if self.atr is not None:
                self.atr = wilder_rma_next(self.atr, tr_close, self.atr_len)
            else:
                # если нет — инициализируем как seed
                self.atr = tr_close
        self.prev_close = c

        self.last_bar_key = bar_key
        self.fired_entry_this_bar = False
        self.prev_tick_ema50 = None
        self.prev_tick_ema200 = None
        self.cur_bar_high = h
        self.cur_bar_low  = l
        self.atr_tick = None  # сбросим интрабар оценку, новый бар начнётся заново

    # ---------- SL prices ----------
    def _sl_prices(self, avg_price: float, *, use_intrabar: bool) -> Tuple[Optional[float], Optional[float]]:
        if not self.use_sl or avg_price <= 0:
            return (None, None)
        if self.sl_mode == "Percent":
            long_sl  = avg_price * (1.0 - self.sl_pct/100.0)
            short_sl = avg_price * (1.0 + self.sl_pct/100.0)
            return (long_sl, short_sl)
        # ATR mode
        atr_val = (self.atr_tick if use_intrabar and (self.atr_tick is not None) else self.atr)
        if atr_val is None:
            return (None, None)
        delta = atr_val * self.atr_mult
        long_sl  = avg_price - delta
        short_sl = avg_price + delta
        return (long_sl, short_sl)

    # ---------- Stop-loss checker ----------
    async def _check_stop_and_exit(self, px: float, *, use_intrabar: bool):
        if not self.use_sl:
            return
        try:
            minq = max(self.c.min_qty, 0.0)
            await self.c.fetch_account()  # освежим позицию/entry
            if self.c.dual_side:
                # LONG side
                if self.c.long_pos > minq and self.c.long_entry > 0:
                    long_sl, _ = self._sl_prices(self.c.long_entry, use_intrabar=use_intrabar)
                    if long_sl is not None and px <= long_sl:
                        q = self.c._round_qty_floor(self.c.long_pos)
                        _, err = await self.c.place_market_smart("SELL", px, reduce_only=True, position_side="LONG", qty_override=q)
                        self.last_action = "LONG SL hit" if err is None else f"LONG SL error: {err}"
                        self.last_action_ts = now_ms()
                # SHORT side
                if self.c.short_pos > minq and self.c.short_entry > 0:
                    _, short_sl = self._sl_prices(self.c.short_entry, use_intrabar=use_intrabar)
                    if short_sl is not None and px >= short_sl:
                        q = self.c._round_qty_floor(self.c.short_pos)
                        _, err = await self.c.place_market_smart("BUY", px, reduce_only=True, position_side="SHORT", qty_override=q)
                        self.last_action = "SHORT SL hit" if err is None else f"SHORT SL error: {err}"
                        self.last_action_ts = now_ms()
            else:
                pos = self.c.net_pos
                if pos > minq and self.c.entry_px > 0:
                    long_sl, _ = self._sl_prices(self.c.entry_px, use_intrabar=use_intrabar)
                    if long_sl is not None and px <= long_sl:
                        q = self.c._round_qty_floor(abs(pos))
                        _, err = await self.c.place_market_smart("SELL", px, reduce_only=True, qty_override=q)
                        self.last_action = "LONG SL hit" if err is None else f"LONG SL error: {err}"
                        self.last_action_ts = now_ms()
                elif pos < -minq and self.c.entry_px > 0:
                    _, short_sl = self._sl_prices(self.c.entry_px, use_intrabar=use_intrabar)
                    if short_sl is not None and px >= short_sl:
                        q = self.c._round_qty_floor(abs(pos))
                        _, err = await self.c.place_market_smart("BUY", px, reduce_only=True, qty_override=q)
                        self.last_action = "SHORT SL hit" if err is None else f"SHORT SL error: {err}"
                        self.last_action_ts = now_ms()
        except Exception as e:
            self.last_error = f"SL check: {e}"

    # ---------- действие по сигналу (вход/переворот) ----------
    async def _act_on_signal(self, sig: Optional[str], ref_price: float):
        if not sig:
            return
        try:
            await self.c.fetch_account()
            await self.c.ensure_leverage(self.target_leverage)
            min_qty = max(self.c.min_qty, 0.0)

            if self.c.dual_side:
                if sig == "LONG":
                    if self.c.short_pos > min_qty:
                        q = self.c._round_qty_floor(self.c.short_pos)
                        _, err = await self.c.place_market_smart("BUY", ref_price, reduce_only=True, position_side="SHORT", qty_override=q)
                        await self.c.fetch_account()
                        if err: self.last_action = f"EXIT SHORT FAILED ({err})"; self.last_action_ts = now_ms(); return
                    await asyncio.sleep(0.25)
                    _, err = await self.c.place_market_smart("BUY", ref_price, reduce_only=False, position_side="LONG")
                    await self.c.fetch_account()
                    self.last_action = "ENTER LONG ok" if err is None and self.c.long_pos > min_qty else f"ENTER LONG FAILED ({err})"
                elif sig == "SHORT":
                    if self.c.long_pos > min_qty:
                        q = self.c._round_qty_floor(self.c.long_pos)
                        _, err = await self.c.place_market_smart("SELL", ref_price, reduce_only=True, position_side="LONG", qty_override=q)
                        await self.c.fetch_account()
                        if err: self.last_action = f"EXIT LONG FAILED ({err})"; self.last_action_ts = now_ms(); return
                    await asyncio.sleep(0.25)
                    _, err = await self.c.place_market_smart("SELL", ref_price, reduce_only=False, position_side="SHORT")
                    await self.c.fetch_account()
                    self.last_action = "ENTER SHORT ok" if err is None and self.c.short_pos > min_qty else f"ENTER SHORT FAILED ({err})"
            else:
                pos = self.c.net_pos
                if sig == "LONG":
                    if pos < -min_qty:
                        q = self.c._round_qty_floor(abs(pos))
                        _, err = await self.c.place_market_smart("BUY", ref_price, reduce_only=True, qty_override=q)
                        await self.c.fetch_account()
                        if err: self.last_action = f"EXIT SHORT FAILED ({err})"; self.last_action_ts = now_ms(); return
                    await asyncio.sleep(0.25)
                    _, err = await self.c.place_market_smart("BUY", ref_price, reduce_only=False)
                    await self.c.fetch_account()
                    self.last_action = "ENTER LONG ok" if err is None and self.c.net_pos > min_qty else f"ENTER LONG FAILED ({err})"
                elif sig == "SHORT":
                    if pos > min_qty:
                        q = self.c._round_qty_floor(abs(pos))
                        _, err = await self.c.place_market_smart("SELL", ref_price, reduce_only=True, qty_override=q)
                        await self.c.fetch_account()
                        if err: self.last_action = f"EXIT LONG FAILED ({err})"; self.last_action_ts = now_ms(); return
                    await asyncio.sleep(0.25)
                    _, err = await self.c.place_market_smart("SELL", ref_price, reduce_only=False)
                    await self.c.fetch_account()
                    self.last_action = "ENTER SHORT ok" if err is None and self.c.net_pos < -min_qty else f"ENTER SHORT FAILED ({err})"

            self.last_action_ts = now_ms()
            self.last_signal = sig
            await self.c.fetch_account()
            self._refresh_state_nowait()
        except Exception as e:
            self.last_error = f"act: {e}"

    def _refresh_state_nowait(self):
        try:
            min_qty = max(self.c.min_qty, 0.0)
            if self.c.dual_side:
                if self.c.long_pos > min_qty:  self.bot_state = "LONG"
                elif self.c.short_pos > min_qty: self.bot_state = "SHORT"
                else: self.bot_state = "Waiting"
            else:
                pos = self.c.net_pos
                self.bot_state = "Waiting" if abs(pos) < 1e-12 else ("LONG" if pos > 0 else "SHORT")
        except Exception:
            self.bot_state = "Waiting"

    # ---------- WebSocket loops ----------
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
                            px = _kline_tick_close(k)
                            is_closed = bool(k.get("x", False))
                            bar_key = f"{k.get('t')}_{k.get('T')}"

                            # high/low текущего бара (из события)
                            h_now = _kline_tick_high(k, px)
                            l_now = _kline_tick_low(k, px)

                            # новый бар?
                            if bar_key != self.last_bar_key:
                                # при смене бара: сброс интрабар EMA и стартовые H/L
                                self.last_bar_key = bar_key
                                self.fired_entry_this_bar = False
                                self.prev_tick_ema50 = None
                                self.prev_tick_ema200 = None
                                self.cur_bar_high = h_now
                                self.cur_bar_low  = l_now
                                # atr_tick пересчитаем от текущего TR
                                if self.prev_close is not None and self.atr is not None:
                                    tr_now = true_range(h_now, l_now, self.prev_close)
                                    self.atr_tick = wilder_rma_next(self.atr, tr_now, self.atr_len)

                            if not is_closed:
                                # обновляем high/low текущего бара
                                if self.cur_bar_high is None or h_now > self.cur_bar_high:
                                    self.cur_bar_high = h_now
                                if self.cur_bar_low is None or l_now < self.cur_bar_low:
                                    self.cur_bar_low = l_now
                                # интрабар ATR оценка
                                if self.prev_close is not None and self.atr is not None:
                                    tr_now = true_range(self.cur_bar_high, self.cur_bar_low, self.prev_close)
                                    self.atr_tick = wilder_rma_next(self.atr, tr_now, self.atr_len)

                                # интрабар логика допускается только если confirmOff
                                if not self.on_close_only and (self.ema50 is not None) and (self.ema200 is not None):
                                    ema50_now  = self.alpha50*px  + (1.0 - self.alpha50)*self.ema50
                                    ema200_now = self.alpha200*px + (1.0 - self.alpha200)*self.ema200

                                    if (self.prev_tick_ema50 is not None) and (self.prev_tick_ema200 is not None):
                                        long_cross  = (self.prev_tick_ema50 <= self.prev_tick_ema200) and (ema50_now > ema200_now)
                                        short_cross = (self.prev_tick_ema50 >= self.prev_tick_ema200) and (ema50_now < ema200_now)
                                        if long_cross:  self.pending = 1
                                        elif short_cross: self.pending = -1

                                        denom = max(abs(ema200_now), 1e-12)
                                        spread_now = abs(ema50_now - ema200_now) / denom * 100.0
                                        can_fire = (not self.one_signal_per_bar) or (not self.fired_entry_this_bar)
                                        if can_fire and self.pending == 1 and spread_now >= self.spread_min_pct:
                                            await self._act_on_signal("LONG", px)
                                            self.pending = 0
                                            self.fired_entry_this_bar = True
                                        elif can_fire and self.pending == -1 and spread_now >= self.spread_min_pct:
                                            await self._act_on_signal("SHORT", px)
                                            self.pending = 0
                                            self.fired_entry_this_bar = True

                                    self.prev_tick_ema50 = ema50_now
                                    self.prev_tick_ema200 = ema200_now

                                # Стоп-лосс проверяем всегда (не ограничиваем oneSignalPerBar)
                                await self._check_stop_and_exit(px, use_intrabar=True)
                                continue

                            # --- бар закрывается: считаем кросс и спред на закрытии ---
                            prev_fast  = self.ema50
                            prev_slow  = self.ema200
                            next_fast  = tv_ema_next(prev_fast, px, self.fast_len)  if prev_fast  is not None else None
                            next_slow  = tv_ema_next(prev_slow, px, self.slow_len) if prev_slow is not None else None

                            if (prev_fast is not None) and (prev_slow is not None) and (next_fast is not None) and (next_slow is not None):
                                long_cross  = (prev_fast <= prev_slow) and (next_fast > next_slow)
                                short_cross = (prev_fast >= prev_slow) and (next_fast < next_slow)
                                if long_cross:  self.pending = 1
                                elif short_cross: self.pending = -1

                                denom = max(abs(next_slow), 1e-12)
                                spread_close = abs(next_fast - next_slow) / denom * 100.0
                                can_fire = (not self.one_signal_per_bar) or (not self.fired_entry_this_bar)
                                if can_fire and self.pending == 1 and spread_close >= self.spread_min_pct:
                                    await self._act_on_signal("LONG", px)
                                    self.pending = 0
                                    self.fired_entry_this_bar = True
                                elif can_fire and self.pending == -1 and spread_close >= self.spread_min_pct:
                                    await self._act_on_signal("SHORT", px)
                                    self.pending = 0
                                    self.fired_entry_this_bar = True

                            # обновляем закрытые EMA/ATR и сбрасываем интрабарные
                            o = float(k.get("o", px))
                            h = float(k.get("h", px))
                            l = float(k.get("l", px))
                            c = px
                            await self._on_bar_close(o, h, l, c, bar_key)

                            # проверка SL и на закрытии
                            await self._check_stop_and_exit(px, use_intrabar=False)

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
                                o = float(closed["o"]); h = float(closed["h"]); l = float(closed["l"]); c = float(closed["c"])
                                bar_key = str(int(closed["t"]))

                                prev_fast  = self.ema50
                                prev_slow  = self.ema200
                                next_fast  = tv_ema_next(prev_fast, c, self.fast_len)  if prev_fast  is not None else None
                                next_slow  = tv_ema_next(prev_slow, c, self.slow_len) if prev_slow is not None else None

                                if (prev_fast is not None) and (prev_slow is not None) and (next_fast is not None) and (next_slow is not None):
                                    long_cross  = (prev_fast <= prev_slow) and (next_fast > next_slow)
                                    short_cross = (prev_fast >= prev_slow) and (next_fast < next_slow)
                                    if long_cross:  self.pending = 1
                                    elif short_cross: self.pending = -1

                                    denom = max(abs(next_slow), 1e-12)
                                    spread_close = abs(next_fast - next_slow) / denom * 100.0
                                    can_fire = (not self.one_signal_per_bar) or (not self.fired_entry_this_bar)
                                    if can_fire and self.pending == 1 and spread_close >= self.spread_min_pct:
                                        await self._act_on_signal("LONG", c)
                                        self.pending = 0
                                        self.fired_entry_this_bar = True
                                    elif can_fire and self.pending == -1 and spread_close >= self.spread_min_pct:
                                        await self._act_on_signal("SHORT", c)
                                        self.pending = 0
                                        self.fired_entry_this_bar = True

                                # ATR: закрытие 1s бара
                                if self.prev_close is not None:
                                    tr_close = true_range(h, l, self.prev_close)
                                    if self.atr is not None:
                                        self.atr = wilder_rma_next(self.atr, tr_close, self.atr_len)
                                    else:
                                        self.atr = tr_close
                                self.prev_close = c

                                # обновляем EMA и состояние бара
                                await self._on_bar_close(o, h, l, c, bar_key)

                                # стоп на закрытии секунды
                                await self._check_stop_and_exit(c, use_intrabar=False)
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
            "onCloseOnly": self.on_close_only,
            "oneSignalPerBar": self.one_signal_per_bar,
            "spreadMinPct": self.spread_min_pct,
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
        self.cur_on_close_only = DEFAULT_ON_CLOSE_ONLY
        self.cur_one_signal_per_bar = DEFAULT_ONE_SIGNAL_PER_BAR
        self.cur_spread_min_pct = DEFAULT_SPREAD_MIN_PCT

        # новые текущие параметры Pine-паритета
        self.cur_fast_len = FAST_LEN_DEFAULT
        self.cur_slow_len = SLOW_LEN_DEFAULT
        self.cur_use_sl   = DEFAULT_USE_SL
        self.cur_sl_mode  = DEFAULT_SL_MODE
        self.cur_sl_pct   = DEFAULT_SL_PCT
        self.cur_atr_len  = DEFAULT_ATR_LEN
        self.cur_atr_mult = DEFAULT_ATR_MULT

        self._tasks: List[asyncio.Task] = []

        # reconcile/external
        self.dual_side: bool = False
        self.external: Dict[str, Dict[str, Any]] = {}

        # --- Автопик ---
        self.auto_pick: bool = False
        self.auto_current_symbol: Optional[str] = None
        self.auto_current_pct: Optional[float] = None

        # --- кэш USDT-M PERP
        self._valid_syms_cache: set[str] = set()
        self._valid_syms_cached_at_ms: int = 0
        self._valid_syms_ttl_ms: int = 5 * 60 * 1000

    async def _valid_usdtm_perp_symbols(self) -> set:
        now = now_ms()
        if self._valid_syms_cache and (now - self._valid_syms_cached_at_ms) < self._valid_syms_ttl_ms:
            return self._valid_syms_cache
        try:
            r = await _get_with_backoff(self.http, f"{BINANCE_API}/fapi/v1/exchangeInfo")
        except Exception:
            r = None
        try:
            data = r.json() if r else {"symbols": []}
            valid = {
                s["symbol"] for s in data.get("symbols", [])
                if s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL" and s.get("status") == "TRADING"
            }
            if valid:
                self._valid_syms_cache = valid
                self._valid_syms_cached_at_ms = now
            return valid
        except Exception:
            return self._valid_syms_cache

    async def pick_top1_symbol(self) -> Tuple[str, float]:
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
                    spread_min_pct: float, one_signal_per_bar: bool, on_close_only: bool,
                    fast_len: int = FAST_LEN_DEFAULT, slow_len: int = SLOW_LEN_DEFAULT,
                    use_sl: bool = DEFAULT_USE_SL, sl_mode: str = DEFAULT_SL_MODE,
                    sl_pct: float = DEFAULT_SL_PCT, atr_len: int = DEFAULT_ATR_LEN, atr_mult: float = DEFAULT_ATR_MULT):
        if self.running:
            await self.stop()
        if interval not in ALL_INTERVALS:
            raise ValueError(f"Unsupported interval: {interval}")

        self.cur_interval = interval
        self.cur_leverage = max(1, min(int(leverage), LEVERAGE_CAP))
        self.cur_spread_min_pct = float(spread_min_pct)
        self.cur_one_signal_per_bar = bool(one_signal_per_bar)
        self.cur_on_close_only = bool(on_close_only)

        self.cur_fast_len = max(1, int(fast_len))
        self.cur_slow_len = max(2, int(slow_len))
        if self.cur_fast_len >= self.cur_slow_len:
            self.cur_fast_len = self.cur_slow_len - 1

        self.cur_use_sl   = bool(use_sl)
        self.cur_sl_mode  = "ATR" if str(sl_mode).strip().upper() == "ATR" else "Percent"
        self.cur_sl_pct   = max(0.0, float(sl_pct))
        self.cur_atr_len  = max(1, int(atr_len))
        self.cur_atr_mult = max(0.0, float(atr_mult))

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
                          self.cur_spread_min_pct, self.cur_one_signal_per_bar, self.cur_on_close_only,
                          self.cur_fast_len, self.cur_slow_len,
                          self.cur_use_sl, self.cur_sl_mode, self.cur_sl_pct, self.cur_atr_len, self.cur_atr_mult,
                          self.http)
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
        for r in list(self.runners.values()):
            try:
                await r.c.fetch_account()
                if r.c.dual_side:
                    if r.c.long_pos > r.c.min_qty:
                        q = r.c._round_qty_floor(r.c.long_pos)
                        await r.c.place_market_smart("SELL", r.c.entry_px or (await r.c.fetch_last_price() or 0.0),
                                                     reduce_only=True, position_side="LONG", qty_override=q)
                    if r.c.short_pos > r.c.min_qty:
                        q = r.c._round_qty_floor(r.c.short_pos)
                        await r.c.place_market_smart("BUY", r.c.entry_px or (await r.c.fetch_last_price() or 0.0),
                                                     reduce_only=True, position_side="SHORT", qty_override=q)
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
                      spread_min_pct: float, one_signal_per_bar: bool, on_close_only: bool,
                      fast_len: int = FAST_LEN_DEFAULT, slow_len: int = SLOW_LEN_DEFAULT,
                      use_sl: bool = DEFAULT_USE_SL, sl_mode: str = DEFAULT_SL_MODE,
                      sl_pct: float = DEFAULT_SL_PCT, atr_len: int = DEFAULT_ATR_LEN, atr_mult: float = DEFAULT_ATR_MULT):
        await self.stop()
        await self.start(symbols, interval, leverage, spread_min_pct, one_signal_per_bar, on_close_only,
                         fast_len, slow_len, use_sl, sl_mode, sl_pct, atr_len, atr_mult)

    async def _income_loop(self):
        while self.running:
            try:
                await self.refresh_income()
            except Exception as e:
                print("income loop:", e)
            await asyncio.sleep(60)

    async def refresh_income(self):
        end_ms = now_ms() + TIME_DRIFT_MS
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
                        "timestamp": end_ms, "recvWindow": 5000,
                        "startTime": start, "endTime": end_ms,
                        "symbol": sym, "incomeType": "REALIZED_PNL", "limit": 1000
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
        while self.running:
            try:
                ts = now_ms() + TIME_DRIFT_MS
                try:
                    rds = await _signed_get_with_backoff(self.http, f"{BINANCE_API}/fapi/v1/positionSide/dual",
                                                         params={"timestamp": ts, "recvWindow": 5000})
                    self.dual_side = bool(rds.json().get("dualSidePosition", False))
                except Exception:
                    pass

                qp = _sign({"timestamp": ts, "recvWindow": 5000})
                acc_r = await self.http.get(f"{BINANCE_API}/fapi/v2/account", params=qp, headers={"X-MBX-APIKEY": API_KEY})
                acc_r.raise_for_status()
                acc = acc_r.json()

                avail_usdt = 0.0
                for a in acc.get("assets", []):
                    if a.get("asset") == "USDT":
                        avail_usdt = float(a.get("availableBalance", "0"))
                        break

                ext: Dict[str, Dict[str, Any]] = {}
                tmp: Dict[str, Dict[str, float]] = {}
                for p in acc.get("positions", []):
                    sym = p.get("symbol", "")
                    if sym in self.runners:
                        continue
                    side = p.get("positionSide", "BOTH")
                    amt = float(p.get("positionAmt", "0"))
                    ent = float(p.get("entryPrice", "0"))
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
                    if not has_pos:
                        continue
                    if self.dual_side and (d["long"]>1e-12 or d["short"]>1e-12):
                        state = "External HEDGE" if (d["long"]>1e-12 and d["short"]>1e-12) else ("External LONG" if d["long"]>1e-12 else "External SHORT")
                        pos_text = f"L:{d['long']:.6f} / S:{d['short']:.6f}"
                        entry_text = f"L:{d['long_entry']:.6f} / S:{d['short_entry']:.6f}"
                    else:
                        state = "External LONG" if d["net"]>0 else "External SHORT"
                        pos_text = f"{d['net']:.6f}"
                        entry_text = f"{d['entry']:.6f}"
                    ext[sym] = {
                        "symbol": sym, "interval": self.cur_interval, "source": "external", "leverage": self.cur_leverage,
                        "onCloseOnly": self.cur_on_close_only, "oneSignalPerBar": self.cur_one_signal_per_bar, "spreadMinPct": self.cur_spread_min_pct,
                        "lastSignal": None, "botState": state, "availableUSDT": avail_usdt,
                        "netPos": pos_text, "entryPx": entry_text, "ema50": None, "ema100": None, "ema200": None,
                        "pnlToday": 0.0, "lastAction": "External position", "lastError": None
                    }
                self.external = ext

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
        while self.running:
            try:
                if self.auto_pick:
                    any_open = False
                    for r in self.runners.values():
                        try:
                            await r.c.fetch_account()
                            minq = max(r.c.min_qty, 0.0)
                            if r.c.dual_side:
                                if r.c.long_pos > minq or r.c.short_pos > minq:
                                    any_open = True; break
                            else:
                                if abs(r.c.net_pos) > minq:
                                    any_open = True; break
                        except Exception:
                            pass
                    if not any_open:
                        top_sym, top_pct = await self.pick_top1_symbol()
                        if top_sym and (top_sym not in self.runners):
                            await self.restart([top_sym], self.cur_interval, self.cur_leverage,
                                               self.cur_spread_min_pct, self.cur_one_signal_per_bar, self.cur_on_close_only,
                                               self.cur_fast_len, self.cur_slow_len,
                                               self.cur_use_sl, self.cur_sl_mode, self.cur_sl_pct, self.cur_atr_len, self.cur_atr_mult)
                            self.auto_pick = True
                            self.auto_current_symbol = top_sym
                            self.auto_current_pct = top_pct
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
                "on_close_only": self.cur_on_close_only,
                "one_signal_per_bar": self.cur_one_signal_per_bar,
                "spread_min_pct": self.cur_spread_min_pct,
                "fast_len": self.cur_fast_len,
                "slow_len": self.cur_slow_len,
                "use_sl": self.cur_use_sl,
                "sl_mode": self.cur_sl_mode,
                "sl_pct": self.cur_sl_pct,
                "atr_len": self.cur_atr_len,
                "atr_mult": self.cur_atr_mult
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
<title>EMA Cross + Spread — Binance Futures</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
:root { --bg:#0f172a; --card:#111827; --muted:#94a3b8; --text:#e5e7eb; --border:#1f2937; --green:#22c55e; --red:#ef4444; --blue:#3b82f6; --amber:#f59e0b; --gray:#6b7280; }
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
.long { background:rgba(34,197,94,.2); color:#86efac; }
.short { background:rgba(239,68,68,.2); color:#fca5a5; }
/* switches */
.switch { position:relative; width:54px; height:30px; background:#374151; border-radius:999px; transition:background .2s ease; border:1px solid #4b5563; cursor:pointer; }
.switch input { display:none; }
.switch .thumb { position:absolute; top:3px; left:3px; width:24px; height:24px; background:#fff; border-radius:50%; transition:left .2s ease; box-shadow: 0 2px 8px rgba(0,0,0,.35); }
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
<h2>EMA Cross + Spread — Binance Futures</h2>
<div class="card">
  <div class="grid">
    <div>
      <label class="title">Symbols (comma)</label><br/>
      <input id="sym" type="text" value="" placeholder="choose a coin"/>
      <div class="small">Пусто — авто выбор топ-1 24h% (USDT-M PERP). Фолбэк: CYBERUSDT.</div>
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
      <label class="title">Min EMA spread (%)</label><br/>
      <input id="spread" type="number" value="1.00" min="0" step="0.05"/>
    </div>
    <div>
      <label class="title">Confirm on bar close</label><br/>
      <div id="confirm" class="switch"><input type="checkbox"/><div class="thumb"></div></div>
      <div class="small">OFF = интрабар (как в pine при calc_on_every_tick).</div>
    </div>
    <div>
      <label class="title">One signal per bar</label><br/>
      <div id="onesig" class="switch on"><input type="checkbox"/><div class="thumb"></div></div>
    </div>
  </div>
  <div class="row" style="margin-top:12px;">
    <button id="btnStart" class="primary" onclick="start()">Start</button>
    <button class="danger" onclick="stop()">Stop</button>
    <button class="secondary" onclick="restart()">Restart</button>
    <span id="flag" class="pill off">Stopped</span>
  </div>
</div>

<div style="height:16px"></div>

<div class="card">
  <table class="table">
    <thead>
      <tr>
        <th>Symbol</th><th>TF</th><th>Lev</th>
        <th>ConfirmClose</th><th>1/Bar</th><th>Spread %</th>
        <th>Bot</th><th>Avail USDT</th><th>Equidity</th><th>Entry</th>
        <th>EMA50</th><th>EMA100</th><th>EMA200</th>
        <th>PnL Today (R)</th><th>Last Action</th><th>Error</th>
      </tr>
    </thead>
    <tbody id="tbody"></tbody>
  </table>
  <div class="small" style="margin-top:8px;">
    Реализованный PnL считается за сутки UTC.
  </div>
</div>
</div>

<script>
function toggleInit(id, initialOn=false){
  const el=document.getElementById(id);
  if(initialOn) el.classList.add('on');
  el.addEventListener('click', ()=>{ el.classList.toggle('on'); });
  return el;
}
const confirmSw = toggleInit('confirm', false);
const onesigSw  = toggleInit('onesig',  true);

function symbols(){
  return document.getElementById('sym').value
    .split(',').map(s=>s.trim()).filter(Boolean);
}
function params(){
  return {
    interval: document.getElementById('tf').value,
    leverage: parseInt(document.getElementById('lev').value),
    spread_min_pct: parseFloat(document.getElementById('spread').value),
    one_signal_per_bar: onesigSw.classList.contains('on'),
    on_close_only: confirmSw.classList.contains('on')
  };
}
async function post(path, body){
  const r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body||{})});
  if(!r.ok){
    let msg = await r.text();
    try{ msg = JSON.parse(msg).error || msg; }catch(_){}
    throw new Error(msg);
  }
  return r.json();
}
async function get(path){
  const r=await fetch(path);
  if(!r.ok) throw new Error(await r.text());
  return r.json();
}

// ---------- helpers ----------
function td(v){
  const d=document.createElement('td');
  if(v==null) v='';
  if(typeof v==='number'){
    d.textContent = Math.abs(v) >= 0.0001 ? v.toFixed(6) : v.toString();
  } else { d.textContent = v; }
  return d;
}
function tdInt(v){
  const d=document.createElement('td');
  const n = parseInt(v ?? 0);
  d.textContent = isFinite(n) ? String(n) : '';
  return d;
}
function tdMoney(v){
  const d=document.createElement('td');
  const n = Number(v);
  d.textContent = isFinite(n) ? n.toFixed(2) : '';
  return d;
}
function stateBadge(state){
  const span=document.createElement('span');
  span.className='status-badge';
  if(state==='LONG'){ span.classList.add('long'); span.textContent='LONG'; }
  else if(state==='SHORT'){ span.classList.add('short'); span.textContent='SHORT'; }
  else { span.classList.add('waiting'); span.textContent=state || 'Waiting'; }
  return span;
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
function parseLSPair(txt){
  const res = {L:0, S:0};
  if(typeof txt !== 'string') return res;
  const mL = txt.match(/L:\s*([0-9.+-eE]+)/);
  const mS = txt.match(/S:\s*([0-9.+-eE]+)/);
  if(mL) res.L = parseFloat(mL[1]) || 0;
  if(mS) res.S = parseFloat(mS[1]) || 0;
  return res;
}
function equidityUSDT(w){
  try{
    if(w.source === 'external' || (typeof w.netPos === 'string')){
      const q = parseLSPair(w.netPos || '');
      const e = parseLSPair(w.entryPx || '');
      const val = Math.abs(q.L)*(e.L||0) + Math.abs(q.S)*(e.S||0);
      return val;
    }else{
      const qty = Math.abs(Number(w.netPos || 0));
      const px  = Number(w.entryPx || 0);
      return qty * px;
    }
  }catch(_){ return 0; }
}

function pushRow(tb, w){
  const tr=document.createElement('tr');
  tr.appendChild(td(w.symbol));
  tr.appendChild(td(w.interval));
  tr.appendChild(tdInt(w.leverage));
  tr.appendChild(td(w.onCloseOnly ? 'ON' : 'OFF'));
  tr.appendChild(td(w.oneSignalPerBar ? 'ON' : 'OFF'));
  tr.appendChild(td(Number(w.spreadMinPct).toFixed(2)));
  const stateCell = document.createElement('td');
  stateCell.appendChild(stateBadge(w.botState));
  tr.appendChild(stateCell);
  tr.appendChild(td(w.availableUSDT));
  tr.appendChild(tdMoney(equidityUSDT(w)));
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

  const levEl = document.getElementById('lev');
  const backendLev = parseInt(st?.params?.leverage ?? 1);
  if(document.activeElement !== levEl){
    levEl.value = String(isFinite(backendLev) ? backendLev : 1);
  }
  const spreadEl = document.getElementById('spread');
  const beSpread = Number(st?.params?.spread_min_pct ?? 1.0);
  if(document.activeElement !== spreadEl){
    spreadEl.value = isFinite(beSpread) ? beSpread.toFixed(2) : '1.00';
  }

  if(st?.params){
    if(st.params.on_close_only) document.getElementById('confirm').classList.add('on'); else document.getElementById('confirm').classList.remove('on');
    if(st.params.one_signal_per_bar) document.getElementById('onesig').classList.add('on'); else document.getElementById('onesig').classList.remove('on');
  }

  const tb=document.getElementById('tbody');
  tb.innerHTML='';
  (st.workers||[]).forEach(w=> pushRow(tb, w));
  (st.external||[]).forEach(w=> pushRow(tb, w));
}

async function start(){
  try{
    const p=params();
    // можно добавить кастомные поля здесь при необходимости
    await post('/api/start',{symbols:symbols(), ...p});
    await refresh();
  }catch(e){ alert('Start: '+e.message); }
}
async function stop(){
  try{ await post('/api/stop',{}); await refresh(); }
  catch(e){ alert('Stop: '+e.message); }
}
async function restart(){
  try{
    const p=params();
    await post('/api/restart',{symbols:symbols(), ...p});
    await refresh();
  }catch(e){ alert('Restart: '+e.message); }
}

async function loop(){
  try{
    await refresh();
    const st=await get('/api/status');
    setTimeout(loop, st.running?1200:3000);
  }catch(e){
    console.error(e);
    setTimeout(loop,3000);
  }
}
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

def _read_params(body: dict):
    # старые параметры UI
    syms = body.get("symbols", [])
    interval = str(body.get("interval", DEFAULT_INTERVAL))
    leverage = int(body.get("leverage", DEFAULT_LEVERAGE))
    spread_min_pct = float(body.get("spread_min_pct", DEFAULT_SPREAD_MIN_PCT))
    one_signal_per_bar = bool(body.get("one_signal_per_bar", DEFAULT_ONE_SIGNAL_PER_BAR))
    on_close_only = bool(body.get("on_close_only", DEFAULT_ON_CLOSE_ONLY))
    # новые — если не переданы, берём дефолты (Pine parity)
    fast_len = int(body.get("fast_len", FAST_LEN_DEFAULT))
    slow_len = int(body.get("slow_len", SLOW_LEN_DEFAULT))
    use_sl   = bool(body.get("use_sl", DEFAULT_USE_SL))
    sl_mode  = str(body.get("sl_mode", DEFAULT_SL_MODE))
    sl_pct   = float(body.get("sl_pct", DEFAULT_SL_PCT))
    atr_len  = int(body.get("atr_len", DEFAULT_ATR_LEN))
    atr_mult = float(body.get("atr_mult", DEFAULT_ATR_MULT))
    return syms, interval, leverage, spread_min_pct, one_signal_per_bar, on_close_only, fast_len, slow_len, use_sl, sl_mode, sl_pct, atr_len, atr_mult

@app.post("/api/start")
async def api_start(req: Request):
    if not API_KEY or not API_SECRET:
        return JSONResponse({"error":"Missing BINANCE_API_KEY/SECRET"}, status_code=400)
    body = await req.json()
    (syms, interval, leverage, spread_min_pct, one_signal_per_bar, on_close_only,
     fast_len, slow_len, use_sl, sl_mode, sl_pct, atr_len, atr_mult) = _read_params(body)
    try:
        await MANAGER.start(syms, interval, leverage, spread_min_pct, one_signal_per_bar, on_close_only,
                            fast_len, slow_len, use_sl, sl_mode, sl_pct, atr_len, atr_mult)
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
    (syms, interval, leverage, spread_min_pct, one_signal_per_bar, on_close_only,
     fast_len, slow_len, use_sl, sl_mode, sl_pct, atr_len, atr_mult) = _read_params(body)
    try:
        await MANAGER.restart(syms, interval, leverage, spread_min_pct, one_signal_per_bar, on_close_only,
                              fast_len, slow_len, use_sl, sl_mode, sl_pct, atr_len, atr_mult)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=400)

MANAGER  # keep linter happy

if __name__ == "__main__":
    import uvicorn, os
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)
