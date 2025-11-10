import asyncio, math, pytz
from dataclasses import asdict
from datetime import datetime, time
from typing import Callable, Optional, Dict, Any
import pandas as pd

from .config import ScalperConfig

TICK_ES  = 0.25
VAL_ES   = 12.5
VAL_MES  = 5.0

class IntradayScalper:
    """
    Pluggable scalper:
      - expects a coroutine that yields 1-min bars dict(ts,o,h,l,c,v)
      - expects a broker with place_order(...) and positions()/account_summary()
    """
    def __init__(self, data_stream: Callable, broker, cfg: ScalperConfig):
        self.stream = data_stream
        self.broker = broker
        self.cfg = cfg
        self.running = False
        self.state: Dict[str, Any] = {
            "trades_today": 0,
            "pnl_today": 0.0,
            "position": 0,
            "entry_price": None,
            "stop_price": None,
            "tp_price": None,
            "entry_time": None,
            "last_bar": None
        }
        self.df = pd.DataFrame(columns=["ts","o","h","l","c","v"])

    # ------------- utilities -------------
    def _now_et(self):
        tz = pytz.timezone(self.cfg.tz)
        return datetime.now(tz)

    def _in_session(self):
        now = self._now_et()
        st_hrs, st_min = map(int, self.cfg.start_time.split(":"))
        sp_hrs, sp_min = map(int, self.cfg.stop_time.split(":"))
        return time(st_hrs, st_min) <= now.time() <= time(sp_hrs, sp_min)

    def _tick_value(self):
        return VAL_MES if self.cfg.use_mes else VAL_ES

    def _tick_size(self):
        return TICK_ES

    # ------------- sizing -------------
    async def _contracts(self, stop_ticks:int) -> int:
        acct = await self.broker.account_summary()
        # Try to read NLV/NetLiquidation; fall back to 25k if missing
        nlv = 25000.0
        for k,v in acct.items():
            if "NetLiquidation" in k:
                try: nlv = float(v)
                except: pass
        risk = min(self.cfg.risk_pct * nlv, self.cfg.risk_abs_cap)
        per_contract = stop_ticks * self._tick_value()
        return max(1, int(risk // per_contract))

    # ------------- indicators -------------
    def _update_indicators(self):
        df = self.df
        if df.empty: return
        px = df["c"]
        df["ema_f"] = px.ewm(span=self.cfg.ema_fast, adjust=False).mean()
        df["ema_s"] = px.ewm(span=self.cfg.ema_slow, adjust=False).mean()
        # session VWAP
        # reset when date changes in ET or at 09:30 open — simple approach: VWAP over today's bars
        et = self.df["ts"].dt.tz_convert(self.cfg.tz)
        today_mask = et.dt.date == et.iloc[-1].date()
        df.loc[today_mask, "pv"] = (df.loc[today_mask,"c"] * df.loc[today_mask,"v"]).astype(float)
        df.loc[~today_mask, "pv"] = float("nan")
        v_sum = df.loc[today_mask,"v"].cumsum()
        pv_sum = df.loc[today_mask,"pv"].cumsum()
        df.loc[today_mask,"vwap"] = pv_sum / v_sum
        # 1-min ATR(14)
        tr = pd.concat([
            (df["h"] - df["l"]).abs(),
            (df["h"] - df["c"].shift()).abs(),
            (df["l"] - df["c"].shift()).abs()
        ], axis=1).max(axis=1)
        df["atr14"] = tr.rolling(14).mean()
        self.df = df

    # ------------- signals -------------
    def _long_signal(self) -> bool:
        if len(self.df) < 25: return False
        b = self.df.iloc[-1]
        b1 = self.df.iloc[-2]
        if pd.isna(b["vwap"]) or pd.isna(b["ema_f"]) or pd.isna(b["ema_s"]) or pd.isna(b["atr14"]):
            return False
        # regime filter
        if not (self.cfg.min_atr <= b["atr14"] <= self.cfg.max_atr):
            return False
        trending = b["ema_f"] > b["ema_s"] and b1["ema_f"] >= b1["ema_s"]
        pullback = (b["l"] <= b["ema_f"]) and (b["c"] >= b["ema_f"]) and (b["c"] > b["ema_s"])
        above_vwap = b["c"] > b["vwap"]
        return trending and pullback and above_vwap

    def _short_signal(self) -> bool:
        if len(self.df) < 25: return False
        b = self.df.iloc[-1]
        b1 = self.df.iloc[-2]
        if pd.isna(b["vwap"]) or pd.isna(b["ema_f"]) or pd.isna(b["ema_s"]) or pd.isna(b["atr14"]):
            return False
        if not (self.cfg.min_atr <= b["atr14"] <= self.cfg.max_atr):
            return False
        trending = b["ema_f"] < b["ema_s"] and b1["ema_f"] <= b1["ema_s"]
        pullback = (b["h"] >= b["ema_f"]) and (b["c"] <= b["ema_f"]) and (b["c"] < b["ema_s"])
        below_vwap = b["c"] < b["vwap"]
        return trending and pullback and below_vwap

    # ------------- order helpers -------------
    def _round_to_tick(self, price: float) -> float:
        ts = self._tick_size()
        return round(round(price / ts) * ts, 2)

    async def _enter(self, side: str, price: float):
        stop_ticks = self.cfg.sl_ticks
        tp_ticks   = self.cfg.tp_ticks
        qty = await self._contracts(stop_ticks)
        ts = self._tick_size()
        if side == "BUY":
            tp  = self._round_to_tick(price + tp_ticks * ts)
            sl  = self._round_to_tick(price - stop_ticks * ts)
        else:
            tp  = self._round_to_tick(price - tp_ticks * ts)
            sl  = self._round_to_tick(price + stop_ticks * ts)

        # Send parent (marketable) and manage TP/SL as child OCO if your broker supports it.
        # With Tradovate, you can place a bracket; here we just submit a LIMIT at last price
        # and immediately set internal tp/sl levels; the adapter can be extended to OSO.
        result = await self.broker.place_order(
            side=side, qty=qty, order_type="MKT", expiry=self._current_expiry()
        )
        self.state.update({
            "position": qty if side=="BUY" else -qty,
            "entry_price": price,
            "tp_price": tp,
            "stop_price": sl,
            "entry_time": datetime.utcnow().isoformat()
        })
        return result

    async def _manage_open(self, last_price: float):
        st = self.state
        if st["position"] == 0: return

        # move to breakeven after BE trigger
        ts = self._tick_size()
        be_move = self.cfg.be_trigger_ticks * ts
        if st["position"] > 0:  # long
            if last_price >= st["entry_price"] + be_move:
                st["stop_price"] = max(st["stop_price"], self._round_to_tick(st["entry_price"] + ts))
            # check exits
            if last_price >= st["tp_price"]:
                await self._flatten()
            elif last_price <= st["stop_price"]:
                await self._flatten()
        else:  # short
            if last_price <= st["entry_price"] - be_move:
                st["stop_price"] = min(st["stop_price"], self._round_to_tick(st["entry_price"] - ts))
            if last_price <= st["tp_price"]:
                await self._flatten()
            elif last_price >= st["stop_price"]:
                await self._flatten()

        # time stop
        ent = datetime.fromisoformat(st["entry_time"])
        if (datetime.utcnow() - ent).total_seconds() > self.cfg.time_stop_sec:
            await self._flatten()

    async def _flatten(self):
        st = self.state
        if st["position"] == 0: return
        side = "SELL" if st["position"] > 0 else "BUY"
        qty = abs(st["position"])
        await self.broker.place_order(side=side, qty=qty, order_type="MKT", expiry=self._current_expiry())
        st["position"] = 0
        st["entry_price"] = st["tp_price"] = st["stop_price"] = st["entry_time"] = None
        st["trades_today"] += 1

    # ------------- expiry helper -------------
    def _current_expiry(self) -> str:
        """
        Simple quarterly ES expiry estimator (YYYYMM). For production, resolve via broker.
        """
        now = self._now_et()
        m = ((now.month-1)//3+1)*3
        y = now.year
        if m<now.month:  # past current quarter -> next
            m+=3
            if m>12: m-=12; y+=1
        return f"{y}{m:02d}"

    # ------------- main loop -------------
    async def run(self):
        self.running = True
        self.state.update({"trades_today":0, "pnl_today":0.0})

        async for bar in self.stream():  # expects dict(ts,o,h,l,c,v) each minute
            if not self.running: break
            self.state["last_bar"] = bar
            ts = pd.to_datetime(bar["ts"]).tz_localize("UTC")
            row = {"ts": ts, "o":bar["o"], "h":bar["h"], "l":bar["l"], "c":bar["c"], "v":bar["v"]}
            self.df = pd.concat([self.df, pd.DataFrame([row])], ignore_index=True)
            self._update_indicators()

            # session guardrails
            if not self._in_session():
                if self.state["position"] != 0:
                    await self._flatten()
                continue
            if self.state["trades_today"] >= self.cfg.max_trades:
                continue

            # manage open position
            await self._manage_open(bar["c"])

            # check daily loss (requires PnL; broker-specific—here we skip exact calc)
            # if self.state["pnl_today"] <= -self.cfg.max_daily_loss: continue

            # entries (only if flat)
            if self.state["position"] == 0:
                if self._long_signal():
                    await self._enter("BUY", price=bar["c"])
                elif self._short_signal():
                    await self._enter("SELL", price=bar["c"])

        self.running = False

    def stop(self):
        self.running = False

    def status(self) -> Dict[str, Any]:
        return {"running": self.running, "state": self.state, "config": asdict(self.cfg)}
