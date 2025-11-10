# main.py
# FastAPI app for intraday ES/MES trading + research + strategy control.
# - Market data: Massive.io Stocks plan (SPY/^GSPC proxy); swap to futures feed when available.
# - Brokers: pick via env BROKER_KIND = tradovate | mt5 | ibkr  (default: tradovate)
# - Strategy: VWAP Pullback Scalper (intraday, tight risk)
#
# Prereqs (see your project files):
# providers/
#   massive_stocks.py         -> MassiveStocksProvider with ten_years_sp500() & daily_aggregates()
#   broker_tradovate.py       -> TradovateProvider
#   broker_mt5.py             -> MT5Provider
#   broker_ibkr.py            -> IBKRProvider
# strategy/
#   config.py                 -> ScalperConfig
#   intraday_scalper.py       -> IntradayScalper
# research/
#   features.py               -> add_features()

import os
import asyncio
import datetime as dt
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

# ---- Providers / Strategy imports ----
from providers.massive_stocks import MassiveStocksProvider
from research.features import add_features
from strategy.config import ScalperConfig
from strategy.intraday_scalper import IntradayScalper

# Pick broker adapter by environment
BROKER_KIND = os.getenv("BROKER_KIND", "tradovate").lower()
if BROKER_KIND == "tradovate":
    from providers.broker_tradovate import TradovateProvider as BrokerProvider
elif BROKER_KIND == "mt5":
    from providers.broker_mt5 import MT5Provider as BrokerProvider
elif BROKER_KIND == "ibkr":
    from providers.broker_ibkr import IBKRProvider as BrokerProvider
else:
    raise RuntimeError("Unsupported BROKER_KIND. Use tradovate | mt5 | ibkr")

# ---- App init ----
app = FastAPI(title="S&P 500 Intraday Futures Trading API", version="0.2.0")

# Market data (Massive Stocks plan; use SPY/^GSPC as proxy until you enable a futures feed)
MARKET_PROVIDER = MassiveStocksProvider(api_key=os.getenv("MASSIVE_API_KEY"))

# Broker (execution)
BROKER = BrokerProvider()

# Data directory for cached research artifacts
DATA_DIR = os.getenv("DATA_DIR", "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Simple in-memory handle to running strategy
SCALPER = {"runner": None}


# ------------------------ Models ------------------------
class OrderReq(BaseModel):
    side: str = Field(..., regex="^(?i)(buy|sell)$")
    qty: int = Field(..., gt=0)
    order_type: str = Field(..., regex="^(?i)(mkt|lmt)$")
    expiry: str = Field(..., pattern=r"^\d{6}$", description="YYYYMM (e.g., 202512 for Dec-2025)")
    limit_price: Optional[float] = None
    tif: str = Field(default="GTC", regex="^(?i)(DAY|GTC|IOC|FOK)$")


# ------------------------ Health ------------------------
@app.get("/health")
async def health():
    return {"ok": True, "broker": BROKER_KIND}


# ------------------------ Quotes (proxy via Massive Stocks) ------------------------
@app.get("/quotes/{symbol}")
async def quotes(symbol: str = "SPY", limit: int = 120):
    """
    Returns last and intraday aggregates for a symbol available on your Massive Stocks plan.
    Use SPY (ETF) or ^GSPC (index) as an S&P 500 proxy while you don't have a futures entitlement.
    """
    try:
        last = await MARKET_PROVIDER.last_price(symbol)
        intraday = await MARKET_PROVIDER.intraday(symbol, limit=limit)
        return {"last": last, "intraday": intraday}
    except Exception as e:
        raise HTTPException(502, f"market data error: {e}")


# ------------------------ Broker info ------------------------
@app.get("/account")
async def account():
    try:
        return await BROKER.account_summary()
    except Exception as e:
        raise HTTPException(502, f"broker error: {e}")


@app.get("/positions")
async def positions():
    try:
        return await BROKER.positions()
    except Exception as e:
        raise HTTPException(502, f"broker error: {e}")


# ------------------------ Orders ------------------------
@app.post("/orders")
async def orders(req: OrderReq):
    try:
        return await BROKER.place_order(
            side=req.side,
            qty=req.qty,
            order_type=req.order_type,
            expiry=req.expiry,
            limit_price=req.limit_price,
            tif=req.tif,
        )
    except Exception as e:
        raise HTTPException(400, f"order rejected: {e}")


# ------------------------ Research: Backfill & Features ------------------------
@app.post("/research/backfill")
async def backfill(symbol: str = "SPY", years: int = 10):
    """
    Pull ~N years of daily OHLCV from Massive Stocks and save to Parquet.
    Default: 10 years of SPY.
    """
    try:
        if years == 10:
            df = await MARKET_PROVIDER.ten_years_sp500(symbol)
        else:
            end = dt.date.today()
            start = end - dt.timedelta(days=365 * years + 7)
            df = await MARKET_PROVIDER.daily_aggregates(symbol, start, end, adjusted=True)
        out_fp = os.path.join(DATA_DIR, f"{symbol}_1d.parquet")
        df.to_parquet(out_fp, index=False)
        return {"ok": True, "rows": int(df.shape[0]), "file": out_fp}
    except Exception as e:
        raise HTTPException(502, f"backfill error: {e}")


@app.post("/research/features")
async def build_features(symbol: str = "SPY"):
    """
    Compute research features on cached OHLCV and persist as Parquet.
    """
    try:
        src = os.path.join(DATA_DIR, f"{symbol}_1d.parquet")
        if not os.path.exists(src):
            raise FileNotFoundError(f"No backfill found at {src}. Run /research/backfill first.")
        df = pd.read_parquet(src)
        fdf = add_features(df)
        out_fp = os.path.join(DATA_DIR, f"{symbol}_features.parquet")
        fdf.to_parquet(out_fp, index=False)
        return {"ok": True, "rows": int(fdf.shape[0]), "file": out_fp}
    except Exception as e:
        raise HTTPException(502, f"features error: {e}")


# ------------------------ Strategy: Intraday Scalper ------------------------
async def YOUR_MINUTE_STREAM():
    """
    >>> IMPORTANT <<<
    Replace this with your real minute-bar async generator.
    Options:
      - Aggregate Massive "aggregates-per-second" -> minutes
      - Use Tradovate/Ninja/your broker WS minute bars
    Must yield dict(ts,o,h,l,c,v) with ts in ISO8601 UTC.
    """
    # Example: placeholder that prevents silent runs
    raise NotImplementedError(
        "Wire YOUR_MINUTE_STREAM() to your live minute bars (Massive or broker WS)."
    )


@app.post("/strategy/start")
async def strategy_start(use_mes: bool = False):
    """
    Start the intraday VWAP-pullback scalper.
    - use_mes=True → trade Micro ES (MES) for smaller tick value during tuning.
    """
    if SCALPER["runner"] and SCALPER["runner"].running:
        return {"ok": True, "msg": "already running", "config": SCALPER["runner"].cfg.__dict__}

    cfg = ScalperConfig(use_mes=use_mes)

    runner = IntradayScalper(
        data_stream=YOUR_MINUTE_STREAM,  # <- replace with your actual stream function
        broker=BROKER,
        cfg=cfg,
    )
    SCALPER["runner"] = runner
    asyncio.create_task(runner.run())
    return {"ok": True, "msg": "scalper started", "config": cfg.__dict__}


@app.post("/strategy/stop")
async def strategy_stop():
    r = SCALPER.get("runner")
    if r:
        r.stop()
    return {"ok": True, "msg": "stop requested"}


@app.get("/strategy/status")
async def strategy_status():
    r = SCALPER.get("runner")
    if not r:
        return {"running": False}
    return r.status()
