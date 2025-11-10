import os, time, asyncio, datetime as dt
import httpx
import pandas as pd
from typing import Any, Dict, List

MASSIVE_BASE = "https://api.massive.com"

class MassiveStocksProvider:
    """
    Wrapper for Massive.io Stocks plan endpoints (SPY/^GSPC proxy).
    """

    def __init__(self, api_key: str | None = None, timeout: float = 10.0):
        self.api_key = api_key or os.getenv("MASSIVE_API_KEY")
        self.client = httpx.AsyncClient(
            timeout=timeout,
            headers={"Authorization": f"Bearer {self.api_key}"}
        )

    async def last_price(self, symbol: str) -> Dict[str, Any]:
        """
        Fetches latest trade/quote snapshot.
        """
        r = await self.client.get(f"{MASSIVE_BASE}/v1/stocks/snapshot/{symbol}")
        r.raise_for_status()
        data = r.json()
        return {
            "symbol": symbol,
            "price": data.get("last", {}).get("price"),
            "timestamp": data.get("last", {}).get("timestamp"),
            "source": "massive-stocks"
        }

    async def intraday(self, symbol: str, limit: int = 100) -> Dict[str, Any]:
        """
        Fetch short-term intraday aggregates.
        """
        end = int(time.time())
        start = end - 60 * 60 * 6  # past 6 hours
        r = await self.client.get(
            f"{MASSIVE_BASE}/v1/stocks/aggregates",
            params={"symbol": symbol, "from": start, "to": end,
                    "timespan": "minute", "limit": limit}
        )
        r.raise_for_status()
        return r.json()

    async def daily_aggregates(self, symbol: str, start: dt.date,
                               end: dt.date, adjusted: bool = True,
                               page_days: int = 365) -> pd.DataFrame:
        """
        Retrieve daily OHLCV data between start and end.
        """
        frames: List[pd.DataFrame] = []
        cur = start
        while cur <= end:
            cur_end = min(cur + dt.timedelta(days=page_days - 1), end)
            params = {
                "symbol": symbol,
                "from": int(dt.datetime.combine(cur, dt.time.min).timestamp()),
                "to": int(dt.datetime.combine(cur_end, dt.time.max).timestamp()),
                "timespan": "day",
                "adjusted": "true" if adjusted else "false",
                "limit": 50000,
            }
            r = await self.client.get(f"{MASSIVE_BASE}/v1/stocks/aggregates", params=params)
            r.raise_for_status()
            data = r.json()
            bars = data.get("results") or data.get("bars") or []
            if bars:
                df = pd.DataFrame(bars)
                rename = {"timestamp":"ts","open":"o","high":"h","low":"l","close":"c","volume":"v"}
                df = df.rename(columns=rename)
                df["ts"] = pd.to_datetime(df["ts"], unit="s", utc=True, errors="coerce")
                df = df[["ts","o","h","l","c","v"]].sort_values("ts")
                frames.append(df)
            cur = cur_end + dt.timedelta(days=1)
            await asyncio.sleep(0.15)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=["ts","o","h","l","c","v"])

    async def ten_years_sp500(self, symbol: str = "SPY") -> pd.DataFrame:
        """
        Shortcut for ~10 years of SPY daily history.
        """
        end = dt.date.today()
        start = end - dt.timedelta(days=365*10)
        return await self.daily_aggregates(symbol, start, end, adjusted=True)
