import os, httpx, time
from typing import Dict, Any, Optional

BASES = {
    "demo": "https://demo.tradovateapi.com/v1",
    "live": "https://live.tradovateapi.com/v1",
}

class TradovateProvider:
    """
    Minimal Tradovate order & account adapter.
    """

    def __init__(self):
        env = os.getenv("TRADOVATE_ENV", "demo").lower()
        self.base = BASES.get(env, BASES["demo"])
        self.username = os.getenv("TRADOVATE_USERNAME")
        self.api_key = os.getenv("TRADOVATE_API_KEY")
        self.api_secret = os.getenv("TRADOVATE_API_SECRET")
        self.account_id = int(os.getenv("TRADOVATE_ACCOUNT_ID", "0"))
        self._token: Optional[str] = None
        self._token_exp: float = 0
        self.client = httpx.AsyncClient(timeout=15.0)

    async def _auth(self):
        now = time.time()
        if self._token and now < self._token_exp - 30:
            return
        r = await self.client.post(
            f"{self.base}/auth/accesstokenrequest",
            json={
                "name": self.username,
                "secret": self.api_secret,
                "appId": self.api_key,
                "appVersion": "1.0",
                "deviceId": "server",
            },
        )
        r.raise_for_status()
        data = r.json()
        self._token = data["accessToken"]
        self._token_exp = now + 3000

    def _h(self) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    async def account_summary(self) -> Dict[str, Any]:
        await self._auth()
        r = await self.client.get(f"{self.base}/account/item?id={self.account_id}", headers=self._h())
        r.raise_for_status()
        return r.json()

    async def positions(self) -> Dict[str, Any]:
        await self._auth()
        r = await self.client.get(f"{self.base}/position/list", headers=self._h())
        r.raise_for_status()
        return r.json()

    async def place_order(self, side: str, qty: int, order_type: str,
                          expiry: str, limit_price: Optional[float] = None,
                          tif: str = "GTC") -> Dict[str, Any]:
        await self._auth()
        order = {
            "accountId": self.account_id,
            "symbol": "ES",
            "orderQty": qty,
            "action": side.upper(),
            "timeInForce": tif.upper(),
        }
        if order_type.upper() == "MKT":
            order["orderType"] = "Market"
        elif order_type.upper() == "LMT":
            order["orderType"] = "Limit"
            order["price"] = limit_price
        else:
            raise ValueError("order_type must be MKT or LMT")

        r = await self.client.post(f"{self.base}/order/placeOrder", headers=self._h(), json=order)
        r.raise_for_status()
        return r.json()
