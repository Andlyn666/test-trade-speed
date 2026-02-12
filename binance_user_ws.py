#!/usr/bin/env python3
"""
Binance User Data WebSocket — 用户私有订单流.
Doc: https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream
     https://developers.binance.com/docs/derivatives/usds-margined-futures/user-data-streams

- Spot: POST /api/v3/userDataStream -> wss://stream.binance.com:9443/ws/<listenKey>
- Futures: POST /fapi/v1/listenKey -> wss://fstream.binance.com/ws/<listenKey>
- Order updates: executionReport (spot) / ORDER_TRADE_UPDATE (futures)
"""
import asyncio
import hashlib
import hmac
import json
import time
from typing import Any, Callable, Dict, Optional

import aiohttp


BINANCE_SPOT_API = "https://api.binance.com"
BINANCE_FUTURES_API = "https://fapi.binance.com"
BINANCE_SPOT_WS = "wss://stream.binance.com:9443/ws"
BINANCE_FUTURES_WS = "wss://fstream.binance.com/ws"


def _sign(secret: str, query: str) -> str:
    return hmac.new(secret.encode(), query.encode(), hashlib.sha256).hexdigest()


async def get_listen_key(
    api_key: str,
    secret: str,
    market_type: str,  # 'spot' | 'swap'
    proxy: Optional[str] = None,
) -> str:
    """Get listenKey for user data stream. Spot uses API key only; Futures uses signed request."""
    kwargs: Dict[str, Any] = {"headers": {"X-MBX-APIKEY": api_key}}
    if proxy:
        kwargs["proxy"] = proxy

    if market_type == "swap":
        ts = int(time.time() * 1000)
        params = f"timestamp={ts}"
        sig = _sign(secret, params)
        url = f"{BINANCE_FUTURES_API}/fapi/v1/listenKey?{params}&signature={sig}"
    else:
        url = f"{BINANCE_SPOT_API}/api/v3/userDataStream"

    async with aiohttp.ClientSession() as session:
        async with session.post(url, **kwargs) as resp:
            resp.raise_for_status()
            data = await resp.json()

    key = data.get("listenKey")
    if not key:
        raise RuntimeError(f"No listenKey in response: {data}")
    return key


def _extract_order_id_from_event(data: Dict[str, Any]) -> Optional[str]:
    """Extract orderId from Binance user stream event."""
    event = data.get("e") or data.get("eventType")
    if event == "executionReport":
        # Spot format
        oid = data.get("i") or data.get("orderId")
        if oid is not None:
            return str(oid)
        return data.get("c")  # clientOrderId
    if event == "ORDER_TRADE_UPDATE":
        # Futures format
        o = data.get("o") or {}
        oid = o.get("i") or o.get("orderId")
        if oid is not None:
            return str(oid)
        return o.get("c")  # clientOrderId
    return None


class BinanceUserOrdersWS:
    """
    Binance user order stream: connects with listenKey, parses order updates,
    calls on_order(order_id, first_seen_ns) when an order is seen.
    """

    def __init__(
        self,
        listen_key: str,
        market_type: str,  # 'spot' | 'swap'
        on_order: Optional[Callable[[str, int], None]] = None,
        proxy: Optional[str] = None,
        ping_interval: float = 30.0,
    ):
        self.listen_key = listen_key
        self.market_type = market_type
        self.on_order = on_order
        self.proxy = proxy
        self.ping_interval = ping_interval
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def url(self) -> str:
        base = BINANCE_FUTURES_WS if self.market_type == "swap" else BINANCE_SPOT_WS
        return f"{base}/{self.listen_key}"

    async def connect(self) -> None:
        kwargs: Dict[str, Any] = {}
        if self.proxy:
            kwargs["proxy"] = self.proxy
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(self.url, **kwargs)

    async def close(self) -> None:
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._session:
            await self._session.close()
            self._session = None

    async def run_forever(self) -> None:
        if not self._ws:
            await self.connect()

        try:
            async for msg in self._ws:
                now_ns = time.perf_counter_ns()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except json.JSONDecodeError:
                        continue
                    if data.get("e") == "pong" or data.get("result") is not None:
                        continue
                    oid = _extract_order_id_from_event(data)
                    if oid and self.on_order:
                        self.on_order(oid, now_ns)
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                    break
        finally:
            pass
