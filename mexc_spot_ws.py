#!/usr/bin/env python3
"""
MEXC Spot WebSocket Market Streams client.
Doc: https://www.mexc.com/api-docs/spot-v3/websocket-market-streams

- Endpoint: wss://wbs-api.mexc.com/ws
- Subscribe/unsubscribe and PING/PONG are JSON; market data push is Protobuf (optional decode).
- Symbol must be UPPERCASE (e.g. BTCUSDT).
- Max 30 subscriptions per connection; send PING to keep alive (no traffic 60s -> disconnect).
"""
import asyncio
import json
import time
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Union

import aiohttp


MEXC_WS_URL = "wss://wbs-api.mexc.com/ws"

# Channel templates (symbol in UPPERCASE, e.g. BTCUSDT)
CHANNEL_DEALS = "spot@public.aggre.deals.v3.api.pb@{interval}@{symbol}"  # 100ms or 10ms
CHANNEL_DEPTH = "spot@public.aggre.depth.v3.api.pb@{interval}@{symbol}"  # 100ms or 10ms
CHANNEL_LIMIT_DEPTH = "spot@public.limit.depth.v3.api.pb@{symbol}@{level}"  # level 5,10,20
CHANNEL_BOOK_TICKER = "spot@public.aggre.bookTicker.v3.api.pb@{interval}@{symbol}"
CHANNEL_BOOK_TICKER_BATCH = "spot@public.bookTicker.batch.v3.api.pb@{symbol}"
CHANNEL_KLINE = "spot@public.kline.v3.api.pb@{symbol}@{interval}"  # Min1, Min5, Min15, ...
CHANNEL_MINI_TICKER = "spot@public.miniTicker.v3.api.pb@{symbol}@{timezone}"  # UTC+8 etc.
CHANNEL_MINI_TICKERS = "spot@public.miniTickers.v3.api.pb@{timezone}"


def _symbol_ccxt_to_ws(symbol: str) -> str:
    """Convert CCXT symbol (BTC/USDT) to WS format (BTCUSDT)."""
    return symbol.replace("/", "").upper()


def channel_deals(symbol: str, interval_ms: str = "100ms") -> str:
    return CHANNEL_DEALS.format(interval=interval_ms, symbol=_symbol_ccxt_to_ws(symbol))


def channel_depth(symbol: str, interval_ms: str = "100ms") -> str:
    return CHANNEL_DEPTH.format(interval=interval_ms, symbol=_symbol_ccxt_to_ws(symbol))


def channel_limit_depth(symbol: str, level: int = 5) -> str:
    return CHANNEL_LIMIT_DEPTH.format(symbol=_symbol_ccxt_to_ws(symbol), level=level)


def channel_book_ticker(symbol: str, interval_ms: str = "100ms") -> str:
    return CHANNEL_BOOK_TICKER.format(interval=interval_ms, symbol=_symbol_ccxt_to_ws(symbol))


def channel_kline(symbol: str, interval: str = "Min15") -> str:
    return CHANNEL_KLINE.format(symbol=_symbol_ccxt_to_ws(symbol), interval=interval)


class MEXCSpotWS:
    """
    Async WebSocket client for MEXC Spot market streams.
    Control (subscribe/unsubscribe/PONG) is JSON; data push is Protobuf (binary).
    """

    def __init__(
        self,
        url: str = MEXC_WS_URL,
        ping_interval: float = 30.0,
        proxy: Optional[str] = None,
    ):
        self.url = url
        self.ping_interval = ping_interval
        self.proxy = proxy
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._subscriptions: List[str] = []
        self._ping_task: Optional[asyncio.Task] = None
        self._recv_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        """Open WebSocket connection."""
        kwargs: Dict[str, Any] = {}
        if self.proxy:
            kwargs["proxy"] = self.proxy
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(self.url, **kwargs)

    async def close(self) -> None:
        """Close WebSocket and session."""
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._session:
            await self._session.close()
            self._session = None

    def subscribe(self, channels: List[str]) -> None:
        """Queue subscription (sent on next _send_control)."""
        for ch in channels:
            if ch not in self._subscriptions and len(self._subscriptions) < 30:
                self._subscriptions.append(ch)

    def unsubscribe(self, channels: List[str]) -> None:
        """Queue unsubscription."""
        for ch in channels:
            if ch in self._subscriptions:
                self._subscriptions.remove(ch)

    async def _send_json(self, obj: Dict[str, Any]) -> None:
        if not self._ws or self._ws.closed:
            return
        await self._ws.send_str(json.dumps(obj))

    async def send_subscription(self, channels: List[str]) -> None:
        """Send SUBSCRIPTION request."""
        await self._send_json({"method": "SUBSCRIPTION", "params": channels})
        for ch in channels:
            if ch not in self._subscriptions and len(self._subscriptions) < 30:
                self._subscriptions.append(ch)

    async def send_unsubscription(self, channels: List[str]) -> None:
        """Send UNSUBSCRIPTION request."""
        await self._send_json({"method": "UNSUBSCRIPTION", "params": channels})
        for ch in channels:
            if ch in self._subscriptions:
                self._subscriptions.remove(ch)

    async def send_ping(self) -> None:
        """Send PING to keep connection alive."""
        await self._send_json({"method": "PING"})

    async def _ping_loop(self) -> None:
        while True:
            await asyncio.sleep(self.ping_interval)
            try:
                await self.send_ping()
            except Exception:
                break

    def _parse_frame(self, msg: aiohttp.WSMessage) -> Optional[Dict[str, Any]]:
        """Parse one frame into a dict: type 'control' (JSON) or 'binary' (protobuf bytes)."""
        if msg.type == aiohttp.WSMsgType.TEXT:
            try:
                data = json.loads(msg.data)
                return {"type": "control", "data": data}
            except json.JSONDecodeError:
                return {"type": "text", "raw": msg.data}
        if msg.type == aiohttp.WSMsgType.BINARY:
            return {"type": "binary", "data": msg.data}
        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
            return {"type": "close", "data": getattr(msg, "data", None)}
        return None

    async def run_forever(
        self,
        on_message: Optional[Callable[[Dict[str, Any]], Any]] = None,
    ) -> None:
        """
        Run receive loop forever; call on_message for each frame.
        Frame: {"type": "control"|"binary"|"text"|"close", "data": ...}.
        """
        if not self._ws:
            await self.connect()
        self._ping_task = asyncio.create_task(self._ping_loop())

        try:
            async for msg in self._ws:
                parsed = self._parse_frame(msg)
                if parsed is None:
                    continue
                if parsed["type"] == "close":
                    break
                if on_message:
                    try:
                        if asyncio.iscoroutinefunction(on_message):
                            await on_message(parsed)
                        else:
                            on_message(parsed)
                    except Exception as e:
                        print(f"  mexc_ws on_message error: {e}")
        finally:
            if self._ping_task and not self._ping_task.done():
                self._ping_task.cancel()

    async def stream(
        self,
        channels: List[str],
        on_message: Optional[Callable[[Dict[str, Any]], Any]] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """
        Subscribe to channels and yield messages until closed.
        Yields dicts: {"type": "control"|"binary", "data": ...}.
        """
        await self.connect()
        await self.send_subscription(channels)
        queue: asyncio.Queue = asyncio.Queue()

        def put(parsed: Dict[str, Any]) -> None:
            queue.put_nowait(parsed)

        async def recv_loop() -> None:
            try:
                w = self._ws
                if not w:
                    return
                async for msg in w:
                    parsed = self._parse_frame(msg)
                    if parsed and parsed["type"] == "close":
                        queue.put_nowait(parsed)
                        return
                    if parsed:
                        queue.put_nowait(parsed)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                queue.put_nowait({"type": "error", "data": str(e)})

        self._recv_task = asyncio.create_task(recv_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())

        try:
            while True:
                parsed = await asyncio.wait_for(queue.get(), timeout=self.ping_interval + 5)
                if parsed.get("type") in ("close", "error"):
                    break
                if on_message:
                    try:
                        if asyncio.iscoroutinefunction(on_message):
                            await on_message(parsed)
                        else:
                            on_message(parsed)
                    except Exception:
                        pass
                yield parsed
        finally:
            if self._ping_task and not self._ping_task.done():
                self._ping_task.cancel()
            if self._recv_task and not self._recv_task.done():
                self._recv_task.cancel()


async def demo() -> None:
    """Subscribe to trades and book ticker for BTCUSDT, print control and binary info."""
    import os
    proxy = os.environ.get("MEXC_PROXY") or os.environ.get("HTTPS_PROXY")
    ws = MEXCSpotWS(ping_interval=25.0, proxy=proxy)
    channels = [
        channel_deals("BTC/USDT", "100ms"),
        channel_book_ticker("BTC/USDT", "100ms"),
    ]
    print(f"Subscribing to: {channels}")
    try:
        await ws.connect()
        await ws.send_subscription(channels)
        count = 0
        async for msg in ws._ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                obj = json.loads(msg.data)
                print(f"  [control] {obj}")
            elif msg.type == aiohttp.WSMsgType.BINARY:
                count += 1
                print(f"  [binary] frame len={len(msg.data)} (total {count})")
            if count >= 5:
                break
    finally:
        await ws.close()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(demo())
