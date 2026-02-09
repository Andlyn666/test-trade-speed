#!/usr/bin/env python3
"""
MEXC Spot User Data WebSocket — 用户私有订单流.
Doc: https://www.mexc.com/api-docs/spot-v3/websocket-user-data-streams

- 先 POST /api/v3/userDataStream 获取 listenKey，再连接 wss://wbs-api.mexc.com/ws?listenKey=xxx
- 订阅 spot@private.orders.v3.api.pb 接收订单状态推送（可用来测限价单「上架」延迟）
"""
import asyncio
import hashlib
import hmac
import json
import re
import time
from typing import Any, Callable, Dict, List, Optional

import aiohttp

try:
    import blackboxprotobuf
except ImportError:
    blackboxprotobuf = None  # type: ignore


MEXC_API_BASE = "https://api.mexc.com"
MEXC_WS_BASE = "wss://wbs-api.mexc.com/ws"
USER_ORDERS_CHANNEL = "spot@private.orders.v3.api.pb"


def _sign(secret: str, query_and_body: str) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        query_and_body.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest().lower()


async def get_listen_key(
    api_key: str,
    secret: str,
    proxy: Optional[str] = None,
) -> str:
    """POST /api/v3/userDataStream 获取 listenKey（需 SPOT 账户读权限）."""
    ts = int(time.time() * 1000)
    total_params = f"timestamp={ts}"
    sig = _sign(secret, total_params)
    url = f"{MEXC_API_BASE}/api/v3/userDataStream?timestamp={ts}&signature={sig}"
    headers = {"X-MEXC-APIKEY": api_key, "Content-Type": "application/json"}
    kwargs: Dict[str, Any] = {"headers": headers}
    if proxy:
        kwargs["proxy"] = proxy
    async with aiohttp.ClientSession() as session:
        async with session.post(url, **kwargs) as resp:
            resp.raise_for_status()
            data = await resp.json()
    key = data.get("listenKey")
    if not key:
        raise RuntimeError(f"No listenKey in response: {data}")
    return key


class MEXCUserOrdersWS:
    """
    用户订单推送 WebSocket：连接带 listenKey 的 ws，订阅 spot@private.orders.v3.api.pb，
    收到订单推送时回调 on_order(order_id, first_seen_ns)。
    推送可能是 JSON 或 Protobuf，优先按 JSON 解析；若为 binary 则忽略（或后续接 proto 解码）。
    """

    def __init__(
        self,
        listen_key: str,
        on_order: Optional[Callable[[str, int], None]] = None,
        proxy: Optional[str] = None,
        ping_interval: float = 30.0,
    ):
        self.listen_key = listen_key
        self.on_order = on_order
        self.proxy = proxy
        self.ping_interval = ping_interval
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._ping_task: Optional[asyncio.Task] = None

    @property
    def url(self) -> str:
        return f"{MEXC_WS_BASE}?listenKey={self.listen_key}"

    async def connect(self) -> None:
        kwargs: Dict[str, Any] = {}
        if self.proxy:
            kwargs["proxy"] = self.proxy
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(self.url, **kwargs)

    async def close(self) -> None:
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
            self._ws = None
        if self._session:
            await self._session.close()
            self._session = None

    async def _send_json(self, obj: Dict[str, Any]) -> None:
        if self._ws and not self._ws.closed:
            await self._ws.send_str(json.dumps(obj))

    async def subscribe_orders(self) -> None:
        await self._send_json({"method": "SUBSCRIPTION", "params": [USER_ORDERS_CHANNEL]})

    async def send_ping(self) -> None:
        await self._send_json({"method": "PING"})

    # 订单 id 格式：MEXC 常见为 C02__ 开头或长数字
    _ORDER_ID_RE = re.compile(r"^(C02__\d+|\d{15,})$")

    def _extract_order_ids(self, obj: Any, out: List[str]) -> None:
        """递归从 dict/list 中提取 clientId、clientOrderId、orderId 或形似订单 id 的字符串."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if v is None:
                    continue
                if isinstance(v, (bytes, bytearray)):
                    v = v.decode("utf-8", errors="replace")
                if k in ("clientId", "clientOrderId", "orderId") and v not in (None, ""):
                    out.append(str(v).strip('"'))
                elif isinstance(v, str) and self._ORDER_ID_RE.match(v.strip('"')):
                    out.append(v.strip('"'))
                else:
                    self._extract_order_ids(v, out)
        elif isinstance(obj, list):
            for x in obj:
                self._extract_order_ids(x, out)
        elif isinstance(obj, (bytes, bytearray)):
            s = obj.decode("utf-8", errors="replace")
            if self._ORDER_ID_RE.match(s.strip('"')):
                out.append(s.strip('"'))
        elif isinstance(obj, str) and self._ORDER_ID_RE.match(obj.strip('"')):
            out.append(obj.strip('"'))

    def _parse_order_update(self, data: Dict[str, Any], now_ns: int) -> None:
        """从 JSON 推送中提取订单 id 并回调；支持嵌套及 protobuf 转 JSON 的多种字段名."""
        ids: List[str] = []
        self._extract_order_ids(data, ids)
        for oid in ids:
            if oid and self.on_order:
                self.on_order(oid, now_ns)

    async def run_forever(self) -> None:
        """连接、订阅订单流并持续收包；收到订单推送时调用 on_order."""
        if not self._ws:
            await self.connect()
        await self.subscribe_orders()
        self._ping_task = asyncio.create_task(self._ping_loop())

        try:
            async for msg in self._ws:
                now_ns = time.perf_counter_ns()
                raw: Optional[bytes] = None
                if msg.type == aiohttp.WSMsgType.TEXT:
                    raw = msg.data if isinstance(msg.data, bytes) else msg.data.encode("utf-8")
                elif msg.type == aiohttp.WSMsgType.BINARY:
                    raw = msg.data
                if raw is not None:
                    data = None
                    try:
                        text = raw.decode("utf-8", errors="replace")
                        data = json.loads(text)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        pass
                    if data:
                        if data.get("msg") == "PONG":
                            continue
                        if "privateOrders" in str(data) or "privateorders" in str(data).lower() or "order" in str(data).lower():
                            self._parse_order_update(data, now_ns)
                    elif raw and blackboxprotobuf is not None:
                        try:
                            decoded, _ = blackboxprotobuf.decode_message(raw)
                            if isinstance(decoded, dict):
                                ids_list: List[str] = []
                                self._extract_order_ids(decoded, ids_list)
                                for oid in ids_list:
                                    if oid and self.on_order:
                                        self.on_order(oid, now_ns)
                        except Exception:
                            pass
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                    break
        finally:
            if self._ping_task and not self._ping_task.done():
                self._ping_task.cancel()

    async def _ping_loop(self) -> None:
        while True:
            await asyncio.sleep(self.ping_interval)
            try:
                await self.send_ping()
            except Exception:
                break
