#!/usr/bin/env python3
"""
Exchange Latency Tester - MEXC / Binance spot & swap 延迟压测工具.
"""
import argparse
import asyncio
import json
import math
import os
import time
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import ccxt.async_support as ccxt
import pandas as pd
from tabulate import tabulate

try:
    from mexc_user_ws import get_listen_key as mexc_get_listen_key, MEXCUserOrdersWS
except ImportError:
    mexc_get_listen_key = None  # type: ignore
    MEXCUserOrdersWS = None  # type: ignore

try:
    from binance_user_ws import get_listen_key as binance_get_listen_key, BinanceUserOrdersWS
except ImportError:
    binance_get_listen_key = None  # type: ignore
    BinanceUserOrdersWS = None  # type: ignore


# ---------------------------------------------------------------------------
# Config & Exchange
# ---------------------------------------------------------------------------

def get_env_config(exchange: str) -> Tuple[str, str]:
    """Get API key and secret for the given exchange."""
    prefix = exchange.upper()
    api_key = os.environ.get(f"{prefix}_API_KEY", "").strip()
    secret = os.environ.get(f"{prefix}_SECRET", "").strip()
    if not api_key or not secret:
        raise SystemExit(
            f"Error: Set {prefix}_API_KEY and {prefix}_SECRET in environment or .env file."
        )
    return api_key, secret


def get_proxy_from_env() -> Optional[str]:
    """Read proxy from env: MEXC_PROXY/BINANCE_PROXY > HTTPS_PROXY > HTTP_PROXY."""
    for key in ("MEXC_PROXY", "BINANCE_PROXY", "HTTPS_PROXY", "HTTP_PROXY"):
        v = os.environ.get(key, "").strip()
        if v:
            return v
    return None


def build_exchange(
    exchange: str,
    api_key: str,
    secret: str,
    market_type: str,  # 'spot' | 'swap'
    timeout_ms: int = 60000,
    proxy: Optional[str] = None,
) -> ccxt.Exchange:
    """Build ccxt async exchange (mexc or binance)."""
    options: Dict[str, Any] = {}
    if market_type == "swap":
        options["defaultType"] = "swap"
    config: Dict[str, Any] = {
        "apiKey": api_key,
        "secret": secret,
        "options": options,
        "timeout": timeout_ms,
        "enableRateLimit": True,
    }
    if proxy:
        if proxy.lower().startswith("socks"):
            config["socksProxy"] = proxy
        else:
            config["httpsProxy"] = proxy

    if exchange == "mexc":
        return ccxt.mexc(config)
    if exchange == "binance":
        if market_type == "swap":
            return ccxt.binanceusdm(config)
        return ccxt.binance(config)
    raise SystemExit(f"Unknown exchange: {exchange}")


# ---------------------------------------------------------------------------
# Warm-up & clock sync
# ---------------------------------------------------------------------------

async def get_clock_offset_ms(exchange: ccxt.Exchange) -> Optional[float]:
    """
    用 MEXC /api/v3/time 做一次时钟同步，返回 server_time - client_time 的偏移 (ms)。
    用 (t_send + t_receive) / 2 作为 client 参考时刻以减弱 RTT 影响。
    """
    t_send_ms = time.time() * 1000
    try:
        server_time_ms = await exchange.fetch_time()
    except Exception:
        return None
    t_receive_ms = time.time() * 1000
    client_mid_ms = (t_send_ms + t_receive_ms) / 2
    return float(server_time_ms) - client_mid_ms


def _format_exception(e: Exception) -> str:
    """Format exception for debugging: type, message, and optional response."""
    parts = [f"{type(e).__name__}: {e}"]
    # ccxt BaseError often has .response with status/body
    if hasattr(e, "response") and e.response is not None:
        r = e.response
        parts.append(f"  response.status={getattr(r, 'status_code', getattr(r, 'status', None))}")
        if hasattr(r, "text") and r.text:
            parts.append(f"  response.text={r.text[:500]}")
        elif hasattr(r, "content"):
            parts.append(f"  response.content={str(r.content)[:500]}")
    if hasattr(e, "details"):
        parts.append(f"  details={e.details}")
    return "\n".join(parts)


async def warm_up(exchange: ccxt.Exchange, count: int = 5) -> None:
    """预热：触发 TCP 握手并保持长连接。超时时自动重试一次."""
    failed = 0
    last_error: Optional[Exception] = None
    first_error_detail: Optional[str] = None
    for i in range(count):
        try:
            await exchange.fetch_time()
        except Exception as e:
            # 对 RequestTimeout 重试一次（首包可能较慢）
            if type(e).__name__ == "RequestTimeout":
                await asyncio.sleep(0.5)
                try:
                    await exchange.fetch_time()
                except Exception as retry_e:
                    failed += 1
                    last_error = retry_e
                    if first_error_detail is None:
                        first_error_detail = _format_exception(retry_e)
            else:
                failed += 1
                last_error = e
                if first_error_detail is None:
                    first_error_detail = _format_exception(e)
        await asyncio.sleep(0.2)
    if failed == 0:
        print(f"  Warm-up done ({count} requests).\n")
    elif failed < count:
        print(f"  Warm-up done ({count} requests, {failed} failed).\n")
        if first_error_detail:
            print("  First failure details (for debugging fetch_time):")
            for line in first_error_detail.splitlines():
                print(f"    {line}")
            print("  Tip: Try --timeout 90000 if the network is slow or use a VPN closer to MEXC.\n")
    else:
        # All requests failed - cannot proceed
        print(f"  Warm-up FAILED: all {count} requests failed.\n")
        if first_error_detail:
            for line in first_error_detail.splitlines():
                print(f"    {line}")
        raise RuntimeError(
            "All warm-up requests failed. Check network connectivity and proxy settings."
        )


# ---------------------------------------------------------------------------
# Market Order Test (Local + Server Latency)
# ---------------------------------------------------------------------------

def _get_transact_time_ms(order: dict) -> Optional[int]:
    """从订单响应中提取 transactTime (ms)."""
    info = order.get("info") or {}
    if "transactTime" in info:
        return int(info["transactTime"])
    ts = order.get("timestamp")
    if ts is not None:
        return int(ts * 1000) if ts < 1e12 else int(ts)
    return None


async def run_market_order_test(
    exchange: ccxt.Exchange,
    symbol: str,
    runs: int,
    market_type: str = "spot",
    clock_offset_ms: Optional[float] = None,
    exchange_name: str = "mexc",
) -> Tuple[List[float], List[float]]:
    """市价单测试：返回 (local_latencies_ms, server_latencies_ms).
    若提供 clock_offset_ms (server - client)，则 Server Latency = transactTime - (request_ts + offset)."""
    markets = await exchange.load_markets()
    if symbol not in markets:
        raise SystemExit(f"Symbol not found: {symbol}")

    m = markets[symbol]
    min_notional = m.get("limits", {}).get("cost", {}).get("min")
    if min_notional is None or min_notional <= 0:
        min_notional = 10.0 if exchange_name == "binance" else 1.0
    MIN_NOTIONAL_USDT = float(min_notional)
    min_amt = max(
        m.get("limits", {}).get("amount", {}).get("min", 0) or 1e-8,
        1e-8,
    )
    if market_type != "spot":
        amount, order_params = min(min_amt, 0.0001), {}
    else:
        amount, order_params = 0, {"cost": MIN_NOTIONAL_USDT}

    local_list: List[float] = []
    server_list: List[float] = []
    success_count = 0

    for i in range(runs):
        t0_ns = time.perf_counter_ns()
        request_ts_ms = int(time.time() * 1000)
        order = None
        try:
            order = await exchange.create_order(
                symbol=symbol,
                type="market",
                side="buy",
                amount=amount,
                params=order_params,
            )
        except Exception as e:
            print(f"  Run {i+1}/{runs} create_order failed: {e}")
            await asyncio.sleep(1)
            continue

        if order is None:
            continue
        t1_ns = time.perf_counter_ns()
        local_ms = (t1_ns - t0_ns) / 1e6
        success_count += 1
        is_first = (success_count == 1)

        transact_ms = _get_transact_time_ms(order)
        server_ms_val: Optional[float] = None
        if transact_ms is not None:
            if clock_offset_ms is not None:
                # 校正：request_ts 是 client 时间，加上 offset 得到 server 时间轴下的“请求发出”时刻
                server_ms_val = transact_ms - (request_ts_ms + clock_offset_ms)
            else:
                server_ms_val = transact_ms - request_ts_ms
        # Skip first successful run from statistics (cold-start latency)
        if is_first:
            print(f"  --- Run {i+1}/{runs} [SKIPPED from stats - cold start] ---")
        else:
            local_list.append(local_ms)
            if server_ms_val is not None:
                server_list.append(server_ms_val)
            print(f"  --- Run {i+1}/{runs} ---")

        # ---- Verbose per-run output ----
        oid = order.get("id") or order.get("clientOrderId") or "N/A"
        status = order.get("status") or "N/A"
        filled = order.get("filled") or order.get("amount") or "N/A"
        avg_price = order.get("average") or order.get("price") or "N/A"
        cost = order.get("cost") or "N/A"
        info = order.get("info") or {}
        transact_time_raw = info.get("transactTime", "N/A")

        print(f"    Symbol       : {symbol}")
        print(f"    Order ID     : {oid}")
        print(f"    Status       : {status}")
        print(f"    Filled       : {filled}")
        print(f"    Avg Price    : {avg_price}")
        print(f"    Cost         : {cost}")
        print(f"    Local Latency: {local_ms:.2f} ms")
        print(f"    request_ts   : {request_ts_ms} (client wall-clock ms)")
        print(f"    transactTime : {transact_time_raw} (server ms)")
        if server_ms_val is not None:
            print(f"    Server Lat.  : {server_ms_val:.2f} ms (transactTime - adjusted request_ts)")
        else:
            print(f"    Server Lat.  : N/A (transactTime missing)")
        print(f"    Raw info     : {json.dumps(info, default=str, ensure_ascii=False)}")
        print()

        await asyncio.sleep(0.3)  # rate limit

    return local_list, server_list


# ---------------------------------------------------------------------------
# Limit Order Test (WS Propagation)
# ---------------------------------------------------------------------------

# 全局结构：供 watch_orders 消费者与下单协程共享
_pending_orders: Dict[str, dict] = {}
_first_seen_cache: Dict[str, int] = {}  # oid -> first_seen_ns，避免 WS 先于 pending 注册的竞态
_ws_task: Optional[asyncio.Task] = None


async def _watch_orders_consumer(
    exchange: ccxt.Exchange,
    symbol: str,
) -> None:
    """后台任务：消费 watch_orders 并更新 _pending_orders；若不支持则轮询 fetch_order."""
    global _pending_orders, _first_seen_cache
    try:
        while True:
            orders = await exchange.watch_orders(symbol)
            now_ns = time.perf_counter_ns()
            for o in (orders if isinstance(orders, list) else [orders]):
                oid = str(o.get("id") or o.get("clientOrderId") or "")
                if not oid:
                    continue
                if oid not in _first_seen_cache:
                    _first_seen_cache[oid] = now_ns
                if oid in _pending_orders and not _pending_orders[oid].get("first_seen_ns"):
                    _pending_orders[oid]["first_seen_ns"] = _first_seen_cache[oid]
                    _pending_orders[oid]["event"].set()
    except asyncio.CancelledError:
        pass
    except Exception as e:
        if "not supported" in str(e).lower():
            # 标准 ccxt 无 watch_orders 时改用轮询 fetch_order 测「订单可见」延迟
            print("  watch_orders not supported, using REST polling for order visibility.\n")
            try:
                while True:
                    for oid, rec in list(_pending_orders.items()):
                        if rec.get("first_seen_ns"):
                            continue
                        try:
                            o = await exchange.fetch_order(oid, symbol)
                            if o:
                                now_ns = time.perf_counter_ns()
                                if oid not in _first_seen_cache:
                                    _first_seen_cache[oid] = now_ns
                                rec["first_seen_ns"] = _first_seen_cache[oid]
                                rec["event"].set()
                        except Exception:
                            pass
                    await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                pass
        else:
            print(f"  watch_orders error: {e}")


async def run_limit_order_test(
    exchange: ccxt.Exchange,
    symbol: str,
    runs: int,
    batch: int,
    market_type: str = "swap",
    clock_offset_ms: Optional[float] = None,
    exchange_name: str = "mexc",
) -> Tuple[List[float], List[float], List[float]]:
    """Limit order test: returns (local_latencies_ms, server_latencies_ms, ws_propagation_ms)."""
    global _pending_orders, _ws_task, _first_seen_cache
    _ws_task = None

    markets = await exchange.load_markets()
    if symbol not in markets:
        raise SystemExit(f"Symbol not found: {symbol}")

    # Get reference price for the limit order
    try:
        ticker = await exchange.fetch_ticker(symbol)
        mid = float(ticker.get("last") or ticker.get("close") or 0)
        if not mid:
            ob = await exchange.fetch_order_book(symbol, limit=5)
            mid = (ob["bids"][0][0] + ob["asks"][0][0]) / 2
    except Exception as e:
        raise SystemExit(f"Cannot get price for {symbol}: {e}")
    if not mid:
        raise SystemExit(f"Cannot get price for {symbol}")

    m = markets[symbol]
    min_amount = max(
        m.get("limits", {}).get("amount", {}).get("min", 0) or 1e-8,
        1e-8,
    )
    # 限价买单价设为 mid * 0.8，避免成交
    limit_price = round(mid * 0.80, 8)
    if limit_price <= 0:
        limit_price = 0.01
    # 满足最小名义价值 (NOTIONAL): amount * price >= min_notional
    min_notional = m.get("limits", {}).get("cost", {}).get("min")
    if min_notional is None or min_notional <= 0:
        min_notional = 15.0 if exchange_name == "binance" else 1.0  # Binance 保守 15 USDT
    if market_type == "spot" and limit_price > 0:
        min_notional_amt = (min_notional * 1.1) / limit_price  # 留 10% 余量应付 precision 舍入
        step = m.get("precision", {}).get("amount")
        if step is not None and step > 0:
            min_notional_amt = math.ceil(min_notional_amt / step) * step  # Binance 用 stepSize
        else:
            p = 10 ** 8
            min_notional_amt = math.ceil(min_notional_amt * p) / p
        min_amount = max(min_amount, min_notional_amt)
    # 同时确保 price 符合 tickSize（由 ccxt 在 create_order 时处理）

    local_list: List[float] = []
    server_list: List[float] = []
    propagation_list: List[float] = []
    _pending_orders.clear()
    _first_seen_cache.clear()

    # Use exchange-specific user data stream when available, else watch_orders
    use_user_ws = False
    user_ws_task: Optional[asyncio.Task] = None
    user_ws: Optional[Any] = None

    def _on_order(push_oid: str, first_seen_ns: int) -> None:
        push_oid = str(push_oid).strip('"')
        if not push_oid:
            return
        if push_oid not in _first_seen_cache:
            _first_seen_cache[push_oid] = first_seen_ns
        for oid, rec in list(_pending_orders.items()):
            if rec.get("first_seen_ns"):
                continue
            if oid == push_oid or oid in push_oid or push_oid in oid:
                rec["first_seen_ns"] = _first_seen_cache.get(push_oid, first_seen_ns)
                rec["event"].set()
                break

    if exchange_name == "mexc" and market_type == "spot" and mexc_get_listen_key and MEXCUserOrdersWS:
        try:
            api_key = getattr(exchange, "apiKey", None) or (exchange.options or {}).get("apiKey")
            secret = getattr(exchange, "secret", None)
            if api_key and secret:
                ws_proxy = getattr(exchange, "httpsProxy", None) or getattr(exchange, "socksProxy", None)
                listen_key = await mexc_get_listen_key(api_key, secret, proxy=ws_proxy)
                user_ws = MEXCUserOrdersWS(listen_key, on_order=_on_order, proxy=ws_proxy, ping_interval=25.0)
                await user_ws.connect()
                user_ws_task = asyncio.create_task(user_ws.run_forever())
                await asyncio.sleep(1.2)
                use_user_ws = True
                print("  Using MEXC User Data WebSocket for order stream.\n")
        except Exception as e:
            print(f"  MEXC User Data WS unavailable ({e}), using fallback.\n")

    if exchange_name == "binance" and binance_get_listen_key and BinanceUserOrdersWS:
        try:
            api_key = getattr(exchange, "apiKey", None) or (exchange.options or {}).get("apiKey")
            secret = getattr(exchange, "secret", None)
            if api_key and secret:
                ws_proxy = getattr(exchange, "httpsProxy", None) or getattr(exchange, "socksProxy", None)
                listen_key = await binance_get_listen_key(api_key, secret, market_type, proxy=ws_proxy)
                user_ws = BinanceUserOrdersWS(listen_key, market_type, on_order=_on_order, proxy=ws_proxy)
                await user_ws.connect()
                user_ws_task = asyncio.create_task(user_ws.run_forever())
                await asyncio.sleep(1.2)
                use_user_ws = True
                print(f"  Using Binance User Data WebSocket for order stream.\n")
        except Exception as e:
            print(f"  Binance User Data WS unavailable ({e}), using fallback.\n")

    if not use_user_ws:
        _ws_task = asyncio.create_task(_watch_orders_consumer(exchange, symbol))
        await asyncio.sleep(1.0)

    def _build_batch_orders(n: int) -> List[dict]:
        """Build a list of order dicts for create_orders."""
        return [
            {
                "symbol": symbol,
                "type": "limit",
                "side": "buy",
                "amount": min_amount,
                "price": limit_price,
            }
            for _ in range(n)
        ]

    # MEXC spot, Binance futures support create_orders; others use create_order
    use_batch_api = (
        (exchange_name == "mexc" and market_type == "spot")
        or (exchange_name == "binance" and market_type == "swap")
    )

    async def _place_batch_orders() -> List[dict]:
        """Place batch orders via create_orders or multiple create_order."""
        if use_batch_api:
            return await exchange.create_orders(_build_batch_orders(batch))
        results = await asyncio.gather(
            *[
                exchange.create_order(symbol, "limit", "buy", min_amount, limit_price)
                for _ in range(batch)
            ]
        )
        return [r for r in results if r is not None]

    try:
        for batch_idx in range(runs):
            # Cancel previous batch orders before placing new ones
            if batch_idx > 0:
                try:
                    await exchange.cancel_all_orders(symbol)
                except Exception as e:
                    print(f"  Cancel previous batch warning: {e}")
                await asyncio.sleep(0.2)

            print(f"  ====== Batch {batch_idx + 1}/{runs} ({batch} orders) ======")
            t0_ns = time.perf_counter_ns()
            request_ts_ms = int(time.time() * 1000)
            try:
                orders = await _place_batch_orders()
            except Exception as e:
                print(f"  Batch create orders failed: {e}")
                continue
            t1_ns = time.perf_counter_ns()

            if not isinstance(orders, list):
                orders = [orders]

            # Register all orders for WS tracking
            batch_tasks = []
            for order in orders:
                oid = str(order.get("id") or order.get("clientOrderId") or "")
                if not oid:
                    continue
                ev = asyncio.Event()
                first_ns = _first_seen_cache.get(oid)
                _pending_orders[oid] = {
                    "t0_ns": t0_ns,
                    "event": ev,
                    "first_seen_ns": first_ns,
                    "order_id": oid,
                }
                if first_ns is not None:
                    ev.set()
                batch_tasks.append((oid, t0_ns, t1_ns, request_ts_ms, order, ev))

            for oid, t0_ns, t1_ns, request_ts_ms, order, ev in batch_tasks:
                # Compute local & server latency from REST response
                local_ms = (t1_ns - t0_ns) / 1e6
                transact_ms = _get_transact_time_ms(order)
                server_ms_val: Optional[float] = None
                if transact_ms is not None:
                    if clock_offset_ms is not None:
                        server_ms_val = transact_ms - (request_ts_ms + clock_offset_ms)
                    else:
                        server_ms_val = transact_ms - request_ts_ms

                # Wait for WS push
                prop_ms: Optional[float] = None
                try:
                    await asyncio.wait_for(ev.wait(), timeout=15.0)
                    rec = _pending_orders.get(oid)
                    if rec and rec.get("first_seen_ns") is not None:
                        prop_ms = (rec["first_seen_ns"] - t0_ns) / 1e6
                except asyncio.TimeoutError:
                    print(f"  Timeout waiting for order {oid} on WS")
                finally:
                    _pending_orders.pop(oid, None)

                is_first_batch = (batch_idx == 0)

                if is_first_batch:
                    print(f"  --- Order {oid} [SKIPPED from stats - cold start batch] ---")
                else:
                    local_list.append(local_ms)
                    if server_ms_val is not None:
                        server_list.append(server_ms_val)
                    if prop_ms is not None:
                        propagation_list.append(prop_ms)
                    print(f"  --- Order {oid} ---")

                print(f"    Local Latency: {local_ms:.2f} ms")
                if server_ms_val is not None:
                    print(f"    Server Lat.  : {server_ms_val:.2f} ms")
                else:
                    print(f"    Server Lat.  : N/A")
                if prop_ms is not None:
                    print(f"    WS Propagation: {prop_ms:.2f} ms")
                else:
                    print(f"    WS Propagation: timeout")
                print()

            await asyncio.sleep(1)

        # Cancel remaining orders after last batch
        try:
            await exchange.cancel_all_orders(symbol)
        except Exception as e:
            print(f"  Cancel all orders warning: {e}")
    finally:
        if user_ws_task:
            user_ws_task.cancel()
            try:
                await user_ws_task
            except asyncio.CancelledError:
                pass
            if user_ws:
                await user_ws.close()
        if _ws_task:
            _ws_task.cancel()
            try:
                await _ws_task
            except asyncio.CancelledError:
                pass

    return local_list, server_list, propagation_list


# ---------------------------------------------------------------------------
# RTT Test (optional)
# ---------------------------------------------------------------------------

async def run_rtt_test(
    exchange: ccxt.Exchange,
    runs: int = 10,
) -> List[float]:
    """网络 RTT 测试：基于 fetch_time 往返 (ms)."""
    rtt_list: List[float] = []
    for _ in range(runs):
        t0 = time.perf_counter_ns()
        try:
            await exchange.fetch_time()
        except Exception:
            continue
        t1 = time.perf_counter_ns()
        rtt_list.append((t1 - t0) / 1e6)
        await asyncio.sleep(0.1)
    return rtt_list


# ---------------------------------------------------------------------------
# Statistics & Output
# ---------------------------------------------------------------------------

def stats(samples: List[float]) -> Dict[str, float]:
    """计算 mean, median, p99, std (jitter)."""
    if not samples:
        return {"mean": 0, "median": 0, "p99": 0, "std": 0}
    s = pd.Series(samples)
    return {
        "mean": float(s.mean()),
        "median": float(s.median()),
        "p99": float(s.quantile(0.99)),
        "std": float(s.std()) if len(s) > 1 else 0.0,
    }


def print_results(
    market_type: str,
    method: str,
    symbol: str,
    local_latencies: List[float],
    server_latencies: List[float],
    ws_propagations: Optional[List[float]] = None,
    rtt_latencies: Optional[List[float]] = None,
    exchange: str = "mexc",
) -> None:
    """以表格输出各指标."""
    print(f"\n--- Results: {exchange} {market_type} {symbol} ({method}) ---\n")
    rows = []

    if local_latencies:
        s = stats(local_latencies)
        rows.append(["Local Latency (ms)", f"{s['mean']:.2f}", f"{s['median']:.2f}", f"{s['p99']:.2f}", f"{s['std']:.2f}"])

    if server_latencies:
        s = stats(server_latencies)
        rows.append(["Server Latency (ms)", f"{s['mean']:.2f}", f"{s['median']:.2f}", f"{s['p99']:.2f}", f"{s['std']:.2f}"])

    if ws_propagations:
        s = stats(ws_propagations)
        rows.append(["WS Propagation (ms)", f"{s['mean']:.2f}", f"{s['median']:.2f}", f"{s['p99']:.2f}", f"{s['std']:.2f}"])

    if rtt_latencies:
        s = stats(rtt_latencies)
        rows.append(["Network RTT (ms)", f"{s['mean']:.2f}", f"{s['median']:.2f}", f"{s['p99']:.2f}", f"{s['std']:.2f}"])

    if not rows:
        print("No data to display.")
        return

    headers = ["Metric", "Mean", "Median", "P99", "Jitter (Std)"]
    print(tabulate(rows, headers=headers, tablefmt="simple"))
    if local_latencies or server_latencies:
        print(f"\nSamples: Local={len(local_latencies)}, Server={len(server_latencies)}")
    if ws_propagations is not None:
        print(f"\nWS Propagation samples: {len(ws_propagations)}")


# ---------------------------------------------------------------------------
# CLI & Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Exchange Latency Tester - MEXC/Binance spot & swap 延迟压测",
    )
    p.add_argument("--exchange", choices=["mexc", "binance"], default="mexc", help="mexc | binance")
    p.add_argument("--type", choices=["spot", "swap"], default="spot", help="spot | swap")
    p.add_argument("--method", choices=["market", "limit"], default="market", help="market | limit")
    p.add_argument("--symbol", type=str, default="BTC/USDT", help="e.g. BTC/USDT or BTC/USDT:USDT for swap")
    p.add_argument("--runs", type=int, default=10, help="Number of test runs")
    p.add_argument("--batch", type=int, default=1, help="Number of limit orders per batch (for --method limit)")
    p.add_argument("--rtt", type=int, default=0, help="Extra RTT test runs (0=disable)")
    p.add_argument("--warmup", type=int, default=5, help="Warm-up request count")
    p.add_argument("--timeout", type=int, default=60000, help="HTTP request timeout in ms (default 60000)")
    p.add_argument("--proxy", type=str, default=None, help="HTTP(S) or SOCKS proxy URL. Overrides MEXC_PROXY/BINANCE_PROXY env.")
    return p.parse_args()


async def main_async() -> None:
    args = parse_args()
    api_key, secret = get_env_config(args.exchange)

    if args.type == "swap" and ":USDT" not in args.symbol and "/USDT" in args.symbol:
        args.symbol = args.symbol + ":USDT"

    proxy = args.proxy or get_proxy_from_env()
    if proxy:
        proxy_source = "CLI" if args.proxy else "env"
        proxy_info = f", proxy={proxy} ({proxy_source})"
    else:
        proxy_info = ""
    print(f"Config: exchange={args.exchange}, type={args.type}, method={args.method}, symbol={args.symbol}, runs={args.runs}, batch={args.batch}{proxy_info}\n")

    exchange = build_exchange(args.exchange, api_key, secret, args.type, timeout_ms=args.timeout, proxy=proxy)
    try:
        await warm_up(exchange, count=args.warmup)
    except Exception as e:
        print(f"Warm-up failed: {e}")
        await exchange.close()
        raise SystemExit(1)

    local_list: List[float] = []
    server_list: List[float] = []
    ws_list: Optional[List[float]] = None
    rtt_list: Optional[List[float]] = None

    try:
        clock_offset_ms: Optional[float] = await get_clock_offset_ms(exchange)
        if clock_offset_ms is not None:
            print(f"  Clock offset (server - client): {clock_offset_ms:+.0f} ms\n")

        if args.method == "market":
            local_list, server_list = await run_market_order_test(
                exchange, args.symbol, args.runs, market_type=args.type, clock_offset_ms=clock_offset_ms,
                exchange_name=args.exchange,
            )
        else:
            local_list, server_list, ws_list = await run_limit_order_test(
                exchange, args.symbol, args.runs, args.batch,
                market_type=args.type, clock_offset_ms=clock_offset_ms,
                exchange_name=args.exchange,
            )
            if args.rtt > 0:
                rtt_list = await run_rtt_test(exchange, runs=args.rtt)

        if args.method == "market" and args.rtt > 0:
            rtt_list = await run_rtt_test(exchange, runs=args.rtt)
    finally:
        await exchange.close()

    print_results(
        market_type=args.type,
        method=args.method,
        symbol=args.symbol,
        local_latencies=local_list,
        server_latencies=server_list,
        ws_propagations=ws_list,
        rtt_latencies=rtt_list,
        exchange=args.exchange,
    )


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
