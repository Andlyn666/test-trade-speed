#!/usr/bin/env python3
"""
MEXC Latency Tester - 从 AWS 东京到 MEXC 撮合引擎的延迟压测工具.
"""
import argparse
import asyncio
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
    from mexc_user_ws import get_listen_key, MEXCUserOrdersWS
except ImportError:
    get_listen_key = None  # type: ignore
    MEXCUserOrdersWS = None  # type: ignore


# ---------------------------------------------------------------------------
# Config & Exchange
# ---------------------------------------------------------------------------

def get_env_config() -> Tuple[str, str]:
    api_key = os.environ.get("MEXC_API_KEY", "").strip()
    secret = os.environ.get("MEXC_SECRET", "").strip()
    if not api_key or not secret:
        raise SystemExit(
            "Error: Set MEXC_API_KEY and MEXC_SECRET in environment or .env file."
        )
    return api_key, secret


def get_proxy_from_env() -> Optional[str]:
    """从环境变量读取代理：MEXC_PROXY > HTTPS_PROXY > HTTP_PROXY."""
    for key in ("MEXC_PROXY", "HTTPS_PROXY", "HTTP_PROXY"):
        v = os.environ.get(key, "").strip()
        if v:
            return v
    return None


def build_exchange(
    api_key: str,
    secret: str,
    market_type: str,  # 'spot' | 'swap'
    timeout_ms: int = 60000,
    proxy: Optional[str] = None,
) -> ccxt.Exchange:
    """Build ccxt async MEXC exchange."""
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
            # ccxt 只允许设置一种 proxy，MEXC API 为 HTTPS，故仅设 httpsProxy
            config["httpsProxy"] = proxy
    return ccxt.mexc(config)


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
    else:
        print(f"  Warm-up done ({count} requests, {failed} failed).\n")
        if first_error_detail:
            print("  First failure details (for debugging fetch_time):")
            for line in first_error_detail.splitlines():
                print(f"    {line}")
            print("  Tip: Try --timeout 90000 if the network is slow or use a VPN closer to MEXC.\n")


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


# MEXC 部分交易对仅支持 Web 交易，API 会返回 10007。按顺序尝试以下备用标的。
FALLBACK_SPOT_SYMBOLS = ["WBTC/USDT"]
FALLBACK_SWAP_SYMBOLS = ["ETH/USDT:USDT", "MX/USDT:USDT", "XRP/USDT:USDT", "DOGE/USDT:USDT"]


def _is_symbol_not_supported_error(e: Exception) -> bool:
    """是否 MEXC 10007：该 symbol 不支持 API 交易."""
    msg = str(e).lower()
    return "10007" in msg or "symbol not support" in msg


def _pick_fallback_symbols(markets: dict, market_type: str, requested: str) -> List[str]:
    """返回可用于 10007 备选的交易对列表（仅包含 markets 中存在的）."""
    candidates = FALLBACK_SWAP_SYMBOLS if market_type == "swap" else FALLBACK_SPOT_SYMBOLS
    out = [s for s in candidates if s in markets and s != requested]
    return out


async def run_market_order_test(
    exchange: ccxt.Exchange,
    symbol: str,
    runs: int,
    market_type: str = "spot",
    clock_offset_ms: Optional[float] = None,
) -> Tuple[List[float], List[float]]:
    """市价单测试：返回 (local_latencies_ms, server_latencies_ms).
    若提供 clock_offset_ms (server - client)，则 Server Latency = transactTime - (request_ts + offset)."""
    markets = await exchange.load_markets()
    if symbol not in markets:
        raise SystemExit(f"Symbol not found: {symbol}")

    # MEXC 现货市价单要求最小交易额 1 USDT（code 30002）
    MIN_NOTIONAL_USDT = 1.0

    fallback_list = _pick_fallback_symbols(markets, market_type, symbol)
    actual_symbol = symbol
    tried: List[str] = []

    async def _get_amount_and_params(sym: str) -> Tuple[float, Dict[str, Any]]:
        """返回 (amount, params)。现货市价买用 params['cost']=1USDT 满足最小额."""
        m = markets[sym]
        min_amt = max(
            m.get("limits", {}).get("amount", {}).get("min", 0) or 1e-8,
            1e-8,
        )
        if market_type != "spot":
            return min(min_amt, 0.0001), {}
        # 现货市价买：用 quoteOrderQty/cost 指定 1 USDT，满足 MEXC 最小 1USDT
        return 0, {"cost": MIN_NOTIONAL_USDT}

    amount, order_params = await _get_amount_and_params(actual_symbol)
    local_list: List[float] = []
    server_list: List[float] = []
    already_warned_fallback = False

    for i in range(runs):
        t0_ns = time.perf_counter_ns()
        request_ts_ms = int(time.time() * 1000)
        order = None
        try:
            order = await exchange.create_order(
                symbol=actual_symbol,
                type="market",
                side="buy",
                amount=amount,
                params=order_params,
            )
        except Exception as e:
            if _is_symbol_not_supported_error(e):
                tried.append(actual_symbol)
                next_symbol = None
                for fb in fallback_list:
                    if fb not in tried and fb in markets:
                        next_symbol = fb
                        break
                if next_symbol and (actual_symbol == symbol or actual_symbol in fallback_list):
                    if not already_warned_fallback:
                        print(f"  Symbol {actual_symbol} not supported for API (10007), trying fallback: {next_symbol}\n")
                        already_warned_fallback = True
                    actual_symbol = next_symbol
                    amount, order_params = await _get_amount_and_params(actual_symbol)
                    try:
                        order = await exchange.create_order(
                            symbol=actual_symbol,
                            type="market",
                            side="buy",
                            amount=amount,
                            params=order_params,
                        )
                    except Exception as retry_e:
                        if _is_symbol_not_supported_error(retry_e):
                            print(f"  Run {i+1}/{runs} create_order failed: {retry_e}")
                            print(
                                "  All tried symbols returned 10007. Please check:\n"
                                "  1) API key has **Spot Trading** permission: https://www.mexc.com/user/openapi\n"
                                "  2) The key is not restricted to other symbols only.\n"
                            )
                        else:
                            print(f"  Run {i+1}/{runs} create_order failed: {retry_e}")
                        await asyncio.sleep(1)
                        continue
                else:
                    print(f"  Run {i+1}/{runs} create_order failed: {e}")
                    if not already_warned_fallback:
                        print(
                            "  Tip: If every symbol returns 10007, enable **Spot Trading** for your API key at https://www.mexc.com/user/openapi\n"
                        )
                        already_warned_fallback = True
                    await asyncio.sleep(1)
                    continue
            else:
                print(f"  Run {i+1}/{runs} create_order failed: {e}")
                await asyncio.sleep(1)
                continue

        if order is None:
            continue
        t1_ns = time.perf_counter_ns()
        local_ms = (t1_ns - t0_ns) / 1e6
        local_list.append(local_ms)

        transact_ms = _get_transact_time_ms(order)
        if transact_ms is not None:
            if clock_offset_ms is not None:
                # 校正：request_ts 是 client 时间，加上 offset 得到 server 时间轴下的“请求发出”时刻
                server_ms = transact_ms - (request_ts_ms + clock_offset_ms)
            else:
                server_ms = transact_ms - request_ts_ms
            server_list.append(server_ms)

        await asyncio.sleep(0.3)  # 降低 rate limit 风险

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
    concurrent: int,
    market_type: str = "swap",
) -> List[float]:
    """限价单上架测试：返回 WS propagation 延迟列表 (ms)."""
    global _pending_orders, _ws_task, _first_seen_cache
    _ws_task = None

    markets = await exchange.load_markets()
    if symbol not in markets:
        raise SystemExit(f"Symbol not found: {symbol}")

    fallback_list = _pick_fallback_symbols(markets, market_type, symbol)
    actual_symbol = symbol

    # 获取参考价并构造不会成交的限价（买单价略低于当前最低卖价）
    mid = 0.0
    candidates = [actual_symbol] + [s for s in fallback_list if s in markets]
    for candidate in candidates:
        if candidate not in markets:
            continue
        try:
            ticker = await exchange.fetch_ticker(candidate)
            mid = float(ticker.get("last") or ticker.get("close") or 0)
            if not mid:
                ob = await exchange.fetch_order_book(candidate, limit=5)
                mid = (ob["bids"][0][0] + ob["asks"][0][0]) / 2
            if mid:
                actual_symbol = candidate
                break
        except Exception as e:
            if _is_symbol_not_supported_error(e):
                print(f"  Symbol {candidate} not supported for API (10007), trying next.\n")
                continue
            raise SystemExit(f"Cannot get price for {candidate}: {e}")
    if not mid:
        raise SystemExit(
            f"Cannot get price for any of {candidates}. "
            "Check API key has Spot Trading permission: https://www.mexc.com/user/openapi"
        )

    m = markets[actual_symbol]
    min_amount = max(
        m.get("limits", {}).get("amount", {}).get("min", 0) or 1e-8,
        1e-8,
    )
    # 限价买单价设为 mid * 0.8，避免成交
    limit_price = round(mid * 0.80, 8)
    if limit_price <= 0:
        limit_price = 0.01
    # MEXC 现货最小交易额 1 USDT：amount * price >= 1，向上取整避免舍入后不足
    if market_type == "spot" and limit_price > 0:
        min_notional_amt = 1.01 / limit_price  # 略大于 1 避免舍入误差
        prec = m.get("precision", {}).get("amount")
        p = 10 ** (prec if isinstance(prec, int) else 8)
        min_notional_amt = math.ceil(min_notional_amt * p) / p
        min_amount = max(min_amount, min_notional_amt)

    propagation_list: List[float] = []
    _pending_orders.clear()
    _first_seen_cache.clear()

    # 优先使用 MEXC 官方用户订单流（User Data Stream），否则用 ccxt watch_orders 或 REST 轮询
    use_mexc_user_ws = (
        market_type == "spot"
        and get_listen_key is not None
        and MEXCUserOrdersWS is not None
    )
    user_ws_task: Optional[asyncio.Task] = None
    user_ws: Optional[Any] = None

    if use_mexc_user_ws:
        try:
            api_key = getattr(exchange, "apiKey", None) or (exchange.options or {}).get("apiKey")
            secret = getattr(exchange, "secret", None)
            proxy = getattr(exchange, "httpsProxy", None) or getattr(exchange, "proxy", None)
            if api_key and secret:
                listen_key = await get_listen_key(api_key, secret, proxy)

                def _on_order(push_oid: str, first_seen_ns: int) -> None:
                    push_oid = push_oid.strip('"')
                    if not push_oid:
                        return
                    if push_oid not in _first_seen_cache:
                        _first_seen_cache[push_oid] = first_seen_ns
                    # 匹配 pending：推送的 clientId 可能与 create_order 返回的 id 格式不同
                    for oid, rec in list(_pending_orders.items()):
                        if rec.get("first_seen_ns"):
                            continue
                        if oid == push_oid or oid in push_oid or push_oid in oid:
                            rec["first_seen_ns"] = _first_seen_cache.get(push_oid, first_seen_ns)
                            rec["event"].set()
                            break

                user_ws = MEXCUserOrdersWS(listen_key, on_order=_on_order, proxy=proxy, ping_interval=25.0)
                await user_ws.connect()
                user_ws_task = asyncio.create_task(user_ws.run_forever())
                await asyncio.sleep(1.2)
                print("  Using MEXC User Data WebSocket for order stream.\n")
        except Exception as e:
            use_mexc_user_ws = False
            print(f"  MEXC User Data WS unavailable ({e}), using fallback.\n")

    if not use_mexc_user_ws:
        _ws_task = asyncio.create_task(_watch_orders_consumer(exchange, actual_symbol))
        await asyncio.sleep(1.0)

    async def place_one_limit_order() -> Optional[Tuple[str, int, asyncio.Event]]:
        ev = asyncio.Event()
        t0_ns = time.perf_counter_ns()
        try:
            order = await exchange.create_order(
                symbol=actual_symbol,
                type="limit",
                side="buy",
                amount=min_amount,
                price=limit_price,
                params={},
            )
            oid = str(order.get("id") or order.get("clientOrderId") or "")
            if not oid:
                return None
            first_ns = _first_seen_cache.get(oid)
            _pending_orders[oid] = {
                "t0_ns": t0_ns,
                "event": ev,
                "first_seen_ns": first_ns,
                "order_id": oid,
            }
            if first_ns is not None:
                ev.set()
            return (oid, t0_ns, ev)
        except Exception as e:
            print(f"  Limit order create failed: {e}")
            return None

    try:
        for batch_start in range(0, runs, concurrent):
            batch_size = min(concurrent, runs - batch_start)
            results = await asyncio.gather(
                *[place_one_limit_order() for _ in range(batch_size)]
            )
            batch_tasks = [(r[0], r[1], r[2]) for r in results if r is not None]

            # 等待本批次所有订单在 WS 上首次出现（最多 15 秒）
            for oid, t0_ns, ev in batch_tasks:
                try:
                    await asyncio.wait_for(ev.wait(), timeout=15.0)
                    rec = _pending_orders.get(oid)
                    if rec and rec.get("first_seen_ns") is not None:
                        prop_ms = (rec["first_seen_ns"] - t0_ns) / 1e6
                        propagation_list.append(prop_ms)
                except asyncio.TimeoutError:
                    print(f"  Timeout waiting for order {oid} on WS")
                finally:
                    _pending_orders.pop(oid, None)

            await asyncio.sleep(0.2)

        # 测试结束后撤单
        try:
            await exchange.cancel_all_orders(actual_symbol)
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

    return propagation_list


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
) -> None:
    """以表格输出各指标."""
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
        description="MEXC Latency Tester - 现货/合约 市价单与限价单延迟压测",
    )
    p.add_argument("--type", choices=["spot", "swap"], default="spot", help="spot | swap")
    p.add_argument("--method", choices=["market", "limit"], default="market", help="market | limit")
    p.add_argument("--symbol", type=str, default="BTC/USDT", help="e.g. BTC/USDT or BTC/USDT:USDT for swap")
    p.add_argument("--runs", type=int, default=10, help="Number of test runs")
    p.add_argument("--concurrent", type=int, default=1, help="Concurrent limit orders per batch (for --method limit)")
    p.add_argument("--rtt", type=int, default=0, help="Extra RTT test runs (0=disable)")
    p.add_argument("--warmup", type=int, default=5, help="Warm-up request count")
    p.add_argument("--timeout", type=int, default=60000, help="HTTP request timeout in ms (default 60000)")
    p.add_argument("--proxy", type=str, default=None, help="HTTP(S) or SOCKS proxy URL (e.g. http://127.0.0.1:7890). Overrides MEXC_PROXY/HTTPS_PROXY.")
    return p.parse_args()


async def main_async() -> None:
    args = parse_args()
    api_key, secret = get_env_config()

    if args.type == "swap" and ":USDT" not in args.symbol and "/USDT" in args.symbol:
        args.symbol = args.symbol + ":USDT"

    proxy = args.proxy or get_proxy_from_env()
    if proxy:
        print(f"Config: type={args.type}, method={args.method}, symbol={args.symbol}, runs={args.runs}, concurrent={args.concurrent}, proxy=ON\n")
    else:
        print(f"Config: type={args.type}, method={args.method}, symbol={args.symbol}, runs={args.runs}, concurrent={args.concurrent}\n")

    exchange = build_exchange(api_key, secret, args.type, timeout_ms=args.timeout, proxy=proxy)
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
        if args.method == "market":
            clock_offset_ms: Optional[float] = await get_clock_offset_ms(exchange)
            if clock_offset_ms is not None:
                print(f"  Clock offset (server − client): {clock_offset_ms:+.0f} ms\n")
            local_list, server_list = await run_market_order_test(
                exchange, args.symbol, args.runs, market_type=args.type, clock_offset_ms=clock_offset_ms
            )
        else:
            ws_list = await run_limit_order_test(
                exchange, args.symbol, args.runs, args.concurrent, market_type=args.type
            )
            # 限价单模式不直接得到 HTTP 往返，可选做一次 RTT
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
    )


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
