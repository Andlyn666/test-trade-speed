
# MEXC 交易延迟压测工具 (MEXC Latency Tester)

本项目是一个基于 `Python` 和 `ccxt` 的高性能测试工具，旨在精准测量从 **AWS 东京机房** 到 **MEXC (抹茶)** 撮合引擎的网络与处理延迟。

## 1. 测试维度

本工具支持 **现货 (Spot)** 与 **合约 (Swap/Futures)** 的全链路延迟测试：

* **市价单成交测试 (Market Order)**：测量从发出请求到服务器确认成交（Transact）的总时长。
* **限价单上架测试 (Limit Order)**：测量限价单发出后，出现在 WebSocket 订单簿更新流中的端到端耗时。
* **网络 RTT 监测**：基于 HTTP Keep-Alive 连接，统计纯物理链路的往返时间。

---

## 2. 环境准备

### 硬件建议

* **区域**：AWS 东京机房 (`ap-northeast-1`)。
* **实例**：建议使用计算优化型实例（如 `c5.large` 或更高），以减少系统内核对高频 IO 的干扰。

### 依赖安装

建议使用项目内虚拟环境后安装（见仓库根目录 `venv`）：

```bash
pip install ccxt aiohttp pandas tabulate
```

或使用依赖文件（若存在）：

```bash
pip install -r requirements.txt
```

---

## 3. 核心测试指标说明

工具将输出以下关键延迟指标（单位：ms）：

| 指标名称 | 定义 | 意义 |
| --- | --- | --- |
| **Local Latency** | 本地发送时刻 → 收到 HTTP 响应的时长 | 客户端感知的 API 总响应时间（RTT + 服务端处理） |
| **Server Latency** | 请求 `timestamp` 与响应中 `transactTime` 的差值（需校正时钟漂移） | 请求到达服务器并完成撮合的时间（单程+处理） |
| **Network Jitter** | 延迟的 Standard Deviation | 反映网络连接的稳定性 |
| **WS Propagation** | 本地下单时刻 → WebSocket 订单簿/订单流中首次出现该订单的时长 | 订单进入撮合引擎并被推送至客户端的端到端时间 |

---

## 4. 快速开始

### 配置 API Key

在运行前，请通过环境变量配置（推荐使用项目根目录下的 `.env` 文件，勿提交到版本库）：

```bash
# .env 示例
MEXC_API_KEY=YOUR_MEXC_API_KEY
MEXC_SECRET=YOUR_MEXC_SECRET
# 若直连 MEXC 超时（如地区限制），可设置代理（可选）
# MEXC_PROXY=http://127.0.0.1:7890
```

运行时也可通过 `--proxy http://127.0.0.1:7890` 指定代理；未设置时自动读取环境变量 `MEXC_PROXY`、`HTTPS_PROXY` 或 `HTTP_PROXY`。

### 运行测试

**模式 A：单次基准测试（现货市价单）**

```bash
python main.py --type spot --method market --symbol BTC/USDT --runs 10

```

**模式 B：高并发上架测试（合约限价单）**

```bash
python main.py --type swap --method limit --symbol BTC/USDT:USDT --runs 50 --concurrent 5

```

---

## 5. 测试流程原理

1. **热身阶段 (Warm-up)**：预先发送 3-5 次无效请求以触发 TCP 握手并保持长连接。
2. **发送阶段**：记录本地高精度纳秒级时间戳。
3. **计算阶段**：
* 读取响应体中的 `transactTime`（MEXC 撮合时间）。
* 计算本地时间与服务器时间的漂移量 (Clock Drift)。
* 统计所有样本的平均值、中位数及 99 分位值 (P99)。



---

## 6. WebSocket 行情流（mexc_spot_ws.py）

根据 [MEXC Spot WebSocket Market Streams](https://www.mexc.com/api-docs/spot-v3/websocket-market-streams) 实现了现货行情 WebSocket 客户端，用于订阅公开行情（成交流、深度、最优挂单等）。

* **端点**：`wss://wbs-api.mexc.com/ws`
* **控制**：订阅/取消订阅、PING/PONG 为 JSON；**行情推送为 Protobuf 二进制**，需按 MEXC 提供的 [websocket-proto](https://github.com/mexcdevelop/websocket-proto) 自行编译解码（当前脚本仅接收并输出二进制长度，或可对接解码逻辑）。

**运行示例**（订阅 BTCUSDT 成交流与最优挂单，收 5 条二进制帧后退出）：

```bash
python mexc_spot_ws.py
```

**在代码中使用**：

```python
from mexc_spot_ws import MEXCSpotWS, channel_deals, channel_book_ticker

async def main():
    ws = MEXCSpotWS(proxy=os.environ.get("MEXC_PROXY"), ping_interval=25.0)
    await ws.connect()
    await ws.send_subscription([
        channel_deals("BTC/USDT", "100ms"),
        channel_book_ticker("BTC/USDT", "100ms"),
    ])
    chs = [channel_deals("BTC/USDT", "100ms"), channel_book_ticker("BTC/USDT", "100ms")]
    async for frame in ws.stream(chs):  # 或使用 run_forever(on_message=...)
        if frame["type"] == "control":
            print(frame["data"])   # 订阅确认、PONG 等
        elif frame["type"] == "binary":
            # frame["data"] 为 bytes，需用 MEXC proto 解码
            pass
```

---

## 7. 注意事项

* **风控警告**：高频发送市价单可能触发 MEXC 的下单频率限制（Rate Limit），请确保账户内有足够的 USDT。
* **精度提醒**：测试限价单上架速度时，脚本会自动开启异步 WebSocket 监听，请确保网络未经过代理。
* **成本建议**：测试合约时建议使用最小面值，并在测试结束后由脚本自动撤单（已集成）。
* **WebSocket 兼容性**：ccxt 对 MEXC 的 `watch_order_book` 存在已知问题（多 symbol 时 nonce 错误、部分版本 `unWatchOrderBook` 挂起）。限价单上架测试可优先采用用户订单流（如 `watch_orders`）或锁定 ccxt 版本（如 ≤4.4.94）再使用 order book 流。
---
