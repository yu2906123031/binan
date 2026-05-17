# binan-main

当前主程序是一个 Binance USDT 永续双向动量策略，入口为 `main.py`，核心实现位于 `scripts/binance_futures_momentum_long.py`。

## 当前策略

- 候选池来源于手动指定币种、Square 热门币，以及 24h 涨幅榜和跌幅榜前 N 个 USDT 合约。
- 每个币同时评估 `long` 和 `short` 两个方向。
- `long` 侧偏向向上突破后的延续，`short` 侧偏向向下破位后的延续。
- 入场判断不只看涨跌幅，还叠加 5m 动量、成交量放大、5m/15m 加速度、1h/4h 趋势、OI、CVD、taker ratio、order book 和 book ticker 微观结构。
- 可选接入 OKX 情绪、情绪加速度、板块共振和 smart money flow 作为加减分项。
- BTC 和 SOL 会先做市场环境过滤，环境会影响候选分数、信号等级、建议仓位，以及入场门槛。
- regime entry gate 当前语义固定如下：顺风方向会把 `min_5m_change_pct` 乘以 `0.75`，并把 `acceleration_ratio` 门槛从默认 `1.25` 放宽到 `1.15`；逆风方向会把 `min_5m_change_pct` 乘以 `1.10`，并把 `acceleration_ratio` 收紧到 `1.45`；`caution` 会把 `min_5m_change_pct` 乘以 `1.05`，并在 `acceleration_ratio` 上额外加 `0.10`。
- 高风险场景会直接 veto，例如 distribution、负 CVD、OI 反转、过度追价、滑点过高、深度不足、smart money 明显流出。

## 仓位与出场

- 仓位按固定 `risk_usdt` 反推，不是固定张数。
- 止损优先取 swing 结构位，并结合 `stop_buffer_pct` 与 ATR 动态距离。
- 运行中的管理逻辑支持先移保本、`TP1` / `TP2` 分批止盈，以及 runner 余仓追踪退出。
- 实盘层还会额外检查是否已有持仓、是否已有未完成订单、最大持仓数、多空持仓上限、净暴露 / 总暴露、单币单边限制、日亏损、连亏和冷却时间。
- 当前还支持组合 bucket 风控：`portfolio_narrative_bucket` 与 `portfolio_correlation_group` 会从外部信号 metadata 或本地 symbol 规则推断，写入 candidate、alert 和 `positions.json`，并参与 theme / correlation / heat 风控。

## 本地启动

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main.py --help
```

推荐先用只扫描模式验证：

```powershell
python main.py --scan-only --symbol BTCUSDT --output-format json
```

也可以指定一组币种：

```powershell
python main.py --scan-only --square-symbols BTCUSDT,ETHUSDT,SOLUSDT --output-format json
```

也可以保留默认候选池，但单独控制涨幅榜 / 跌幅榜扫描数量：

```powershell
python main.py --scan-only --top-gainers 15 --top-losers 15 --output-format json
```

## 运行说明

- `--scan-only` 不会下单。
- 当前代码已支持：在纯扫描模式且本地未配置 Binance API Secret 时，自动跳过启动对账，便于本地验证。
- `--reconcile-only` 只做账户对账，不做扫描与下单。
- `--live` 会真实下单，必须先配置 Binance API Key/Secret，并确认 Futures 权限、IP 白名单和账户权限正确。
- 触发模型已经拆为两阶段：`setup_ready` 表示结构可做，`trigger_fired` 表示至少达到最小微确认数。
- 默认 runtime 状态目录为 `~/.hermes/binance-futures-momentum-long/runtime-state`。
- Windows 控制台如果中文摘要显示异常，优先使用 `--output-format json`。

## aggressive-fee-aware-scalp profiles

- 这组三个 profile 面向高频、激进、高胜率场景，核心目标是有效仓位、覆盖手续费、止损留出普通波动空间、保本节奏适中、`TP1 >= 1R`。
- 滑点、流动性、OI、CVD、funding、heat 与连亏风控继续生效。
- `aggressive-fee-aware-scalp-long-short` 同时评估 `long` 与 `short`。
- `aggressive-fee-aware-scalp-long-only` 只评估 `long`。
- `aggressive-fee-aware-scalp-short-only` 只评估 `short`。
- 原 `10u-aggressive` profile 保留。
- `10u-aggressive-v2` 在 `10u-aggressive` 基础上打开 v2 结构化过滤，默认启用 `market regime gate`、`direction lock`、`fee-aware edge filter`、`symbol quality tier` 与 ATR stop。
- `10u-aggressive-v2` 默认关闭 live sim probe，显式 CLI 参数仍然优先生效。

扫描命令：

```powershell
python main.py --scan-only --profile aggressive-fee-aware-scalp-long-short --output-format json
python main.py --scan-only --profile aggressive-fee-aware-scalp-long-only --output-format json
python main.py --scan-only --profile aggressive-fee-aware-scalp-short-only --output-format json
python main.py --scan-only --profile 10u-aggressive-v2 --output-format json
```

自动运行命令：

```powershell
python main.py --live --auto-loop --profile aggressive-fee-aware-scalp-long-short
python main.py --live --auto-loop --profile aggressive-fee-aware-scalp-long-only
python main.py --live --auto-loop --profile aggressive-fee-aware-scalp-short-only
python main.py --live --auto-loop --profile 10u-aggressive-v2
```

## 扫描输出补充字段

- `candidate_alerts` / `selected_alert` 现在会暴露：
  - `tradeability_score`
  - `expected_edge`
  - `expected_total_fee_pct`
  - `execution_slippage_buffer_pct`
  - `min_profit_buffer_pct`
- `scan.summary_counters` 会汇总：
  - `raw_scan_symbol_count`
  - `evaluated_symbol_count`
  - `evaluated_side_count`
  - `early_filter_passed_count`
  - `setup_ready_count`
  - `trigger_fired_count`
  - `candidate_pool_count`
  - `hard_rejected_count`
- 中文摘要会追加一行 `扫描计数`，方便直接看扫描漏斗。

## 环境变量

可参考 `.env.example` 创建本地 `.env`：

- `BINANCE_FUTURES_API_KEY`
- `BINANCE_FUTURES_API_SECRET`
- `BINANCE_FUTURES_BASE_URL`
- `TELEGRAM_BOT_TOKEN`
- `HERMES_HOME`
- `OKX_SENTIMENT_BRIDGE_PATH`

`OKX_SENTIMENT_BRIDGE_PATH` 是可选项。只有在你想覆盖默认的 `scripts/okx_sentiment_bridge.py` 时才需要填写。

## Heat 风控参数

可以直接从 CLI 控制 `R` 单位 heat 上限：

- `--gross-heat-cap-r`
- `--same-theme-heat-cap-r`
- `--same-correlation-heat-cap-r`

示例：

```powershell
python main.py --scan-only --gross-heat-cap-r 2.8 --same-theme-heat-cap-r 1.1 --same-correlation-heat-cap-r 0.9 --output-format json
```

## 辅助脚本

查看 OKX 情绪桥接脚本参数：

```powershell
python scripts\okx_sentiment_bridge.py --help
```

生成拒单统计报告：

```powershell
python scripts\rejected_analysis.py --help
```

生成平仓 bucket expectancy 报表：

```powershell
python scripts\trade_bucket_analysis.py --runtime-state-dir runtime-state --lookback-days 7
```

生成最近 7 天交易诊断打包产物：

```powershell
python scripts\recent_trade_diagnostics.py --runtime-state-dir runtime-state --lookback-days 7
```

micro-scalp 模式可配合时间止盈收口：

```powershell
python main.py --micro-scalp-mode --micro-scalp-time-stop-sec 300 --micro-scalp-min-profit-r 0.3
```

按单币种回放 runtime 事件链路：

```powershell
python scripts\symbol_replay.py --symbol DOGEUSDT --runtime-state-dir runtime-state
```

## 当前验证状态

- 根入口 `python main.py --help` 可正常启动。
- `python scripts\okx_sentiment_bridge.py --help` 可正常启动。
- `python scripts\rejected_analysis.py --help` 可正常启动。
- `python scripts\trade_bucket_analysis.py --help` 可正常启动。
- `python scripts\symbol_replay.py --help` 可正常启动。
- 最近一次全量测试记录为 `152 passed`；本轮新增/受影响的针对性测试已通过。
