# 架构优化路线

当前系统以 `scripts/binance_futures_momentum_long.py` 为主干，集成了扫描、候选评分、风控、执行、运行态账本、监控、通知和 CLI。测试覆盖较好，但单文件承担过多职责，后续优化应按可验证边界逐步拆分，避免一次性重写实盘路径。

## 目标分层

1. `domain`
   - 方向归一化、`Candidate`、`TradeManagementPlan`、`TradeManagementState`、仓位 key、bucket 推断。
   - 不依赖网络、文件系统或 CLI。

2. `market_data`
   - Binance ticker、klines、funding、OI、order book、book ticker websocket。
   - 只负责采集与标准化，不做交易决策。

3. `signals`
   - 技术指标、市场 regime、OKX sentiment、external signal、smart money、trigger confirmation。
   - 输入为标准化 market data，输出为候选增强字段和 veto reason。

4. `risk`
   - 日内亏损、连亏、冷却、持仓数、long/short 暴露、theme/correlation/heat。
   - 输入为 candidate + runtime/exchange snapshot，输出稳定的 allow/reason payload。

5. `execution`
   - 下单、杠杆、止损、订单恢复、成交反馈标准化。
   - 不直接写 runtime state。

6. `runtime`
   - `positions.json`、`orders.json`、`risk_state.json`、`events.jsonl`、user data stream apply/reconcile。
   - 负责持久化 schema 迁移和事件归一化。

7. `app`
   - `run_scan_once()`、`run_loop()`、CLI、通知编排。
   - 只串联服务，不拼底层 schema 字段。

## 当前已提取的边界

- `persist_live_open_position(...)`
  - live 成交后唯一负责写入 open position payload。
  - 保护 `side / position_side / position_key / entry feedback / portfolio bucket` 持久化契约。

- `append_buy_fill_confirmed_event(...)`
  - background monitor 启动后唯一负责构造 `buy_fill_confirmed` 事件。
  - 避免 `run_loop()` 直接复制 positions 字段。

## 迁移顺序

1. 先抽 runtime 纯函数和 `RuntimeStateStore`，保留旧脚本 re-export，确保现有测试不改导入路径。
2. 再抽 domain dataclass 与 side/key 归一化函数。
3. 抽 risk guard 与 portfolio heat，保持 reject reason 字符串不变。
4. 抽 execution 下单与成交反馈标准化，`run_loop()` 只接收标准 `live_execution` payload。
5. 最后拆 scan/signals/market data，保持 CLI 参数和 JSON 输出兼容。

每一步都必须先有回归测试覆盖旧契约，再移动代码。实盘路径的兼容标准是：`positions.json`、`events.jsonl`、alert payload、reject reason、CLI 参数保持稳定。
