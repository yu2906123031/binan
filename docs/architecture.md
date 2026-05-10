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

## 运行态契约补充

- `docs/runtime_state_machine_and_schema.md`
  - 记录候选阶段状态机、持仓生命周期、`positions.json` / `risk_state.json` / `last_cycle.json` / `events.jsonl` schema。
  - 作为 runtime / execution / domain 后续拆分时的字段兼容基线。
- `docs/refactor-done-definition-and-gates.md`
  - 记录重构完成定义、单文件/单函数拆分预警与 `except Exception` 说明门禁。
  - 作为后续结构改动的统一质量门禁基线。

## 当前已提取的边界

- `persist_live_open_position(...)`
  - live 成交后唯一负责写入 open position payload。
  - 保护 `side / position_side / position_key / entry feedback / portfolio bucket` 持久化契约。

- `append_buy_fill_confirmed_event(...)`
  - background monitor 启动后唯一负责构造 `buy_fill_confirmed` 事件。
  - 避免 `run_loop()` 直接复制 positions 字段。

## 迁移顺序

1. 先抽 runtime 纯函数和 `RuntimeStateStore`，保留旧脚本 re-export，确保现有测试不改导入路径。已完成：`scripts/runtime_store.py` 已承接 `RuntimeStateStore`、positions migration/materialize、runtime event normalization，主脚本通过 `from runtime_store import ...` 回接旧名称，`tests/test_strategy_v2_restore_regression.py` 保持原导入路径并已回归通过。
2. 再抽 execution 开仓与监控入口，延续主脚本 import/re-export 模式。已完成当前批次：`scripts/execution_engine.py` 已承接 `ensure_symbol_margin_type`、`place_live_trade`、`monitor_live_trade`，主脚本通过 `from execution_engine import ...` 回接旧名称，`tests/test_execution_module_regression.py` 已回归通过。
3. 再抽后台线程启动与 monitor contract，保持 `run_loop()` 只负责接线与状态落盘。
4. 再抽 domain dataclass 与 side/key 归一化函数。
5. 抽 risk guard 与 portfolio heat，保持 reject reason 字符串不变。
6. 最后拆 scan/signals/market data，保持 CLI 参数和 JSON 输出兼容。

每一步都必须先有回归测试覆盖旧契约，再移动代码。实盘路径的兼容标准是：`positions.json`、`events.jsonl`、alert payload、reject reason、CLI 参数保持稳定。
