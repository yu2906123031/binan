# Runtime 状态机与持久化 Schema

本文档描述 `scripts/binance_futures_momentum_long.py` 与 `scripts/runtime_store.py` 当前共享的运行态契约，目标是为后续 domain / runtime / execution 拆分提供稳定边界。文档覆盖候选阶段状态机、持久化文件 schema、关键事件类型与降级路径。

## 1. 候选阶段状态机

候选对象当前由主脚本内的 `Candidate` dataclass 表示，阶段相关核心字段包括：

- `setup_ready: bool`
- `trigger_fired: bool`
- `candidate_stage: str`
- `setup_missing: List[str]`
- `trigger_missing: List[str]`
- `trade_missing: List[str]`
- `trigger_confirmation_flags: Dict[str, bool]`
- `trigger_confirmation_count: int`
- `trigger_min_confirmations: int`

阶段收口规则由候选摘要构建逻辑统一导出：

| 阶段 | 判定条件 | 含义 |
| --- | --- | --- |
| `watch_candidate` | `setup_missing` 非空，或未满足更高阶段条件 | 候选处于观察阶段，等待 setup 完整 |
| `setup_candidate` | `setup_ready == True` 且 `trigger_fired == False` | setup 已成立，等待 trigger |
| `trade_candidate` | `trigger_fired == True` | trigger 已确认，允许进入交易执行 |
| `other` | 展示层兜底 bucket | 仅用于 summary 分组 |

推荐把这组字段视为候选阶段最小契约。后续拆出 `domain` 模块时保持字段名与阶段值稳定，可以减少 summary、通知、last_cycle 快照的联动修改面。

## 2. 持仓生命周期状态机

开仓后，运行态主要依赖 `TradeManagementPlan`、`TradeManagementState` 与 `positions.json` 持久化字段协同表达生命周期。

### 2.1 计划态 `TradeManagementPlan`

核心字段：

- `entry_price`
- `stop_price`
- `quantity`
- `initial_risk_per_unit`
- `breakeven_trigger_price`
- `tp1_trigger_price`
- `tp1_close_qty`
- `tp2_trigger_price`
- `tp2_close_qty`
- `runner_qty`
- `side`
- `position_side`
- `breakeven_confirmation_mode`
- `breakeven_min_buffer_pct`

该结构表达静态交易计划，来源包括：

1. 实盘新开仓时由 `build_trade_management_plan(...)` 生成。
2. 重启恢复时由 `build_trade_management_plan_from_position(...)` 从持仓 payload 反推。

### 2.2 进度态 `TradeManagementState`

核心字段：

- `symbol`
- `entry_price`
- `stop_price`
- `remaining_quantity`
- `position_side`
- `position_key`
- `current_stop_price`
- `moved_to_breakeven`
- `tp1_hit`
- `tp2_hit`
- `highest_price_seen`
- `lowest_price_seen`

这组字段表达监控线程随行情推进的可变状态。典型生命周期顺序如下：

1. `buy_fill_confirmed`
   - 开仓成交已确认。
   - `positions.json` 写入 `status: open`、`entry_price`、`filled_quantity`、`trade_management_plan`、`trade_management_state` 初始快照。
2. `protection_confirmed`
   - 止损保护单已成功识别或修复。
   - `protection_status` 与 `stop_order_id` 持久化。
3. `breakeven_moved`
   - 达到 breakeven 条件后，`current_stop_price` 移到开仓价附近。
   - `moved_to_breakeven` 置为 `True`。
4. `tp1_hit`
   - 第一档止盈成交后，`remaining_quantity` 减少，`tp1_hit` 置为 `True`。
5. `tp2_hit`
   - 第二档止盈成交后，`remaining_quantity` 继续减少，`tp2_hit` 置为 `True`。
6. `trade_closed` / `position_closed`
   - 持仓全部平掉，最终记录 realized PnL、exit reason、closed_at 等收口信息。

这条顺序是当前 execution / runtime 的主路径契约。后续模块拆分时，事件名与关键布尔字段继续保持稳定，可以保证恢复逻辑、通知模板与外部 watcher 继续工作。

## 3. runtime 文件 Schema

运行态目录由 `RuntimeStateStore(runtime_state_dir)` 管理。当前关键文件如下。

### 3.1 `positions.json`

用途：保存当前跟踪的持仓账本。写入统一经过 `save_json('positions', ...)` 与 `save_positions_state(...)`，读路径统一经过 `load_json('positions')` 或 `load_json_with_error('positions')`，内部会做 canonicalization。

推荐把每个 position record 视为以下 schema：

```json
{
  "BTCUSDT:LONG": {
    "symbol": "BTCUSDT",
    "side": "LONG",
    "position_side": "LONG",
    "position_key": "BTCUSDT:LONG",
    "status": "open",
    "quantity": 0.01,
    "filled_quantity": 0.01,
    "remaining_quantity": 0.01,
    "entry_price": 62000.0,
    "stop_price": 61200.0,
    "current_stop_price": 61200.0,
    "stop_order_id": 123456,
    "protection_status": "protected",
    "entry_order_id": 987654,
    "entry_client_order_id": "entry-xxx",
    "entry_order_status": "FILLED",
    "margin_type": "isolated",
    "opened_at": "2026-05-09T15:00:00+00:00",
    "updated_at": "2026-05-09T15:03:00+00:00",
    "closed_at": null,
    "portfolio_narrative_bucket": "meme",
    "portfolio_correlation_group": "alts",
    "selected_score": 84.2,
    "trigger_class": "breakout",
    "trade_management_plan": {
      "entry_price": 62000.0,
      "stop_price": 61200.0,
      "quantity": 0.01,
      "initial_risk_per_unit": 800.0,
      "breakeven_trigger_price": 62800.0,
      "tp1_trigger_price": 63200.0,
      "tp1_close_qty": 0.003,
      "tp2_trigger_price": 63600.0,
      "tp2_close_qty": 0.004,
      "runner_qty": 0.003,
      "side": "LONG",
      "position_side": "LONG"
    },
    "trade_management_state": {
      "position_key": "BTCUSDT:LONG",
      "remaining_quantity": 0.01,
      "current_stop_price": 61200.0,
      "moved_to_breakeven": false,
      "tp1_hit": false,
      "tp2_hit": false,
      "highest_price_seen": 62100.0,
      "lowest_price_seen": 61900.0
    }
  }
}
```

稳定约束：

- 顶层 key 采用 `SYMBOL:POSITION_SIDE` canonical key。
- `materialize_positions_state(...)` 负责补齐 `side`、`position_side`、`position_key` 与 lifecycle 缺省字段。
- LONG 持仓可带 legacy alias，持久化时 canonical key 仍是唯一主键。
- `status` 当前至少覆盖 `open` 与 `closed` 两种终态语义。

### 3.2 `risk_state.json`

用途：保存风控累计状态。当前字段来源分散在 risk guard 逻辑中，稳定关注点包括：

- `daily_loss`
- `consecutive_losses`
- `cooldown_until`
- `last_trade_at`
- `last_loss_at`
- `blocked_reason`
- 其他日内风控累计字段

该文件的消费特点是：

- 读取失败时通过 `load_json_with_error` 走默认值回退。
- 降级时发出 `runtime_state_degraded` 事件，并记录 `consumer=load_risk_state`。

### 3.3 `last_cycle.json`

用途：保存最近一轮扫描与执行快照，供 watcher、通知与排查使用。当前由 `persist_cycle_snapshot(...)` 写入，稳定字段关注点包括：

- `updated_at`
- `profile`
- `selected_candidate`
- `cycle_summary`
- `portfolio_snapshot`
- `risk_snapshot`
- `monitor_debug`
- `open_positions`

该文件更偏观察性快照，适合承接 summary / debug 信息，避免把一次扫描上下文回写到 `positions.json`。

### 3.4 `events.jsonl`

用途：保存结构化运行事件流。`append_event(...)` 使用逐行 JSONL 追加、`flush()`、`fsync()` 落盘，`read_events(...)` 对尾部损坏行保持容忍。

每行统一包含：

- `event_type`
- `timestamp`
- `symbol`
- `side`
- `position_side`
- `position_key`
- 该事件专属 payload 字段

常见事件类型：

- `runtime_state_degraded`
- `buy_fill_confirmed`
- `protection_confirmed`
- `breakeven_moved`
- `tp1_hit`
- `tp2_hit`
- `trade_closed`
- `position_closed`
- `candidate_rejected`

## 4. 降级与恢复契约

### 4.1 文件读取降级

当前已事件化的降级场景：

- `positions.json` 解析失败
- `risk_state.json` 解析失败

统一 payload 关注字段：

- `state_key`
- `state_file`
- `error_type`
- `error`
- `fallback_used`
- `consumer`

同一路径通过 cooldown 做 rate limit，避免通知风暴。

### 4.2 重启恢复

恢复路径依赖以下契约：

- `positions.json` 内存在 `trade_management_plan` 与 lifecycle 字段时，可以重建 `TradeManagementState`。
- `current_stop_price` 在恢复时优先级高于初始 `stop_price`，用于延续 trailing / breakeven 后的实际保护位。
- `materialize_positions_state(...)` 在读取时补齐 side/key 与部分默认 lifecycle 字段，保证旧仓位记录可以被当前 monitor 继续消费。

## 5. 后续拆分建议

1. 把 `Candidate` 阶段字段与 `TradeManagementPlan` / `TradeManagementState` 一起抽到 `domain` 模块。
2. 把 `positions.json` record schema 定义为显式 typed contract，减少主脚本散落字段拼装。
3. 把 `events.jsonl` 事件类型与 payload schema 收口为常量或 dataclass，降低字符串漂移风险。
4. 把 `risk_state.json` 当前隐式字段补成显式 schema 文档，并同步测试 fixture。
