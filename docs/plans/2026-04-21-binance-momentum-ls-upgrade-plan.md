# Binance Futures Momentum LS Upgrade Plan

> **For Hermes:** 按本文档顺序推进，把现有 `binance-futures-momentum-long` 升级成 side-aware 双向系统。先文档，后实现；先 schema，后扫描、执行、管理、风控。

**Goal:** 把现有 long-only Binance skill 升级为可开多、可开空、共享风控的双向动量系统，并采用可回归、可迁移、可分阶段落地的升级路线。

**Architecture:** 核心原则是 side-aware。统一让 Candidate、TradeManagementPlan、TradeManagementState、runtime position record、event payload、execution、monitor、reconcile 全部显式携带 `side` 与 `position_key`。扫描层拆成 long/short 双引擎，执行层由 `candidate.side` 决定开仓与保护方向，管理层通过方向乘子统一 TP/BE/runner/trailing，风控层升级为共享暴露池。

**Tech Stack:** Python 单文件主脚本 `scripts/binance_futures_momentum_long.py`、runtime JSON/JSONL 状态文件、pytest 回归测试、Hermes skill 文档。

---

## 1. 升级范围与边界

### 1.1 本轮目标
1. 支持 long 候选扫描。
2. 支持 short 候选扫描。
3. 支持 `candidate.side` 驱动的真实开仓。
4. 支持 side-aware 的初始保护单、TP、BE、runner、trailing。
5. 支持 `position_key = SYMBOL:side` 的 runtime state。
6. 支持 long/short 共享风控池。
7. 先生成文档，再按 phase 逐步升级。

### 1.2 第一阶段边界
- 支持双向开仓。
- 同一 symbol 只允许单边持仓。
- 暂不做 hedge mode。
- short 采用独立建模，不做 long 逻辑硬镜像。
- 继续复用现有事件体系、runtime state 目录、reconcile 骨架与测试体系。

### 1.3 交付物
- 升级规划文档。
- side-aware schema 改造。
- dual scanner 设计与实现。
- side-aware execution 改造。
- side-aware monitor 改造。
- 风控升级说明与测试矩阵。
- 更新后的 SKILL 文档与迁移说明。

---

## 2. 设计原则

1. 双向升级的本质是让整个系统 side-aware。
2. short 是独立策略引擎，重点做“弱币破位延续 + 反弹失败续跌”。
3. 所有交易对象统一带：

```python
side: Literal["long", "short"]
```

4. 所有 runtime 主键统一升级为：

```python
position_key = f"{symbol}:{side}"
```

5. 现有 long 能力持续保留并通过回归测试锁定。
6. 每个 phase 都必须可编译、可测试、可回滚。

---

## 3. 目标架构

### Layer A — Domain Schema / Runtime State
需要统一升级 side 的对象：
- `Candidate`
- `TradeManagementPlan`
- `TradeManagementState`
- `positions.json` 记录
- `events.jsonl` 生命周期事件
- `place_live_trade(...)`
- `monitor_live_trade(...)`
- `reconcile(...)`

新增核心字段：
- `side`
- `position_key`
- `trigger_type`
- `higher_timeframe_bias`
- `overextension_flag`
- `remaining_quantity`
- `current_stop_price`
- `highest_price_seen`
- `lowest_price_seen`

### Layer B — Candidate Engines
拆成两个独立扫描器：
- `scan_long_candidates()`
- `scan_short_candidates()`

统一聚合入口：
- `run_scan_once()`
- `merge_and_rank_candidates()`

### Layer C — Execution / Protection
统一执行接口：

```python
place_live_trade(candidate: Candidate, ...)
```

方向由 `candidate.side` 派生：
- long：买开 + 卖出保护止损
- short：卖开 + 买入保护止损

### Layer D — Management / Monitor
通过方向乘子统一判断：

```python
direction = 1 if side == "long" else -1
risk_per_unit = abs(entry - stop)
reached(target) = direction * (price - target) >= 0
hit_stop(stop) = direction * (price - stop) <= 0
```

### Layer E — Shared Risk
风控从单边升级为共享暴露池：
- `max_long_positions`
- `max_short_positions`
- `max_net_exposure_usdt`
- `max_gross_exposure_usdt`
- `per_symbol_single_side_only`
- `opposite_side_flip_cooldown_minutes`
- `side_risk_multiplier`

---

## 4. 数据结构升级细则

### 4.1 Candidate
保留已有通用字段：
- `entry`
- `stop`
- `quantity`
- `recommended_leverage`
- `score`
- `reasons`
- `state`
- `alert_tier`
- `risk_per_unit = abs(entry - stop)`

新增字段：
- `side`
- `trigger_type`：`breakout | breakdown | reclaim_fail`
- `higher_timeframe_bias`
- `overextension_flag`
- `entry_pattern`
- `directional_conviction`
- `setup_family`：`momentum_breakout | breakdown_continuation | reclaim_fail`

### 4.2 TradeManagementPlan
保留原有结构并补齐方向字段：
- `side`
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
- `trailing_mode`
- `regime_profile`

方向化规则：
- long：`entry + nR`
- short：`entry - nR`

### 4.3 TradeManagementState
新增或标准化字段：
- `side`
- `position_key`
- `remaining_quantity`
- `current_stop_price`
- `moved_to_breakeven`
- `tp1_hit`
- `tp2_hit`
- `highest_price_seen`
- `lowest_price_seen`
- `monitor_mode`
- `trade_management_plan`

### 4.4 positions.json
每条持仓记录升级为：
- `position_key`
- `symbol`
- `side`
- `status`
- `quantity`
- `remaining_quantity`
- `entry_price`
- `stop_price`
- `current_stop_price`
- `stop_order_id`
- `protection_status`
- `moved_to_breakeven`
- `tp1_hit`
- `tp2_hit`
- `highest_price_seen`
- `lowest_price_seen`
- `trade_management_plan`
- `entry_order_feedback`

### 4.5 events.jsonl
事件名保持统一，payload 统一补充：
- `symbol`
- `side`
- `position_key`
- `price`
- `quantity`
- `reason`

保留统一事件名：
- `entry_filled`
- `protection_confirmed`
- `breakeven_moved`
- `tp1_hit`
- `tp2_hit`
- `runner_exited`
- `candidate_rejected`
- `trade_invalidated`

### 4.6 兼容迁移规则
- 旧 long-only 记录默认补 `side=long`。
- 旧 key 若只有 `symbol`，迁移成 `symbol:long`。
- 旧事件读取时，缺 side 的记录补成 `long` 语义。
- 迁移必须幂等，多次加载结果一致。

---

## 5. 扫描器升级设计

### 5.1 Long engine
沿用现有强项：
- 币安广场热币
- 涨幅异动
- 5m / 15m 上行动量加速
- breakout continuation
- 高周期向上
- CVD 上行
- OI 上升且价格继续推高

### 5.2 Short engine
short 不做硬镜像，聚焦两类 setup。

#### Setup A — 弱币破位延续
条件重点：
- 24h 跌幅异常
- quote volume 足够
- 5m / 15m 下行动量加速
- 跌破支撑 / 跌破区间低点
- 1h / 4h 趋势向下
- 跌破后没有快速收回

#### Setup B — 反弹失败再下杀
条件重点：
- 已有一段明确走弱
- 先跌后反弹
- 反弹碰 EMA / VWAP / 前低变前高
- 回落确认失败
- 再做 continuation short

### 5.3 候选合并排序
统一排序器负责：
- 分别生成 long pool 与 short pool
- 按 `execution_priority_score`、`quality_score`、`regime_multiplier`、`side_risk_multiplier` 合并排序
- 大盘偏多时提高 long 权重
- 大盘偏空时提高 short 权重

---

## 6. 指标方向化解释

### 6.1 OI
- long：价格涨 + OI 涨 = 正向确认
- short：价格跌 + OI 涨 = 正向确认
- crowding 极端时触发 squeeze / flush 风险惩罚

### 6.2 taker flow
- long 看主动买盘增强
- short 看主动卖盘增强
- 若只有 taker buy 数据，则派生 sell pressure 指标

### 6.3 CVD
- long：CVD 上行更优
- short：CVD 下行更优
- 跌破后 CVD 快速回正时，short 降级

### 6.4 funding / long-short ratio
- funding 极正：更适合寻找 short
- funding 极负：short squeeze 风险更高，short 降级
- long-short ratio 极端偏空：降低 short 分数
- long-short ratio 极端偏多：降低 long 分数

### 6.5 short 特有 guard
- 距离 VWAP / EMA 负偏离过大，降级
- 最近 3~5 根 5m 已大跌，降级
- 单根长阴爆量后直接追空，降级
- OI 下滑，视作可能尾端释放，降级
- 长下影 + 快速收回，降级

---

## 7. 执行层改造

### 7.1 统一下单接口

```python
place_live_trade(candidate: Candidate, client, ...)
```

### 7.2 开仓方向
- long：`BUY`
- short：`SELL`

### 7.3 初始保护单方向
- long：卖出 stop
- short：买入 stop

### 7.4 reduceOnly / TP / closePosition
全部由 `candidate.side` 派生，禁止任何 long 语义写死。

### 7.5 reconcile
从 `symbol` 语义升级到 `position_key` 语义，同时保留第一阶段约束：
- 同一 symbol 单边持仓
- 交易所 side 与 runtime side 不一致时写入 `trade_invalidated` 或 `runtime_resynced`

---

## 8. 持仓管理改造

### 8.1 统一方向 helper

```python
def direction_sign(side: str) -> int:
    return 1 if side == "long" else -1


def reached_target(price: float, target: float, side: str) -> bool:
    return direction_sign(side) * (price - target) >= 0


def hit_stop(price: float, stop: float, side: str) -> bool:
    return direction_sign(side) * (price - stop) <= 0
```

### 8.2 BE / TP1 / TP2
- long：`entry + nR`
- short：`entry - nR`

### 8.3 trailing
long：
- 记录 `highest_price_seen`
- `trailing_floor = highest_price_seen * (1 - trailing_buffer_pct)`
- 跌破 floor，退出 runner

short：
- 记录 `lowest_price_seen`
- `trailing_ceiling = lowest_price_seen * (1 + trailing_buffer_pct)`
- 反弹突破 ceiling，退出 runner

### 8.4 TP 后 stop 管理
- long：stop 上抬
- short：stop 下压

统一抽象为 helper，避免散落的条件分支。

---

## 9. 风控升级

### 9.1 保留现有账户级风控
- `daily_max_loss_usdt`
- `max_consecutive_losses`
- `symbol_cooldown_minutes`
- `max_open_positions`

### 9.2 新增共享暴露控制
- `max_long_positions`
- `max_short_positions`
- `max_net_exposure_usdt`
- `max_gross_exposure_usdt`
- `per_symbol_single_side_only = true`
- `opposite_side_flip_cooldown_minutes`

### 9.3 市场环境偏置
定义：

```python
effective_risk = base_risk_usdt * regime_multiplier * side_multiplier
```

示例：
- bull：long `1.0`，short `0.3 ~ 0.5`
- bear：short `1.0`，long `0.3 ~ 0.5`
- chop：双边都降风险

### 9.4 第一阶段限制
- 同一 symbol 不同时 long/short
- opposite side flip 加冷却
- 缺保护单时禁止反手

---

## 10. 分阶段实施路线

## Phase 1 — Side-aware schema foundation
**目标：** 先完成对象、状态、事件、位置主键升级。

### Task 1.1
给以下对象加 `side`：
- `Candidate`
- `TradeManagementPlan`
- `TradeManagementState`
- runtime position record
- event payload

### Task 1.2
引入 `position_key = f"{symbol}:{side}"`

### Task 1.3
升级 `positions.json` / `events.jsonl` schema，并提供兼容迁移逻辑：
- 旧 long-only 记录默认补 `side=long`
- 旧 key 若只有 symbol，迁移成 `symbol:long`

### Task 1.4
补测试：
- schema backward compatibility
- event payload includes side
- runtime migration test

**验收标准：**
- 原 long-only 流程仍能跑通
- 所有 runtime 记录都能读出 side

---

## Phase 2 — Dual scanner engines
**目标：** 扫描层支持 long/short 双引擎。

### Task 2.1
从现有扫描逻辑抽出：
- `scan_long_candidates()`

### Task 2.2
新增：
- `scan_short_candidates()`

### Task 2.3
实现 short 两类 setup：
- breakdown continuation
- reclaim_fail continuation

### Task 2.4
实现候选合并排序器：
- `merge_and_rank_candidates()`

### Task 2.5
补测试：
- long candidate `side=long`
- short candidate `side=short`
- short 末端追空 guard
- reclaim_fail setup 识别

**验收标准：**
- `--scan-only` 可同时输出 long / short 候选
- 结果包含 `side` 与 `trigger_type`

---

## Phase 3 — Side-aware execution
**目标：** 开仓与初始保护单支持双向。

### Task 3.1
把 `place_live_trade(...)` 改成接收完整 `Candidate`

### Task 3.2: 统一执行参数派生表

**Objective:** 把 entry / stop / reduceOnly / TP 的方向规则收敛成一张 side-aware 参数映射表，确保执行层只消费标准化派生结果。

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Test: `tests/test_binance_futures_momentum_long.py`
- Doc: `/root/.hermes/skills/binance/binance-futures-momentum-long/docs/plans/2026-04-21-binance-momentum-ls-upgrade-plan.md`

**升级内容：**
1. 为 `candidate.side` 建立统一派生字段：`entry_side`、`stop_side`、`tp_side`、`close_side`、`reduce_only`、`close_position`。
2. 把开仓、保护单、分批止盈、平仓逻辑改成只读取上述派生字段。
3. 对 long/short 分别列出一份映射表并写入实现注释与测试断言。

**执行映射表：**

| candidate.side | entry_side | stop_side | tp_side | close_side | reduce_only |
|---|---|---|---|---|---|
| long | BUY | SELL | SELL | SELL | true |
| short | SELL | BUY | BUY | BUY | true |

**验收标准：**
- 所有下单方向均由 `candidate.side` 单点派生。
- 执行函数内部没有散落的 long/short 硬编码方向分支。
- 测试能直接校验 long/short 的完整方向映射。

### Task 3.3: 补齐对账与保护方向测试

**Objective:** 锁定交易所持仓、订单与 runtime state 三者的一致性，覆盖 long/short 初始保护方向与 reduceOnly 语义。

**Files:**
- Modify: `tests/test_binance_futures_momentum_long.py`
- Modify: `scripts/binance_futures_momentum_long.py`

**新增测试点：**
1. long 持仓的保护单方向为 `SELL`。
2. short 持仓的保护单方向为 `BUY`。
3. long/short 的 TP 单都带 `reduceOnly=true`。
4. open position side、open order side、runtime `position_key` 一致时对账通过。
5. side 不一致时输出 `trade_invalidated` 或 `runtime_resynced`。

**建议最小测试样例：**
- `test_reconcile_accepts_long_position_key_alignment`
- `test_reconcile_accepts_short_position_key_alignment`
- `test_initial_protection_direction_for_long`
- `test_initial_protection_direction_for_short`
- `test_take_profit_orders_use_reduce_only_for_both_sides`

**验收标准：**
- 保护单方向、reduceOnly 语义、对账 side 一致性全部有单元测试覆盖。
- long/short 任一方向改坏都会出现明确失败测试。

### Task 3.4: 补齐 live 路径 smoke tests

**Objective:** 用最小可回归 smoke tests 覆盖 short live path，并同时锁定事件 payload 的 side 字段传播。

**Files:**
- Modify: `tests/test_binance_futures_momentum_long.py`
- Modify: `scripts/binance_futures_momentum_long.py`

**新增 smoke 覆盖：**
1. short 候选进入 live execution 后能成功生成 entry order。
2. short entry fill 后能成功生成初始 stop。
3. `entry_filled` 事件 payload 包含 `symbol`、`side`、`position_key`、`quantity`。
4. `protection_confirmed` 事件 payload 包含 `symbol`、`side`、`position_key`、`stop_order_id`。
5. long live smoke 用例继续通过，保持原路径稳定。

**验收标准：**
- long smoke 通过。
- short smoke 通过。
- 事件契约对 long/short 都稳定。

**验收标准：**
- 能成功下 long
- 能成功下 short
- 初始 stop 方向正确

---

## Phase 4 — Side-aware management and monitor
**目标：** monitor 统一支持 long/short。

### Task 4.1: 引入统一方向 helper

**Objective:** 把方向判断下沉到可复用 helper，供 plan 构建、monitor、reconcile、事件判定统一调用。

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Test: `tests/test_binance_futures_momentum_long.py`

**新增 helper：**
```python
def direction_sign(side: str) -> int:
    return 1 if side == "long" else -1


def reached_target(price: float, target: float, side: str) -> bool:
    return direction_sign(side) * (price - target) >= 0


def hit_stop(price: float, stop: float, side: str) -> bool:
    return direction_sign(side) * (price - stop) <= 0
```

**验收标准：**
- monitor 与 plan 计算共享同一组 helper。
- long/short 命中目标与止损的条件判断完全对称。

### Task 4.2: 重构 `build_trade_management_plan()`

**Objective:** 让 trade management plan 根据 `side` 正确生成 BE / TP1 / TP2 / runner / trailing 的触发价格。

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Test: `tests/test_binance_futures_momentum_long.py`

**升级内容：**
1. 统一使用 `initial_risk_per_unit = abs(entry_price - stop_price)`。
2. long 触发价按 `entry + nR` 生成。
3. short 触发价按 `entry - nR` 生成。
4. `runner_qty`、`tp1_close_qty`、`tp2_close_qty` 的数量拆分继续复用现有配置。
5. 把 `side`、`regime_profile`、`trailing_mode` 写入返回 plan。

**验收标准：**
- 同一套输入模板能分别产出 long plan 与 short plan。
- short 的 trigger prices 全部朝正确方向偏移。

### Task 4.3: 重构 `monitor_live_trade()`

**Objective:** 让 monitor 使用统一方向逻辑处理 long/short 的 BE、TP、runner、失效与退出。

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Test: `tests/test_binance_futures_momentum_long.py`

**升级内容：**
1. 把 target hit、stop hit、runner exit 全部改成 helper 调用。
2. `current_stop_price`、`remaining_quantity`、`moved_to_breakeven` 在两个方向上统一更新。
3. 事件写入统一补 `side`、`position_key`。
4. 监控主循环只保留 side-aware 状态机，不保留分叉的 long-only 分支。

**验收标准：**
- long / short 共用一套 monitor 决策流程。
- monitor 输出事件字段与 runtime 状态字段完整一致。

### Task 4.4: 补齐 trailing 方向逻辑

**Objective:** 为 runner 管理补齐 long/short 的极值跟踪与 trailing 出场条件。

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Test: `tests/test_binance_futures_momentum_long.py`

**升级内容：**
1. long 持续刷新 `highest_price_seen`。
2. short 持续刷新 `lowest_price_seen`。
3. long 用 `trailing_floor` 控制 runner 退出。
4. short 用 `trailing_ceiling` 控制 runner 退出。
5. `positions.json` 中持久化两个极值字段，保证重启后可继续追踪。

**验收标准：**
- runner 的 trailing 退出在 long/short 两侧都可恢复、可追踪、可测试。

### Task 4.5: 补齐管理层回归测试

**Objective:** 用管理层测试矩阵锁定 BE / TP / runner / trailing 在双向系统中的行为一致性。

**Files:**
- Modify: `tests/test_binance_futures_momentum_long.py`

**新增测试点：**
1. `test_long_trade_moves_to_breakeven_after_trigger`
2. `test_short_trade_moves_to_breakeven_after_trigger`
3. `test_long_trade_hits_tp1_tp2_and_runner`
4. `test_short_trade_hits_tp1_tp2_and_runner`
5. `test_long_runner_exits_on_trailing_floor_break`
6. `test_short_runner_exits_on_trailing_ceiling_break`

**验收标准：**
- long 旧逻辑稳定。
- short 全路径管理行为正确。
- trailing floor / ceiling 具备独立回归测试。

**验收标准：**
- short 持仓到 TP1、TP2、runner 行为正确
- long 旧逻辑保持稳定

---

## Phase 5 — Shared risk and regime bias
**目标：** 风控从单边升级到双向共享暴露控制。

### Task 5.1: 新增 side caps

**Objective:** 在账户级风控之外，为多头与空头分别建立独立容量上限。

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Test: `tests/test_binance_futures_momentum_long.py`
- Doc: `/root/.hermes/skills/binance/binance-futures-momentum-long/SKILL.md`

**新增配置：**
- `max_long_positions`
- `max_short_positions`

**验收标准：**
- long cap 与 short cap 独立生效。
- 任一方向超限时，新候选会被明确拒绝并写入原因。

### Task 5.2: 新增净暴露与总暴露控制

**Objective:** 把风险约束从单笔、单方向扩展到组合层，控制净敞口与总敞口。

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Test: `tests/test_binance_futures_momentum_long.py`

**新增配置：**
- `max_net_exposure_usdt`
- `max_gross_exposure_usdt`

**建议定义：**
```python
net_exposure = long_exposure_usdt - short_exposure_usdt
gross_exposure = long_exposure_usdt + short_exposure_usdt
```

**验收标准：**
- 新单提交前能同时检查 net 与 gross exposure。
- 组合层暴露限制会写入结构化拒绝原因。

### Task 5.3: 新增 regime side multiplier

**Objective:** 根据 bull / bear / chop 市场环境，对 long/short 风险权重做方向性偏置。

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Test: `tests/test_binance_futures_momentum_long.py`

**升级内容：**
1. 引入 `side_multiplier`。
2. 统一使用 `effective_risk = base_risk_usdt * regime_multiplier * side_multiplier`。
3. bull 环境提高 long 权重。
4. bear 环境提高 short 权重。
5. chop 环境双边同步降风险。

**验收标准：**
- 风险偏置能随 regime 改变。
- long/short 的最终仓位规模变化可被测试断言。

### Task 5.4: 新增单 symbol 单边限制与反手冷却

**Objective:** 在第一阶段保持单 symbol 单边持仓，并为 opposite side flip 增加冷却约束。

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Test: `tests/test_binance_futures_momentum_long.py`

**新增规则：**
1. `per_symbol_single_side_only = true`
2. `opposite_side_flip_cooldown_minutes`
3. 存在未确认保护单时禁止反手
4. runtime 发现同 symbol opposite side 冲突时，直接拒绝新单并记录事件

**验收标准：**
- 同一 symbol 不会同时开多开空。
- 反手受冷却与保护状态双重约束。

### Task 5.5: 补齐共享风控测试矩阵

**Objective:** 用共享风控测试矩阵锁定 side caps、exposure、single-side-only、flip cooldown 四类约束。

**Files:**
- Modify: `tests/test_binance_futures_momentum_long.py`

**新增测试点：**
1. `test_blocks_candidate_when_net_exposure_exceeds_limit`
2. `test_blocks_candidate_when_gross_exposure_exceeds_limit`
3. `test_blocks_opposite_side_within_flip_cooldown`
4. `test_blocks_same_symbol_opposite_side_when_single_side_only_enabled`
5. `test_allows_preferred_side_to_use_higher_risk_under_regime_bias`

**验收标准：**
- 风控拒绝原因可定位。
- 双向持仓组合的风险利用率与 regime 偏置一致。

**验收标准：**
- 多空同开时风险不会对称满载
- 风险偏置与 regime 一致

---

## Phase 6 — Docs and runtime operations
**目标：** 文档、skill、运行说明、迁移说明同步完成。

### Task 6.1: 更新 SKILL 命名与说明

**Objective:** 让 skill 名称、简介、目标能力与双向 side-aware 架构一致。

**Files:**
- Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/SKILL.md`
- Doc: `/root/.hermes/skills/binance/binance-futures-momentum-long/docs/plans/2026-04-21-binance-momentum-ls-upgrade-plan.md`

**升级内容：**
1. 在 SKILL 标题与描述中明确 long/short 双引擎。
2. 若目录名称暂时保留 `binance-futures-momentum-long`，在文档中明确“能力升级为 LS，目录名后续迁移”。
3. 在简介中加入 side-aware execution、monitor、shared risk。
4. 保留第一阶段约束：单 symbol 单边持仓。

**验收标准：**
- SKILL 文档能独立说明双向版本的能力边界与运行方式。

### Task 6.2: 补运行说明

**Objective:** 用最短路径说明升级后系统如何扫描、执行、管理与受限运行。

**Files:**
- Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/SKILL.md`
- Optional: `/root/.hermes/skills/binance/binance-futures-momentum-long/docs/README.md`

**运行说明必须覆盖：**
1. long/short 双引擎如何产出候选。
2. side-aware 持仓管理如何处理 BE、TP1、TP2、runner trailing。
3. 第一阶段保持单 symbol 单边模式。
4. shared risk 与 regime bias 如何影响仓位。
5. `positions.json` 与 `events.jsonl` 的运行期职责。

**验收标准：**
- 新读者可仅凭运行说明理解升级后的操作边界。

### Task 6.3: 补 runtime migration 文档

**Objective:** 给旧 long-only runtime 数据提供幂等迁移说明，确保升级过程可回滚、可恢复。

**Files:**
- Create or Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/docs/runtime-migration.md`
- Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/SKILL.md`

**迁移文档必须覆盖：**
1. 旧 `positions.json` 如何补 `side=long`。
2. 旧 symbol key 如何升级成 `symbol:long`。
3. 旧 `events.jsonl` 缺 side 的兼容读取策略。
4. 幂等迁移校验方式。
5. 回滚前需要保留的备份文件。

**验收标准：**
- migration 文档包含输入样例、输出样例、幂等规则、回滚说明。

### Task 6.4: 补测试与验收命令文档

**Objective:** 把双向升级后的核心测试、smoke、回归、验收命令写成固定清单，便于逐 phase 执行。

**Files:**
- Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/SKILL.md`
- Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/docs/plans/2026-04-21-binance-momentum-ls-upgrade-plan.md`

**文档内容：**
1. schema migration tests
2. dual scanner tests
3. execution tests
4. monitor / trailing tests
5. shared risk tests
6. long-only regression tests
7. 分阶段推荐命令顺序

**建议命令模板：**
```bash
pytest tests/test_binance_futures_momentum_long.py -q
pytest tests/test_binance_futures_momentum_long.py -k "migration or side or short or trailing" -q
python scripts/binance_futures_momentum_long.py --scan-only
```

**验收标准：**
- 文档里存在一份可直接执行的验收命令清单。

---

## 11. 最小落地顺序

推荐严格按顺序推进：
1. 先改 schema
2. 再拆 dual scanner
3. 再改 execution
4. 再改 monitor
5. 再加 shared risk
6. 最后更新 skill 与部署说明

这个顺序便于逐步验证与回归。

---

## 12. 测试矩阵

### 12.1 单元测试
- `build_candidate()` with side
- `build_trade_management_plan()` for long/short
- `reached_target()` / `hit_stop()` helpers
- `scan_short_candidates()` setup detection

### 12.2 状态与兼容测试
- positions migration from old schema
- event payload side propagation
- reconcile runtime by `position_key`

### 12.3 执行测试
- long entry + long protection
- short entry + short protection
- reduceOnly side correctness

### 12.4 管理测试
- long BE / TP1 / TP2 / runner
- short BE / TP1 / TP2 / runner
- trailing floor / ceiling

### 12.5 风控测试
- side caps
- net/gross exposure
- same symbol single-side-only
- flip cooldown

### 12.6 回归测试
- 现有 long strategy regression
- candidate ordering stability for long-only mode
- event contract regression

---

## 13. 推荐命名与运行描述

### 13.1 升级命名
建议升级后名称：

`binance-futures-momentum-ls`

### 13.2 运行描述
Binance USDT-M 永续双向动量系统：
- long 做热币突破延续
- short 做弱币破位延续 / 反弹失败续跌
- 统一使用固定 USDT 风险建仓
- 开仓后进入方向感知持仓管理
- 使用 BE、TP1、TP2、runner trailing
- 全程持久化 `positions.json` 与 `events.jsonl`
- 受共享风控与净暴露约束

---

## 14. 当前执行建议

当前先从 **Phase 1 / Task 1.1 ~ 1.4** 开始。
本轮先完成：
1. 文档写入
2. schema 与迁移方案确认
3. 再进入代码升级

当前最关键的一句：
**双向升级的核心是让整个 skill 从 long-only 变成 side-aware。**
