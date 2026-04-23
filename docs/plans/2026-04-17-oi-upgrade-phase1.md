# Binance OI Upgrade Phase 1 Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** 在不替换当前实盘 bot 主结构的前提下，把现有 momentum 候选构建链升级为可承接 OI 异动识别的第一阶段版本。

**Architecture:** 复用现有 `binance_futures_momentum_long.py` 的 `Candidate -> build_candidate() -> run_scan_once()` 主链，不先改 supervisor / watcher。第一阶段只做“候选层特征扩展 + 分数/原因输出扩展 + 最小状态分类输出”，保证当前 live 执行路径仍兼容。所有新增行为先由 `tests/test_strategy_v2.py` 用 TDD 锁定。

**Tech Stack:** Python, pytest, dataclasses, Binance futures REST, 现有单文件策略脚本结构。

---

## Confirmed code entrypoints

- 主脚本：`/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py`
- 测试入口：`/root/.hermes/skills/binance/binance-futures-momentum-long/tests/test_strategy_v2.py`
- 关键结构：
  - `Candidate` dataclass: `scripts/binance_futures_momentum_long.py:57`
  - `evaluate_higher_timeframe_trend(...)`: `:994`
  - `build_candidate(...)`: `:1066`
  - `run_scan_once(...)`: `:1708`
  - `run_loop(...)`: `:1887`
- 非首阶段范围：
  - `/root/.hermes/scripts/binance_momentum_supervisor.py`
  - `/root/.hermes/scripts/binance_position_watcher.py`

## Phase 1 scope boundaries

### In scope
1. 给 candidate 增加 OI / taker / funding percentile / state 分类所需字段。
2. 在 `build_candidate()` 内增加“相对异常值”特征计算入口。
3. 在 `reasons` 与 `score` 基础上增加更结构化的解释输出。
4. 输出一个最小可用状态：`none/watch/launch/chase/overheated`。
5. 保持现有 live 交易链兼容；若新字段缺失，回退为不加分不加状态。
6. 先用静态/可注入样本完成测试，不在本阶段接入独立 replay 系统。

### Out of scope
1. 不重写 supervisor / watcher。
2. 不做独立 OI 引擎进程。
3. 不做完整 distribution 检测闭环。
4. 不做实时 websocket / 多层 universe 调度器。
5. 不改下单与持仓管理规则。

## New behavior to add in Phase 1

### 1) Candidate schema extension
在 `Candidate` 增加以下字段：
- `oi_change_pct_5m: float`
- `oi_change_pct_15m: float`
- `oi_acceleration_ratio: float`
- `taker_buy_ratio: Optional[float]`
- `funding_rate_percentile_hint: Optional[float]`
- `setup_score: float`
- `exhaustion_score: float`
- `state: str`
- `state_reasons: List[str]`

约束：
- 不删除现有 `score` / `reasons`，只在其上兼容扩展。
- `state` 默认值必须可回退到 `"none"`。

### 2) Feature calculation strategy
不要先引入复杂外部依赖。先在主脚本内新增纯函数，便于测试：
- `compute_relative_oi_features(...)`
- `classify_candidate_state(...)`
- 可选：`score_candidate_state(...)`

建议输入：
- 最近 5m / 15m / 1h klines 派生结果
- funding 当前值与短均值
- 可选 taker buy ratio / OI 序列（若当前无真实接口，则允许由测试直接注入）

第一阶段的核心不是拿到“真实完美 OI 数据”，而是先把：
- 特征位
- 分数位
- 状态位
- reasons 位
完整接入候选构建链。

### 3) State machine minimum contract
先做最小硬约束：
- `overheated` 优先级最高，一旦触发直接覆盖普通 score 映射。
- 普通映射只允许：`none -> watch -> launch -> chase`
- 本阶段不开放 `distribution` 自动触发，但预留扩展位与注释。
- `state_reasons` 必须输出命中依据，不能只给 state 名称。

建议最小判定方向：
- `watch`: 动量/突破基础成立，但 OI 异动不足
- `launch`: 动量成立 + OI 正向异常 + HTF 允许
- `chase`: launch 基础上，短周期加速更强，但未过热
- `overheated`: RSI / funding / price-distance / 过强加速任一硬覆盖

### 4) Compatibility rule
- `run_scan_once()` 返回中的 `candidates[]` 与 `selected` 增加新字段，但保留旧字段不变。
- `run_loop()` 不按新 state 改交易决策；本阶段只增强扫描解释和候选排序基础。
- 若后续要把 `state` 真正接入 live 下单门控，再放到 Phase 2。

---

## Task 1: Add failing tests for candidate state classification

**Objective:** 先锁定候选状态输出契约。

**Files:**
- Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/tests/test_strategy_v2.py`
- Modify later: `/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py`

**Step 1: Write failing tests**
新增至少 3 个测试：
- `test_build_candidate_marks_launch_when_momentum_and_oi_are_aligned`
- `test_build_candidate_marks_overheated_when_distance_or_funding_is_extreme`
- `test_build_candidate_defaults_state_to_none_when_oi_features_absent`

测试要求：
- 继续沿用当前 `build_candidate(...)` 测试风格
- 断言 `candidate.state`
- 断言 `candidate.state_reasons`
- 断言旧字段 `score` / `reasons` 仍存在

**Step 2: Run test to verify failure**
Run:
`python -m pytest /root/.hermes/skills/binance/binance-futures-momentum-long/tests/test_strategy_v2.py -q`
Expected: 新增测试失败，原因是 `Candidate` 还没有新字段或 `build_candidate()` 未生成状态。

**Step 3: Implement minimal schema + defaults**
在 `Candidate` 中增加字段，并在 `build_candidate()` 先填默认值：
- `state="none"`
- `state_reasons=[]`
- 新增 OI / taker / percentile 字段默认 0 或 `None`

**Step 4: Re-run tests**
Run 同上。
Expected: 至少从字段缺失类失败，推进到分类逻辑失败。

## Task 2: Add pure-function tests for OI-relative feature scoring

**Objective:** 把 OI 相对特征计算从 `build_candidate()` 中抽成纯函数并先测试。

**Files:**
- Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/tests/test_strategy_v2.py`
- Modify later: `/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py`

**Step 1: Write failing tests**
新增测试：
- `test_compute_relative_oi_features_detects_positive_acceleration`
- `test_compute_relative_oi_features_handles_missing_series_gracefully`

建议断言：
- 返回 dict 包含 `oi_change_pct_5m`、`oi_change_pct_15m`、`oi_acceleration_ratio`
- 缺失数据时不抛异常，并返回可回退结果

**Step 2: Verify RED**
Run 指定测试。
Expected: `NameError` / `AttributeError`。

**Step 3: Implement minimal pure function**
新增纯函数，输入可以先是简单数字或 list，避免先绑真实 API。

**Step 4: Verify GREEN**
Run 指定测试，再跑全文件。

## Task 3: Integrate new feature output into build_candidate()

**Objective:** 让 `build_candidate()` 真正携带 OI 相对特征与结构化状态。

**Files:**
- Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py:1066-1285`
- Test: `/root/.hermes/skills/binance/binance-futures-momentum-long/tests/test_strategy_v2.py`

**Step 1: Write failing integration tests**
新增测试验证：
- `build_candidate()` 返回对象含新字段
- `setup_score` / `exhaustion_score` 可区分
- `overheated` 会覆盖普通状态

**Step 2: Verify RED**
Run 指定测试。

**Step 3: Minimal implementation**
在 `build_candidate()`：
- 组装新特征
- 调用 `classify_candidate_state(...)`
- 将结果写回 `Candidate`
- 对旧 `score` 维持兼容，必要时让 `score = setup_score - exhaustion_score * weight + legacy score components`

**Step 4: Verify GREEN**
跑指定测试，再跑全文件。

## Task 4: Expose new fields in run_scan_once() output

**Objective:** 让 scan-only 结果可直接观察新状态与新特征。

**Files:**
- Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py:1828-1883`
- Test: `/root/.hermes/skills/binance/binance-futures-momentum-long/tests/test_strategy_v2.py`

**Step 1: Write failing tests**
新增测试：
- `test_run_scan_once_includes_state_and_oi_fields_in_candidate_output`
- `test_run_scan_once_selected_payload_exposes_state_fields`

**Step 2: Verify RED**
Run 指定测试。

**Step 3: Implement minimal serialization**
在 `result["candidates"]` 和 `result["selected"]` 中增加：
- `state`
- `state_reasons`
- `setup_score`
- `exhaustion_score`
- `oi_change_pct_5m`
- `oi_change_pct_15m`
- `oi_acceleration_ratio`
- `taker_buy_ratio`
- `funding_rate_percentile_hint`

**Step 4: Verify GREEN**
跑全文件。

## Task 5: Add guardrail comments and docs for Phase 2 handoff

**Objective:** 避免第一阶段被误当成完整 OI 系统。

**Files:**
- Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py`
- Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/docs/plans/2026-04-17-oi-upgrade-phase1.md`

**Step 1: Add code comments**
在新函数附近注明：
- 当前 state 仅用于扫描解释，不直接改 live execution gating
- `distribution` 延后到 Phase 2
- 真实 OI / taker buy 数据接入点待后续补齐

**Step 2: Final verification**
Run:
- `python -m pytest /root/.hermes/skills/binance/binance-futures-momentum-long/tests/test_strategy_v2.py -q`
- `python -m py_compile /root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py`

Expected:
- 全量测试通过
- 语法检查通过

---

## Implementation notes

1. 第一阶段优先新增纯函数，不要把更多复杂逻辑直接堆进 `run_scan_once()`。
2. `Candidate` 扩展要后向兼容，避免打断现有序列化和排序。
3. 没有真实 OI 数据时，先允许函数接受可选注入值；之后再把数据获取层接进来。
4. `overheated` 必须是硬覆盖，不受普通 score 排名影响。
5. `distribution` 不在本阶段上线，但在命名和注释里预留位。

## Verification checklist

- [ ] 新增测试先失败后通过
- [ ] `Candidate` 新字段不破坏旧测试
- [ ] `run_scan_once()` 新输出保持 JSON 可序列化
- [ ] 现有 live 流程未改下单门控逻辑
- [ ] scan-only 输出能解释 state / OI 特征 / 过热原因

## Ready-for-implementation handoff

完成本计划后，下一步直接执行：
1. 先写失败测试
2. 再补 `Candidate` 字段与纯函数
3. 再接入 `build_candidate()`
4. 最后补 `run_scan_once()` 输出

这会得到一个“可解释、可测试、兼容现有实盘链路”的 OI 第一阶段骨架。