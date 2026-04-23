# Binance Futures Full Upgrade Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** 在不丢失当前实盘守护能力的前提下，把现有 Binance 合约 bot 从“单脚本动量追多 + 基础守护”升级为“可对账、可恢复、可解释、可扩展 OI 状态机、执行更稳健”的实盘系统。

**Architecture:** 保持现有主脚本 `binance_futures_momentum_long.py` 为交易执行入口，但把升级拆成四层：数据/特征层、候选/状态机层、执行/风控层、观测/恢复层。短期先修复实盘安全与 runtime 漂移，再把 OI Phase 1 的解释层扩成真实数据驱动的 Phase 2，最后收口执行加固与恢复闭环。

**Tech Stack:** Python 3、Binance Futures REST API、现有单文件策略脚本 + runtime JSON 状态存储 + supervisor/watcher 守护链路、pytest。

---

## 0. Current Ground Truth (must preserve)

1. 当前真实账户仍有 `SOONUSDT` 多仓 88，且 `open_orders = []`，说明真实仓位没有保护单。
2. 当前真实进程仍在运行：
   - watcher PID `2658175`
   - supervisor PID `2698889`
   - bot PID `2699901`
3. `runtime/positions.json` 已能看到 `SOONUSDT`，但状态被写成 `status=closed` 且 `protection_status=missing`，说明 runtime 对账语义不稳定。
4. `reconcile_runtime_state()` 目前只能识别“缺保护单”，会发 `protection_missing` 事件，但不会自动补保护单。
5. `place_stop_market_order()` 当前走 `/fapi/v1/algoOrder`，需要专门验证这条链路为何没有在真实仓位上留下有效保护单。
6. OI Phase 1 已完成：`Candidate` 新字段、`compute_relative_oi_features()`、`classify_candidate_state()`、`build_candidate()` enrichment、测试 `36 passed`。

---

## 1. Upgrade Objectives

### 1.1 Safety first
- 任何时刻先保证“真实仓位 ≈ runtime 认知 ≈ 保护单状态”。
- 若出现仓位但无保护单，系统应进入“修复优先”而不是继续扫描新单。
- supervisor 不应只看“账户非空仓就阻止重启”，还要识别“非空仓但缺保护，需要进入修复/告警模式”。

### 1.2 Strategy evolution
- 保留现有动量追多主骨架。
- 把 OI Phase 1 从 explainability 升级为真实 OI / taker buy ratio / funding percentile 驱动的状态层。
- 新状态机先影响排序与风险预算，再逐步影响 live gating。

### 1.3 Operability
- 让 runtime 成为“可信镜像”而不是“历史残留账本”。
- 所有关键状态变化都要有结构化事件、可验证测试、可人工复盘。
- 支持启动自检、重启后恢复、异常时停机而不是静默继续跑偏。

---

## 2. Target Architecture

### Layer A: Market Data / Feature Inputs
Files:
- Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py`
- Create later if needed: `/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/oi_feature_cache.py`
- Test: `/root/.hermes/skills/binance/binance-futures-momentum-long/tests/test_strategy_v2.py`

Responsibilities:
- 获取并缓存：
  - OI 当前值、5m 前、15m 前
  - taker buy / sell 倾向
  - funding 历史基线（至少短期 percentile 占位，后续扩到正式 percentile）
- 统一单位到 USD notional。
- 明确缺失值回退策略，避免 live 侧被 `None` 污染。

### Layer B: Candidate / State Machine
Files:
- Modify: `.../binance_futures_momentum_long.py`
- Test: `.../tests/test_strategy_v2.py`

Responsibilities:
- 在现有 `build_candidate()` 基础上升级 `compute_relative_oi_features()` 输入来源。
- 落地状态：`none/watch/launch/chase/distribution/overheated`。
- 增加：
  - `failed_launch`
  - hysteresis（进出阈值分离）
  - distribution 硬触发逻辑
- 先影响排序和解释，再渐进影响 live gating。

### Layer C: Execution / Protection / Reconciliation
Files:
- Modify: `.../binance_futures_momentum_long.py`
- Modify: `/root/.hermes/scripts/binance_momentum_supervisor.py`
- Modify: `/root/.hermes/scripts/binance_position_watcher.py`
- Test: `.../tests/test_strategy_v2.py`

Responsibilities:
- 规范化入场后保护单建立。
- 启动时对账交易所真实仓位、挂单、runtime 仓位、保护状态。
- 若发现缺保护单：
  - 优先补单 / 或显式进入 halt + 强告警
  - 禁止继续扫新单
- 明确区分：
  - orphan position
  - missing protection
  - stale runtime position
  - exchange flat but runtime open

### Layer D: Observability / Recovery
Files:
- Modify: `.../binance_futures_momentum_long.py`
- Modify: `/root/.hermes/scripts/binance_momentum_supervisor.py`
- Modify: `/root/.hermes/scripts/binance_position_watcher.py`
- Optional docs: `docs/plans/*.md`

Responsibilities:
- 统一事件命名、payload schema、通知节奏。
- 增加恢复类事件：
  - `protection_repair_started`
  - `protection_repair_succeeded`
  - `protection_repair_failed`
  - `runtime_resynced`
  - `restart_blocked_missing_protection`
- 重启链路在恢复模式和正常模式之间明确切换。

---

## 3. Immediate Risk Findings to Address Before Any Aggressive Upgrade

### Finding A: Real position without protection
Evidence:
- Binance 实时查询：`SOONUSDT` 持仓 88，`open_orders=[]`
- runtime：`SOONUSDT.protection_status = missing`
- reconcile 事件：`positions_missing_protection=["SOONUSDT"]`

Required behavior:
- 一旦发现该状态，不允许 bot 继续以“只是提醒一下”的模式运行。
- 必须把系统推进到“修复保护 or 强停机 + 阻止新单”。

### Finding B: Runtime drift semantics are weak
Evidence:
- `positions.json` 中 `SOONUSDT` 已存在，但被写成 `status=closed`
- `reconcile_runtime_state()` 对“交易所仍有仓位”时只更新 `protection_status` 和 `updated_at`，没有强制纠正状态字段。

Required behavior:
- 对账后如果交易所仍有仓位，runtime 必须写回 `status=open` 或 `status=orphan`，不能保留 `closed`。

### Finding C: Protection placement path needs hard validation
Evidence:
- `place_stop_market_order()` 使用 `/fapi/v1/algoOrder`
- 真实账户最终没有留下保护单

Required behavior:
- 明确验证 Binance 这条接口在当前账户/参数下是否有效。
- 若不稳定，切到更可控的 `/fapi/v1/order` + `STOP_MARKET` 路径，或至少增加回查确认。

---

## 4. Task Plan

### Task 1: Stabilize runtime reconciliation semantics

**Objective:** 让 runtime 仓位状态准确反映交易所真相，解决 `closed` / `open` / `orphan` 漂移。

**Files:**
- Modify: `/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py:807-861`
- Test: `/root/.hermes/skills/binance/binance-futures-momentum-long/tests/test_strategy_v2.py`

**Step 1: Write failing tests**
- 测试“交易所仍有仓位时，已有 runtime 条目即使是 `closed` 也会被改回 `open`”
- 测试“交易所有仓位但 runtime 无条目时，记为 `orphan`”
- 测试“交易所已平仓时，runtime 条目会被改成 `closed`”

**Step 2: Run tests to verify failure**
Run:
`python -m pytest /root/.hermes/skills/binance/binance-futures-momentum-long/tests/test_strategy_v2.py -q`
Expected: 新增对账测试失败。

**Step 3: Implement minimal fix**
- 在 `reconcile_runtime_state()` 中：
  - 交易所存在仓位且 runtime 已有条目时，强制写回：
    - `status = 'open'`（如果本地不是 orphan）
    - `quantity`
    - `entry_price`
    - `mark_price`
    - `unrealized_pnl`
  - 仅当 symbol 不在交易所仓位中时才写 `closed`

**Step 4: Run tests to verify pass**
Expected: 对账相关新增测试通过。

**Step 5: Commit**
`git commit -m "fix: resync runtime positions from exchange state"`

---

### Task 2: Add protection-order verification after entry

**Objective:** 开仓后不是“下了保护单请求就算完成”，而是必须回查交易所确认保护单确实存在。

**Files:**
- Modify: `.../binance_futures_momentum_long.py:1491-1641`
- Test: `.../tests/test_strategy_v2.py`

**Step 1: Write failing tests**
- 测试“`place_live_trade()` 在 stop 下单返回成功但回查不到保护单时，抛出明确异常或标记 repair-needed”
- 测试“回查到保护单时，返回正常 `initial_stop_placed`”

**Step 2: Run tests to verify failure**
Expected: 保护单确认逻辑尚不存在。

**Step 3: Implement minimal fix**
- 新增辅助函数，例如 `ensure_symbol_has_protection(client, symbol)`
- `place_stop_market_order()` 执行后立刻查 `fetch_open_orders(symbol)`
- 若仍无保护：
  - 记录 `protection_missing`
  - 将 runtime position 标成 `missing`
  - 抛错让上层决定 halt / repair

**Step 4: Run tests to verify pass**
Expected: 保护单验证测试通过。

**Step 5: Commit**
`git commit -m "fix: verify protection order after live entry"`

---

### Task 3: Introduce startup protection-repair mode

**Objective:** 启动时若发现真实仓位缺保护单，系统进入修复模式，而不是只提醒后继续扫单。

**Files:**
- Modify: `.../binance_futures_momentum_long.py:2074-2116`
- Modify: `/root/.hermes/scripts/binance_momentum_supervisor.py`
- Test: `.../tests/test_strategy_v2.py`

**Step 1: Write failing tests**
- 测试“`positions_missing_protection` 非空时，run_loop 不进入正常扫描分支”
- 测试 supervisor 在账户有仓位且缺保护单时输出 `restart_blocked_missing_protection` 或 `repair_required`

**Step 2: Run tests to verify failure**
Expected: 当前只会发送 `protection_missing` 事件，不会阻止后续扫描。

**Step 3: Implement minimal fix**
- 引入参数：`--repair-missing-protection-on-startup` 或 `--halt-on-missing-protection`
- 默认先安全：发现缺保护时不继续扫新单
- supervisor 查询 `open_orders` 时，如果有仓位但无保护，改成专门阻塞原因，而不是仅 `account_not_flat`

**Step 4: Run tests to verify pass**
Expected: 启动缺保护的路径被明确拦住。

**Step 5: Commit**
`git commit -m "feat: block startup on missing protection"`

---

### Task 4: Harden protection placement transport/path

**Objective:** 确认并稳固 Binance 保护单下单路径，避免 `/algoOrder` 成功假象。

**Files:**
- Modify: `.../binance_futures_momentum_long.py:1491-1518`
- Test: `.../tests/test_strategy_v2.py`
- Optional doc: this plan file notes

**Step 1: Write failing tests**
- 针对新的 stop order builder 写参数序列化测试
- 若切换接口，给新 payload 写最小测试

**Step 2: Verify current live/API behavior manually**
- 先用隔离测试逻辑/小样本验证 `/algoOrder` 的真实返回与 `openOrders` 可见性
- 如果接口语义不匹配现有检查逻辑，则替换实现

**Step 3: Implement minimal fix**
- 优先方案：改为 `/fapi/v1/order` 的 `STOP_MARKET` reduce-only 路径
- 若必须保留 `/algoOrder`，至少在返回后增加专门的“保护单查询接口/字段兼容”逻辑

**Step 4: Run tests to verify pass**
Expected: stop 下单路径有覆盖测试，且回查逻辑不再空转。

**Step 5: Commit**
`git commit -m "fix: harden stop-market placement path"`

---

### Task 5: Phase 2 OI input foundation

**Objective:** 把 OI Phase 1 的占位输入换成真实数据输入，但先不大改 live gating。

**Files:**
- Modify: `.../binance_futures_momentum_long.py`
- Test: `.../tests/test_strategy_v2.py`
- Optional create: `scripts/oi_feature_cache.py`

**Step 1: Write failing tests**
- 测试真实 OI 数据样本进入 `compute_relative_oi_features()` 后能生成稳定特征
- 测试缺失值 fallback
- 测试 `build_candidate()` 使用真实 OI 输入时字段正确回填

**Step 2: Run tests to verify failure**
Expected: 当前 `oi_now/oi_5m_ago/oi_15m_ago` 还是 `None`。

**Step 3: Implement minimal fix**
- 新增 OI 与 taker buy ratio 抓取/缓存
- 在 `build_candidate()` 里接入真实输入
- 保持 `live trade gating remains unchanged`，只先影响 state enrichment / ranking

**Step 4: Run tests to verify pass**
Expected: OI 输入相关测试通过。

**Step 5: Commit**
`git commit -m "feat: wire real oi inputs into candidate enrichment"`

---

### Task 6: Complete state machine semantics

**Objective:** 从 Phase 1 的 `none/watch/launch/chase/overheated` 扩到更完整实用的实盘状态机。

**Files:**
- Modify: `.../binance_futures_momentum_long.py`
- Test: `.../tests/test_strategy_v2.py`

**Step 1: Write failing tests**
- `distribution` 触发测试
- `failed_launch` / cooldown 测试
- hysteresis 进出阈值分离测试

**Step 2: Run tests to verify failure**
Expected: 当前这些能力不存在。

**Step 3: Implement minimal fix**
- 扩展 `classify_candidate_state()`
- 若需要，新增 `transition_candidate_state(prev_state, features)` 辅助函数
- 保持输出可解释：`state_reasons` 必须结构化可读

**Step 4: Run tests to verify pass**
Expected: 状态机新增测试通过。

**Step 5: Commit**
`git commit -m "feat: expand oi state machine semantics"`

---

### Task 7: Let state influence live gating safely

**Objective:** 让新状态机先以低风险方式进入实盘决策层。

**Files:**
- Modify: `.../binance_futures_momentum_long.py:2123-2187`
- Test: `.../tests/test_strategy_v2.py`

**Step 1: Write failing tests**
- 测试 `overheated` 不允许入场
- 测试 `distribution` 不允许新追单
- 测试 `watch` 只保留观察，不开单
- 测试 `launch/chase` 才能进入 live 执行候选

**Step 2: Run tests to verify failure**
Expected: 当前 live gating 还没显式用到 state。

**Step 3: Implement minimal fix**
- 在 `best` 候选 live 执行前增加 state-aware gating
- 保持可配置开关，方便回退

**Step 4: Run tests to verify pass**
Expected: state-aware gating 测试通过。

**Step 5: Commit**
`git commit -m "feat: apply candidate state to live gating"`

---

### Task 8: Upgrade supervisor and watcher semantics

**Objective:** 让守护与盯盘从“是否活着”升级为“是否安全、是否一致、是否可恢复”。

**Files:**
- Modify: `/root/.hermes/scripts/binance_momentum_supervisor.py`
- Modify: `/root/.hermes/scripts/binance_position_watcher.py`

**Step 1: Write failing tests or explicit manual verification checklist**
- 若暂不拆可测模块，至少先把关键逻辑提成纯函数后补测试

**Step 2: Implement minimal fix**
- supervisor 区分：
  - `account_not_flat`
  - `missing_protection`
  - `open_orders_present`
  - `runtime_drift_detected`
- watcher 增加：
  - `protection_missing`
  - `runtime_exchange_mismatch`
  - `watcher_resynced`

**Step 3: Verify behavior**
- 通过后台 watch patterns 确认新事件可触发
- `process poll/log` 与系统 `pgrep` 同时验证

**Step 4: Commit**
`git commit -m "feat: enrich supervisor and watcher safety events"`

---

### Task 9: Add replay / regression fixtures for high-risk states

**Objective:** 让后续升级不再依赖“临场盯实盘”才能验证。

**Files:**
- Modify: `.../tests/test_strategy_v2.py`
- Optional create: `.../tests/fixtures/*.json`

**Step 1: Add fixtures**
- OI 拉升但 funding 过热
- 假突破后 failed_launch
- distribution 出货
- 仓位存在但保护单缺失

**Step 2: Add regression tests**
- `build_candidate()` / `run_loop()` / `reconcile_runtime_state()` 都应覆盖这些样本

**Step 3: Verify**
Run:
`python -m pytest /root/.hermes/skills/binance/binance-futures-momentum-long/tests/test_strategy_v2.py -q`
Expected: 全绿。

**Step 4: Commit**
`git commit -m "test: add replay-style regression fixtures for safety and oi states"`

---

## 5. Recommended Execution Order

1. Task 1 — 修 runtime 对账语义
2. Task 2 — 开仓后强制回查保护单
3. Task 3 — 启动缺保护进入阻塞/修复模式
4. Task 4 — 加固 stop 下单链路
5. Task 8 — 升级 supervisor / watcher 事件语义
6. Task 5 — 接入真实 OI 输入
7. Task 6 — 扩完整状态机
8. Task 7 — 状态机渐进进入 live gating
9. Task 9 — 回放/回归夹具补齐

这样排序的原因：先把“真实钱会出事”的地方收口，再做 alpha 升级。

---

## 6. Verification Checklist

Every stage must re-run:

```bash
python -m py_compile \
  /root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py \
  /root/.hermes/scripts/binance_momentum_supervisor.py \
  /root/.hermes/scripts/binance_position_watcher.py

python -m pytest \
  /root/.hermes/skills/binance/binance-futures-momentum-long/tests/test_strategy_v2.py -q
```

Runtime verification after safety-related changes:

```bash
date '+%F %T %Z'
pgrep -af 'binance_momentum_supervisor.py|binance_futures_momentum_long.py|binance_position_watcher.py' || true
```

Exchange truth verification after safety-related changes:
- 实时查：账户余额 / 持仓 / open orders
- 必须确认：
  - 如果有仓位，则保护单状态明确
  - runtime 与交易所状态一致
  - supervisor 不会误判平仓/误拉起新 bot

---

## 7. Definition of Done

“原币安合约全面升级”完成的标准不是只看代码提交，而是同时满足：

1. 真实仓位出现时，runtime 一定同步为正确 open/orphan 状态。
2. 真实仓位没有保护单时，系统一定进入阻塞或修复流程，不会继续扫新单。
3. 开仓后保护单不是“尝试挂了”，而是“已确认存在”。
4. OI 输入已经从占位值升级到真实数据。
5. `distribution` / `failed_launch` / hysteresis 已进入状态机。
6. 新状态机至少以 gating 或 ranking 之一实质影响 live 决策。
7. supervisor / watcher / runtime 事件可支撑复盘与告警。
8. 全量测试与语法检查通过。

---

## 8. Immediate next implementation slice

如果马上开始实现，优先做这 3 件：

1. Task 1：修 `reconcile_runtime_state()`，解决 runtime `closed/open` 漂移
2. Task 2：开仓后强制回查保护单是否真实存在
3. Task 3：启动时发现 `positions_missing_protection` 就阻断后续扫描

这三项做完，实盘安全面才算真正收口一大半。
