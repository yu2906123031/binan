# 币安合约项目重构计划清单

## 目标
建立一份持续维护的重构主清单，用于跟踪每次升级后的已完成任务、待办任务、验证方式与剩余风险，避免后续优化重新从头梳理。

## 使用规则
1. 每次功能升级或结构重构后，立即更新本文件。
2. 已完成任务从待办区移动到“已完成升级记录”。
3. 每条已完成记录必须包含：日期、范围、修改文件、验证结果、遗留风险。
4. 新发现的问题直接追加到对应阶段的待办任务中。
5. 如果某项任务被拆分，保留父任务编号，并追加子任务编号，例如 `P0-1a`、`P0-1b`。

---

## 当前状态总览
- 项目主复杂度集中在 `scripts/binance_futures_momentum_long.py`
- 当前测试基线：
  - `pytest -q` 通过
  - `pytest -q tests/test_strategy_v2_restore_regression.py` → `70 passed`
  - `pytest -q tests/test_hermes_outer_watcher.py tests/test_cli_args.py tests/test_run_loop_smoke.py` → `57 passed`
  - `pytest -q tests/test_strategy_v2_restore_regression.py tests/test_run_loop_smoke.py` → `111 passed`
  - `python -m pip check` 通过
  - `python -m compileall -q scripts main.py tests` 通过
- 当前结构性重点：
  - runtime-state 原子写入
  - watcher 可靠性增强
  - 主策略脚本拆模块
  - 风控 / 下单 / 持仓监控边界测试补强

---

## 已完成升级记录

### 2026-05-09｜runtime_store 模块提取与主脚本回接
- 状态：已完成
- 范围：把 runtime-state 持久化、positions canonicalization、runtime event normalization 从主脚本抽到独立模块，并保持旧调用点兼容
- 修改文件：
  - `scripts/runtime_store.py`
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `docs/architecture.md`
- 完成内容：
  - 新建 `scripts/runtime_store.py`，承接 `RuntimeStateStore`、`load_positions_state`、`save_positions_state`、`migrate_positions_state`、`materialize_positions_state`、`normalize_runtime_event_payload`
  - 同步收口 runtime_store 依赖的 side/key/time/恢复辅助函数，保证提取后模块可独立工作
  - 主脚本改为 `from runtime_store import ...` 回接旧名称，现有调用点与测试导入路径保持稳定
  - 测试 harness 增加 `scripts/` 到 `sys.path`，保证 `importlib.util.spec_from_file_location(...)` 方式加载主脚本时能解析同目录提取模块
  - `docs/architecture.md` 已记录 runtime 提取阶段完成状态
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'runtime_store_module_matches_script or runtime_store_read_events_skips_malformed_trailing_jsonl_row or restore_position_lifecycle_fields_marks_zero_risk_plan_as_recovery_incomplete or restore_position_lifecycle_fields_normalizes_valid_plan_side or runtime_store_save_positions_state_canonicalizes_before_return or runtime_store_load_json_exposes_canonical_positions_only or runtime_store_load_json_rewrites_duplicate_legacy_keys or runtime_store_append_event_backfills_side_and_position_key_from_payload or runtime_store_append_event_fsyncs_and_terminates_each_jsonl_row'` → `10 passed, 60 deselected in 0.17s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py` → `70 passed in 0.51s`
- 遗留风险：
  - `runtime_store.py` 当前仍携带部分 side/key/恢复辅助函数，下一阶段适合继续抽到 domain 模块
  - 主脚本与新模块当前通过顶层导入耦合，后续拆 domain 时需要保持导入方向单向稳定
- 对应待办编号：
  - P1-1a
  - P1-1b
  - P1-1c

### 2026-05-09｜events.jsonl 追加写入可靠性增强
- 状态：已完成
- 范围：`append_event()` 增加落盘刷写保障，`read_events()` 保持尾部损坏容忍
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
- 完成内容：
  - `append_event()` 在每次写入 JSONL 行后执行 `flush()` 与 `os.fsync()`
  - 保持单行 JSON + 换行的追加语义，便于崩溃后保留已完整写入的历史行
  - 新增回归测试覆盖 side / position_key 回填、逐行换行落盘、尾部截断坏行容忍
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "append_event_fsyncs_and_terminates_each_jsonl_row or read_events_skips_malformed_trailing_jsonl_row or append_event_backfills_side_and_position_key_from_payload"` → `3 passed, 65 deselected in 0.11s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py tests/test_run_loop_smoke.py` → `111 passed in 0.97s`
- 遗留风险：
  - `events.jsonl` 体积增长仍未做 rotate / archive 策略
  - 追加写入仍缺少跨进程文件锁，多个 writer 并发场景仍依赖单进程使用约束
- 对应待办编号：
  - P0-1e

### 2026-05-09｜RuntimeStateStore 原子写入与无副作用读取
- 状态：已完成
- 范围：`RuntimeStateStore` 的 runtime json 写入原子化，`positions` 读取去除读时回写副作用
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
- 完成内容：
  - 新增 `_atomic_write_json()`，统一采用临时文件 + `os.replace()` 落盘
  - `save()` 与 `save_json()` 全部切换为原子写入路径
  - `load_json('positions')` 与 `load_json_with_error('positions')` 保留内存态 canonicalization，移除读时回写文件副作用
  - 新增回归测试覆盖读路径无副作用与 `save_json()` 原子 replace 行为
  - 现有 canonical 修复场景改为通过显式 `save_positions_state()` 持久化，语义更清晰
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "load_json_positions_does_not_rewrite or save_json_writes_via_atomic_replace or load_json_rewrites_duplicate_legacy_keys or load_json_repairs_corrupted_short_plan_side or load_json_marks_flat_stop"` → `5 passed, 57 deselected in 0.29s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "malformed_last_cycle or malformed_user_data_stream or malformed_risk_state or malformed_positions_json"` → `6 passed, 60 deselected in 0.33s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py` → `66 passed in 0.25s`
- 遗留风险：
  - `append_event()` 仍采用追加写入语义，后续可单独评估 fsync 与 rotate 策略
- 对应待办编号：
  - P0-1a
  - P0-1b
  - P0-1c

### 2026-05-09｜strategy 状态降级路径事件化
- 状态：已完成
- 范围：strategy 读取损坏 `positions.json` 时输出 rate-limited runtime event
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
- 完成内容：
  - 新增 `load_json_with_error`，保留默认回退值同时暴露读取错误元数据
  - `build_local_open_positions_for_risk` 在 `positions.json` 解析失败时写入 `runtime_state_degraded`
  - 事件字段覆盖 `state_key`、`state_file`、`error_type`、`error`、`fallback_used`、`consumer`
  - 相同降级路径通过 rate limit 合并重复事件
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k malformed_positions_json` → `1 passed, 56 deselected in 0.12s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "risk_guard or max_open_positions or malformed_positions_json"` → `3 passed, 54 deselected in 0.12s`
- 遗留风险：
  - 其他 `load_json(..., default)` 降级消费点还没统一接入同类事件
- 对应待办编号：
  - P0-3c

### 2026-05-09｜risk_state 降级路径事件化
- 状态：已完成
- 范围：`load_risk_state` 读取损坏 `risk_state.json` 时输出 rate-limited runtime event
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
- 完成内容：
  - `load_risk_state` 改为使用 `load_json_with_error`
  - `risk_state.json` 解析失败时写入 `runtime_state_degraded`
  - 事件字段覆盖 `state_key`、`state_file`、`error_type`、`error`、`fallback_used`、`consumer`
  - 同类重复事件通过 rate limit 合并
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k malformed_risk_state_json` → `1 passed, 57 deselected in 0.11s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "malformed_positions_json or malformed_risk_state_json or risk_guard or max_open_positions"` → `4 passed, 54 deselected in 0.11s`
- 遗留风险：
  - `positions` 以外的其他状态消费点还没统一接入降级事件
- 对应待办编号：
  - P0-3c follow-up

### 2026-05-09｜watcher 状态读取错误显式事件化
- 状态：已完成
- 范围：outer watcher 区分空状态与读取失败
- 修改文件：
  - `scripts/hermes_outer_watcher.py`
  - `tests/test_hermes_outer_watcher.py`
- 完成内容：
  - 为 `positions.json` / `events.jsonl` 读取失败增加结构化错误事件与 stderr 机器可读载荷
  - 新增 `EXIT_EVENTS_READ_ERROR = 4` 与 `EXIT_STATE_READ_ERROR = 5`
  - `malformed positions.json`、`malformed events.jsonl` 测试覆盖已补齐
- 验证：
  - `pytest -q tests/test_hermes_outer_watcher.py` → `10 passed in 0.06s`
- 遗留风险：
  - strategy 侧 runtime event 限流告警还未接入
- 对应待办编号：
  - P0-3a / P0-3b / P0-3d

### 2026-05-09｜watcher interrupt 退出语义补齐
- 状态：已完成
- 范围：outer watcher 中断事件化与退出码稳定化
- 修改文件：
  - `scripts/hermes_outer_watcher.py`
  - `tests/test_hermes_outer_watcher.py`
- 完成内容：
  - pre-entry 阶段捕获 `KeyboardInterrupt`
  - 输出 `watcher_interrupted` 结构化事件
  - stderr 输出 `status=interrupted` 机器可读结果
  - 返回 `EXIT_INTERRUPTED=130`
  - 新增回归测试覆盖 interrupt 路径
- 验证：
  - `pytest -q tests/test_hermes_outer_watcher.py` → `8 passed in 0.03s`
  - `pytest -q tests/test_cli_args.py tests/test_run_loop_smoke.py` → `54 passed in 22.24s`
- 遗留风险：
  - post-entry 阶段尚未对外部中断做显式事件化
  - 状态读取异常与 events 损坏仍未进入退出码分类
  - wait-limit 退出码仍保持成功语义，尚未区分 supervisor 控制性停止
- 对应待办编号：
  - P0-2c
  - P0-2e

### 2026-05-09｜watcher runner timeout 与缺失 runner 退出语义
- 状态：已完成
- 范围：outer watcher 超时控制、缺失 runner 快速失败、退出码语义增强
- 修改文件：
  - `scripts/hermes_outer_watcher.py`
  - `tests/test_hermes_outer_watcher.py`
- 完成内容：
  - 新增 `--runner-timeout-sec` 参数，支持为每次策略运行设置超时
  - `_run_once()` 透传 `subprocess.run(..., timeout=...)`
  - 子进程超时后输出 `watcher_runner_timeout` 结构化事件
  - 缺失 runner 时输出 `watcher_missing_runner` 事件并在启动前退出
  - 引入明确退出码常量：成功、缺失 runner、runner 超时、中断保留位
  - 新增回归测试覆盖参数解析、timeout 透传、timeout 退出路径、missing runner 路径
- 验证：
  - `pytest -q tests/test_hermes_outer_watcher.py` → `7 passed in 0.04s`
  - `pytest -q tests/test_cli_args.py tests/test_run_loop_smoke.py` → `54 passed in 26.49s`
- 遗留风险：
  - 非零 returncode 仍直接透传策略退出码，尚未做 watcher 级错误分类
  - interrupt 场景存在退出码常量，尚未接入显式捕获与测试
  - 状态读取异常事件化仍待 P0-3 落地
- 对应待办编号：
  - P0-2a
  - P0-2b
  - P0-2d

### 2026-05-09｜扫描摘要运行态健康信息补齐
- 状态：已完成
- 范围：中文扫描摘要、结构化 summary、运行态健康可观测性
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_scan_summary_mode_regression.py`
- 完成内容：
  - `build_cn_scan_summary()` 纳入 `book_ticker_websocket`、`user_data_stream_monitor`、`user_data_stream_alert`
  - `render_cn_scan_summary()` 输出运行监控文本行
  - `listen_key` 进入摘要前做脱敏
  - 新增回归测试 `test_build_cn_scan_summary_includes_runtime_health_sections`
- 验证：
  - `pytest -q tests/test_scan_summary_mode_regression.py tests/test_run_loop_smoke.py` → `47 passed`
  - `pytest -q` 通过
- 遗留风险：
  - 仅提升可观测性，未处理 runtime-state 并发写入问题
  - watcher 对文件损坏、超时、子进程卡死的容错仍待增强

---

## 待办任务总表

### P0｜稳定性与故障可诊断性

#### P0-1 RuntimeStateStore 原子写入
- 目标：消除 `write_text()` 直接覆盖带来的半写入/脏读风险
- 涉及文件：
  - `scripts/binance_futures_momentum_long.py`
  - 未来建议新建 `scripts/runtime_store.py`
- 子任务：
  - [x] P0-1a 提取统一原子写函数：临时文件 + rename
  - [x] P0-1b `save()` / `save_json()` 切换为原子写
  - [x] P0-1c `load_json('positions')` 去掉读时回写迁移副作用
  - [x] P0-1d 为 positions / last_cycle / risk_state / user_data_stream 增加损坏场景测试
  - [x] P0-1e `append_event()` 增加 flush/fsync 与 JSONL 尾部损坏容忍回归测试
- 验证：
  - `pytest -q tests/test_run_loop_smoke.py`
  - 新增 state-store 专项测试
- 完成标记：已完成

#### P0-2 watcher 增加 runner timeout 与严格退出语义
- 目标：让 outer watcher 成为真正 supervisor
- 涉及文件：
  - `scripts/hermes_outer_watcher.py`
  - `tests/test_hermes_outer_watcher.py`
- 子任务：
  - [x] P0-2a 增加 `--runner-timeout-sec`
  - [x] P0-2b 子进程超时后发结构化事件
  - [x] P0-2c 区分成功、超时、等待超限、状态读取异常的退出码
  - [x] P0-2d runner 路径启动前校验
  - [x] P0-2e 增加 timeout / missing runner / interrupt 测试
- 验证：
  - `pytest -q tests/test_hermes_outer_watcher.py`
- 完成标记：已完成

#### P0-3 watcher / strategy 状态读取错误显式事件化
- 目标：把“空状态”和“读取失败”区分开
- 涉及文件：
  - `scripts/hermes_outer_watcher.py`
  - `scripts/binance_futures_momentum_long.py`
- 子任务：
  - [x] P0-3a `load_json` / `_read_events` 返回错误语义
  - [x] P0-3b watcher 输出 `watcher_state_read_error` / `watcher_events_read_error`
  - [x] P0-3c 关键降级路径接入 rate-limited runtime event
  - [x] P0-3d 增加 malformed json / jsonl 测试
- 验证：
  - `pytest -q tests/test_hermes_outer_watcher.py tests/test_run_loop_smoke.py`
- 完成标记：已完成

#### P0-4 下单与保护逻辑边界测试补强
- 目标：压低真实交易回归风险
- 涉及文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `tests/test_run_loop_smoke.py`
- 子任务：
  - [x] P0-4a quantity / minQty / stepSize / rounding 边界测试
  - [x] P0-4b protection 缺失 / 修复 / 恢复场景测试
  - [x] P0-4c timeout 后订单确认与恢复路径补测
  - [x] P0-4d 校验 `place_live_trade` 的最小下单量逻辑
- 验证：
  - `pytest -q tests/test_strategy_v2.py tests/test_strategy_v2_restore_regression.py tests/test_run_loop_smoke.py`
  - 结果：`174 passed`
- 完成标记：已完成

---

### P1｜结构解耦与主脚本瘦身

#### P1-1 拆出 runtime_store 模块
- 目标：把 state 持久化从主脚本剥离
- 预计文件：
  - 新建 `scripts/runtime_store.py`
  - 调整 `scripts/binance_futures_momentum_long.py`
- 子任务：
  - [x] P1-1a 提取 `RuntimeStateStore`
  - [x] P1-1b 提取 event append/read helpers
  - [x] P1-1c 抽离 positions state migration / materialize
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py`
  - 结果：`70 passed in 0.51s`
- 完成标记：已完成

#### P1-2 拆出 summary_render 模块
- 目标：把摘要构建与渲染从主脚本剥离
- 预计文件：
  - 新建 `scripts/summary_render.py`
  - 调整测试 `tests/test_scan_summary_mode_regression.py`
- 子任务：
  - [x] P1-2a 提取 `build_cn_scan_summary`
  - [x] P1-2b 提取 `render_cn_scan_summary`
  - [x] P1-2c 固化运行态健康字段 schema
- 验证：
  - `pytest -q tests/test_scan_summary_mode_regression.py`
  - 结果：`6 passed in 0.10s`
- 完成标记：已完成

#### P1-3 拆出 execution 模块
- 目标：分离下单编排、订单确认、保护单布设
- 预计文件：
  - 新建 `scripts/execution.py`
  - 调整主脚本与恢复测试
- 子任务：
  - [ ] P1-3a 提取 `place_live_trade`
  - [ ] P1-3b 提取 quantity validation helper
  - [ ] P1-3c 提取 protection verification / repair helper
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py`
- 完成标记：未开始

#### P1-4 拆出 trade_monitor 模块
- 目标：把持仓监控线程逻辑与 runtime side effect 分层
- 预计文件：
  - 新建 `scripts/trade_monitor.py`
- 子任务：
  - [ ] P1-4a 提取 `monitor_live_trade`
  - [ ] P1-4b 明确 monitor input/output contract
  - [ ] P1-4c 降低线程对文件状态的直接耦合
- 验证：
  - `pytest -q tests/test_run_loop_smoke.py`
- 完成标记：未开始

#### P1-5 拆出 candidate_builder 与 risk_engine 模块
- 目标：降低策略选择逻辑和风险逻辑的耦合度
- 子任务：
  - [ ] P1-5a 拆 `build_candidate`
  - [ ] P1-5b 拆 `classify_candidate_state`
  - [ ] P1-5c 拆 `evaluate_risk_guards` / `evaluate_portfolio_risk_guards`
  - [ ] P1-5d 用 dataclass 收口宽参数接口
- 验证：
  - `pytest -q`
- 完成标记：未开始

---

### P2｜长期质量门禁

#### P2-1 引入静态质量检查
- 子任务：
  - [ ] P2-1a 选型 `ruff`
  - [ ] P2-1b 选型 `mypy` 或 `pyright`
  - [ ] P2-1c 建立最小可执行配置
- 完成标记：未开始

#### P2-2 建立状态机与 schema 文档
- 子任务：
  - [ ] P2-2a 文档化 positions status
  - [ ] P2-2b 文档化 runtime-state 文件结构
  - [ ] P2-2c 文档化 watcher 事件与退出码
- 完成标记：未开始

#### P2-3 建立重构完成定义
- 子任务：
  - [ ] P2-3a 单文件超过 3000 行进入拆分预警
  - [ ] P2-3b 单函数超过 120 行进入拆分预警
  - [ ] P2-3c 新增 `except Exception` 需要说明原因
- 完成标记：未开始

---

## 每次升级后的更新模板

复制下面模板，追加到“已完成升级记录”：

```md
### YYYY-MM-DD｜升级标题
- 状态：已完成
- 范围：
- 修改文件：
  - `path/to/file`
- 完成内容：
  - 
- 验证：
  - `pytest ...` → 
- 遗留风险：
  - 
- 对应待办编号：
  - P0-x / P1-x / P2-x
```

---

## 下一步建议
按顺序执行：`P1-2 拆出 summary_render 模块` → `P1-3 拆出 execution 模块` → `P1-4 拆出 trade_monitor 模块` → `P1-5 拆出 candidate_builder 与 risk_engine 模块`。
