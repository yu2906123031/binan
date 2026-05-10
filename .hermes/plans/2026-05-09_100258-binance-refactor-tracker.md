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
  - `pytest -q tests/test_strategy_v2.py` → `65 passed in 0.41s`
  - `pytest -q tests/test_strategy_v2.py tests/test_hermes_outer_watcher.py` → `75 passed in 0.46s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py` → `70 passed`
  - `pytest -q tests/test_execution_module_regression.py` → `6 passed in 0.12s`
  - `pytest -q tests/test_scan_summary_mode_regression.py` → `6 passed in 0.15s`
  - `pytest -q tests/test_cli_args.py tests/test_run_loop_smoke.py tests/test_scan_summary_mode_regression.py` → `60 passed in 1.04s`
  - `pytest -q tests/test_strategy_v2.py tests/test_cli_args.py tests/test_run_loop_smoke.py tests/test_scan_summary_mode_regression.py` → `125 passed in 1.16s`
  - `pytest -q tests/test_hermes_outer_watcher.py tests/test_cli_args.py tests/test_run_loop_smoke.py` → `57 passed`
  - `pytest -q tests/test_strategy_v2.py tests/test_hermes_outer_watcher.py tests/test_cli_args.py tests/test_run_loop_smoke.py tests/test_scan_summary_mode_regression.py` → `135 passed in 1.30s`
  - `pytest -q tests/test_risk_engine_unit.py` → `6 passed in 0.12s`
  - `pytest -q tests/test_risk_engine_unit.py tests/test_portfolio_risk_guards.py tests/test_strategy_v2.py tests/test_hermes_outer_watcher.py tests/test_cli_args.py tests/test_run_loop_smoke.py tests/test_scan_summary_mode_regression.py` → `146 passed in 1.34s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py tests/test_run_loop_smoke.py` → `111 passed`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `82 passed in 0.37s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py -k "build_candidate_wrapper or smart_money_veto or malformed_positions_json or malformed_risk_state_json or build_trade_management_plan_from_position or reconcile_runtime_state"` → `15 passed, 67 deselected in 0.19s`
  - `python -m pip check` 通过
  - `python -m compileall -q scripts main.py tests` 通过
- 当前结构性重点：
  - runtime-state 原子写入
  - watcher 可靠性增强
  - 主策略脚本拆模块
  - 风控 / 下单 / 持仓监控边界测试补强

---

## 已完成升级记录

### 2026-05-10｜下沉 build_local_open_positions_for_risk runtime-state helper
- 状态：已完成
- 范围：围绕 `build_local_open_positions_for_risk(...)` 的 positions 读取边界，抽出 runtime-state risk helper，并补充 wrapper / module parity 回归与 degraded event consumer 字段校验
- 修改文件：
  - `scripts/runtime_state_risk_helpers.py`
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 新增 `scripts/runtime_state_risk_helpers.py`，承载 `build_local_open_positions_from_state(...)`
  - 主脚本 `build_local_open_positions_for_risk(...)` 保留原入口，改为在 degraded event 发射后委托给新 helper
  - malformed `positions.json` 场景下，补齐 `consumer='build_local_open_positions_for_risk'` 事件字段
  - 新增 parity regression，直接比较主脚本 wrapper 与新 helper 在相同 positions state 输入下的输出一致性
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "build_local_open_positions_for_risk_emits_rate_limited_event_on_malformed_positions_json or build_local_open_positions_for_risk_matches_runtime_state_helper"` → RED：`2 failed`，错误分别为缺少 `consumer` 字段与 `FileNotFoundError: /root/binan/scripts/runtime_state_risk_helpers.py`
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "build_local_open_positions_for_risk_emits_rate_limited_event_on_malformed_positions_json or build_local_open_positions_for_risk_matches_runtime_state_helper"` → `2 passed, 73 deselected in 0.14s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py tests/test_execution_module_regression.py` → `87 passed in 0.36s`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py scripts/risk_state_helpers.py scripts/runtime_state_risk_helpers.py tests/test_strategy_v2_restore_regression.py tests/test_execution_module_regression.py` → 通过
- 遗留风险：
  - `build_local_open_positions_for_risk(...)` 的 store 级 degraded event 节流仍留在主脚本，下一步适合继续把 positions read + event envelope 归并成更完整的 runtime-state consumer helper
  - 新增 `runtime_state_risk_helpers.py` 当前尚未纳入项目级 mypy 文件列表
- 对应待办编号：
  - P1-5g

### 2026-05-10｜下沉 risk_state helper 模块并补 parity 回归
- 状态：已完成
- 范围：把 `normalize_loaded_risk_state(...)` 与 `refresh_risk_state_heat_snapshot(...)` 从主脚本下沉到独立模块，同时保留主脚本 wrapper 契约并新增 module parity 回归
- 修改文件：
  - `scripts/risk_state_helpers.py`
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 新增 `scripts/risk_state_helpers.py`，承载 `normalize_loaded_risk_state(...)` 与 `refresh_risk_state_heat_snapshot(...)` 的抽取实现
  - 主脚本保留同名 wrapper，并通过依赖注入把 `default_risk_state` 与 `compute_positions_heat_snapshot` 回接到新模块，维持既有调用点与测试入口稳定
  - 在 `tests/test_strategy_v2_restore_regression.py` 新增两条 parity regression，直接校验主脚本 wrapper 与抽取模块在相同输入下输出一致
  - 继续保留 `load_risk_state(...)` 的 runtime-state degraded event 行为与已存在 heat snapshot 回归覆盖
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "normalize_loaded_risk_state_matches_risk_state_module or refresh_risk_state_heat_snapshot_matches_risk_state_module"` → RED：`2 failed`，报错 `FileNotFoundError: /root/binan/scripts/risk_state_helpers.py`
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "normalize_loaded_risk_state_matches_risk_state_module or refresh_risk_state_heat_snapshot_matches_risk_state_module or load_risk_state_merges_defaults_and_refreshes_heat_snapshot or load_risk_state_preserves_existing_heat_when_snapshot_has_no_open_positions or malformed_risk_state_json"` → `5 passed, 69 deselected in 0.14s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `86 passed in 0.38s`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py scripts/risk_state_helpers.py tests/test_strategy_v2_restore_regression.py` → 通过
- 遗留风险：
  - `load_risk_state(...)` 与 `build_local_open_positions_for_risk(...)` 仍共享 `positions` 读取边界，下一步适合继续抽离 runtime-state risk helper
  - 新增 `risk_state_helpers.py` 当前尚未纳入项目级 mypy 文件列表，后续可与下一批 helper 一起扩到静态检查基线
- 对应待办编号：
  - P1-5f

### 2026-05-10｜拆分 load_risk_state 归一化与 heat snapshot helper
- 状态：已完成
- 范围：把 `load_risk_state` 内部的风险状态归一化与持仓热度刷新逻辑拆成独立 helper，并补齐该 seam 的专项回归测试
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 新增 `normalize_loaded_risk_state(...)`，集中处理 `risk_state.json` 读取后的默认字段合并与 dict 字段修复
  - 新增 `refresh_risk_state_heat_snapshot(...)`，集中处理 `positions` 热度快照回填，固定有持仓时更新 `portfolio_heat_open_r`、`portfolio_heat_r_by_theme`、`portfolio_heat_r_by_correlation`
  - `load_risk_state(...)` 改为保留降级事件逻辑，同时串联两个 helper，缩短后续抽离风险状态相关 helper 的改动面
  - 新增两条 focused regression：一条锁定默认值合并 + 热度快照刷新，一条锁定无 open position 时保留已存 heat 字段
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "malformed_risk_state_json or load_risk_state_merges_defaults_and_refreshes_heat_snapshot or load_risk_state_preserves_existing_heat_when_snapshot_has_no_open_positions"` → `3 passed, 69 deselected in 0.14s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py tests/test_execution_module_regression.py` → `84 passed in 0.36s`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` → 通过
- 遗留风险：
  - `normalize_loaded_risk_state(...)` 与 `refresh_risk_state_heat_snapshot(...)` 仍定义在主脚本，下一步适合继续把 risk-state helper 下沉到独立模块并补直连单测文件
  - `build_local_open_positions_for_risk(...)` 与 `load_risk_state(...)` 仍共享 `positions` 读取语义，后续可继续收口成更稳定的 runtime-state risk helper 边界
- 对应待办编号：
  - P1-5e

### 2026-05-10｜新增 risk_engine 直连单测并缩短风控回归链路
- 状态：已完成
- 范围：为 `scripts/risk_engine.py` 新增独立单测文件，直接覆盖流动性分级、动态阈值、主题热度与仓位翻转冷却等核心风控 guard
- 修改文件：
  - `tests/test_risk_engine_unit.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 新增 `tests/test_risk_engine_unit.py`，通过独立动态加载 `risk_engine.py` 直接校验 helper 与 guard 输出 contract
  - 覆盖 `compute_dynamic_risk_thresholds(...)` 的高波动 / 高量能放宽逻辑，固定返回 `max_slippage_pct`、`max_open_interest_delta_pct`、`max_heat_score` 的联动提升结果
  - 覆盖 `evaluate_risk_guards(...)` 的两类直连阻断路径：执行流动性过差触发 `candidate_execution_liquidity_poor`，同主题热度超限触发 `candidate_same_theme_heat_overexposure`
  - 覆盖 `evaluate_portfolio_risk_guards(...)` 的组合约束路径：空头持仓数上限触发 `max_short_positions_reached`，单向持仓保护打开时记录 `opposite_side_flip_cooldown_active`
  - 补充 `build_position_exposure_snapshot(...)` 与 `normalize_position_side(...)` 的直连断言，缩短后续 `risk_engine` 行为回归与类型收口的验证链路
- 验证：
  - `python -m pytest -q tests/test_risk_engine_unit.py` → `6 passed in 0.12s`
  - `python -m pytest -q tests/test_risk_engine_unit.py tests/test_portfolio_risk_guards.py tests/test_strategy_v2.py tests/test_hermes_outer_watcher.py tests/test_cli_args.py tests/test_run_loop_smoke.py tests/test_scan_summary_mode_regression.py` → `146 passed in 1.34s`
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 13 source files`
  - `python -m ruff check /root/binan` → `All checks passed!`
- 遗留风险：
  - `scripts/binance_futures_momentum_long.py` 中仍有一部分风控组合路径通过主脚本集成回归间接覆盖，后续适合继续抽离更细 helper 后补对应直连单测
  - `risk_engine.py` 当前已纳入 `[tool.mypy].files`，配套测试文件当前以 `tests/test_portfolio_risk_guards.py` 与本次新增文件为主，后续可继续吸收更多主脚本风控场景
- 对应待办编号：
  - P1-2d
  - P2-1c

### 2026-05-10｜扩展项目级 mypy 到 risk_engine 与 hermes_outer_watcher 基线
- 状态：已完成
- 范围：把 `scripts/risk_engine.py`、`scripts/hermes_outer_watcher.py` 与 `tests/test_hermes_outer_watcher.py` 纳入项目级 mypy 最小配置，并补齐 watcher 子进程调用与动态加载测试的类型收口
- 修改文件：
  - `pyproject.toml`
  - `scripts/hermes_outer_watcher.py`
  - `tests/test_hermes_outer_watcher.py`
- 完成内容：
  - 将 `scripts/risk_engine.py`、`scripts/hermes_outer_watcher.py`、`tests/test_hermes_outer_watcher.py` 新增到 `[tool.mypy].files`
  - 为 `tests/test_hermes_outer_watcher.py` 增加 `assert spec is not None` 与 `assert spec.loader is not None`，让 `module_from_spec(spec)`、`spec.name`、`spec.loader.exec_module(...)` 满足 importlib 动态加载的可空收口要求
  - 为 `scripts/hermes_outer_watcher.py` 的 `_run_once(...)` 显式标注 `run_kwargs: Dict[str, Any]`，并把 `subprocess.run(...)` 结果收口为 `CompletedProcess[str]`，稳定 `capture_output/text/timeout` 组合下的类型推断
  - `scripts/risk_engine.py` 已在现有实现下直接通过 mypy，无需额外改动
- 验证：
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 13 source files`
  - `python -m pytest -q tests/test_hermes_outer_watcher.py` → `10 passed in 0.08s`
  - `python -m pytest -q tests/test_strategy_v2.py tests/test_hermes_outer_watcher.py tests/test_cli_args.py tests/test_run_loop_smoke.py tests/test_scan_summary_mode_regression.py` → `135 passed in 1.30s`
  - `python -m ruff check /root/binan` → `All checks passed!`
- 遗留风险：
  - `scripts/binance_futures_momentum_long.py` 主体仍未纳入项目级 mypy，后续更适合沿已提取 helper 与新增专项测试继续渐进收口
  - 风控逻辑当前更多通过主脚本回归间接覆盖，后续可考虑补 `risk_engine` 直连单测，缩短类型与行为回归链路
- 对应待办编号：
  - P2-1c

### 2026-05-10｜扩展项目级 mypy 到 strategy_v2 与 summary_render 基线
- 状态：已完成
- 范围：把 `tests/test_strategy_v2.py` 与 `scripts/summary_render.py` 纳入项目级 mypy 最小配置，并补齐动态加载与容器变量的最小类型声明
- 修改文件：
  - `pyproject.toml`
  - `tests/test_strategy_v2.py`
  - `scripts/summary_render.py`
- 完成内容：
  - 将 `tests/test_strategy_v2.py` 与 `scripts/summary_render.py` 新增到 `[tool.mypy].files`
  - 为 `tests/test_strategy_v2.py` 增加 `assert spec is not None`，让 `module_from_spec(spec)`、`spec.name`、`spec.loader.exec_module(...)` 满足 importlib 动态加载的可空收口要求
  - 为 `scripts/summary_render.py` 的 `runtime_summary` 补充 `Dict[str, Dict[str, Any]]` 注解，稳定运行监控摘要构造的容器类型
  - 保持 `strategy_v2` 专项回归对主脚本 sibling import 行为的真实覆盖，同时将 `summary_render` 正式纳入项目级静态检查基线
- 验证：
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 10 source files`
  - `python -m pytest -q tests/test_strategy_v2.py` → `65 passed in 0.41s`
  - `python -m pytest -q tests/test_strategy_v2.py tests/test_cli_args.py tests/test_run_loop_smoke.py tests/test_scan_summary_mode_regression.py` → `125 passed in 1.16s`
  - `python -m ruff check /root/binan` → `All checks passed!`
- 遗留风险：
  - `scripts/risk_engine.py`、`scripts/hermes_outer_watcher.py` 仍未纳入 `[tool.mypy].files`
  - `scripts/binance_futures_momentum_long.py` 主体体量仍大，直接纳入 mypy 前更适合继续沿子模块边界渐进收口
- 对应待办编号：
  - P2-1c

### 2026-05-10｜扩展项目级 mypy 到动态加载主脚本的专项测试
- 状态：已完成
- 范围：把 `tests/test_cli_args.py`、`tests/test_run_loop_smoke.py`、`tests/test_scan_summary_mode_regression.py` 纳入项目级 mypy 最小配置，并补齐 `test_scan_summary_mode_regression.py` 的 importlib 动态加载前置断言
- 修改文件：
  - `pyproject.toml`
  - `tests/test_scan_summary_mode_regression.py`
- 完成内容：
  - 将 3 个动态加载主脚本的专项测试加入 `[tool.mypy].files`
  - 复核 `tests/test_cli_args.py` 与 `tests/test_run_loop_smoke.py`，确认二者已具备 `SCRIPTS_DIR` 注入 `sys.path` 的前置条件
  - 为 `tests/test_scan_summary_mode_regression.py` 新增 `assert spec is not None` 与 `assert summary_render_spec is not None`，消除 `module_from_spec(...)` 与 `spec.name/spec.loader` 的可空类型报错
  - 维持动态加载测试对主脚本 sibling import 行为的真实覆盖，同时让项目级 mypy 基线可直接覆盖到这组专项回归
- 验证：
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 8 source files`
  - `python -m pytest -q tests/test_scan_summary_mode_regression.py` → `6 passed in 0.15s`
  - `python -m pytest -q tests/test_cli_args.py tests/test_run_loop_smoke.py tests/test_scan_summary_mode_regression.py` → `60 passed in 1.04s`
  - `python -m ruff check /root/binan` → `All checks passed!`
- 遗留风险：
  - `tests/test_strategy_v2.py` 已单独修过加载前置条件，目前尚未加入 `[tool.mypy].files`
  - 更多 `scripts/*.py` 仍在项目级 mypy 覆盖范围之外，下一阶段适合扩到 `summary_render.py`、`risk_engine.py`、`hermes_outer_watcher.py`
- 对应待办编号：
  - P2-1c

### 2026-05-10｜P2-1c 建立项目级 mypy 最小配置并补齐 importlib 测试加载基线
- 状态：已完成
- 范围：把最小 mypy 配置正式写入 `pyproject.toml`，并修正 `tests/test_strategy_v2.py` 的脚本动态加载前置条件，使主脚本提取模块后的导入路径在专项回归中保持稳定
- 修改文件：
  - `pyproject.toml`
  - `tests/test_strategy_v2.py`
- 完成内容：
  - 在 `pyproject.toml` 新增 `[tool.mypy]`，锁定 Python 3.11，并把当前最小静态检查范围收敛为 3 个核心模块加 2 个关键回归测试文件
  - 将 `.venv-typecheck` 纳入 Ruff 排除目录，避免本地类型检查环境干扰仓库扫描
  - 为 `tests/test_strategy_v2.py` 增加 `SCRIPTS_DIR` 注入 `sys.path` 的加载前置步骤，使 `binance_futures_momentum_long.py` 在导入 `execution_engine` 时具备与既有回归测试一致的 sibling import 解析条件
  - 验证项目级 mypy 配置可直接通过 `.venv-typecheck/bin/python -m mypy` 执行，无需再显式传文件列表
- 验证：
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 5 source files`
  - `python -m ruff check /root/binan` → `All checks passed!`
  - `python -m pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `82 passed in 0.37s`
  - `python -m pytest -q tests/test_strategy_v2.py tests/test_hermes_outer_watcher.py` → `75 passed in 0.46s`
- 遗留风险：
  - 当前 `[tool.mypy]` 仍聚焦最小高价值文件集，`scripts/binance_futures_momentum_long.py` 与其他脚本尚未纳入项目级类型检查范围
  - 其他通过 `importlib.util.spec_from_file_location(...)` 动态加载主脚本的测试文件，建议统一复核是否都显式注入了 `SCRIPTS_DIR`
- 对应待办编号：
  - P2-1c

### 2026-05-10｜P2-1b 选型 mypy 并补齐最小静态类型检查基线
- 状态：已完成
- 范围：为已提取的核心模块与关键回归测试建立最小 mypy 检查基线，完成 `P2-1b` 选型并验证与现有回归兼容
- 修改文件：
  - `scripts/runtime_store.py`
  - `scripts/execution_engine.py`
  - `tests/test_execution_module_regression.py`
  - `tests/test_strategy_v2_restore_regression.py`
- 完成内容：
  - 选型 `mypy` 作为类型检查器，基于项目现有 Python 3.11 代码与 dataclass / typed helper 风格推进最小落地
  - 为 `tests/test_execution_module_regression.py` 与 `tests/test_strategy_v2_restore_regression.py` 的 `importlib.util.spec_from_file_location(...)` 加入 `spec is not None` 与 `loader is not None` 断言，固化动态加载前置条件
  - 修正 `scripts/runtime_store.py` 中 `trade_management_plan` 归一化分支的可空映射收口
  - 修正 `scripts/execution_engine.py` 中 `trade_management_plan` 深拷贝与 `protection_check` 读取的可空类型路径，消除最小 mypy 基线报错
- 验证：
  - `python -m ruff check /root/binan` → `All checks passed!`
  - `.venv-typecheck/bin/python -m mypy scripts/candidate_builder.py scripts/execution_engine.py scripts/runtime_store.py tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `Success: no issues found in 5 source files`
  - `python -m pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `82 passed in 0.64s`
- 遗留风险：
  - 当前 mypy 基线覆盖范围聚焦已提取核心模块与关键回归测试，尚未扩展到整个 `scripts/` 与 `tests/`
  - 项目级 mypy 配置尚未写入 `pyproject.toml`，当前基线依赖显式目标文件列表执行
- 对应待办编号：
  - P2-1b

### 2026-05-09｜candidate_builder 提取 build_candidate 并保留主脚本旧调用契约
- 状态：已完成
- 范围：把 `build_candidate` 主体迁移到 `scripts/candidate_builder.py`，同时让 `scripts/binance_futures_momentum_long.py` 保持旧 keyword contract 与新抽取接口兼容
- 修改文件：
  - `scripts/candidate_builder.py`
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_execution_module_regression.py`
- 完成内容：
  - 新增 wrapper 回归 `test_build_candidate_wrapper_accepts_legacy_keyword_contract`，先验证旧参数集调用主脚本 wrapper 的签名兼容性，再固化兼容回接结果
  - 主脚本 `build_candidate` wrapper 接受旧参数集合与新抽取参数集合，并把 legacy `short_bias / oi_now / oi_5m_ago / oi_15m_ago / cvd_delta / cvd_zscore` 收口到 `microstructure_inputs`
  - wrapper 把 legacy `okx_sentiment_score / okx_sentiment_acceleration / sector_resonance_score / smart_money_flow_score` 归一化成抽取模块消费的 `okx_sentiment` 与 `smart_money_context`
  - wrapper 继续把主脚本依赖注入 `candidate_builder.build_candidate`，保持 extracted module 不直接反向依赖 monolith
  - `tests/test_execution_module_regression.py` 已覆盖 wrapper 与抽取模块的一致性，以及旧 keyword contract 的兼容转发
- 验证：
  - `pytest -q tests/test_execution_module_regression.py::test_build_candidate_wrapper_accepts_legacy_keyword_contract -vv` → RED 失败：`unexpected keyword argument 'risk_usdt'`
  - `pytest -q tests/test_execution_module_regression.py::test_build_candidate_wrapper_accepts_legacy_keyword_contract tests/test_execution_module_regression.py::test_build_candidate_wrapper_matches_extracted_module -vv` → `2 passed in 0.15s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py -k "build_candidate_wrapper or smart_money_veto or malformed_positions_json or malformed_risk_state_json or build_trade_management_plan_from_position or reconcile_runtime_state"` → `15 passed, 67 deselected in 0.19s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `82 passed in 0.36s`
- 遗留风险：
  - `load_risk_state`、`build_local_open_positions_for_risk`、`build_trade_management_plan_from_position`、`reconcile_runtime_state` 仍留在主脚本，下一阶段继续对应 P1-5b / P1-5c 与后续 domain 拆分
  - `build_candidate` 仍依赖较宽参数面，`dataclass` 收口工作仍在 P1-5d
- 对应待办编号：
  - P1-5a

### 2026-05-09｜execution 模块降低 monitor 对 positions 文件状态的直接耦合
- 状态：已完成
- 范围：为 `monitor_live_trade` 增加可注入的初始持仓状态输入，降低线程对 `positions.json` 读取的硬耦合，同时保持现有 runtime side effect 行为稳定
- 修改文件：
  - `scripts/execution_engine.py`
  - `tests/test_execution_module_regression.py`
- 完成内容：
  - `monitor_live_trade` 新增 `initial_positions_state` 注入入口，允许调用方直接提供监控起始持仓快照
  - 初始状态解析优先消费注入快照，未注入时继续回退到 `store.load_json('positions', {})`
  - 新增 `position_state_source` debug 字段，显式标记本轮监控状态来源为 `injected` 或 `store`
  - 修正 restart 场景 stop 恢复优先级，恢复时优先采用 `current_stop_price`，保持 trailing / breakeven 后的止损位连续性
  - 新增回归测试覆盖 injected state 路径，验证可在不依赖持久化 positions 文件结果的情况下完成监控执行且不污染输入快照
- 验证：
  - `pytest -q tests/test_execution_module_regression.py -k injected_position_state` → `1 failed`，确认 RED
  - `pytest -q tests/test_execution_module_regression.py -k "injected_position_state or matches_script_monitor_live_trade"` → `2 passed, 7 deselected in 0.15s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py -k "monitor_live_trade or start_trade_monitor_thread or injected_position_state or reconcile_runtime_state or sync_tracked_positions_with_exchange"` → `19 passed, 60 deselected in 0.21s`
- 遗留风险：
  - monitor loop 内部每轮仍会从 store 重新读取 positions，用于吸收外部状态更新；下一步可继续把 loop state 刷新策略下沉为显式 provider
  - `start_trade_monitor_thread` 仍以 `store` 作为唯一线程入参，后续适合把 injected state 从启动入口继续向上传递
- 对应待办编号：
  - P1-4c

### 2026-05-09｜execution 模块提取 protection helper 并回接主脚本
- 状态：已完成
- 范围：把 protection verification / repair helper 从主策略脚本抽到 `execution_engine.py`，并保持主脚本兼容入口
- 修改文件：
  - `scripts/execution_engine.py`
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_execution_module_regression.py`
- 完成内容：
  - 在 `scripts/execution_engine.py` 新增 `resolve_position_protection_status` 与 `repair_missing_protection`
  - 为 protection verification 增补 open algo order 精确匹配字段与回归断言，固化 `matched_via`、triggerPrice、quantity 输出 contract
  - 主脚本改为通过 `execution_*` 包装函数回接 protection helper，保留原函数名与调用点稳定
  - execution 回归测试新增 script/extracted 一致性覆盖，验证 protection helper 抽取后行为保持一致
- 验证：
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py -k "resolve_position_protection_status or repair_missing_protection or reconcile_runtime_state or sync_tracked_positions_with_exchange"` → `15 passed, 63 deselected in 0.18s`
- 遗留风险：
  - `reconcile_runtime_state` 仍留在主脚本，protection repair orchestration 与 runtime store 更新逻辑还未继续下沉
  - `allow_missing_when_flat` 当前为兼容参数，后续适合与 protection status contract 一并清理
- 对应待办编号：
  - P1-3c

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
  - 新建 `scripts/execution_engine.py`
  - 调整主脚本与执行回归测试
- 子任务：
  - [x] P1-3a 提取 `place_live_trade`
  - [x] P1-3b 提取 `ensure_symbol_margin_type`
  - [x] P1-3c 提取 protection verification / repair helper
- 验证：
  - `pytest -q tests/test_execution_module_regression.py`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py -k "resolve_position_protection_status or repair_missing_protection or reconcile_runtime_state or sync_tracked_positions_with_exchange"`
  - 结果：`15 passed, 63 deselected in 0.18s`
- 完成标记：已完成

#### P1-4 拆出 trade_monitor 模块
- 目标：把持仓监控线程逻辑与 runtime side effect 分层
- 预计文件：
  - 延续收敛到 `scripts/execution_engine.py`，待线程启动入口与 contract 稳定后再独立为 `scripts/trade_monitor.py`
- 子任务：
  - [x] P1-4a 提取 `monitor_live_trade`
  - [x] P1-4b 明确 monitor input/output contract
  - [x] P1-4c 降低线程对文件状态的直接耦合
- 验证：
  - `pytest -q tests/test_execution_module_regression.py`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py -k "monitor_live_trade or start_trade_monitor_thread or injected_position_state or reconcile_runtime_state or sync_tracked_positions_with_exchange"`
  - 结果：`19 passed, 60 deselected in 0.21s`
- 完成标记：已完成

#### P1-5 拆出 candidate_builder 与 risk_engine 模块
- 目标：降低策略选择逻辑和风险逻辑的耦合度
- 子任务：
  - [x] P1-5a 拆 `build_candidate`
  - [x] P1-5b 拆 `classify_candidate_state`
  - [x] P1-5c 拆 `evaluate_risk_guards` / `evaluate_portfolio_risk_guards`
  - [x] P1-5d 用 dataclass 收口宽参数接口
  - [x] P1-5e 拆 `load_risk_state` 内部归一化 / heat snapshot helper
  - [x] P1-5f 下沉 `risk_state_helpers.py` 并补主脚本 / 模块 parity 回归
  - [x] P1-5g 下沉 `runtime_state_risk_helpers.py` 并补 `build_local_open_positions_for_risk(...)` parity / degraded event 回归
- 验证：
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py -k "build_candidate_wrapper or smart_money_veto or malformed_positions_json or malformed_risk_state_json or build_trade_management_plan_from_position or reconcile_runtime_state"`
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "load_risk_state_merges_defaults_and_refreshes_heat_snapshot or load_risk_state_preserves_existing_heat_when_snapshot_has_no_open_positions"`
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "normalize_loaded_risk_state_matches_risk_state_module or refresh_risk_state_heat_snapshot_matches_risk_state_module"`
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "build_local_open_positions_for_risk_emits_rate_limited_event_on_malformed_positions_json or build_local_open_positions_for_risk_matches_runtime_state_helper"`
- 完成标记：已完成

---

### P2｜长期质量门禁

#### P2-1 引入静态质量检查
- 子任务：
  - [x] P2-1a 选型 `ruff`
  - [x] P2-1b 选型 `mypy`
  - [x] P2-1c 建立最小可执行配置
- 完成标记：已完成

#### P2-2 建立状态机与 schema 文档
- 子任务：
  - [x] P2-2a 文档化 positions status
  - [x] P2-2b 文档化 runtime-state 文件结构
  - [x] P2-2c 文档化 watcher 事件与退出码
- 完成标记：已完成

#### P2-3 建立重构完成定义
- 子任务：
  - [x] P2-3a 单文件超过 3000 行进入拆分预警
  - [x] P2-3b 单函数超过 120 行进入拆分预警
  - [x] P2-3c 新增 `except Exception` 需要说明原因
- 完成标记：已完成

#### P2 当前产出
- `docs/runtime_state_machine_and_schema.md`
- `docs/refactor-done-definition-and-gates.md`
- `docs/architecture.md` 已补充运行态契约与门禁文档引用

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
按顺序执行：继续把 `build_local_open_positions_for_risk(...)` 的 store 级 degraded event 节流与 positions 读取 envelope 一起下沉到更完整的 runtime-state consumer helper → 把 `risk_state_helpers.py` 与 `runtime_state_risk_helpers.py` 连同对应回归一起纳入项目级 mypy 覆盖范围 → 继续缩短 runtime-state risk 路径对主脚本集成回归的依赖，补独立 helper 直连测试
