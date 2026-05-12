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
  - `pytest -q tests/test_strategy_v2.py` → `69 passed in 0.52s`
  - `pytest -q tests/test_strategy_v2.py tests/test_hermes_outer_watcher.py` → `75 passed in 0.46s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py` → `123 passed in 0.81s`
  - `pytest -q tests/test_execution_module_regression.py` → `14 passed in 0.12s`
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

### 2026-05-11｜补齐 OKX simulated 对 absolute profit targets 的契约回归
- 状态：已完成
- 范围：核对 OKX simulated 持仓管理路径对 `tp1_profit_usdt` / `tp2_profit_usdt` 的支持现状，补回归测试固定“无 persisted plan 时走 args，存在 persisted plan 时优先沿用持久化 plan”的 contract
- 修改文件：
  - `tests/test_strategy_v2.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 代码现状确认：`manage_okx_simulated_positions(...)` 已经通过 `build_trade_management_plan_from_position(...)` 进入 absolute profit targets 路径；当 `trade_management_plan` 已持久化时直接回放 plan payload，当 plan 缺失时回退到 `args.tp1_profit_usdt` / `args.tp2_profit_usdt` 重新构建
  - 新增 `test_manage_okx_simulated_positions_uses_args_absolute_profit_targets_when_plan_missing`，固定无 persisted plan 时，OKX simulated 会按 args 里的绝对利润目标在 `102.5` 触发 TP1、减仓 `1.0`
  - 将已有 OKX simulated 回归收窄为 persisted-plan 优先语义，重命名为 `test_manage_okx_simulated_positions_uses_persisted_absolute_profit_targets_over_args`，固定 runtime-state 已持久化 absolute profit targets 时优先使用 plan 内数值
  - 结论：本轮关闭的是 contract 缺口，生产实现已具备 OKX simulated absolute profit targets 透传能力，本次无需新增生产代码
- 验证：
  - `pytest -q /root/binan/tests/test_strategy_v2.py -k 'manage_okx_simulated_positions and absolute_profit_targets'` → `2 passed, 68 deselected in 0.36s`
  - `pytest -q /root/binan/tests/test_strategy_v2.py` → `69 passed in 0.52s`
- 遗留风险：
  - 当前 OKX simulated 路径仍通过 `place_okx_reduce_only_market(...)` 直接按市价减仓，后续若要把 absolute profit targets 扩展到更细的成交归因或通知 payload，适合继续补 event payload 中的目标利润字段
  - 目前 focused 回归覆盖 LONG 仓位，后续适合补一条 SHORT 仓位 absolute profit targets 的对称验证，锁定方向换算 contract
- 对应待办编号：
  - t4
  - t5
  - t6

### 2026-05-11｜两段止盈支持绝对利润目标，第二段按剩余仓位全平
- 状态：已完成
- 范围：为 Binance Futures 动量策略增加两段止盈下单能力，支持 `tp1_profit_usdt` / `tp2_profit_usdt` 绝对利润目标；确认第二段止盈语义为“在整笔仓位累计浮盈约 10 USDT 时，把剩余仓位全部平掉”
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `scripts/execution_engine.py`
  - `tests/test_strategy_v2.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 扩展 `TradeManagementPlan`，加入 `tp1_profit_usdt` / `tp2_profit_usdt` 字段，保留 `tp1_trigger_price` / `tp2_trigger_price`、`tp1_close_qty` / `tp2_close_qty`、`runner_qty` 运行态契约
  - 更新 `build_trade_management_plan(...)`，支持绝对利润目标换算触发价：当传入 `tp1_profit_usdt` / `tp2_profit_usdt` 时，按整笔当前持仓 `quantity` 计算达到对应累计浮盈所需的价格
  - 更新 live execution 下单链路，开仓成功后同时挂：止损保护单、第一段止盈单、第二段止盈单，并把 `tp1_order` / `tp2_order` 回写到交易结果 payload
  - 明确第二段语义：`tp2_close_qty` 按剩余仓位数量下单，实现“10刀左右剩余仓位全平”
  - 补 focused regression，固定绝对利润目标与两段止盈下单 contract
- 验证：
  - `pytest -q tests/test_strategy_v2.py -k 'build_trade_management_plan_supports_absolute_profit_targets or place_live_trade_places_stop_and_two_take_profit_orders or place_live_trade_scales_quantity_down_to_zero_and_skips_entry_order'` → `3 passed`
  - `pytest -q tests/test_strategy_v2.py tests/test_strategy_v2_restore_regression.py -k 'trade_management_plan or take_profit or place_live_trade_places_stop_and_two_take_profit_orders or build_trade_management_plan_supports_absolute_profit_targets'` → `7 passed, 181 deselected in 0.20s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'profit_targets or absolute_profit_targets or preserves_short_side_from_position_side'` → `3 passed, 120 deselected in 0.17s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py` → `123 passed in 0.81s`
- 遗留风险：
  - OKX simulated 下单路径当前仍沿用原先 `tp1_r` / `tp2_r` 入口，后续适合补齐与 Binance live 路径一致的绝对利润参数透传
  - runtime-state 恢复链路当前已兼容 plan 字段持久化，后续适合继续补一条恢复后识别 `tp1_profit_usdt` / `tp2_profit_usdt` 的专门回归，固定跨重启 contract
- 对应待办编号：
  - t2

### 2026-05-11｜修复 Binance Multi-Assets 模式导致的 isolated preflight 卡死
- 状态：已完成
- 范围：定位 `binance_execution_preflight_failed` 的真实根因，并让 live execution 在 Binance Futures Multi-Assets 账户下从 isolated 申请自动降级到 crossed，解除持续无法下单的 preflight 卡点
- 修改文件：
  - `scripts/execution_engine.py`
  - `tests/test_execution_module_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 从 `/root/.hermes/binance-futures-momentum-long/runtime-state/events.jsonl` 抽取出持续重复的 `binance_execution_preflight_failed` 明细，根因稳定指向 Binance API `-4168`：`Unable to adjust to isolated-margin mode under the Multi-Assets mode.`
  - 扩展 `ensure_symbol_margin_type(...)`，当请求 `ISOLATED` 且交易所返回 `-4168` / `Multi-Assets mode` 时，返回可继续执行的降级结果，记录 `actual='CROSSED'`、`multi_assets_mode=True`、`fallback_reason='binance_multi_assets_mode_blocks_isolated'`
  - 保持原有 `-4046` already-set 路径契约，同时让后续 leverage 设置与下单流程继续执行，避免 preflight 在 marginType 切换阶段直接失败
  - 新增 focused regression，固定 Multi-Assets 模式下的 crossed fallback contract，防止后续重构把该 live 交易兼容性回归掉
- 验证：
  - `pytest -q /root/binan/tests/test_execution_module_regression.py -q` → `14 passed`
  - `python -m compileall -q scripts/execution_engine.py tests/test_execution_module_regression.py` → 通过
- 遗留风险：
  - CLI 默认 `--margin-type` 仍是 `ISOLATED`，当前修复通过 runtime fallback 兼容 Multi-Assets 账户；后续可按账户画像把默认值或 profile 配置显式收敛到 `CROSSED`
  - 当前 last_cycle 快照里没有收口 margin fallback 统计；后续适合把 `multi_assets_mode` / `fallback_reason` 汇入 cycle summary，便于长期巡检
- 对应待办编号：
  - P0-runtime-live-preflight-multi-assets

### 2026-05-10｜补 runtime_state_risk_helpers 直连 contract 单测
- 状态：已完成
- 范围：为 `runtime_state_risk_helpers.py` 新增直连单测，固定 malformed JSON、degraded-event envelope、default / heat snapshot 合并与 helper 对主脚本 wrapper 的契约边界

### 2026-05-10｜下沉 auto-loop user-data-stream monitor orchestration helper
- 状态：已完成
- 范围：把 `run_loop(...)` 里 auto-loop 分支的 user-data-stream monitor orchestration 抽到独立 helper，收窄主脚本 monitor orchestration 面积，并为 skipped / existing listen key / empty state 三条路径补 focused regression
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 新增 `run_auto_loop_user_data_stream_monitor(...)`，统一承载 OKX simulated 下的 skipped payload、existing listen key 下的 `run_user_data_stream_monitor_cycle(...)` 调度、positions 写回、alert 发射
  - `run_loop(...)` 的 auto-loop 分支改为只消费 helper 返回的 `{monitor, alert}`，减少局部状态分支和重复依赖注入
  - 新增三条 focused regression，覆盖 `OKX_SIMULATED` skipped payload、existing listen key 刷新失败告警链路、无 listen key 时的空返回 contract
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'run_auto_loop_user_data_stream_monitor or max_open_positions_blocks_trade'` → `4 passed, 77 deselected in 0.43s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `94 passed in 0.40s`
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 15 source files`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` → 通过
- 遗留风险：
  - auto-loop 分支里的 book-ticker websocket orchestration 仍留在 `run_loop(...)`，下一步适合沿同一路径继续下沉 summary + health 装配 helper
  - 当前 helper 仍以内联 `args` 读取配置，后续若继续压缩主脚本依赖面，适合把 refresh / timeout 配置解析进一步收敛为参数对象
- 对应待办编号：
  - P1-5k

### 2026-05-10｜继续下沉 auto-loop user-data-stream monitor core seam
- 状态：已完成
- 范围：继续压缩 `run_auto_loop_user_data_stream_monitor(...)` 的 orchestration 宽度，把 existing-state 调度链路下沉到独立 core helper，并为 payload / persistence / alert seam 补 focused regression
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 新增 `run_auto_loop_user_data_stream_monitor_core(...)`，集中承载 existing `listen_key` 路径下的 `run_user_data_stream_monitor_cycle(...)`、positions 写回、alert 发射，wrapper 只保留 config 构建与 persisted state 装配
  - 为 `build_user_data_stream_position_payload(...)`、`persist_user_data_stream_monitor_to_positions(...)`、`emit_user_data_stream_alert_if_needed(...)` 增加 focused regression，固定 monitor payload、按 symbol 写回 positions、通知 payload contract
  - 新增 wrapper / core focused regression，覆盖 explicit config 下的 core 调度 contract、无 `listen_key` 空返回 contract、以及 wrapper 读取 store 后委派 core 的 seam
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'run_auto_loop_user_data_stream_monitor_core or run_auto_loop_user_data_stream_monitor_wrapper_builds_state_and_delegates or build_user_data_stream_position_payload or persist_user_data_stream_monitor_to_positions or emit_user_data_stream_alert_if_needed or run_auto_loop_user_data_stream_monitor_uses_explicit_config_without_args_namespace'` → `7 passed, 107 deselected in 0.15s`
  - `pytest -q tests/test_run_loop_smoke.py` → `43 passed in 0.91s`
- 遗留风险：
  - user-data-stream 分支的 skipped path 仍留在 wrapper，下一步可继续把 skipped payload builder 独立成更窄 seam
  - helper 目前仍直接触达 runtime store 写入 positions，后续若继续抽离到模块层，适合把持久化与通知 emitter 一并转成 builder 注入
- 对应待办编号：
  - t3

### 2026-05-10｜下沉 auto-loop book-ticker websocket orchestration helper
- 状态：已完成
- 范围：把 `run_loop(...)` 里 auto-loop 分支的 book-ticker websocket orchestration 抽到独立 helper，统一 summary + health 装配与 websocket 缺失降级事件发射，并补 focused regression
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 新增 `run_auto_loop_book_ticker_websocket_monitor(...)`，集中承载 websocket 可用时的 `resolve_auto_loop_book_ticker_symbols(...)` + `run_book_ticker_websocket_supervisor(...)` 调度，以及 `book_ticker_ws_status` health 装配
  - helper 内收口 websocket 缺失时的 unavailable summary 与 `book_ticker_ws_unavailable` 限流事件发射，保持主循环只消费统一返回结构
  - `run_loop(...)` 的 auto-loop 分支改为复用新 helper 返回的 `{summary, health}` 组装 `cycle['book_ticker_websocket']`
  - 新增两条 focused regression，覆盖 supervisor 调用与 health 装配路径、以及 websocket 缺失时的 unavailable contract 与限流事件 payload
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'run_auto_loop_book_ticker_websocket_monitor'` → `2 passed, 81 deselected in 0.14s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `96 passed in 0.41s`
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 15 source files`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` → 通过
- 遗留风险：
  - auto-loop monitor orchestration 里仍有 helper 直接读取 `args` 的配置面，后续适合把 book-ticker / user-data-stream 的配置解析进一步收敛成显式参数对象
  - 当前 helper 只收口单周期 orchestration，后续若继续拆分，可评估把 monitor summary 组合与 cycle 写入 contract 独立下沉
- 对应待办编号：
  - P1-5l

### 2026-05-10｜收敛 auto-loop monitor helper 配置为显式参数对象
- 状态：已完成
- 范围：为 auto-loop 的 book-ticker / user-data-stream monitor helper 增加显式 config 对象与 builder，把 helper 内直接读取 `args` 的配置面收敛到入口层，并补 focused regression
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 新增 `AutoLoopBookTickerWebsocketMonitorConfig`、`AutoLoopUserDataStreamMonitorConfig` 以及对应 builder，统一把 auto-loop monitor 相关配置从 `args` 提取为显式参数对象
  - `run_auto_loop_user_data_stream_monitor(...)` 改为消费 `monitor_config`，把 `okx_simulated_trading`、refresh interval、disconnect timeout 的读取收敛到 builder
  - `run_auto_loop_book_ticker_websocket_monitor(...)` 增加显式 `config` 参数，`run_loop(...)` 在 auto-loop 入口先构建 config 再注入 helper，收窄 monitor orchestration 的隐式依赖面
  - 新增四条 focused regression，覆盖 config builder contract，以及 helper 在传入显式 config 时可脱离 `argparse.Namespace` 继续工作
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'build_auto_loop_ or uses_explicit_config_without_args_namespace'` → `4 passed, 83 deselected in 0.14s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `100 passed in 0.43s`
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 15 source files`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` → 通过
- 遗留风险：
  - book-ticker helper 当前 config 还是 marker object，后续可继续把 symbol resolution / supervisor cycle 上限等策略参数显式化
  - helper 仍保留 `args` 形参用于通知与 symbol provider，后续适合继续沿依赖注入路径收敛到更窄 contract
- 对应待办编号：
  - P1-5m

### 2026-05-10｜显式注入 auto-loop book-ticker symbol provider
- 状态：已完成
- 范围：继续收窄 `run_auto_loop_book_ticker_websocket_monitor(...)` 的隐式依赖，把 symbol resolution 从 helper 内部 `resolve_auto_loop_book_ticker_symbols(client, args)` 调用改为显式 provider 注入，并补 focused regression
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 扩展 `AutoLoopBookTickerWebsocketMonitorConfig`，新增 `symbol_provider` 字段，允许 helper 直接消费显式 provider
  - 新增 `make_auto_loop_book_ticker_symbol_provider(client, args)` 与更新后的 `build_auto_loop_book_ticker_websocket_monitor_config(client, args)`，把 `resolve_auto_loop_book_ticker_symbols(...)` 封装在入口 builder 层
  - `run_auto_loop_book_ticker_websocket_monitor(...)` 改为优先消费 config 注入的 provider，并把 supervisor 的 `initial_symbols` 与 `symbol_provider` 都统一绑定到该 provider contract
  - `run_loop(...)` 的 auto-loop 入口改为传入 `client` 构建 book-ticker config，进一步压缩 helper 对 `args` 的隐式依赖面
  - 新增 focused regression，覆盖 builder 生成 provider，以及 helper 在传入显式 config 时可脱离 `argparse.Namespace` 继续工作
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'build_auto_loop_book_ticker_websocket_monitor_config_is_explicit_marker or run_auto_loop_book_ticker_websocket_monitor_uses_explicit_config_without_args_namespace'` → `2 passed, 85 deselected in 0.40s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `100 passed in 0.41s`
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 15 source files`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` → 通过
- 遗留风险：
  - `run_auto_loop_book_ticker_websocket_monitor(...)` 仍保留 `args` 形参，当前主要用于 fallback builder 路径；后续适合继续把通知与其余 helper 依赖拆到更窄 contract
  - symbol provider 当前只封装 symbol resolution，后续可继续把 supervisor cycle 上限与 health key 之类 orchestration 常量并入显式 config
- 对应待办编号：
  - P1-5n

### 2026-05-10｜显式注入 auto-loop book-ticker health loader
- 状态：已完成
- 范围：继续收窄 `run_auto_loop_book_ticker_websocket_monitor(...)` 对 runtime store 细节的耦合，把 `book_ticker_ws_status` 的读取 key 与 health loader 收进显式 config / adapter，并补 focused regression
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 扩展 `AutoLoopBookTickerWebsocketMonitorConfig`，新增 `health_loader` 与 `health_store_key`
  - 新增 `make_auto_loop_book_ticker_health_loader(store, health_store_key)`，把 `store.load_json(...)` 与 dict fallback 封装成显式 adapter
  - `build_auto_loop_book_ticker_websocket_monitor_config(client, args, store=...)` 改为同时构建 symbol provider 与 health loader
  - `run_auto_loop_book_ticker_websocket_monitor(...)` 改为优先消费 config 注入的 `health_loader`，并保留基于 `health_store_key` 的 fallback adapter 路径
  - `run_loop(...)` 的 auto-loop 入口改为在构建 book-ticker config 时传入 `store`
  - 新增 focused regression，覆盖 builder 生成 health loader，以及 helper 在传入显式 config 时可脱离 store key 细节继续工作
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'build_auto_loop_book_ticker_websocket_monitor_config_is_explicit_marker or run_auto_loop_book_ticker_websocket_monitor_uses_explicit_config_without_args_namespace'` → `2 passed, 85 deselected in 0.41s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `100 passed in 0.42s`
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 15 source files`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` → 通过
- 遗留风险：
  - `run_auto_loop_book_ticker_websocket_monitor(...)` 仍直接依赖 `append_rate_limited_runtime_event(...)` 与 websocket availability 判断，后续适合继续把 unavailable event emitter / websocket capability probe 也抽成显式 adapter
  - `book_ticker_ws_status` 已收敛到 config 层，后续可继续把 `max_supervisor_cycles=1` 这类 orchestration 常量并入 config
- 对应待办编号：
  - P1-5o

### 2026-05-10｜显式注入 auto-loop book-ticker websocket capability probe 与 unavailable emitter
- 状态：已完成
- 范围：继续收窄 `run_auto_loop_book_ticker_websocket_monitor(...)` 对全局 websocket 模块与 runtime event 发射细节的耦合，把 capability probe 与 unavailable emitter 收进显式 config / adapter，并补 focused regression
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 扩展 `AutoLoopBookTickerWebsocketMonitorConfig`，新增 `websocket_capability_probe` 与 `unavailable_event_emitter`
  - 新增 `make_auto_loop_book_ticker_websocket_capability_probe()`，把 `globals().get('websocket')` 下沉成显式 capability adapter
  - 新增 `make_auto_loop_book_ticker_unavailable_event_emitter(store)`，把 `append_rate_limited_runtime_event(...)` unavailable 路径封装成显式 emitter
  - `build_auto_loop_book_ticker_websocket_monitor_config(client, args, store=...)` 改为同时构建 symbol provider、health loader、capability probe、unavailable emitter
  - `run_auto_loop_book_ticker_websocket_monitor(...)` 改为优先消费 config 注入的 probe / emitter，并保留基于 builder 的 fallback adapter 路径
  - 新增 focused regression，覆盖 builder 生成 probe / emitter，以及 helper 在传入显式 unavailable adapters 时可脱离 store event side effect 继续工作
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'build_auto_loop_book_ticker_websocket_monitor_config_wires_explicit_probe_and_unavailable_emitter or run_auto_loop_book_ticker_websocket_monitor_uses_explicit_unavailable_adapters_without_store_event_side_effects'` → `2 passed, 87 deselected in 0.14s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `102 passed in 0.69s`
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 15 source files`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` → 通过
- 遗留风险：
  - `run_auto_loop_book_ticker_websocket_monitor(...)` 仍保留 `args` 形参，当前主要服务 fallback builder 路径；后续适合继续把通知与 supervisor orchestration 常量一起收口到更窄 contract
  - `max_supervisor_cycles=1` 与 unavailable summary schema 仍散落在 helper 内，下一刀适合继续把这些 orchestration 常量与 summary builder 下沉到显式 config / adapter
- 对应待办编号：
  - P1-5p

### 2026-05-10｜显式注入 auto-loop book-ticker supervisor cycle limit 与 unavailable summary builder
- 状态：已完成
- 范围：继续收窄 `run_auto_loop_book_ticker_websocket_monitor(...)` 的 orchestration 常量与 unavailable summary 构造，把 `max_supervisor_cycles=1` 与 unavailable summary builder 收进显式 config / adapter，并补 focused regression
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 扩展 `AutoLoopBookTickerWebsocketMonitorConfig`，新增 `unavailable_summary_builder` 与 `max_supervisor_cycles`
  - 新增 `make_auto_loop_book_ticker_unavailable_summary_builder()`，把 unavailable summary 的默认构造收成显式 adapter
  - `build_auto_loop_book_ticker_websocket_monitor_config(client, args, store=...)` 改为同时构建 unavailable summary builder，并显式固定 `max_supervisor_cycles=1`
  - `run_auto_loop_book_ticker_websocket_monitor(...)` 改为优先消费 config 注入的 summary builder，并把 supervisor cycle 上限改成读取 `monitor_config.max_supervisor_cycles`
  - 新增 focused regression，覆盖 builder 暴露 summary builder / cycle limit，以及 helper 在传入显式 summary builder 时正确透传 unavailable summary 与 supervisor cycle limit
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'build_auto_loop_book_ticker_websocket_monitor_config_is_explicit_marker or build_auto_loop_book_ticker_websocket_monitor_config_wires_explicit_probe_and_unavailable_emitter or run_auto_loop_book_ticker_websocket_monitor_uses_explicit_config_without_args_namespace or run_auto_loop_book_ticker_websocket_monitor_uses_explicit_unavailable_adapters_without_store_event_side_effects'` → `4 passed, 85 deselected in 0.14s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `102 passed in 0.43s`
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 15 source files`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` → 通过
- 遗留风险：
  - `run_auto_loop_book_ticker_websocket_monitor(...)` 仍保留 `args` 形参，当前主要服务 fallback builder 路径；后续适合继续把 symbol/provider fallback 与 health fallback 一并下沉到更窄 orchestration contract
  - unavailable reason 当前仍在 helper 内以 `'websocket_client_missing'` 文字常量出现，下一刀适合继续把 reason 枚举或 summary factory 输入收进显式 config
- 对应待办编号：
  - P1-5q

### 2026-05-10｜显式注入 auto-loop book-ticker unavailable reason
- 状态：已完成
- 范围：继续收窄 `run_auto_loop_book_ticker_websocket_monitor(...)` unavailable 路径里的文字常量，把 `'websocket_client_missing'` 收进显式 config，并补 focused regression 锁定 summary builder 入参
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 扩展 `AutoLoopBookTickerWebsocketMonitorConfig`，新增 `unavailable_reason: str = 'websocket_client_missing'`
  - `build_auto_loop_book_ticker_websocket_monitor_config(client, args, store=...)` 改为显式写入 `unavailable_reason='websocket_client_missing'`
  - `run_auto_loop_book_ticker_websocket_monitor(...)` 改为把 `monitor_config.unavailable_reason` 传给 `unavailable_summary_builder(...)`
  - focused regression 更新为断言 builder 暴露 `unavailable_reason`，并验证 helper 在传入自定义 reason 时透传到 summary builder / emitter / 返回 summary
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'build_auto_loop_book_ticker_websocket_monitor_config_is_explicit_marker or build_auto_loop_book_ticker_websocket_monitor_config_wires_explicit_probe_and_unavailable_emitter or run_auto_loop_book_ticker_websocket_monitor_uses_explicit_unavailable_adapters_without_store_event_side_effects'` → `3 passed, 86 deselected in 0.13s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `102 passed in 0.42s`
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 15 source files`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` → 通过
- 遗留风险：
  - `run_auto_loop_book_ticker_websocket_monitor(...)` 仍保留 `args` 与 `store` fallback builder 路径，后续适合继续把 provider / loader 默认构建迁移到更外层调用方
  - unavailable summary builder 当前仍只消费 `reason` 单参数；后续若要继续扩 schema，适合把 event_source 或 summary payload factory 一起收进口径稳定的 adapter
- 对应待办编号：
  - P1-5r

### 2026-05-10｜锁定 complete-config path 无 fallback builder 依赖
- 状态：已完成
- 范围：验证 `run_auto_loop_book_ticker_websocket_monitor(...)` 在传入完整 `config` 时已经形成稳定 orchestration seam，直接消费注入的 `symbol_provider` / `health_loader`，并保持对 config builder 与 fallback builders 的零依赖
- 修改文件：
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 新增 `test_run_auto_loop_book_ticker_websocket_monitor_complete_config_path_avoids_fallback_builders`
  - 在测试里显式把 `build_auto_loop_book_ticker_websocket_monitor_config(...)`、`make_auto_loop_book_ticker_symbol_provider(...)`、`make_auto_loop_book_ticker_health_loader(...)` 设为触发 `AssertionError` 的哨兵，锁定 complete-config path 的零 fallback 访问 contract
  - 验证 helper 在传入完整 `AutoLoopBookTickerWebsocketMonitorConfig(...)` 时直接消费注入 provider / loader，正确透传到 supervisor 与返回 health
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'run_auto_loop_book_ticker_websocket_monitor_complete_config_path_avoids_fallback_builders'` → `1 passed, 89 deselected in 0.40s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `103 passed in 0.44s`
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 15 source files`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` → 通过
- 遗留风险：
  - helper 形参层面仍保留 `args` 与 `store`，当前完整 config path 已稳定，后续适合继续拆出一个更窄的 core helper，只保留 orchestration 所需最小依赖
  - 当前新增的是 contract regression，生产代码已经满足该 contract；下一刀更适合把 fallback builder 责任外移到调用方，收窄公开 helper 签名
- 对应待办编号：
  - P1-5s

### 2026-05-10｜切出 auto-loop book-ticker core helper，外层仅负责 fallback config
- 状态：已完成
- 范围：把 `run_auto_loop_book_ticker_websocket_monitor(...)` 继续收窄为 wrapper，只保留 fallback config 构建职责；把 websocket probe / unavailable path / supervisor / health orchestration 下沉到一个只消费 `store + config` 的 core helper
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 新增 `run_auto_loop_book_ticker_websocket_monitor_core(*, store, config)`
  - `run_auto_loop_book_ticker_websocket_monitor(client, store, args, config=None)` 改为：先构建 `monitor_config`，再直接委托给 core helper
  - core helper 只消费 `store` 与完整 `AutoLoopBookTickerWebsocketMonitorConfig(...)`，承接 websocket capability probe、unavailable summary/emitter、supervisor 调用、health loader 读取
  - 新增 focused regression：
    - `test_run_auto_loop_book_ticker_websocket_monitor_core_uses_minimal_orchestration_dependencies`
    - `test_run_auto_loop_book_ticker_websocket_monitor_builds_config_then_delegates_to_core`
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'run_auto_loop_book_ticker_websocket_monitor_core_uses_minimal_orchestration_dependencies or run_auto_loop_book_ticker_websocket_monitor_builds_config_then_delegates_to_core'` → `2 passed, 90 deselected in 0.14s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `105 passed in 0.42s`
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 15 source files`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` → 通过
- 遗留风险：
  - 旧 wrapper 仍保留 `client` 与 `args` 形参，当前只承担 config builder/fallback seam；后续适合继续把 builder 使用点上移到更外层 call site，进一步缩小公开入口
  - `run_auto_loop_book_ticker_websocket_monitor_core(...)` 仍直接消费 dataclass config；后续若要继续稳定 seam，适合补 protocol/type alias 收口 config 依赖面
- 对应待办编号：
  - P1-5t

### 2026-05-10｜把 book-ticker config builder 使用点上移到 run_loop
- 状态：已完成
- 范围：让外层 auto-loop orchestration 直接构建 `book_ticker_config` 并调用 `run_auto_loop_book_ticker_websocket_monitor_core(...)`，把旧 wrapper 从主执行路径移出，仅保留兼容性的 build+delegate seam
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - `run_loop(...)` 的 auto-loop 分支改为：
    - 先调用 `build_auto_loop_book_ticker_websocket_monitor_config(client, args, store=store)`
    - 再直接调用 `run_auto_loop_book_ticker_websocket_monitor_core(store=store, config=book_ticker_config)`
  - 旧 wrapper `run_auto_loop_book_ticker_websocket_monitor(...)` 从主 orchestration path 退出，当前只保留给旧调用点和回归 contract 使用
  - 新增 focused regression：`test_run_cycle_auto_loop_builds_book_ticker_config_then_calls_core_helper`
  - 测试中把 wrapper 设为 `AssertionError` 哨兵，锁定外层 orchestration 对 core helper 的直接调用 contract
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'run_cycle_auto_loop_builds_book_ticker_config_then_calls_core_helper or run_auto_loop_book_ticker_websocket_monitor_builds_config_then_delegates_to_core'` → `2 passed, 91 deselected in 0.45s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `106 passed in 0.61s`
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 15 source files`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` → 通过
- 遗留风险：
  - 旧 wrapper 仍保留公开入口价值，后续适合清点脚本内外剩余调用点，再决定是删除 wrapper 还是把它明确标成兼容层
  - `run_loop(...)` 里的 auto-loop 分支已经直接依赖 core helper，后续适合对 user-data-stream monitor 做同样 seam 收窄，统一两条 monitor orchestration 形态
- 对应待办编号：
  - P1-5u

### 2026-05-10｜清点 wrapper 剩余调用点并标记 compatibility seam
- 状态：已完成
- 范围：审计 `run_auto_loop_book_ticker_websocket_monitor(...)` 在脚本与测试中的剩余调用点，确认生产路径已经全部迁移到 `run_auto_loop_book_ticker_websocket_monitor_core(...)`，并把旧 wrapper 明确标记为 compatibility seam。
- 变更：
  - 全仓搜索 `run_auto_loop_book_ticker_websocket_monitor(...)`，确认脚本内只剩函数定义，没有任何生产调用点；当前剩余引用全部位于 `tests/test_strategy_v2_restore_regression.py`
  - 给 `run_auto_loop_book_ticker_websocket_monitor(...)` 补上 docstring：`Compatibility wrapper: build fallback config then delegate to core helper.`，把职责边界固定为兼容入口
  - 保留 wrapper contract test，并增加对 compatibility docstring 的断言，确保后续重构中不会再把它误当成主 orchestration 入口
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'run_auto_loop_book_ticker_websocket_monitor_builds_config_then_delegates_to_core or run_cycle_auto_loop_builds_book_ticker_config_then_calls_core_helper'` ✅
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` ✅（`106 passed`）
  - `.venv-typecheck/bin/python -m mypy` ✅
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` ✅
- 对应待办编号：
  - P1-5v

### 2026-05-10｜拆分 book-ticker core helper 的 unavailable / available 分支
- 状态：已完成
- 范围：把 `run_auto_loop_book_ticker_websocket_monitor_core(...)` 收窄为 branch orchestration，并把 unavailable path 与 supervisor+health path 各自下沉到独立 helper。
- 变更：
  - 新增 `run_auto_loop_book_ticker_websocket_monitor_unavailable_branch(...)`，承接 unavailable summary builder、event emitter、返回 payload 的拼装
  - 新增 `run_auto_loop_book_ticker_websocket_monitor_available_branch(...)`，承接 symbol provider、supervisor 调用、health loader 读取
  - `run_auto_loop_book_ticker_websocket_monitor_core(...)` 现在只负责 websocket capability probe 与 branch dispatch
  - available branch 显式要求 `config.symbol_provider`，通过 `ValueError` 固定可用分支的最小依赖契约
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'run_auto_loop_book_ticker_websocket_monitor_unavailable_branch_uses_summary_and_emitter or run_auto_loop_book_ticker_websocket_monitor_available_branch_uses_supervisor_and_health or run_auto_loop_book_ticker_websocket_monitor_core_uses_minimal_orchestration_dependencies'` ✅
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` ✅（`108 passed`）
  - `.venv-typecheck/bin/python -m mypy` ✅
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py` ✅
- 对应待办编号：
  - P1-5w

### 2026-05-10｜available branch 下沉 supervisor summary / health reader seams
- 状态：已完成
- 范围：继续收窄 `run_auto_loop_book_ticker_websocket_monitor_available_branch(...)`，把 supervisor 执行与 health 读取分别下沉为独立 seam。
- 变更：
  - 新增 `build_auto_loop_book_ticker_supervisor_summary(...)`，集中负责 `symbol_provider -> initial_symbols -> run_book_ticker_websocket_supervisor(...)`。
  - 新增 `read_auto_loop_book_ticker_health(...)`，集中负责 `config.health_loader` 与 `make_auto_loop_book_ticker_health_loader(...)` 的选择与执行。
  - `run_auto_loop_book_ticker_websocket_monitor_available_branch(...)` 现在只保留 `symbol_provider` 前置校验、summary seam 调用、health seam 调用、result assembly。
- 测试：
  - 新增 `test_build_auto_loop_book_ticker_supervisor_summary_runs_supervisor`，锁定 supervisor seam 的入参与 provider 调用行为。
  - 新增 `test_read_auto_loop_book_ticker_health_uses_config_loader`，锁定 config 自带 loader 优先级。
  - 更新 `test_run_auto_loop_book_ticker_websocket_monitor_available_branch_uses_supervisor_and_health`，要求 available branch 通过两个新 seam 编排。
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'run_auto_loop_book_ticker_websocket_monitor_available_branch_uses_supervisor_and_health or build_auto_loop_book_ticker_supervisor_summary_runs_supervisor or read_auto_loop_book_ticker_health_uses_config_loader'`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py`
  - `.venv-typecheck/bin/python -m mypy`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py`
- 对应待办编号：
  - P1-5x

### 2026-05-10｜unavailable branch 下沉 result seam
- 状态：已完成
- 范围：继续收窄 `run_auto_loop_book_ticker_websocket_monitor_unavailable_branch(...)`，把 unavailable payload 组装下沉为独立 seam。
- 变更：
  - 新增 `build_auto_loop_book_ticker_unavailable_result(...)`，集中负责 unavailable payload 的 result assembly。
  - `run_auto_loop_book_ticker_websocket_monitor_unavailable_branch(...)` 现在只保留 summary seam 调用、event emitter seam 调用、result seam 调用。
- 测试：
  - 更新 `test_run_auto_loop_book_ticker_websocket_monitor_unavailable_branch_uses_summary_and_emitter`，要求 unavailable branch 通过 result seam 返回结果。
  - 新增 `test_build_auto_loop_book_ticker_unavailable_result_returns_default_payload`，锁定默认 unavailable payload 契约。
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'run_auto_loop_book_ticker_websocket_monitor_unavailable_branch_uses_summary_and_emitter or build_auto_loop_book_ticker_unavailable_result_returns_default_payload'`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py`
  - `.venv-typecheck/bin/python -m mypy`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py`
- 对应待办编号：
  - P1-5y

### 2026-05-10｜monitor config builder 下沉 optional/default seam helpers
- 状态：已完成
- 范围：继续收窄 `build_auto_loop_book_ticker_websocket_monitor_config(...)`，把 store 相关与默认 seam 相关装配拆成两个独立 helper。
- 变更：
  - 新增 `build_auto_loop_book_ticker_monitor_optional_store_seams(store, health_store_key='book_ticker_ws_status')`：统一返回 `health_loader` / `unavailable_event_emitter`。
  - 新增 `build_auto_loop_book_ticker_monitor_default_seams()`：统一返回 `websocket_capability_probe` / `unavailable_summary_builder`。
  - `build_auto_loop_book_ticker_websocket_monitor_config(...)` 改为只做 `health_store_key` 常量、symbol provider、两个 seam helper 调用、dataclass assembly。
- 测试：
  - 新增 `test_build_auto_loop_book_ticker_monitor_optional_store_seams_wires_loader_and_emitter`
  - 新增 `test_build_auto_loop_book_ticker_monitor_default_seams_wires_probe_and_summary_builder`
  - 更新 `test_build_auto_loop_book_ticker_websocket_monitor_config_is_explicit_marker`，锁定 config builder 通过两个 seam helper 组装。
  - 保持 `test_build_auto_loop_book_ticker_websocket_monitor_config_wires_explicit_probe_and_unavailable_emitter` 通过，覆盖原有 wiring 契约。
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'build_auto_loop_book_ticker_monitor_optional_store_seams_wires_loader_and_emitter or build_auto_loop_book_ticker_monitor_default_seams_wires_probe_and_summary_builder or build_auto_loop_book_ticker_websocket_monitor_config_is_explicit_marker or build_auto_loop_book_ticker_websocket_monitor_config_wires_explicit_probe_and_unavailable_emitter'`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py`
  - `.venv-typecheck/bin/python -m mypy`
- 结果：focused 4 例通过；相关回归 `113 passed`；mypy 通过。

### 2026-05-10｜monitor core / branch fallback 下沉 resolver helpers
- 状态：已完成
- 范围：把 monitor core、unavailable branch、health reader 里的 fallback seam 选择逻辑统一收口为 resolver helpers，进一步压薄 orchestration 层。
- 变更：
  - 新增 `resolve_auto_loop_book_ticker_websocket_capability_probe(config)`。
  - 新增 `resolve_auto_loop_book_ticker_unavailable_summary_builder(config)`。
  - 新增 `resolve_auto_loop_book_ticker_unavailable_event_emitter(store, config)`。
  - 新增 `resolve_auto_loop_book_ticker_health_loader(store, config)`。
  - `run_auto_loop_book_ticker_websocket_monitor_core(...)` 改为通过 `resolve_auto_loop_book_ticker_websocket_capability_probe(...)` 取 probe，再只做 branch dispatch。
  - `run_auto_loop_book_ticker_websocket_monitor_unavailable_branch(...)` 改为通过 summary/emitter resolver 取 seam。
  - `read_auto_loop_book_ticker_health(...)` 改为通过 health loader resolver 取 seam。
- 测试：
  - 新增 `test_resolve_auto_loop_book_ticker_websocket_capability_probe_prefers_config_probe`
  - 新增 `test_resolve_auto_loop_book_ticker_unavailable_summary_builder_prefers_config_builder`
  - 新增 `test_resolve_auto_loop_book_ticker_unavailable_event_emitter_prefers_config_emitter`
  - 新增 `test_resolve_auto_loop_book_ticker_health_loader_prefers_config_loader`
  - 更新 `test_run_auto_loop_book_ticker_websocket_monitor_core_uses_minimal_orchestration_dependencies`，锁定 core 只做 resolver + branch dispatch。
  - 保持 `test_run_auto_loop_book_ticker_websocket_monitor_unavailable_branch_uses_summary_and_emitter` 与 `test_read_auto_loop_book_ticker_health_uses_config_loader` 通过，覆盖 resolver 接入后的旧契约。
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'resolve_auto_loop_book_ticker_websocket_capability_probe_prefers_config_probe or resolve_auto_loop_book_ticker_unavailable_summary_builder_prefers_config_builder or resolve_auto_loop_book_ticker_unavailable_event_emitter_prefers_config_emitter or resolve_auto_loop_book_ticker_health_loader_prefers_config_loader or run_auto_loop_book_ticker_websocket_monitor_core_uses_minimal_orchestration_dependencies or run_auto_loop_book_ticker_websocket_monitor_unavailable_branch_uses_summary_and_emitter or read_auto_loop_book_ticker_health_uses_config_loader'`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py`
  - `.venv-typecheck/bin/python -m mypy`
- 结果：focused 7 例通过；相关回归 `117 passed`；mypy 通过。

### 2026-05-10｜available branch 补 symbol provider resolver seam
- 状态：已完成
- 范围：继续让 available branch 与 unavailable branch 对齐，把 symbol provider 的 fallback 选择显式收口为 resolver helper。
- 变更：
  - 新增 `resolve_auto_loop_book_ticker_symbol_provider(client, args, config)`。
  - resolver 契约：优先返回 `config.symbol_provider`；缺省时回退到 `make_auto_loop_book_ticker_symbol_provider(client, args)`。
  - 保持现有 available branch contract 不变，为下一步把 available branch 从直接读取 config 过渡到统一 resolver 形态打底。
- 测试：
  - 新增 `test_resolve_auto_loop_book_ticker_symbol_provider_prefers_config_provider`
  - 新增 `test_resolve_auto_loop_book_ticker_symbol_provider_builds_fallback_provider`
  - 保持 `test_run_auto_loop_book_ticker_websocket_monitor_available_branch_uses_supervisor_and_health` 通过，锁定 available branch 旧契约继续稳定。
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k 'resolve_auto_loop_book_ticker_symbol_provider_prefers_config_provider or resolve_auto_loop_book_ticker_symbol_provider_builds_fallback_provider or run_auto_loop_book_ticker_websocket_monitor_available_branch_uses_supervisor_and_health'`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py tests/test_strategy_v2_restore_regression.py`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py`
  - `.venv-typecheck/bin/python -m mypy`
- 结果：focused 3 例通过；相关回归 `119 passed`；mypy 通过。

### 2026-05-10｜available branch 下沉 result builder 与 max_supervisor_cycles resolver
- 状态：已完成
- 范围：继续让 available branch 与 unavailable branch 对齐，把简单字段读取和返回值组装从 branch 编排里拆出
- 改动：
  - 新增 `resolve_auto_loop_book_ticker_max_supervisor_cycles(config)`，统一读取 `config.max_supervisor_cycles`
  - 新增 `build_auto_loop_book_ticker_available_result(summary, health)`，统一 available branch 默认 payload 结构
  - `run_auto_loop_book_ticker_websocket_monitor_available_branch(...)` 改为：resolver 取 max cycles → summary builder → health reader → result builder
- 测试：
  - 新增 `test_build_auto_loop_book_ticker_available_result_returns_default_payload`
  - 新增 `test_resolve_auto_loop_book_ticker_max_supervisor_cycles_reads_config_value`
  - 更新 `test_run_auto_loop_book_ticker_websocket_monitor_available_branch_uses_supervisor_and_health`
- 结果：focused 3 例通过；相关回归 `121 passed`；mypy 通过。

### 2026-05-10｜下沉 monitor event normalization helper 并复用到 execution parity 回归
  - `pyproject.toml`
  - `scripts/runtime_state_risk_helpers.py`
  - `tests/test_execution_module_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 将 `scripts/risk_state_helpers.py` 与 `scripts/runtime_state_risk_helpers.py` 加入 `[tool.mypy].files`
  - 为 `runtime_state_risk_helpers.py` 增加 `AppendRuntimeStateDegradedEvent` 与 `RefreshRiskStateHeatSnapshot` protocol，显式收口 `append_rate_limited_runtime_event(...)` 与 `refresh_risk_state_heat_snapshot(...)` 的注入签名
  - 调整 `tests/test_execution_module_regression.py` 的 `monitor_live_trade` parity 断言，只比较稳定事件序列与收口后的 `trade_invalidated` 终态 payload，屏蔽 `consumer` 与 `time_in_trade_minutes` 这类运行时抖动字段
- 验证：
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 15 source files`
  - `pytest -q tests/test_strategy_v2_restore_regression.py tests/test_execution_module_regression.py` → `90 passed in 0.41s`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py scripts/risk_state_helpers.py scripts/runtime_state_risk_helpers.py tests/test_strategy_v2_restore_regression.py tests/test_execution_module_regression.py` → 通过
- 遗留风险：
  - runtime-state risk 路径当前仍主要通过 `tests/test_strategy_v2_restore_regression.py` 与 execution parity 回归覆盖，后续适合补 `runtime_state_risk_helpers.py` 直连单测文件，进一步缩短验证链路
  - `tests/test_execution_module_regression.py` 当前对 `trade_invalidated` 事件采用稳定字段比较，后续若要继续抽 monitor seam，适合把事件归一化 helper 单独下沉复用
- 对应待办编号：
  - P1-5i
  - P2-1c

### 2026-05-10｜下沉 monitor event normalization helper 并复用到 execution parity 回归
- 状态：已完成
- 范围：把 `monitor_live_trade` parity 回归里用于稳定字段比较的 event 归一化逻辑下沉到 `scripts/execution_engine.py`，减少测试内局部去抖代码并固定 `trade_invalidated` 终态对比 contract
- 修改文件：
  - `scripts/execution_engine.py`
  - `tests/test_execution_module_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 在 `scripts/execution_engine.py` 新增 `normalize_monitor_event_row(...)` 与 `normalize_monitor_event_rows(...)`，统一剥离 `recorded_at`、`opened_at`、`closed_at`、`consumer` 等运行时抖动字段
  - 将 `trade_invalidated` 的 `time_in_trade_minutes` 归入 helper 内的稳定字段收口逻辑，固定 monitor parity 终态 payload 的比较边界
  - `tests/test_execution_module_regression.py` 改为直接复用新 helper，对 script / extracted module 的事件序列比较只保留稳定事件类型与收口后的末条事件断言
  - 新增 `test_normalize_monitor_event_rows_strips_runtime_noise_fields`，直接锁定 helper 对普通事件与 `trade_invalidated` 事件的去抖 contract
- 验证：
  - `pytest -q tests/test_execution_module_regression.py -k 'monitor_live_trade or normalize_monitor_event_rows'` → `4 passed, 9 deselected in 0.20s`
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py` → `91 passed in 0.41s`
  - `.venv-typecheck/bin/python -m mypy` → `Success: no issues found in 15 source files`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py scripts/execution_engine.py scripts/risk_state_helpers.py scripts/runtime_state_risk_helpers.py tests/test_strategy_v2_restore_regression.py tests/test_execution_module_regression.py` → 通过
- 遗留风险：
  - 当前 helper 聚焦 monitor parity 的稳定字段收口，后续若要继续抽 monitor seam，适合把更多事件 schema 常量与断言入口一并下沉
  - 主脚本 `binance_futures_momentum_long.py` 里的 monitor orchestration 仍保留较宽依赖注入面，下一步适合继续评估线程启动入口与 provider seam 的拆分优先级
- 对应待办编号：
  - P1-5j

### 2026-05-10｜下沉 load_risk_state consumer helper
- 状态：已完成
- 范围：把 `load_risk_state(...)` 的 `risk_state.json` 读取、degraded-event envelope、`positions` 读取与 heat snapshot 刷新边界一起下沉到 `runtime_state_risk_helpers.py`，并补齐 consumer-helper parity 回归
- 修改文件：
  - `scripts/runtime_state_risk_helpers.py`
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 在 `scripts/runtime_state_risk_helpers.py` 新增 `load_runtime_risk_state(...)`，集中承载 `risk_state.json` 读取、`runtime_state_degraded` 限流事件发射、fallback 到 `default_risk_state()`、`positions` 读取与 heat snapshot 刷新
  - 主脚本 `load_risk_state(...)` 收敛为 wrapper，仅负责注入 `_should_emit_runtime_state_degraded`、`append_rate_limited_runtime_event`、`default_risk_state`、`normalize_loaded_risk_state`、`refresh_risk_state_heat_snapshot`
  - `refresh_risk_state_heat_snapshot(...)` wrapper 调整为运行时读取 `compute_positions_heat_snapshot`，保留 monkeypatch 场景下与既有测试契约一致的动态绑定行为
  - 新增 normal / malformed `risk_state.json` 两条 consumer-helper parity regression，直接校验主脚本 wrapper 与 helper 在输出结果、事件 payload 上的一致性
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "load_risk_state_matches_runtime_state_consumer_helper or load_risk_state_matches_runtime_state_consumer_helper_on_malformed_risk_state_json or load_risk_state_merges_defaults_and_refreshes_heat_snapshot or malformed_risk_state_json"` → RED：`2 failed, 2 passed`，报错 `AttributeError: module 'runtime_state_risk_helpers' has no attribute 'load_runtime_risk_state'`
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "load_risk_state_matches_runtime_state_consumer_helper or load_risk_state_matches_runtime_state_consumer_helper_on_malformed_risk_state_json or load_risk_state_merges_defaults_and_refreshes_heat_snapshot or malformed_risk_state_json"` → RED：`2 failed, 2 passed`，报错 `AttributeError: module 'runtime_state_risk_helpers' has no attribute 'refresh_risk_state_heat_snapshot'`
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "load_risk_state_matches_runtime_state_consumer_helper or load_risk_state_matches_runtime_state_consumer_helper_on_malformed_risk_state_json or load_risk_state_merges_defaults_and_refreshes_heat_snapshot or malformed_risk_state_json"` → `4 passed, 74 deselected in 0.36s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py tests/test_execution_module_regression.py` → `90 passed in 0.39s`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py scripts/risk_state_helpers.py scripts/runtime_state_risk_helpers.py tests/test_strategy_v2_restore_regression.py tests/test_execution_module_regression.py` → 通过
- 遗留风险：
  - `runtime_state_risk_helpers.py` 与 `risk_state_helpers.py` 仍未纳入项目级 mypy 文件列表，下一步适合连同对应回归一起加入静态检查基线
  - runtime-state risk 路径仍主要通过主脚本集成回归覆盖，后续适合继续补 helper 直连测试与更细粒度 seam
- 对应待办编号：
  - P1-5i

### 2026-05-10｜下沉 build_local_open_positions_for_risk consumer helper
- 状态：已完成
- 范围：把 `build_local_open_positions_for_risk(...)` 的 positions 读取与 degraded-event envelope 一起下沉到独立 runtime-state consumer helper，并补齐 malformed positions 的 module parity 回归
- 修改文件：
  - `scripts/runtime_state_risk_helpers.py`
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2_restore_regression.py`
  - `/root/binan/.hermes/plans/2026-05-09_100258-binance-refactor-tracker.md`
- 完成内容：
  - 在 `scripts/runtime_state_risk_helpers.py` 新增 `load_local_open_positions_for_risk(...)`，集中承载 `positions.json` 读取、`runtime_state_degraded` 限流事件发射与 fallback envelope
  - 主脚本 `build_local_open_positions_for_risk(...)` 收敛为 wrapper，仅负责把 `_should_emit_runtime_state_degraded`、`append_rate_limited_runtime_event`、`iter_canonical_open_positions` 等依赖注入到 helper
  - 新增 malformed `positions.json` 场景的 consumer-helper parity regression，直接校验主脚本 wrapper 与 helper 在事件 payload 与空结果上的一致性
  - 保留既有 `build_local_open_positions_from_state(...)` helper，形成“读取 envelope / 状态转 rows”两层边界
- 验证：
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "build_local_open_positions_for_risk_matches_runtime_state_consumer_helper_on_malformed_positions_json or build_local_open_positions_for_risk_matches_runtime_state_helper"` → RED：`1 failed, 1 passed`，报错 `AttributeError: module 'runtime_state_risk_helpers' has no attribute 'load_local_open_positions_for_risk'`
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "build_local_open_positions_for_risk_matches_runtime_state_consumer_helper_on_malformed_positions_json or build_local_open_positions_for_risk_matches_runtime_state_helper or build_local_open_positions_for_risk_emits_rate_limited_event_on_malformed_positions_json"` → RED：`2 failed, 1 passed`，报错 `TypeError: append_rate_limited_runtime_event() missing 1 required positional argument: 'key'`
  - `pytest -q tests/test_strategy_v2_restore_regression.py -k "build_local_open_positions_for_risk_matches_runtime_state_consumer_helper_on_malformed_positions_json or build_local_open_positions_for_risk_matches_runtime_state_helper or build_local_open_positions_for_risk_emits_rate_limited_event_on_malformed_positions_json"` → `3 passed, 73 deselected in 0.15s`
  - `pytest -q tests/test_strategy_v2_restore_regression.py tests/test_execution_module_regression.py` → `88 passed in 0.38s`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py scripts/risk_state_helpers.py scripts/runtime_state_risk_helpers.py tests/test_strategy_v2_restore_regression.py tests/test_execution_module_regression.py` → 通过
- 遗留风险：
  - `load_risk_state(...)` 仍直接读取 `positions` 并驱动 heat snapshot，下一步适合继续把该 consumer 边界与 `load_local_open_positions_for_risk(...)` 共享的读取 / degraded 语义继续收口
  - `runtime_state_risk_helpers.py` 当前尚未纳入项目级 mypy 文件列表
- 对应待办编号：
  - P1-5h

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
  - [x] P1-5h 下沉 `load_local_open_positions_for_risk(...)`，收口 positions 读取 + degraded event envelope consumer helper
  - [x] P1-5i 下沉 `load_runtime_risk_state(...)`，收口 `risk_state` / `positions` 读取与 heat snapshot consumer 边界
- 验证：
  - `pytest -q tests/test_execution_module_regression.py tests/test_strategy_v2_restore_regression.py`
  - `python -m compileall -q scripts/binance_futures_momentum_long.py scripts/risk_state_helpers.py scripts/runtime_state_risk_helpers.py tests/test_strategy_v2_restore_regression.py tests/test_execution_module_regression.py`
- 下一步：评估 `binance_futures_momentum_long.py` 里剩余 monitor orchestration / thread provider seam 的下沉优先级，并继续收窄主脚本对 runtime 依赖注入面的体量

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
按顺序执行：先把 monitor / event normalization 里的稳定字段归一化逻辑抽成可复用 helper，减少 execution parity 测试中的局部去抖与重复断言 → 再评估 `binance_futures_momentum_long.py` 中剩余 runtime-state consumer seam 的下沉优先级与收益 → 最后决定是否继续拆出更细的 runtime-state consumer helpers

### 2026-05-11｜放宽 regime 触发阈值
- 状态：已完成
- 范围：下调 `derive_regime_entry_thresholds(...)` 在趋势顺风与逆风环境下的 5m 涨跌幅 / 加速度门槛，让 watch/setup 候选更早进入 trigger confirmation 流程
- 修改文件：
  - `scripts/binance_futures_momentum_long.py`
  - `tests/test_strategy_v2.py`
- 完成内容：
  - 下调 long risk_on 与 short risk_off 的 `min_5m_change_pct` 从 `0.85x` 到 `0.75x`，`acceleration_ratio` 从 `-0.15` 到 `-0.25` 的放宽幅度
  - 下调 long risk_off 与 short risk_on 的收紧幅度：`min_5m_change_pct` 从 `1.25x` 收敛到 `1.1x`，`acceleration_ratio` 从 `+0.35` 收敛到 `+0.2`
  - 保留 caution 档的温和调节，维持 trigger confirmation、crowding guard、high-elastic pullback guard contract 不变
  - 更新 `test_derive_regime_entry_thresholds_bias_by_regime_and_side` 断言，固定新阈值 contract
- 验证：
  - `pytest -q /root/binan/tests/test_strategy_v2.py -k "derive_regime_entry_thresholds_bias_by_regime_and_side or evaluate_trigger_confirmation_requires_two_micro_confirmations or evaluate_trigger_confirmation_blocks_crowded_high_elastic_long_without_pullback"` → `3 passed, 62 deselected in 0.15s`
  - `pytest -q /root/binan/tests/test_cli_args.py /root/binan/tests/test_strategy_v2.py` → `79 passed in 0.50s`
- 遗留风险：
  - 这次调整降低的是 candidate builder 的 regime 门槛，真实触发频率还会受 `watch_breakout_tolerance_pct`、成交量 gate、higher timeframe gate 共同约束
  - 若后续仍觉得触发偏少，优先评估 near-breakout tolerance 与 setup/watch 阶段的 admission gate，再看是否需要继续放宽 trigger confirmation 本身
- 对应待办编号：
  - P1-3 / 策略阈值微调
