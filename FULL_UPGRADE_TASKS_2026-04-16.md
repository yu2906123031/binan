# Binance 全量优化实施清单

## Phase 0 - 安全与恢复
1. 新增 runtime state store（positions/orders/risk/events/heartbeats）
2. 新增 orphan/reconcile 启动恢复逻辑
3. 新增账户级风控（daily max loss / consecutive losses / symbol cooldown / total risk）
4. 新增关键异常告警（止损缺失、对账失败、连续 API 异常）

## Phase 1 - 模块化
5. 从单文件脚本抽离 package：config / state_store / risk / execution / runtime / logging
6. 保持现有脚本入口兼容

## Phase 2 - 运行质量
7. 新增 heartbeat
8. 新增 JSONL 结构化事件日志
9. 新增保护单 / 仓位对账修复器
10. 新增执行质量控制（滑点/点差/深度）

## Phase 3 - 验证与文档
11. 补测试覆盖并跑全量测试
12. 更新 SKILL.md 与运行说明

## 2026-04-20 当前增量优化审计（共 17 条）
1. `candidate_rejected` 事件标准化落地，已完成
2. `trade_invalidated` 事件标准化落地，已完成
3. `exit_reason` 结构化字段落地，已完成
4. Candidate 三层拆分：`must_pass_flags / quality_score / execution_priority_score`，已完成
5. 最大追价距离字段 `entry_distance_from_breakout_pct`，已完成
6. VWAP 追价距离字段 `entry_distance_from_vwap_pct`，已完成
7. `candle_extension_pct` 过热字段，已完成
8. `recent_3bar_runup_pct` 过热字段，已完成
9. `overextension_flag` 分级字段，已完成
10. 两段式入场状态 `setup_ready`，已完成
11. 两段式入场状态 `trigger_fired`，已完成
12. 执行前滑点字段 `expected_slippage_pct`，已完成
13. 深度成交占比字段 `book_depth_fill_ratio`，已完成
14. 对外 alert 暴露新增 Candidate 扩展字段，已完成
15. breakeven 结构确认字段 `breakeven_confirmation_mode`，已完成
16. 事件契约回归测试覆盖新增字段，已完成
17. 文档与测试同步更新，已完成

## 2026-04-20 第二批闭环优化增量
1. `place_live_trade()` 现已把 Binance 市价单成交反馈结构化写回 `entry_order_feedback`
2. `entry_filled` 事件现已带 `filled_quantity / entry_order_id / entry_client_order_id / entry_order_status / entry_cum_quote / entry_update_time`
3. 后台 auto-loop 的 `positions.json` 现已持久化开仓成交回执核心字段，便于重启接管与对账
4. `buy_fill_confirmed` 事件现已同步落成交回执字段，后续 watcher / 报表 / 告警可直接消费
5. 新增 smoke tests 覆盖“先发 entry_filled，再挂初始止损”和“后台线程模式持久化成交反馈”两条闭环

## 2026-04-20 文档核对结论
- 当前文档列出的 17 条增量优化项已全部落地，代码与测试已有对应覆盖。
- 第二批闭环优化 5 条也已落地，包含 `entry_order_feedback`、`entry_filled` 成交回执字段、`positions.json` 持久化成交反馈、`buy_fill_confirmed` 成交确认字段、以及 user data stream smoke tests。
- 本轮新增 user data stream listen key 续期/失败/断线监控基础能力已落地：
  - `run_user_data_stream_monitor_cycle()` 已实现 listen key 首次创建、定时续期、续期失败计数、断线超时标记。
  - `user_data_stream` 状态现已持久化 `started_at / last_refresh_at / updated_at / refresh_failure_count / disconnect_count`。
  - CLI 已新增 `--user-stream-refresh-interval-minutes` 与 `--user-stream-disconnect-timeout-minutes` 两个运行参数。
  - `tests/test_run_loop_smoke.py` 已补 4 条监控 smoke tests，覆盖 started / refreshed / refresh_failed / disconnected 四条链路。
- 本轮核对依据：`scripts/binance_futures_momentum_long.py`、`tests/test_run_loop_smoke.py` 与既有策略测试文件。

## 2026-04-20 当前已验证的映射
1. `candidate_rejected` / `trade_invalidated` / `exit_reason` 已在 runtime events 中结构化落地。
2. Candidate 扩展字段 `must_pass_flags / quality_score / execution_priority_score / entry_distance_from_breakout_pct / entry_distance_from_vwap_pct / candle_extension_pct / recent_3bar_runup_pct / overextension_flag / setup_ready / trigger_fired / expected_slippage_pct / book_depth_fill_ratio` 已进入 alert、reject event、live 风控链路。
3. `breakeven_confirmation_mode` 已进入 trade management plan、`breakeven_moved` 事件、落盘仓位状态。
4. 执行质量分层已升级到 `expected_slippage_r / execution_liquidity_grade / execution_quality_size_multiplier / execution_quality_size_bucket`，并接入缩仓、hard veto、risk guard、reject stats；当前 `execution_liquidity_grade` 已纳入 `spread_bps / orderbook_slope / cancel_rate` 三个微观结构输入。
5. 开仓成交回执已贯通 `place_live_trade()` 返回值、`entry_filled`、`buy_fill_confirmed`、`positions.json`、以及 user data stream `ORDER_TRADE_UPDATE` 生命周期落盘。

## 2026-04-20 当前仍值得继续优化的方向
1. `candidate_rejected` 已有 reason label 与聚合统计，现已补独立 rejected-analysis 脚本 `scripts/rejected_analysis.py`，可直接基于 `events.jsonl` 输出按 `reject_reason / reject_reason_label / execution_liquidity_grade / overextension_flag` 聚合的 JSON 与 Markdown 报告。
2. 执行质量当前已有 A+/A/B/C/D 五档与缩仓倍率，`spread / 盘口层级斜率 / 盘口撤单率` 已纳入 `execution_liquidity_grade`；下一步适合继续把这些输入改成真实 order book / websocket 驱动的实时采样，而非静态字段占位。
3. breakeven 结构确认当前已有 `breakeven_confirmation_mode`，下一步适合把 CVD、抬高低点、最小利润缓冲做成可配置多条件组合。
4. 组合暴露控制仍适合补 `narrative_bucket / correlation_group / portfolio_exposure_pct_by_theme`，把同主题仓位合并限额。
5. 单 symbol replay / backtest 框架仍值得落地，用同一事件契约回放 `candidate_selected -> entry_filled -> exit_reason` 全链路。
6. user data stream 已接入订单生命周期事件，listen key 续期/失败/断线基础监控已落地；下一步适合把 monitor cycle 真正挂进 auto-loop 主循环或独立 watcher，并补 listen key 续期失败告警、断线重连指标、以及 REST / WebSocket 成交差异对账。

## 2026-04-20 本轮验证结果
- 核对文件：`scripts/binance_futures_momentum_long.py`、`tests/test_strategy_v2.py`、`tests/test_strategy_v2_restore_regression.py`、`tests/test_run_loop_smoke.py`、`FULL_UPGRADE_TASKS_2026-04-16.md`。
- 重点验证：执行质量缩仓与 veto、事件契约扩展字段、成交回执持久化、user data stream 生命周期 smoke tests。
- 当前编译状态：`python -m py_compile scripts/binance_futures_momentum_long.py tests/test_run_loop_smoke.py tests/test_strategy_v2_restore_regression.py tests/test_strategy_v2.py` 已通过。
- 当前回归测试状态：`pytest -q tests/test_rejected_analysis.py tests/test_strategy_v2.py tests/test_strategy_v2_restore_regression.py tests/test_run_loop_smoke.py` 结果为 `64 passed`。
- 本轮已新增 rejected-analysis 聚合脚本 `scripts/rejected_analysis.py`，可直接读取 `runtime-state/events.jsonl`，输出 JSON 与 Markdown 报告，并按 `reject_reason / reject_reason_label / execution_liquidity_grade / overextension_flag / symbol` 聚合。
- 本轮已新增 `tests/test_rejected_analysis.py`，覆盖聚合逻辑与文件落盘链路。
- 风险守卫验证结果：`grade=C 且 depth<0.5` 时可追加 `candidate_execution_liquidity_poor`；纯滑点 veto 场景维持 `candidate_execution_slippage_risk` 单理由断言。
- 下一批优先级：推进执行质量二次升级，把 spread、盘口层级斜率、盘口撤单率纳入 `execution_liquidity_grade`，随后补组合暴露控制与单 symbol replay/backtest。
