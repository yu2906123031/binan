     1|---
     2|name: binance-futures-momentum-long
     3|description: |
     4|  基于“币安广场热币 + 涨幅榜异动”筛选 USDT 本位永续合约标的，
     5|  只做追多，按固定 USDT 风险自动反推仓位，并可选择实盘下单。
     6|  默认 dry-run，仅扫描与输出候选，不会直接下单。
     7|version: 1.6.0
     8|author: Hermes Agent
     9|license: MIT
    10|metadata:
    11|  tags: [binance, futures, momentum, breakout, trading, long-short]
    12|  exchange: binance
    13|  market: usdtm_perpetual
    14|  default_risk_usdt: 10
    15|---
    16|
    17|# Binance Futures Momentum LS
    18|
    19|## Surf Skill distilled rules from 424.pdf
    20|
    21|Use these rules first when modifying, running, or reviewing this Binance USDT-M futures strategy. They are distilled from `424.pdf` and the follow-up production fixes.
    22|
    23|1. **Live entry must verify exchange execution settings before any market order**
    24|   - Set margin mode before entry; default to `ISOLATED`.
    25|   - Set leverage before entry and hard-fail if the exchange response does not match requested leverage.
    26|   - Persist `margin_type`, `margin_type_check`, `leverage`, and `leverage_check` into entry events, live execution payloads, and `positions.json`.
    27|   - Treat leverage mismatch as a preflight hard gate, not a warning.
    28|
    29|2. **Small-account live profiles must cap notional exposure**
    30|   - For `10u-active` and `10u-aggressive`, keep `max_notional_usdt` around `500.0` unless the user explicitly overrides it.
    31|   - Keep sizing risk-first: quantity comes from `risk_usdt / risk_per_unit`, then cap by `max_notional_usdt`, then round by exchange step size.
    32|   - Do not increase trade frequency by raising risk; improve diagnostics and entry quality first.
    33|
    34|3. **High-elastic long entries need pullback and crowding checks**
    35|   - If a long candidate has strong 24h move, sharp 5m move, or obvious chase characteristics, do not let breakout alone mark it executable.
    36|   - Require pullback/retest quality: price should remain above VWAP while not being too far from VWAP/EMA.
    37|   - Reject or keep as non-triggered when taker buy pressure, long/short ratio, or funding show crowded long conditions.
    38|   - Keep the candidate visible for diagnostics, but set `setup_ready=False` / `trigger_fired=False` until the extra gates pass.
    39|
    40|4. **Keep the profitable protection structure**
    41|   - Preserve initial stop placement immediately after entry.
    42|   - Preserve staged take-profit planning (`TP1`, `TP2`, runner) and breakeven logic.
    43|   - Do not weaken protection checks to force more trades; missing protection remains a hard halt or repair path.
    44|
    45|5. **Make every no-trade decision observable**
    46|   - Maintain `early_rejected_stats` for early `build_candidate()` filters such as `long_breakout_not_confirmed`, `short_breakdown_not_confirmed`, `recent_5m_change_below_gate`, and `quote_volume_below_gate`.
    47|   - Keep hard veto reasons structured through `candidate_rejected` and `rejected_stats`.
    48|   - The dashboard should make no-trade reasons visible in Chinese so an operator can tell whether the system is waiting for structure, blocked by risk, or failing exchange preflight.
    49|
    50|6. **Before live runs, follow the safe sequence**
    51|   - Run tests after code changes: `python -m pytest`.
    52|   - Run reconcile before live: `python main.py --reconcile-only --halt-on-orphan-position --no-repair-missing-protection --output-format json`.
    53|   - Use one-position small-account live mode first: `python main.py --live --auto-loop --profile 10u-active --margin-type ISOLATED --max-open-positions 1 --max-long-positions 1 --max-short-positions 1 --poll-interval-sec 60 --output-format json`.
    54|   - Do not bypass API/IP/timestamp/preflight failures to start live trading.
    55|
    56|7. **Review priorities from the 424 replay**
    57|   - Fix execution drift before optimizing signal frequency.
    58|   - Concentrate risk on complete structures with clear stop and protection, not merely the hottest mover.
    59|   - For PEPE/DOGE-like high-beta longs, prefer secondary confirmation and pullback quality over direct chase entries.
    60|   - Keep XAU-like full protection and staged exit behavior as the model for acceptable execution quality.
    61|
    62|8. **OKX Agent Trade Kit signals should be upgraded before live signal fusion**
    63|   - OpenClaw users can ask the agent to update OKX trade-kit and focus on the `okx-market-filter` and `okx-sentiment-tracker` skills.
    64|   - CLI/MCP users should run `npm update -g okx-trade-mcp okx-trade-cli`; if the packages are not installed yet, install them with `npm install -g okx-trade-mcp okx-trade-cli`.
    65|   - The bridge also requires `mcporter`; install or update it before using `--okx-auto`.
    66|   - Prefer a read-only MCP command for Binance live signal fusion, for example `okx-trade-mcp --modules market,skills --read-only --no-log`.
    67|   - `--okx-auto` only works when paired with `--okx-mcp-command`; otherwise it returns no OKX payload.
    68|   - `okx-market-filter` should feed symbol selection, liquidity/volume, OI, funding, and sector resonance into `okx_sentiment_score`, `sector_resonance_score`, and `smart_money_flow_score`.
    69|   - `okx-sentiment-tracker` should feed sentiment direction and acceleration into `okx_sentiment_score` and `okx_sentiment_acceleration`.
    70|   - If OKX skills require credentials or are unavailable, keep Binance scanning alive and degrade to market-only or zero OKX scores instead of blocking the strategy.
    18|
## 当前已落地更新

## V1.6.0 双向升级文档（最新21条）

1. **所有交易对象全面 side-aware**
   - `Candidate`、`TradeManagementPlan`、`TradeManagementState`、`positions.json`、`events.jsonl`、`place_live_trade(...)`、`monitor_live_trade(...)`、`reconcile(...)` 全部增加 `side: Literal["long", "short"]`。
   - `Candidate` 保留 `entry`、`stop`、`quantity`、`recommended_leverage`、`score`、`reasons`、`state`、`alert_tier`，新增 `side`、`trigger_type`、`higher_timeframe_bias`、`overextension_flag`。
   - 风险单位统一改成 `risk_per_unit = abs(entry - stop)`，让多空共用一套表达。

2. **选币逻辑拆成两套策略语义**
   - Long 继续聚焦热币突破延续：热度、涨幅异动、5m/15m 上行动量、突破、高周期向上支持。
   - Short 独立聚焦弱币破位延续与反弹失败续跌：24h 跌幅异常、下行动量加速、跌破支撑或反弹碰 EMA/VWAP 后失败再下杀。
   - 文档与实现都按“long/short 两套 edge”组织，提升实盘一致性。

3. **扫描器升级为双引擎**
   - 新增 `scan_long_candidates()` 与 `scan_short_candidates()`。
   - 两个候选池分别打分、过滤、排序，再统一合并成总候选池。
   - Long 更重视热度、主动买盘、CVD 上行、OI 上升配合价格上推、15m/1h 趋势向上、突破后结构完整。
   - Short 更重视跌幅异常、主动卖盘、CVD 下行、OI 上升配合价格下压、15m/1h 趋势向下、跌破后无快速收回。

4. **指标解释全面方向化**
   - `OI`、`taker buy/sell`、`funding`、`long-short ratio`、`CVD`、`bandwidth` 都要按 `side` 解释。
   - Long 把“价涨 + OI 涨 + 主动买盘增强 + CVD 上行”视为正向确认。
   - Short 把“价跌 + OI 涨 + 主动卖盘增强 + CVD 下行”视为正向确认。
   - 拥挤度过高时，long 防多头末端过热，short 防 squeeze 与空头挤压。

5. **执行层改成 side-aware 下单与保护单**
   - `place_live_trade(candidate: Candidate, ...)` 由 `candidate.side` 决定开仓方向、保护单方向、`reduceOnly` 行为与 TP 方向。
   - Long：买开，保护单为卖出 stop。
   - Short：卖开，保护单为买入 stop。
   - 下单后统一写入 side-aware 的 `positions.json` 与 `events.jsonl`。

6. **仓位管理抽象为方向乘子**
   - 定义 `direction = 1 if side == "long" else -1`。
   - 统一判定：`risk_per_unit = abs(entry - stop)`、`reached(target) = direction * (price - target) >= 0`、`hit_stop(stop) = direction * (price - stop) <= 0`。
   - 这样 TP、stop、breakeven、runner 逻辑可复用同一套监控函数。

7. **TradeManagementPlan 原结构保留，语义升级为双向**
   - 保留 `entry_price`、`stop_price`、`quantity`、`initial_risk_per_unit`、`breakeven_trigger_price`、`tp1_trigger_price`、`tp1_close_qty`、`tp2_trigger_price`、`tp2_close_qty`、`runner_qty`。
   - 新增 `side`，所有 trigger 价格按方向生成。
   - Long：`entry + nR`；Short：`entry - nR`。

8. **持仓管理动作序列保持统一**
   - 统一动作仍为：`breakeven_moved` → `tp1_hit` → `tp2_hit` → `runner_exited`。
   - Long 在 TP 后上抬 stop。
   - Short 在 TP 后下压 stop。
   - 事件名保持统一，payload 通过 `side` 区分方向。

9. **runner trailing 逻辑扩展到 short**
   - Long 继续记录 `highest_price_seen`，`trailing_floor = highest_price_seen * (1 - trailing_buffer_pct)`。
   - Short 新增 `lowest_price_seen`，`trailing_ceiling = lowest_price_seen * (1 + trailing_buffer_pct)`。
   - Long 在跌破 floor 时退出 runner。
   - Short 在反弹突破 ceiling 时退出 runner。

10. **positions.json 升级为双向持仓账本**
   - 最少新增：`side`、`position_key`、`remaining_quantity`、`current_stop_price`、`moved_to_breakeven`、`tp1_hit`、`tp2_hit`、`highest_price_seen`、`lowest_price_seen`、`trade_management_plan`、`monitor_mode`。
   - `position_key` 推荐采用 `BTCUSDT:long` 这种形式，为未来 hedge mode 预留账本兼容性。

11. **风险控制从单边扩展到共享风控池**
   - 保留 `daily_max_loss_usdt`、`max_consecutive_losses`、`symbol_cooldown_minutes`、`max_open_positions`、orphan / protection 风控。
   - 新增 `max_long_positions`、`max_short_positions`、`max_net_exposure_usdt`、`max_gross_exposure_usdt`、`per_symbol_single_side_only`、`opposite_side_flip_cooldown_minutes`。
   - 第一阶段默认采用“同一 symbol 单侧持仓”模式。

12. **风险按市场环境与 side 做偏置**
   - 新增 `effective_risk = base_risk_usdt * regime_multiplier * side_multiplier`。
   - BTC/ETH 大级别偏强时，long 侧风险权重更高。
   - 大盘偏弱时，short 侧风险权重更高。
   - 震荡环境下，两边同时降风险并提升准入门槛。

13. **short 侧增加两类额外防守**
   - 防瀑布末端追空：当价格对 EMA/VWAP 负偏离过大、近 3-5 根 5m 已大跌、单根爆量长阴刚出现、或 OI 开始下掉时，降低分数或直接 veto。
   - 防 short squeeze：当 funding 过负、crowding 偏空、长下影快速收回时，降低 short 优先级。

14. **事件系统保持统一命名，payload 补 side**
   - `entry_filled`、`protection_confirmed`、`breakeven_moved`、`tp1_hit`、`tp2_hit`、`runner_exited`、`candidate_rejected`、`trade_invalidated` 保持统一。
   - 事件 payload 最少补齐 `symbol`、`side`、`price`、`quantity`、`reason`、`position_key`。
   - 这样统计与回放可以跨方向统一分析。

15. **最小改造路线固定为三阶段**
   - 第 1 阶段：双向交易上线，且同一 symbol 只允许单侧持仓。
   - 第 2 阶段：加入市场 regime 偏置，动态压缩多空不同 side 的风险。
   - 第 3 阶段：只有在明确需要时再升级 hedge mode。

16. **V3 命名建议与系统定义**
   - 当前 long-only 形态升级命名建议为 `binance-futures-momentum-ls`。
   - 运行描述更新为：Binance USDT-M 永续双向动量系统，long 做热币突破延续，short 做弱币破位延续 / 反弹失败续跌，统一使用固定 USDT 风险建仓，并由 side-aware 持仓管理与共享风控约束。

17. **本次升级的四个核心改造块**
   - 候选扫描拆成 long/short 双引擎。
   - 执行与保护单改成 side-aware。
   - `monitor_live_trade()` 改成方向抽象。
   - 风险控制从单边扩展为共享暴露管理。

18. **方向风险倍率与标准化输出打通**
   - 新增 `derive_side_risk_multiplier(side, regime_label)`，按市场状态给多空方向单独风险倍率：`risk_on` 偏多 1.15 / 偏空 0.85，`risk_off` 偏多 0.85 / 偏空 1.15，`caution` 双向 0.90，`neutral` 维持 1.00。
   - `recommended_position_size_pct()` 扩展为同时接收 `regime_multiplier` 与 `side_multiplier`，基础仓位建议直接体现“市场状态 × 方向偏置”双层缩放。
   - `Candidate` 新增 `side_risk_multiplier` 字段；`run_scan_once()` 在市场状态判定后写入 `candidate.side_risk_multiplier`，并把 `market_regime_multiplier=`、`side_risk_multiplier=` 追加进 reasons。
   - `build_standardized_alert()` 输出新增 `side_risk_multiplier`，`base_position_size_pct` 与 `position_size_pct` 保持和新版基础仓位建议一致，标准化告警可直接回放真实 sizing。
   - 测试新增 CLI 倍率单测、方向倍率单测、标准化告警透传单测；同步校正 `run_scan_once` 仓位断言与 execution reject 聚合断言，确保双向倍率和 reject 统计逻辑一致。

19. **主题暴露与相关性暴露风控已补齐回归测试**
   - `default_risk_state()`、`load_risk_state()`、`evaluate_risk_guards()` 当前已统一维护 `portfolio_exposure_pct_by_theme` 与 `portfolio_exposure_pct_by_correlation` 两个共享桶。
   - 当候选携带 `portfolio_narrative_bucket` / `portfolio_correlation_group`，且新增仓位会触发 `max_portfolio_exposure_pct_per_theme` 或 `max_portfolio_exposure_pct_per_correlation_group` 上限时，风险层会稳定返回 `candidate_portfolio_theme_overexposure` 与 `candidate_portfolio_correlation_overexposure`。
   - 回归测试已覆盖 theme / correlation 双命中场景，后续继续扩展组合暴露时可以直接沿用当前 reject label 体系。

20. **方向倍率公式已有独立断言锁定**
   - `derive_side_risk_multiplier()` 当前行为已通过独立单测锁定：`risk_on` 偏多、`risk_off` 偏空、`caution` 双向收缩、`neutral` 维持基线。
   - `recommended_position_size_pct()` 的最终输出已通过组合倍率单测锁定，确保 tier 基础仓位会乘上 `regime_multiplier × side_multiplier`。
   - `build_standardized_alert()` 也已补充 `market_regime_multiplier` 断言，标准化输出与扫描期 sizing 保持同一口径。

21. **本轮升级验证状态**
   - 当前回归集 `tests/test_strategy_v2.py tests/test_portfolio_risk_guards.py tests/test_cli_args.py tests/test_rejected_analysis.py` 已全绿，结果为 `49 passed`。
   - 本轮新增覆盖点集中在方向倍率、标准化输出、主题/相关性暴露风控，文档与测试口径已同步到最新状态。

## V1.6.0 推荐升级顺序

1. **先改文档与对象契约**
   - 先把 `Candidate`、`TradeManagementPlan`、`positions.json`、`events.jsonl`、`place_live_trade()`、`monitor_live_trade()`、`reconcile()` 的 side-aware 契约写清楚。

2. **再落第 1 阶段代码**
   - 目标：支持 long + short，下单、止损、账本、事件、监控都能识别 `side`，且同一 symbol 只允许单侧持仓。
   - 第一阶段实现约定固定为双字段：`side` 存交易语义 `long|short`，`position_side` 存 Binance 执行/账本语义 `LONG|SHORT`。
   - `positions.json` 与 `events.jsonl` 必须同时落这两个字段，并始终补齐 `position_key = SYMBOL:POSITION_SIDE`，这样扫描层、执行层、运行时事件、reconcile 才能共用统一主键。
   - 2026-04 新增一条关键约束：运行时持久化与事件补全函数必须显式做大小写与语义归一化。`normalize_position_side()` 的输入与默认值都要稳定收敛到 `LONG/SHORT`；`monitor_live_trade()` 构造 `TradeManagementPlan` / `TradeManagementState` 时必须同时传入 `side` 与 `position_side`，避免 short 仓因为 dataclass 默认值回退成 `LONG`。
   - `RuntimeStateStore.append_event()` / `normalize_runtime_event_payload()` 在回填 `events.jsonl` 时要优先保住 `position_key` 与 `position_side` 的一致性；当事件主要用于恢复、对账、回放与 side-aware 断言时，`row['side']` 也应稳定写成仓位方向值 `LONG/SHORT`，这样短仓事件会持续落在正确方向桶里。

3. **完成后先跑 scan-only 验证**
   - 优先验证 long/short 双池扫描、候选排序、risk guard、positions.json 字段与事件落盘。

4. **再接 live 小仓位验证**
   - 先用极小风险验证 long 开仓、short 开仓、保护单方向、reconcile、runner 状态记录。

2026-04 最新已落地一批第一阶段优化：
    22|- `candidate_rejected` 事件已接入 live risk guard 与 `max_open_positions` 拦截分支，事件会带 `symbol` 与 `reasons`
    23|- `candidate_rejected` 现已补齐 `reject_reason`、`reject_reason_label`、`expected_slippage_r`、`execution_liquidity_grade`，便于执行质量归因与拒绝分桶统计
    24|- 共享暴露风控第 1 批参数已真正落地到 CLI 与 live 入口：`--max-long-positions`、`--max-short-positions`、`--max-net-exposure-usdt`、`--max-gross-exposure-usdt`、`--per-symbol-single-side-only` / `--allow-symbol-hedge`、`--opposite-side-flip-cooldown-minutes`
    25|- live 入口当前会先抓取 `open_positions`，再把 `evaluate_risk_guards()` 与 `evaluate_portfolio_risk_guards()` 结果合并写入 `cycle['risk_guard']`；命中共享暴露限制时同样会落 `candidate_rejected` 事件
    26|- `evaluate_portfolio_risk_guards()` 读取 Binance 持仓暴露优先走 `notional`、`positionAmt`、`entryPrice` / `markPrice` 链路；写测试样本时用 `notional` 比 `notional_usdt` 更贴近真实输入
    27|- 新增组合风控测试时，若通过 `importlib.util.module_from_spec()` + `exec_module()` 单独导入策略脚本，需要先执行 `sys.modules[spec.name] = module`，这样 dataclass + postponed annotations 才能稳定初始化
    24|- `build_candidate()` 已写入 Candidate 三层字段：`must_pass_flags`、`quality_score`、`execution_priority_score`
    25|- `build_candidate()` 已写入 `entry_distance_from_breakout_pct`、`entry_distance_from_vwap_pct`、`candle_extension_pct`、`recent_3bar_runup_pct`、`overextension_flag`
    26|- `build_candidate()` 已写入 `setup_ready`、`trigger_fired`、`expected_slippage_pct`、`book_depth_fill_ratio`
    27|- `build_candidate()` / 执行质量链路已新增 `spread_bps`、`orderbook_slope`、`cancel_rate` 三个微观结构字段，并纳入 `execution_liquidity_grade` 计算
    28|- `build_standardized_alert()` 已对外暴露追价距离、过热、两段式入场、执行质量字段；当前额外包含 `expected_slippage_r`、`execution_liquidity_grade`、`spread_bps`、`orderbook_slope`、`cancel_rate`
    29|- `monitor_live_trade()` 已补齐 `trade_invalidated` 事件与 `exit_reason` 持久化
    30|- `TradeManagementPlan` / 管理动作链路已支持 `breakeven_confirmation_mode`
    31|- breakeven stop 替换流程与 lifecycle event 契约已有回归测试覆盖
    32|
    33|## 当前结构诊断与优先优化方向
    34|
    35|按 2026-04 当前代码实测，主策略已经具备“候选扫描 + 一票否决 + alert tier + 初始止损 + 基础 runtime event”的骨架，下一阶段最值钱的优化顺序如下：
    36|
    37|1. 先优化“别买错”
    38|   - `build_candidate()` 目前已经同时计算 setup / exhaustion / microstructure / sentiment / regime 因子，`apply_hard_veto_filters()` 也已有 distribution、negative CVD、OI 反转、smart money outflow 等一票否决。
    39|   - 下一步建议把候选准入显式拆成三层，并在代码/日志里分别落字段：
    40|     - `must_pass`：必要条件层，失败直接淘汰
    41|     - `quality_score`：质量评分层，决定能不能进
    42|     - `execution_priority`：执行优先级层，多个候选时排序
    43|   - 这样可以把“走势可能继续涨”和“当前入场质量适合追”分开，减少单一总分把高热尾段顶上来的噪音。
    44|
    45|2. 先做过热惩罚与追价距离限制
    46|   - 当前已有 `distance_from_ema20_5m_pct`、`distance_from_vwap_15m_pct`、`state in {momentum_extension, overheated}` 与 `extended_chase_veto`，基础方向正确。
    47|   - 下一步建议新增标准化字段并直接参与 veto / candidate_rejected：
    48|     - `entry_distance_from_breakout_pct`
    49|     - `entry_distance_from_vwap_pct`
    50|     - `candle_extension_pct`
    51|     - `recent_3bar_runup_pct`
    52|     - `overextension_flag = none / mild / high`
    53|   - 执行规则建议：
    54|     - 超过最大追价距离直接放弃
    55|     - 近 3-5 根 5m 总涨幅过大时降级为 `watch` 或直接 veto
    56|     - 单根爆量长阳后的首根追单默认进入二次确认模式
    57|
    58|3. 把入场升级成“两段式”
    59|   - 当前 `build_candidate()` 基本属于单段式：满足 breakout + 量价 + 高周期趋势后直接产出 candidate。
    60|   - 下一步建议新增两层状态：
    61|     - `setup_ready`
    62|     - `trigger_fired`
    63|   - 推荐 trigger 模板：
    64|     - 突破后 1-2 根内收盘站稳 breakout level 上方
    65|     - 突破后回踩不破再收回
    66|     - CVD / taker buy / OI 二次确认继续增强
    67|   - 这样会显著降低“刚碰一下就假突破”的入场。
    68|
    69|4. 滑点与盘口深度预检查优先级很高
    70|   - 当前仓位反推基于 `risk_usdt / (entry - stop)`，适合理论风险控制。
    71|   - 下一步建议在真实下单前新增：
    72|     - `expected_slippage_pct`
    73|     - `expected_slippage_r`
    74|     - `book_depth_fill_ratio`
    75|     - `execution_liquidity_grade = A / B / C`
    76|   - 推荐规则：
    77|     - `expected_slippage_r > 0.15 ~ 0.25` 时放弃或缩仓
    78|     - 深度不足时优先降 size，再决定是否 veto
    79|
    80|5. 止损要继续沿“结构 + 波动”双约束推进
    81|   - 当前已接入 `swing low + ATR(14)*1.5`，属于正确基础版。
    82|   - 下一步建议把 stop 选择理由显式记录到 candidate / event：
    83|     - `stop_model = structure / atr / blended`
    84|     - `stop_too_tight_flag`
    85|     - `stop_too_wide_flag`
    86|   - 交易准入建议增加：当 stop 太宽导致 R 结构失真时，直接不做。
    87|
    88|6. breakeven 逻辑要从“价格到点”升级成“价格到点 + 结构确认”
    89|   - 当前 `evaluate_management_actions()` 在 `current_price >= plan.breakeven_trigger_price` 时直接 move stop to breakeven。
    90|   - 下一步建议叠加至少一个确认条件：
    91|     - 收盘仍站稳 1R 上方
    92|     - CVD / taker buy 没有明显衰减
    93|     - 5m 抬高低点成立
    94|     - 距离 entry 已有最小缓冲
    95|   - 这类改动通常能直接减少“刚提保本就被洗掉，然后继续涨”的问题。
    96|
    97|7. 组合暴露控制要进入主流程
    98|   - 当前已有 `daily_max_loss_usdt`、`max_consecutive_losses`、`symbol_cooldown_minutes`、`max_open_positions`。
    99|   - 下一步建议新增：
   100|     - `narrative_bucket`
   101|     - `correlation_group`
   102|     - `portfolio_exposure_pct_by_theme`
   103|     - `market_regime_risk_multiplier`
   104|   - 实际规则建议：
   105|     - 同叙事只保留最优 1-2 个
   106|     - BTC / ETH 走弱时对 alt long 全局降风险
   107|     - 假突破密集期自动抬高准入门槛
   108|
   109|8. 事件与 reasons 字段标准化要尽快做
   110|   - 当前 runtime 事件已有 `entry_filled`、`protection_confirmed`、`breakeven_moved`、`tp1_hit`、`tp2_hit`、`runner_exited`。
   111|   - 下一步强烈建议补齐两类事件：
   112|     - `candidate_rejected`
   113|     - `trade_invalidated`
   114|   - 并补充结构化字段：
   115|     - `trend_regime = strong_up / weak_up / chop`
   116|     - `entry_pattern = breakout / reclaim / pullback_break`
   117|     - `overextension_flag = none / mild / high`
   118|     - `liquidity_grade = A / B / C`
   119|     - `exit_reason = stop / breakeven / tp1_tp2_runner / manual / protection_fail`
   120|   - 有了这些字段，后续才能稳定做 rejected analysis、执行质量归因、回测分桶统计。
   121|
   122|## 推荐实施顺序
   123|
   124|### 第一阶段：先减少假突破和末端追高
   125|
   126|推荐按下面顺序落地：
   127|
   128|1. `candidate_rejected` / `trade_invalidated` 标准化事件
   129|2. 最大追价距离与过热惩罚字段
   130|3. 两段式入场 `setup_ready -> trigger_fired`
   131|4. 滑点 / 深度预检查
   132|5. 更严格的 breakeven 触发条件
   133|
   134|这一阶段的目标是优先改善：
   135|- 假突破止损率
   136|- 热币末端追高
   137|- 实盘执行偏差
   138|- 连续亏损环境下的回撤斜率
   139|
   140|### 第二阶段：提升持仓管理与组合层生存能力
   141|
   142|1. `stop_model` 与 stop 合理性约束
   143|2. TP / runner 按 regime 做动态模板
   144|3. trailing 从单一 `highest_price_seen` 升级为 `ema / atr / structure` 多模型
   145|4. narrative / correlation exposure cap
   146|5. 假突破环境识别后自动降风险
   147|
   148|### 第三阶段：进入可迭代系统
   149|
   150|1. 建最小 replay / backtest 框架
   151|2. 把信号质量与执行质量拆开统计
   152|3. 输出 MFE / MAE / TP1 到达率 / TP2 到达率 / runner 贡献
   153|4. 对不同 filter、state、entry pattern、market regime 做分桶评估
   154|
   155|## 面向代码的具体改造建议
   156|
   157|### Candidate 层
   158|
   159|- 为 `Candidate` 增加：
   160|  - `must_pass_flags`
   161|  - `quality_score`
   162|  - `execution_priority_score`
   163|  - `entry_distance_from_breakout_pct`
   164|  - `entry_distance_from_vwap_pct`
   165|  - `candle_extension_pct`
   166|  - `recent_3bar_runup_pct`
   167|  - `overextension_flag`
   168|  - `entry_pattern`
   169|  - `trend_regime`
   170|  - `liquidity_grade`
   171|
   172|### Scan / veto 层
   173|
   174|- 在 `run_scan_once()` 中对每个被淘汰 candidate 记录 `candidate_rejected`，payload 至少包含：
   175|  - `symbol`
   176|  - `reject_reason`
   177|  - `state`
   178|  - `score`
   179|  - `quality_score`
   180|  - `execution_priority_score`
   181|  - `overextension_flag`
   182|  - `market_regime_label`
   183|- 对 `apply_hard_veto_filters()` 返回值做枚举化维护，确保每个 veto 原因都可统计。
   184|
   185|### Entry / execution 层
   186|
   187|- 在 live 下单前加入：
   188|  - breakout 距离校验
   189|  - VWAP 距离校验
   190|  - orderbook 深度检查
   191|  - 预估滑点检查
   192|- 对 setup / trigger 采用显式状态机，避免 candidate 一生成就直接下单。
   193|
   194|### Management 层
   195|
   196|- 扩展 `TradeManagementPlan`：
   197|  - `breakeven_confirmation_mode`
   198|  - `regime_profile`
   199|  - `trailing_mode`
   200|  - `exit_reason`
   201|- 扩展 `evaluate_management_actions()`：
   202|  - BE 触发加入结构确认
   203|  - trailing 支持 `ema / atr / structure`
   204|  - 区分趋势延续型与消息脉冲型模板
   205|
   206|### Analytics 层
   207|
   208|- runtime event 建议统一保留：
   209|  - `signal_quality_bucket`
   210|  - `execution_quality_bucket`
   211|  - `mae_r`
   212|  - `mfe_r`
   213|  - `max_heat_pct`
   214|  - `realized_r`
   215|- 后续回放框架优先复用 `events.jsonl` / `trade_log.jsonl` 契约，减少二次迁移成本。
   216|
   217|## 测试与验证补充
   218|
   219|后续每次改造建议把测试分成四层：
   220|
   221|1. 单元测试
   222|   - `apply_hard_veto_filters()`
   223|   - `classify_candidate_state()`
   224|   - `build_trade_management_plan()`
   225|   - `evaluate_management_actions()`
   226|2. 策略回归测试
   227|   - 保证现有 V2 候选排序与 alert tier 关键样例稳定
   228|3. 事件契约测试
   229|   - 校验 `candidate_rejected`、`trade_invalidated`、`exit_reason` 字段完整
   230|4. 单 symbol 回放测试
   231|   - 用固定 K 线样本回放 `setup -> trigger -> manage -> exit`
   232|
   233|## 当前最值得先做的 8 个优化
   234|
   235|1. 过热过滤增强
   236|2. 两段式入场确认
   237|3. 最大追价距离限制
   238|4. 滑点 / 深度预检查
   239|5. breakeven 触发更严格
   240|6. 组合相关性暴露控制
   241|7. 假突破环境识别后自动降风险
   242|8. `candidate_rejected` / `exit_reason` 标准化落盘
   243|
   244|## 作用
   245|
   246|把“刷币安广场热币做多突破延续 + 刷弱币做空破位延续/反弹失败续跌，按固定 USDT 风险建仓”落成一个本地可执行脚本。
   247|
   248|脚本能力：
   249|- 支持做多与做空
   250|- 扫描 USDT 本位永续合约
   251|- 支持运行参数预设：
   252|  - `--profile 10u-aggressive`：10U 小资金激进模式
   253|- 综合：
   254|  - 币安广场热币列表（手动传入，或尝试从公开页面抓取）
   255|  - 24h 涨幅榜
   256|  - 5m 突破与短时动量
   257|- 用固定风险金额反推仓位大小
   258|- 默认只扫描，不实盘下单
   259|- 开启 `--live` 后，会提交真实订单并尝试挂保护性止损；但按 2026-04 的实测，`monitor_live_trade()` 与 `start_trade_monitor_thread()` 仍是占位实现，所以“自动分批止盈 / 保本移动止损 / runner 尾仓退出”目前还未真正自动执行，不能把它当成已完成的实盘管理器
   260|- Binance Futures 当前 `STOP_MARKET` 保护单应走 algo endpoint；如果沿用旧 endpoint，可能报错：`Order type not supported for this endpoint. Please use the Algo Order API endpoints instead.`
   261|- 实盘通知时序应确保：市价开仓成交后立即发送 `entry_filled`，不要等到整笔交易链路（含止损挂单）全部成功后再发，否则会被后续异常短路
   262|- `--auto-loop` 当前仍未真正形成常驻外层轮询主循环：按 2026-04 实测，`main()` 只调用一次 `run_loop()`，而 `run_loop()` 也只执行单轮 reconcile + scan + 可选 live entry，进程打印单次结果后即退出；当前更准确的定位是“单次扫描/单次触发实盘”，距离“固定轮询间隔持续扫描并自动开单”的常驻执行器还差外层循环实现
   263|- 支持 `--max-open-positions` 控制总持仓上限；例如设为 `4` 时，低于 4 个活动仓位会继续找新机会，达到上限才停止继续开新仓
   264|- 为了真正支持多仓，文档曾描述 `--auto-loop --live` 下的新仓持仓管理已改为后台线程启动；但按 2026-04 实测，后台线程入口目前仍是空线程占位，不能据此假设已具备真实多仓托管能力
   265|- 已新增 runtime state 持久化目录 `--runtime-state-dir`（默认 `~/.hermes/binance-futures-momentum-long/runtime-state`），会落地 `positions.json`、`risk_state.json`、`trade_log.jsonl` 等状态文件，供重启恢复与事后审计使用
   266|- 启动时会先做一次 reconcile：把交易所现有仓位与本地状态账本对齐，识别 orphan 仓位与“缺少保护单”的仓位
   267|- 当 `--halt-on-orphan-position` 触发暂停时，会写入 `risk_state.json` / `trade_log.jsonl`，并额外发送 `strategy_halted` 告警通知，带出 `halt_reason`、orphan 仓位、缺保护仓位摘要
   268|- reconcile 默认开启“保护单缺失自动补挂修复器”：对已跟踪、仍持仓、且本地保留了 `stop_price` 的仓位，会自动补挂 replacement `STOP_MARKET`，并把结果写入 `reconcile.protection_repairs` 与 `positions.json.stop_order_id`
   269|- 需要保守模式时可加 `--no-repair-missing-protection`，此时保留原有 `protection_missing` 告警与 halt 语义
   270|- 若启动 reconcile 未完成修复且仍发现已有仓位缺少保护单，会写入 runtime event，并发送 `protection_missing` 告警通知
   271|- 处理开仓后保护单确认时，不能把“无保护单”一律视为异常；必须同时查询该 symbol 当前是否仍有活动持仓。若仓位已被快速平掉/归零，应标记为 `flat` 而不是 `missing`，避免把已平仓状态误报成 `missing protection`
   272|- 已新增账户级状态风控开关：`--daily-max-loss-usdt`、`--max-consecutive-losses`、`--symbol-cooldown-minutes`、`--halt-on-orphan-position`、`--reconcile-only`
   273|- 候选打分与状态识别已升级为 V2：支持 `CVD` / `CVD z-score`、short squeeze（负 funding + OI 上升 + 正 CVD）、distribution（价格强但 CVD 转负）与 overheated 过滤
   274|- V2 微观结构补强：`compute_relative_oi_features()` 现已把 `oi_zscore_5m`、`volume_zscore_5m`、`bollinger_bandwidth_pct`、`price_above_vwap`、`oi_notional_percentile` 一并纳入；经验上不要再用固定 OI 百分比做主判据，而应优先看 7d/近期样本上的相对异常值与 OI notional 所处历史分位，才能更稳地识别“真异动 vs 日常高波动”
   275|- 候选评分已预留并接入扩展因子：`okx_sentiment_score`、`okx_sentiment_acceleration`、`sector_resonance_score`、`smart_money_flow_score`；并统一走 `compute_sentiment_resonance_bonus()` 聚合加减分，遇到聪明钱明显流出时会同时扣分并压缩正向 bonus
   276|- `build_candidate()` 当前同时保留 `smart_money_flow_score` 原始交易所/OKX 输入与 `smart_money_effective` 合并后风险判定：标准化 alert 与外部通知字段继续输出原始 `smart_money_flow_score`，控盘/派发风控层使用 exchange + onchain 合并值，便于区分展示值与风控值
   277|- 情绪信号新增“提前量”层：`compute_leading_sentiment_signal()` 会优先奖励“情绪仍偏冷/中性但加速度突然转强”的 early-turn 结构，并对 `okx_sentiment_score >= 0.75` 的过热状态附加负分；经验上不要等 OKX 情绪已经很热才给高分，否则更容易落入接力尾段
   278|- 已新增 `compute_squeeze_signal()` 与 `compute_control_risk_score()` 两层：前者把 `negative funding + short_bias + oi_zscore + 正向 CVD` 聚合为逼空优先级分数，后者把 `oi_notional_percentile` 过高、`short_bias` 过低、`smart_money_flow_score` 明显转负 统一折算成控盘/派发风险分；两者都会直接进入 `build_candidate()` 的总分模型
   279|- `run_scan_once()` 现已真正支持从 `--okx-sentiment-inline` / `--okx-sentiment-file` 读取 OKX 情绪输入，格式为 `SYMBOL|sentiment|acceleration|sector|smart_money`（也兼容逗号分隔）；主扫描会把这 4 个值注入 `build_candidate()`，不再只是保留字段但未串主流程
   280|- 已新增 OKX 自动联动入口：`--okx-auto`、`--okx-sentiment-command`、`--okx-mcp-command`、`--okx-sentiment-timeout`。扫描时会先合并手工 inline/file 情绪，再尝试从命令输出或 `okx-trade-mcp` 自动拉取并覆盖同 symbol 的最新情绪值
   281|- OKX 自动输入解析已扩展为同时支持：文本行 `SYMBOL|sentiment|acceleration|sector|smart_money`、逗号分隔、JSON 行、以及 `data: {...}` 形式的 SSE 行；如果外部命令 stdout/stderr 返回 JSON 嵌套对象，也会递归提取 `symbol/instId`、`sentiment(_score)`、`acceleration`、`sector_score`、`smart_money_flow` 等字段
   282|- 已新增桥接脚本 `/root/.hermes/okx_sentiment_bridge.py`：通过 `mcporter` 调 `okx-trade-mcp` 的 `market_filter`、`market_filter_oi_change`、`news_get_sentiment_ranking`、`news_get_coin_sentiment`，输出主策略可直接消费的 `SYMBOL|okx_sentiment_score|okx_sentiment_acceleration|sector_resonance_score|smart_money_flow_score` 行格式；适合作为 `--okx-sentiment-command` 的直接数据源
   283|- 经验结论：当前机器上 OKX 的 news/sentiment 工具调用可能报 `Private endpoint requires API credentials.`。因此桥接脚本必须允许“新闻层失败但 market/OI 层继续输出”的降级路径，避免整个 Binance 扫描因为 OKX 私有新闻接口缺凭证而中断；此时 `okx_sentiment_score` 可能为 0，但 `sector_resonance_score` 与 `smart_money_flow_score` 仍可由 OI / 价格 / funding 微结构代理信号生成
   284|- OKX `okx-trade-mcp` 的私有接口校验遵循“三件套”规则：即使 API key 只是 `读取` 权限，也必须同时提供 `OKX_API_KEY`、`OKX_SECRET_KEY`、`OKX_PASSPHRASE`。若 passphrase 为空字符串或缺失，`mcporter` 调用通常会先返回 `ConfigError: Partial API credentials detected.`，随后 MCP 连接直接关闭并表现为 `MCP error -32000: Connection closed`；因此在接 OKX sentiment/news/account 类私有能力前，必须先确认用户手里不仅有 key/secret，还有创建该 key 时自定义的 passphrase。
   285|- 主扫描新增 `apply_hard_veto_filters()` 一票否决层：若出现 `distribution`、`exhaustion_score >= setup_score + 12 且 cvd_delta <= 0`（`distribution_blacklist`，用于拦截“状态未正式切到 distribution 但已经明显派发/衰竭”的高位假启动）、`CVD < 0 且 cvd_zscore <= -2.5`、`oi_change_pct_5m < 0`、`24h涨幅>=15% 且 state∈{chase,momentum_extension,overheated}`、或 `smart_money_flow_score <= -0.35`，则直接不进入候选池；这一步优先于 market regime 乘数与 alert tier 计算，用来拦截假突破/高位接力/聪明钱派发
   286|- 已新增 `compute_market_regime_filter()`，可基于 BTC/SOL K 线判断 risk-on / risk-off，并产出 `score_multiplier` 与原因标签（如 `btc_trend_down`、`sol_momentum_breakdown`），现已真正串入 `run_scan_once()` 主扫描：会对 candidate 分数做统一乘数修正，并回写 `regime_label` / `regime_multiplier`；经验上 BTC 应比 SOL 权重大：趋势下破时建议 BTC 乘数更重（如 `*0.7`）而 SOL 稍轻（如 `*0.8`），并额外检查近 4 根收盘的短线动量破位（BTC 约 `<= -2%`、SOL 约 `<= -3%`）生成 `btc_momentum_breakdown` / `sol_momentum_breakdown`。若双杀或双动量破位，应直接打成 `risk_off` 并把总乘数钳制到 `<=0.55`；若只坏一边，则至少 `caution` 且总乘数钳制到 `<=0.85`
   287|- 主扫描现已新增标准化预警输出：每个 candidate 会附带 `alert_tier`、`position_size_pct`，并在返回结果中额外提供 `selected_alert`、`candidate_alerts` 与等价的 `candidates` 键，便于直接接 Telegram/微信群通知、webhook 或自动下单模块
   288|- 新增稳定集成约定：外部热币源优先采用“双输出”——一份纯 symbol 文件给现有 `--square-symbols-file` / 兼容旧链路，一份结构化 `external_top_symbols.json` 给后续 richer ranking / metadata 扩展。经验上这是兼顾兼容性与可演进性的最稳方案。
   289|- `run_scan_once()` 当前会把 `market_regime_multiplier=...`、`market_regime:<reason>`、`alert_tier=...`、`position_size_pct=...` 稳定回写到 candidate reasons，方便回归测试与通知层直接复用
   290|- 外部 JSON 输出契约建议至少包含：`symbols`（按优先级排序的 symbol 数组）、`source`、`generated_at`、`top_count`、`top_symbols`。其中 `top_symbols` 里的每一项建议保留 `rank`、`symbol`、`priceChangePercent`、`marketCap`、`openInterestUsd`、`topHoldersPct`、`topHoldersLongPct`、`stablecoinPair`、`contractType`、`specialTags`、`topHolderAddresses` 等字段，方便策略侧平滑升级为 richer filter。
   291|- 预警分级经验：`risk_off` 下即便原始分数较高，也应把 `alert_tier` 置为 `blocked` 且 `position_size_pct=0.0`，避免把“大盘坏掉时的漂亮局部走势”误发成可执行信号
   292|- 新的 alert tier 风控约束：`distribution` 必须直接 `blocked`；`momentum_extension` 不能再升到 `high/critical`，即使分数很高也应降为 `watch`（低分时可直接 `blocked`）；`overheated` 也只能 `watch/blocked`。这一步的经验来源于“已经拉过/高位派发仍被高分误报”的实盘噪音，需要在 `classify_alert_tier()` 层做二次约束，而不是只依赖原始 score
   293|- 仓位输出一致性经验：`recommended_position_size_pct()` 必须显式处理 `blocked -> 0.0`，避免上游 tier 扩展后默认分支意外保留旧仓位值
   294|- 标准化预警输出的验证重点：`build_standardized_alert()` 除基础字段外，应稳定包含 `position_size_pct`、`atr_stop_distance`、`market_regime_label`、`market_regime_multiplier`，以及 OKX 情绪字段（尤其 `okx_sentiment_acceleration`），这样 Telegram/微信群预警才能直接复用为“可执行交易卡片”而无需二次拼装
   295|- 止损已支持 ATR 动态距离：`build_candidate()` 会计算 `atr_stop_distance = ATR(14) * 1.5` 并可传给 `build_trade_management_plan()`，避免妖币固定百分比止损过于容易被插针
   296|- V2 状态机维护经验：`launch` 不应只因价格/量能满足就触发，至少还要有正向 OI 异动（如 `oi_zscore_5m > 0`）；`chase` 也应要求 `oi_zscore_5m > 0` 或 `cvd_delta > 0` 之一，否则在缺失微观结构确认时应保持 `none`
   297|- V2 输出一致性经验：当最终 `state == 'none'` 时，应清空 `state_reasons`，避免把 `price_above_vwap` 之类的准备信号误当成已成立状态标签
   298|- 当前实测可确认：扫描、候选打分、OKX bridge 情绪注入、真实开仓、初始保护止损、reconcile 对账、`strategy_halted` / `protection_missing` / `scan_alert` 通知都可用；但自动保本、TP1/TP2、runner、后台持仓监控仍未完成实现
   299|- 维护/重构该 skill 下脚本时，先做文件级备份（例如复制到 `/tmp/` 或同目录 `.bak`）再执行批量改写；本 skill 目录不是 git 仓库，误用整文件覆写时无法靠 `git diff/checkout` 恢复。特别是对 `scripts/binance_futures_momentum_long.py` 这类大文件，写入后必须立即用 `py_compile` 或导入检查确认核心入口（如 `build_candidate`、`run_scan_once`、`main`）仍存在，避免把策略主干意外截断后才继续后续修改。
   300|- 通知目标支持 Telegram 与企业微信/微信客服通道（`weixin:<chat_id>`）
   301|- `--notify-target` supports comma-separated multi-target delivery; example: `telegram:<chat_id>,weixin:<chat_id>`
   302|
   303|脚本路径：
   304|`/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py`
   305|
   306|## 默认策略逻辑
   307|
   308|1. 候选池来源：
   309|   - `--square-symbols` 传入的热币
   310|   - 或尝试从 Binance Square 首页 “Most Searched (6H)” 抓取
   311|   - 同时叠加期货 24h 涨幅榜前 N 名
   312|2. 排序：
   313|   - 热度权重 40% + 涨幅榜权重 60%
   314|   - 热币与涨幅榜交集优先
   315|   - 默认只取前 5-8 个进入深度检查
   316|3. 开仓条件（v2 强化版）：
   317|   - 当前价突破近 N 根 5m K 线高点
   318|   - 最近 5m 涨幅超过阈值
   319|   - 5m 成交量 > 过去 20 根均量的指定倍数（默认 1.8x）
   320|   - 5m RSI(14) 低于阈值（默认 < 80）
   321|   - 5m MACD 柱状图继续扩大
   322|   - 价格突破近期小级别结构高点
   323|   - 1h 或 4h 趋势向上（价格在 EMA20 上方且 MACD 不弱于 0 轴）
   324|   - 当前价距离 5m EMA20 / 15m VWAP 不可过远，避免追顶
   325|   - Funding Rate 与近 3 次 funding 平均值不可过热
   326|   - 24h 成交额必须达到阈值（默认 5000 万 USDT）
   327|   - 额外状态层会用 `classify_candidate_state()` 标记 `launch` / `chase` / `squeeze` / `distribution` / `overheated`
   328|   - 若能提供 OI/CVD/情绪数据，优先关注：
   329|     - `short squeeze`：OI 快升 + 价格上涨 + CVD 为正 + funding 偏负
   330|     - `distribution`：价格继续冲高但 CVD 转负
   331|     - `sentiment_sector_alignment`：OKX 情绪升温 + 板块共振 + 聪明钱未流出
   332|4. 仓位计算：
   333|   - `仓位数量 = 风险USDT / (入场价 - 止损价)`
   334|   - 止损默认放在最近 5m swing low 下方 1%
   335|   - 同时会计算 `ATR(14) * 1.5` 作为动态止损距离；若更合理，会写入 `atr_stop_distance` 并供分批止盈 / 保本逻辑复用
   336|   - 默认风险 `10U`
   337|5. 杠杆建议：
   338|   - 根据止损距离动态给出推荐杠杆
   339|   - 波动越大，推荐杠杆越低
   340|6. 下单与持仓管理：
   341|   - `--scan-only`：仅输出候选与建议
   342|   - `--live`：提交市价做多 + `STOP_MARKET` / algo 保护止损
   343|   - 当前实测已打通：开仓、初始止损、基础 reconcile 与告警通知
   344|   - 当前尚未真正打通：达到 `1R` 后自动保本、达到 `1.5R/2R` 后自动分批止盈、runner 尾仓自动退出；这些参数与交易卡片字段已存在，但自动执行管理器仍待实现
   345|7. 定时轮询自动开单：
   346|   - 开启 `--auto-loop` 后，脚本会按 `--poll-interval-sec` 周期重扫
   347|   - 若同时启用 `--live`，当前可用于“命中信号后自动下单 + 初始止损 + 对账”，但不要假设它已经能在后台完整接管持仓管理
   348|   - 通过 `--max-scan-cycles` 可限制轮询次数，`0` 表示无限循环
   349|
   350|## 环境变量
   351|
   352|实盘/测试网下单时需要：
   353|- `BINANCE_FUTURES_API_KEY`
   354|- `BINANCE_FUTURES_API_SECRET`
   355|
   356|可选：
   357|- `BINANCE_FUTURES_BASE_URL`
   358|  - 主网默认：`https://fapi.binance.com`
   359|  - 测试网可设：`https://testnet.binancefuture.com`
   360|- Telegram 通知：`TELEGRAM_BOT_TOKEN`
   361|- 微信通知：`WEIXIN_TOKEN`、`WEIXIN_ACCOUNT_ID`
   362|- 微信 Home 频道可直接用：`WEIXIN_HOME_CHANNEL`
   363|
   364|## 常用命令
   365|
   366|只扫描，不下单：
   367|
   368|```bash
   369|python /root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py \
   370|  --square-symbols DOGS,ICP,TLM,ATOM,ALICE,DOGE,JTO,FIL,SUI,LTC \
   371|  --risk-usdt 10 \
   372|  --scan-only
   373|```
   374|
   375|推荐：用本地 Square symbols 文件，再扫描（更稳，适合长期后台运行）：
   376|
   377|```bash
   378|python /root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py \
   379|  --square-symbols-file /root/.hermes/binance_square_symbols.txt \
   380|  --risk-usdt 10 \
   381|  --scan-only
   382|```
   383|
   384|```bash
   385|python /root/.hermes/skills/binance/binance-futures-momentum-long/scripts/yaobiradar_v2_scorer.py
   386|```
   387|
   388|该脚本会读取 `/root/.hermes/yaobiradar_v2_candidates.json`，按 `hot_score + momentum_score + liquidity_score + breakout_score` 生成外部信号排序，并写出：
   389|- `/root/.hermes/binance_square_symbols.txt`
   390|- `/root/.hermes/binance_external_signal.json`
   391|
   392|示例输入：
   393|
   394|```json
   395|[
   396|  {
   397|    "symbol": "DOGE",
   398|    "hot_score": 32,
   399|    "momentum_score": 28,
   400|    "liquidity_score": 16,
   401|    "breakout_score": 18,
   402|    "reasons": ["hot_board", "breakout"]
   403|  },
   404|  {
   405|    "symbol": "SUIUSDT",
   406|    "hot_score": 25,
   407|    "momentum_score": 24,
   408|    "liquidity_score": 20,
   409|    "breakout_score": 10,
   410|    "reasons": ["steady_trend"]
   411|  }
   412|]
   413|```
   414|
   415|再运行主策略：
   416|
   417|```bash
   418|python /root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py \
   419|  --square-symbols-file /root/.hermes/binance_square_symbols.txt \
   420|  --risk-usdt 10 \
   421|  --scan-only
   422|```
   423|
   424|若只想给特定币对补 OKX 情绪：
   425|
   426|```bash
   427|python /root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py \
   428|  --symbol DOGEUSDT \
   429|  --okx-sentiment-command "python /root/.hermes/okx_sentiment_bridge.py --symbols DOGEUSDT,SUIUSDT,PEPEUSDT,WIFUSDT" \
   430|  --scan-only
   431|```
   432|
   433|尝试自动抓 Binance Square 热币，再扫描：
   434|
   435|```bash
   436|python /root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py \
   437|  --use-square-page \
   438|  --risk-usdt 10 \
   439|  --scan-only
   440|```
   441|
   442|实盘下单（默认主网，谨慎使用）：
   443|
   444|```bash
   445|export BINANCE_FUTURES_API_KEY='***'
   446|export BINANCE_FUTURES_API_SECRET='***'
   447|python /root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py \
   448|  --square-symbols DOGE,SUI,LTC \
   449|  --risk-usdt 10 \
   450|  --leverage 3 \
   451|  --live
   452|```
   453|
   454|测试网下单并自动管理持仓：
   455|
   456|```bash
   457|export BINANCE_FUTURES_BASE_URL='https://testnet.binancefuture.com'
   458|python /root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py \
   459|  --square-symbols DOGE,SUI,LTC \
   460|  --risk-usdt 10 \
   461|  --leverage 3 \
   462|  --tp1-r 1.5 \
   463|  --tp2-r 2.0 \
   464|  --live
   465|```
   466|
   467|定时轮询自动开单（每 60 秒扫一次，最多 20 轮）：
   468|
   469|```bash
   470|python /root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py \
   471|  --square-symbols DOGE,SUI,LTC \
   472|  --risk-usdt 10 \
   473|  --live \
   474|  --auto-loop \
   475|  --poll-interval-sec 60 \
   476|  --max-scan-cycles 20
   477|```
   478|
   479|当前实盘后台推荐启动命令（小资金 / 稳定版 Square 文件源 / Telegram + 微信双通知）：
   480|
   481|```bash
   482|python /root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py \
   483|  --live \
   484|  --auto-loop \
   485|  --max-scan-cycles 0 \
   486|  --poll-interval-sec 60 \
   487|  --risk-usdt 1 \
   488|  --leverage 3 \
   489|  --square-symbols-file /root/.hermes/binance_square_symbols.txt \
   490|  --notify-target telegram:<chat_id>,weixin:<chat_id>
   491|```
   492|
   493|说明：
   494|- `--max-scan-cycles 0` 表示无限循环，适合长期后台运行
   495|- `--square-symbols-file` 会绕开 Binance Square 页面 WAF 问题
   496|- 若账户已有未平仓仓位，建议先确认策略已启用“禁止再开新仓”安全阀，再恢复后台运行
   497|
   498|10U 激进模式（仍建议先 scan-only）：
   499|
   500|```bash
   501|
