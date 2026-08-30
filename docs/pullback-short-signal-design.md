# 做空信号重做方案：pullback_short（弱势币反弹遇阻做空）

日期：2026-08-14
作者：Hermes Agent
状态：设计中（待 TDD 实现 + scan-only 验证）

## 1. 背景与问题

当前 `five-usdt-scalp-v2` 做空信号是「动量追空」：要求价格已跌破近期低点（破位），
再叠加 OI 下降 + taker 卖出 + CVD 卖压三重同向确认，才允许入场。

90 天真实成交对账结论：
- 胜率 32.7%（7 月后 28.1%），盈亏比 2.22~3.11 尚可，但手续费侵蚀 + 妖币大亏单吃掉了利润。
- 8 月 BEAR_TREND 环境：early_filter 0 通过、开出来的 27 笔胜率仅 14.8%、多空通杀。

根因：**破位追空在震荡熊市里天然失效** —— 跌破低点后常快速反弹（假破位），
追进去正好接反弹，反复小止损；而「顺势做空」的破位信号在震荡市又等不到。

## 2. 新信号定义

在现有「破位追空」（breakdown）之外，新增「反弹遇阻做空」（pullback_short）分支，
两者并存、按信号各自触发。pullback_short 是「转折型」入场：不追已经跌下来的动量，
而是等弱势币反弹到阻力位、动量衰竭时做空。

核心优势：
- 入场点更接近阻力位 → 止损更紧 → 盈亏比更好。
- 逆小势（反弹）顺大势（下跌趋势），贴合 BEAR_TREND 环境。
- 对标 8 月唯一亮点：BLESSUSDT 持续做空弱势币 +1.58（反复反弹遇阻做空）。

## 3. 判定规则（pullback_short_setup）

前置：`trade_side == SHORT`。以下 4 条同时满足即认定「反弹遇阻做空」机会：

1. **弱势币**：`higher_tf_allowed`（`evaluate_higher_timeframe_trend` 对 SHORT 方向
   allowed = 1h 或 4h 价格 ≤ EMA20 且 MACD 柱 ≤ 0，即大周期下跌趋势）。

2. **处于反弹**：`last_price > breakout_level * (1 + rebound_min_pct)`，
   即当前价高于近期低点（`breakout_level = min(lows_5m[-lookback-1:-1])`）至少
   `rebound_min_pct`（默认 0.5%），确认价格未破位、而是从低位反弹上来了。

3. **反弹到阻力位**：价格接近 EMA20(5m) 或 VWAP(15m) 阻力位。
   做空方向下 `distance_from_ema20_5m_pct` 与 `distance_from_vwap_15m_pct` 定义见
   candidate_builder（`(1 - last_price/ema)*100`，价格在均线上方时为负）。
   判定：`-pullback_resistance_tolerance_pct <= distance_from_ema20_5m_pct <= pullback_resistance_tolerance_pct`
   或 VWAP 同区间（默认 tolerance 1.5%）。

4. **遇阻回落**：反弹动能衰竭，`macd_5m['hist'] < macd_5m['prev_hist']`
   （MACD 柱开始收窄/转负），或最近 1 根 5m K 线收阴（`last_price < open`）。

满足条件后，候选进入后续 setup/trigger 流程，但**豁免「破位」硬要求**
（`short_breakdown_not_confirmed`、`micro_structure_break_not_confirmed`），
OI/CVD 确认降级为加分项而非硬门槛（在 trigger 层保留，但允许 trigger_relax 放行）。

## 4. 止损与仓位

- 止损：反弹高点（`recent_swing_low` = 近期 highs 最高）+ `stop_buffer_pct`，
  仍走现有 `stop_price` 结构逻辑，保证 `stop_price > last_price`（做空）。
- 仓位：仍由 `plan_five_usdt_target_trade` 按 `risk_usdt / max_loss_usdt / notional` 反向计算，
  不改风险边界。

## 5. 实现方式（最小侵入）

- candidate_builder.py：新增辅助函数 `detect_pullback_short_setup(...)`，
  在 early filter 的做空分支里计算 `pullback_short_setup` 布尔，
  并将其并入 `near_breakout_setup` 的豁免逻辑（或单独豁免破位条件）。
- 主文件 binance_futures_momentum_long.py：
  - 新增 profile 参数 `pullback_short_enabled`（默认 True）、
    `pullback_rebound_min_pct`（0.5）、`pullback_resistance_tolerance_pct`（1.5）。
  - trigger 阶段对 pullback_short 候选放宽破位确认。
- 新增 state 归类：pullback_short 候选的 state 至少为 `watch`（setup_score 达到阈值）。

## 6. 测试计划（TDD）

1. RED：构造弱势币（1h/4h 下跌）+ 反弹到 EMA20 + MACD 转弱的做空输入，
   断言 `detect_pullback_short_setup` 返回 True。
2. RED：非弱势币 / 未反弹 / 反弹未到阻力位 / 动能未衰竭，断言返回 False。
3. 断言 pullback_short 候选不再被 `short_breakdown_not_confirmed` 拒绝。
4. 断言止损在反弹高点上方、仓位计算不突破 max_loss。

## 7. 部署计划

1. 编译 + 跑 focused 测试 + 全量相关切片 + `git diff --check`。
2. **scan-only 先行**：用 `--scan-only` 在真实行情下跑若干周期，观察 pullback_short
   候选的产生频率与质量（对比现有 breakdown 候选），确认不产生垃圾信号。
3. 观察确认后再切换实盘，保留 max_loss / protection / notional 边界不变。
