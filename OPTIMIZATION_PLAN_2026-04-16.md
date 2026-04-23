# Binance 项目优化方案（2026-04-16）

> 目标：把当前“可运行的单脚本 REST 轮询策略”升级成“可持续运行、可观测、可恢复、风控更完整”的小型交易系统。

## 当前已确认现状

1. 主策略脚本：
   - `/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py`
2. 当前测试：
   - `python -m pytest /root/.hermes/skills/binance/binance-futures-momentum-long/tests/test_strategy_v2.py -q`
   - 结果：`21 passed`
3. 当前热币源：
   - `/root/.hermes/binance_square_symbols.txt`
   - 已由 cron `refresh-binance-square-symbols` 每 15 分钟刷新
4. 当前账户快照任务：
   - cron `binance-u-futures-account-snapshot` 每 120 分钟推送一次到微信 Home
5. 当前实盘进程：
   - 存在一个长期运行进程：
   - `python -u .../binance_futures_momentum_long.py --live --auto-loop --max-scan-cycles 0 --poll-interval-sec 60 --risk-usdt 1 --leverage 3 --max-open-positions 4 --square-symbols-file /root/.hermes/binance_square_symbols.txt --notify-target telegram:-5125444265,weixin:o9cq8057ZI7ybume1S-QWi-5uXlw@im.wechat`
6. 当前真实持仓：
   - ORDIUSDT LONG 1.7，说明系统已在真实运行

## 当前优点

- 已有稳定的 symbols 文件源，绕开 Square 页面 WAF
- 已支持 live / auto-loop / max-open-positions
- 已支持 Telegram + 微信双通知
- 已有 21 条单元测试，覆盖核心计算、通知、多仓门控等
- 当前实盘进程在运行，说明最小闭环已打通

## 当前主要短板

### P0：风控与恢复短板
1. 没有“单日亏损上限 / 熔断 / 连亏暂停”
2. 没有“单币冷却期”
3. 没有“进程重启后自动接管旧仓位”
4. 没有“订单状态对账 / 挂单丢失修复”
5. 当前是 REST 轮询，不是 websocket 驱动，保护动作有延迟

### P1：工程化短板
1. 策略、执行、风控、通知都在单文件里，维护成本高
2. 缺少持久化状态文件（positions/orders/events/risk state）
3. 缺少结构化日志和事件审计
4. 缺少 healthcheck / 心跳 / 关键异常告警
5. 缺少回放 / 复盘报表

### P2：策略层短板
1. 目前还是单策略（追多 breakout）
2. 评分模型固定，缺少参数回测与自动调优
3. 没有更细的成交质量控制（滑点、盘口深度、成交额冲击）
4. 没有账户级资金分配策略

## 建议目标架构

拆成 6 个模块：

1. `market_data`
   - 负责 Square symbols、24h ticker、klines、funding、后续 websocket
2. `signal_engine`
   - 负责候选池、排序、开仓信号
3. `risk_engine`
   - 负责单笔风险、账户总风险、单日熔断、冷却、持仓上限
4. `execution_engine`
   - 负责下单、挂止损、止盈、撤单、订单修复、重启接管
5. `portfolio_state`
   - 负责持久化：open positions、active stop orders、trade journal、risk counters
6. `notification/reporting`
   - 负责 Telegram/微信通知、心跳、日报、异常报告

## 优化优先级

## 第一阶段（最该先做，1~2 天）——先补“不会死得莫名其妙”

### 1. 增加状态持久化
落地内容：
- 新增运行状态目录，例如：
  - `/root/.hermes/binance_runtime/positions.json`
  - `/root/.hermes/binance_runtime/orders.json`
  - `/root/.hermes/binance_runtime/risk_state.json`
  - `/root/.hermes/binance_runtime/trade_log.jsonl`
- 每次开仓、挂止损、TP、BE、runner、止损退出都写事件日志

价值：
- 重启后可恢复
- 能审计问题
- 能做报表

### 2. 增加重启恢复 / 接管旧仓位
落地内容：
- 启动时读取 Binance 当前 open positions
- 若发现本地状态缺失但交易所存在仓位：
  - 标记为“孤儿仓位”
  - 自动拉取对应 open orders
  - 检查是否已有止损保护
  - 缺失时触发高优先级告警，必要时自动补保护单
- 增加 `--reconcile-only` 模式
- reconcile 阶段同步关闭本地残留 tracked position：当本地记录仍是 monitoring/open，交易所仓位已消失时，自动把 `status` 收敛到 `closed`、`remaining_quantity=0`、`stop_order_id=None`、`protection_status=flat`，并在结果里输出 `closed_tracked_positions`

价值：
- 避免脚本重启后失控裸奔
- 本地状态会持续向交易所真实仓位收敛，后续风控、报表、心跳读到的是最新状态

### 3. 增加账户级风控
落地内容：
- `--daily-max-loss-usdt`
- `--max-consecutive-losses`
- `--symbol-cooldown-minutes`
- `--max-total-risk-usdt`
- `--halt-on-orphan-position`

价值：
- 这是当前最缺的安全阀

### 4. 增加关键告警
落地内容：
- 止损挂单失败：立即 Telegram+微信高优先级告警
- 本地状态与交易所不一致：告警
- 连续 API 失败 / 网络失败：告警
- 超过 N 分钟无扫描 / 无心跳：告警

## 第二阶段（2~4 天）——把它从脚本变成系统

### 5. 拆分单文件
建议拆分为：
- `binance_project/config.py`
- `binance_project/client.py`
- `binance_project/market_data.py`
- `binance_project/signals.py`
- `binance_project/risk.py`
- `binance_project/execution.py`
- `binance_project/state_store.py`
- `binance_project/notifications.py`
- `binance_project/runtime.py`

价值：
- 降低维护难度
- 单元测试更容易继续补

### 6. 增加结构化日志
落地内容：
- 全部核心动作统一输出 JSON log
- 字段统一：`event_type / symbol / order_id / position_qty / price / reason / ts`
- 区分：INFO / WARN / ERROR / TRADE_EVENT / RISK_EVENT

### 7. 增加健康检查与心跳
落地内容：
- 每 5~10 分钟发一次 heartbeat
- 内容包含：
  - 进程在线
  - 当前 open positions 数
  - 上次扫描时间
  - 上次成功下单时间
  - 最近异常数

## 第三阶段（4~7 天）——提升执行质量与实盘稳定性

### 8. websocket 化
优先顺序：
- 先接 mark price / book ticker / kline stream
- 再接 user data stream（订单/仓位更新）

价值：
- TP/BE/runner 不再依赖 15s REST 轮询
- 下单与仓位状态同步更快

### 9. 执行质量控制
落地内容：
- 下单前检查 spread / top-of-book 深度
- 增加最大允许滑点
- 流动性不足则拒单

### 10. 订单修复器
落地内容：
- 周期性核对：仓位 / 止损单 / reduce-only 单
- 若发现仓位存在但保护单缺失，自动补挂
- 若发现本地剩余数量与交易所不一致，自动修正状态

## 第四阶段（策略增强）

### 11. 参数管理与 profile 体系
当前已有：
- `default`
- `10u-aggressive`

建议新增：
- `conservative`
- `balanced`
- `high-volatility`

并支持：
- YAML 配置文件加载 profile
- 运行时输出 profile 完整快照

### 12. 复盘与报表
落地内容：
- 每日汇总：胜率、盈亏、平均 R、最大回撤、分币种表现
- 每笔交易落库/落 jsonl
- 可生成 Telegram/微信摘要

### 13. 研究/回测层
落地内容：
- 把 signal 函数抽象成可离线回测调用
- 对关键参数做 walk-forward / grid search
- 用真实 trade log 校准信号阈值

## 推荐实施顺序

### 立即做（P0）
1. 状态持久化
2. 重启恢复 / reconcile
3. 账户级风控
4. 关键告警

### 接着做（P1）
5. 模块拆分
6. 结构化日志
7. 健康检查 / 心跳

### 再做（P2）
8. websocket
9. 执行质量控制
10. 订单修复器
11. 报表
12. 参数回测

## 我建议的最小可执行版本（MVP）

如果只允许先做 3 件事，我建议是：

1. 重启接管旧仓位
2. 单日熔断 + 连亏暂停 + 单币冷却
3. 本地状态账本 + 保护单对账修复

这是收益最大的三项，因为它们直接降低实盘事故概率。

## 验收标准

### 风控验收
- 连续亏损达到阈值后，不再开新仓
- 当日亏损超阈值后，策略进入 halt 状态
- 同一币种止损后，冷却期内不能重复开仓

### 恢复验收
- 手动杀进程再重启，能识别已有仓位
- 若已有仓位但无保护单，能告警并补挂
- 本地状态丢失时，可通过 `--reconcile-only` 自动修正

### 运行验收
- 连续运行 24h 不因状态漂移失控
- 所有核心事件可在 jsonl 日志中追踪
- 心跳正常推送

## 已落地进展（2026-04-20）

当前脚本已经完成并写入代码的优化项：

1. 状态持久化：`RuntimeStateStore` 已落地，支持 `positions.json / orders.json / risk_state.json / events.jsonl`
2. 重启恢复 / reconcile：已支持启动时对账、孤儿仓位识别、`--reconcile-only`
3. 本地 tracked position 自动收敛：交易所仓位消失时会把本地状态收敛到 `closed`
4. 缺失保护单修复：已支持 `--repair-missing-protection` 自动补挂保护单
5. 账户级风控：已支持 `--daily-max-loss-usdt`
6. 账户级风控：已支持 `--max-consecutive-losses`
7. 账户级风控：已支持 `--symbol-cooldown-minutes`
8. 账户级风控：已支持 `--max-open-positions`
9. 关键告警通道：已支持 Telegram / 微信双通知目标
10. 结构化事件审计：关键运行事件已写入 `events.jsonl`
11. user data stream 基础能力：已支持 listen key 创建、刷新、关闭
12. user data stream 健康监控：已支持 refresh / disconnect 检测与健康状态写盘
13. user data stream 订单更新归一化：已支持 `ORDER_TRADE_UPDATE` 标准化与持仓写盘
14. 微观结构输入：已接入 order book depth
15. 微观结构输入：已接入短窗口 `bookTicker` 样本并计算 `cancel_rate`

当前仍值得继续推进的重点项：

16. 把 `collect_book_ticker_samples` 从短轮询升级成真正的 websocket 缓存源，让 `cancel_rate` 更贴近实盘微观结构
17. 把 websocket 市场数据从“候选指标输入”继续推进到“保护动作与执行层驱动”，让 TP / BE / runner / 仓位同步更快

## 下一步建议

当前最值得先做的是第 16 项：

- 新增 `bookTicker` websocket 缓存层
- 把缓存样本写入 runtime state
- `collect_book_ticker_samples` 优先读缓存，缓存缺失时再回退 REST
- 为缓存健康度、样本新鲜度、回退次数补结构化事件与测试

完成第 16 项后，再推进第 17 项，把 websocket 继续下沉到保护动作与执行监控。
