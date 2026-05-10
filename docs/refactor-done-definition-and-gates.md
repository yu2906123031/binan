# 重构完成定义与质量门禁

本文档定义 `/root/binan` 当前重构阶段的完成标准与持续门禁，用于在后续功能迭代中持续压低单体脚本回弹、异常吞没和结构退化风险。

## 1. 完成定义

一次结构重构可以标记为完成，需要同时满足下面四类条件。

### 1.1 契约稳定

- CLI 参数名、默认值、输出格式保持兼容，或在变更记录中显式说明。
- `positions.json`、`risk_state.json`、`last_cycle.json`、`events.jsonl` 的字段契约保持稳定。
- 关键事件名保持稳定，至少包括：
  - `runtime_state_degraded`
  - `buy_fill_confirmed`
  - `protection_confirmed`
  - `breakeven_moved`
  - `tp1_hit`
  - `tp2_hit`
  - `trade_closed`
  - `position_closed`
  - `candidate_rejected`
- 旧导入路径仍可工作，或由 wrapper / re-export 维持兼容。

### 1.2 测试覆盖

- 变更前先补 RED 测试，变更后回到 GREEN。
- 至少运行与改动边界直接相关的专项测试。
- 涉及主路径接线、运行态 schema、恢复逻辑、监控线程、watcher 退出语义的改动，需要额外跑跨模块回归集。

推荐最小验证集：

```bash
python -m ruff check /root/binan
PYTHONPATH=/root/binan/scripts pytest -q \
  tests/test_strategy_v2.py \
  tests/test_strategy_v2_restore_regression.py \
  tests/test_execution_module_regression.py \
  tests/test_hermes_outer_watcher.py
```

涉及更广改动时，执行全量：

```bash
pytest -q
```

### 1.3 文档同步

- tracker 必须更新到最新状态。
- 架构边界变化需要更新 `docs/architecture.md`。
- 涉及状态机、schema、退出码、事件 contract 的变更，需要同步更新对应专题文档。

### 1.4 可回滚性

- 提取后的模块保持小步迁移，可通过 wrapper 或 re-export 快速回接。
- 主脚本与提取模块的边界具备回归测试，支持快速定位行为漂移。

## 2. 持续质量门禁

### 2.1 单文件行数门禁

- 单文件超过 **3000 行** 进入拆分预警。
- 当前观测结果：
  - `scripts/binance_futures_momentum_long.py`：`6520` 行，处于高优先级拆分区。
  - `scripts/execution_engine.py`：`870` 行。
  - `scripts/candidate_builder.py`：`618` 行。
  - `scripts/runtime_store.py`：`452` 行。
  - `scripts/risk_engine.py`：`151` 行。

治理规则：

1. 新增功能优先进入已提取模块。
2. 单体脚本新增逻辑前，优先检查是否已有对应边界模块可承接。
3. 主脚本继续承接新职责时，需要在 tracker 中登记拆分债务。

### 2.2 单函数行数门禁

- 单函数超过 **120 行** 进入拆分预警。
- 当前超限函数：
  - `binance_futures_momentum_long.py::run_loop` `388` 行
  - `binance_futures_momentum_long.py::run_scan_once` `299` 行
  - `binance_futures_momentum_long.py::manage_okx_simulated_positions` `183` 行
  - `binance_futures_momentum_long.py::classify_candidate_state` `154` 行
  - `binance_futures_momentum_long.py::apply_runtime_profile` `137` 行
  - `binance_futures_momentum_long.py::run_book_ticker_websocket_supervisor` `128` 行
  - `candidate_builder.py::build_candidate` `613` 行
  - `execution_engine.py::monitor_live_trade` `369` 行
  - `execution_engine.py::place_live_trade` `295` 行
  - `hermes_outer_watcher.py::main` `173` 行

治理规则：

1. 超限函数的新需求优先拆 helper，再追加行为。
2. 同一函数连续两次迭代继续扩张时，下一次改动默认先做拆分。
3. 超限函数相关 PR 或提交，验证清单里必须写明拆分计划或已提取边界。

### 2.3 宽泛异常门禁

- 新增 `except Exception` 需要在代码旁说明原因。
- 推荐说明内容包括：
  - 保护的外部依赖或不稳定输入来源
  - 回退策略
  - 为什么当前层级适合吞掉异常

推荐写法：

```python
try:
    payload = response.json()
except Exception:
    # 第三方返回体格式不稳定，降级为原始文本用于错误透传
    payload = response.text
```

当前仓库内已有大量 `except Exception`。后续治理按两条线推进：

1. 新代码立即执行说明门禁。
2. 旧代码在触达式重构时顺手补充注释或改成更窄异常类型。

## 3. 推荐检查命令

### 3.1 行数与超长函数扫描

```bash
wc -l scripts/*.py
python - <<'PY'
import ast, pathlib
root = pathlib.Path('scripts')
for path in sorted(root.glob('*.py')):
    text = path.read_text(encoding='utf-8-sig')
    tree = ast.parse(text)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            end = getattr(node, 'end_lineno', None)
            if end and end - node.lineno + 1 > 120:
                print(path.name, node.name, node.lineno, end, end - node.lineno + 1)
PY
```

### 3.2 宽泛异常扫描

```bash
python -m ruff check /root/binan
rg "except Exception" scripts tests
```

## 4. 当前阶段的直接执行标准

P2 阶段之后，任何结构性改动满足下面标准即可视为达标：

1. 相关 RED/GREEN 测试完整。
2. `ruff check` 通过。
3. 专项回归通过，必要时全量 `pytest -q` 通过。
4. tracker、架构文档、专题契约文档已同步。
5. 新增代码满足文件长度、函数长度、异常说明三条门禁。

## 5. 下一轮拆分优先级

按照当前复杂度与风险，建议优先顺序：

1. `run_loop`
2. `run_scan_once`
3. `candidate_builder.build_candidate`
4. `execution_engine.monitor_live_trade`
5. `execution_engine.place_live_trade`
6. `classify_candidate_state`
