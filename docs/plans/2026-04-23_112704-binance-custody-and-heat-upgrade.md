# Binance Futures Momentum Strategy Optimization Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Upgrade the existing Binance futures momentum system with per-position custody state management, staged trigger confirmation, execution-grade hard vetoes, heat-based portfolio risk, and review-grade analytics fields.

**Architecture:** Extend the current side-aware runtime instead of rewriting the scanner. The core path is `Candidate -> live veto -> place_live_trade -> positions.json/events.jsonl -> monitor/reconcile`, so the lowest-risk upgrade is to harden each stage in place: candidate gating, execution validation, per-position management state machine, and portfolio heat accounting. Exchange sync should use Binance user data stream plus `/fapi/v3/positionRisk` snapshots, and conditional stop migration should follow cancel-and-recreate semantics compatible with Binance Algo Service.

**Tech Stack:** Python single-file strategy engine `scripts/binance_futures_momentum_long.py`, pytest regression suite under `tests/`, runtime JSON/JSONL state, Binance Futures REST + user data stream.

---

## Current context

Code inspection shows these strong foundations already exist:
- `Candidate`, `TradeManagementPlan`, `TradeManagementState`, `positions.json`, and `events.jsonl` are already side-aware.
- `build_trade_management_plan()`, `evaluate_management_actions()`, and `apply_management_action()` already support BE/TP/runner and already use cancel-old + recreate-new stop logic.
- `fetch_exchange_meta()` and `fetch_funding_rates()` already exist.
- `apply_hard_veto_filters()` and `evaluate_risk_guards()` already enforce execution slippage and liquidity checks, though thresholds and scope are softer than the target design.
- `build_candidate()` still sets `setup_ready` and `trigger_fired` from a single-stage breakout/breakdown condition around line ~2148.
- `place_live_trade()` still sends protection through `/fapi/v1/order`, so Algo Service migration support needs explicit handling.
- Portfolio guards currently center on position counts and gross/net exposure; they do not yet track remaining open risk heat in R units.

That means the highest-leverage path is targeted extension, not scanner replacement.

---

## Target upgrades

### 1. Per-position custody layer
Implement an explicit lifecycle state machine per position:
- `opening`
- `protected`
- `tp1_done`
- `be_locked`
- `tp2_done`
- `runner`
- `closed`
- `orphan_repair`

Persist it in `positions.json`, emit every transition to `events.jsonl`, and reconcile it against Binance user stream + `/fapi/v3/positionRisk`.

Suggested default management values:
- `be_trigger_r = 1.0`
- `be_offset_r = 0.05`
- `tp1_r = 1.5`
- `tp1_close_pct = 0.35`
- `tp2_r = 2.2`
- `tp2_close_pct = 0.35`
- `runner_trail = EMA20_5m or 1.8 * ATR_5m`

### 2. Hard two-stage trigger model
Separate:
- `setup_ready`: structure and regime are ready
- `trigger_fired`: at least 2 micro confirmations are true

Micro-confirmation pool:
- 3m or 5m close beyond breakout/breakdown level
- retest holds VWAP or EMA20 in trade direction
- OI change and taker buy/sell ratio align with side
- funding and top trader positioning stay out of extreme crowding

Add long-side chase filters:
- `max_distance_to_vwap_atr = 0.6`
- `max_distance_to_ema20_atr = 0.8`

### 3. Execution hard gates before live order placement
Promote execution diagnostics from display-only into live veto and preflight validation.

Expand risk sizing conceptually to:

```text
effective_risk = base_risk_usdt
               * regime_multiplier
               * side_multiplier
               * drawdown_multiplier
               * execution_multiplier
```

Session-start mandatory sync:
- `/fapi/v2/account`
- `/fapi/v1/multiAssetsMargin`
- `/fapi/v1/positionSide/dual`

Pre-order mandatory sync:
- `/fapi/v1/exchangeInfo`
- `/fapi/v1/leverageBracket`

Hard reject conditions:
- `expected_slippage_r > 0.15`
- actual leverage differs from planned leverage
- quantity fails tick, lot, or min notional filters
- stop or take-profit trigger violates `triggerProtect`

Timeout handling:
- treat `-1007` as order-status-unknown
- consult user data stream first
- then query order status
- only then decide whether a retry is safe

Algo Service requirement:
- Binance migrated STOP / TP / trailing conditional orders to Algo Service in late 2025
- stop migration logic must use cancel old + create new
- system must ingest `ALGO_UPDATE` alongside `ORDER_TRADE_UPDATE` and `ACCOUNT_UPDATE`

### 4. Portfolio heat in R instead of only notional
Add:
- `portfolio_heat_r = Σ open_risk_remaining_r + pending_order_risk_r`
- `same_theme_heat_r`
- `same_correlation_cluster_heat_r`

Suggested starting limits:
- `gross_heat_cap_r = 2.8`
- `same_theme_heat_cap_r = 1.1`
- `dd_step_1 = -1R -> new risk * 0.7`
- `dd_step_2 = -2R -> new risk * 0.4 or halt`
- `same_setup_2_losses -> freeze 4~6h`

### 5. Weekly bucket analytics for setup pruning
Persist for every trade:
- `setup_class`
- `trigger_class`
- `mfe_r`
- `mae_r`
- `time_to_1r`
- `time_in_trade_minutes`
- `slippage_r`
- `exit_reason`

Aggregate weekly by:
- `regime × side × state × trigger_class × score_decile`

Use expectancy to disable the weakest 10%–20% buckets.

---

## Files likely to change

### Primary implementation
- Modify: `scripts/binance_futures_momentum_long.py`

### Tests
- Modify: `tests/test_strategy_v2.py`
- Modify: `tests/test_strategy_v2_restore_regression.py`
- Modify: `tests/test_run_loop_smoke.py`
- Create if needed: `tests/test_binance_execution_preflight.py`
- Create if needed: `tests/test_position_custody_state_machine.py`

### Docs
- Modify: `SKILL.md`
- Modify: `docs/plans/2026-04-21-binance-momentum-ls-upgrade-plan.md`
- Create: `docs/plans/2026-04-23-binance-custody-and-heat-upgrade.md`

---

## Implementation plan

### Phase 1: Lock current behavior with tests

#### Task 1: Add regression tests for current two-stage gaps
**Objective:** Capture the current single-stage trigger behavior so the later change is explicit and reviewable.

**Files:**
- Modify: `tests/test_strategy_v2_restore_regression.py`

**Steps:**
1. Add a test proving `build_candidate()` currently marks both `setup_ready` and `trigger_fired` from a direct breakout/breakdown condition.
2. Add a fixture variant where structure is good and breakout level is touched yet only one micro-confirm is true.
3. Mark expected future behavior in assertion comments so the delta is clear during refactor.
4. Run targeted pytest for the new test block.

**Run:**
```bash
pytest tests/test_strategy_v2_restore_regression.py -k 'trigger or setup_ready' -v
```

#### Task 2: Add regression tests for current management lifecycle fields
**Objective:** Preserve current BE/TP/runner semantics before adding new custody states.

**Files:**
- Modify: `tests/test_strategy_v2.py`
- Modify: `tests/test_strategy_v2_restore_regression.py`

**Steps:**
1. Add tests around `build_trade_management_plan()` using the target default values 1.0 / 1.5 / 2.2 / 0.35 / 0.35.
2. Add tests around `evaluate_management_actions()` and `apply_management_action()` for long and short flows.
3. Assert current stop replacement behavior stays cancel-then-create.
4. Run targeted pytest.

**Run:**
```bash
pytest tests/test_strategy_v2.py tests/test_strategy_v2_restore_regression.py -k 'management or breakeven or tp1 or tp2 or runner' -v
```

---

### Phase 2: Add per-position custody state machine

#### Task 3: Extend runtime position schema with custody state
**Objective:** Persist explicit lifecycle state in `positions.json` and migration helpers.

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Modify: `tests/test_strategy_v2.py`

**Implementation details:**
- Add fields like:
  - `custody_state`
  - `breakeven_price`
  - `tp1_done_at`
  - `tp2_done_at`
  - `runner_trail_mode`
  - `last_sync_source`
  - `algo_stop_order_id`
  - `algo_tp_order_ids`
- Update `migrate_positions_state()` and `materialize_positions_state()` defaults.
- Keep legacy aliases compatible.

**Verification:**
```bash
pytest tests/test_strategy_v2.py -k 'positions or migrate' -v
```

#### Task 4: Implement deterministic custody transition helper
**Objective:** Centralize all state transitions and event emission.

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Create: `tests/test_position_custody_state_machine.py`

**Implementation details:**
- Add helper such as `advance_custody_state(position, trigger, context)`.
- Allowed transitions:
  - `opening -> protected`
  - `protected -> tp1_done`
  - `protected -> be_locked`
  - `tp1_done -> be_locked`
  - `be_locked -> tp2_done`
  - `tp2_done -> runner`
  - `runner -> closed`
  - any live state -> `orphan_repair`
- Emit `append_runtime_event(..., 'custody_state_changed', ...)` with old/new state.

**Verification:**
```bash
pytest tests/test_position_custody_state_machine.py -v
```

#### Task 5: Wire custody state into monitor and reconcile loops
**Objective:** Keep runtime state aligned with partial exits, stop migration, and exchange truth.

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Modify: `tests/test_run_loop_smoke.py`

**Implementation details:**
- `place_live_trade()` should create `opening`, then promote to `protected` once protection confirms.
- `monitor_live_trade()` should update custody state on TP1, BE lock, TP2, runner, and closed.
- Reconcile path should enter `orphan_repair` when exchange position exists without matching protection metadata.
- Use `/fapi/v3/positionRisk` fields including `breakEvenPrice` as sync baseline.

**Verification:**
```bash
pytest tests/test_run_loop_smoke.py tests/test_strategy_v2_restore_regression.py -k 'reconcile or monitor or orphan' -v
```

---

### Phase 3: Add Binance Algo Service compatible protection handling

#### Task 6: Introduce exchange capability abstraction for conditional orders
**Objective:** Route STOP/TP/trailing creation through an exchange adapter that supports Algo Service semantics.

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Modify: `tests/test_strategy_v2_restore_regression.py`

**Implementation details:**
- Wrap conditional order placement in helpers like:
  - `place_protective_stop_order(...)`
  - `cancel_protective_order(...)`
  - `replace_protective_stop_order(...)`
- Add support for `ALGO_UPDATE` normalization.
- Preserve cancel-old + recreate-new semantics for moving stops.
- Surface `-4120` as explicit capability mismatch diagnostics.

**Verification:**
```bash
pytest tests/test_strategy_v2_restore_regression.py -k 'algo or stop_replacement or protection' -v
```

#### Task 7: Add order-status-unknown recovery for timeout cases
**Objective:** Prevent duplicate orders after `-1007`.

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Create if needed: `tests/test_binance_execution_preflight.py`

**Implementation details:**
- Add recovery helper like `resolve_unknown_order_status(...)`.
- Recovery order:
  1. inspect recent user data stream events
  2. query order status by client order id or order id
  3. decide filled / open / absent
- `place_live_trade()` should use this path before any retry logic.

**Verification:**
```bash
pytest tests/test_binance_execution_preflight.py -k '1007 or unknown order' -v
```

---

### Phase 4: Harden setup_ready and trigger_fired

#### Task 8: Add crowding and micro-confirm fetch helpers
**Objective:** Gather the extra data needed for staged trigger confirmation.

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Modify: `tests/test_strategy_v2.py`

**Implementation details:**
- Add helpers for:
  - `/futures/data/topLongShortPositionRatio`
  - `/futures/data/takerlongshortRatio`
  - existing `/fapi/v1/fundingRate`
- Normalize the latest values into a compact confirmation payload.
- Add robust defaults when endpoints are empty or rate-limited.

**Verification:**
```bash
pytest tests/test_strategy_v2.py -k 'funding or longshort or taker' -v
```

#### Task 9: Refactor `build_candidate()` into staged setup + trigger confirmation
**Objective:** Make `setup_ready` structural and `trigger_fired` confirmation-based.

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Modify: `tests/test_strategy_v2_restore_regression.py`

**Implementation details:**
- Split current logic into helpers such as:
  - `evaluate_setup_ready(...)`
  - `evaluate_trigger_confirmations(...)`
- Require at least 2 confirmations for `trigger_fired`.
- Add long-side hard chase filters using ATR-normalized VWAP and EMA20 distance caps.
- Persist `trigger_class` and micro-confirm counts in candidate payload and alert output.

**Verification:**
```bash
pytest tests/test_strategy_v2_restore_regression.py -k 'trigger_fired or setup_ready or overextension' -v
```

---

### Phase 5: Promote execution diagnostics into hard preflight vetoes

#### Task 10: Add session-start account and mode sync
**Objective:** Validate account mode and trading assumptions before scan or execution.

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Modify: `tests/test_run_loop_smoke.py`

**Implementation details:**
- Add a startup sync helper that reads:
  - `/fapi/v2/account`
  - `/fapi/v1/multiAssetsMargin`
  - `/fapi/v1/positionSide/dual`
- Persist mode snapshot into runtime state and events.
- Fail fast on incompatible position mode assumptions.

**Verification:**
```bash
pytest tests/test_run_loop_smoke.py -k 'account or position mode or multi assets' -v
```

#### Task 11: Add pre-order exchange validation and leverage bracket checks
**Objective:** Turn exchange metadata mismatches into reject-grade hard gates.

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Create if needed: `tests/test_binance_execution_preflight.py`

**Implementation details:**
- Add helper such as `validate_execution_preflight(candidate, meta, leverage, account_snapshot, bracket_snapshot)`.
- Check:
  - `expected_slippage_r <= 0.15`
  - actual leverage equals planned leverage after set-leverage response
  - quantity obeys step size, min qty, min notional
  - stop and TP trigger comply with `triggerProtect`
- Emit explicit reject reasons for analytics.

**Verification:**
```bash
pytest tests/test_binance_execution_preflight.py -k 'preflight or leverage or triggerProtect or slippage' -v
```

#### Task 12: Add drawdown multiplier into effective risk sizing
**Objective:** Scale new risk after losses before quantity is computed.

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Modify: `tests/test_strategy_v2.py`

**Implementation details:**
- Add helper like `derive_drawdown_risk_multiplier(risk_state)`.
- Start with:
  - `0.7` after `-1R`
  - `0.4` or halt after `-2R`
- Thread the multiplier into sizing and alert payloads.

**Verification:**
```bash
pytest tests/test_strategy_v2.py -k 'drawdown multiplier or position size' -v
```

---

### Phase 6: Upgrade portfolio controls to heat-based risk

#### Task 13: Extend risk state schema with heat fields
**Objective:** Track remaining open risk in R across live and pending positions.

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Modify: `tests/test_strategy_v2.py`

**Implementation details:**
- Add fields:
  - `portfolio_heat_r`
  - `pending_order_risk_r`
  - `same_theme_heat_r`
  - `same_correlation_cluster_heat_r`
  - `same_setup_loss_streak`
  - `setup_freeze_until`
- Build helper to compute open remaining risk from `positions.json` and current stop distance.

**Verification:**
```bash
pytest tests/test_strategy_v2.py -k 'heat or risk state' -v
```

#### Task 14: Enforce heat caps inside risk guards
**Objective:** Reject new trades when residual risk budget is exhausted.

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Modify: `tests/test_run_loop_smoke.py`

**Implementation details:**
- Update `evaluate_risk_guards()` and any portfolio snapshot helper to use heat caps.
- Enforce:
  - `gross_heat_cap_r = 2.8`
  - `same_theme_heat_cap_r = 1.1`
- Add setup-specific freeze after two consecutive losses on the same setup class.

**Verification:**
```bash
pytest tests/test_run_loop_smoke.py tests/test_strategy_v2.py -k 'heat cap or setup freeze or risk guards' -v
```

---

### Phase 7: Add review-grade trade analytics fields

#### Task 15: Persist MFE/MAE/time metrics in runtime lifecycle
**Objective:** Capture analytics fields during monitoring without post-hoc reconstruction.

**Files:**
- Modify: `scripts/binance_futures_momentum_long.py`
- Modify: `tests/test_strategy_v2_restore_regression.py`

**Implementation details:**
- Add tracking fields on open position payloads:
  - `mfe_r`
  - `mae_r`
  - `time_to_1r`
  - `time_in_trade_minutes`
  - `setup_class`
  - `trigger_class`
  - `slippage_r`
  - `exit_reason`
- Update on each monitoring tick and finalize on closure.

**Verification:**
```bash
pytest tests/test_strategy_v2_restore_regression.py -k 'mfe or mae or time_in_trade or exit_reason' -v
```

#### Task 16: Add weekly bucket report helper
**Objective:** Produce expectancy tables by regime/side/state/trigger bucket.

**Files:**
- Modify: `scripts/rejected_analysis.py` or create a dedicated report script
- Create if needed: `scripts/trade_bucket_analysis.py`
- Modify: `SKILL.md`

**Implementation details:**
- Read closed-trade events or runtime snapshots.
- Bucket by:
  - `regime`
  - `side`
  - `state`
  - `trigger_class`
  - `score_decile`
- Output counts, win rate, avg expectancy, avg MFE, avg MAE.

**Verification:**
```bash
pytest -k 'bucket analysis' -v
python scripts/trade_bucket_analysis.py --help
```

---

## Testing sequence

Run in this order after implementation:

```bash
pytest tests/test_strategy_v2.py -v
pytest tests/test_strategy_v2_restore_regression.py -v
pytest tests/test_run_loop_smoke.py -v
pytest tests/test_position_custody_state_machine.py -v
pytest tests/test_binance_execution_preflight.py -v
pytest -q
```

If the runtime has a dry-run mode for the strategy, add one end-to-end verification pass:

```bash
python scripts/binance_futures_momentum_long.py --help
python scripts/binance_futures_momentum_long.py [existing dry-run args]
```

---

## Key design decisions

1. **Keep the scanner and extend it in place.** The code already has side-aware schema and management hooks, so direct extension carries lower regression risk.
2. **Centralize custody transitions.** A single state transition helper keeps monitor, reconcile, and analytics consistent.
3. **Treat exchange truth as primary.** User stream and `positionRisk` should drive reconciliation, while local JSON remains the durable cache.
4. **Use hard vetoes before sending orders.** Display-only diagnostics help review; preflight checks protect capital.
5. **Size by residual risk budget.** Heat in R matches actual downside exposure better than raw notional.

---

## Risks and tradeoffs

- Binance Algo Service details may differ from the current REST helper assumptions, so endpoint confirmation is required before live rollout.
- User data stream ingestion may need a broader event normalization layer if `ALGO_UPDATE` payload shape differs from `ORDER_TRADE_UPDATE`.
- Adding more REST lookups per symbol can raise latency and rate-limit pressure; cache account-mode and leverage-bracket snapshots aggressively.
- Heat accounting depends on accurate remaining risk after partial exits and stop migration, so custody state and stop sync must land before heat caps are trusted.

---

## Open questions to confirm during implementation

1. Exact Binance Algo Service endpoint and signed payload fields for USDⓈ-M conditional orders on this account.
2. Whether trailing runner should default to `EMA20_5m`, `1.8 * ATR_5m`, or a regime-dependent switch.
3. Whether `same_setup_2_losses -> freeze 4~6h` should start at 4h by default or expose a CLI flag.
4. Whether weekly bucket disabling should be report-only first or auto-veto live setups once enough sample size exists.

---

## Recommended execution order

Start with Phases 1, 2, and 5. That sequence gives you regression protection, custody-state durability, and reject-grade execution safety before touching the higher-variance trigger model.
