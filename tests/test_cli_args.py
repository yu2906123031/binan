import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = Path('/root/.hermes/skills/binance/binance-futures-momentum-long/scripts/binance_futures_momentum_long.py')


def load_module():
    spec = importlib.util.spec_from_file_location('bfml_test', SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_exposes_runtime_and_execution_flags():
    mod = load_module()
    args = mod.parse_args([
        '--symbol', 'DOGEUSDT',
        '--scan-only',
        '--square-symbols', 'DOGEUSDT,SUIUSDT',
        '--square-symbols-file', '/tmp/symbols.txt',
        '--external-signal-json', '/tmp/ext.json',
        '--lookback-bars', '15',
        '--swing-bars', '7',
        '--stop-buffer-pct', '0.02',
        '--max-open-positions', '4',
        '--max-long-positions', '2',
        '--max-short-positions', '3',
        '--max-net-exposure-usdt', '1500',
        '--max-gross-exposure-usdt', '2500',
        '--allow-symbol-hedge',
        '--opposite-side-flip-cooldown-minutes', '45',
        '--notify-target', 'telegram:123',
        '--disable-notify',
        '--telegram-bot-token-env', 'TG_TOKEN',
        '--reconcile-only',
        '--halt-on-orphan-position',
        '--daily-max-loss-usdt', '50',
        '--max-consecutive-losses', '3',
        '--symbol-cooldown-minutes', '20',
        '--runtime-state-dir', '/tmp/runtime',
    ])
    assert args.symbol == 'DOGEUSDT'
    assert args.scan_only is True
    assert args.live is False
    assert args.square_symbols == 'DOGEUSDT,SUIUSDT'
    assert args.square_symbols_file == '/tmp/symbols.txt'
    assert args.external_signal_json == '/tmp/ext.json'
    assert args.lookback_bars == 15
    assert args.swing_bars == 7
    assert abs(args.stop_buffer_pct - 0.02) < 1e-9
    assert args.max_open_positions == 4
    assert args.max_long_positions == 2
    assert args.max_short_positions == 3
    assert abs(args.max_net_exposure_usdt - 1500.0) < 1e-9
    assert abs(args.max_gross_exposure_usdt - 2500.0) < 1e-9
    assert args.per_symbol_single_side_only is False
    assert args.opposite_side_flip_cooldown_minutes == 45
    assert args.notify_target == 'telegram:123'
    assert args.disable_notify is True
    assert args.telegram_bot_token_env == 'TG_TOKEN'
    assert args.reconcile_only is True
    assert args.halt_on_orphan_position is True
    assert abs(args.daily_max_loss_usdt - 50.0) < 1e-9
    assert args.max_consecutive_losses == 3
    assert args.symbol_cooldown_minutes == 20
    assert args.runtime_state_dir == '/tmp/runtime'


def test_parse_args_defaults_cover_run_loop_dependencies():
    mod = load_module()
    args = mod.apply_runtime_profile(mod.parse_args([]))
    assert args.symbol == ''
    assert args.scan_only is False
    assert args.live is False
    assert args.square_symbols == ''
    assert args.square_symbols_file == ''
    assert args.external_signal_json == ''
    assert args.lookback_bars == 12
    assert args.swing_bars == 6
    assert abs(args.stop_buffer_pct - 0.01) < 1e-9
    assert args.max_open_positions == 1
    assert args.notify_target == ''
    assert args.disable_notify is False
    assert args.telegram_bot_token_env == 'TELEGRAM_BOT_TOKEN'
    assert args.reconcile_only is False
    assert args.halt_on_orphan_position is False
    assert abs(args.daily_max_loss_usdt - 0.0) < 1e-9
    assert args.max_consecutive_losses == 0
    assert args.symbol_cooldown_minutes == 0
    assert args.runtime_state_dir.endswith('runtime-state')


def test_recommended_position_size_pct_applies_regime_and_side_multiplier():
    mod = load_module()
    assert abs(mod.recommended_position_size_pct('high', regime_multiplier=0.8, side_multiplier=1.15) - 2.76) < 1e-9
    assert abs(mod.recommended_position_size_pct('blocked', regime_multiplier=1.0, side_multiplier=1.15) - 0.0) < 1e-9


def test_derive_side_risk_multiplier_biases_by_regime_and_side():
    mod = load_module()
    assert abs(mod.derive_side_risk_multiplier('LONG', 'risk_on') - 1.15) < 1e-9
    assert abs(mod.derive_side_risk_multiplier('SHORT', 'risk_on') - 0.85) < 1e-9
    assert abs(mod.derive_side_risk_multiplier('SHORT', 'risk_off') - 1.15) < 1e-9
    assert abs(mod.derive_side_risk_multiplier('LONG', 'caution') - 0.9) < 1e-9
