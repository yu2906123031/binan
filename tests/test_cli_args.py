import importlib.util
import sys
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'binance_futures_momentum_long.py'


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
        '--use-external-setup-relaxation',
        '--top-losers', '9',
        '--lookback-bars', '15',
        '--swing-bars', '7',
        '--stop-buffer-pct', '0.02',
        '--breakeven-confirmation-mode', 'price_only',
        '--breakeven-min-buffer-pct', '0.002',
        '--max-open-positions', '4',
        '--max-long-positions', '2',
        '--max-short-positions', '3',
        '--max-net-exposure-usdt', '1500',
        '--max-gross-exposure-usdt', '2500',
        '--margin-type', 'CROSSED',
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
        '--gross-heat-cap-r', '2.8',
        '--same-theme-heat-cap-r', '1.1',
        '--same-correlation-heat-cap-r', '0.9',
        '--runtime-state-dir', '/tmp/runtime',
    ])
    assert args.symbol == 'DOGEUSDT'
    assert args.scan_only is True
    assert args.live is False
    assert args.square_symbols == 'DOGEUSDT,SUIUSDT'
    assert args.square_symbols_file == '/tmp/symbols.txt'
    assert args.external_signal_json == '/tmp/ext.json'
    assert args.use_external_setup_relaxation is True
    assert args.top_losers == 9
    assert args.lookback_bars == 15
    assert args.swing_bars == 7
    assert abs(args.stop_buffer_pct - 0.02) < 1e-9
    assert args.breakeven_confirmation_mode == 'price_only'
    assert abs(args.breakeven_min_buffer_pct - 0.002) < 1e-9
    assert args.max_open_positions == 4
    assert args.max_long_positions == 2
    assert args.max_short_positions == 3
    assert abs(args.max_net_exposure_usdt - 1500.0) < 1e-9
    assert abs(args.max_gross_exposure_usdt - 2500.0) < 1e-9
    assert args.margin_type == 'CROSSED'
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
    assert abs(args.gross_heat_cap_r - 2.8) < 1e-9
    assert abs(args.same_theme_heat_cap_r - 1.1) < 1e-9
    assert abs(args.same_correlation_heat_cap_r - 0.9) < 1e-9
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
    assert args.use_external_setup_relaxation is False
    assert args.top_losers == 20
    assert args.lookback_bars == 12
    assert args.swing_bars == 6
    assert abs(args.stop_buffer_pct - 0.01) < 1e-9
    assert args.max_open_positions == 1
    assert args.notify_target == ''
    assert args.disable_notify is False
    assert args.telegram_bot_token_env == 'TELEGRAM_BOT_TOKEN'
    assert args.margin_type == 'ISOLATED'
    assert args.breakeven_confirmation_mode == 'ema_support'
    assert abs(args.breakeven_min_buffer_pct - 0.001) < 1e-9
    assert args.reconcile_only is False
    assert args.halt_on_orphan_position is False
    assert abs(args.daily_max_loss_usdt - 0.0) < 1e-9
    assert args.max_consecutive_losses == 0
    assert args.symbol_cooldown_minutes == 0
    assert abs(args.gross_heat_cap_r - 0.0) < 1e-9
    assert abs(args.same_theme_heat_cap_r - 0.0) < 1e-9
    assert abs(args.same_correlation_heat_cap_r - 0.0) < 1e-9
    assert args.runtime_state_dir.endswith('runtime-state')


def test_active_profile_relaxes_small_account_entry_thresholds():
    mod = load_module()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', '10u-active']))
    assert args.risk_usdt == 1.0
    assert args.max_notional_usdt == 500.0
    assert args.lookback_bars == 6
    assert args.min_5m_change_pct == 0.8
    assert args.min_volume_multiple == 1.25
    assert args.min_quote_volume == 10_000_000


def test_okx_sim_active_profile_uses_exploratory_simulation_thresholds():
    mod = load_module()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'okx-sim-active']))
    assert args.risk_usdt == 1.0
    assert args.max_notional_usdt == 300.0
    assert args.leverage == 3
    assert args.lookback_bars == 3
    assert args.swing_bars == 4
    assert args.top_gainers == 40
    assert args.top_losers == 40
    assert args.min_5m_change_pct == 0.5
    assert args.min_volume_multiple == 1.05
    assert args.min_quote_volume == 5_000_000
    assert args.watch_breakout_tolerance_pct == 0.8
    assert args.setup_breakout_tolerance_pct == 0.35
    assert args.oi_hard_reversal_threshold_pct == 0.8
    assert args.sim_probe_entry_enabled is True
    assert args.sim_probe_size_ratio == 0.2
    assert args.sim_probe_min_score == 62.0


def test_load_dotenv_loads_values_without_overriding_existing_env(tmp_path, monkeypatch):
    mod = load_module()
    dotenv_path = tmp_path / '.env'
    dotenv_path.write_text(
        '\n'.join([
            '# local secrets',
            'BINANCE_FUTURES_API_KEY=from-file',
            'BINANCE_FUTURES_API_SECRET="quoted-secret"',
            'export BINANCE_FUTURES_BASE_URL=https://example.test',
        ]),
        encoding='utf-8',
    )
    monkeypatch.setenv('BINANCE_FUTURES_API_KEY', 'from-shell')
    monkeypatch.delenv('BINANCE_FUTURES_API_SECRET', raising=False)
    monkeypatch.delenv('BINANCE_FUTURES_BASE_URL', raising=False)

    loaded = mod.load_dotenv(dotenv_path)

    assert loaded == {
        'BINANCE_FUTURES_API_SECRET': 'quoted-secret',
        'BINANCE_FUTURES_BASE_URL': 'https://example.test',
    }
    assert mod.os.getenv('BINANCE_FUTURES_API_KEY') == 'from-shell'
    assert mod.os.getenv('BINANCE_FUTURES_API_SECRET') == 'quoted-secret'
    assert mod.os.getenv('BINANCE_FUTURES_BASE_URL') == 'https://example.test'


def test_binance_public_get_retries_transient_timeout(monkeypatch):
    mod = load_module()
    monkeypatch.setattr(mod.time, 'sleep', lambda _seconds: None)

    class Response:
        status_code = 200

        def json(self):
            return {'ok': True}

    class Session:
        def __init__(self):
            self.headers = {}
            self.calls = 0

        def get(self, url, params=None, timeout=15):
            self.calls += 1
            if self.calls == 1:
                raise mod.requests.ReadTimeout('slow')
            return Response()

    session = Session()
    client = mod.BinanceFuturesClient('https://example.test', session=session, max_get_retries=2)

    assert client.get('/fapi/v1/klines', {'symbol': 'BTCUSDT'}) == {'ok': True}
    assert session.calls == 2


def test_external_accumulation_setup_params_are_explicitly_enabled():
    mod = load_module()
    signal = {
        'external_signal_score': 88,
        'portfolio_narrative_bucket': 'accumulation',
        'external_reasons': ['status=volume_warming'],
    }

    assert mod.derive_external_setup_params(signal, enabled=False) == {'enabled': False}
    params = mod.derive_external_setup_params(signal, enabled=True)

    assert params['enabled'] is True
    assert params['score'] == 88.0
    assert params['max_breakout_distance_pct'] == 2.5
    assert params['min_quote_volume'] == 1_000_000.0


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
