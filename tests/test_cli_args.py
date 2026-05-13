import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / 'scripts' / 'binance_futures_momentum_long.py'
SCRIPTS_DIR = SCRIPT_PATH.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


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


def test_parse_args_accepts_supervisor_live_flags():
    mod = load_module()
    args = mod.parse_args([
        '--profile', '10u-aggressive',
        '--live',
        '--auto-loop',
        '--max-scan-cycles', '0',
        '--poll-interval-sec', '60',
        '--monitor-poll-interval-sec', '15',
        '--risk-usdt', '5',
        '--max-notional-usdt', '20',
        '--leverage', '5',
        '--max-open-positions', '1',
        '--square-symbols-file', '/tmp/symbols.txt',
        '--notify-target', 'telegram:-5125444265,weixin:chatid',
    ])
    assert args.profile == '10u-aggressive'
    assert args.live is True
    assert args.auto_loop is True
    assert args.max_scan_cycles == 0
    assert args.poll_interval_sec == 60
    assert args.monitor_poll_interval_sec == 15
    assert abs(args.risk_usdt - 5.0) < 1e-9
    assert abs(args.max_notional_usdt - 20.0) < 1e-9
    assert args.leverage == 5
    assert args.max_open_positions == 1
    assert args.square_symbols_file == '/tmp/symbols.txt'
    assert args.notify_target == 'telegram:-5125444265,weixin:chatid'


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
    assert args.lookback_bars == 4
    assert args.swing_bars == 4
    assert args.min_5m_change_pct == 0.5
    assert args.min_volume_multiple == 0.9
    assert args.min_quote_volume == 5_000_000
    assert args.top_gainers == 35
    assert args.top_losers == 35
    assert args.max_candidates == 12
    assert args.watch_breakout_tolerance_pct == 0.8
    assert args.setup_breakout_tolerance_pct == 0.35
    assert args.max_distance_from_ema_pct == 8.0
    assert args.max_distance_from_vwap_pct == 7.0
    assert args.oi_hard_reversal_threshold_pct == 1.0
    assert args.execution_slippage_hard_veto_r == 0.4
    assert args.execution_slippage_risk_threshold_r == 0.25


def test_aggressive_profile_uses_relaxed_live_entry_thresholds():
    mod = load_module()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', '10u-aggressive']))
    assert args.risk_usdt == 1.2
    assert args.max_notional_usdt == 500.0
    assert args.leverage == 5
    assert args.lookback_bars == 4
    assert args.swing_bars == 4
    assert args.tp1_r == 5.0
    assert args.tp1_close_pct == 0.5
    assert args.tp2_r == 10.0
    assert args.tp2_close_pct == 0.5
    assert args.entry_tp1_offset_abs == 5.0
    assert args.entry_tp2_offset_abs == 10.0
    assert args.min_5m_change_pct == 0.35
    assert args.min_volume_multiple == 0.8
    assert args.min_quote_volume == 5_000_000
    assert args.top_gainers == 45
    assert args.top_losers == 45
    assert args.max_candidates == 16
    assert args.max_open_positions == 3
    assert args.max_long_positions == 3
    assert args.max_short_positions == 3
    assert args.watch_breakout_tolerance_pct == 0.75
    assert args.setup_breakout_tolerance_pct == 0.4
    assert args.max_distance_from_ema_pct == 9.0
    assert args.max_distance_from_vwap_pct == 8.0


def test_binance_sim_active_profile_uses_exploratory_simulation_thresholds():
    mod = load_module()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'binance-sim-active']))
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
    assert args.sim_probe_max_breakout_distance_pct == 0.35
    assert args.binance_simulated_trading is True
    assert args.base_url == 'https://testnet.binancefuture.com'


def test_high_vol_alt_mode_profile_enables_short_side_scan_and_relaxed_probe_thresholds():
    mod = load_module()
    args = mod.apply_runtime_profile(mod.parse_args(['--profile', 'high_vol_alt_mode']))

    assert args.risk_usdt == 1.0
    assert args.max_notional_usdt == 300.0
    assert args.leverage == 3
    assert args.lookback_bars == 3
    assert args.swing_bars == 4
    assert args.top_gainers == 40
    assert args.top_losers == 40
    assert args.max_candidates == 12
    assert args.max_rsi_5m == 84.0
    assert args.min_5m_change_pct == 0.5
    assert args.min_volume_multiple == 1.05
    assert args.min_quote_volume == 5_000_000
    assert args.watch_breakout_tolerance_pct == 0.8
    assert args.setup_breakout_tolerance_pct == 0.35
    assert args.oi_hard_reversal_threshold_pct == 0.8
    assert args.sim_probe_entry_enabled is True
    assert args.sim_probe_size_ratio == 0.2
    assert args.sim_probe_min_score == 62.0
    assert args.sim_probe_max_breakout_distance_pct == 0.35
    assert args.execution_slippage_hard_veto_r == 0.75
    assert args.execution_slippage_risk_threshold_r == 0.5
    assert args.max_distance_from_ema_pct == 8.0
    assert args.max_distance_from_vwap_pct == 7.0
    assert args.max_funding_rate == 0.0008
    assert args.max_funding_rate_avg == 0.0005
    assert args.binance_simulated_trading is False
    assert args.okx_simulated_trading is False
    assert args.base_url == 'https://fapi.binance.com'


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


def test_signed_get_resyncs_server_time_after_recvwindow_error(monkeypatch):
    mod = load_module()

    class Response:
        def __init__(self, status_code=200, payload=None, text=''):
            self.status_code = status_code
            self._payload = payload or {}
            self.text = text

        def json(self):
            return self._payload

    class Session:
        def __init__(self):
            self.headers = {}
            self.get_calls = []
            self.time_calls = 0
            self.account_calls = 0

        def get(self, url, params=None, timeout=15):
            self.get_calls.append((url, dict(params or {}), timeout))
            if url.endswith('/fapi/v1/time'):
                payload = {'serverTime': 2_000_000 if self.time_calls == 0 else 3_000_000}
                self.time_calls += 1
                return Response(payload=payload)
            self.account_calls += 1
            if self.account_calls == 1:
                return Response(status_code=400, payload={'code': -1021, 'msg': 'Timestamp for this request is outside of the recvWindow.'})
            return Response(payload={'assets': []})

    now_values = iter([1000.0, 1000.1, 1000.15, 2000.0, 2000.1, 2000.15])
    monkeypatch.setattr(mod.time, 'time', lambda: next(now_values))

    session = Session()
    client = mod.BinanceFuturesClient('https://example.test', api_key='k', api_secret='s', session=session)

    assert client.signed_get('/fapi/v2/account') == {'assets': []}
    assert session.time_calls == 2
    assert session.account_calls == 2
    first_account_params = session.get_calls[1][1]
    second_account_params = session.get_calls[3][1]
    assert first_account_params['timestamp'] == 2_000_100
    assert second_account_params['timestamp'] == 3_000_100
    assert first_account_params['recvWindow'] == 10_000
    assert second_account_params['recvWindow'] == 10_000


def test_signed_get_raises_original_error_when_recvwindow_resync_still_fails(monkeypatch):
    mod = load_module()

    class Response:
        def __init__(self, status_code=200, payload=None, text=''):
            self.status_code = status_code
            self._payload = payload or {}
            self.text = text

        def json(self):
            return self._payload

    class Session:
        def __init__(self):
            self.headers = {}
            self.time_calls = 0
            self.account_calls = 0

        def get(self, url, params=None, timeout=15):
            if url.endswith('/fapi/v1/time'):
                self.time_calls += 1
                return Response(payload={'serverTime': 2_000_000})
            self.account_calls += 1
            return Response(status_code=400, payload={'code': -1021, 'msg': 'Timestamp for this request is outside of the recvWindow.'})

    now_values = iter([1000.0, 1000.1, 1000.15, 1000.2, 1000.3, 1000.35])
    monkeypatch.setattr(mod.time, 'time', lambda: next(now_values))

    session = Session()
    client = mod.BinanceFuturesClient('https://example.test', api_key='k', api_secret='s', session=session)

    with pytest.raises(mod.BinanceAPIError, match='outside of the recvWindow'):
        client.signed_get('/fapi/v2/account')

    assert session.time_calls == 2
    assert session.account_calls == 2


def test_sync_server_time_uses_midpoint_without_negative_bias(monkeypatch):
    mod = load_module()

    class Response:
        status_code = 200

        def json(self):
            return {'serverTime': 2_000_000}

    class Session:
        def __init__(self):
            self.headers = {}

        def get(self, url, params=None, timeout=15):
            return Response()

    now_values = iter([1000.0, 1000.1])
    monkeypatch.setattr(mod.time, 'time', lambda: next(now_values))

    client = mod.BinanceFuturesClient('https://example.test', session=Session())

    assert client.sync_server_time() == 999_950


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
