import importlib.util
import json
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / 'scripts'
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

runtime_store = __import__('runtime_store')
SCRIPT_PATH = SCRIPTS_DIR / 'runtime_state_backup_hardening.py'
spec = importlib.util.spec_from_file_location('runtime_state_backup_hardening', SCRIPT_PATH)
mod = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def _store(tmp_path):
    cls = runtime_store.RuntimeStateStore
    mod.install_runtime_state_backup_hardening(runtime_store)
    return cls(str(tmp_path))


def _corrupt_all_backups(tmp_path: Path, filename: str) -> None:
    backups = list(tmp_path.glob(f'{filename}.bak*'))
    assert backups
    for backup in backups:
        backup.write_text('{broken-backup', encoding='utf-8')


def test_successful_save_creates_matching_backup(tmp_path):
    store = _store(tmp_path)
    payload = {'BTCUSDT:LONG': {'symbol': 'BTCUSDT', 'side': 'long', 'quantity': 0.01, 'entry_price': 60000}}
    store.save_json('positions', payload)

    primary = tmp_path / 'positions.json'
    backup = tmp_path / 'positions.json.bak'
    assert primary.exists()
    assert backup.exists()
    assert primary.read_text(encoding='utf-8') == backup.read_text(encoding='utf-8')


def test_corrupted_primary_positions_recovers_from_backup(tmp_path):
    store = _store(tmp_path)
    payload = {'BTCUSDT:LONG': {'symbol': 'BTCUSDT', 'side': 'long', 'quantity': 0.01, 'entry_price': 60000}}
    store.save_json('positions', payload)
    (tmp_path / 'positions.json').write_text('{broken', encoding='utf-8')

    recovered, error = store.load_json_with_error('positions', {})
    assert 'BTCUSDT:LONG' in recovered
    assert error is not None
    assert error['recovered_from_backup'] is True
    assert error['backup_file'] == 'positions.json.bak'

    normal = store.load_json('positions', {})
    assert 'BTCUSDT:LONG' in normal


def test_corrupted_primary_runtime_state_recovers_from_backup(tmp_path):
    store = _store(tmp_path)
    expected = {'cooldown_until': 123.4, 'runtime_mode': 'live'}
    store.save(expected)
    (tmp_path / 'runtime_state.json').write_text('{broken', encoding='utf-8')

    assert store.load() == expected


def test_corrupted_primary_runtime_state_with_all_backups_bad_fails_closed(tmp_path):
    store = _store(tmp_path)
    store.save({'runtime_mode': 'live'})
    (tmp_path / 'runtime_state.json').write_text('{broken-primary', encoding='utf-8')
    _corrupt_all_backups(tmp_path, 'runtime_state.json')

    assert store.load() == {}


def test_primary_and_all_position_backups_corrupted_returns_default_with_error(tmp_path):
    store = _store(tmp_path)
    store.save_json('positions', {'ETHUSDT:SHORT': {'symbol': 'ETHUSDT', 'side': 'short', 'quantity': 0.2, 'entry_price': 3000}})
    (tmp_path / 'positions.json').write_text('{broken-primary', encoding='utf-8')
    _corrupt_all_backups(tmp_path, 'positions.json')

    recovered, error = store.load_json_with_error('positions', {'sentinel': True})
    assert recovered == {'sentinel': True}
    assert error is not None
    assert error['state_file'] == 'positions.json'
    assert error['backup_file'] == 'positions.json.bak'
    assert error['backup_error_type']


def test_backup_is_refreshed_on_each_successful_save(tmp_path):
    store = _store(tmp_path)
    store.save_json('settings', {'generation': 1})
    store.save_json('settings', {'generation': 2})
    (tmp_path / 'settings.json').write_text('{broken', encoding='utf-8')

    recovered, error = store.load_json_with_error('settings', {})
    assert recovered == {'generation': 2}
    assert error is not None
    assert error['recovered_from_backup'] is True


def test_direct_backup_rotation_preserves_three_generations_and_recovers_older(tmp_path):
    path = tmp_path / 'direct.json'
    mod._write_backup(path, {'generation': 1})
    mod._write_backup(path, {'generation': 2})
    mod._write_backup(path, {'generation': 3})

    assert json.loads((tmp_path / 'direct.json.bak').read_text(encoding='utf-8')) == {'generation': 3}
    assert json.loads((tmp_path / 'direct.json.bak.1').read_text(encoding='utf-8')) == {'generation': 2}
    assert json.loads((tmp_path / 'direct.json.bak.2').read_text(encoding='utf-8')) == {'generation': 1}

    (tmp_path / 'direct.json.bak').write_text('{broken-latest-backup', encoding='utf-8')
    recovered, meta = mod._load_backup(path)
    assert recovered == {'generation': 2}
    assert meta is not None
    assert meta['recovered_from_backup'] is True
    assert meta['backup_file'] == 'direct.json.bak.1'
    assert meta['backup_generation'] == 1


def test_installation_is_idempotent(tmp_path):
    mod.install_runtime_state_backup_hardening(runtime_store)
    first_atomic = runtime_store.RuntimeStateStore._atomic_write_json
    first_load = runtime_store.RuntimeStateStore.load
    mod.install_runtime_state_backup_hardening(runtime_store)
    assert runtime_store.RuntimeStateStore._atomic_write_json is first_atomic
    assert runtime_store.RuntimeStateStore.load is first_load

    store = runtime_store.RuntimeStateStore(str(tmp_path))
    store.save_json('state', {'ok': True})
    assert store.load_json('state', {}) == {'ok': True}
