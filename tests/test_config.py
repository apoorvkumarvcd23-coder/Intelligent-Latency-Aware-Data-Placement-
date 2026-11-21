"""Configuration preflight needs only pytest/PyYAML, with no service clients."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

from common.contracts import ContractError, load_config, validate_config

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def raw():
    return yaml.safe_load((ROOT / 'config/config.yaml').read_text(encoding='utf-8'))


@pytest.mark.parametrize('profile', ['config.yaml', 'core-demo.yaml', 'core-hdfs.yaml'])
def test_checked_in_profiles_are_valid(profile):
    assert load_config(ROOT / 'config' / profile)['datasets']['count'] >= 8


def test_normalization_and_default_seed_do_not_alias_input_or_other_loads(raw):
    del raw['workload']['seed']
    original = copy.deepcopy(raw)
    normalized = validate_config(raw)
    assert raw == original and normalized['workload']['seed'] == 7
    assert type(normalized['cloud']['latency_ms']) is float
    normalized['edge']['capacity'] = 1
    assert validate_config(raw)['edge']['capacity'] == 30


@pytest.mark.parametrize('section,key,value', [
    ('ml', 'min_samples', 1), ('ml', 'min_samples', 6001),
    ('ml', 'retrain_interval_seconds', 0), ('placement', 'window_seconds', 3601),
    ('workload', 'rate_per_sec', 1001), ('workload', 'zipf_s', 0),
    ('workload', 'phase_seconds', 0), ('metrics', 'history_len', 10001),
    ('edge', 'redis_port', 0), ('edge', 'redis_port', 65536),
    ('edge', 'latency_ms', float('inf')), ('cloud', 'latency_ms', float('-inf')),
    ('workload', 'seed', True), ('workload', 'seed', 1.0),
])
def test_more_numeric_bounds_name_the_setting(raw, section, key, value):
    raw[section][key] = value
    with pytest.raises(ContractError, match=section + r'\.' + key):
        validate_config(raw)


@pytest.mark.parametrize('changes,message', [
    ({'placement': {'top_k': 31}}, 'top_k <= edge capacity'),
    ({'edge': {'capacity': 201}}, 'edge capacity <= dataset count'),
    ({'workload': {'hot_set_size': 201}}, 'Hot set'),
    ({'datasets': {'count': 257, 'size_kb': 1024}}, '256 MiB'),
    ({'datasets': {'count': 256, 'size_kb': 1024}, 'edge': {'capacity': 129}}, '128 MiB'),
    ({'placement': {'decision_interval_seconds': 11}}, 'Decision interval'),
])
def test_incompatible_settings_fail_as_a_whole(raw, changes, message):
    for section, values in changes.items():
        raw[section].update(values)
    with pytest.raises(ContractError, match=message):
        validate_config(raw)


def test_resource_budget_exact_limits_are_accepted(raw):
    raw['datasets'].update(count=256, size_kb=1024)
    raw['edge']['capacity'] = 128
    raw['placement']['ema_alpha'] = 1
    assert validate_config(raw)['datasets']['count'] == 256


@pytest.mark.parametrize('endpoint', [
    'http://namenode:invalid', 'http://namenode:65536', 'http://namenode:0',
    'http://[broken', 'http://namenode:-1',
])
def test_malformed_hdfs_endpoints_fail_before_client_construction(raw, endpoint):
    raw['cloud']['webhdfs_url'] = endpoint
    with pytest.raises(ContractError, match='webhdfs_url'):
        validate_config(raw)


@pytest.mark.parametrize('duplicate', ['section', 'setting', 'merge'])
def test_duplicate_yaml_keys_are_rejected_instead_of_silently_overriding(raw, tmp_path, duplicate):
    text = yaml.safe_dump(raw)
    if duplicate == 'section':
        text += '\nedge:\n  capacity: 1\n'
    elif duplicate == 'setting':
        text = text.replace('  capacity: 30', '  capacity: 30\n  capacity: 1')
    else:
        text = text.replace('edge:\n', 'edge:\n  <<: {capacity: 1}\n')
    path = tmp_path / 'duplicate.yaml'
    path.write_text(text, encoding='utf-8')
    with pytest.raises(ContractError, match='unique'):
        load_config(path)


@pytest.mark.parametrize('fault', ['malformed', 'tag', 'utf8', 'oversize', 'missing', 'directory'])
def test_config_file_errors_are_consistent_without_echoing_source(raw, tmp_path, fault):
    path = tmp_path / 'config.yaml'
    secret = 'synthetic-private-marker'
    if fault == 'malformed':
        path.write_text('cloud: [' + secret, encoding='utf-8')
    elif fault == 'tag':
        path.write_text('cloud: !!python/object:' + secret + ' {}', encoding='utf-8')
    elif fault == 'utf8':
        path.write_bytes(b'cloud: \xff' + secret.encode())
    elif fault == 'oversize':
        path.write_text(yaml.safe_dump(raw) + '\n#' + secret + 'x' * 65536, encoding='utf-8')
    elif fault == 'directory':
        path.mkdir()
    with pytest.raises(ContractError) as error:
        load_config(path)
    assert secret not in str(error.value)


def test_explicit_path_precedes_environment_and_invalid_override_never_falls_back(raw, tmp_path, monkeypatch):
    good = tmp_path / 'selected.yaml'
    good.write_text(yaml.safe_dump(raw), encoding='utf-8')
    monkeypatch.setenv('PLACEMENT_CONFIG', str(tmp_path / 'missing.yaml'))
    assert load_config(good)['edge']['capacity'] == 30
    with pytest.raises(ContractError):
        load_config()
    monkeypatch.setenv('PLACEMENT_CONFIG', str(good))
    with pytest.raises(ContractError):
        load_config('')


@pytest.mark.parametrize('service', ['placement_engine', 'workload_generator', 'dashboard'])
def test_repository_service_loaders_share_contract_without_runtime_imports(service):
    script = """
import importlib.abc, importlib.util, pathlib, sys
class NoServices(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'redis','kafka','pyspark','numpy','streamlit','hdfs','fakeredis'}:
            raise AssertionError('Unexpected runtime import: ' + fullname)
sys.meta_path.insert(0, NoServices())
from common.contracts import load_config
spec=importlib.util.spec_from_file_location('service_config',sys.argv[1])
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
assert module.load_config is load_config
assert module.load_config(sys.argv[2])['edge']['capacity']==30
"""
    result = subprocess.run(
        [sys.executable, '-c', script, str(ROOT / 'services' / service / 'config_util.py'),
         str(ROOT / 'config/config.yaml')],
        cwd=ROOT, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('profile', ['config.yaml', 'core-demo.yaml', 'core-hdfs.yaml'])
def test_actual_preflight_cli_reports_policy_without_connections_or_data_writes(profile, tmp_path):
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    selected = ROOT / 'config' / profile
    result = subprocess.run(
        [sys.executable, '-m', 'common.check_config', '--config', str(selected)],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    expected = load_config(selected)
    assert report['backend_policy'] == expected['cloud']['backend']
    assert report['synthetic_payload_bytes'] == expected['datasets']['count'] * expected['datasets']['size_kb'] * 1024
    assert report['connectivity'] == 'not checked'
    assert 'redis_host' not in report and 'webhdfs_url' not in report
    assert list(tmp_path.iterdir()) == []


def test_actual_preflight_rejection_has_no_partial_success_or_yaml_contents(tmp_path):
    path = tmp_path / 'bad.yaml'
    path.write_text('cloud: [synthetic-private-marker', encoding='utf-8')
    result = subprocess.run(
        [sys.executable, '-m', 'common.check_config', '--config', str(path)],
        cwd=ROOT, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 2 and result.stdout == ''
    assert 'valid UTF-8 YAML' in result.stderr
    assert 'synthetic-private-marker' not in result.stderr
