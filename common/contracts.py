"""Strict configuration and bounded versioned access events; no service imports."""
import copy
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
from typing import TypedDict

import yaml


class Config(TypedDict):
    kafka: dict
    cloud: dict
    edge: dict
    datasets: dict
    placement: dict
    ml: dict
    workload: dict
    metrics: dict


class ContractError(ValueError):
    pass


def dataset_id(value):
    if not isinstance(value, str) or not re.fullmatch(r'ds_[0-9]{4,6}', value):
        raise ContractError('Invalid dataset identifier')
    number = int(value[3:])
    if number < 1 or value != f'ds_{number:04d}':
        raise ContractError('Noncanonical dataset identifier')
    return value


NUMBERS = {
    'edge': {'redis_port': (1, 65535, int), 'latency_ms': (0, 60000, float), 'capacity': (1, 1000, int)},
    'cloud': {'latency_ms': (0, 60000, float)},
    'datasets': {'count': (1, 10000, int), 'size_kb': (1, 1024, int)},
    'placement': {'top_k': (1, 1000, int), 'window_seconds': (1, 3600, float),
                  'ema_alpha': (0.000001, 1, float), 'decision_interval_seconds': (0.1, 600, float)},
    'ml': {'retrain_interval_seconds': (0.1, 3600, float), 'min_samples': (2, 6000, int)},
    'workload': {'rate_per_sec': (0.1, 1000, float), 'zipf_s': (0.01, 10, float),
                 'phase_seconds': (1, 3600, float), 'hot_set_size': (1, 10000, int),
                 'seed': (0, 2**32-1, int)},
    'metrics': {'history_len': (2, 10000, int)},
}
TEXT = {'kafka': ['bootstrap_servers', 'topic'],
        'cloud': ['backend','webhdfs_url','hdfs_user','base_path','simulated_path'],
        'edge':['redis_host'], 'placement':['predictor'], 'metrics':['redis_key_prefix']}


def validate_config(raw) -> Config:
    if not isinstance(raw, dict) or set(raw) != set(NUMBERS) | {'kafka'}:
        raise ContractError('Configuration requires exactly the supported sections')
    cfg = copy.deepcopy(raw)
    if not isinstance(cfg.get('workload'), dict):
        raise ContractError('workload must be a mapping')
    cfg['workload'].setdefault('seed', 7)
    for section, fields in NUMBERS.items():
        values = cfg[section]
        if not isinstance(values, dict):
            raise ContractError(f'{section} must be a mapping')
        for key, (low, high, kind) in fields.items():
            value = values.get(key)
            if type(value) not in (int, float) or not low <= value <= high or not math.isfinite(value):
                raise ContractError(f'{section}.{key} must be finite in [{low}, {high}]')
            if kind is int and type(value) is not int:
                raise ContractError(f'{section}.{key} must be an integer')
            values[key] = kind(value)
    for section, fields in TEXT.items():
        if not isinstance(cfg[section], dict):
            raise ContractError(f'{section} must be a mapping')
        for key in fields:
            value = cfg[section].get(key)
            if not isinstance(value, str) or not value or len(value) > 300 or any(ord(c) < 32 for c in value):
                raise ContractError(f'{section}.{key} requires bounded text')
    for section, values in cfg.items():
        if set(values) != set(NUMBERS.get(section, {})) | set(TEXT.get(section, [])):
            raise ContractError(f'Unknown or missing {section} setting')
    if cfg['cloud']['backend'] not in {'auto','hdfs','simulated'} or cfg['placement']['predictor'] not in {'ema','ml','both'}:
        raise ContractError('Unknown backend or predictor policy')
    base = cfg['cloud']['base_path']
    if not base.startswith('/') or base == '/' or '..' in PurePosixPath(base).parts or str(PurePosixPath(base)) != base:
        raise ContractError('HDFS base_path must be a normalized absolute subdirectory')
    if not (Path(cfg['cloud']['simulated_path']).is_absolute() or PurePosixPath(cfg['cloud']['simulated_path']).is_absolute()):
        raise ContractError('simulated_path must be absolute')
    from urllib.parse import urlsplit
    url = urlsplit(cfg['cloud']['webhdfs_url'])
    if url.scheme != 'http' or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise ContractError('webhdfs_url requires a credential-free HTTP endpoint')
    for key in ('topic',):
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,99}', cfg['kafka'][key]):
            raise ContractError('Unsafe Kafka topic')
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,50}', cfg['metrics']['redis_key_prefix']):
        raise ContractError('Unsafe metrics prefix')
    if not cfg['placement']['top_k'] <= cfg['edge']['capacity'] <= cfg['datasets']['count']:
        raise ContractError('Require top_k <= edge capacity <= dataset count')
    if cfg['workload']['hot_set_size'] > cfg['datasets']['count']:
        raise ContractError('Hot set exceeds dataset count')
    if cfg['datasets']['count'] * cfg['datasets']['size_kb'] > 262144:
        raise ContractError('Synthetic dataset budget exceeds 256 MiB')
    if cfg['edge']['capacity'] * cfg['datasets']['size_kb'] > 131072:
        raise ContractError('Edge dataset budget exceeds 128 MiB')
    if cfg['placement']['decision_interval_seconds'] > cfg['placement']['window_seconds']:
        raise ContractError('Decision interval exceeds aggregation window')
    return cfg


def load_config(path=None) -> Config:
    configured = path or os.environ.get('PLACEMENT_CONFIG')
    if configured is None:
        configured = '/app/config/config.yaml'
        if not Path(configured).is_file():
            configured = Path(__file__).resolve().parents[1] / 'config/config.yaml'
    with Path(configured).open(encoding='utf-8') as source:
        return validate_config(yaml.safe_load(source))


def parse_event(raw, cfg, now):
    try:
        if not isinstance(raw, bytes) or not 1 <= len(raw) <= 4096:
            raise ValueError('bytes')
        def reject_constant(_):
            raise ValueError('nonfinite')
        event = json.loads(raw.decode('utf-8'), parse_constant=reject_constant)
        if not isinstance(event, dict) or set(event) != {'version','event_id','dataset_id','ts','size_kb','phase'}:
            raise ValueError('schema')
        if type(event['version']) is not int or event['version'] != 1:
            raise ValueError('version')
        ds = dataset_id(event['dataset_id'])
        if int(ds[3:]) > cfg['datasets']['count']:
            raise ValueError('unknown_dataset')
        if not isinstance(event['event_id'], str) or not re.fullmatch(r'[a-f0-9]{64}', event['event_id']):
            raise ValueError('event_id')
        ts = event['ts']
        if type(ts) not in (int, float) or not now-cfg['placement']['window_seconds']*3 <= ts <= now+5 or not math.isfinite(ts):
            raise ValueError('timestamp')
        if type(event['size_kb']) is not int or event['size_kb'] != cfg['datasets']['size_kb']:
            raise ValueError('size')
        if type(event['phase']) is not int or not 0 <= event['phase'] <= 1000000:
            raise ValueError('phase')
        return event
    except (ValueError, UnicodeError, TypeError, KeyError, RecursionError):
        raise ContractError('invalid_access_event') from None


def event_bytes(event):
    return json.dumps(event, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def event_identifier(seed, sequence):
    return hashlib.sha256(f'{seed}:{sequence}'.encode()).hexdigest()
