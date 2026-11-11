import copy
from concurrent.futures import ThreadPoolExecutor
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys

import fakeredis
import pytest
import yaml

from common.contracts import ContractError, dataset_id, parse_event, validate_config, event_bytes
from common.workload import SeededWorkload
from edge_cache import EdgeCache
from hdfs_client import CloudStore, StorageUnavailable
from placement import PlacementEngine
from popularity import EmaPopularity
from ml_model import MlPredictor
from seed import seed_cloud


@pytest.fixture
def cfg(tmp_path):
    raw = yaml.safe_load((Path(__file__).resolve().parents[1] / 'config/config.yaml').read_text())
    raw['cloud']['simulated_path'] = str(tmp_path / 'cloud')
    raw['cloud']['backend'] = 'simulated'
    raw['datasets'].update(count=8, size_kb=1)
    raw['edge']['capacity'] = 2
    raw['placement'].update(top_k=2, window_seconds=4, decision_interval_seconds=1)
    raw['workload'].update(hot_set_size=2, rate_per_sec=8, phase_seconds=3, seed=17)
    return validate_config(raw)


@pytest.mark.parametrize('section,key,value', [
    ('edge','capacity',0),('edge','capacity',True),('edge','capacity',1.5),('edge','capacity',10**1000),
    ('placement','ema_alpha',float('nan')),('workload','rate_per_sec',float('inf')),
    ('cloud','latency_ms',-1),('cloud','backend','typo'),('cloud','base_path','/datasets/../private'),
    ('cloud','webhdfs_url','http://name:secret@localhost'),('placement','predictor','unknown'),
    ('metrics','redis_key_prefix','unsafe:shared'),('workload','seed',-1),
    ('datasets','size_kb',1000000),('kafka','topic','../topic'),
    ('placement','decision_interval_seconds',9),('workload','hot_set_size',9),
])
def test_invalid_config_fails_before_services(cfg, section, key, value):
    cfg[section][key] = value
    with pytest.raises(ContractError):
        validate_config(cfg)


def test_config_rejects_unknown_or_missing_fields_without_mutating_input(cfg):
    original = copy.deepcopy(cfg)
    assert validate_config(cfg) == original and cfg == original
    cfg['cloud']['credential'] = 'never-accepted'
    with pytest.raises(ContractError):
        validate_config(cfg)
    del cfg['cloud']['credential']
    del cfg['edge']['redis_host']
    with pytest.raises(ContractError):
        validate_config(cfg)


@pytest.mark.parametrize('service',['placement_engine','workload_generator','dashboard'])
def test_shared_loader_works_in_flat_docker_application_layout(cfg,tmp_path,service):
    root = Path(__file__).resolve().parents[1]
    flat = tmp_path / 'app'
    flat.mkdir()
    shutil.copytree(root/'common',flat/'common',ignore=shutil.ignore_patterns('__pycache__'))
    shutil.copyfile(root/'services'/service/'config_util.py',flat/'config_util.py')
    config = flat/'config.yaml'
    config.write_text(yaml.safe_dump(cfg))
    result = subprocess.run([sys.executable,'-c',
        'from config_util import load_config; import sys; assert load_config(sys.argv[1])["edge"]["capacity"]==2',
        str(config)],cwd=flat,capture_output=True,text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('value', ['../ds_0001','/ds_0001','ds_0001/../x','ds_0001\\x',
                                   'ds_0000','ds_00001','ds_1','ds_0001:other','ds_１２３４',None])
def test_dataset_boundary_rejects_unsafe_ids(cfg, value):
    cloud = CloudStore(cfg)
    for operation in (cloud.read, cloud.size, cloud.exists, lambda v:cloud.write(v,b'new')):
        with pytest.raises(ContractError):
            operation(value)
    assert cloud.list_datasets() == []


class HdfsFixture:
    """Failure-injection adapter, explicitly not a real HDFS integration."""
    def __init__(self):
        self.online = True
        self.fail_read = self.fail_write = False
        self.files = {}
    def status(self, path, strict=True):
        if not self.online:
            raise OSError('fixture unavailable')
        return {} if path == '/' or path in self.files else None
    def makedirs(self, path):
        pass
    def list(self, path):
        self.status('/')
        return [p.rsplit('/',1)[1] for p in self.files]
    def write(self, path, data, overwrite):
        self.status('/')
        if self.fail_write:
            self.files[path] = b'partial'
            raise OSError('injected interrupted write')
        self.files[path] = data
    def read(self, path):
        self.status('/')
        if self.fail_read:
            raise OSError('injected read failure')
        return io.BytesIO(self.files[path])


def test_strict_hdfs_never_uses_mirror_even_after_runtime_failure(cfg):
    hdfs = HdfsFixture()
    cfg['cloud']['backend'] = 'hdfs'
    store = CloudStore(cfg, lambda:hdfs)
    store.write('ds_0001', b'authoritative')
    hdfs.online = False
    for action in (lambda:store.read('ds_0001'),lambda:store.write('ds_0001',b'new'),
                   lambda:store.exists('ds_0001'),store.list_datasets):
        with pytest.raises(StorageUnavailable, match='Strict HDFS'):
            action()
    assert store.backend == 'hdfs'
    assert not Path(cfg['cloud']['simulated_path']).exists()
    with pytest.raises(StorageUnavailable):
        CloudStore(cfg, lambda:hdfs)


def test_auto_verified_fallback_and_recovery_uploads_every_local_dataset(cfg):
    hdfs = HdfsFixture()
    cfg['cloud']['backend'] = 'auto'
    store = CloudStore(cfg, lambda:hdfs)
    store.write('ds_0001', b'first')
    hdfs.online = False
    assert store.read('ds_0001') == b'first'
    assert store.backend == 'simulated' and store.degraded
    store.write('ds_0002', b'created-during-outage')
    hdfs.online = True
    assert store.revalidate()
    assert store.backend == 'hdfs' and not store.degraded
    assert store.read('ds_0002') == b'created-during-outage'
    assert hdfs.files['/datasets/ds_0001'] == b'first'


def test_auto_unexpected_hdfs_content_does_not_replace_verified_mirror(cfg):
    hdfs = HdfsFixture()
    cfg['cloud']['backend'] = 'auto'
    store = CloudStore(cfg,lambda:hdfs)
    store.write('ds_0001',b'trusted')
    hdfs.files['/datasets/ds_0001'] = b'partial-corrupt-write'
    assert store.read('ds_0001') == b'trusted'
    assert store.degraded and store.backend == 'simulated'


def test_auto_restart_reconciles_offline_created_mirror_before_hdfs_activation(cfg):
    hdfs = HdfsFixture()
    hdfs.online = False
    cfg['cloud']['backend'] = 'auto'
    original = CloudStore(cfg,lambda:hdfs)
    original.write('ds_0001',b'created-offline')
    hdfs.online = True
    restarted = CloudStore(cfg,lambda:hdfs)
    assert restarted.backend == 'hdfs'
    assert hdfs.files['/datasets/ds_0001']==b'created-offline'


@pytest.mark.parametrize('fault',['missing','corrupt','metadata','partial-recovery'])
def test_auto_fallback_fails_closed_on_missing_or_corrupt_mirror(cfg, fault):
    hdfs = HdfsFixture()
    cfg['cloud']['backend'] = 'auto'
    store = CloudStore(cfg, lambda:hdfs)
    store.write('ds_0001', b'trusted')
    hdfs.online = False
    if fault == 'missing':
        store._sim_path('ds_0001').unlink()
    elif fault == 'corrupt':
        store._sim_path('ds_0001').write_bytes(b'corrupt')
    elif fault == 'metadata':
        (store.sim_path / '.mirror.json').write_text('{}')
    if fault != 'partial-recovery':
        with pytest.raises(StorageUnavailable):
            store.read('ds_0001')
    else:
        assert store.read('ds_0001') == b'trusted'
        hdfs.online, hdfs.fail_write = True, True
        assert not store.revalidate() and store.backend == 'simulated'
        assert store.read('ds_0001') == b'trusted'
        hdfs.fail_write = False
        assert store.revalidate()


def test_seed_replay_verifies_existing_bytes_and_every_miss_reads_current_data(cfg):
    cloud = CloudStore(cfg)
    assert seed_cloud(cloud,cfg) == seed_cloud(cloud,cfg) == 8
    edge = EdgeCache(cfg, fakeredis.FakeRedis())
    engine = PlacementEngine(cfg,cloud,edge)
    assert engine.serve('ds_0001')[2] == 1024
    cloud.write('ds_0001', b'new-current-bytes')
    assert engine.serve('ds_0001')[2] == len(b'new-current-bytes')
    assert cloud.size('ds_0001') == len(b'new-current-bytes') and cloud.last_read_ms == 0
    with pytest.raises(RuntimeError, match='differs'):
        seed_cloud(cloud,cfg)


def test_symlink_is_rejected_at_local_storage_boundary(cfg,tmp_path):
    cloud = CloudStore(cfg)
    outside = tmp_path / 'outside'
    outside.write_bytes(b'untouched')
    try:
        cloud._sim_path('ds_0001').symlink_to(outside)
    except OSError:
        pytest.skip('Host disallows creating symlinks')
    with pytest.raises(ContractError):
        cloud.write('ds_0001', b'bad')
    assert outside.read_bytes() == b'untouched'


def test_atomic_edge_capacity_and_membership_repair(cfg):
    client = fakeredis.FakeRedis()
    cache = EdgeCache(cfg,client)
    def put(i):
        try:
            cache.put(f'ds_{i:04d}', b'data')
            return True
        except RuntimeError:
            return False
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(put,range(1,9))) == 2
    assert cache.count() == 2
    member = next(iter(cache.members()))
    client.delete(cache._data_key(member))
    assert cache.get(member) is None and cache.count() == 1
    client.set(cache._data_key('ds_0008'), b'orphan')
    if 'ds_0008' not in cache.members():
        assert cache.get('ds_0008') is None


def test_reconcile_deduplicates_and_preserves_cache_on_failed_cloud_read(cfg):
    cloud, cache = CloudStore(cfg), EdgeCache(cfg,fakeredis.FakeRedis())
    seed_cloud(cloud,cfg)
    engine = PlacementEngine(cfg,cloud,cache)
    result = engine.reconcile(['ds_0001','ds_0001','ds_0002','ds_0003'])
    assert result['promoted'] == 2 and cache.members() == {'ds_0001','ds_0002'}
    assert engine.serve('ds_0001')[1]
    cloud._sim_path('ds_0003').unlink()
    with pytest.raises(StorageUnavailable):
        engine.reconcile(['ds_0003'])
    assert cache.members() == {'ds_0001','ds_0002'}
    with pytest.raises(ContractError):
        engine.reconcile(['ds_0001','../escape'])
    assert cache.count() == 2


def test_seeded_trace_is_repeatable_and_phase_rotation_changes_hot_set(cfg):
    a, b = SeededWorkload(cfg,1000,'fixed-run'), SeededWorkload(cfg,1000,'fixed-run')
    events = [a.next_event() for _ in range(80)]
    assert events == [b.next_event() for _ in range(80)]
    assert set(a.phase_order(0)[:2]).isdisjoint(a.phase_order(1)[:2])
    assert events[0]['phase']==0 and events[24]['phase']==1
    for event in events:
        assert parse_event(event_bytes(event),cfg,event['ts']) == event
    c = SeededWorkload(cfg,1000,'another-run')
    assert c.next_event()['event_id'] != events[0]['event_id']


@pytest.mark.parametrize('change', [
    {'dataset_id':'../escape'},{'dataset_id':'ds_9999'},{'version':2}, {'phase':-1},
    {'size_kb':2},{'event_id':'invalid'},{'ts':float('nan')},{'ts':900}, {'ts':1010},{'ts':10**1000},
])
def test_bad_event_is_rejected_without_source_text_logging(cfg,change):
    event = SeededWorkload(cfg,1000,'test').next_event()
    event.update(change)
    with pytest.raises(ContractError,match='invalid_access_event'):
        parse_event(json.dumps(event).encode(),cfg,1000)


@pytest.mark.parametrize('raw',[b'not JSON',b'\xff',b'[]',b'null',b'X'*4097,b'['*1500+b'0'+b']'*1500])
def test_malformed_raw_payload_isolated(cfg,raw):
    with pytest.raises(ContractError):
        parse_event(raw,cfg,1000)


def test_stable_ranking_and_prior_features_are_labelled_after_observation():
    ema = EmaPopularity(0.5)
    ema.update({'ds_0002':2,'ds_0001':2})
    assert ema.top_k(2)==['ds_0001','ds_0002']
    assert MlPredictor.top_k({'ds_0002':1,'ds_0001':1,'ds_0003':float('nan')},2)==['ds_0001','ds_0002']
    model = MlPredictor(None,2)
    previous = {'ds_0001':[1.,0.,0.5,0.,1.]}
    model.add_training_samples(previous, {'ds_0001':7})
    assert model.samples == [[1.,0.,0.5,0.,1.,7.]] and previous['ds_0001'][-1] == 1.
