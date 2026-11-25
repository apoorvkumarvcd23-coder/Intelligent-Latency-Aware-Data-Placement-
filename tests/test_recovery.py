"""Injected adapter faults test recovery policy, not actual Hadoop availability."""
import copy
import hashlib
import io
import json
from pathlib import Path

import pytest
import yaml

from common.contracts import validate_config
from hdfs_client import CloudStore, StorageUnavailable


class RecoveryHdfs:
    def __init__(self):
        self.online = True
        self.files = {}
        self.writes = []
        self.fail_dataset = None
        self.corrupt_readback = False
        self.read_limits = []

    def status(self, path, strict=True):
        if not self.online:
            raise OSError('synthetic outage')
        return {} if path == '/' or path in self.files else None

    def makedirs(self, path):
        self.status('/')

    def write(self, path, data, overwrite=True):
        self.status('/')
        self.writes.append(path)
        if path.endswith('/' + str(self.fail_dataset)):
            self.files[path] = b'partial'
            raise OSError('synthetic interrupted upload')
        self.files[path] = data

    def read(self, path):
        self.status('/')
        owner = self

        class BoundedRead(io.BytesIO):
            def read(self, size=-1):
                owner.read_limits.append(size)
                return super().read(size)

        data = b'wrong-readback' if self.corrupt_readback else self.files[path]
        return BoundedRead(data)


@pytest.fixture
def setup_store(tmp_path):
    cfg = yaml.safe_load((Path(__file__).resolve().parents[1]/'config/config.yaml').read_text())
    cfg['cloud'].update(backend='auto',simulated_path=str(tmp_path/'mirror'))
    cfg = validate_config(cfg)
    hdfs = RecoveryHdfs()
    store = CloudStore(cfg,lambda:hdfs)
    store.write('ds_0001',b'first-trusted')
    store.write('ds_0002',b'second-trusted')
    hdfs.online = False
    assert store.read('ds_0001') == b'first-trusted'
    hdfs.writes.clear()
    return cfg,hdfs,store


@pytest.mark.parametrize('fault',['missing-manifest','empty-manifest','orphan','missing-later','corrupt-later'])
@pytest.mark.parametrize('restart',[False,True])
def test_incomplete_inventory_never_activates_or_mutates_hdfs(setup_store,fault,restart):
    cfg,hdfs,store = setup_store
    manifest = store.sim_path/'.mirror.json'
    saved_manifest = manifest.read_bytes()
    if fault == 'missing-manifest':
        manifest.unlink()
    elif fault == 'empty-manifest':
        manifest.write_text('{}')
    elif fault == 'orphan':
        (store.sim_path/'ds_0003').write_bytes(b'uncommitted-candidate')
    elif fault == 'missing-later':
        (store.sim_path/'ds_0002').unlink()
    else:
        (store.sim_path/'ds_0002').write_bytes(b'corrupt')
    before = dict(hdfs.files)
    hdfs.online = True
    if restart:
        store = CloudStore(cfg,lambda:hdfs)
    else:
        assert not store.revalidate()
    assert store.backend == 'simulated' and store.degraded
    assert hdfs.writes == [] and hdfs.files == before
    # Restore only the explicitly known test bytes; revalidation must then work.
    manifest.write_bytes(saved_manifest)
    (store.sim_path/'ds_0002').write_bytes(b'second-trusted')
    (store.sim_path/'ds_0003').unlink(missing_ok=True)
    assert store.revalidate() and store.backend == 'hdfs' and not store.degraded


def test_interrupted_second_upload_stays_degraded_and_retry_reconciles_all(setup_store):
    cfg,hdfs,store = setup_store
    store.write('ds_0003',b'created-offline')
    local = {f.name:f.read_bytes() for f in store.sim_path.iterdir()}
    hdfs.online, hdfs.fail_dataset = True,'ds_0002'
    assert not store.revalidate() and store.degraded and store.backend == 'simulated'
    assert hdfs.writes == ['/datasets/ds_0001','/datasets/ds_0002']
    assert store.read('ds_0002') == b'second-trusted'
    assert store.read('ds_0003') == b'created-offline'
    assert {f.name:f.read_bytes() for f in store.sim_path.iterdir()} == local
    hdfs.fail_dataset = None
    restarted = CloudStore(cfg,lambda:hdfs)
    assert restarted.backend == 'hdfs' and not restarted.degraded
    assert all(hdfs.files['/datasets/'+name] == data for name,data in local.items() if name.startswith('ds_'))


def test_bad_recovery_readback_is_bounded_and_prevents_activation(setup_store):
    _,hdfs,store = setup_store
    hdfs.online, hdfs.corrupt_readback = True,True
    assert not store.revalidate() and store.backend == 'simulated' and store.degraded
    assert store.read('ds_0001') == b'first-trusted'
    assert hdfs.read_limits[-1] == 1024**2+1
    hdfs.corrupt_readback = False
    assert store.revalidate()


@pytest.mark.parametrize('fault',['empty','duplicate'])
def test_invalid_manifest_never_marks_local_payload_as_verified(setup_store,fault):
    _,hdfs,store = setup_store
    first=hashlib.sha256(b'first-trusted').hexdigest()
    second=hashlib.sha256(b'second-trusted').hexdigest()
    manifest='{}' if fault=='empty' else (
        '{"ds_0001":"'+'0'*64+'","ds_0001":"'+first+'","ds_0002":"'+second+'"}'
    )
    (store.sim_path/'.mirror.json').write_text(manifest)
    hdfs.online = True
    assert not store.revalidate() and store.degraded
    assert hdfs.writes == []
    with pytest.raises(StorageUnavailable):
        store.read('ds_0001')


def test_uncommitted_atomic_candidates_are_never_uploaded(setup_store):
    _,hdfs,store = setup_store
    candidate=store.sim_path/'.ds_0003.synthetic.candidate'
    candidate.write_bytes(b'partial-not-a-managed-dataset')
    hdfs.online=True
    assert store.revalidate() and not store.degraded
    assert set(hdfs.files)=={'/datasets/ds_0001','/datasets/ds_0002'}
    assert candidate.read_bytes()==b'partial-not-a-managed-dataset'


def test_empty_digest_bound_mirror_cannot_be_recovered(setup_store):
    _,hdfs,store = setup_store
    (store.sim_path/'ds_0002').write_bytes(b'')
    manifest = json.loads((store.sim_path/'.mirror.json').read_text())
    manifest['ds_0002'] = hashlib.sha256(b'').hexdigest()
    (store.sim_path/'.mirror.json').write_text(json.dumps(manifest))
    hdfs.online = True
    assert not store.revalidate() and store.degraded and hdfs.writes == []


def test_strict_mode_runtime_denials_never_touch_even_corrupt_local_state(setup_store):
    cfg,hdfs,auto = setup_store
    (auto.sim_path/'.mirror.json').write_text('synthetic-broken-metadata')
    (auto.sim_path/'ds_0002').write_bytes(b'corrupt-local-payload')
    strict_cfg = copy.deepcopy(cfg)
    strict_cfg['cloud']['backend'] = 'hdfs'
    hdfs.online = True
    strict = CloudStore(strict_cfg,lambda:hdfs)
    before = {f.name:f.read_bytes() for f in auto.sim_path.iterdir()}
    hdfs.online = False
    for operation in (lambda:strict.read('ds_0001'),lambda:strict.write('ds_0001',b'new'),lambda:strict.exists('ds_0001')):
        with pytest.raises(StorageUnavailable,match='Strict HDFS'):
            operation()
    assert strict.backend == 'hdfs' and not strict.degraded
    assert {f.name:f.read_bytes() for f in auto.sim_path.iterdir()} == before
