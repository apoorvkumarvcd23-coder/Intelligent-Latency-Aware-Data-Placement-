"""Live WebHDFS strict/fallback/recovery drill, coordinated by an owned host stack.

No fake HDFS client, subprocess mock, or offline result counts as this integration.
The host stops/restarts the actual NameNode and DataNode between handshake files.
"""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time

root = Path(__file__).resolve().parents[1]
if (root/'services/placement_engine').is_dir():
    sys.path[:0] = [str(root),str(root/'services/placement_engine')]

from config_util import load_config
from common.workload import SeededWorkload
from edge_cache import EdgeCache
from hdfs_client import CloudStore, StorageUnavailable
from main import build_spark
from metrics import MetricsPublisher
from ml_model import MlPredictor
from runtime import DecisionLoop
from seed import seed_cloud
from stream_processor import WindowAggregator


def wait_for(path):
    deadline = time.monotonic()+300
    while not path.is_file():
        if time.monotonic()>deadline:
            raise RuntimeError('Host did not complete owned HDFS transition: '+path.name)
        time.sleep(0.2)


def denied(operation):
    try:
        operation()
    except StorageUnavailable:
        return True
    raise AssertionError('An unavailable or unverified storage operation succeeded')


def demonstrate(cfg,control):
    cfg = copy.deepcopy(cfg)
    cfg['cloud'].update(backend='hdfs',base_path='/core09-drill',simulated_path='/data/cloud/strict-never-created')
    strict = CloudStore(cfg)
    assert strict.backend=='hdfs' and seed_cloud(strict,cfg)==8
    strict.write('ds_0009',b'written-to-actual-strict-hdfs')
    assert strict.read('ds_0009')==b'written-to-actual-strict-hdfs'
    assert not strict.sim_path.exists()
    cfg['cloud'].update(backend='auto',simulated_path='/data/cloud/verified-mirror')
    auto = CloudStore(cfg)
    expected = {ds:strict.read(ds) for ds in strict.list_datasets()}
    for ds,data in expected.items():
        assert auto.read(ds)==data  # Real HDFS reads populate digest-bound mirrors.
    spark=edge=None
    snapshots=[]
    try:
        spark=build_spark()
        edge=EdgeCache(cfg)
        prefix=cfg['metrics']['redis_key_prefix']
        edge.redis().delete(prefix+':metrics:latest',prefix+':metrics:history',prefix+':workload:phase')
        loop=DecisionLoop(cfg,auto,edge,WindowAggregator(spark),MlPredictor(spark,cfg['ml']['min_samples']))
        metrics=MetricsPublisher(cfg,edge.redis())
        trace=SeededWorkload(cfg,1700000100.0,'core09-hdfs-drill-v1')
        def cycle(stage):
            edge.clear()  # Force observed storage reads, not cache-only success.
            events=[trace.next_event() for _ in range(24)]
            phase=events[-1]['phase']
            snapshot=loop.cycle(events,events[-1]['ts'],phase)
            snapshot.update(clock_mode='hdfs-outage-recovery-drill',recorded_at=time.time(),
                            drill_stage=stage,
                            accepted_total=(len(snapshots)+1)*24,invalid_total=0,duplicates_total=0)
            metrics.publish(snapshot)
            edge.redis().set(cfg['metrics']['redis_key_prefix']+':workload:phase',json.dumps({
                'phase':phase,'hot_set':trace.phase_order(phase)[:2]}))
            snapshots.append(snapshot)
            return snapshot
        def signal(name,value):
            target=control/(name+'.json')
            candidate=control/(name+'.candidate')
            candidate.write_text(json.dumps(value,indent=2)+'\n')
            candidate.replace(target)
        cycle('healthy-warmup')
        healthy=cycle('healthy')
        assert healthy['cloud_backend']=='hdfs' and not healthy['cloud_degraded']
        signal('healthy',{'strict_write_read_verified':True,'strict_mirror_absent':True})
        wait_for(control/'outage-now')
        strict_failures={
            'read':denied(lambda:strict.read('ds_0001')),
            'write':denied(lambda:strict.write('ds_0009',b'must-not-be-written')),
            'exists':denied(lambda:strict.exists('ds_0001')),
            'list':denied(strict.list_datasets),
        }
        strict_cfg=copy.deepcopy(cfg)
        strict_cfg['cloud'].update(backend='hdfs',simulated_path=str(strict.sim_path))
        strict_failures['new_client_startup']=denied(lambda:CloudStore(strict_cfg))
        assert not strict.sim_path.exists()
        assert auto.read('ds_0001')==expected['ds_0001']
        assert auto.backend=='simulated' and auto.degraded
        mirror=auto.sim_path/'ds_0002'
        saved=mirror.read_bytes()
        mirror.rename(auto.sim_path/'ds_0002-held')
        try: missing_denied=denied(lambda:auto.read('ds_0002'))
        finally: (auto.sim_path/'ds_0002-held').rename(mirror)
        mirror.write_bytes(b'corrupt-synthetic-mirror')
        try: corrupt_denied=denied(lambda:auto.read('ds_0002'))
        finally: mirror.write_bytes(saved)
        auto.write('ds_0010',b'created-offline-in-verified-mirror')
        expected['ds_0010']=b'created-offline-in-verified-mirror'
        assert auto.read('ds_0010')==expected['ds_0010'] and not auto.revalidate()
        outage=cycle('outage')
        assert outage['cloud_backend']=='simulated' and outage['cloud_degraded']
        signal('outage',{'strict_denials':strict_failures,'missing_mirror_denied':missing_denied,
                         'corrupt_mirror_denied':corrupt_denied,'offline_write_verified':True})
        wait_for(control/'recovery-now')
        # A canary in actual HDFS proves failed preflight performs no dataset uploads.
        canary=b'actual-hdfs-preflight-canary'
        strict.write('ds_0001',canary)
        assert strict.read('ds_0001')==canary
        manifest_path=auto.sim_path/'.mirror.json'
        saved_manifest=manifest_path.read_bytes()
        inventory_denials={}
        orphan=auto.sim_path/'ds_0011'
        for fault in ['missing-manifest','empty-manifest','unlisted-payload']:
            try:
                if fault=='missing-manifest': manifest_path.unlink()
                elif fault=='empty-manifest': manifest_path.write_text('{}')
                else: orphan.write_bytes(b'partial-uncommitted-local-write')
                restarted=CloudStore(cfg)
                inventory_denials[fault]=(restarted.backend=='simulated' and restarted.degraded)
                assert inventory_denials[fault]
                assert strict.read('ds_0001')==canary
            finally:
                manifest_path.write_bytes(saved_manifest)
                orphan.unlink(missing_ok=True)
        # A bad mirror must not be promoted even when the actual HDFS service returns.
        mirror.write_bytes(b'corrupt-before-recovery')
        try:
            assert not auto.revalidate() and auto.backend=='simulated' and auto.degraded
            assert strict.read('ds_0001')==canary
        finally: mirror.write_bytes(saved)
        assert auto.revalidate() and auto.backend=='hdfs' and not auto.degraded
        for ds,data in expected.items():
            assert auto.read(ds)==data and strict.read(ds)==data
        assert not strict.sim_path.exists()
        recovered=cycle('recovered')
        assert recovered['cloud_backend']=='hdfs' and not recovered['cloud_degraded']
        report={
            'integration':'Actual WebHDFS NameNode/DataNode stop/start, real Redis and local Spark SQL/MLlib; no HDFS adapters',
            'status':'passed','strict_startup_and_write_read_verified':True,
            'strict_outage_denials':strict_failures,'strict_local_mirror_never_created':True,
            'verified_mirror_reads_during_outage':True,'missing_mirror_denied':missing_denied,
            'corrupt_mirror_denied':corrupt_denied,'offline_write_uploaded_and_read_back':True,
            'corrupt_mirror_prevented_recovery_activation':True,
            'restart_inventory_denials':inventory_denials,
            'invalid_inventory_preserved_actual_hdfs_canary':True,
            'later_corrupt_mirror_prevented_earlier_dataset_upload':True,
            'same_clients_used_across_actual_service_restart':True,
            'verified_hdfs_sha256':{ds:hashlib.sha256(data).hexdigest() for ds,data in expected.items()},
            'spark_version':spark.version,'snapshots':snapshots,
            'limits':'Single managed writer, one DataNode, synthetic bytes. Modelled WAN delay is not measured network latency; this is not a benchmark, exactly-once claim or production HA test.'}
        signal('recovered',report)
        print(json.dumps({'status':'passed','verified_hdfs_files':len(expected)}),flush=True)
    finally:
        if spark is not None: spark.stop()
        if edge is not None: edge.redis().close()


if __name__=='__main__':
    run=os.environ.get('HDFS_DRILL_RUN','')
    if not re.fullmatch(r'[a-f0-9]{32}',run):
        raise RuntimeError('Use scripts/run_hdfs_demo.py to coordinate an isolated run')
    control=Path('/artifacts/hdfs-control')/run
    if not control.is_dir() or any(control.iterdir()):
        raise RuntimeError('Expected a new empty host-owned handshake directory')
    demonstrate(load_config(),control)
