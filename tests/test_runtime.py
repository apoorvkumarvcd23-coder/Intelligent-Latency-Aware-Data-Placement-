from collections import Counter
import hashlib
import json

import fakeredis

from common.contracts import event_bytes
from common.events import EventGate
from common.workload import SeededWorkload
from edge_cache import EdgeCache
from hdfs_client import CloudStore
from ml_model import MlPredictor
from runtime import DecisionLoop
from seed import seed_cloud
from test_boundaries import cfg


def test_invalid_event_digest_only_and_bounded_replay_filter(cfg):
    connection = fakeredis.FakeRedis()
    gate = EventGate(cfg,connection,max_seen=2)
    trace = SeededWorkload(cfg,1000,'fixed')
    first = event_bytes(trace.next_event())
    assert gate.accept(first,1000)
    assert gate.accept(first,1000) is None
    for _ in range(120):
        assert gate.accept(b'synthetic-invalid-marker',1000) is None
    assert (gate.accepted,gate.duplicates,gate.invalid)==(1,1,120)
    rows = connection.lrange('lap:invalid-events',0,-1)
    assert len(rows)==100
    assert all(json.loads(row)=={'reason':'invalid_access_event',
        'payload_sha256':hashlib.sha256(b'synthetic-invalid-marker').hexdigest()} for row in rows)
    assert b'synthetic-invalid-marker' not in b''.join(rows)
    gate.accept(event_bytes(trace.next_event()),1000)
    gate.accept(event_bytes(trace.next_event()),1000)
    assert len(gate.seen)==2
    assert gate.accept(first,1000) is not None  # bounded in-process dedup, not exactly once


class PureAggregation:
    def aggregate(self,history):
        return dict(Counter(ds for ds,ts in history)),{ds:max(t for d,t in history if d==ds) for ds,ts in history}


def test_prior_forecasts_scored_before_current_training_and_io_labels_reconcile(cfg):
    cloud,edge = CloudStore(cfg),EdgeCache(cfg,fakeredis.FakeRedis())
    seed_cloud(cloud,cfg)
    model = MlPredictor(None,2)
    model.train = lambda:setattr(model,'model',True)  # deterministic unit adapter; real MLlib is CI-only
    model.predict = lambda features:{'ds_0003':10.0,'ds_0004':5.0}
    loop = DecisionLoop(cfg,cloud,edge,PureAggregation(),model)
    first = loop.cycle([{'dataset_id':'ds_0001','ts':1000.},{'dataset_id':'ds_0002','ts':1000.}],1000)
    assert first['ema_hitrate'] is None and first['ml_hitrate'] is None
    second = loop.cycle([{'dataset_id':'ds_0003','ts':1001.}],1001)
    assert second['previous_ema_hot_set']==['ds_0001','ds_0002'] and second['ema_hitrate']==0
    assert second['ml_hitrate'] is None and second['ml_ready']
    third = loop.cycle([{'dataset_id':'ds_0003','ts':1002.}],1002)
    assert third['previous_ml_hot_set']==['ds_0003','ds_0004'] and third['ml_hitrate']==1
    assert third['edge_count']<=2 and third['modelled_tier_latency_ms_avg']==2
    assert abs(third['avg_latency_ms']-third['measured_io_ms_avg']-2)<1e-9
