"""Bounded real Kafka/Spark MLlib/Redis replay; explicitly simulated cloud."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import platform
import sys
import time
from uuid import uuid4

root = Path(__file__).resolve().parents[1]
if (root / 'services/placement_engine').is_dir():
    sys.path[:0] = [str(root),str(root / 'services/placement_engine')]

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from config_util import load_config
from common.contracts import event_bytes
from common.events import EventGate
from common.workload import SeededWorkload
from main import build_spark
from hdfs_client import CloudStore
from edge_cache import EdgeCache
from metrics import MetricsPublisher
from ml_model import MlPredictor
from runtime import DecisionLoop
from seed import seed_cloud
from stream_processor import WindowAggregator


def demonstrate(cfg):
    cfg = copy.deepcopy(cfg)
    cfg['kafka']['topic'] += '-'+uuid4().hex[:8]
    spark = producer = consumer = admin = edge = None
    try:
        spark = build_spark()
        cloud,edge = CloudStore(cfg),EdgeCache(cfg)
        edge.redis().ping()
        edge.clear()
        prefix = cfg['metrics']['redis_key_prefix']
        edge.redis().delete(prefix+':metrics:latest',prefix+':metrics:history',prefix+':invalid-events',prefix+':workload:phase')
        seed_cloud(cloud,cfg)
        assert seed_cloud(cloud,cfg) == 8
        loop = DecisionLoop(cfg,cloud,edge,WindowAggregator(spark),MlPredictor(spark,cfg['ml']['min_samples']))
        gate,metrics = EventGate(cfg,edge.redis()),MetricsPublisher(cfg,edge.redis())
        admin = KafkaAdminClient(bootstrap_servers=cfg['kafka']['bootstrap_servers'])
        admin.create_topics([NewTopic(cfg['kafka']['topic'],1,1)])
        consumer = KafkaConsumer(cfg['kafka']['topic'],bootstrap_servers=cfg['kafka']['bootstrap_servers'],
            group_id='core-demo-'+uuid4().hex,auto_offset_reset='earliest',enable_auto_commit=False)
        producer = KafkaProducer(bootstrap_servers=cfg['kafka']['bootstrap_servers'],acks='all')
        clock = 1700000000.0
        trace = SeededWorkload(cfg,clock,'core06-fixture-v1')
        snapshots,wire,received = [],[],0
        for cycle in range(6):
            events = [trace.next_event() for _ in range(24)]
            payloads = [event_bytes(e) for e in events]
            wire.extend(payloads)
            if cycle == 0:
                payloads += [b'not-json', b'\xff',event_bytes({**events[0],'dataset_id':'../escape'}),payloads[0]]
            futures = [producer.send(cfg['kafka']['topic'],value=raw) for raw in payloads]
            producer.flush(timeout=15)
            for future in futures:
                future.get(timeout=15)
            expected = received + len(payloads)
            batch = []
            deadline = time.monotonic()+30
            while received < expected and time.monotonic()<deadline:
                for messages in consumer.poll(timeout_ms=500,max_records=500).values():
                    for message in messages:
                        received += 1
                        event = gate.accept(message.value,clock+cycle+1)
                        if event:
                            batch.append(event)
            assert received == expected and len(batch)==24, 'Kafka/event counts do not reconcile'
            snapshot = loop.cycle(batch,clock+cycle+1,events[-1]['phase'])
            snapshot.update(clock_mode='seeded-event-time-replay',recorded_at=time.time(),
                            accepted_total=gate.accepted,invalid_total=gate.invalid,duplicates_total=gate.duplicates)
            metrics.publish(snapshot)
            snapshots.append(snapshot)
            edge.redis().set(cfg['metrics']['redis_key_prefix']+':workload:phase',json.dumps({
                'phase':events[-1]['phase'], 'hot_set':trace.phase_order(events[-1]['phase'])[:2]}))
            print(json.dumps({'cycle':cycle,'ml_ready':snapshot['ml_ready'],'edge':snapshot['edge_members']}),flush=True)
        assert received==148 and gate.accepted==144 and gate.invalid==3 and gate.duplicates==1
        assert any(s['ml_ready'] and s['ml_hot_set'] for s in snapshots)
        assert len({tuple(s['edge_members']) for s in snapshots}) > 1
        assert len({s['phase'] for s in snapshots}) == 3
        for snapshot in snapshots:
            assert snapshot['edge_count']<=snapshot['capacity']==2
            for field,previous in [('ema_hitrate','previous_ema_hot_set'),('ml_hitrate','previous_ml_hot_set')]:
                if snapshot[previous] is not None:
                    expected = sum(n for ds,n in snapshot['observed_counts'].items() if ds in snapshot[previous])/24
                    assert abs(snapshot[field]-expected)<1e-9
            assert abs(snapshot['avg_latency_ms']-snapshot['measured_io_ms_avg']-snapshot['modelled_tier_latency_ms_avg'])<1e-9
        assert edge.redis().llen(cfg['metrics']['redis_key_prefix']+':invalid-events')==3
        return {
            'integration':'actual Kafka broker, local Spark SQL/MLlib and Redis; no model/service mocks',
            'cloud_backend':cloud.backend,'hdfs_integration':False,
            'clock':'fixed historical event-time replay, not paced live traffic',
            'seed':17,'trace_sha256':hashlib.sha256(b'\n'.join(wire)).hexdigest(),
            'received':received,'accepted':gate.accepted,'invalid':gate.invalid,'duplicates':gate.duplicates,
            'spark_version':spark.version,'python_version':platform.python_version(),
            'snapshots':snapshots,
            'limits':'Six development cycles, not a benchmark or ML improvement claim. HDFS policy tests use failure-injection adapters; real HDFS drill and full release acceptance remain.',
        }
    finally:
        if producer is not None:
            producer.close(timeout=10)
        if consumer is not None:
            consumer.close()
        if admin is not None:
            admin.close()
        if spark is not None:
            spark.stop()
        if edge is not None:
            edge.redis().close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,default=Path('artifacts/placement-core.json'))
    args = parser.parse_args()
    report = demonstrate(load_config())
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2,allow_nan=False)+'\n')
    print('CORE06_REPORT_BEGIN\n'+json.dumps(report,indent=2,allow_nan=False)+'\nCORE06_REPORT_END')


if __name__ == '__main__':
    main()
