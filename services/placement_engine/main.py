"""Kafka polling -> validated events -> local Spark/MLlib decision cycles."""
import logging
import signal
import time

from kafka import KafkaConsumer
from pyspark.sql import SparkSession

from config_util import load_config
from common.events import EventGate
from hdfs_client import CloudStore
from edge_cache import EdgeCache
from seed import seed_cloud
from ml_model import MlPredictor
from stream_processor import WindowAggregator
from metrics import MetricsPublisher
from runtime import DecisionLoop

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s: %(message)s')
log = logging.getLogger('engine')


def build_spark():
    spark = (SparkSession.builder.master('local[2]').appName('LatencyAwarePlacement')
             .config('spark.sql.shuffle.partitions','2').config('spark.ui.showConsoleProgress','false')
             .config('spark.driver.memory','1g').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark


def connect_consumer(cfg):
    for attempt in range(40):
        try:
            return KafkaConsumer(cfg['kafka']['topic'],
                bootstrap_servers=cfg['kafka']['bootstrap_servers'],
                auto_offset_reset='latest', enable_auto_commit=True,
                group_id='placement-engine',consumer_timeout_ms=1000)
        except Exception:
            log.warning('Kafka not ready; retry %d',attempt+1)
            time.sleep(3)
    raise RuntimeError('Could not connect Kafka consumer')


def main():
    cfg = load_config()  # fail before opening services
    spark = consumer = edge = None
    def stop(_signum,_frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM,stop)
    try:
        spark = build_spark()
        cloud,edge = CloudStore(cfg),EdgeCache(cfg)
        edge.redis().ping()
        seed_cloud(cloud,cfg)
        edge.clear()  # explicitly documented fresh-cache startup, not durable replay
        loop = DecisionLoop(cfg,cloud,edge,WindowAggregator(spark),MlPredictor(spark,cfg['ml']['min_samples']))
        metrics,gate = MetricsPublisher(cfg,edge.redis()),EventGate(cfg,edge.redis())
        consumer = connect_consumer(cfg)
        interval = cfg['placement']['decision_interval_seconds']
        while True:
            started = time.monotonic()
            batch = []
            while time.monotonic()-started < interval and len(batch)<500:
                for messages in consumer.poll(timeout_ms=200,max_records=500-len(batch)).values():
                    for message in messages:
                        event = gate.accept(message.value,time.time())
                        if event:
                            batch.append(event)
            phase = batch[-1]['phase'] if batch else -1
            snapshot = loop.cycle(batch,time.time(),phase)
            snapshot.update(clock_mode='live',accepted_total=gate.accepted,invalid_total=gate.invalid,duplicates_total=gate.duplicates)
            metrics.publish(snapshot)
            log.info('cycle=%d accepted=%d invalid=%d duplicate=%d backend=%s model=%s',
                     snapshot['cycle'],gate.accepted,gate.invalid,gate.duplicates,cloud.backend,snapshot['predictor_active'])
            if snapshot['cycle'] % 20 == 0:
                cloud.revalidate()
    except KeyboardInterrupt:
        log.info('Stopping placement engine')
    finally:
        if consumer is not None:
            consumer.close()
        if spark is not None:
            spark.stop()
        if edge is not None:
            edge.redis().close()


if __name__ == '__main__':
    main()
