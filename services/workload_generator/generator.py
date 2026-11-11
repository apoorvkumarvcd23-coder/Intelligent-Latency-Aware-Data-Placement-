"""Seeded Zipf trace; phase follows event sequence rather than scheduler jitter."""
import json
import logging
import time
from uuid import uuid4

import redis
from kafka import KafkaProducer

from config_util import load_config
from common.contracts import event_bytes
from common.workload import SeededWorkload

log = logging.getLogger('workload')


def main():
    cfg = load_config()
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(bootstrap_servers=cfg['kafka']['bootstrap_servers'],linger_ms=20,retries=5)
        except Exception:
            log.warning('Kafka not ready; retrying')
            time.sleep(3)
    connection = redis.Redis(host=cfg['edge']['redis_host'],port=cfg['edge']['redis_port'],socket_timeout=3)
    trace = SeededWorkload(cfg,time.time(),uuid4().hex)
    phase = -1
    try:
        while True:
            event = trace.next_event()
            time.sleep(max(0,event['ts']-time.time()))
            if event['phase'] != phase:
                phase = event['phase']
                connection.set(cfg['metrics']['redis_key_prefix']+':workload:phase',json.dumps({
                    'phase':phase,'hot_set':trace.phase_order(phase)[:cfg['workload']['hot_set_size']]}))
            producer.send(cfg['kafka']['topic'],event_bytes(event))
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=10)
        connection.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
