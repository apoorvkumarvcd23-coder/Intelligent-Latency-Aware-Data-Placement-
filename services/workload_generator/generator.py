"""
Synthetic workload generator.

Emits dataset access events to Kafka following a Zipfian popularity law (a few
datasets get most of the requests — realistic locality of reference). Crucially,
the *hot set* SHIFTS every `phase_seconds`: we reshuffle which datasets occupy
the popular Zipf ranks. That shift is what forces the placement engine to adapt,
and it's the moment you point at during the demo (hit-rate dips, then recovers).

The current phase + hot set are also written to Redis so the dashboard can show
"what's actually hot right now" alongside "what the engine placed at the edge".
"""
import json
import time
import random
import logging

import numpy as np
import redis
from kafka import KafkaProducer

from config_util import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("workload")


def main():
    cfg = load_config()
    w = cfg["workload"]
    N = int(cfg["datasets"]["count"])
    size_kb = int(cfg["datasets"]["size_kb"])
    s = float(w["zipf_s"])
    hot_n = int(w["hot_set_size"])
    phase_seconds = float(w["phase_seconds"])
    rate = float(w["rate_per_sec"])
    interval = 1.0 / rate if rate > 0 else 0.025

    topic = cfg["kafka"]["topic"]
    prefix = cfg["metrics"]["redis_key_prefix"]

    ids = [f"ds_{i:04d}" for i in range(1, N + 1)]
    ranks = np.arange(1, N + 1, dtype=float)
    weights = 1.0 / np.power(ranks, s)
    weights /= weights.sum()

    # Connect to Kafka (retry until the broker is reachable).
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=cfg["kafka"]["bootstrap_servers"],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=20,
                retries=5,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("Kafka not ready (%s); retrying in 3s", e)
            time.sleep(3)

    r = redis.Redis(host=cfg["edge"]["redis_host"], port=int(cfg["edge"]["redis_port"]), db=0)

    def publish_phase(phase: int, order: list):
        hot = order[:hot_n]
        r.set(f"{prefix}:workload:phase",
              json.dumps({"phase": phase, "hot_set": hot, "hot_set_size": hot_n}))
        log.info("PHASE %d begins — new hot set (top %d): %s ...", phase, hot_n, hot[:8])

    order = ids[:]
    random.shuffle(order)               # rank -> dataset mapping (reshuffled each phase)
    phase = 0
    publish_phase(phase, order)
    phase_start = time.time()
    sent = 0

    while True:
        now = time.time()
        if now - phase_start >= phase_seconds:
            phase += 1
            order = ids[:]
            random.shuffle(order)
            publish_phase(phase, order)
            phase_start = now

        rank_idx = int(np.random.choice(N, p=weights))
        ds = order[rank_idx]
        event = {"dataset_id": ds, "ts": now, "size_kb": size_kb, "phase": phase}
        producer.send(topic, event)
        sent += 1
        if sent % 200 == 0:
            log.info("emitted %d events (phase %d)", sent, phase)
        time.sleep(interval)


if __name__ == "__main__":
    main()
