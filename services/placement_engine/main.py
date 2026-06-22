"""
Placement engine — main control loop.

Per decision cycle:
  1. Drain a micro-batch of access events from Kafka.
  2. SERVE each access (edge hit = fast, cloud miss = slow + bandwidth) -> latency,
     hit-rate, bandwidth metrics.
  3. Spark windowed aggregation of the recent access history (locality analysis).
  4. Update the EMA baseline; engineer features and (periodically) train the Spark
     MLlib demand model; predict next-window demand.
  5. Compare EMA vs MLlib hot-sets head-to-head on this batch.
  6. Reconcile the edge cache to the chosen predictor's hot-set (promote/evict =
     migration overhead).
  7. Publish all metrics to Redis for the dashboard.

Spark runs in LOCAL mode in-process. The cloud tier auto-falls-back to a local
store if HDFS is down, so the loop never dies.
"""
import json
import logging
import time
from collections import deque

import redis
from kafka import KafkaConsumer
from pyspark.sql import SparkSession

from config_util import load_config
from hdfs_client import CloudStore
from edge_cache import EdgeCache
from seed import seed_cloud
from popularity import EmaPopularity
from ml_model import MlPredictor
from stream_processor import WindowAggregator
from placement import PlacementEngine
from metrics import MetricsPublisher

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("engine")


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("LatencyAwarePlacement")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def connect_consumer(cfg) -> KafkaConsumer:
    for attempt in range(40):
        try:
            return KafkaConsumer(
                cfg["kafka"]["topic"],
                bootstrap_servers=cfg["kafka"]["bootstrap_servers"],
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="placement-engine",
                consumer_timeout_ms=1000,
            )
        except Exception as e:  # noqa: BLE001
            log.warning("Kafka consumer not ready (%s); retry %d", e, attempt)
            time.sleep(3)
    raise RuntimeError("could not connect Kafka consumer")


def main():
    cfg = load_config()
    p = cfg["placement"]
    K = int(p["top_k"])
    window_seconds = float(p["window_seconds"])
    interval = float(p["decision_interval_seconds"])
    predictor_mode = p.get("predictor", "both")
    retrain_interval = float(cfg["ml"]["retrain_interval_seconds"])
    prefix = cfg["metrics"]["redis_key_prefix"]

    log.info("Starting placement engine — predictor=%s K=%d window=%.0fs interval=%.0fs",
             predictor_mode, K, window_seconds, interval)

    spark = build_spark()
    cloud = CloudStore(cfg)
    edge = EdgeCache(cfg)
    rconn = edge.redis()

    log.info("Seeding cloud tier (backend=%s)...", cloud.backend)
    n_datasets = seed_cloud(cloud, cfg)
    edge.clear()   # fresh edge for each run

    ema = EmaPopularity(cfg["placement"]["ema_alpha"])
    ml = MlPredictor(spark, cfg["ml"]["min_samples"])
    aggregator = WindowAggregator(spark)
    engine = PlacementEngine(cfg, cloud, edge)
    metrics = MetricsPublisher(cfg, rconn)

    consumer = connect_consumer(cfg)
    log.info("Connected to Kafka. Entering decision loop.")

    history = deque()              # (dataset_id, ts) within the rolling window
    prev_window_counts = {}
    prev_features = {}
    prev_ema_set = set()           # last cycle's EMA hot-set (for fair predictive comparison)
    prev_ml_set = set()            # last cycle's MLlib hot-set
    last_retrain = 0.0
    cycle = 0

    while True:
        cycle_start = time.time()

        # 1. drain a micro-batch (poll blocks up to `interval`)
        batch = []
        polled = consumer.poll(timeout_ms=int(interval * 1000), max_records=20000)
        for _tp, msgs in polled.items():
            for m in msgs:
                batch.append(m.value)

        # 2. serve each access
        served = hits = 0
        lat_sum = 0.0
        cloud_bytes = 0
        batch_counts = {}
        for ev in batch:
            ds = ev["dataset_id"]
            ts = float(ev["ts"])
            lat, hit, cb = engine.serve(ds)
            served += 1
            hits += 1 if hit else 0
            lat_sum += lat
            cloud_bytes += cb
            batch_counts[ds] = batch_counts.get(ds, 0) + 1
            history.append((ds, ts))

        # trim history to the rolling window
        now = time.time()
        cutoff = now - window_seconds
        while history and history[0][1] < cutoff:
            history.popleft()

        # 3. Spark windowed aggregation (locality-of-reference analysis)
        window_counts, last_ts = aggregator.aggregate(list(history))

        # 4. EMA baseline + feature engineering
        ema.update(batch_counts)
        features = {}
        candidate_ds = set(window_counts) | set(ema.scores)
        for ds in candidate_ds:
            recent = window_counts.get(ds, 0)
            prev = prev_window_counts.get(ds, 0)
            emav = ema.score_of(ds)
            recency = now - last_ts.get(ds, now - window_seconds * 3)
            recency = min(recency, window_seconds * 3)
            trend = recent - prev
            features[ds] = [float(recent), float(prev), float(emav), float(recency), float(trend)]

        # supervised learning: label last cycle's features with the count we just saw
        if prev_features:
            ml.add_training_samples(prev_features, window_counts)
        if ml.can_train() and (now - last_retrain) >= retrain_interval:
            try:
                ml.train()
                last_retrain = now
            except Exception as e:  # noqa: BLE001
                log.warning("ML training failed this cycle: %s", e)
        ml_preds = ml.predict(features) if ml.ready() else {}

        # 5. EMA vs MLlib — FAIR predictive comparison: each predictor's hot-set
        #    from the PREVIOUS cycle is scored against the batch it then had to
        #    serve (no peeking at the current batch). This is the honest "did the
        #    prediction hold up next window?" measure.
        ema_hr = (sum(1 for ev in batch if ev["dataset_id"] in prev_ema_set) / served) if served else 0.0
        ml_hr = (sum(1 for ev in batch if ev["dataset_id"] in prev_ml_set) / served) if served else 0.0

        ema_top = ema.top_k(K)
        ml_top = MlPredictor.top_k(ml_preds, K) if ml_preds else ema_top

        if predictor_mode == "ema":
            active = ema_top
        elif predictor_mode == "ml":
            active = ml_top if ml.ready() else ema_top
        else:  # both -> use ML once trained, else fall back to EMA
            active = ml_top if ml.ready() else ema_top

        # 6. reconcile edge to chosen hot-set (migration overhead)
        recon = engine.reconcile(active)

        # 7. publish metrics
        dur = max(time.time() - cycle_start, 1e-3)
        hit_rate = hits / served if served else 0.0
        snapshot = {
            "ts": now,
            "cycle": cycle,
            "served": served,
            "avg_latency_ms": round(lat_sum / served, 2) if served else 0.0,
            "cache_hit_rate": round(hit_rate, 4),
            "throughput_eps": round(served / dur, 1),
            "bandwidth_kbps": round(cloud_bytes / 1024.0 / dur, 1),
            "storage_util": round(engine.storage_utilization(), 3),
            "edge_count": edge.count(),
            "capacity": engine.capacity,
            "migrations_cycle": recon["migrations_cycle"],
            "migrations_total": engine.migrations_total,
            "migration_bytes_total": engine.migration_bytes_total,
            "ema_hitrate": round(ema_hr, 4),
            "ml_hitrate": round(ml_hr, 4),
            "predictor_active": predictor_mode if (predictor_mode != "both") else ("ml" if ml.ready() else "ema"),
            "ml_ready": ml.ready(),
            "ml_samples": len(ml.samples),
            "cloud_backend": cloud.backend,
            "known_datasets": len(ema.scores),
            "total_datasets": n_datasets,
            "phase": _current_phase(rconn, prefix),
        }
        metrics.publish(snapshot)

        if cycle % 1 == 0:
            log.info("cycle=%d served=%d hit=%.0f%% lat=%.1fms migr=%d backend=%s ml=%s "
                     "ema_hr=%.0f%% ml_hr=%.0f%%",
                     cycle, served, hit_rate * 100, snapshot["avg_latency_ms"],
                     recon["migrations_cycle"], cloud.backend, ml.ready(),
                     ema_hr * 100, ml_hr * 100)

        # occasionally re-check whether HDFS recovered
        if cycle % 20 == 0:
            cloud.revalidate()

        prev_window_counts = window_counts
        prev_features = features
        prev_ema_set = set(ema_top)
        prev_ml_set = set(ml_top)
        cycle += 1


def _current_phase(rconn, prefix):
    try:
        raw = rconn.get(f"{prefix}:workload:phase")
        if raw:
            return json.loads(raw).get("phase", -1)
    except Exception:  # noqa: BLE001
        pass
    return -1


if __name__ == "__main__":
    main()
