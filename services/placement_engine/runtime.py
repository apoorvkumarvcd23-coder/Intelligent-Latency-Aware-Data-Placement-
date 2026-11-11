"""One decision cycle shared by the live consumer and bounded integration demo."""
from collections import deque
import time

from ml_model import MlPredictor
from popularity import EmaPopularity
from placement import PlacementEngine


class DecisionLoop:
    def __init__(self, cfg, cloud, edge, aggregator, model):
        self.cfg, self.cloud, self.edge = cfg,cloud,edge
        self.aggregator, self.ml = aggregator,model
        self.ema = EmaPopularity(cfg['placement']['ema_alpha'])
        self.engine = PlacementEngine(cfg,cloud,edge)
        self.history = deque()
        self.previous_counts, self.previous_features = {},{}
        self.previous_ema = self.previous_ml = None
        self.last_train = float('-inf')
        self.cycle_id, self.history_dropped = 0,0

    def cycle(self, batch, now, phase=-1):
        started = time.perf_counter()
        served = len(batch)
        batch_counts = {}
        hits = cloud_bytes = 0
        io_ms = modelled_ms = 0.0
        for event in batch:
            ds = event['dataset_id']
            _, hit, size = self.engine.serve(ds)
            hits += int(hit)
            cloud_bytes += size
            io_ms += self.engine.last_io_ms
            modelled_ms += self.engine.last_modelled_ms
            batch_counts[ds] = batch_counts.get(ds,0)+1
            self.history.append((ds,float(event['ts'])))
        horizon = self.cfg['placement']['window_seconds']
        current = sorted((e for e in self.history if now-horizon <= e[1] <= now+5),key=lambda e:e[1])
        self.history_dropped += max(0,len(current)-20000)
        self.history = deque(current[-20000:])
        window_counts,last_ts = self.aggregator.aggregate(list(self.history))
        # Score the prediction that existed before this batch was observed/trained.
        def score(previous):
            return (sum(count for ds,count in batch_counts.items() if ds in previous)/served
                    if previous is not None and served else None)
        ema_hr,ml_hr = score(self.previous_ema),score(self.previous_ml)
        old_ema = sorted(self.previous_ema) if self.previous_ema is not None else None
        old_ml = sorted(self.previous_ml) if self.previous_ml is not None else None
        self.ema.update(batch_counts)
        features = {}
        for ds in sorted(set(window_counts) | set(self.ema.scores)):
            recent,prev = window_counts.get(ds,0),self.previous_counts.get(ds,0)
            recency = max(0,min(now-last_ts.get(ds,now-horizon*3),horizon*3))
            features[ds] = [float(recent),float(prev),self.ema.score_of(ds),float(recency),float(recent-prev)]
        if self.previous_features:
            self.ml.add_training_samples(self.previous_features,window_counts)
        training_failed = False
        if self.ml.can_train() and now-self.last_train >= self.cfg['ml']['retrain_interval_seconds']:
            try:
                self.ml.train()
                self.last_train = now
            except Exception:
                training_failed = True
        predictions = self.ml.predict(features) if self.ml.ready() else {}
        k = self.cfg['placement']['top_k']
        ema_top = self.ema.top_k(k)
        ml_top = MlPredictor.top_k(predictions,k) if predictions else []
        mode = self.cfg['placement']['predictor']
        active_ml = mode != 'ema' and bool(ml_top)
        active = ml_top if active_ml else ema_top
        recon = self.engine.reconcile(active)
        duration = max(time.perf_counter()-started,0.000001)
        snapshot = {
            'ts':now,'recorded_at':time.time(),'cycle':self.cycle_id,'served':served,
            'avg_latency_ms':(io_ms+modelled_ms)/served if served else 0,
            'measured_io_ms_avg':io_ms/served if served else 0,
            'modelled_tier_latency_ms_avg':modelled_ms/served if served else 0,
            'latency_semantics':'modelled tier/WAN plus measured Redis/filesystem/HDFS I/O; no WAN delay sleep',
            'cache_hit_rate':hits/served if served else 0,
            'throughput_eps':served/duration,'bandwidth_kbps':cloud_bytes/1024/duration,
            'storage_util':self.engine.storage_utilization(),'edge_count':self.edge.count(),
            'capacity':self.engine.capacity,**recon,
            'migrations_total':self.engine.migrations_total,
            'migration_bytes_total':self.engine.migration_bytes_total,
            'ema_hitrate':ema_hr,'ml_hitrate':ml_hr,
            'previous_ema_hot_set':old_ema,'previous_ml_hot_set':old_ml,
            'ema_hot_set':ema_top,'ml_hot_set':ml_top,'edge_members':sorted(self.edge.members()),
            'observed_counts':batch_counts,'predictor_active':'ml' if active_ml else 'ema',
            'ml_ready':self.ml.ready(),'ml_samples':len(self.ml.samples),'ml_training_failed':training_failed,
            'cloud_backend':self.cloud.backend,'cloud_policy':self.cloud.policy,
            'cloud_degraded':self.cloud.degraded,'known_datasets':len(self.ema.scores),
            'total_datasets':self.cfg['datasets']['count'],'phase':phase,
            'history_dropped':self.history_dropped,
            'prediction_scoring':'previous-cycle hot sets against current accepted events; rolling feature windows overlap',
        }
        self.previous_counts,self.previous_features = window_counts,features
        self.previous_ema,self.previous_ml = set(ema_top),set(ml_top) if ml_top else None
        self.cycle_id += 1
        return snapshot
