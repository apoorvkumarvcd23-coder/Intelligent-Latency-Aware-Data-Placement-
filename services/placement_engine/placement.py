"""
PlacementEngine — turns a predicted hot-set into physical data movement.

Given the target set of "hot" dataset ids (from EMA or MLlib), it reconciles the
edge cache: PROMOTE datasets that should be hot but aren't cached (copy bytes
cloud -> edge), and EVICT datasets that are cached but no longer hot (edge -> cloud
is just a delete, since the cloud copy is authoritative). Every move is counted as
migration overhead (count + bytes), one of the evaluation metrics.

It also owns the SERVE path: for each access it decides edge-hit (fast) vs
cloud-miss (slow + bandwidth), which produces the latency / hit-rate / bandwidth
metrics.
"""
import logging
import time
from common.contracts import dataset_id

log = logging.getLogger("placement")


class PlacementEngine:
    def __init__(self, cfg: dict, cloud, edge):
        self.cloud = cloud
        self.edge = edge
        self.capacity = int(cfg["edge"]["capacity"])
        self.edge_latency = float(cfg["edge"]["latency_ms"])
        self.cloud_latency = float(cfg["cloud"]["latency_ms"])

        # cumulative migration overhead
        self.migrations_total = 0
        self.migration_bytes_total = 0
        self.last_io_ms = 0.0
        self.last_modelled_ms = 0.0

    # ----- serve path -------------------------------------------------------
    def serve(self, ds: str):
        """
        Serve one access. Returns (latency_ms, hit: bool, cloud_bytes: int).
        Latency is MODELLED (tier latency) plus, on a real cloud read, the
        measured I/O time — so throughput stays high while the numbers are honest.
        """
        dataset_id(ds)
        started = time.perf_counter()
        data = self.edge.get(ds)
        hit = data is not None
        if not hit:
            data = self.cloud.read(ds)
        self.last_io_ms = (time.perf_counter()-started)*1000
        self.last_modelled_ms = self.edge_latency if hit else self.cloud_latency
        return self.last_modelled_ms + self.last_io_ms, hit, 0 if hit else len(data)

    # ----- placement reconciliation ----------------------------------------
    def reconcile(self, target_hot: list):
        """Make the edge cache hold exactly the top `capacity` of target_hot."""
        desired = list(dict.fromkeys(dataset_id(ds) for ds in target_hot))[: self.capacity]
        desired_set = set(desired)
        current = self.edge.members()

        to_promote = [d for d in desired if d not in current]
        to_evict = sorted(d for d in current if d not in desired_set)
        prepared = {ds: self.cloud.read(ds) for ds in to_promote}

        moved = 0
        moved_bytes = 0
        for ds in to_evict:
            self.edge.evict(ds)
            moved += 1
        for ds in to_promote:
            data = prepared[ds]
            self.edge.put(ds, data)
            moved += 1
            moved_bytes += len(data)

        self.migrations_total += moved
        self.migration_bytes_total += moved_bytes
        return {"promoted": len(to_promote), "evicted": len(to_evict),
                "migrations_cycle": moved, "migration_bytes_cycle": moved_bytes}

    def storage_utilization(self) -> float:
        return self.edge.count() / self.capacity if self.capacity else 0.0
