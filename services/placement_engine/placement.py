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

    # ----- serve path -------------------------------------------------------
    def serve(self, ds: str):
        """
        Serve one access. Returns (latency_ms, hit: bool, cloud_bytes: int).
        Latency is MODELLED (tier latency) plus, on a real cloud read, the
        measured I/O time — so throughput stays high while the numbers are honest.
        """
        if self.edge.contains(ds):
            return self.edge_latency, True, 0
        # miss -> fetch from cloud tier (real read, size memoized after first time)
        size = self.cloud.size(ds)
        latency = self.cloud_latency + self.cloud.last_read_ms
        return latency, False, size

    # ----- placement reconciliation ----------------------------------------
    def reconcile(self, target_hot: list):
        """Make the edge cache hold exactly the top `capacity` of target_hot."""
        desired = list(target_hot)[: self.capacity]
        desired_set = set(desired)
        current = self.edge.members()

        to_promote = [d for d in desired if d not in current]
        to_evict = [d for d in current if d not in desired_set]

        moved = 0
        moved_bytes = 0
        for ds in to_evict:
            self.edge.evict(ds)
            moved += 1
        for ds in to_promote:
            data = self.cloud.read(ds)        # real cloud read of the bytes to cache
            self.edge.put(ds, data)
            moved += 1
            moved_bytes += len(data)

        self.migrations_total += moved
        self.migration_bytes_total += moved_bytes
        return {"promoted": len(to_promote), "evicted": len(to_evict),
                "migrations_cycle": moved, "migration_bytes_cycle": moved_bytes}

    def storage_utilization(self) -> float:
        return self.edge.count() / self.capacity if self.capacity else 0.0
