"""
EdgeCache — the "edge" storage tier, backed by Redis.

Edge storage is fast and close to the user. We model a fixed-capacity edge node
that holds the bytes of whichever datasets the placement engine decides are hot.
Membership is tracked in a Redis SET; bytes live under per-dataset keys.
"""
import redis


class EdgeCache:
    def __init__(self, cfg: dict):
        e = cfg["edge"]
        self.capacity = int(e.get("capacity", 30))
        self.latency_ms = float(e.get("latency_ms", 2))
        prefix = cfg["metrics"]["redis_key_prefix"]
        self.members_key = f"{prefix}:edge:members"
        self.data_prefix = f"{prefix}:edge:data:"
        self.r = redis.Redis(host=e["redis_host"], port=int(e["redis_port"]), db=0)

    # raw redis handle reused by the metrics publisher
    def redis(self) -> redis.Redis:
        return self.r

    def _data_key(self, ds: str) -> str:
        return self.data_prefix + ds

    def contains(self, ds: str) -> bool:
        return bool(self.r.sismember(self.members_key, ds))

    def members(self) -> set:
        return {m.decode() for m in self.r.smembers(self.members_key)}

    def count(self) -> int:
        return int(self.r.scard(self.members_key))

    def put(self, ds: str, data: bytes):
        pipe = self.r.pipeline()
        pipe.set(self._data_key(ds), data)
        pipe.sadd(self.members_key, ds)
        pipe.execute()

    def evict(self, ds: str):
        pipe = self.r.pipeline()
        pipe.delete(self._data_key(ds))
        pipe.srem(self.members_key, ds)
        pipe.execute()

    def get(self, ds: str) -> bytes:
        return self.r.get(self._data_key(ds))

    def clear(self):
        for ds in self.members():
            self.evict(ds)
