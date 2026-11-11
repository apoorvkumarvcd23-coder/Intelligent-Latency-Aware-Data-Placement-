"""
EdgeCache — the "edge" storage tier, backed by Redis.

Edge storage is fast and close to the user. We model a fixed-capacity edge node
that holds the bytes of whichever datasets the placement engine decides are hot.
Membership is tracked in a Redis SET; bytes live under per-dataset keys.
"""
import redis
from common.contracts import dataset_id

PUT = '''
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 and
   redis.call('SCARD', KEYS[1]) >= tonumber(ARGV[3]) then return 0 end
redis.call('SET', KEYS[2], ARGV[2])
redis.call('SADD', KEYS[1], ARGV[1])
return 1
'''
GET = '''
if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  redis.call('DEL', KEYS[2])
  return false
end
local value = redis.call('GET', KEYS[2])
if not value then redis.call('SREM', KEYS[1], ARGV[1]) end
return value
'''


class EdgeCache:
    def __init__(self, cfg: dict, client=None):
        e = cfg["edge"]
        self.capacity = int(e.get("capacity", 30))
        self.latency_ms = float(e.get("latency_ms", 2))
        prefix = cfg["metrics"]["redis_key_prefix"]
        self.members_key = f"{prefix}:edge:members"
        self.data_prefix = f"{prefix}:edge:data:"
        self.r = client if client is not None else redis.Redis(host=e["redis_host"], port=int(e["redis_port"]), db=0, socket_timeout=3)

    # raw redis handle reused by the metrics publisher
    def redis(self) -> redis.Redis:
        return self.r

    def _data_key(self, ds: str) -> str:
        return self.data_prefix + dataset_id(ds)

    def contains(self, ds: str) -> bool:
        return self.get(ds) is not None

    def members(self) -> set:
        return {m.decode() for m in self.r.smembers(self.members_key)}

    def count(self) -> int:
        return int(self.r.scard(self.members_key))

    def put(self, ds: str, data: bytes):
        if not isinstance(data, bytes) or not 1 <= len(data) <= 1024**2:
            raise ValueError('Edge data requires 1 byte to 1 MiB')
        if not self.r.eval(PUT, 2, self.members_key, self._data_key(ds), ds, data, self.capacity):
            raise RuntimeError('Edge capacity reached')

    def evict(self, ds: str):
        pipe = self.r.pipeline()
        pipe.delete(self._data_key(ds))
        pipe.srem(self.members_key, ds)
        pipe.execute()

    def get(self, ds: str) -> bytes:
        return self.r.eval(GET, 2, self.members_key, self._data_key(ds), ds)

    def clear(self):
        for ds in self.members():
            self.evict(ds)
