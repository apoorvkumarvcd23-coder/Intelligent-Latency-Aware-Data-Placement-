"""
MetricsPublisher — writes evaluation metrics to Redis for the dashboard.

Stores a `:latest` JSON snapshot plus a capped `:history` list so the dashboard
can draw time-series charts without its own database.
"""
import json


class MetricsPublisher:
    def __init__(self, cfg: dict, redis_conn):
        self.r = redis_conn
        prefix = cfg["metrics"]["redis_key_prefix"]
        self.latest_key = f"{prefix}:metrics:latest"
        self.history_key = f"{prefix}:metrics:history"
        self.history_len = int(cfg["metrics"]["history_len"])

    def publish(self, snapshot: dict):
        payload = json.dumps(snapshot)
        pipe = self.r.pipeline()
        pipe.set(self.latest_key, payload)
        pipe.lpush(self.history_key, payload)
        pipe.ltrim(self.history_key, 0, self.history_len - 1)
        pipe.execute()
