"""Bounded per-process deduplication and metadata-only invalid-event records."""
from collections import OrderedDict
import hashlib
import json

from common.contracts import ContractError, parse_event


class EventGate:
    def __init__(self, cfg, redis_conn=None, max_seen=10000):
        self.cfg, self.redis = cfg, redis_conn
        self.seen = OrderedDict()
        self.max_seen = max_seen
        self.accepted = self.invalid = self.duplicates = 0

    def accept(self, raw, now):
        try:
            event = parse_event(raw, self.cfg, now)
        except ContractError:
            self.invalid += 1
            if self.redis is not None:
                key = self.cfg['metrics']['redis_key_prefix'] + ':invalid-events'
                sample = raw[:4096] if isinstance(raw, bytes) else b'nonbytes'
                record = json.dumps({'reason':'invalid_access_event',
                                     'payload_sha256':hashlib.sha256(sample).hexdigest()})
                pipe = self.redis.pipeline()
                pipe.lpush(key,record)
                pipe.ltrim(key,0,99)
                pipe.execute()
            return None
        identity = event['event_id']
        if identity in self.seen:
            self.duplicates += 1
            self.seen.move_to_end(identity)
            return None
        self.seen[identity] = None
        if len(self.seen) > self.max_seen:
            self.seen.popitem(last=False)
        self.accepted += 1
        return event
