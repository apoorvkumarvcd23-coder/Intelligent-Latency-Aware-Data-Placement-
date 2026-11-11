"""Repeatable sequence/phase-driven Zipf workload with explicit replay inputs."""
import random
import numpy as np
from common.contracts import event_identifier


class SeededWorkload:
    def __init__(self, cfg, start, run_id):
        self.cfg, self.start, self.run_id = cfg, float(start), run_id
        self.sequence = 0
        self.seed = cfg['workload']['seed']
        self.rng = np.random.default_rng(self.seed)
        self.order = [f'ds_{i:04d}' for i in range(1, cfg['datasets']['count']+1)]
        random.Random(self.seed).shuffle(self.order)
        ranks = np.arange(1, len(self.order)+1, dtype=float)
        self.weights = 1 / ranks ** cfg['workload']['zipf_s']
        self.weights /= self.weights.sum()
        self.per_phase = max(1, round(cfg['workload']['rate_per_sec'] * cfg['workload']['phase_seconds']))

    def phase_order(self, phase):
        offset = (phase * self.cfg['workload']['hot_set_size']) % len(self.order)
        return self.order[offset:] + self.order[:offset]

    def next_event(self):
        phase = self.sequence // self.per_phase
        order = self.phase_order(phase)
        event = {
            'version':1, 'event_id':event_identifier(f'{self.seed}:{self.run_id}', self.sequence),
            'dataset_id':order[int(self.rng.choice(len(order), p=self.weights))],
            'ts':self.start + self.sequence/self.cfg['workload']['rate_per_sec'],
            'phase':phase, 'size_kb':self.cfg['datasets']['size_kb'],
        }
        self.sequence += 1
        return event
