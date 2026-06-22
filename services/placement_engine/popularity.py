"""
EmaPopularity — the statistical baseline predictor.

Exponential moving average of per-window access counts. Datasets accessed this
window get a boost; datasets not accessed decay. Top-K by EMA score = the edge
candidates. Cheap, no training, easy to explain — the baseline we compare the
MLlib model against.
"""


class EmaPopularity:
    def __init__(self, alpha: float):
        self.alpha = float(alpha)
        self.scores = {}

    def update(self, window_counts: dict):
        """window_counts: {dataset_id: count_in_this_window}."""
        seen = set(self.scores) | set(window_counts)
        for ds in seen:
            c = float(window_counts.get(ds, 0))
            prev = self.scores.get(ds, 0.0)
            self.scores[ds] = self.alpha * c + (1.0 - self.alpha) * prev

    def top_k(self, k: int) -> list:
        return [ds for ds, _ in sorted(self.scores.items(), key=lambda x: -x[1])[:k]]

    def score_of(self, ds: str) -> float:
        return self.scores.get(ds, 0.0)
