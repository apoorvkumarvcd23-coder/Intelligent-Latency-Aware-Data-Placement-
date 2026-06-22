"""
MlPredictor — the Spark MLlib demand-prediction model.

For each dataset we engineer features from the streaming aggregation:
    recent  : access count in the current window
    prev    : access count in the previous window
    ema     : EMA popularity score
    recency : seconds since last access (capped)
    trend   : recent - prev
The label is the NEXT window's access count (supervised: we observe it one cycle
later). A Spark MLlib LinearRegression learns demand = f(features); datasets are
ranked by predicted next-window demand to choose the edge set.

Compared head-to-head with the EMA baseline every cycle (see main.py), this shows
whether learning the trend/recency signal beats a plain moving average.
"""
import logging

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

log = logging.getLogger("ml")

FEATURES = ["recent", "prev", "ema", "recency", "trend"]


class MlPredictor:
    def __init__(self, spark, min_samples: int, max_buffer: int = 6000):
        self.spark = spark
        self.min_samples = int(min_samples)
        self.max_buffer = max_buffer
        self.model = None
        self.samples = []          # list of [recent, prev, ema, recency, trend, label]
        self.last_train_size = 0

    def add_training_samples(self, prev_features: dict, observed_counts: dict):
        """Pair last cycle's features with the count we just observed (the label)."""
        for ds, feats in prev_features.items():
            label = float(observed_counts.get(ds, 0))
            self.samples.append(list(feats) + [label])
        if len(self.samples) > self.max_buffer:
            self.samples = self.samples[-self.max_buffer:]

    def can_train(self) -> bool:
        return len(self.samples) >= self.min_samples

    def ready(self) -> bool:
        return self.model is not None

    def train(self):
        cols = FEATURES + ["label"]
        df = self.spark.createDataFrame(self.samples, cols)
        df = VectorAssembler(inputCols=FEATURES, outputCol="features").transform(df)
        lr = LinearRegression(featuresCol="features", labelCol="label",
                              regParam=0.1, elasticNetParam=0.0, maxIter=25)
        self.model = lr.fit(df)
        self.last_train_size = len(self.samples)
        log.info("MLlib model trained on %d samples (R2=%.3f)",
                 self.last_train_size, self.model.summary.r2)

    def predict(self, features: dict) -> dict:
        """features: {ds: [recent, prev, ema, recency, trend]} -> {ds: predicted demand}."""
        if not self.model or not features:
            return {}
        rows = [(ds, *vals) for ds, vals in features.items()]
        df = self.spark.createDataFrame(rows, ["dataset_id"] + FEATURES)
        df = VectorAssembler(inputCols=FEATURES, outputCol="features").transform(df)
        out = self.model.transform(df).select("dataset_id", "prediction").collect()
        return {r["dataset_id"]: float(r["prediction"]) for r in out}

    @staticmethod
    def top_k(predictions: dict, k: int) -> list:
        return [ds for ds, _ in sorted(predictions.items(), key=lambda x: -x[1])[:k]]
