"""
WindowAggregator — the Spark Streaming analytics step.

Each decision cycle the engine drains a micro-batch of access events from Kafka
and hands them here. We build a real Spark DataFrame and compute the windowed
per-dataset aggregation (count + last-access time) with Spark SQL. This is the
"Spark Streaming / locality-of-reference analysis" component of the syllabus,
implemented as reliable Spark micro-batches (no fragile Kafka-connector jars).
"""
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType


SCHEMA = StructType([
    StructField("dataset_id", StringType(), False),
    StructField("ts", DoubleType(), False),
])


class WindowAggregator:
    def __init__(self, spark):
        self.spark = spark

    def aggregate(self, events: list):
        """
        events: list of (dataset_id, ts) tuples for the current window.
        returns (counts: {ds: int}, last_ts: {ds: float})
        """
        if not events:
            return {}, {}
        df = self.spark.createDataFrame(events, schema=SCHEMA)
        rows = (
            df.groupBy("dataset_id")
              .agg(F.count("*").alias("cnt"), F.max("ts").alias("last_ts"))
              .collect()
        )
        counts = {r["dataset_id"]: int(r["cnt"]) for r in rows}
        last_ts = {r["dataset_id"]: float(r["last_ts"]) for r in rows}
        return counts, last_ts
