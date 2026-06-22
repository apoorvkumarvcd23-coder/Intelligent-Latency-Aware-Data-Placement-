# Self-contained Spark image: PySpark from PyPI on a Python + Java base.
# Avoids depending on any external Spark registry image (more reliable).
# bookworm (Debian 12) still ships openjdk-17 — the JVM Spark 3.5 officially supports.
FROM python:3.10-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN mkdir -p /data/cloud /app/model_store

ENV PYSPARK_PYTHON=python3 \
    PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_IP=127.0.0.1

# Spark runs in LOCAL mode (configured in build_spark()) — real PySpark + MLlib.
CMD ["python", "main.py"]
