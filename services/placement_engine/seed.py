"""
Seed the cloud tier with synthetic dataset files (ds_0001 .. ds_NNNN).

Idempotent: skips datasets that already exist. Called on engine startup so the
demo is self-contained (no manual bootstrap step required), and also usable
standalone. Each dataset is `size_kb` of deterministic filler bytes.
"""
import logging

log = logging.getLogger("seed")


def dataset_ids(count: int) -> list:
    return [f"ds_{i:04d}" for i in range(1, count + 1)]


def seed_cloud(cloud, cfg: dict):
    count = int(cfg["datasets"]["count"])
    size_kb = int(cfg["datasets"]["size_kb"])
    payload = (b"LAP-EDGE-CLOUD-DATASET-" * 64)  # ~1.5 KB chunk of filler
    blob = (payload * ((size_kb * 1024) // len(payload) + 1))[: size_kb * 1024]

    existing = set()
    try:
        existing = set(cloud.list_datasets())
    except Exception:  # noqa: BLE001
        pass

    created = 0
    for ds in dataset_ids(count):
        if ds in existing:
            continue
        cloud.write(ds, blob)
        created += 1
    log.info("seed: backend=%s datasets=%d created=%d size_kb=%d",
             cloud.backend, count, created, size_kb)
    return count
