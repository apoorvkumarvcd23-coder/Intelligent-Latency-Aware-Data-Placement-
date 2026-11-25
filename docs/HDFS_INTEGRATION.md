# Real HDFS integration and recovery drill

## Verified 15 November 2026

Source `3e7bafe3c99bf58686ea95b9c05f3613d536aebe` passed
[clean Linux CI](https://github.com/apoorvkumarvcd23-coder/Intelligent-Latency-Aware-Data-Placement-/actions/runs/43633766007):
all 63 boundary tests, the retained simulated profile and the complete real-HDFS
replay/outage/recovery/browser path. [Permanent JSON evidence](hdfs-integration-2026-11-15.json)
includes all six phase-replay and four storage-drill snapshots, actual Hadoop
3.2.1/Spark 3.5.1 versions, ten verified HDFS file hashes and the artifact digest.
The replay reconciles 148 records = 144 accepted + three invalid + one duplicate.
The separate storage drill performs 96 accepted accesses across healthy warm-up,
healthy, outage and recovered cycles; it is not another Kafka ingestion count.

Inspected actual CI screenshots:
[strict HDFS phase replay](evidence/2026-11-15/placement-hdfs.png),
[healthy](evidence/2026-11-15/hdfs-healthy.png),
[outage and readable mirror warning](evidence/2026-11-15/hdfs-outage.png),
[recovered](evidence/2026-11-15/hdfs-recovered.png).
Native Windows passes 62 tests with one host symlink-permission skip and all
three Compose configuration checks. Its Docker engine remained unavailable;
the real Hadoop proof is the independent clean Linux run, not a host repair.

## Reproduce the isolated lab

This isolated lab adds an actual Hadoop NameNode/DataNode to the existing
Kafka/local Spark SQL and MLlib/Redis replay. `compose.core-demo.yml` continues
to use explicitly simulated filesystem cloud. `compose.hdfs-demo.yml` selects
strict HDFS for the six-cycle replay; it never accepts fallback as success.

The host needs Docker Compose v2, Python 3.10+ and the pinned browser
verifier. Allow several GB of free memory for Hadoop, Kafka, Spark and Chromium.
No public cloud, accounts or private datasets are used. Hadoop is unauthenticated
and its published UI is loopback-only; this is a disposable single-DataNode lab.

From a clean checkout, choose a unique project name beginning with `core09-`:

```sh
export COMPOSE_PROJECT_NAME=core09-local-demo
python -m pip install playwright==1.56.0
python -m playwright install chromium
mkdir -p artifacts
docker compose -f compose.hdfs-demo.yml build demo dashboard hdfs-drill
docker compose -f compose.hdfs-demo.yml up -d --wait --wait-timeout 240 kafka redis namenode datanode dashboard
docker compose -f compose.hdfs-demo.yml exec -T namenode hdfs dfsadmin -safemode wait
docker compose -f compose.hdfs-demo.yml run --rm demo
python scripts/check_dashboard.py --backend hdfs --output artifacts/placement-hdfs.png
python scripts/run_hdfs_demo.py
docker compose -f compose.hdfs-demo.yml down -v --remove-orphans
```

In PowerShell set `$env:COMPOSE_PROJECT_NAME = 'core09-local-demo'` and create
`artifacts` with `New-Item -ItemType Directory artifacts`. Always run cleanup
with that same dedicated project name: it deletes only this demo's volumes.
Do not use a shared or production Compose project. Stop a failed run and inspect
its JSON/logs before retrying; the coordinator gives every handshake a fresh ID.

The full replay reconciles 148 Kafka records into 144 accepted events, three
invalid records and one duplicate. The storage drill separately:

1. Writes/reads actual strict HDFS bytes and verifies no local strict mirror.
2. Reads managed files through an `auto` client to create SHA-256-bound mirrors.
3. Keeps those clients alive while the host stops both Hadoop containers.
   Strict read/write/list/exists and new-client startup must fail.
4. Reads the verified mirror, rejects missing/corrupt mirror bytes, and writes
   a new synthetic dataset offline. Actual decision cycles publish degraded
   metrics to real Redis; Chromium checks the degraded dashboard badge.
5. Starts the same Hadoop services/volumes, waits for health and safe-mode exit,
   and proves missing/empty metadata or an unlisted local payload prevents a
   restarted auto client from activating HDFS. A known canary in actual HDFS
   must remain unchanged through each denial. A corrupt later mirror must also
   prevent uploads of earlier datasets, not merely block final activation. After repair,
   explicit `revalidate()` uploads and reads back every managed dataset.
6. Verifies the offline-created file through the original strict client and
   records recovered decision metrics and a real browser screenshot.

The report is `artifacts/hdfs-drill.json`, with actual data hashes, strict
denials, recovery flags, Hadoop/Spark versions and all four decision snapshots.
The XL02 `restart_inventory_denials`,
`invalid_inventory_preserved_actual_hdfs_canary` and
`later_corrupt_mirror_prevented_earlier_dataset_upload` fields record the added
checks. These use the actual WebHDFS clients and filesystem faults, not an HDFS
adapter or patched network response. A partial remote upload/read-back fault is
separately injected in unit tests; the live drill does not claim to interrupt an
individual upload at an arbitrary byte boundary.
Four snapshots include healthy warm-up, healthy, degraded and recovered cycles.
Screenshots show healthy, degraded and recovered states. The `hdfs-integration`
CI job reproduces the clean-checkout path and uploads public synthetic evidence.
A successful job is required before calling the real-HDFS gate complete.

Recovery assumes a single managed writer; it reconciles the verified mirror
back to HDFS and does not merge external concurrent HDFS edits. There is no
automatic background health probe or high-availability guarantee. Tier/WAN
delay remains modelled separately from measured I/O. These tiny fixtures do not
establish production scale, a fair benchmark or an ML percentage improvement.
Final four-project acceptance remains 17 November; no release tag is implied.
