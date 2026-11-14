# Real HDFS integration and recovery drill

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
   and proves a corrupt mirror prevents recovery activation. After repair,
   explicit `revalidate()` uploads and reads back every managed dataset.
6. Verifies the offline-created file through the original strict client and
   records recovered decision metrics and a real browser screenshot.

The report is `artifacts/hdfs-drill.json`, with actual data hashes, strict
denials, recovery flags, Hadoop/Spark versions and all three decision snapshots.
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
