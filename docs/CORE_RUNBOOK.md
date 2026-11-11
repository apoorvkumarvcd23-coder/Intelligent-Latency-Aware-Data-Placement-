# Placement core: reproducible safety and integration

Python 3.10+ supports the unit suite without Spark, Kafka or Redis services:

```sh
python -m venv .venv
# Activate the environment, then:
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

On Windows, if the system pytest temp directory is inaccessible, create
`artifacts/` and pass a fresh `--basetemp artifacts/check-<unique-name>`.
The symlink test may skip when Windows denies symlink creation; Linux CI runs it.

## Bounded actual-service replay

Docker Compose is required. This uses isolated Kafka, Redis, Java 17/PySpark
3.5.1 and the dashboard, with a disposable filesystem cloud. There is no exchange
data, model account, credential, employer code or external inference service.

```sh
docker compose -p placement-core -f compose.core-demo.yml build demo dashboard
docker compose -p placement-core -f compose.core-demo.yml up -d --wait kafka redis dashboard
docker compose -p placement-core -f compose.core-demo.yml run --rm demo
```

Open **http://127.0.0.1:8508**. Report: ignored `artifacts/placement-core.json`.
The replay uses eight one-KiB synthetic datasets, capacity two, seed 17, six
24-event cycles and three phases. It sends 144 valid requests, three malformed/
unsafe records and one exact duplicate through the real broker. The shared
live-engine event gate, Spark aggregation, MLlib training/prediction, placement,
Redis publisher and dashboard are exercised; no service/model adapter is mocked.
The report asserts 148 received = 144 accepted + 3 invalid + 1 duplicate, bounded
cache state, model readiness, changed placement and exact prior-prediction scores.

The clock is **historical event-time replay**, not a paced live workload or a
performance benchmark. The input trace hash is repeatable for the same config,
start time and run ID. Spark durations/floating-point coefficients are not claimed
byte-identical across machines. Kafka topic names are unique per run. The demo
explicitly resets only its own isolated Redis namespace; do not point it at a
personal Redis instance. Full-stack volumes/containers are separate from this demo.

Stop only this stack with:

```sh
docker compose -p placement-core -f compose.core-demo.yml down -v --remove-orphans
```

CI runs this clean-clone replay plus empty/populated browser checks. Its
`placement-core-evidence` artifact contains the actual JSON and PNG. The optional
browser script `--log-image` emits only the public-synthetic screenshot so an
agent can inspect that image through a text-only CI connector. No credentials or
private data are present in that page. A failed CI run is not completion evidence.

## Storage policy

- `simulated`: local dataset bytes plus a SHA-256 mirror manifest; no HDFS probe.
- `hdfs`: startup and operations require HDFS, with no local fallback dependency.
- `auto`: HDFS is preferred; only existing checksum-verified mirror bytes can be
  served during failure. Missing/corrupt bytes fail closed. Data written during
  fallback is uploaded and read back for verification before recovery switches
  to HDFS. Unexpected HDFS bytes do not overwrite an already verified mirror.

Auto mode assumes one application writer in its managed HDFS/mirror namespace.
It is not multi-writer synchronization. Mirror data/manifest use atomic file
replacement; a crash between replacements can leave a mismatch, which fails
closed. Broader crash/recovery drills are later gates. Initial legacy local files
without a manifest are not implicitly trusted. Use a fresh isolated demo store.
The deterministic seed verifies existing managed bytes and refuses a mismatch.

Dataset IDs must be canonical `ds_0001` style. Both storage and Redis boundaries
validate them. Local symlinks are rejected. Configuration rejects unknown keys,
bad enums, booleans-as-numbers, NaN/infinity and inconsistent budgets before opening
services. Total seed bytes are bounded to 256 MiB; edge bytes to 128 MiB.

## Event, cache and timing contracts

Version 1 events require event_id, dataset_id, finite timestamp, configured size
and nonnegative phase. Payloads are at most 4,096 bytes. The live consumer accepts
at most three rolling windows of age and five seconds of forward clock skew.
Invalid records persist only a reason and payload digest in a 100-record Redis
list. The per-process event-ID cache retains 10,000 IDs. Duplicates outside that
bound or after process restart may be observed again; there is no exactly-once claim.

Redis Lua updates dataset membership and bytes atomically and refuses new members
at capacity. Placement prefetches all source bytes before evicting old members, so
a failed source read leaves the old set intact. Reconciliation as a whole is not
a distributed transaction; one engine owns the namespace. Restart currently clears
its edge namespace and Kafka still uses auto-commit. Durable checkpoint/replay
semantics remain a later task. Clients and Spark are closed on normal interruption.

Every miss reads current cloud bytes; size-cache timing is not reused. The
dashboard separates measured Redis/cloud I/O from configured tier/WAN latency.
The combined number is a model plus measurement, not measured WAN latency. There
is no artificial WAN sleep. Throughput/bandwidth rates are normalized by decision
processing duration, not a distributed load benchmark. ML warm-up scores are
absent. Previous-cycle hot sets are scored on subsequent accepted events; rolling
feature/label windows overlap, so no held-out generalization claim follows.

## Remaining acceptance

Actual HDFS integration and complete full-stack walkthrough are CORE09 work.
Fair multi-seed/LRU benchmarks, persisted models, broader replay/recovery and full
v1 gates follow in December. Final core acceptance is still 17 November, with
no tag before its clean-clone/evidence audit. Local Docker failure must be repaired
for local container demos; clean Linux CI evidence does not claim that host repair.
