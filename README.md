# Intelligent Latency-Aware Data Placement & Workload Optimization in Edge-Cloud Systems

## December 2026 delivery

The December v1 target hardens the existing placement lab with automated tests, strict backend policies and reproducible evaluation. WAN latency remains explicitly modelled.

See the [implementation plan and acceptance checks](docs/IMPLEMENTATION_PLAN.md).
The core demo target is **17 November 2026**; feature delivery and hardening
continue through **31 December 2026**. Completion requires verified evidence.

**CORE06 safety slice:** shared strict configuration, versioned bounded events,
safe dataset paths, strict HDFS failure behavior, checksum-verified auto fallback,
atomic Redis capacity, deterministic workload/ranking and explicit measured versus
modelled latency are implemented. A bounded real Kafka/Spark MLlib/Redis replay
and browser check are available in the [core runbook](docs/CORE_RUNBOOK.md).
Use the [CI workflow](https://github.com/apoorvkumarvcd23-coder/Intelligent-Latency-Aware-Data-Placement-/actions/workflows/checks.yml)
and its JSON/PNG artifact for integration evidence; unit adapters alone do not
prove real services. See the [claim ledger](docs/CLAIM_EVIDENCE.md).
Real HDFS integration, broader recovery/benchmarking and final acceptance remain;
there is no core/release tag yet.

**Big Data Systems — course project**
*Hadoop HDFS · local Spark SQL · Apache Kafka · Spark MLlib · Redis · Docker*

A working system that **monitors user access patterns in real time, predicts which
datasets will be hot, and automatically moves data between a fast "edge" tier and a
large "cloud" tier to minimise query latency.** Hot data is promoted to the edge;
cold data is demoted to the cloud. The system is fully containerised and runs with a
single command.

> 📖 **New here? Read [docs/HOW_IT_WORKS.md](docs/HOW_IT_WORKS.md)** — a complete
> guide that explains everything twice (once like you're five, once for real),
> including the full implementation, how to run it, and how to give the demo.

---

## 1. The problem

Traditional systems use **static** data placement — everything lives in the cloud, or
at a fixed edge location. When user access patterns change, this causes high latency,
wasted bandwidth, and poor use of scarce edge storage. We need placement that **adapts
to the live workload**.

## 2. The idea

```
            access events                 predict + decide              physically move
 users  ───────────────────►  Kafka  ──►  local Spark SQL  ──►  Placement engine  ──►  Edge ⇄ Cloud
                                          + EMA + MLlib              (promote/evict)
```

- **Monitor**: every access is an event on **Kafka** (`access-logs` topic).
- **Analyse**: Python polls Kafka; **local Spark SQL** aggregates accesses per dataset over rolling windows
  (locality-of-reference analysis).
- **Predict**: two predictors run side-by-side — an **EMA** statistical baseline and a
  **Spark MLlib** regression model that forecasts next-window demand from
  count / recency / trend features.
- **Place**: the **placement engine** keeps the top-K predicted-hot datasets on the
  edge (promote), and evicts the rest (demote). Every move is counted as migration
  overhead.
- **Serve**: a request hitting the edge is fast (~2 ms); a miss is served from the
  cloud (HDFS read **+80 ms** modelled WAN latency) and consumes bandwidth.

## 3. Architecture

See [docs/architecture.mmd](docs/architecture.mmd) and [docs/data-flow.mmd](docs/data-flow.mmd)
(render at <https://mermaid.live> or with the VS Code *Markdown Preview Mermaid* extension).

| Container | Role | Tech |
|-----------|------|------|
| `kafka` | access-log event bus | Apache Kafka (KRaft, no Zookeeper) |
| `namenode` + `datanode` | **cloud** storage tier | Hadoop HDFS |
| `redis` | **edge** cache tier + metrics/state store | Redis |
| `engine` | streaming + ML + placement + serving | Apache Spark (local mode) + MLlib |
| `workload` | synthetic users (Zipfian, shifting hot-set) | Python |
| `dashboard` | live metrics UI | Streamlit |

### Edge vs Cloud = latency tiers
- **Edge = Redis** — fast, capacity-limited cache near the user (`edge.latency_ms`, `edge.capacity`).
- **Cloud = HDFS** — large, authoritative, far away. A real HDFS read stays in the request
  path; an additional modelled WAN latency (`cloud.latency_ms`) represents the distance.

### Reliability by design (important)
Two engineering choices make this **run reliably** on a laptop, which is the priority here:

1. **Spark runs in local mode** inside the engine container (`local[*]`) — still real
   PySpark DataFrame micro-batches + real MLlib; this is not Structured Streaming or a remote Spark cluster, with no fragile
   standalone-cluster networking.
2. **The cloud tier has an automatic fallback.** `engine` talks to HDFS through a
   `CloudStore` abstraction. If HDFS is unreachable (slow start, crash, or you stop it
   mid-demo), `auto` policy switches to a **checksum-verified local mirror**.
   Missing/corrupt mirrors fail closed. Strict `hdfs` policy never falls back.
   Controlled by `cloud.backend`
   (`hdfs` | `simulated` | `auto`, default `auto`). The dashboard shows policy,
   active backend and degradation. WAN/tier latency is modelled, not slept or measured.

   Kafka consumption uses `kafka-python` micro-batches (then processed with Spark
   DataFrames) instead of the Spark-Kafka connector jar — one less thing that can break.

## 4. Quick start

**Prerequisites:** Docker Desktop running, ~4–4.5 GB free for the stack, ~6 GB disk.

```powershell
# Windows (PowerShell) — from the project root
./scripts/demo.ps1
```
```bash
# Linux / macOS / Git-Bash
./scripts/demo.sh
```

Or manually:
```bash
docker compose up -d --build
```

Then open:
- **Dashboard (metrics):** <http://localhost:8501>
- **Spark UI:** <http://localhost:4040>
- **HDFS NameNode UI:** <http://localhost:9870>

Stop with `docker compose down` (add `-v` to wipe HDFS/edge data too).

### What you'll see (the demo story)
The workload generator keeps a **Zipfian** hot set and **shifts it every ~60 s**. Watch
the dashboard:
1. Inspect which datasets are promoted as observed demand changes.
2. At a phase shift, compare the previous hot set with the new accesses.
3. Measure whether the selected predictor adapts; recovery speed and any gain
   depend on the trace and model. Negative results are valid evidence.
4. The **EMA vs MLlib** chart compares both predictors on identical live traffic.

## 5. Evaluation metrics (all on the dashboard)

| Metric | Meaning |
|--------|---------|
| **Average query latency** | mean serve time (edge hits cheap, cloud misses expensive) |
| **Cache hit rate** | fraction of accesses served from the edge |
| **Throughput** | accesses served per second |
| **Cloud bandwidth** | bytes/s pulled from the cloud tier on misses |
| **Storage utilization** | edge slots used / capacity |
| **Migration overhead** | count + bytes of promote/evict moves |
| **EMA vs MLlib hit rate** | head-to-head predictor accuracy on live traffic |

## 6. Configuration

Everything is tunable in one place — [config/config.yaml](config/config.yaml) — and
applied on restart. Useful knobs:

- `placement.predictor`: `ema` | `ml` | `both` (default `both`: use ML once trained, EMA before)
- `placement.top_k`, `edge.capacity`: how much data the edge holds
- `cloud.latency_ms`, `edge.latency_ms`: the tier latency gap
- `workload.zipf_s`, `workload.hot_set_size`, `workload.phase_seconds`, `workload.rate_per_sec`
- `cloud.backend`: `hdfs` requires HDFS, `simulated` uses local bytes, `auto` permits verified fallback

## 7. Mapping to the Big Data Systems syllabus

| Syllabus concept | Where it appears |
|------------------|------------------|
| Data storage & **locality of reference** | hot/cold classification, edge promotion |
| **Hadoop & HDFS** | cloud storage tier (`namenode`/`datanode`, WebHDFS) |
| **Distributed architectures** | multi-service edge-cloud design over Docker network |
| **Apache Spark** | engine runs on Spark; DataFrame aggregation |
| **Spark Streaming** | windowed micro-batch analysis of the Kafka access stream |
| **Apache Kafka** | real-time access-log ingestion |
| **ML-based analytics** | Spark MLlib demand prediction vs EMA baseline |

## 8. Project layout

```
docker-compose.yml          config/config.yaml      hadoop.env
docs/                       architecture.mmd, data-flow.mmd
scripts/                    demo.ps1, demo.sh, bootstrap_hdfs.sh
services/placement_engine/  main.py stream_processor.py popularity.py ml_model.py
                            placement.py hdfs_client.py edge_cache.py metrics.py seed.py
services/workload_generator/ generator.py
services/dashboard/         app.py
```

## 9. Troubleshooting

- **Dashboard says "waiting…"** — give it a minute; the engine seeds data and trains the
  model before the first metrics. Check `docker compose logs -f engine`.
- **HDFS UI not loading / engine shows a degraded mirror** — auto mode serves only
  verified mirror bytes and verifies the complete mirror on recovery. Strict HDFS
  stops on failure. Configured simulated mode is labelled separately.
- **Out of memory** — lower `workload.rate_per_sec`, or `docker compose down` other stacks.
- **Reset everything** — `docker compose down -v && docker compose up -d --build`.
