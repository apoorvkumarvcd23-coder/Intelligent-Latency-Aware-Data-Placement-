# Current implemented claims

CORE09 source `3e7bafe3c99bf58686ea95b9c05f3613d536aebe` passes
[all three CI jobs](https://github.com/apoorvkumarvcd23-coder/Intelligent-Latency-Aware-Data-Placement-/actions/runs/43633766007),
including 63 Linux boundary checks, both storage profiles and actual Hadoop
stop/start with verified-mirror recovery. See [the permanent report](hdfs-integration-2026-11-15.json)
and [HDFS runbook/screenshots](HDFS_INTEGRATION.md). The older CORE06 evidence
below remains explicitly simulated filesystem cloud.

Verified source `ea7669e7e63b9c550797595b19f11270fa272036` passed
[CI run 43089970944](https://github.com/apoorvkumarvcd23-coder/Intelligent-Latency-Aware-Data-Placement-/actions/runs/43089970944),
including 63 boundary checks and the real-service/browser replay. See the
[recorded JSON](core-demo-2026-11-12.json), [actual screenshot](core-demo-2026-11-12.png)
and [runbook](CORE_RUNBOOK.md). Cloud storage for that replay was simulated.

| Behavior | Code | Executable check |
| --- | --- | --- |
| Shared bounded configuration fails before services start | `common/contracts.py`, three compatibility loaders | Invalid types/nonfinite/ranges/policies/keys and cross-field tests |
| Dataset IDs cannot become traversal paths or Redis key fragments | `dataset_id`, storage/cache boundaries | Invalid IDs and symlink rejection |
| Strict HDFS never silently switches backend | `CloudStore` | Startup, runtime read/write/list/status failure injection |
| Auto fallback accepts only verified mirror bytes and verifies recovery before switching | `CloudStore` | Missing/corrupt mirror, unexpected HDFS bytes, partial recovery and created-during-outage tests |
| Redis membership/data changes are atomic and capacity is bounded | `EdgeCache` Lua | Concurrent writers, missing bytes, orphan keys and count checks; real Redis demo |
| Source read failure preserves the previous edge set | `PlacementEngine.reconcile` | Prepare-before-evict failure regression |
| Each miss performs current I/O; modelled tier latency is separately labelled | `serve`, `DecisionLoop`, dashboard | Changed bytes/repeated miss checks and exact metric decomposition |
| Seeded trace, phase rotation and ranking are stable | `SeededWorkload`, EMA/ML ranking | Exact replay inputs, separate run IDs, tie ordering |
| Malformed/unknown/stale events do not reach placement | `parse_event`, `EventGate` | Raw JSON/UTF-8/size/version/time/ID cases; bounded digest-only rejection list and dedup |
| Forecasts are scored against later accepted events | `DecisionLoop` | Prior-set unit check and independently recomputed integration snapshot scores |
| Kafka, Spark SQL/MLlib, Redis and dashboard are exercised together | `demo_integration.py`, `check_dashboard.py`, CI | Passing workflow JSON report and actual browser screenshot artifact required |
| Real strict HDFS phase replay uses actual NameNode/DataNode storage | `compose.hdfs-demo.yml`, `core-hdfs.yaml`, `demo_integration.py` | 148 Kafka records reconcile to 144 accepted/3 invalid/1 duplicate; six HDFS snapshots, actual MLlib and browser |
| Strict HDFS runtime/startup fails during an observed real service outage | `demo_hdfs.py`, `run_hdfs_demo.py`, `CloudStore` | Host stops both Hadoop containers; original client read/write/exists/list and new-client startup denied; strict mirror never exists |
| Auto reads only verified mirrors and uploads offline-created data before recovery activation | `CloudStore`, live storage drill | Missing/corrupt bytes denied; corrupt mirror blocks activation; all ten files read back through actual HDFS after service restart |
| Degraded storage is readable in the dashboard | `services/dashboard/app.py`, `check_dashboard.py` | Actual outage screenshot and browser assertion require the full warning, not only a truncated metric |

The tracked `.env` was privately reviewed: only recognized public image/version/
project settings, no credential fields. It is now untracked and ignored; Compose
has safe image defaults. `hadoop.env` is unauthenticated local-demo configuration.
The published ports are loopback-only. This is not an authenticated public service.

Unit tests use explicit failure-injection HDFS and deterministic model adapters;
they are not real HDFS or ML inference evidence. The earlier CORE06 replay uses real
Kafka, Spark MLlib and Redis, with explicitly **simulated filesystem cloud** and
a fixed event-time clock. Check the latest CI result before counting it as passed.
This small development trace is not an ML quality or throughput benchmark.

The separate CORE09 profile exercises real HDFS as recorded above. Its four
storage-drill snapshots clear cache before access; its 96 accesses are separate
from the 148-record Kafka replay. The run is a single-DataNode, single-managed-
writer local lab; recovery reconciles its verified mirror, not concurrent
external HDFS writers. Native Windows Docker remained unavailable.

Still open: final clean-clone/claim audit,
full resume-ready acceptance, fair multi-seed/LRU experiments, model persistence,
durable replay/checkpoint boundaries and broader recovery/retention. No release
tag or measured improvement claim is earned by the safety slice alone.
