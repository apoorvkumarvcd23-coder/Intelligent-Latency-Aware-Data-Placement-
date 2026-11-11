# Current implemented claims

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

The tracked `.env` was privately reviewed: only recognized public image/version/
project settings, no credential fields. It is now untracked and ignored; Compose
has safe image defaults. `hadoop.env` is unauthenticated local-demo configuration.
The published ports are loopback-only. This is not an authenticated public service.

Unit tests use explicit failure-injection HDFS and deterministic model adapters;
they are not real HDFS or ML inference evidence. The integration replay uses real
Kafka, Spark MLlib and Redis, with explicitly **simulated filesystem cloud** and
a fixed event-time clock. Check the latest CI result before counting it as passed.
This small development trace is not an ML quality or throughput benchmark.

Still open: actual HDFS integration drill (CORE09), final clean-clone/claim audit,
full resume-ready acceptance, fair multi-seed/LRU experiments, model persistence,
durable replay/checkpoint boundaries and broader recovery/retention. No release
tag or measured improvement claim is earned by the safety slice alone.
