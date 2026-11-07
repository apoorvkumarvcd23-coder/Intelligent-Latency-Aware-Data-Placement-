# Intelligent Latency-Aware Data Placement — December 2026 plan

## Immediate core milestone — 17 November 2026

The existing application becomes a resume-ready core within the next ten days:
validated configuration/events, safe dataset paths, strict HDFS versus explicit
fallback, a seeded phase-change workload, tests and an actual Kafka/Spark/Redis
integration demonstration. Core work is 12 November, integration evidence on
15 November and final acceptance on 17 November. Preserve honest modelled-WAN
labels and inspect tracked runtime configuration privately. Multi-seed/LRU
benchmarking, model persistence and replay/shutdown hardening follow afterwards
through December; no measured improvement is claimed before the experiment.

## Source-grounded starting point

Reviewed main `2452ddfede57a555e349ba7c606f210058b7a3ed`: README/architecture guide,
Compose/configuration, workload generator, local Spark aggregation, EMA and MLlib
prediction, placement/cache/storage adapters, metrics, seed and dashboard.

The engine polls Kafka in Python, aggregates a rolling history with local Spark,
scores previous-window hot sets, then reconciles Redis against an authoritative
HDFS/simulated tier. It is not Spark Structured Streaming across remote sites.
No automated test suite or GitHub Actions workflow is tracked at this revision.

Concrete gaps found in code:

- `CloudStore._resolve_backend()` falls back even when policy is `hdfs`, despite
  the comment that this mode requires HDFS; `_fallback()` likewise ignores policy.
- Dataset IDs are joined into local/HDFS paths without validation at this boundary.
- Config loading is untyped and duplicated across services; invalid values can
  reach rate/window/capacity calculations.
- Malformed Kafka JSON/event fields can interrupt deserialisation or the main loop.
- Workload random sources are not seeded; equal-score rankings can depend on set
  iteration order, weakening reproducible comparisons.
- Model volume is declared, but `MlPredictor` has no save/load implementation.
- The cloud `size()` cache can return bytes without a current read while latency
  adds `last_read_ms` from an earlier operation. Separate modelled from measured I/O.
- Kafka auto-commit and startup cache clearing need explicit replay/reset semantics;
  the current main loop has no graceful shutdown/final checkpoint handling.
- `.env` is tracked. Values were not inspected in this planning pass. Review it
  privately and replace tracked runtime configuration with a safe example where
  appropriate; do not assume that tracking alone proves a leaked credential.

## Bounded v1

Finish a reproducible, tested **placement experiment lab**, keeping Kafka, Spark
MLlib, Redis, HDFS and Streamlit. Provide two explicit profiles: local simulated
storage for quick tests, and real HDFS integration. Name the active backend and
latency model in every result. No claim of production multi-region deployment or
guaranteed ML improvement is part of v1.

Validate typed configuration once in a shared module: positive capacities/rates,
finite latency, valid EMA alpha, windows/intervals, bounded dataset count/size and
known backend/predictor values. Require safe dataset IDs at storage boundaries.
Use loopback-bound published ports for the local demo, health checks and pinned
runtime dependencies/images where practical.

Strict `hdfs` mode must fail with a useful error when unavailable. `auto` mode may
switch to a verified mirror and visibly report degradation/recovery. Test missing
mirrors, partial writes, backend recovery and seeded data consistency. Cache
membership and data must stay consistent and capacity must never be exceeded.

Add versioned event validation and a bounded invalid-event/dead-letter path.
Document at-least-once replay and make decision/metric accounting robust to
duplicate events or explicitly deduplicate by event ID. Commit Kafka progress
only after the chosen processing/checkpoint boundary; do not promise exactly-once
behaviour across Kafka and Redis. Handle signals and close clients/Spark cleanly.

## Benchmark design

Record a seeded access trace with phase changes, then replay the same events for
EMA, MLlib and a simple LRU baseline with identical capacity, warm-up and initial
cache state. Compare next-window predictions against unseen next-window demand.
Persist model/version/features and restore only compatible models. Keep training
and evaluation windows separate; current-batch labels must not influence the
prediction used to score that same batch.

Report hit rate, misses/bytes, promotions/evictions, migration bytes, actual I/O
duration, modelled tier/WAN latency and processing duration as distinct metrics.
Use at least three fixed seeds and two capacities on a bounded trace. A result
where ML is worse is valid evidence. Save trace hash, config, versions/hardware
and full results; publish no invented percentage improvement.

## Task sequence and acceptance

| ID | Work unit | Observable acceptance |
| --- | --- | --- |
| L01 | Test harness, CI and validated shared config | Unit suite runs without Kafka/Spark services; invalid config has useful errors |
| L02 | Safe dataset paths and backend policy | Traversal fails; strict HDFS never silently falls back; auto recovery is tested |
| L03 | Event contract and bad-event isolation | Invalid JSON/IDs/timestamps do not kill healthy event processing; counters reconcile |
| L04 | Seeded workload and deterministic ranking | Same seed/config emits identical trace; ties have stable dataset-ID ordering |
| L05 | Predictor persistence and leakage tests | Compatible model reload works; old-window predictions are scored before new labels |
| L06 | Honest metric semantics | Cached size never reuses stale read timing; measured/modelled metrics are labelled separately |
| L07 | Fair replay benchmark | EMA/ML/LRU consume identical traces and initial states; all results and negative findings retained |
| L08 | Replay/checkpoint and shutdown | Controlled crash/restart and duplicate events have documented bounded effects |
| L09 | Compose profiles, HDFS drill and dashboard health | Simulated and real-HDFS runs are distinguishable; degradation/staleness are visible |
| L10 | Release, reproducibility and runbook | Clean clone, tests, integration drill and seeded benchmark meet documented criteria |

## Release evidence

Default pytest isolates pure placement, configuration, ranking and failure
behaviour with fixtures. Integration tests exercise real Redis/Spark and the
real-HDFS profile separately. Include known resource requirements and a
five-minute phase-shift demonstration, benchmark JSON, architecture/data-flow
diagrams and failure/recovery instructions.

`v1.0.0` requires L01–L10, passing checks and honest benchmark evidence. Modelled
WAN latency remains an explicit simulation boundary, even if real HDFS is used.
