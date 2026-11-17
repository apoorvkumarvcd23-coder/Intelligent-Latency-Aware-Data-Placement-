# Core acceptance — 17 November 2026

This audit covers the bounded **v0.1.0 core**, with extensions and full v1.0.0
hardening continuing through December. The clean checkout audited here is
`c7fea84dfa6e2dd752db36b5d6645ef27e5a3c59`. The release tag is created only after the reviewed acceptance
commit also passes its complete clean-checkout CI. Exact immutable tag/commit/CI
links are recorded in the [portfolio release ledger](https://github.com/apoorvkumarvcd23-coder/apoorvkumarvcd23-coder/blob/main/docs/CORE_RELEASES_2026-11-18.md).
Native audit observations are dated 17 November; publication and release checks
carried forward to 18 November. Commit and tag timestamps use their actual time.
No version tag substitutes for the checks.

## Architecture and contract ledger

Seeded versioned Kafka events → validated bounded gate → local Spark rolling counts → next-cycle EMA/MLlib decisions → capacity-bounded Redis and strict HDFS/verified mirror → labelled Streamlit metrics.

| Core contract | Implementation | Actual verification |
| --- | --- | --- |
| Validated configuration/events/dataset paths | common/contracts.py; common/events.py; hdfs_client.py | 63 Linux boundary tests;62 native passes plus Windows symlink skip; strict traversal/type/time/event bounds |
| Actual seeded Kafka/Spark/MLlib/Redis adaptation | common/workload.py; services/placement_engine/runtime.py; scripts/demo_integration.py | Actual 148-record replay →144 accepted/3 invalid/1 duplicate; next-cycle scoring independently recomputed |
| Strict HDFS and explicit verified-mirror fallback | services/placement_engine/hdfs_client.py CloudStore | Actual NameNode/DataNode writes/reads; strict startup/read/write/list/exists denied during live service outage |
| Verified recovery and real dashboard | demo_hdfs.py; run_hdfs_demo.py; check_dashboard.py | Same clients survive service stop/start; corrupt/missing mirrors denied, offline writes read back and degraded/recovered browser states verified |
| Honest measurement/provenance and repeatable release checks | placement.py serve; runtime.py metrics; .github/workflows/checks.yml | Modelled tier/WAN delay separate from measured I/O; fixed seed/hash; manual workflow dispatch supports immutable tag reruns |

See the [detailed claim-to-code/test ledger](CLAIM_EVIDENCE.md) and
[full workflow/runbook](HDFS_INTEGRATION.md). The [machine-readable audit](core-acceptance-2026-11-17.json)
records native observations and hashes of the existing public evidence, retaining
their platform/model distinction. The [previous verified CI](https://github.com/apoorvkumarvcd23-coder/Intelligent-Latency-Aware-Data-Placement-/actions/runs/43633766007)
is source-grounded evidence; fresh release-head CI must pass before tagging.

[Actual HDFS screenshots and runbook](HDFS_INTEGRATION.md) retain their Linux origin.

## Reproduce from a clean clone

Install the prerequisites in README and follow the full runbook before these
commands. Use a new ignored artifact directory and synthetic/public data only.
Keep generated account/config files private. All service demos use disposable
owned local services; no paid service or external account is required.

```sh
# Follow HDFS_INTEGRATION.md using a dedicated core09-* Compose project.
python -m pytest -q
# GitHub Actions → placement-core → Run workflow accepts an immutable tag/ref.
# It runs boundaries, simulated-storage integration and actual-HDFS outage/recovery.
# All three jobs must pass; native Docker was unavailable at this audit.
```

The native audit used actual installed dependencies and fresh disposable data.
Its platform is recorded explicitly; native fallback does not prove a different
database/service backend. Linux CI executes the documented real-service path.

## Defensible project wording

These are permitted implementation claims for this core, not a personal resume
file or a claim of customer/employer use:

- Built a seeded Kafka/local Spark MLlib/EMA placement lab using Redis/HDFS, with validated events and explicit strict/fallback storage policies.
- Verified 148 replay records as 144 accepted, 3 invalid and 1 duplicate; exercised actual HDFS outage and verified-mirror recovery with measured I/O separated from modelled latency.

## Remaining scope and limits

- Single-DataNode, unauthenticated loopback-only local lab with a single managed mirror writer.
- WAN/tier delays are modelled; observed tiny-fixture rates are not throughput benchmarks.
- Fair multi-seed/LRU comparison, model persistence, durable replay/checkpoints and broader December recovery remain open.
- No production HA, exactly-once semantics or ML percentage-improvement claim.
