"""
Live metrics dashboard (Streamlit) for the latency-aware placement engine.

Reads everything from Redis (written by the placement engine + workload generator)
and auto-refreshes. Shows the six evaluation metrics over time, the EMA-vs-MLlib
head-to-head, and how well the edge cache tracks the *true* hot set.
"""
import json

import pandas as pd
import redis
import streamlit as st

from config_util import load_config

st.set_page_config(page_title="Latency-Aware Edge-Cloud Placement", layout="wide")

CFG = load_config()
PREFIX = CFG["metrics"]["redis_key_prefix"]


@st.cache_resource
def get_redis():
    return redis.Redis(host=CFG["edge"]["redis_host"], port=int(CFG["edge"]["redis_port"]), db=0)


def read_state(r):
    latest_raw = r.get(f"{PREFIX}:metrics:latest")
    latest = json.loads(latest_raw) if latest_raw else {}
    hist_raw = r.lrange(f"{PREFIX}:metrics:history", 0, -1)
    hist = [json.loads(x) for x in reversed(hist_raw)] if hist_raw else []
    phase_raw = r.get(f"{PREFIX}:workload:phase")
    phase = json.loads(phase_raw) if phase_raw else {}
    edge_members = {m.decode() for m in r.smembers(f"{PREFIX}:edge:members")}
    return latest, hist, phase, edge_members


st.title("🛰️ Intelligent Latency-Aware Data Placement — Edge ↔ Cloud")
st.caption("Kafka → Spark Streaming → ML demand prediction → placement engine. "
           "Edge = Redis cache (fast) · Cloud = HDFS (slow, +latency).")


@st.fragment(run_every="2s")
def render():
    r = get_redis()
    latest, hist, phase, edge_members = read_state(r)

    if not latest:
        st.info("Waiting for the placement engine to publish its first metrics… "
                "(make sure the stack is up and the workload is running)")
        return

    backend = latest.get("cloud_backend", "?")
    badge = "🟢 REAL HDFS" if backend == "hdfs" else "🟡 SIMULATED cloud (HDFS fallback)"
    cols = st.columns(4)
    cols[0].metric("Avg query latency", f"{latest['avg_latency_ms']:.1f} ms")
    cols[1].metric("Cache hit rate", f"{latest['cache_hit_rate'] * 100:.1f} %")
    cols[2].metric("Throughput", f"{latest['throughput_eps']:.0f} ev/s")
    cols[3].metric("Cloud bandwidth", f"{latest['bandwidth_kbps']:.0f} KB/s")

    cols = st.columns(4)
    cols[0].metric("Edge storage used", f"{latest['edge_count']}/{latest['capacity']}",
                   f"{latest['storage_util'] * 100:.0f}%")
    cols[1].metric("Migrations (total)", f"{latest['migrations_total']}",
                   f"+{latest['migrations_cycle']} this cycle")
    cols[2].metric("Migration overhead", f"{latest['migration_bytes_total'] / 1024:.0f} KB moved")
    cols[3].metric("Cloud backend", badge)

    ml_state = "trained ✅" if latest.get("ml_ready") else f"warming up ({latest.get('ml_samples', 0)} samples)"
    st.caption(f"Workload phase: **{latest.get('phase', '?')}** · "
               f"Active predictor: **{latest.get('predictor_active', '?').upper()}** · "
               f"MLlib model: **{ml_state}** · "
               f"Known datasets: {latest.get('known_datasets', 0)}/{latest.get('total_datasets', 0)}")

    if len(hist) < 2:
        st.info("Collecting history for charts…")
        return

    df = pd.DataFrame(hist)
    df["t"] = range(len(df))
    df = df.set_index("t")

    left, right = st.columns(2)
    with left:
        st.subheader("Average query latency (ms)")
        st.line_chart(df[["avg_latency_ms"]])
        st.subheader("Cache hit rate")
        st.line_chart(df[["cache_hit_rate"]])
    with right:
        st.subheader("EMA vs MLlib predictor — hit rate on live traffic")
        st.line_chart(df[["ema_hitrate", "ml_hitrate"]])
        st.caption("Higher = the predictor's chosen hot-set matched real accesses better.")
        st.subheader("Cloud bandwidth (KB/s) & throughput (ev/s)")
        st.line_chart(df[["bandwidth_kbps", "throughput_eps"]])

    st.subheader("Migrations per cycle (data movement overhead)")
    st.bar_chart(df[["migrations_cycle"]])

    # How well does the edge cache track the TRUE hot set?
    st.subheader("Edge cache vs ground-truth hot set")
    hot = set(phase.get("hot_set", []))
    if hot and edge_members:
        overlap = hot & edge_members
        jac = len(overlap) / len(hot | edge_members) if (hot | edge_members) else 0
        c = st.columns(3)
        c[0].metric("True hot datasets cached", f"{len(overlap)}/{len(hot)}")
        c[1].metric("Edge ∩ Hot (Jaccard)", f"{jac:.2f}")
        c[2].metric("Currently at edge", f"{len(edge_members)}")
        st.write("**At edge now:**", ", ".join(sorted(edge_members)) or "—")
        st.write("**Truly hot now:**", ", ".join(sorted(hot)) or "—")
    else:
        st.caption("Waiting for hot-set + edge membership data…")


render()
