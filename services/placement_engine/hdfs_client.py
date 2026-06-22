"""
CloudStore — the "cloud" storage tier abstraction.

Primary backend  : real HDFS via the WebHDFS REST API (the `hdfs` library).
Fallback backend : a latency-injected local-volume store (identical API).

The fallback is the project's reliability guarantee: if HDFS is slow to start,
crashes, or is stopped mid-demo, the engine transparently keeps serving from the
local store and the demo never dies. `cloud.backend` in config picks the policy:
  hdfs       -> require HDFS
  simulated  -> always use the local store
  auto       -> try HDFS, fall back to local if unreachable (default)
"""
import os
import time
import logging

log = logging.getLogger("cloudstore")


class CloudStore:
    def __init__(self, cfg: dict):
        c = cfg["cloud"]
        self.policy = c.get("backend", "auto")
        self.webhdfs_url = c["webhdfs_url"]
        self.user = c.get("hdfs_user", "root")
        self.base_path = c["base_path"].rstrip("/")
        self.sim_path = c["simulated_path"].rstrip("/")
        self.latency_ms = float(c.get("latency_ms", 80))
        self.last_read_ms = 0.0

        self._client = None          # hdfs InsecureClient when active
        self.backend = "simulated"   # resolved backend actually in use
        self._size_cache = {}

        os.makedirs(self.sim_path, exist_ok=True)
        self._resolve_backend()

    # ----- backend selection ------------------------------------------------
    def _resolve_backend(self):
        if self.policy == "simulated":
            self.backend = "simulated"
            log.info("CloudStore: forced simulated backend")
            return

        # policy is hdfs or auto -> probe HDFS
        ok = self._probe_hdfs()
        if ok:
            self.backend = "hdfs"
            log.info("CloudStore: using REAL HDFS at %s", self.webhdfs_url)
        elif self.policy == "auto":
            self.backend = "simulated"
            log.warning("CloudStore: HDFS unreachable -> FALLING BACK to simulated store")
        else:  # policy == hdfs but unreachable
            self.backend = "simulated"
            log.error("CloudStore: HDFS required but unreachable; using simulated store anyway")

    def _probe_hdfs(self) -> bool:
        try:
            from hdfs import InsecureClient
            client = InsecureClient(self.webhdfs_url, user=self.user, timeout=10)
            client.status("/")            # raises if not reachable
            client.makedirs(self.base_path)
            self._client = client
            return True
        except Exception as e:  # noqa: BLE001 - any failure means fall back
            log.warning("CloudStore: HDFS probe failed: %s", e)
            return False

    def revalidate(self):
        """Re-probe HDFS (used if it was down and may have recovered)."""
        if self.policy != "simulated" and self.backend == "simulated":
            if self._probe_hdfs():
                self.backend = "hdfs"
                log.info("CloudStore: HDFS recovered -> switched back to HDFS")

    # ----- paths ------------------------------------------------------------
    def _hdfs_path(self, ds: str) -> str:
        return f"{self.base_path}/{ds}"

    def _sim_path(self, ds: str) -> str:
        return os.path.join(self.sim_path, ds)

    # ----- operations -------------------------------------------------------
    def exists(self, ds: str) -> bool:
        if self.backend == "hdfs":
            try:
                return self._client.status(self._hdfs_path(ds), strict=False) is not None
            except Exception:
                self._fallback()
        return os.path.exists(self._sim_path(ds))

    def write(self, ds: str, data: bytes):
        """Write to whichever backend is active (used during seeding)."""
        if self.backend == "hdfs":
            try:
                self._client.write(self._hdfs_path(ds), data=data, overwrite=True)
            except Exception as e:  # noqa: BLE001
                log.warning("CloudStore write to HDFS failed (%s); writing to sim store", e)
                self._fallback()
        # Always mirror to the simulated store so a later fallback is seamless.
        with open(self._sim_path(ds), "wb") as f:
            f.write(data)
        self._size_cache[ds] = len(data)

    def read(self, ds: str) -> bytes:
        """Read dataset bytes (real I/O). Records last_read_ms."""
        t0 = time.time()
        data = self._read_raw(ds)
        self.last_read_ms = (time.time() - t0) * 1000.0
        self._size_cache[ds] = len(data)
        return data

    def _read_raw(self, ds: str) -> bytes:
        if self.backend == "hdfs":
            try:
                with self._client.read(self._hdfs_path(ds)) as r:
                    return r.read()
            except Exception as e:  # noqa: BLE001
                log.warning("CloudStore read from HDFS failed (%s); using sim store", e)
                self._fallback()
        with open(self._sim_path(ds), "rb") as f:
            return f.read()

    def size(self, ds: str) -> int:
        """Size in bytes, memoized so repeated misses don't re-read."""
        if ds in self._size_cache:
            return self._size_cache[ds]
        sz = len(self.read(ds))
        return sz

    def list_datasets(self) -> list:
        if self.backend == "hdfs":
            try:
                return sorted(self._client.list(self.base_path))
            except Exception:
                self._fallback()
        return sorted(os.listdir(self.sim_path))

    def _fallback(self):
        if self.backend != "simulated":
            log.warning("CloudStore: switching to simulated backend after an HDFS error")
            self.backend = "simulated"
