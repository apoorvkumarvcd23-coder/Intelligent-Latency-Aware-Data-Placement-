"""Explicit HDFS or verified local-mirror storage; single managed writer."""
import hashlib
import json
import os
from pathlib import Path
import time
from uuid import uuid4

from common.contracts import ContractError, dataset_id


class StorageUnavailable(RuntimeError):
    pass


class CloudStore:
    def __init__(self, cfg, client_factory=None):
        c = cfg['cloud']
        self.policy = c['backend']
        if self.policy not in {'auto','hdfs','simulated'}:
            raise ContractError('Unknown storage policy')
        self.webhdfs_url, self.user = c['webhdfs_url'], c['hdfs_user']
        self.base_path = c['base_path'].rstrip('/')
        self.sim_path = Path(c['simulated_path']).resolve()
        self.latency_ms = float(c['latency_ms'])
        self.last_read_ms = 0.0
        self.backend = 'simulated'
        self.degraded = self.policy == 'auto'
        self._client = None
        self.client_factory = client_factory
        self._size_cache = {}
        if self.policy != 'hdfs':
            self.sim_path.mkdir(parents=True, exist_ok=True)
        if self.policy != 'simulated':
            if self._probe_hdfs():
                if self.policy == 'auto' and self._manifest():
                    self.revalidate()  # restart must reconcile offline-created mirror data too
                else:
                    self.backend, self.degraded = 'hdfs', False
            elif self.policy == 'hdfs':
                raise StorageUnavailable('Strict HDFS policy requires reachable HDFS')

    def _probe_hdfs(self):
        try:
            if self.client_factory:
                client = self.client_factory()
            else:
                from hdfs import InsecureClient
                client = InsecureClient(self.webhdfs_url, user=self.user, timeout=3)
            client.status('/')
            client.makedirs(self.base_path)
            self._client = client
            return True
        except Exception:
            return False

    def _fallback(self):
        if self.policy == 'hdfs':
            raise StorageUnavailable('Strict HDFS operation failed; fallback is disabled') from None
        self.backend, self.degraded = 'simulated', self.policy == 'auto'

    def _hdfs_path(self, ds):
        return self.base_path + '/' + dataset_id(ds)

    def _sim_path(self, ds):
        target = self.sim_path / dataset_id(ds)
        if target.is_symlink() or target.resolve().parent != self.sim_path:
            raise ContractError('Dataset path escapes the configured store')
        return target

    def _manifest(self):
        path = self.sim_path / '.mirror.json'
        if path.is_symlink():
            raise StorageUnavailable('Unsafe mirror metadata')
        if not path.exists():
            return {}
        try:
            if path.stat().st_size > 1_000_000:
                raise ValueError()
            data = json.loads(path.read_text())
            if not isinstance(data, dict):
                raise ValueError()
            for key, value in data.items():
                dataset_id(key)
                if not isinstance(value, str) or len(value) != 64 or any(c not in '0123456789abcdef' for c in value):
                    raise ValueError()
            return data
        except (ValueError, OSError):
            raise StorageUnavailable('Invalid mirror metadata') from None

    @staticmethod
    def _atomic(path, data):
        candidate = path.with_name('.' + path.name + '.' + uuid4().hex + '.candidate')
        try:
            with candidate.open('xb') as target:
                target.write(data)
                target.flush()
                os.fsync(target.fileno())
            candidate.replace(path)
        finally:
            candidate.unlink(missing_ok=True)

    def _mirror(self, ds, data):
        manifest = self._manifest()
        self._atomic(self._sim_path(ds), data)
        manifest[ds] = hashlib.sha256(data).hexdigest()
        self._atomic(self.sim_path / '.mirror.json', json.dumps(manifest, sort_keys=True).encode())

    def _read_mirror(self, ds):
        try:
            expected = self._manifest().get(ds)
            with self._sim_path(ds).open('rb') as source:
                data = source.read(1024**2+1)
            if len(data)>1024**2:
                raise StorageUnavailable('Mirror exceeds dataset byte budget')
            if expected is None or hashlib.sha256(data).hexdigest() != expected:
                raise StorageUnavailable('Missing or corrupt verified mirror')
            return data
        except OSError:
            raise StorageUnavailable('Missing or unreadable verified mirror') from None

    def revalidate(self):
        if self.policy == 'simulated' or self.backend == 'hdfs':
            return self.backend == 'hdfs'
        if not self._probe_hdfs():
            return False
        try:
            for ds in sorted(self._manifest()):
                data = self._read_mirror(ds)
                path = self._hdfs_path(ds)
                self._client.write(path, data=data, overwrite=True)
                with self._client.read(path) as source:
                    if source.read() != data:
                        raise StorageUnavailable('HDFS recovery verification failed')
            self.backend, self.degraded = 'hdfs', False
            return True
        except Exception:
            if self.policy == 'hdfs':
                raise StorageUnavailable('Strict HDFS recovery failed') from None
            return False

    def exists(self, ds):
        path = self._hdfs_path(ds)
        if self.backend == 'hdfs':
            try:
                return self._client.status(path, strict=False) is not None
            except Exception:
                self._fallback()
        if not self._sim_path(ds).exists():
            return False
        self._read_mirror(ds)
        return True

    def write(self, ds, data):
        path = self._hdfs_path(ds)
        if not isinstance(data, bytes) or not data or len(data) > 1024**2:
            raise ContractError('Dataset requires 1 byte to 1 MiB')
        if self.backend == 'hdfs':
            try:
                self._client.write(path, data=data, overwrite=True)
            except Exception:
                self._fallback()
        if self.policy != 'hdfs':
            self._mirror(ds, data)
        self._size_cache[ds] = len(data)

    def read(self, ds):
        path = self._hdfs_path(ds)
        self.last_read_ms = 0.0
        started = time.perf_counter()
        if self.backend == 'hdfs':
            try:
                with self._client.read(path) as source:
                    data = source.read(1024**2+1)
                if not data or len(data)>1024**2:
                    raise StorageUnavailable('HDFS dataset byte budget violated')
                if self.policy == 'auto':
                    expected = self._manifest().get(ds)
                    if expected is not None and hashlib.sha256(data).hexdigest()!=expected:
                        raise StorageUnavailable('HDFS bytes differ from the verified managed dataset')
            except Exception:
                self._fallback()
                data = self._read_mirror(ds)
            else:
                if self.policy == 'auto':
                    self._mirror(ds, data)
        else:
            data = self._read_mirror(ds)
        self.last_read_ms = (time.perf_counter()-started)*1000
        self._size_cache[ds] = len(data)
        return data

    def size(self, ds):
        dataset_id(ds)
        self.last_read_ms = 0.0
        return self._size_cache[ds] if ds in self._size_cache else len(self.read(ds))

    def list_datasets(self):
        if self.backend == 'hdfs':
            try:
                names = self._client.list(self.base_path)
            except Exception:
                self._fallback()
                names = self._manifest()
        else:
            names = self._manifest()
        result = []
        for name in names:
            try:
                result.append(dataset_id(name))
            except ContractError:
                continue
        return sorted(result)
