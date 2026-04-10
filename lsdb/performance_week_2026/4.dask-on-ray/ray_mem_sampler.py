import threading
import time

import requests

PROMETHEUS_URL = "http://localhost:9090"


class RayMemorySampler:
    """Sample Ray cluster memory via Prometheus metrics.

    Polls two metrics at a fixed interval:
    - ``ray_component_rss_mb``: ray components memory usage
    - ``ray_object_store_used_memory``: object store memory usage

    Usage::

        ray.init(num_cpus=4)
        with RayMemorySampler() as ms:
            do_work()
        ray.shutdown()
        print(f"cluster: {ms.peak_cluster_mem_gb:.2f} GiB")
        print(f"store:   {ms.peak_object_store_gb:.2f} GiB")
    """

    def __init__(self, interval: float = 0.5, prometheus_url: str = PROMETHEUS_URL):
        self.interval = interval
        self.prometheus_url = prometheus_url
        self.cluster_mem_samples: list[tuple[float, float]] = []
        self.object_store_samples: list[tuple[float, float]] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _query(self, metric: str) -> float:
        """Instant PromQL query, summing the metric across all nodes."""
        resp = requests.get(
            f"{self.prometheus_url}/api/v1/query",
            params={"query": f"sum({metric})"},
            timeout=2,
        )
        result = resp.json()["data"]["result"]
        return float(result[0]["value"][1]) if result else 0.0

    def _poll(self) -> None:
        while not self._stop.wait(self.interval):
            try:
                ts = time.time()
                self.cluster_mem_samples.append((ts, self._query("ray_component_rss_mb")))
                self.object_store_samples.append((ts, self._query("ray_object_store_used_memory")))
            except Exception:
                pass

    def __enter__(self) -> "RayMemorySampler":
        self.cluster_mem_samples = []
        self.object_store_samples = []
        self._stop.clear()
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join()

    @property
    def peak_cluster_mem_gb(self) -> float:
        if not self.cluster_mem_samples:
            return 0.0
        return max(s[1] for s in self.cluster_mem_samples) / 1024

    @property
    def peak_object_store_gb(self) -> float:
        if not self.object_store_samples:
            return 0.0
        return max(s[1] for s in self.object_store_samples) / 2**30
