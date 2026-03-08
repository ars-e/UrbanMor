import logging
from dataclasses import dataclass
from threading import Lock

logger = logging.getLogger("urbanmor.observability")


@dataclass
class TimingStats:
    count: int = 0
    total_ms: float = 0.0
    max_ms: float = 0.0

    def record(self, duration_ms: float) -> None:
        self.count += 1
        self.total_ms += duration_ms
        self.max_ms = max(self.max_ms, duration_ms)

    @property
    def avg_ms(self) -> float:
        if self.count == 0:
            return 0.0
        return self.total_ms / self.count


class ObservabilityState:
    def __init__(self) -> None:
        self._lock = Lock()
        self._requests = TimingStats()
        self._queries = TimingStats()
        self._query_by_label: dict[str, TimingStats] = {}
        self._custom_cache_hits = 0
        self._custom_cache_misses = 0
        self._metric_failures = 0

    def record_request(self, duration_ms: float) -> None:
        with self._lock:
            self._requests.record(duration_ms)

    def record_query(self, label: str, duration_ms: float) -> None:
        with self._lock:
            self._queries.record(duration_ms)
            stats = self._query_by_label.setdefault(label, TimingStats())
            stats.record(duration_ms)

    def record_custom_cache(self, cache_hit: bool) -> None:
        with self._lock:
            if cache_hit:
                self._custom_cache_hits += 1
            else:
                self._custom_cache_misses += 1

    def record_metric_failure(self) -> None:
        with self._lock:
            self._metric_failures += 1

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            total_custom = self._custom_cache_hits + self._custom_cache_misses
            cache_hit_ratio = (self._custom_cache_hits / total_custom) if total_custom > 0 else None
            by_label = {
                k: {
                    "count": v.count,
                    "avg_ms": round(v.avg_ms, 3),
                    "max_ms": round(v.max_ms, 3),
                }
                for k, v in sorted(self._query_by_label.items())
            }
            return {
                "request_timing": {
                    "count": self._requests.count,
                    "avg_ms": round(self._requests.avg_ms, 3),
                    "max_ms": round(self._requests.max_ms, 3),
                },
                "query_timing": {
                    "count": self._queries.count,
                    "avg_ms": round(self._queries.avg_ms, 3),
                    "max_ms": round(self._queries.max_ms, 3),
                    "by_label": by_label,
                },
                "cache": {
                    "custom_cache_hits": self._custom_cache_hits,
                    "custom_cache_misses": self._custom_cache_misses,
                    "custom_cache_hit_ratio": cache_hit_ratio,
                },
                "metric_computation_failures": self._metric_failures,
            }


def configure_logging() -> None:
    root = logging.getLogger()
    if not root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )


OBS = ObservabilityState()
