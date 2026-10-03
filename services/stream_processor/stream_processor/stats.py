from __future__ import annotations

from collections import deque


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def summarize(vals: list[float]) -> dict[str, float]:
    s = sorted(vals)
    return {"n": len(s), "p50": round(percentile(s, 50), 2), "p95": round(percentile(s, 95), 2),
            "p99": round(percentile(s, 99), 2), "max": round(s[-1], 2) if s else 0.0}


class LatencyWindow:
    def __init__(self, size: int = 5000) -> None:
        self.vals: deque[float] = deque(maxlen=size)

    def add(self, v: float) -> None:
        self.vals.append(v)

    def summary(self) -> dict[str, float]:
        return summarize(list(self.vals))
