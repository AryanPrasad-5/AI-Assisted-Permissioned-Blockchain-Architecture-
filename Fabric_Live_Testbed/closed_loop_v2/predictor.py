"""Local Fabric performance predictor built from the X0 crossover scan. Pure python.

Implements the repo PerformancePredictor protocol: predict(arrival_rate_tps, config) -> PerformancePrediction(throughput_tps,
latency_ms). Data = X0 rungs that were CLIENT-VALID in every replicate (analysis_v2.build_surface). Between rungs it interpolates
linearly in log(rate); outside the measured range it REFUSES (the controller then abstains and holds), it never extrapolates.

latency_ms is the mean latency (the quantity the repo utility prices: 0.01 per ms). throughput_tps is the offered rate times the
measured delivered fraction (reference round wall of that B's lowest-rate rung divided by the round wall at this rate, capped at 1),
with a deadband: a fraction >= 0.95 counts as 1.0 so the drain tail of an unsaturated round does not look like backlog.
"""

from __future__ import annotations

import csv
import math
from bisect import bisect_left
from pathlib import Path

from . import ensure_paths

ensure_paths()

from fabric_perf.controllers.base import FabricConfig, PerformancePrediction  # noqa: E402

DELIVERED_DEADBAND = 0.95   # fixed before X0; delivered fraction is normalized by each B's lowest-rate rung (round-wall drain tail)


class OutsideSurface(ValueError):
    pass


class LocalSurfacePredictor:
    def __init__(self, points: list[dict]):
        """points: dicts with B, rate_tps, mean_ms, delivered_fraction (already aggregated over replicates)."""
        self.by_b: dict[int, list[tuple[float, float, float]]] = {}
        for p in points:
            self.by_b.setdefault(int(p["B"]), []).append((float(p["rate_tps"]), float(p["mean_ms"]), float(p["delivered_fraction"])))
        for b, pts in self.by_b.items():
            pts.sort()
            if len(pts) < 2 or len({r for r, _, _ in pts}) != len(pts):
                raise ValueError(f"B={b}: the surface needs >= 2 distinct rates")
        self.last_prediction_metadata: dict = {}
        self.identity = "local-x0-surface:log-rate-linear:no-extrapolation:v1"

    @property
    def rate_range(self) -> dict[int, tuple[float, float]]:
        return {b: (pts[0][0], pts[-1][0]) for b, pts in self.by_b.items()}

    @classmethod
    def from_csv(cls, path: Path) -> "LocalSurfacePredictor":
        rows = list(csv.DictReader(Path(path).open(encoding="utf-8")))
        return cls([{"B": r["B"], "rate_tps": r["rate_tps"], "mean_ms": r["mean_ms"], "delivered_fraction": r["delivered_fraction"]}
                    for r in rows if str(r.get("client_valid", "True")) == "True"])

    def _interp(self, b: int, rate: float, col: int) -> float:
        pts = self.by_b[b]
        if rate < pts[0][0] - 1e-9 or rate > pts[-1][0] + 1e-9:
            raise OutsideSurface(f"B={b}: rate {rate:.2f} tx/s outside the measured range {pts[0][0]:g}-{pts[-1][0]:g}")
        xs = [math.log(p[0]) for p in pts]
        x = math.log(max(rate, pts[0][0]))
        i = min(max(bisect_left(xs, x), 1), len(xs) - 1)
        x0, x1 = xs[i - 1], xs[i]
        y0, y1 = pts[i - 1][col], pts[i][col]
        w = 0.0 if x1 == x0 else (x - x0) / (x1 - x0)
        return y0 + min(max(w, 0.0), 1.0) * (y1 - y0)

    def predict(self, arrival_rate_tps: float, config: FabricConfig) -> PerformancePrediction:
        b = int(config.max_message_count)
        if b not in self.by_b:
            raise OutsideSurface(f"B={b} was not measured in X0")
        rate = float(arrival_rate_tps)
        if not math.isfinite(rate) or rate <= 0:
            raise OutsideSurface(f"non-positive forecast {rate}")
        latency = self._interp(b, rate, 1)
        frac = min(1.0, self._interp(b, rate, 2))
        frac = 1.0 if frac >= DELIVERED_DEADBAND else frac
        self.last_prediction_metadata = {"model": self.identity, "B": b, "rate_tps": rate, "mean_ms": latency, "delivered_fraction": frac}
        return PerformancePrediction(throughput_tps=rate * frac, latency_ms=latency)
