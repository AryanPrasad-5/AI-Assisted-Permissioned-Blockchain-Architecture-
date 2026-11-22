"""Recorder round files -> one observation per finished Caliper round. Stdlib only.

The per-tx recorder (scripts/e1/caliper/e1_per_tx_recorder.js) writes, at round end, <base>.csv, <base>.submits.csv and
then <base>.manifest.json with the SHA-256 of both CSVs. The manifest is written last, so its presence (and matching
hashes) marks a complete round. Latency = final_ms - create_ms, both stamped by Caliper's own clock in one process.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
from dataclasses import asdict, dataclass
from pathlib import Path


class TelemetryError(ValueError):
    pass


def round_base(recorder_dir: Path, run_id: str, round_index: int, label: str, worker: int = 0) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in label)
    return Path(recorder_dir) / f"{run_id}_w{worker}_r{round_index}_{safe}"


def nearest_rank(sorted_values: list[float], q: float) -> float | None:
    if not sorted_values:
        return None
    k = max(1, math.ceil(q * len(sorted_values)))
    return sorted_values[k - 1]


@dataclass(frozen=True)
class RoundObservation:
    round_index: int
    label: str
    rows: int
    success: int
    failed: int
    other: int
    pending_at_deactivate: int
    latency_mean_ms: float | None
    latency_p50_ms: float | None
    latency_p95_ms: float | None
    latency_p99_ms: float | None
    latency_max_ms: float | None
    first_create_ms: int | None
    last_final_ms: int | None
    wall0_ms: int
    wall1_ms: int
    wall_minus_monotonic_ms: float | None
    csv_sha256: str
    manifest_sha256: str

    def as_dict(self) -> dict:
        return asdict(self)


def manifest_ready(base: Path) -> bool:
    return Path(f"{base}.manifest.json").is_file()


def read_round(base: Path, round_index: int, label: str) -> tuple[RoundObservation, list[dict]]:
    """Parse one finished round. Raises TelemetryError on any integrity problem (never repairs)."""
    man_path, csv_path = Path(f"{base}.manifest.json"), Path(f"{base}.csv")
    try:
        man_raw = man_path.read_bytes()
        manifest = json.loads(man_raw)
        csv_raw = csv_path.read_bytes()
    except (OSError, json.JSONDecodeError) as exc:
        raise TelemetryError(f"round {round_index}: unreadable recorder output: {exc}") from exc
    if hashlib.sha256(csv_raw).hexdigest() != manifest.get("files", {}).get("csv_sha256"):
        raise TelemetryError(f"round {round_index}: recorder CSV hash differs from its manifest")
    if manifest.get("round") != round_index or manifest.get("label") != label:
        raise TelemetryError(f"round {round_index}: manifest names round {manifest.get('round')} label {manifest.get('label')}")
    rows = list(csv.DictReader(io.StringIO(csv_raw.decode("utf-8"))))
    lat, creates, finals = [], [], []
    success = failed = 0
    for r in rows:
        if r.get("status") == "success":
            success += 1
            try:
                c, f = int(float(r["create_ms"])), int(float(r["final_ms"]))
            except (KeyError, TypeError, ValueError) as exc:
                raise TelemetryError(f"round {round_index}: success row without timestamps") from exc
            if f < c:
                raise TelemetryError(f"round {round_index}: final before create for {r.get('tx_id')}")
            lat.append(float(f - c))
            creates.append(c)
            finals.append(f)
        elif r.get("status") == "failed":
            failed += 1
    lat.sort()
    counts, clock = manifest.get("counts", {}), manifest.get("clock", {})
    if counts.get("rows") != len(rows):
        raise TelemetryError(f"round {round_index}: manifest rows {counts.get('rows')} != CSV rows {len(rows)}")
    obs = RoundObservation(
        round_index=round_index, label=label, rows=len(rows), success=success, failed=failed,
        other=len(rows) - success - failed, pending_at_deactivate=int(counts.get("pending_at_deactivate", -1)),
        latency_mean_ms=(sum(lat) / len(lat)) if lat else None, latency_p50_ms=nearest_rank(lat, 0.50),
        latency_p95_ms=nearest_rank(lat, 0.95), latency_p99_ms=nearest_rank(lat, 0.99), latency_max_ms=lat[-1] if lat else None,
        first_create_ms=min(creates) if creates else None, last_final_ms=max(finals) if finals else None,
        wall0_ms=int(clock.get("wall0_ms", 0)), wall1_ms=int(clock.get("wall1_ms", 0)),
        wall_minus_monotonic_ms=clock.get("wall_minus_monotonic_ms"),
        csv_sha256=hashlib.sha256(csv_raw).hexdigest(), manifest_sha256=hashlib.sha256(man_raw).hexdigest())
    return obs, rows
