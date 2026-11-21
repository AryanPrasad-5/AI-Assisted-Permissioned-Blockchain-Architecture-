"""Worker-aware coverage (task A0k, 2026-11-21): many workers, one history, and no completeness that any missing worker could fake.

What the repository establishes about workers (inspection, A0k report section 8)
--------------------------------------------------------------------------------
* Every pilot and replay config sets ``workers: {number: 1}`` (``scripts/e1/e1_la2_render.py:123``, ``p4_replay/render.py:63``).
* Both E1 workload modules REFUSE more than one worker (``e1_create_asset.js:35``, ``e1_trace_replay.js:60``); their asset keys
  (``keyFor(runId, roundIndex, seq)``) carry no worker index, so two workers would collide on keys. A multi-worker E1 run cannot happen today.
* Caliper itself splits ``txNumber`` across workers (``p4_replay/caliper_check.py`` F5, read from the v0.6.0 source on 2026-11-14) and passes
  ``workerIndex`` to each observer (``createTxObserver(options, messenger, workerIndex)``). Each recorder's instance id carries ``w<index>``.
* Each worker is a separate process with its own ``process.hrtime`` base; its wall clock is the host's ``Date.now()`` only if the workers share a
  host, which nothing here can verify.

So this module is a CONTRACT for a future multi-worker run, tested only on synthetic streams. It makes the dangerous mistake impossible: one
worker's complete evidence can never stand for the others'.

The contract
------------
``WorkerRoster`` names the workers that MUST report and the clock domain each declares. ``MultiWorkerCoverage`` combines one
``RecorderVerifiedCoverage`` per worker:

* a second is accepted (a positive count or a genuine zero) only if EVERY expected worker's own coverage accepts it, because a positive total with
  one worker silent is a lower bound, not a count;
* an unknown roster, a missing worker, or workers in different clock domains leave every second unverified (they are not invalid: they are
  unknown), reported with a reason code;
* a duplicate worker identity, an unexpected worker, a journal whose own ``worker`` field disagrees with its roster entry, journals of different runs,
  a worker whose evidence is itself invalid, or a declared total that disagrees with the workers' source counts make the WHOLE attestation invalid.

Events are never matched across workers (by timestamp or otherwise): the history adds per-second counts from the union of their submit events, and a
cross-worker timestamp tie is only counted, never resolved. Clocks of different workers are not assumed synchronized: only workers that DECLARE one
clock domain are combined.

Pure standard library; no process, network, model or Fabric use.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from datetime import datetime, timedelta
from typing import Any, Mapping, Sequence

from .recorder_coverage import (RC_HASH_CHAIN, RC_CONTRADICTORY, RecorderCoverageSnapshot, RecorderVerifiedCoverage, HASH_FAILURE_CODES)
from .recorder_health_bridge import ORIGIN_SYNTHETIC
from .workload_coverage import (ACCEPTED_CLASSES, MODE_ATTESTED, PROVENANCE_SCHEMA, SCOPE_RECORDER_CALLBACK, CoverageClass, CoverageEvidenceError,
                                CoveragePolicy, iso, parse_utc)

WORKER_SCHEMA = "worker_coverage/1"
ROSTER_BASES = ("benchmark_config", "runtime_observed")

# reason codes
RC_ROSTER_UNKNOWN = "worker_roster_unknown"
RC_WORKER_MISSING = "worker_missing"
RC_CLOCK_DOMAINS = "worker_clock_domains_differ"
RC_WORKER_DUPLICATE = "worker_duplicate_identity"
RC_WORKER_UNEXPECTED = "worker_unexpected"
RC_WORKER_IDENTITY = "worker_identity_mismatch"
RC_WORKER_RUN = "worker_run_mismatch"
RC_WORKER_INVALID = "worker_evidence_invalid"
RC_TOTALS = "conflicting_source_totals"

# the order in which a worker's problem outranks another's when one second is rejected for several reasons
_SEVERITY = (CoverageClass.INVALID_ATTESTATION, CoverageClass.INTERRUPTED_COLLECTION, CoverageClass.STALE_COLLECTOR_EVIDENCE,
             CoverageClass.COVERAGE_GAP, CoverageClass.UNKNOWN_COVERAGE_ZERO)


class WorkerContractError(CoverageEvidenceError):
    """The roster or the worker set cannot be built. ``code`` is machine-readable."""


@dataclass(frozen=True)
class WorkerRoster:
    """The workers that must report, with the clock domain each declares. ``workers`` is a mapping ``worker index -> clock domain``."""

    workers: Mapping[int, str]
    basis: str | None
    declared_by: str = ""

    def __post_init__(self) -> None:
        for index, domain in self.workers.items():
            if isinstance(index, bool) or not isinstance(index, int) or index < 0:
                raise WorkerContractError("roster_invalid", f"worker index {index!r} must be a non-negative integer")
            if not isinstance(domain, str) or not domain.strip():
                raise WorkerContractError("roster_invalid", f"worker {index} declares no clock domain")
        if self.workers and self.basis not in ROSTER_BASES:
            raise WorkerContractError("roster_invalid", f"a roster needs a basis from {ROSTER_BASES}; got {self.basis!r}")

    @classmethod
    def from_benchmark_config(cls, number: int, clock_domain: str, *, declared_by: str = "benchmark.yaml workers.number") -> "WorkerRoster":
        """The roster a benchmark config implies (``workers: {number: N}`` gives workers 0..N-1). A DECLARATION: nothing checks that Caliper started them."""
        if isinstance(number, bool) or not isinstance(number, int) or number < 1:
            raise WorkerContractError("roster_invalid", "workers.number must be a positive integer")
        return cls({i: clock_domain for i in range(number)}, "benchmark_config", declared_by)

    @classmethod
    def unknown(cls) -> "WorkerRoster":
        """No verified roster: the completeness of any multi-worker interval is UNKNOWN."""
        return cls({}, None, "")

    @property
    def known(self) -> bool:
        return bool(self.workers)

    def as_dict(self) -> dict[str, Any]:
        return {"workers": {str(k): v for k, v in sorted(self.workers.items())}, "basis": self.basis, "declared_by": self.declared_by, "known": self.known,
                "verification": "a roster is declared (configuration or runtime), never proven by this module"}


@dataclass(frozen=True)
class MultiWorkerSnapshot(RecorderCoverageSnapshot):
    """The combined coverage at one decision time. ``workers`` holds each worker's own snapshot; ``blocks`` are (code, message) pairs that leave
    every second unverified without invalidating the evidence."""

    workers: Mapping[int, Any] = None
    blocks: tuple = ()
    roster: Mapping[str, Any] | None = None

    # ------------------------------------------------------------------------------------------------ classification
    def classify(self, second: datetime, count: int | None) -> tuple[CoverageClass, str]:
        if count is None:
            return CoverageClass.MISSING_OBSERVATION, "the series has no row for this second; it is never read as zero"
        if not self.valid:
            return CoverageClass.INVALID_ATTESTATION, f"{self.failure_code}: {self.failure_reason}"
        if self.blocks:
            code, message = self.blocks[0]
            return (CoverageClass.UNKNOWN_COVERAGE_ZERO if count == 0 else CoverageClass.COVERAGE_GAP), f"{code}: {message}"
        probe = 1 if count > 0 else 0                        # a positive total needs every worker covered; a zero needs every worker source-verified
        rejected: list = []
        for index in sorted(self.workers):
            cls, why = self.workers[index].classify(second, probe)
            if cls not in ACCEPTED_CLASSES:
                rejected.append((cls, f"worker {index}: {why}"))
        if not rejected:
            return ((CoverageClass.OBSERVED_POSITIVE, f"all {len(self.workers)} expected worker(s) cover this second")
                    if count > 0 else (CoverageClass.VERIFIED_ZERO, f"all {len(self.workers)} expected worker(s) verify this zero"))
        for severity in _SEVERITY:
            for cls, why in rejected:
                if cls == severity:
                    return cls, why
        return rejected[0]

    # ------------------------------------------------------------------------------------------- provenance helpers
    def recorder_only_zero_seconds(self, counts: Sequence, day_start: datetime, limit_index: int) -> int:
        """Zero-count seconds below ``limit_index`` that every worker's recorder covers but at least one worker has no source-verified span for."""
        if not self.valid or self.blocks or not self.workers:
            return 0
        total = 0
        for index in range(min(limit_index, len(counts))):
            if counts[index] != 0:
                continue
            moment = day_start + timedelta(seconds=index)
            per_worker = [w.classify(moment, 0)[0] for w in self.workers.values()]
            if per_worker and all(c in (CoverageClass.VERIFIED_ZERO, CoverageClass.UNKNOWN_COVERAGE_ZERO) for c in per_worker) \
                    and any(c == CoverageClass.UNKNOWN_COVERAGE_ZERO for c in per_worker):
                total += 1
        return total

    def cause_codes(self) -> list[str]:
        codes: set[str] = {code for code, _m in self.blocks}
        if not self.valid:
            codes.add(RC_HASH_CHAIN if self.failure_code in HASH_FAILURE_CODES else RC_CONTRADICTORY)
            if self.failure_code in {RC_WORKER_DUPLICATE, RC_WORKER_UNEXPECTED, RC_WORKER_IDENTITY, RC_WORKER_RUN, RC_WORKER_INVALID, RC_TOTALS}:
                codes.add(self.failure_code)
            return sorted(codes)
        for snap in (self.workers or {}).values():
            codes.update(snap.cause_codes())
        return sorted(codes)

    def summary(self) -> dict[str, Any]:
        doc = super().summary()
        workers = self.workers or {}
        doc["schema"] = PROVENANCE_SCHEMA
        doc["worker_coverage"] = {"schema": WORKER_SCHEMA, "roster": None if self.roster is None else dict(self.roster),
                                  "blocks": [{"code": c, "message": m} for c, m in self.blocks],
                                  "workers": {str(i): {"valid": s.valid, "scope": s.scope, "failure_code": s.failure_code,
                                                       "source_verified_seconds": int(sum((e - st).total_seconds() for st, e in s.source_verified))}
                                              for i, s in sorted(workers.items())},
                                  "acceptance": "a second is accepted only if every expected worker accepts it; one worker's evidence never stands for another's",
                                  "events_are_never_matched_across_workers": True}
        origins = sorted({o for s in workers.values() for o in getattr(s, "origins", ())})
        doc["recorder_evidence"] = {
            "schema": "recorder_coverage_provenance/1", "coverage_scope": [SCOPE_RECORDER_CALLBACK] if workers else [], "evidence_origins": origins,
            "synthetic": ORIGIN_SYNTHETIC in origins or not origins, "submit_units_visible": sum(getattr(s, "submit_units_visible", 0) for s in workers.values()),
            "operational_coverage": "verified per worker from each worker's hash-chained journal",
            "callback_coverage": "per worker: each heartbeat's cumulative count equals the units in that worker's live submits file",
            "source_completeness": {"verified_seconds": min((int(sum((e - st).total_seconds() for st, e in s.source_verified)) for s in workers.values()),
                                                             default=0),
                                    "basis_claimed_by_collector": sorted({b for s in workers.values() for b in s.source_basis_claims}),
                                    "independence": "DECLARED by each collector; this module cannot verify it"},
            "live_source_coverage_verified": False,
            "unverified_reason": "no independent source-completeness evidence has been verified against a real Caliper/Fabric run; synthetic evidence is never live coverage",
            "positive_second_caveat": "a positive count can still under-count if callbacks were missed"}
        return doc


class MultiWorkerCoverage(RecorderVerifiedCoverage):
    """``RecorderVerifiedCoverage`` for several workers. It subclasses the single-worker class so the recorder history and Stage 3 accept it, but it
    holds no journal of its own: every answer is the combination of the per-worker coverages."""

    def __init__(self, *, source_id: str, roster: WorkerRoster, workers: Sequence[tuple[int, RecorderVerifiedCoverage]],
                 declared_source_total: int | None = None, run_id: str | None = None):
        if not isinstance(source_id, str) or not source_id:
            raise CoverageEvidenceError("source_identity_missing", "coverage needs a source_id")
        if declared_source_total is not None and (isinstance(declared_source_total, bool) or not isinstance(declared_source_total, int) or declared_source_total < 0):
            raise WorkerContractError("declared_total_invalid", "declared_source_total must be a non-negative integer")
        # deliberately no super().__init__: there is no single journal. The attributes the history and the A0i derivations read are set here.
        self.source_id, self.mode = source_id, MODE_ATTESTED
        self.events, self.submit_events, self.observed_events = (), (), None
        self.policy, self.run_id, self.channel_id = CoveragePolicy(), run_id, None
        self.no_evidence_reason, self.journal_sha256, self.torn_tail = None, None, False
        self.source_evidence = None
        self.roster = roster
        self.declared_source_total = declared_source_total
        self.worker_list = tuple(workers)
        for index, cov in self.worker_list:
            if not isinstance(cov, RecorderVerifiedCoverage) or isinstance(cov, MultiWorkerCoverage):
                raise WorkerContractError("worker_coverage_type_invalid", f"worker {index} needs its own RecorderVerifiedCoverage")
            if cov.source_id != source_id:
                raise WorkerContractError("worker_source_mismatch", f"worker {index} is for source {cov.source_id!r}, not {source_id!r}")

    def declared_scopes(self) -> set:
        return {scope for _i, cov in self.worker_list for scope in cov.declared_scopes()}

    # ------------------------------------------------------------------------------------------------------ snapshot
    def snapshot(self, as_of: Any) -> MultiWorkerSnapshot:
        moment = parse_utc(as_of, "as_of")
        common = {"mode": MODE_ATTESTED, "source_id": self.source_id, "as_of": moment, "derivation": None, "failure_code": None, "failure_reason": None,
                  "scope": SCOPE_RECORDER_CALLBACK}
        roster_doc = self.roster.as_dict()

        def invalid(code: str, message: str, snaps: Mapping[int, Any] | None = None) -> MultiWorkerSnapshot:
            return MultiWorkerSnapshot(**{**common, "failure_code": code, "failure_reason": message, "scope": None}, workers=snaps or {}, roster=roster_doc)

        ids = [index for index, _c in self.worker_list]
        duplicated = sorted({i for i in ids if ids.count(i) > 1})
        if duplicated:
            return invalid(RC_WORKER_DUPLICATE, f"worker identity {duplicated} is claimed by more than one evidence stream")
        by_id = dict(self.worker_list)
        expected = set(self.roster.workers)
        unexpected = sorted(set(by_id) - expected) if self.roster.known else []
        if unexpected:
            return invalid(RC_WORKER_UNEXPECTED, f"worker(s) {unexpected} are not in the roster")
        snaps = {index: cov.snapshot(moment) for index, cov in by_id.items()}
        for index, snap in sorted(snaps.items()):
            if not snap.valid:
                return invalid(RC_WORKER_INVALID, f"worker {index}: {snap.failure_code}: {snap.failure_reason}", snaps)
        for index, cov in sorted(by_id.items()):
            starts = [e for e in cov.events if e.get("kind") == "start" and parse_utc(e["t_utc"], "t_utc") <= moment]
            claimed = {e.get("worker") for e in starts}
            if claimed - {index}:
                return invalid(RC_WORKER_IDENTITY, f"the journal of worker {index} says it belongs to worker {sorted(map(str, claimed))}", snaps)
        runs = {cov.run_id for cov in by_id.values()} | {e.get("run_id") for cov in by_id.values() for e in cov.events[:1]}
        if len({r for r in runs if r is not None}) > 1:
            return invalid(RC_WORKER_RUN, f"the workers' journals belong to different runs: {sorted(map(str, runs))}", snaps)
        if self.declared_source_total is not None:
            total = sum(self._last_source_count(cov, moment) for cov in by_id.values())
            if total != self.declared_source_total:
                return invalid(RC_TOTALS, f"the workers' source counts sum to {total}, the declared total is {self.declared_source_total}", snaps)
        blocks: list = []
        if not self.roster.known:
            blocks.append((RC_ROSTER_UNKNOWN, "the worker roster is unknown, so no interval can be called complete"))
        else:
            missing = sorted(expected - set(by_id))
            if missing:
                blocks.append((RC_WORKER_MISSING, f"expected worker(s) {missing} supplied no evidence"))
            if len(set(self.roster.workers.values())) > 1:
                blocks.append((RC_CLOCK_DOMAINS, f"workers declare different clock domains {sorted(set(self.roster.workers.values()))}; their timestamps are not joined"))
        origins = sorted({o for s in snaps.values() for o in s.origins})
        units = sum(s.submit_units_visible for s in snaps.values())
        claims = tuple(sorted({b for s in snaps.values() for b in s.source_basis_claims}))
        independence = tuple(sorted({b for s in snaps.values() for b in s.independence_claims}))
        return MultiWorkerSnapshot(**common, workers=snaps, blocks=tuple(blocks), roster=roster_doc, origins=tuple(origins), scopes=(SCOPE_RECORDER_CALLBACK,),
                                   source_basis_claims=claims, independence_claims=independence, submit_units_visible=units)

    @staticmethod
    def _last_source_count(cov: RecorderVerifiedCoverage, moment: datetime) -> int:
        last = 0
        for ev in cov.events:
            if parse_utc(ev["t_utc"], "t_utc") > moment:
                break
            if ev.get("kind") in ("heartbeat", "stop") and isinstance(ev.get("source_events_expected"), int):
                last = ev["source_events_expected"]
        return last


def merge_worker_submit_events(per_worker: Mapping[int, Sequence[Mapping[str, Any]]]) -> tuple[list, dict[str, Any]]:
    """The union of every worker's submit events, ordered by (time, instance, seq). Event ids must be unique across workers (an instance id carries the
    worker index), and a cross-worker timestamp tie is counted and kept in a stable order, never resolved or matched."""
    merged: list = []
    seen: dict[str, int] = {}
    for index in sorted(per_worker):
        for event in per_worker[index]:
            key = str(event["id"])
            if key in seen:
                raise WorkerContractError("worker_event_id_collision", f"event id {key!r} appears in worker {seen[key]} and worker {index}")
            seen[key] = index
            merged.append(dict(event))
    merged.sort(key=lambda e: (int(e["ts_ms"]), str(e["instance"]), int(e.get("seq", 0))))
    ties = sum(1 for a, b in zip(merged, merged[1:]) if a["ts_ms"] == b["ts_ms"] and a["instance"] != b["instance"])
    return merged, {"workers": sorted(per_worker), "events": len(merged), "cross_worker_timestamp_ties": ties,
                    "ordering": "by (ts_ms, instance, seq); ties across workers are counted, not resolved; events are never matched across workers"}
