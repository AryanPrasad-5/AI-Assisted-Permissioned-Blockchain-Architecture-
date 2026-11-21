"""Opt-in frame-quality contract for Stage 3 (live shadow) runs (task A0c).

Nothing here is a default. ``IntegratedControllerRunner`` behaves exactly as before unless a contract is passed.

What the contract adds, for a run that is meant to compare static, reactive and predictive recommendations:

1. Valid, contemporaneous client telemetry is REQUIRED. A frame that lacks any required client measurement is
   rejected for every controller alike, instead of being "valid" while the reactive controller abstains and the
   static and predictive controllers carry on (the divergence recorded as limitation L1 in the A0b report).
2. The client telemetry must come from a continuous source. A per-round summary is a round-end artefact; it is not a
   contemporaneous observation, so a frame built from one is rejected.
3. The state's own telemetry age must be known.
4. The number of CONSECUTIVE abstained frames is bounded. The bound is an operator-supplied positive integer with no
   default and no scientific justification claimed: it is a safety stop, not a statistical threshold. A contract cannot
   be constructed without it (fail closed).
5. Initialization (task A0d, opt-in). A committed-throughput rate needs two ledger observations, so the first frame of a
   run cannot carry one. With ``max_initialization_frames`` set (operator-supplied, no default), a frame whose ONLY
   shortfall is that the ledger journal holds just its baseline observation is an INITIALIZATION frame: every controller
   abstains with a distinct reason, nothing is decided or applied, and the frame is counted separately. Initialization is
   legal only before the first accepted frame, it never resets the consecutive-abstention count (so alternating
   warm-up and invalid frames cannot dodge the safety stop), and it is bounded: one more initialization frame than the
   operator allowed stops the run. A frame with any other defect is an ordinary invalid frame. Without the option the
   first frame is an ordinary abstention, exactly as before.

The contract only decides whether a frame is usable. It never proposes, applies or reports a configuration change.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Any, Mapping

from .base import ControllerState

# The measurements the production capture CLI already requires, plus the p95 the evaluation reports. Each is a
# ControllerState field, filled only from measured client/ledger data (never from a prediction).
STAGE3_CLIENT_FIELDS = (
    "observed_arrival_rate_tps",
    "observed_throughput_tps",
    "observed_latency_ms",
    "observed_latency_p95_ms",
    "queue_depth",
)
CONTRACT_VERSION = "stage3_frame_quality/1"
THROUGHPUT_FIELD = "observed_throughput_tps"
INITIALIZATION_REASON_CODE = "stage3_initialization_ledger_baseline"
INITIALIZATION_LIMIT_REASON_CODE = "stage3_initialization_limit_exceeded"
INITIALIZING_LEDGER_STATE = "initializing"      # fabric_perf.controllers.ledger_evidence.STATE_INITIALIZING


@dataclass(frozen=True)
class QualityIssue:
    """Why a frame fails the contract. ``kind`` is 'missing' or 'freshness_unknown' (the runner maps it to the
    controllers' abstention reason, so the contract module needs no import of the runner)."""

    reason_code: str
    message: str
    kind: str
    details: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Stage3FrameQualityContract:
    max_consecutive_abstentions: int
    required_client_fields: tuple[str, ...] = STAGE3_CLIENT_FIELDS
    require_continuous_client_source: bool = True
    require_known_state_age: bool = True
    max_initialization_frames: int | None = None

    def __post_init__(self) -> None:
        limit = self.max_consecutive_abstentions
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("Stage 3 requires an operator-supplied max_consecutive_abstentions, a positive integer; "
                             f"got {limit!r}. No default is assumed.")
        names = tuple(self.required_client_fields)
        valid = {item.name for item in fields(ControllerState)}
        if not names or any(not isinstance(n, str) for n in names):
            raise ValueError("required_client_fields must be a non-empty tuple of ControllerState field names")
        unknown = sorted(set(names) - valid)
        if unknown:
            raise ValueError(f"required_client_fields names no such ControllerState field: {unknown}")
        if len(set(names)) != len(names):
            raise ValueError("required_client_fields contains a duplicate")
        object.__setattr__(self, "required_client_fields", names)
        for flag in ("require_continuous_client_source", "require_known_state_age"):
            if not isinstance(getattr(self, flag), bool):
                raise ValueError(f"{flag} must be a bool")
        warmup = self.max_initialization_frames
        if warmup is not None:
            if isinstance(warmup, bool) or not isinstance(warmup, int) or warmup < 1:
                raise ValueError("max_initialization_frames must be an operator-supplied positive integer or None; "
                                 f"got {warmup!r}. No default is assumed.")
            if THROUGHPUT_FIELD not in names:
                raise ValueError(f"max_initialization_frames only applies when {THROUGHPUT_FIELD!r} is a required client field")

    @classmethod
    def from_mapping(cls, mapping: Mapping[str, Any]) -> "Stage3FrameQualityContract":
        """Build from a configuration mapping. A missing or null threshold is an error, never a default."""
        if not isinstance(mapping, Mapping):
            raise ValueError("Stage 3 frame-quality configuration must be a mapping")
        if mapping.get("max_consecutive_abstentions") is None:
            raise ValueError("Stage 3 requires max_consecutive_abstentions; none was provided")
        extra = sorted(set(mapping) - {item.name for item in fields(cls)})
        if extra:
            raise ValueError(f"unknown Stage 3 frame-quality option(s): {extra}")
        kwargs = dict(mapping)
        if "required_client_fields" in kwargs:
            kwargs["required_client_fields"] = tuple(kwargs["required_client_fields"])
        return cls(**kwargs)

    def initialization_candidate(self, frame: Any) -> dict[str, Any] | None:
        """Details if ``frame`` could be a warm-up frame, else None. Reads only; the caller applies the bound and the order rule.

        A candidate has exactly one shortfall: committed throughput is absent because the ledger observation journal holds
        only its baseline. Anything else wrong (another missing field, a read failure, an inconsistency, a compromised
        stream, no ledger journal at all) is not initialization.
        """
        if self.max_initialization_frames is None:
            return None
        provenance = getattr(frame, "provenance", None) or {}
        health = provenance.get("continuous_caliper_health") or {}
        ledger = health.get("ledger") or {}
        if ledger.get("state") != INITIALIZING_LEDGER_STATE:
            return None
        if getattr(frame.state, THROUGHPUT_FIELD, None) is not None:
            return None
        if set(getattr(frame, "critical_missing", ()) or ()) - {THROUGHPUT_FIELD}:
            return None
        if getattr(frame, "inconsistencies", ()):
            return None
        if (health.get("integrity") or {}).get("status") != "OK":
            return None
        return {"ledger_state": ledger.get("state"), "baseline_height": ledger.get("baseline_height"),
                "latest_observation_seq": ledger.get("latest_observation_seq"), "ledger_reason": ledger.get("reason"),
                "max_initialization_frames": self.max_initialization_frames}

    def check(self, frame: Any, *, waived_fields: tuple[str, ...] = ()) -> QualityIssue | None:
        """The first reason ``frame`` is unusable for a Stage 3 comparison, or None. Reads only; changes nothing.

        Field presence and staleness are also enforced by ``TelemetryFrame.validate`` (the runner passes the required
        fields to it). This adds the source and state-age requirements and names a missing field explicitly.
        ``waived_fields`` is used only by the runner for a validated initialization frame; nothing else waives a field.
        """
        provenance = getattr(frame, "provenance", None) or {}
        if self.require_continuous_client_source and not provenance.get("continuous_caliper_source"):
            return QualityIssue(
                "client_source_not_continuous",
                "Stage 3 requires a continuous client telemetry source; a round-end summary or no client source "
                "is not a contemporaneous observation",
                "missing", {"caliper_summary_configured": bool(provenance.get("caliper_summary"))})
        state = frame.state
        if self.require_known_state_age and getattr(state, "telemetry_age_s", None) is None:
            return QualityIssue("state_age_unknown", "the state's telemetry age is unknown", "freshness_unknown", {})
        absent = [name for name in self.required_client_fields
                  if name not in waived_fields and getattr(state, name, None) is None]
        if absent:
            return QualityIssue("stage3_client_field_missing",
                                f"required client measurement(s) missing: {absent}", "missing", {"fields": absent})
        return None

    def as_dict(self) -> dict[str, Any]:
        return {"version": CONTRACT_VERSION,
                "max_consecutive_abstentions": self.max_consecutive_abstentions,
                "max_consecutive_abstentions_basis": "operator-supplied safety stop; no scientific threshold is claimed",
                "required_client_fields": list(self.required_client_fields),
                "require_continuous_client_source": self.require_continuous_client_source,
                "require_known_state_age": self.require_known_state_age,
                "max_initialization_frames": self.max_initialization_frames,
                "max_initialization_frames_basis": (
                    "initialization not enabled: a first frame without committed throughput is an ordinary abstention"
                    if self.max_initialization_frames is None else
                    "operator-supplied warm-up bound; no scientific value is claimed")}
