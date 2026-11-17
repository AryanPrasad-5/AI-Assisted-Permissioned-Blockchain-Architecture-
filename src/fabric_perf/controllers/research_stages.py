"""The three research stages stay separate (owner decision D5, 2026-11-17).

Stage 1  Bounded Caliper validation. Real transactions on a dedicated validation channel; no controller.
Stage 2  Offline replay. The validated observations are replayed through Static, Reactive and Predictive,
         in memory only, to verify reproducibility and keep proposals apart from applied changes.
Stage 3  Live shadow integration. Genuine live observations feed all three controllers in shadow mode,
         with the live actuator disabled.

Each stage has its own acceptance criteria and its own authorization. Passing a stage authorizes nothing
else, and an authorization that names several stages is refused. No stage may change live Fabric
configuration through a controller. Reports must say whether each value is observed, replayed, predicted
or merely proposed.

This module is a register plus guards. It starts, stops and executes nothing, and it does not grant
authorization: a person does that, in a message, for one stage. Pure standard library.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Iterable, Mapping


class ResearchStage(str, Enum):
    CALIPER_VALIDATION = "stage1_caliper_validation"
    OFFLINE_REPLAY = "stage2_offline_replay"
    LIVE_SHADOW = "stage3_live_shadow"


class EvidenceClass(str, Enum):
    OBSERVED = "observed"      # measured on the running system during this stage
    REPLAYED = "replayed"      # recorded observations passed through code again
    PREDICTED = "predicted"    # the output of a model or forecaster
    PROPOSED = "proposed"      # a controller's proposal; never applied by these stages


class StageViolation(ValueError):
    """A stage was asked to do something its contract forbids."""


class StageAuthorizationError(StageViolation):
    """No authorization names exactly this stage."""


@dataclass(frozen=True)
class StageContract:
    stage: ResearchStage
    purpose: str
    emits: frozenset[EvidenceClass]
    runs_controllers: bool
    allowed_actuators: frozenset[str]
    acceptance: tuple[tuple[str, str], ...]
    authorization_phrase: str
    authorizes_next_stage: bool = False


_S1, _S2, _S3 = ResearchStage.CALIPER_VALIDATION, ResearchStage.OFFLINE_REPLAY, ResearchStage.LIVE_SHADOW

STAGE_CONTRACTS: Mapping[ResearchStage, StageContract] = {
    _S1: StageContract(
        _S1, "Verify real transaction submission and validation, client-side throughput and latency, real block "
             "creation and block-fill metrics, and preserve independent ledger evidence.",
        frozenset({EvidenceClass.OBSERVED}), False, frozenset(),
        (("S1-A1", "Warm-up and measured transactions are counted separately; each phase has no pending, duplicate or "
                   "observer-error transactions, and every transaction is VALID in the independently fetched ledger."),
         ("S1-A2", "The block-fill baseline is captured before the first transaction and classified (family absent, "
                   "series absent, zero, non-zero, scrape failed, malformed); a delta is reported only when both readings "
                   "are valid, and an absent baseline is reported as unavailable, never as zero."),
         ("S1-A3", "The ledger height delta equals the number of new blocks on peers and orderers; the counter delta is "
                   "compared with it where the metric semantics permit, and a mismatch is reported, not corrected."),
         ("S1-A4", "Throughput and latency (offered, delivered, mean, p95 from per-transaction records) are reported for "
                   "the measured round only; the warm-up round is reported separately and never merged unlabelled."),
         ("S1-A5", "The validation channel's configuration is unchanged, there is no leader change or container restart, "
                   "and the protected reference channels keep the same heights and head hashes."),
         ("S1-A6", "The Caliper container is stopped gracefully, not removed; its image, volumes and networks are "
                   "unchanged; its final state is recorded in the report.")),
        "I authorize exactly one bounded Caliper validation run"),
    _S2: StageContract(
        _S2, "Replay the validated observations through Static, Reactive and Predictive to verify reproducibility, "
             "keeping proposals separate from confirmed (in-memory) configuration changes.",
        frozenset({EvidenceClass.REPLAYED, EvidenceClass.PREDICTED, EvidenceClass.PROPOSED}), True,
        frozenset({"ShadowActuator", "RecordingActuator"}),
        (("S2-A1", "Every input is a Stage 1 artifact or a pinned recorded input verified by SHA-256 before use, and every "
                   "reported value carries its evidence class."),
         ("S2-A2", "All strategies receive identical frames (equal input digests) and only built-in shadow or recording "
                   "actuators exist; live submissions are zero."),
         ("S2-A3", "A second run reproduces the same behavioural signature; HOLD, SELECT, ABSTAIN, proposal and confirmed "
                   "application stay distinct records."),
         ("S2-A4", "Outcome counts and abstention reasons are reported per controller; no performance or benefit claim is "
                   "made, because a replay cannot create counterfactual outcomes."),
         ("S2-A5", "A stale, missing, future-dated or inconsistent observation yields an abstention with its stated "
                   "reason, never a decision.")),
        "I authorize offline replay of the validated Stage 1 observations"),
    _S3: StageContract(
        _S3, "Consume genuine live workload and Fabric observations, build complete timestamped frames, and run all three "
             "controllers in shadow mode with the live actuator disabled.",
        frozenset({EvidenceClass.OBSERVED, EvidenceClass.PREDICTED, EvidenceClass.PROPOSED}), True,
        frozenset({"ShadowActuator"}),
        (("S3-A1", "Frames are complete and timestamped from genuine live observations; every source is within the validated "
                   "age limit and the read status is SUCCESSFUL, or the frame is rejected."),
         ("S3-A2", "All three controllers run in shadow mode through a ShadowActuator; zero configuration submissions and no "
                   "confirmed application."),
         ("S3-A3", "Failed, partial or invalid reads and stale, missing, future-dated or inconsistent frames produce "
                   "structured abstentions (or a recorded stop) that keep the original error context."),
         ("S3-A4", "A missing or unavailable workload second is never filled; forecasting abstains while the UTC-day prefix "
                   "is incomplete."),
         ("S3-A5", "Reports separate observed values from predicted values and from proposals.")),
        "I authorize live shadow integration"),
}


def contract(stage: ResearchStage) -> StageContract:
    return STAGE_CONTRACTS[ResearchStage(stage)]


def assert_no_live_actuation(stage: ResearchStage, actuators: Iterable[object]) -> None:
    """Refuse any actuator the stage's contract does not name. Stage 1 runs no controller at all."""
    allowed = contract(stage).allowed_actuators
    names = [type(actuator).__name__ for actuator in actuators]
    illegal = sorted({name for name in names if name not in allowed})
    if illegal:
        raise StageViolation(f"{ResearchStage(stage).value} permits actuators {sorted(allowed) or 'none'}; found {illegal}. "
                             "No controller may change live Fabric configuration in a research stage.")


def assert_may_emit(stage: ResearchStage, evidence_class: EvidenceClass) -> None:
    """A stage may only label results with the evidence classes it can truthfully produce."""
    if EvidenceClass(evidence_class) not in contract(stage).emits:
        raise StageViolation(f"{ResearchStage(stage).value} cannot produce {EvidenceClass(evidence_class).value} evidence")


@dataclass(frozen=True)
class StageAuthorization:
    """An owner's authorization for exactly one stage, kept as a record. It is not self-granting."""

    stage: ResearchStage
    text: str
    granted_at_utc: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "stage", ResearchStage(self.stage))
        named = [s for s in ResearchStage if STAGE_CONTRACTS[s].authorization_phrase.lower() in self.text.lower()]
        if len(named) > 1:
            raise StageAuthorizationError(
                f"an authorization must name one stage; this text names {[s.value for s in named]}")
        try:
            stamp = datetime.fromisoformat(self.granted_at_utc.replace("Z", "+00:00"))
        except (AttributeError, ValueError) as exc:
            raise StageAuthorizationError("granted_at_utc must be an ISO-8601 timestamp") from exc
        if stamp.tzinfo is None:
            raise StageAuthorizationError("granted_at_utc must include a timezone")


def require_authorization(stage: ResearchStage, grants: Iterable[StageAuthorization]) -> StageAuthorization:
    """Return the grant that names exactly ``stage``. A grant for another stage never carries over."""
    stage = ResearchStage(stage)
    phrase = STAGE_CONTRACTS[stage].authorization_phrase.lower()
    for grant in grants:
        if grant.stage is stage and phrase in grant.text.lower():
            return grant
    raise StageAuthorizationError(
        f"no authorization names {stage.value}; authorization for another stage does not carry over")
