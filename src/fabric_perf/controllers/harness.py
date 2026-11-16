"""Side-effect-controlled experiment harness for controller comparisons."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Protocol

from .base import BaseController, ControllerState, FabricConfig, coerce_config


@dataclass(frozen=True)
class ApplyResult:
    accepted: bool
    applied_config: FabricConfig
    elapsed_ms: float
    error: str | None = None
    previous_config: FabricConfig | None = None
    requested_config: FabricConfig | None = None
    verified_config: FabricConfig | None = None
    submission_start_timestamp_utc: str | None = None
    submission_completion_timestamp_utc: str | None = None
    config_block_commit_timestamp_utc: str | None = None
    independent_verification_timestamp_utc: str | None = None
    submission_duration_ms: float | None = None
    submission_to_commit_ms: float | None = None
    commit_to_independent_verification_ms: float | None = None
    total_verified_adaptation_ms: float | None = None
    after_block_number: int | None = None
    verification: Mapping[str, Any] | None = None
    failure_category: str | None = None
    remote_state_unknown: bool = False


class ConfigurationActuator(Protocol):
    @property
    def current_config(self) -> FabricConfig: ...

    def apply(self, config: FabricConfig) -> ApplyResult: ...


class ConfigurationPolicy(Protocol):
    def validate(self, config: FabricConfig) -> None: ...


def _record_value(record: Any, name: str, default: Any = None) -> Any:
    if isinstance(record, Mapping):
        return record.get(name, default)
    return getattr(record, name, default)


def _config_from_record(value: Any) -> FabricConfig | None:
    if value is None:
        return None
    try:
        return coerce_config(value)
    except (TypeError, ValueError):
        return None


def _apply_result_from_callback(
    result: Any,
    *,
    previous: FabricConfig,
    requested: FabricConfig,
    elapsed_ms: float,
    require_verified_result: bool,
) -> ApplyResult:
    """Normalize the verified Fabric actuator record without losing evidence."""
    if isinstance(result, ApplyResult):
        return result
    if result is None:
        if require_verified_result:
            return ApplyResult(False, previous, elapsed_ms, "verified actuator returned no result record",
                               previous_config=previous, requested_config=requested)
        return ApplyResult(True, requested, elapsed_ms, previous_config=previous, requested_config=requested,
                           verified_config=requested)
    success = _record_value(result, "success")
    after = _config_from_record(_record_value(result, "after_config"))
    accepted = success is True and after is not None and after == requested
    error = _record_value(result, "error")
    if not accepted and not error:
        error = "actuator result was not commit-verified for the requested configuration"
    submission_start = _record_value(result, "submission_start_timestamp_utc")
    submission_end = _record_value(result, "submission_completion_timestamp_utc")
    commit_ts = _record_value(result, "config_block_commit_timestamp_utc")
    independent_ts = _record_value(result, "independent_verification_timestamp_utc")
    total_ms = _record_value(result, "total_adaptation_latency_ms")
    submission_ms = _record_value(result, "submission_duration_ms")
    commit_ms = _record_value(result, "commit_latency_ms")
    independent_ms = _record_value(result, "commit_to_independent_verification_ms")
    return ApplyResult(
        accepted=accepted,
        applied_config=after if accepted else previous,
        elapsed_ms=elapsed_ms,
        error=error,
        previous_config=previous,
        requested_config=requested,
        verified_config=after,
        submission_start_timestamp_utc=submission_start,
        submission_completion_timestamp_utc=submission_end,
        config_block_commit_timestamp_utc=commit_ts,
        independent_verification_timestamp_utc=independent_ts,
        submission_duration_ms=submission_ms,
        submission_to_commit_ms=commit_ms,
        commit_to_independent_verification_ms=independent_ms,
        total_verified_adaptation_ms=total_ms,
        after_block_number=_record_value(result, "after_block_number"),
        verification=_record_value(result, "verification_checks"),
        failure_category=_record_value(result, "failure_category"),
        remote_state_unknown=bool(_record_value(result, "remote_state_unknown", False)),
    )


class RecordingActuator:
    """Deterministic actuator for unit tests and offline controller dry runs."""

    def __init__(self, initial_config: FabricConfig):
        self._current_config = coerce_config(initial_config)
        self.applications: list[ApplyResult] = []

    @property
    def current_config(self) -> FabricConfig:
        return self._current_config

    def apply(self, config: FabricConfig) -> ApplyResult:
        start = time.perf_counter_ns()
        candidate = coerce_config(config)
        previous = self._current_config
        self._current_config = candidate
        result = ApplyResult(True, candidate, (time.perf_counter_ns() - start) / 1_000_000,
                             previous_config=previous, requested_config=candidate, verified_config=candidate)
        self.applications.append(result)
        return result


class CallableActuator:
    """Measured actuator around an explicitly supplied Fabric update function.

    The callback must complete only after the caller's configuration update
    protocol has accepted the request. It must not be treated as instantaneous.
    """

    def __init__(self, initial_config: FabricConfig, apply_fn: Callable[[FabricConfig], Any], *,
                 require_verified_result: bool = False,
                 verify_fn: Callable[[Any, FabricConfig], Any] | None = None):
        self._current_config = coerce_config(initial_config)
        self.apply_fn = apply_fn
        self.require_verified_result = require_verified_result
        self.verify_fn = verify_fn

    @property
    def current_config(self) -> FabricConfig:
        return self._current_config

    def apply(self, config: FabricConfig) -> ApplyResult:
        candidate = coerce_config(config)
        previous = self._current_config
        start = time.perf_counter_ns()
        try:
            callback_result = self.apply_fn(candidate)
        except Exception as exc:
            return ApplyResult(False, self._current_config, (time.perf_counter_ns() - start) / 1_000_000,
                               f"{type(exc).__name__}: {exc}", previous_config=previous,
                               requested_config=candidate, failure_category="submission_or_commit_unknown",
                               remote_state_unknown=True)
        elapsed = (time.perf_counter_ns() - start) / 1_000_000
        result = _apply_result_from_callback(callback_result, previous=previous, requested=candidate,
                                             elapsed_ms=elapsed, require_verified_result=self.require_verified_result)
        if result.accepted and self.verify_fn is not None:
            try:
                verification = self.verify_fn(callback_result, candidate)
                if isinstance(verification, bool):
                    verified_ok = verification
                    verification_data = {"success": verification}
                elif isinstance(verification, Mapping):
                    verified_ok = verification.get("success") is True
                    verification_data = dict(verification)
                else:
                    verified_ok = False
                    verification_data = {"success": False, "error": "invalid verifier result"}
                if not verified_ok:
                    return replace(result, accepted=False, applied_config=previous,
                                   verified_config=None,
                                   error=verification_data.get("error", "independent verification failed"),
                                   verification=verification_data,
                                   failure_category="independent_verification_failed",
                                   remote_state_unknown=True)
                result = replace(
                    result,
                    independent_verification_timestamp_utc=verification_data.get("timestamp_utc", result.independent_verification_timestamp_utc),
                    commit_to_independent_verification_ms=verification_data.get("commit_to_independent_verification_ms", result.commit_to_independent_verification_ms),
                    verification=verification_data,
                )
            except Exception as exc:
                return replace(result, accepted=False, applied_config=previous, verified_config=None,
                               error=f"independent verification error: {type(exc).__name__}: {exc}",
                               failure_category="independent_verification_error",
                               remote_state_unknown=True)
        if result.accepted:
            self._current_config = result.verified_config or result.applied_config
        return result


class VerifiedCallableActuator(CallableActuator):
    """Live actuator adapter that requires an independently verified record.

    ``apply_fn`` should wrap the existing Fabric configuration actuator and
    return its committed ``UpdateRecord``. ``verify_fn`` should perform the
    separate read-only configuration retrieval/transition verification. A
    failed verification leaves the local configuration unchanged and marks
    the remote state as unknown so the caller must stop or reconcile before
    making another decision.
    """

    def __init__(self, initial_config: FabricConfig, apply_fn: Callable[[FabricConfig], Any], *,
                 verify_fn: Callable[[Any, FabricConfig], Any]):
        if verify_fn is None:
            raise ValueError("verified actuator requires an independent verify_fn")
        super().__init__(initial_config, apply_fn, require_verified_result=True, verify_fn=verify_fn)


class ShadowActuator:
    """Record proposed actions without invoking any live callback."""

    def __init__(self, initial_config: FabricConfig):
        self._current_config = coerce_config(initial_config)
        self.proposals: list[ApplyResult] = []

    @property
    def current_config(self) -> FabricConfig:
        return self._current_config

    def apply(self, config: FabricConfig) -> ApplyResult:
        candidate = coerce_config(config)
        result = ApplyResult(False, self._current_config, 0.0,
                             "shadow_mode_no_submission", previous_config=self._current_config,
                             requested_config=candidate)
        self.proposals.append(result)
        return result


class ControllerExperimentHarness:
    """Run any controller over the same telemetry snapshot stream.

    No workload or Fabric process is started here. The same harness can consume
    snapshots from Caliper/E1 telemetry for each strategy, while the actuator
    determines whether a run is offline, segmented, or authorized for live
    updates.
    """

    def __init__(self, controller: BaseController, actuator: ConfigurationActuator, *, run_id: str,
                 configuration_policy: ConfigurationPolicy | None = None):
        self.controller = controller
        self.actuator = actuator
        self.run_id = run_id
        self.configuration_policy = configuration_policy

    def process(self, state: ControllerState) -> dict:
        state.current_config = self.actuator.current_config
        start = time.perf_counter_ns()
        decision = self.controller.decide(state)
        decision.decision_overhead_ms = (time.perf_counter_ns() - start) / 1_000_000
        application = None
        if decision.changed and decision.config != self.actuator.current_config:
            try:
                if self.configuration_policy is not None:
                    self.configuration_policy.validate(decision.config)
                    decision.metadata["safety_gate"] = {"accepted": True, "reason": "approved_candidate"}
                application = self.actuator.apply(decision.config)
            except Exception as exc:
                decision.metadata["safety_gate"] = {"accepted": False, "reason": str(exc)}
                application = ApplyResult(False, self.actuator.current_config, 0.0,
                                          f"safety_gate_rejected: {type(exc).__name__}: {exc}",
                                          previous_config=self.actuator.current_config,
                                          requested_config=decision.config)
        return {
            "run_id": self.run_id,
            "state": state.as_dict(),
            "decision": decision.as_dict(),
            "application": None if application is None else {
                "accepted": application.accepted,
                "applied_config": application.applied_config.as_dict(),
                "elapsed_ms": application.elapsed_ms,
                "error": application.error,
                "previous_config": None if application.previous_config is None else application.previous_config.as_dict(),
                "requested_config": None if application.requested_config is None else application.requested_config.as_dict(),
                "verified_config": None if application.verified_config is None else application.verified_config.as_dict(),
                "submission_start_timestamp_utc": application.submission_start_timestamp_utc,
                "submission_completion_timestamp_utc": application.submission_completion_timestamp_utc,
                "config_block_commit_timestamp_utc": application.config_block_commit_timestamp_utc,
                "independent_verification_timestamp_utc": application.independent_verification_timestamp_utc,
                "submission_duration_ms": application.submission_duration_ms,
                "submission_to_commit_ms": application.submission_to_commit_ms,
                "commit_to_independent_verification_ms": application.commit_to_independent_verification_ms,
                "total_verified_adaptation_ms": application.total_verified_adaptation_ms,
                "after_block_number": application.after_block_number,
                "verification": application.verification,
                "failure_category": application.failure_category,
                "remote_state_unknown": application.remote_state_unknown,
            },
        }

    def run(self, states: Iterable[ControllerState], output_path: Path | None = None) -> list[dict]:
        records = [self.process(state) for state in states]
        if output_path is not None:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with output_path.open("x", encoding="utf-8", newline="\n") as fh:
                json.dump(records, fh, indent=2)
                fh.write("\n")
        return records
