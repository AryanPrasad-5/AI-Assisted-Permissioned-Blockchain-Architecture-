"""Arm policies: how each strategy turns the just-finished round into the next segment's MaxMessageCount. Stdlib only.

static      repo StaticController, fixed at the initial configuration
reactive    repo ReactiveController, fed LIVE with the finished round's measured mean latency (Caliper per-tx recorder)
predictive  the repo PredictiveController's decisions for this window, computed offline on 2026-11-15 (D-E2-10) and applied
            online at the same decision times. Its inputs are the exogenous Flashbots arrival history (EWMA forecast) and the
            HFBTP model, never Fabric telemetry; the only Fabric-dependent input is the current configuration, so the stored
            decision is valid only while the live configuration equals the stored precondition (B=10). Otherwise it abstains.
            No model inference runs in this package.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import replace

from . import ensure_paths

ensure_paths()

from fabric_perf.controllers.base import ControllerState, FabricConfig, StaticController  # noqa: E402
from fabric_perf.controllers.strategies import ReactiveController  # noqa: E402

ARMS = ("static", "reactive", "predictive")


class PolicyError(ValueError):
    pass


def ms_to_iso(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).isoformat(timespec="milliseconds")


def config_with_b(base: FabricConfig, b: int) -> FabricConfig:
    return replace(base, max_message_count=int(b))


class ArmPolicy:
    arm = "base"

    def __init__(self, initial: FabricConfig, menu: tuple[int, ...]):
        if initial.max_message_count not in menu:
            raise PolicyError(f"initial B {initial.max_message_count} is not in the menu {menu}")
        self.initial, self.menu = initial, tuple(menu)

    def decide(self, decision_index: int, observation, current: FabricConfig, telemetry_age_s: float | None) -> dict:
        raise NotImplementedError

    def applied(self, config: FabricConfig) -> None:
        """Called only after the actuator has committed AND independently verified a new configuration."""

    def _result(self, decision_index, target_b, current, reason, *, abstained=False, metadata=None) -> dict:
        if target_b not in self.menu:
            raise PolicyError(f"{self.arm}: decision {decision_index} selected B={target_b}, outside the menu {self.menu}")
        return {"decision_index": decision_index, "arm": self.arm, "current_B": current.max_message_count, "target_B": int(target_b),
                "changed": int(target_b) != current.max_message_count, "reason": reason, "abstained": bool(abstained),
                "controller_metadata": metadata or {}}


class StaticPolicy(ArmPolicy):
    arm = "static"

    def __init__(self, initial, menu):
        super().__init__(initial, menu)
        self.controller = StaticController(initial)

    def decide(self, decision_index, observation, current, telemetry_age_s):
        d = self.controller.decide(ControllerState(current_config=current))
        return self._result(decision_index, d.config.max_message_count, current, d.reason, metadata=dict(d.metadata))


class ReactivePolicy(ArmPolicy):
    arm = "reactive"

    def __init__(self, initial, menu, *, congested_b: int, latency_high_ms: float, latency_low_ms: float,
                 consecutive_samples: int, cooldown_samples: int, max_telemetry_age_s: float):
        super().__init__(initial, menu)
        if congested_b not in menu or congested_b == initial.max_message_count:
            raise PolicyError("congested B must be a different menu value")
        self.params = {"congested_B": congested_b, "latency_high_ms": latency_high_ms, "latency_low_ms": latency_low_ms,
                       "consecutive_samples": consecutive_samples, "cooldown_samples": cooldown_samples,
                       "max_telemetry_age_s": max_telemetry_age_s, "observed_metric": "round latency_mean_ms"}
        self.controller = ReactiveController(
            default_config=initial, congested_config=config_with_b(initial, congested_b), latency_high_ms=latency_high_ms,
            latency_low_ms=latency_low_ms, consecutive_samples=consecutive_samples, cooldown_samples=cooldown_samples,
            max_telemetry_age_s=max_telemetry_age_s)

    def decide(self, decision_index, observation, current, telemetry_age_s):
        if observation is None:
            d = self.controller.reject_observation("telemetry_missing_fail_safe", error="round telemetry unavailable")
            return self._result(decision_index, d.config.max_message_count, current, d.reason, abstained=True, metadata=d.metadata)
        state = ControllerState(
            timestamp_utc=ms_to_iso(observation.wall1_ms), current_config=current,
            observed_latency_ms=observation.latency_mean_ms, observed_latency_p95_ms=observation.latency_p95_ms,
            failed_transactions=observation.failed, telemetry_age_s=telemetry_age_s)
        d = self.controller.decide(state)
        abstained = d.reason.endswith("fail_safe")
        return self._result(decision_index, d.config.max_message_count, current, d.reason, abstained=abstained,
                            metadata=dict(d.metadata))

    def applied(self, config):
        self.controller.configuration_applied(config)


class PredictivePolicy(ArmPolicy):
    arm = "predictive"

    def __init__(self, initial, menu, *, stored_rows: list[dict], decision_times_utc: list[str]):
        super().__init__(initial, menu)
        if len(stored_rows) != len(decision_times_utc):
            raise PolicyError("stored predictive table and decision schedule differ in length")
        for i, (row, ts) in enumerate(zip(stored_rows, decision_times_utc)):
            if int(row["decision_index"]) != i:
                raise PolicyError(f"stored predictive row {i} has decision_index {row['decision_index']}")
            if dt.datetime.fromisoformat(row["decision_ts_utc"]) != dt.datetime.fromisoformat(ts):
                raise PolicyError(f"stored predictive row {i} is for {row['decision_ts_utc']}, schedule says {ts}")
            if int(row["predictive_B_stored"]) not in menu or int(row["current_B_before"]) not in menu:
                raise PolicyError(f"stored predictive row {i} uses a configuration outside the menu")
        self.rows = stored_rows

    def decide(self, decision_index, observation, current, telemetry_age_s):
        row = self.rows[decision_index]
        precondition = int(row["current_B_before"])
        meta = {"source": "stored D-E2-10 decision (repo PredictiveController, 2026-11-15); no inference here",
                "stored_B": int(row["predictive_B_stored"]), "stored_abstained": row["predictive_abstained"] in (True, "True", "true"),
                "stored_cause": row["predictive_cause_stored"], "stored_forecast_tps": row["predictive_forecast_tps_stored"],
                "precondition_B": precondition}
        if current.max_message_count != precondition:
            return self._result(decision_index, current.max_message_count, current, "stored_decision_precondition_failed_hold",
                                abstained=True, metadata=meta)
        reason = "stored_" + str(row["predictive_cause_stored"])
        return self._result(decision_index, int(row["predictive_B_stored"]), current, reason,
                            abstained=meta["stored_abstained"], metadata=meta)


class ScriptedPolicy(ArmPolicy):
    """ACTUATION_CHECK only: a fixed, pre-declared B schedule that exercises the live switch path. Not a strategy."""
    arm = "scripted"

    def __init__(self, initial, menu, *, schedule_b: list[int]):
        super().__init__(initial, menu)
        self.schedule_b = [int(b) for b in schedule_b]

    def decide(self, decision_index, observation, current, telemetry_age_s):
        return self._result(decision_index, self.schedule_b[decision_index], current, "scripted_actuation_check",
                            metadata={"purpose": "ACTUATION_CHECK"})


def build_policy(arm_doc: dict, initial: FabricConfig) -> ArmPolicy:
    arm, menu = arm_doc["arm"], tuple(int(b) for b in arm_doc["menu_B"])
    if arm == "static":
        return StaticPolicy(initial, menu)
    if arm == "scripted":
        return ScriptedPolicy(initial, menu, schedule_b=arm_doc["scripted"]["schedule_B"])
    if arm == "reactive":
        p = arm_doc["reactive"]
        return ReactivePolicy(initial, menu, congested_b=int(p["congested_B"]), latency_high_ms=float(p["latency_high_ms"]),
                              latency_low_ms=float(p["latency_low_ms"]), consecutive_samples=int(p["consecutive_samples"]),
                              cooldown_samples=int(p["cooldown_samples"]), max_telemetry_age_s=float(p["max_telemetry_age_s"]))
    if arm == "predictive":
        p = arm_doc["predictive"]
        return PredictivePolicy(initial, menu, stored_rows=p["rows"], decision_times_utc=arm_doc["decision_times_utc"])
    raise PolicyError(f"unknown arm {arm!r}")
