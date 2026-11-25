"""v2 arm policies (stdlib). Reuses closed_loop.policies for static / reactive / scripted / stored-predictive and adds:

predictive_live   the repo PredictiveController fed with a CAUSAL forecast generated at decision time:
                    history  = 600 s of warm-up counts (exogenous historical monitoring, ending before the pre-roll)
                               + the per-second counts of the rounds that have FINISHED (rounds 0..d at decision d)
                    forecast = frozen EWMA (alpha 0.005, H=60 s, >=600 s history) of that history
                    model    = LocalSurfacePredictor built from X0 (a different set of runs from the M series)
                    choice   = repo utility (throughput - 0.01/ms * latency - 1.0 * backlog), cooldown 3, margin 0
                  Rounds after d are never passed to the controller. The observed counts come from the dispatched schedule,
                  checked against the recorder (obs.rows must equal the scheduled count of the round, else the policy abstains).
                  The decision record carries the inputs, the forecast and every candidate's predicted latency.
"""

from __future__ import annotations

from . import ensure_paths

ensure_paths()

from fabric_perf.controllers.base import ControllerState, FabricConfig  # noqa: E402
from fabric_perf.controllers.strategies import PredictiveController  # noqa: E402

from Fabric_Live_Testbed.closed_loop.policies import (  # noqa: E402
    ArmPolicy, PolicyError, build_policy as build_policy_v1, config_with_b, ms_to_iso)

from .forecast import HORIZON_S, MIN_HISTORY_S, CausalEwma  # noqa: E402
from .predictor import LocalSurfacePredictor  # noqa: E402
from .protocol import PREDICTIVE  # noqa: E402

ARMS_V2 = ("static", "reactive", "predictive_live")


class PredictiveLivePolicy(ArmPolicy):
    arm = "predictive_live"

    def __init__(self, initial: FabricConfig, menu: tuple[int, ...], *, warmup_counts: list[int],
                 round_counts: dict[int, list[int]], scheduled_n: dict[int, int], predictor: LocalSurfacePredictor):
        super().__init__(initial, menu)
        if len(warmup_counts) < MIN_HISTORY_S:
            raise PolicyError(f"warm-up history has {len(warmup_counts)} s, the frozen forecaster needs {MIN_HISTORY_S}")
        self.warm = [int(v) for v in warmup_counts]
        self.round_counts = {int(k): [int(x) for x in v] for k, v in round_counts.items()}
        self.scheduled_n = {int(k): int(v) for k, v in scheduled_n.items()}
        for k, v in self.round_counts.items():
            if sum(v) != self.scheduled_n[k]:
                raise PolicyError(f"round {k}: per-second counts sum to {sum(v)}, scheduled {self.scheduled_n[k]}")
        self.predictor = predictor
        self.forecaster = CausalEwma()
        candidates = [config_with_b(initial, b) for b in menu]
        self.controller = PredictiveController(
            predictor, initial, arrival_forecaster=self.forecaster, candidate_configs=candidates,
            horizon_seconds=PREDICTIVE["horizon_seconds"], latency_penalty_per_ms=PREDICTIVE["latency_penalty_per_ms"],
            backlog_penalty_per_tps=PREDICTIVE["backlog_penalty_per_tps"],
            minimum_utility_improvement=PREDICTIVE["minimum_utility_improvement"],
            cooldown_samples=PREDICTIVE["cooldown_samples"], max_telemetry_age_s=PREDICTIVE["max_telemetry_age_s"])

    def history_before_decision(self, decision_index: int) -> list[float]:
        """Per-second arrival counts available when decision d is taken: warm-up + rounds 0..d. Never later rounds."""
        hist: list[float] = [float(v) for v in self.warm]
        for r in range(0, decision_index + 1):
            hist.extend(float(v) for v in self.round_counts[r])
        return hist

    def decide(self, decision_index, observation, current, telemetry_age_s):
        if observation is None:
            d = self.controller.reject_observation("telemetry_missing_fail_safe", error="round telemetry unavailable")
            return self._result(decision_index, current.max_message_count, current, d.reason, abstained=True, metadata=d.metadata)
        if observation.rows != self.scheduled_n[decision_index]:
            d = self.controller.reject_observation("observed_arrivals_differ_from_schedule_fail_safe",
                                                   error=f"round {decision_index}: {observation.rows} rows, "
                                                         f"{self.scheduled_n[decision_index]} scheduled")
            return self._result(decision_index, current.max_message_count, current, d.reason, abstained=True, metadata=d.metadata)
        hist = self.history_before_decision(decision_index)
        state = ControllerState(
            timestamp_utc=ms_to_iso(observation.wall1_ms), current_config=current,
            observed_latency_ms=observation.latency_mean_ms, observed_latency_p95_ms=observation.latency_p95_ms,
            failed_transactions=observation.failed, arrival_history_tps=tuple(hist), telemetry_age_s=telemetry_age_s)
        d = self.controller.decide(state)
        abstained = d.reason.endswith("fail_safe")
        meta = dict(d.metadata)
        meta.update({"forecast_tps": d.predicted_arrival_rate_tps, "history_seconds_used": len(hist),
                     "history_last_round_used": decision_index, "forecast_model": self.forecaster.model_identity,
                     "performance_model": self.predictor.identity, "forecast_horizon_s": HORIZON_S,
                     "forecast_target": f"mean arrival rate of segment {decision_index} (the next 60 s)"})
        return self._result(decision_index, d.config.max_message_count, current, d.reason, abstained=abstained, metadata=meta)

    def applied(self, config):
        self.controller.configuration_applied(config)


def build_policy_v2(arm_doc: dict, initial: FabricConfig) -> ArmPolicy:
    if arm_doc["arm"] != "predictive_live":
        return build_policy_v1(arm_doc, initial)
    p = arm_doc["predictive_live"]
    menu = tuple(int(b) for b in arm_doc["menu_B"])
    predictor = LocalSurfacePredictor(p["surface_points"])
    return PredictiveLivePolicy(initial, menu, warmup_counts=p["warmup_counts"],
                                round_counts={int(k): v for k, v in p["round_counts"].items()},
                                scheduled_n={int(k): v for k, v in p["scheduled_n"].items()}, predictor=predictor)
