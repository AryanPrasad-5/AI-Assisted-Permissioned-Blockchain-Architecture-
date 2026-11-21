"""A0g: the three telemetry freshness limits are operator-supplied, validated, and reach the validators (2026-11-21).

Checklist items 29-35: source-skew boundary, future-skew boundary, clock-discrepancy boundary, invalid threshold, missing
threshold under strict Stage 3, CLI-to-validator propagation, historical-caller compatibility. Offline; fixtures only.
"""

from __future__ import annotations

import dataclasses
import json
import socket
import subprocess
import urllib.request
from datetime import datetime, timedelta

import pytest

from tests import a0f_support as f
from tests import a0g_support as g
from fabric_perf.controllers import live_integration as li
from fabric_perf.controllers import stage3_shadow_runner as runner
from fabric_perf.controllers import telemetry_freshness as tf
from fabric_perf.controllers.telemetry_freshness import FreshnessPolicy

import e1_stage3_shadow_runner as cli


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("a process or network call was attempted")
    for obj, names in ((subprocess, ("run", "Popen", "call", "check_output")), (socket, ("create_connection", "getaddrinfo")),
                       (urllib.request, ("urlopen",))):
        for name in names:
            monkeypatch.setattr(obj, name, boom)


def c(n=5, latency=100):
    return {"count": n, "latency_ms": latency, "status": "success"}


SKEW_STEPS = [f.step(client=c()), f.step(client=c(), ledger_valid_tx=5), f.step(6.0, ledger_valid_tx=5)]


def skew_params():
    params = f.params()
    params["telemetry_adapter"]["window_s"] = 12.0
    return params


def frame_two(tmp_path, name, *, steps, policy, transform=None, params=None):
    _, rows, _ = g.run(tmp_path, steps=steps, policy_doc=policy, params_doc=params or f.params(), name=name, transform=transform)
    return f.by_frame(rows)[2]


# ---------------------------------------------------------------------------------------------- 29 source skew
def test_source_skew_limit_changes_the_outcome_for_the_same_evidence(tmp_path):
    base = dict(freshness_max_age_s=30.0, client_stale_after_s=30.0)
    tight = frame_two(tmp_path, "tight", steps=SKEW_STEPS, params=skew_params(), policy=g.policy_v2(**base, max_source_skew_s=5.0))
    loose = frame_two(tmp_path, "loose", steps=SKEW_STEPS, params=skew_params(), policy=g.policy_v2(**base, max_source_skew_s=10.0))
    assert tight["static"]["frame"]["status"] == "rejected" and tight["static"]["frame"]["reason_code"] == "telemetry_inconsistent"
    assert "source_timestamp_skew_exceeds_limit" in tight["static"]["error"]
    assert loose["static"]["frame"]["status"] == "accepted"
    assert all(r["frame"]["status"] == "accepted" for r in loose.values())


def test_a_source_skew_tighter_than_the_default_rejects_what_the_default_accepts(tmp_path):
    # client completions ~3 s old next to a fresh ledger interval: inside the default 5 s skew, outside a 1 s skew
    steps = [f.step(client=c()), f.step(client=c(), ledger_valid_tx=5), f.step(3.0, ledger_valid_tx=5)]
    default = frame_two(tmp_path, "d", steps=steps, policy=g.policy_v2(max_source_skew_s=5.0))
    tight = frame_two(tmp_path, "t", steps=steps, policy=g.policy_v2(max_source_skew_s=1.0))
    assert default["static"]["frame"]["status"] == "accepted"
    assert tight["static"]["frame"]["status"] == "rejected" and "source_timestamp_skew_exceeds_limit" in tight["static"]["error"]


# ---------------------------------------------------------------------------------------------- 30 future skew
def stamp_ahead(seconds):
    def transform(index, frame):
        if index != 2:
            return frame
        observed = datetime.fromisoformat(frame.observation_timestamp_utc.replace("Z", "+00:00"))
        ahead = (observed + timedelta(seconds=seconds)).isoformat()
        return dataclasses.replace(frame, source_timestamps_utc={**frame.source_timestamps_utc, "ledger": ahead})
    return transform


def test_future_skew_limit_changes_the_outcome_for_the_same_evidence(tmp_path):
    steps = f.steady(2)
    zero = frame_two(tmp_path, "zero", steps=steps, policy=g.policy_v2(max_future_skew_s=0.0), transform=stamp_ahead(0.4))
    half = frame_two(tmp_path, "half", steps=steps, policy=g.policy_v2(max_future_skew_s=0.5), transform=stamp_ahead(0.4))
    assert {r["frame"]["reason_code"] for r in zero.values()} == {"future_dated_observation"}
    assert all(r["frame"]["status"] == "accepted" for r in half.values())


def test_a_source_ahead_by_more_than_the_future_limit_is_rejected_even_when_the_limit_is_loosened(tmp_path):
    steps = f.steady(2)
    far = frame_two(tmp_path, "far", steps=steps, policy=g.policy_v2(max_future_skew_s=0.5), transform=stamp_ahead(0.9))
    assert {r["frame"]["reason_code"] for r in far.values()} == {"future_dated_observation"}


# ---------------------------------------------------------------------------------------------- 31 clock discrepancy
class SteppedClock:
    """Wall time that jumps by ``step_s`` between the first and second collection while monotonic time advances 1 s."""

    def __init__(self, start, step_s):
        self.wall, self.mono, self.step_s, self.calls = start, 0, step_s, 0

    def utc_now(self):
        return self.wall.isoformat(timespec="milliseconds")

    def monotonic_ns(self):
        return self.mono

    def advance(self):
        self.wall += timedelta(seconds=1 + self.step_s)
        self.mono += 1_000_000_000


def clock_detected(tmp_path, name, *, limit, step_s):
    inputs_dir = tmp_path / f"in_{name}"
    inputs_dir.mkdir()
    inputs = runner.load_inputs(f.write(inputs_dir / "p.json", f.params()), f.write(inputs_dir / "c.json", g.policy_v2(max_clock_discrepancy_s=limit)),
                                f.write(inputs_dir / "x.json", f.fixture(steps=f.steady(1))))
    values = inputs.values
    freshness = inputs.policy.freshness_policy()
    rig = runner.FixtureRig(tmp_path / f"rig_{name}", channel=values["channel"], run_id=values["run_id"], fixture=inputs.fixture,
                            window_s=values["telemetry_adapter"]["window_s"], stale_after_s=inputs.policy.client_stale_after_s,
                            recorder_latency=values["telemetry_adapter"]["recorder_latency"], endpoints={"p": "http://127.0.0.1:1/metrics"},
                            reported=inputs.fixture.channel_configuration, freshness=freshness, horizon_seconds=10)
    clock = SteppedClock(inputs.fixture.start, step_s)
    rig.provider._clock = clock
    assert rig.provider._clock_monitor.max_discrepancy_s == limit               # the operator value is the monitor's limit
    rig.provider.collect(run_id=values["run_id"], decision_index=0)
    clock.advance()
    frame = rig.provider.collect(run_id=values["run_id"], decision_index=1)
    return "wall_clock_step_detected" in frame.inconsistencies, frame.provenance["clock_consistency"]


def test_clock_discrepancy_limit_changes_the_outcome_for_the_same_clock_step(tmp_path):
    small_limit, small_info = clock_detected(tmp_path, "a", limit=0.5, step_s=0.8)
    large_limit, large_info = clock_detected(tmp_path, "b", limit=1.5, step_s=0.8)
    assert small_limit is True and small_info["limit_s"] == 0.5 and small_info["discrepancy_s"] == pytest.approx(0.8, abs=1e-6)
    assert large_limit is False and large_info["limit_s"] == 1.5 and large_info["discrepancy_s"] is None


def test_the_fixture_runner_does_not_exercise_the_clock_check_and_says_so(tmp_path):
    # Frames in fixture mode carry a scripted observation time, so the monotonic cross-check is never evaluated. The limit is
    # applied to the provider (previous test) but a fixture run cannot show a Windows-WSL clock agreement.
    _, rows, _ = g.run(tmp_path, steps=f.steady(2))
    assert rows[0]["schema"] == "stage3_shadow_decision/2"
    manifest = json.loads(next((tmp_path / "out_run").glob("run_manifest.json")).read_text(encoding="utf-8"))
    assert "not compensated" in manifest["freshness"]["note"]


# ---------------------------------------------------------------------------------------------- 32 invalid thresholds
BAD = [("max_source_skew_s", 0), ("max_source_skew_s", -1.0), ("max_source_skew_s", float("nan")), ("max_source_skew_s", float("inf")),
       ("max_source_skew_s", 31.0), ("max_source_skew_s", "5"), ("max_source_skew_s", True),
       ("max_future_skew_s", -0.1), ("max_future_skew_s", float("nan")), ("max_future_skew_s", 1.5), ("max_future_skew_s", "0"),
       ("max_clock_discrepancy_s", 0), ("max_clock_discrepancy_s", -1), ("max_clock_discrepancy_s", float("inf")),
       ("max_clock_discrepancy_s", 6.0), ("max_clock_discrepancy_s", None)]


@pytest.mark.parametrize("key,value", BAD)
def test_an_invalid_threshold_is_rejected_before_anything_is_created(tmp_path, key, value):
    doc = g.policy_v2()
    doc["telemetry"][key] = value
    path = f.write(tmp_path / "policy.json", doc)
    with pytest.raises(runner.Stage3RunnerError, match="controller policy rejected"):
        runner.load_inputs(f.PARAMS_FILE, path, f.NOMINAL_FILE)


def test_units_are_seconds_and_a_milliseconds_slip_is_an_error_not_a_clip(tmp_path):
    for key, slip in (("max_source_skew_s", 5000), ("max_clock_discrepancy_s", 1000), ("max_future_skew_s", 500)):
        doc = g.policy_v2()
        doc["telemetry"][key] = slip
        with pytest.raises(runner.Stage3RunnerError):
            runner.load_inputs(f.PARAMS_FILE, f.write(tmp_path / f"{key}.json", doc), f.NOMINAL_FILE)


# ---------------------------------------------------------------------------------------------- 33 missing thresholds
@pytest.mark.parametrize("key", ["max_source_skew_s", "max_future_skew_s", "max_clock_discrepancy_s"])
def test_a_missing_threshold_in_a_v2_policy_is_an_error_with_no_default(tmp_path, key):
    doc = g.policy_v2()
    del doc["telemetry"][key]
    with pytest.raises(runner.Stage3RunnerError, match=f"telemetry.{key}: missing or null"):
        runner.load_inputs(f.PARAMS_FILE, f.write(tmp_path / "p.json", doc), f.NOMINAL_FILE)


def test_strict_stage3_refuses_a_v1_policy_that_would_use_library_defaults(tmp_path):
    with pytest.raises(runner.Stage3RunnerError, match="operator-supplied"):
        runner.load_inputs(f.PARAMS_FILE, f.POLICY_FILE, f.NOMINAL_FILE, require_operator_freshness=True)
    ok = runner.load_inputs(f.PARAMS_FILE, g.POLICY_V2_FILE, f.NOMINAL_FILE, require_operator_freshness=True)
    assert ok.policy.freshness_basis == runner.FRESHNESS_OPERATOR


def test_a_v1_key_set_is_not_accepted_under_a_v2_schema_and_vice_versa(tmp_path):
    v2_without = g.policy_v2()
    v2_without["schema"] = "stage3_controller_policy/1"
    with pytest.raises(runner.Stage3RunnerError, match="telemetry.max_source_skew_s: unknown key"):
        runner.load_inputs(f.PARAMS_FILE, f.write(tmp_path / "a.json", v2_without), f.NOMINAL_FILE)


# ---------------------------------------------------------------------------------------------- 34 propagation
def test_the_three_values_reach_the_provider_the_runner_and_the_frame_validator(tmp_path):
    inputs = runner.load_inputs(f.PARAMS_FILE, f.write(tmp_path / "p.json", g.policy_v2(
        max_source_skew_s=2.5, max_future_skew_s=0.25, max_clock_discrepancy_s=0.75)), f.NOMINAL_FILE)
    wanted = FreshnessPolicy(max_age_s=5.0, max_future_skew_s=0.25, max_source_skew_s=2.5, max_clock_discrepancy_s=0.75)
    assert inputs.policy.freshness_policy() == wanted
    seen = {}
    original = li.TelemetryFrame.validate

    def spy(self, **kwargs):
        seen["policy"] = kwargs.get("policy")
        return original(self, **kwargs)
    li.TelemetryFrame.validate = spy
    try:
        manifest = runner.run_fixture_shadow(inputs, tmp_path / "out")
    finally:
        li.TelemetryFrame.validate = original
    assert seen["policy"] == wanted                                         # the frame validator received the operator policy
    assert manifest["freshness"]["limits_s"] == wanted.as_dict() and manifest["freshness"]["basis"] == "operator_supplied"
    native = json.loads((tmp_path / "out" / "native" / "integration_manifest.json").read_text(encoding="utf-8"))
    assert native["freshness_policy"] == wanted.as_dict()                    # the library's own record of the policy it applied


def test_the_cli_applies_the_policy_file_it_is_given(tmp_path, capsys):
    steps = [f.step(client=c()), f.step(client=c(), ledger_valid_tx=5), f.step(3.0, ledger_valid_tx=5)]
    fixture = f.write(tmp_path / "fixture.json", f.fixture(steps=steps))
    outcomes = {}
    for label, skew in (("tight", 1.0), ("default", 5.0)):
        policy = f.write(tmp_path / f"policy_{label}.json", g.policy_v2(max_source_skew_s=skew))
        code = cli.main(["fixture", "--params", str(f.PARAMS_FILE), "--controller-policy", str(policy), "--fixture", str(fixture),
                         "--out-dir", str(tmp_path / f"out_{label}"), "--require-operator-freshness"])
        assert code == 0
        outcomes[label] = json.loads(capsys.readouterr().out)["frame_status_counts"]
    assert outcomes["tight"]["rejected"] == outcomes["default"]["rejected"] + 1


def test_the_cli_flag_refuses_a_v1_policy(tmp_path, capsys):
    code = cli.main(["fixture", "--params", str(f.PARAMS_FILE), "--controller-policy", str(f.POLICY_FILE), "--fixture", str(f.NOMINAL_FILE),
                     "--out-dir", str(tmp_path / "o"), "--require-operator-freshness"])
    assert code == cli.EXIT_INVALID and not (tmp_path / "o").exists()
    assert "operator-supplied" in capsys.readouterr().out


def test_a_conflicting_runner_policy_is_refused(tmp_path):
    inputs = runner.load_inputs(f.PARAMS_FILE, g.POLICY_V2_FILE, f.NOMINAL_FILE)
    with pytest.raises(runner.Stage3RunnerError, match="conflicts"):
        runner.Stage3ShadowRunner({"static": runner.StaticController(inputs.static_config)}, inputs.static_config, run_id="r", shadow=True,
                                  max_telemetry_age_s=5.0, freshness_policy=FreshnessPolicy(max_age_s=4.0))


# ---------------------------------------------------------------------------------------------- 35 historical callers
def test_the_library_defaults_and_the_library_runner_are_unchanged():
    assert (tf.DEFAULT_MAX_OBSERVATION_AGE_S, tf.DEFAULT_MAX_SOURCE_SKEW_S, tf.DEFAULT_MAX_CLOCK_DISCREPANCY_S) == (5.0, 5.0, 1.0)
    default = FreshnessPolicy()
    assert (default.max_age_s, default.max_future_skew_s, default.max_source_skew_s, default.max_clock_discrepancy_s) == (5.0, 0.0, 5.0, 1.0)
    from fabric_perf.controllers.base import StaticController, FabricConfig
    plain = li.IntegratedControllerRunner({"static": StaticController(FabricConfig())}, FabricConfig(), run_id="r", shadow=True)
    assert plain.freshness_policy == FreshnessPolicy()


def test_a_v1_policy_runs_as_before_with_library_defaults_and_v1_rows(tmp_path):
    inputs = runner.load_inputs(f.PARAMS_FILE, f.POLICY_FILE, f.NOMINAL_FILE)
    assert inputs.policy.freshness_basis == runner.FRESHNESS_LEGACY
    assert inputs.policy.freshness_policy() == FreshnessPolicy(max_age_s=inputs.policy.freshness_max_age_s)
    manifest, rows, out = f.run(tmp_path)
    assert {r["schema"] for r in rows} == {"stage3_shadow_decision/1"} and "predictive_evidence" not in rows[0]
    assert "freshness" not in manifest and "predictive" not in manifest


def test_library_validate_defaults_for_a_historical_caller_are_unchanged():
    assert FreshnessPolicy().max_future_skew_s == 0.0
    import inspect
    assert inspect.signature(li.TelemetryFrame.validate).parameters["max_age_s"].default == 5.0
    assert inspect.signature(li.ProductionTelemetryProvider.__init__).parameters["freshness_policy"].default is None
