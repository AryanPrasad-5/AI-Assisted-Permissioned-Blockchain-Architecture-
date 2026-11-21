"""A0h: operator freshness limits reach the actual validators, including a clock-discrepancy fixture scenario (2026-11-21).

Checklist items 29-35. ``live_integration.py`` is pinned by the A0e candidate manifest and two existing tests, so it is NOT edited;
the Stage 3 boundary (``Stage3ShadowRunner`` plus the rig's provider) is where the one operator policy is injected. These tests prove
the injection by OUTCOME: the same evidence is accepted or rejected depending only on the limit, at the validators themselves.
"""

from __future__ import annotations

import dataclasses
import json
import socket
import subprocess
import urllib.request
from datetime import datetime, timedelta

import pytest

from tests import a0h_support as h
from fabric_perf.controllers import live_integration as li
from fabric_perf.controllers import stage3_shadow_runner as runner
from fabric_perf.controllers.base import StaticController
from fabric_perf.controllers.telemetry_freshness import ClockConsistencyMonitor, FreshnessPolicy

import e1_stage3_shadow_runner as cli

f, g = h.f, h.g


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


def wide(**limits):
    """Policy with age limits wide enough that only the limit under test can decide the outcome."""
    return g.policy_v2(freshness_max_age_s=30.0, client_stale_after_s=30.0, **limits)


def skew_params():
    params = f.params()
    params["telemetry_adapter"]["window_s"] = 12.0
    return params


def frame(rows, index):
    return f.by_frame(rows)[index]


def skew_frame(tmp_path, name, *, advance_s, limit):
    steps = [f.step(client=c()), f.step(client=c(), ledger_valid_tx=5), f.step(advance_s, ledger_valid_tx=5)]
    _, rows, _ = g.run(tmp_path, steps=steps, params_doc=skew_params(), policy_doc=wide(max_source_skew_s=limit), name=name)
    return frame(rows, 2)


def stamp_ahead(seconds):
    def transform(index, fr):
        if index != 2:
            return fr
        observed = datetime.fromisoformat(fr.observation_timestamp_utc.replace("Z", "+00:00"))
        return dataclasses.replace(fr, source_timestamps_utc={**fr.source_timestamps_utc, "ledger": (observed + timedelta(seconds=seconds)).isoformat()})
    return transform


def future_frame(tmp_path, name, *, ahead_s, limit):
    _, rows, _ = g.run(tmp_path, steps=f.steady(2), policy_doc=g.policy_v2(max_future_skew_s=limit), name=name, transform=stamp_ahead(ahead_s))
    return frame(rows, 2)


def rejected(row_set, code):
    return all(r["frame"]["status"] == "rejected" and r["frame"]["reason_code"] == code for r in row_set.values())


def accepted(row_set):
    return all(r["frame"]["status"] == "accepted" for r in row_set.values())


# ------------------------------------------------------------------------------------------ 29 constructor-policy propagation
OPERATOR = dict(max_source_skew_s=3.0, max_future_skew_s=0.25, max_clock_discrepancy_s=0.7)


def test_29_the_one_operator_policy_reaches_the_provider_the_monitor_the_runner_and_the_frame_validator(tmp_path, monkeypatch):
    providers, validated = [], []

    class RecordingProvider(li.ProductionTelemetryProvider):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            providers.append(self)

    original = li.TelemetryFrame.validate

    def spy(self, *a, **k):
        validated.append(k.get("policy"))
        return original(self, *a, **k)
    monkeypatch.setattr(runner, "ProductionTelemetryProvider", RecordingProvider)
    monkeypatch.setattr(li.TelemetryFrame, "validate", spy)
    manifest, _, out = h.run_captured(tmp_path, h.history(), policy_doc=h.policy_h60(**OPERATOR))
    expected = FreshnessPolicy(max_age_s=5.0, **OPERATOR)
    assert len(providers) == 1 and providers[0].freshness_policy == expected
    assert providers[0]._clock_monitor.max_discrepancy_s == 0.7 and providers[0].max_source_skew_s == 3.0
    assert validated and all(p == expected for p in validated)             # the runner's frame validation used the same four limits
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert summary["freshness_limits"] == expected.as_dict() == manifest["freshness"]["limits_s"]
    native = json.loads((out / "native" / "integration_manifest.json").read_text(encoding="utf-8"))
    assert native["freshness_policy"] == expected.as_dict()


def test_29b_the_runner_takes_the_policy_through_its_constructor_and_refuses_a_conflicting_age():
    config = f.fixture()["channel_configuration"]
    from fabric_perf.controllers.base import coerce_config
    cfg = coerce_config(config)
    policy = FreshnessPolicy(max_age_s=5.0, **OPERATOR)
    ctrl = {"static": StaticController(cfg)}
    made = runner.Stage3ShadowRunner(ctrl, cfg, run_id="r", shadow=True, max_telemetry_age_s=5.0, freshness_policy=policy)
    assert made.freshness_policy is policy
    with pytest.raises(runner.Stage3RunnerError, match="conflicts with max_telemetry_age_s"):
        runner.Stage3ShadowRunner(ctrl, cfg, run_id="r", shadow=True, max_telemetry_age_s=6.0, freshness_policy=policy)
    with pytest.raises(runner.Stage3RunnerError, match="must be a FreshnessPolicy"):
        runner.Stage3ShadowRunner(ctrl, cfg, run_id="r", shadow=True, max_telemetry_age_s=5.0, freshness_policy=dict(OPERATOR))
    with pytest.raises(TypeError):                                        # the policy is not optional on the Stage 3 route
        runner.Stage3ShadowRunner(ctrl, cfg, run_id="r", shadow=True, max_telemetry_age_s=5.0)


def test_29c_a_provider_with_a_different_policy_is_refused_even_when_only_the_age_agrees():
    from fabric_perf.controllers.base import coerce_config
    cfg = coerce_config(f.fixture()["channel_configuration"])
    operator = FreshnessPolicy(max_age_s=5.0, **OPERATOR)

    class Provider:
        def __init__(self, policy):
            self.freshness_policy = policy

        def collect(self, **kwargs):
            raise RuntimeError("collect reached")
    other = FreshnessPolicy(max_age_s=5.0, max_source_skew_s=4.0, max_future_skew_s=0.25, max_clock_discrepancy_s=0.7)
    stage3 = runner.Stage3ShadowRunner({"static": StaticController(cfg)}, cfg, run_id="r", shadow=True, max_telemetry_age_s=5.0, freshness_policy=operator)
    with pytest.raises(runner.Stage3RunnerError, match="one operator policy must reach both"):
        stage3.run_provider(Provider(other), samples=1)
    with pytest.raises(RuntimeError, match="collect reached"):            # the same policy passes the check and collection starts
        stage3.run_provider(Provider(operator), samples=1)
    library = li.IntegratedControllerRunner({"static": StaticController(cfg)}, cfg, run_id="r", shadow=True, max_telemetry_age_s=5.0)
    with pytest.raises(RuntimeError, match="collect reached"):            # the library alone compares max_age_s only: the gap this closes
        library.run_provider(Provider(other), samples=1)


# ------------------------------------------------------------------------------------------------- 30 source-skew boundary
def test_30_the_source_skew_limit_decides_the_outcome_of_identical_evidence(tmp_path):
    # In this fixture the oldest fresh source (the submit events) is stamped 0.15 s before the previous collection, so the spread
    # between sources is exactly advance_s + 0.15 s. 4.85 s therefore gives a spread of exactly 5.000 s.
    at_limit = skew_frame(tmp_path, "a", advance_s=4.85, limit=5.0)
    over = skew_frame(tmp_path, "b", advance_s=4.851, limit=5.0)
    tight = skew_frame(tmp_path, "c", advance_s=4.85, limit=4.99)
    assert accepted(at_limit)                                             # a spread equal to the limit is within it
    assert rejected(over, "telemetry_inconsistent") and "source_timestamp_skew_exceeds_limit" in over["static"]["error"]
    assert rejected(tight, "telemetry_inconsistent") and "source_timestamp_skew_exceeds_limit" in tight["static"]["error"]
    assert accepted(skew_frame(tmp_path, "d", advance_s=4.851, limit=5.5))   # the same 5.001 s spread passes under a looser limit


# --------------------------------------------------------------------------------------------------- 31 future-skew boundary
def test_31_the_future_skew_limit_decides_the_outcome_of_identical_evidence(tmp_path):
    assert accepted(future_frame(tmp_path, "a", ahead_s=0.4, limit=0.5))
    assert rejected(future_frame(tmp_path, "b", ahead_s=0.4, limit=0.0), "future_dated_observation")
    assert rejected(future_frame(tmp_path, "c", ahead_s=0.6, limit=0.5), "future_dated_observation")
    assert accepted(future_frame(tmp_path, "d", ahead_s=0.5, limit=0.5))   # exactly at the limit is accepted
    assert rejected(future_frame(tmp_path, "e", ahead_s=0.51, limit=0.5), "future_dated_observation")


# -------------------------------------------------------------------------------- 32 clock-discrepancy boundary, 35 fixture scenario
def clock_run(tmp_path, name, *, limit, step_s, captured=True):
    policy = h.policy_h60(max_clock_discrepancy_s=limit)
    if captured:
        return h.run_captured(tmp_path, h.history(), policy_doc=policy, name=name, wall_clock_steps={2: step_s})
    inputs_dir = tmp_path / f"in_{name}"
    inputs_dir.mkdir()
    inputs = runner.load_inputs(f.write(inputs_dir / "p.json", f.params()), f.write(inputs_dir / "c.json", policy),
                                f.write(inputs_dir / "x.json", f.fixture(steps=f.steady(3))))
    manifest = runner.run_fixture_shadow(inputs, tmp_path / f"out_{name}", _wall_clock_steps={2: step_s})
    return manifest, f.rows_of(tmp_path / f"out_{name}"), tmp_path / f"out_{name}"


def test_32_the_clock_discrepancy_limit_decides_the_outcome_for_the_same_wall_clock_step(tmp_path):
    _, small, _ = clock_run(tmp_path, "a", limit=0.79, step_s=0.8, captured=False)
    _, large, _ = clock_run(tmp_path, "b", limit=0.81, step_s=0.8, captured=False)
    assert rejected(frame(small, 2), "telemetry_inconsistent") and "wall_clock_step_detected" in frame(small, 2)["static"]["error"]
    assert accepted(frame(large, 2))
    for rows in (small, large):                                           # one step is reported once; the next frame is judged normally
        assert accepted(frame(rows, 3)) and accepted(frame(rows, 1))


def test_32b_a_step_far_below_the_limit_is_not_detected_and_a_step_above_the_ceiling_is_a_configuration_error(tmp_path):
    _, rows, _ = clock_run(tmp_path, "a", limit=1.0, step_s=0.2, captured=False)
    assert accepted(frame(rows, 2))
    with pytest.raises(runner.Stage3RunnerError):                         # the library's own ceiling (5 s) applies to the operator value
        clock_run(tmp_path, "b", limit=5.5, step_s=0.2, captured=False)
    with pytest.raises(ValueError):
        ClockConsistencyMonitor(5.5)


def test_35_a_clock_step_in_a_captured_history_run_makes_every_controller_abstain_on_that_frame_only(tmp_path):
    manifest, rows, _ = clock_run(tmp_path, "a", limit=0.5, step_s=0.8)
    bad = frame(rows, 2)
    assert rejected(bad, "telemetry_inconsistent") and all(r["outcome"] == "abstain" for r in bad.values())
    assert "wall_clock_step_detected" in bad["predictive"]["error"] and bad["predictive"]["predictive_evidence"]["forecast"]["value_tps"] is None
    good = frame(rows, 3)
    assert accepted(good) and good["predictive"]["outcome"] in ("propose", "hold")       # recovery: a valid history is used again
    assert good["predictive"]["predictive_evidence"]["forecast"]["value_tps"] == 55.0
    assert manifest["status"] == "COMPLETED" and manifest["captured_history"]["frames"][2]["no_future_data"] is True
    _, accepted_rows, _ = clock_run(tmp_path, "b", limit=1.5, step_s=0.8)
    assert accepted(frame(accepted_rows, 2))


def test_35b_only_the_hook_turns_on_the_clock_check_default_runs_still_script_every_observation_time(tmp_path, monkeypatch):
    collected = []
    original = li.ProductionTelemetryProvider.collect

    def spy(self, *, run_id, decision_index, observation_timestamp_utc=None):
        collected.append(observation_timestamp_utc)
        return original(self, run_id=run_id, decision_index=decision_index, observation_timestamp_utc=observation_timestamp_utc)
    monkeypatch.setattr(li.ProductionTelemetryProvider, "collect", spy)
    g.run(tmp_path, steps=f.steady(2), name="default")
    assert collected and all(stamp is not None for stamp in collected)    # scripted: the clock-discrepancy check does not run (A0g F4)
    collected.clear()
    clock_run(tmp_path, "hook", limit=1.0, step_s=0.0, captured=False)
    assert collected and all(stamp is None for stamp in collected)        # unscripted: the monitor compares wall and monotonic time


# ----------------------------------------------------------------------------------------- 33 strict thresholds are required
@pytest.mark.parametrize("key", ["max_source_skew_s", "max_future_skew_s", "max_clock_discrepancy_s"])
def test_33_each_missing_operator_limit_is_refused_by_name(tmp_path, key):
    doc = g.policy_v2()
    del doc["telemetry"][key]
    inputs_dir = tmp_path / "in"
    inputs_dir.mkdir()
    with pytest.raises(runner.Stage3RunnerError, match=key):
        runner.load_inputs(f.write(inputs_dir / "p.json", f.params()), f.write(inputs_dir / "c.json", doc), f.write(inputs_dir / "x.json", f.fixture()))


@pytest.mark.parametrize("key,bad", [("max_source_skew_s", -1), ("max_source_skew_s", "5"), ("max_source_skew_s", True), ("max_source_skew_s", None),
                                      ("max_source_skew_s", 31), ("max_future_skew_s", -0.1), ("max_future_skew_s", 2),
                                      ("max_clock_discrepancy_s", 0), ("max_clock_discrepancy_s", 6), ("max_clock_discrepancy_s", float("nan"))])
def test_33b_an_invalid_limit_is_refused_not_clipped(tmp_path, key, bad):
    doc = g.policy_v2()
    doc["telemetry"][key] = bad
    inputs_dir = tmp_path / "in"
    inputs_dir.mkdir()
    with pytest.raises(runner.Stage3RunnerError):
        runner.load_inputs(f.write(inputs_dir / "p.json", f.params()), f.write(inputs_dir / "c.json", doc), f.write(inputs_dir / "x.json", f.fixture()))


def test_33c_strict_stage_3_refuses_a_policy_without_operator_limits_on_the_api_and_on_the_cli(tmp_path, capsys):
    paths = [f.write(tmp_path / "p.json", f.params()), f.write(tmp_path / "c.json", f.policy()), f.write(tmp_path / "x.json", f.fixture())]
    with pytest.raises(runner.Stage3RunnerError, match="operator-supplied"):
        runner.load_inputs(*paths, require_operator_freshness=True)
    assert runner.load_inputs(*paths).policy.freshness_basis == runner.FRESHNESS_LEGACY      # without the flag it loads, labelled legacy
    argv = ["fixture", "--params", str(paths[0]), "--controller-policy", str(paths[1]), "--fixture", str(paths[2]), "--out-dir", str(tmp_path / "o"),
            "--require-operator-freshness"]
    assert cli.main(argv) == cli.EXIT_INVALID and not (tmp_path / "o").exists()
    capsys.readouterr()


# ------------------------------------------------------------------------------------------------- 34 backward compatibility
def test_34_schema_1_runs_keep_the_library_defaults_and_their_original_output_shape(tmp_path):
    manifest, rows, out = f.run(tmp_path, name="legacy")
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert {r["schema"] for r in rows} == {"stage3_shadow_decision/1"} and "freshness_limits" not in summary and "freshness" not in manifest
    native = json.loads((out / "native" / "integration_manifest.json").read_text(encoding="utf-8"))
    assert native["freshness_policy"] == FreshnessPolicy(max_age_s=5.0).as_dict()          # library defaults for the other three limits


def test_34b_the_library_defaults_and_the_pinned_library_file_are_unchanged():
    assert FreshnessPolicy().as_dict() == {"max_age_s": 5.0, "max_future_skew_s": 0.0, "max_source_skew_s": 5.0, "max_clock_discrepancy_s": 1.0}
    import json as _json
    entries = {e["path"]: e for e in _json.loads(f.CANDIDATE_MANIFEST.read_text(encoding="utf-8"))["entries"]}
    import hashlib
    path = h.REPO / "src/fabric_perf/controllers/live_integration.py"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == entries["src/fabric_perf/controllers/live_integration.py"]["sha256"]
