"""A0h: captured history -> history guard -> fixture forecaster -> predictive contract -> fixture evaluation -> Stage 3 record (2026-11-21).

Checklist items 36-44. Offline. The forecaster and the performance table are labelled fixtures; the HISTORY is real-shaped, timestamped,
fed only up to each decision time and checked by the real ``GuardedForecaster``. Nothing is loaded, trained or forecast.
"""

from __future__ import annotations

import json
import socket
import subprocess
import urllib.request
from datetime import datetime, timedelta

import pytest

from tests import a0h_support as h
from fabric_perf.controllers import captured_history as ch
from fabric_perf.controllers import channel_selection as cs
from fabric_perf.controllers import stage3_predictive as sp
from fabric_perf.controllers import stage3_shadow_runner as runner

import e1_stage3_shadow_runner as cli


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("a process or network call was attempted")
    for obj, names in ((subprocess, ("run", "Popen", "call", "check_output")), (socket, ("create_connection", "getaddrinfo")),
                       (urllib.request, ("urlopen",))):
        for name in names:
            monkeypatch.setattr(obj, name, boom)


def predictive(rows, frame):
    return [r for r in rows if r["controller"] == "predictive" and r["frame"]["index"] == frame][0]


def spy_forecaster(monkeypatch):
    """Record every call that reaches the fixture estimator (after the real guard), keeping the real signature the guard inspects."""
    calls = []
    original = sp.TimestampedFixtureForecaster.predict_timestamped

    def spy(self, history_timestamps_utc, history_tps, horizon_seconds, *, now_utc, max_history_age_s=5.0, history_ref=None):
        calls.append({"times": tuple(history_timestamps_utc), "counts": tuple(history_tps), "horizon": horizon_seconds, "now": now_utc,
                      "max_age": max_history_age_s, "ref": history_ref})
        return original(self, history_timestamps_utc, history_tps, horizon_seconds, now_utc=now_utc, max_history_age_s=max_history_age_s,
                        history_ref=history_ref)
    monkeypatch.setattr(sp.TimestampedFixtureForecaster, "predict_timestamped", spy)
    return calls


# ------------------------------------------------------------------------------------------------------- 36 valid history
def test_36_a_valid_captured_history_reaches_the_predictive_controller_and_is_recorded(tmp_path):
    manifest, rows, out = h.run_captured(tmp_path, h.history())
    assert manifest["status"] == "COMPLETED" and manifest["live_ready"] is False
    for frame in (1, 2, 3):
        row = predictive(rows, frame)
        evidence = row["predictive_evidence"]
        reference = evidence["workload_observation"]["history_reference"]
        assert row["frame"]["status"] == "accepted" and row["outcome"] in ("propose", "hold") and evidence["failure"] is None
        assert reference["status"] == "complete" and reference["bin_count"] == 43_201 + frame and reference["source"] == "synthetic-test-day"
        assert reference["first_bin_timestamp_utc"] == "2026-09-21T00:00:00+00:00"
        assert datetime.fromisoformat(reference["last_bin_timestamp_utc"]) + timedelta(seconds=1) <= datetime.fromisoformat(row["frame"]["observation_timestamp_utc"])
        assert evidence["forecast"]["value_tps"] == 55.0 and evidence["forecast"]["is_fixture_value"] is True
        assert "captured history synthetic-test-day" in evidence["workload_observation"]["basis"] and "not Fabric" in evidence["workload_observation"]["basis"]
    block = manifest["captured_history"]
    assert [f["validation_outcome"] for f in block["frames"]] == [ch.VALID_COMPLETE] * 4
    assert [f["valid_bin_count"] for f in block["frames"]] == [43_201, 43_202, 43_203, 43_204]
    assert all(f["no_future_data"] for f in block["frames"]) and block["source"]["is_fabric_observation"] is False
    assert block["zero_policy"] == "verified_only" and block["live_semantics"] is True and block["forecast_horizon_seconds"] == 60
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert summary["captured_history_frame_outcomes"] == {"complete_prefix": 4}
    assert (out / "run_manifest.json").is_file() and runner.verify_run_directory(out) == []


# ----------------------------------------------------------------------------------------------------- 37 invalid history
def test_37_a_history_with_an_unverified_zero_never_becomes_a_forecast(tmp_path):
    manifest, rows, _ = h.run_captured(tmp_path, h.history({100: 0}))
    for frame in (1, 2, 3):
        row = predictive(rows, frame)
        assert row["outcome"] == "abstain" and "unavailable seconds" in row["error"] and "not interpolated" in row["error"]
        assert row["predictive_evidence"]["forecast"]["value_tps"] is None and row["predicted_performance"] is None
        assert row["config_change_proposed"] is False and row["recommended_config"] is None
        assert row["predictive_evidence"]["workload_observation"]["history_reference"]["status"] == "incomplete"
    assert [f["validation_outcome"] for f in manifest["captured_history"]["frames"]] == [ch.VALID_INCOMPLETE] * 4
    assert manifest["captured_history"]["frames"][1]["unavailable_seconds"] == 1
    for controller in ("static", "reactive"):                             # the other two controllers do not depend on the history
        assert all(r["frame"]["status"] == "accepted" and r["outcome"] == "hold"
                   for r in rows if r["controller"] == controller and r["frame"]["index"] > 0)


def test_37b_a_history_shorter_than_600_bins_is_refused_by_the_guard_with_its_reason(tmp_path, monkeypatch):
    calls = spy_forecaster(monkeypatch)
    manifest, rows, _ = h.run_captured(tmp_path, h.history(), start_s=300)
    for frame in (1, 2, 3):
        row = predictive(rows, frame)
        assert row["outcome"] == "abstain" and row["error"].startswith("forecast_history_insufficient:") and "600 required" in row["error"]
        assert row["predictive_evidence"]["failure"]["error_type"] == "PredictiveIntegrationError"
        assert row["predictive_evidence"]["forecast"]["value_tps"] is None
    assert calls == []                                                    # the estimator never saw the short history
    assert {f["validation_outcome"] for f in manifest["captured_history"]["frames"]} == {ch.VALID_INSUFFICIENT}


def test_37c_a_missing_or_broken_forecast_is_recorded_with_its_reason_and_is_never_a_valid_forecast(tmp_path):
    for mode, expected in (("missing", "forecast_result_invalid"), ("nan", "finite"), ("negative", "finite")):
        steps = h.f.steady(3)
        steps[2] = dict(steps[2], forecast=mode)
        _, rows, _ = h.run_captured(tmp_path, h.history(), steps=steps, name=f"m_{mode}")
        row = predictive(rows, 2)
        assert row["outcome"] == "abstain" and expected in row["error"] and row["predictive_evidence"]["forecast"]["value_tps"] is None
        assert predictive(rows, 3)["outcome"] in ("propose", "hold")      # the next frame recovers


# ---------------------------------------------------------------------------------------- 38 fixture forecaster, 39 guards
def test_38_the_fixture_forecaster_receives_the_genuine_timestamped_history(tmp_path, monkeypatch):
    calls = spy_forecaster(monkeypatch)
    h.run_captured(tmp_path, h.history())
    assert len(calls) == 3                                                # frames 1-3; frame 0 is the initialization frame
    for call in calls:
        times = [datetime.fromisoformat(t) for t in call["times"]]
        assert times[0] == h.DAY_START and len(times) >= ch.MIN_HISTORY_BINS and call["horizon"] == 60
        assert all(b - a == timedelta(seconds=1) for a, b in zip(times, times[1:]))
        assert times[-1] + timedelta(seconds=1) <= datetime.fromisoformat(call["now"])          # no bin that was incomplete at the decision time
        assert call["ref"].status == "complete" and call["ref"].bin_count == len(times) and len(call["counts"]) == len(times)
        assert call["counts"][:3] == (20.0, 21.0, 22.0)                   # the stored values, unchanged
    assert [len(c["times"]) for c in calls] == [43_202, 43_203, 43_204]


def test_39_the_real_history_guard_runs_on_every_predictive_decision(tmp_path, monkeypatch):
    seen = []
    original = sp.GuardedForecaster.validate_history

    def spy(self, timestamps, values, horizon_seconds, *, now_utc, max_history_age_s):
        seen.append(len(tuple(timestamps)))
        return original(self, timestamps, values, horizon_seconds, now_utc=now_utc, max_history_age_s=max_history_age_s)
    monkeypatch.setattr(sp.GuardedForecaster, "validate_history", spy)
    h.run_captured(tmp_path, h.history())
    assert seen == [43_202, 43_203, 43_204]


@pytest.mark.parametrize("forecast,abstains", [(7.5, True), (9.999, True), (10.0, False), (55.0, False), (200.0, False), (200.01, True)])
def test_39b_a_forecast_outside_10_to_200_makes_the_predictive_controller_abstain_without_clipping(tmp_path, forecast, abstains):
    _, rows, _ = h.run_captured(tmp_path, h.history(), forecast=forecast, name=f"f{forecast}")
    row = predictive(rows, 2)
    evidence = row["predictive_evidence"]
    if abstains:
        assert row["outcome"] == "abstain" and evidence["ood_status"] == "outside_training_grid"
        assert "arrival_rate_outside_training_grid" in row["error"] and "not clipped" in row["error"]
        assert evidence["failure"]["error_type"] == "UnsupportedExtrapolationError"
        assert evidence["forecast"]["value_tps"] == forecast and row["predicted_performance"] is None and row["recommended_config"] is None
    else:
        assert row["outcome"] in ("propose", "hold") and evidence["failure"] is None and evidence["ood_status"] == "not_applicable_fixture_model"


# ------------------------------------------------------------------------------------------ 40 three controllers, one frame
def test_40_all_three_controllers_receive_the_same_frame(tmp_path):
    _, rows, _ = h.run_captured(tmp_path, h.history())
    by_frame = h.f.by_frame(rows)
    assert sorted(by_frame) == [0, 1, 2, 3]
    for index, row_set in by_frame.items():
        assert sorted(row_set) == ["predictive", "reactive", "static"]
        frames = {json.dumps(r["frame"], sort_keys=True) for r in row_set.values()}
        assert len(frames) == 1, index                                    # identical frame identity, timestamp and status
        assert len({r["frame"]["id"] for r in row_set.values()}) == 1
        assert {r["frame"]["observation_timestamp_utc"] for r in row_set.values()} == {f"2026-09-21T12:00:0{index + 1}.000+00:00"}


# -------------------------------------------------------------------------------------------- 41 evidence, 42 no actuation
def test_41_the_decision_record_carries_the_predictive_evidence_and_labels_it_a_fixture(tmp_path):
    manifest, rows, _ = h.run_captured(tmp_path, h.history())
    assert {r["schema"] for r in rows} == {"stage3_shadow_decision/2"}
    evidence = predictive(rows, 1)["predictive_evidence"]
    assert evidence["mode"] == "fixture" and evidence["fixture_data"] is True and evidence["real_inference_performed"] is False
    assert evidence["forecast"]["is_fixture_value"] is True and evidence["performance_model"]["predictions_are_fixture_values"] is True
    assert "NOT the frozen EWMA" in evidence["forecast"]["identity"] and "NOT the frozen HFBTP" in evidence["performance_model"]["identity"]
    assert evidence["forecast"]["origin_timestamp_utc"] == "2026-09-21T12:00:02+00:00" and evidence["recommendation_is_not_applied"] is True
    assert evidence["performance_model"]["feature_schema_id"] == sp.FEATURE_SCHEMA_ID
    assert all(r["predictive_evidence"] is None for r in rows if r["controller"] != "predictive")
    assert manifest["models"]["frozen_hfbtp_model_loaded"] is False and manifest["models"]["frozen_ewma_loaded"] is False
    assert manifest["predictive"]["workload_history_basis"].startswith("captured history synthetic-test-day")
    assert "Ethereum mempool arrivals, not Fabric observations" in manifest["captured_history"]["limitations"]


def test_41b_schema_2_is_a_superset_of_schema_1_and_schema_1_runs_are_unchanged(tmp_path):
    _, v2_rows, _ = h.run_captured(tmp_path, h.history())
    _, v1_rows, _ = h.f.run(tmp_path, name="legacy")
    assert {r["schema"] for r in v1_rows} == {"stage3_shadow_decision/1"} and all("predictive_evidence" not in r for r in v1_rows)
    assert set(v1_rows[0]) <= set(v2_rows[0]) and set(v2_rows[0]) - set(v1_rows[0]) == {"predictive_evidence"}


def test_42_no_configuration_is_ever_applied_and_the_actuators_are_shadow_only(tmp_path):
    manifest, rows, out = h.run_captured(tmp_path, h.history())
    assert manifest["actuation"] == {"configuration_changes_applied": 0, "actuator": "ShadowActuator (records proposals only)",
                                     "live_actuator_available": False}
    assert all(r["proposal_applied"] is False and r["actuator"]["configuration_changes_applied"] == 0 and r["actuator"]["submitted_to_fabric"] is False
               and r["actuator"]["live_actuator_available"] is False for r in rows)
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    assert summary["configuration_changes_applied"] == 0 and summary["shadow_actuator_proposals_accepted"] == 0
    assert summary["shadow_actuator_proposals_recorded"] > 0              # proposals exist as records, and only as records


# ------------------------------------------------------------------------------------ 43 real inference, 44 protected channels
def test_43_real_inference_stays_disabled_with_a_captured_history(tmp_path, monkeypatch):
    assert sp.REAL_INFERENCE_AUTHORIZED is False
    inputs_dir = tmp_path / "in"
    inputs_dir.mkdir()
    inputs = runner.load_inputs(h.f.write(inputs_dir / "p.json", h.f.params()), h.f.write(inputs_dir / "c.json", h.policy_h60()),
                                h.f.write(inputs_dir / "x.json", h.captured_fixture()))
    out = tmp_path / "out_real"
    with pytest.raises(sp.RealInferenceNotAuthorized):
        runner.run_fixture_shadow(inputs, out, predictive_mode="real", captured_history=h.history())
    assert not out.exists()                                               # refused before anything was created
    opened = []
    real_open = open
    monkeypatch.setattr("builtins.open", lambda *a, **k: (opened.append(str(a[0])), real_open(*a, **k))[1])
    code = cli.main(["fixture", "--params", str(tmp_path / "missing_p.json"), "--controller-policy", "missing_c.json", "--fixture", "missing_x.json",
                     "--out-dir", str(tmp_path / "out_cli"), "--predictive-mode", "real", "--captured-history", str(tmp_path / "missing.csv")])
    assert code == cli.EXIT_REAL_INFERENCE_REFUSED and opened == [] and not (tmp_path / "out_cli").exists()


def test_43b_the_runner_refuses_a_captured_history_it_cannot_serve(tmp_path):
    with pytest.raises(runner.Stage3RunnerError, match="stage3_controller_policy/2"):          # /1 policy: no place for the evidence
        h.run_captured(tmp_path, h.history(), policy_doc=h.f.policy(), name="p1")
    wrong_horizon = h.g.policy_v2()                                       # the A0g example policy has horizon 10
    with pytest.raises(runner.Stage3RunnerError, match="60 s forecast only"):
        h.run_captured(tmp_path, h.history(), policy_doc=wrong_horizon, name="h10")
    with pytest.raises(runner.Stage3RunnerError, match="inside the captured UTC day"):
        h.run_captured(tmp_path, h.history(), fixture_doc=h.captured_fixture(start_s=86_395, steps=h.f.steady(10)), name="late")
    with pytest.raises(runner.Stage3RunnerError, match="inside the captured UTC day"):
        h.run_captured(tmp_path, h.history(), fixture_doc=h.f.fixture(), name="otherday")      # the A0f fixture starts on 2026-11-21
    assert not (tmp_path / "out_p1").exists() and not (tmp_path / "out_h10").exists() and not (tmp_path / "out_late").exists()


@pytest.mark.parametrize("channel", ["e1-r002", "e1-cfg001", "E1-B001", "e1-r001", " e1-r002 "])
def test_44_a_protected_channel_is_refused_and_there_is_no_fallback(tmp_path, channel):
    params = h.f.params()
    params["channel"] = channel
    inputs_dir = tmp_path / "in"
    inputs_dir.mkdir()
    with pytest.raises(runner.Stage3RunnerError, match="protected reference channel|earlier experiment"):    # refused by the parameter contract, before any run
        runner.load_inputs(h.f.write(inputs_dir / "p.json", params), h.f.write(inputs_dir / "c.json", h.policy_h60()),
                           h.f.write(inputs_dir / "x.json", h.captured_fixture()))
    assert not (tmp_path / "out").exists()


# -------------------------------------------------------------------------------------------------- CLI route (fixture mode)
def test_cli_runs_a_captured_history_fixture_from_a_stored_series_file(tmp_path, capsys):
    counts = {i: 12 + i % 5 for i in range(1500)}
    series = tmp_path / "series.csv"
    series.write_text("timestamp_utc,arrival_count,date_utc,frequency_seconds\n" + "".join(
        f"{h.at(i)},{v},2026-09-21,1\n" for i, v in counts.items()), encoding="utf-8")
    base = [h.f.write(tmp_path / "p.json", h.f.params()), h.f.write(tmp_path / "c.json", h.policy_h60()),
            h.f.write(tmp_path / "x.json", h.captured_fixture(start_s=1000))]
    argv = ["fixture", "--params", str(base[0]), "--controller-policy", str(base[1]), "--fixture", str(base[2]), "--out-dir", str(tmp_path / "out"),
            "--captured-history", str(series), "--captured-history-sha256", ch.sha256_stream(series), "--captured-history-utc-day", "2026-09-21",
            "--require-operator-freshness"]
    assert cli.main(argv) == cli.EXIT_OK
    manifest = json.loads((tmp_path / "out" / "run_manifest.json").read_text(encoding="utf-8"))
    assert manifest["captured_history"]["source"]["dataset_sha256"] == ch.sha256_stream(series)
    assert {f["validation_outcome"] for f in manifest["captured_history"]["frames"]} == {ch.VALID_COMPLETE}
    assert manifest["captured_history"]["frames"][0]["valid_bin_count"] == 1001
    capsys.readouterr()
    bad = argv[:-1] + ["--out-dir", str(tmp_path / "out2")]
    bad[bad.index("--captured-history-sha256") + 1] = "0" * 64
    assert cli.main(bad) == cli.EXIT_INVALID and not (tmp_path / "out2").exists() and "dataset_hash_mismatch" in capsys.readouterr().out
    partial = ["fixture", "--params", str(base[0]), "--controller-policy", str(base[1]), "--fixture", str(base[2]), "--out-dir", str(tmp_path / "out3"),
               "--captured-history", str(series)]
    assert cli.main(partial) == cli.EXIT_INVALID and not (tmp_path / "out3").exists()


def test_44b_the_channel_selection_module_itself_refuses_the_protected_channels():
    for channel in ("e1-r002", "e1-cfg001", "E1-B001", "e1-r001"):
        with pytest.raises(cs.ChannelSelectionError):
            cs.select_experiment_channel(channel)
