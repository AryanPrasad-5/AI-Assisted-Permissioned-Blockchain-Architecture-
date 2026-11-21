"""A0f: Stage 3 initialization, abstention and termination behaviour of the operational runner (2026-11-21).

Covers checklist items 29-35. The limits are operator-supplied through the A0e parameter contract; nothing is defaulted.
"""

from __future__ import annotations

import json
import socket
import subprocess
import urllib.request

import pytest

from tests import a0f_support as s

import e1_stage3_shadow_runner as cli  # noqa: E402  (importable once tests.a0f_support has put scripts/e1 on sys.path)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("a process or network call was attempted")
    for obj, names in ((subprocess, ("run", "Popen", "call", "check_output")), (socket, ("create_connection", "getaddrinfo")),
                       (urllib.request, ("urlopen",))):
        for name in names:
            monkeypatch.setattr(obj, name, boom)


def c(n=5, latency=100):
    return {"count": n, "latency_ms": latency}


def good():
    return s.step(client=c(), ledger_valid_tx=5)


def bad():
    """An unusable frame (the configuration read fails) that is not a warm-up frame."""
    return s.step(client=c(), ledger_valid_tx=5, config_reader_ok=False)


def stage3(abstentions=3, init=2):
    doc = s.params()
    doc["stage3"] = {"max_consecutive_abstentions": abstentions, "max_initialization_frames": init}
    return doc


def go(tmp_path, steps, *, abstentions=3, init=2, name="run"):
    manifest, rows, out = s.run(tmp_path, fixture_doc=s.fixture(steps=steps), params_doc=stage3(abstentions, init), name=name)
    summary = json.loads((out / "run_summary.json").read_text(encoding="utf-8"))
    evidence = json.loads((out / "native" / "validation_evidence.json").read_text(encoding="utf-8"))
    return s.by_frame(rows), summary, evidence, manifest, out


# ---------------------------------------------------------------------------------------------- 29 first-frame initialization
def test_the_first_frame_is_an_initialization_frame_and_spends_no_abstention_budget(tmp_path):
    frames, summary, evidence, _, _ = go(tmp_path, [s.step(client=c()), good()])
    for row in frames[0].values():
        assert row["frame"]["status"] == "initialization" and row["frame"]["reason_code"] == "stage3_initialization_ledger_baseline"
        assert row["outcome"] == "abstain" and row["reason"] == "telemetry_initializing_fail_safe"
        assert row["observed"]["ledger_committed_throughput_tps"] is None                # no throughput was invented
        assert row["recommended_config"] is None and row["config_change_proposed"] is False
    assert summary["native"]["initialization_frame_count"] == 1
    assert summary["native"]["longest_consecutive_abstention_run"] == 0
    assert evidence["validation_records"][0]["handled"] == "initializing"


def test_with_initialization_disabled_by_the_operator_the_first_frame_is_an_ordinary_rejection(tmp_path):
    frames, summary, _, _, _ = go(tmp_path, [s.step(client=c()), good()], init=None)
    assert {r["frame"]["status"] for r in frames[0].values()} == {"rejected"}
    assert summary["native"]["initialization_frame_count"] == 0 and summary["native"]["longest_consecutive_abstention_run"] == 1


# ---------------------------------------------------------------------------------------------- 30 valid transition
def test_the_first_measured_frame_is_accepted_and_all_three_controllers_decide(tmp_path):
    frames, summary, _, _, _ = go(tmp_path, [s.step(client=c()), good(), good()])
    assert {r["frame"]["status"] for r in frames[1].values()} == {"accepted"}
    assert {r["outcome"] for r in frames[1].values()} <= {"hold", "propose"}
    assert all(r["observed"]["ledger_committed_throughput_tps"] == 5.0 for r in frames[1].values())
    assert summary["frame_status_counts"] == {"accepted": 2, "initialization": 1, "rejected": 0}


def test_initialization_is_legal_only_before_the_first_accepted_frame(tmp_path):
    # after an accepted frame, a frame that again lacks a rate is an ordinary rejection, never a second warm-up
    steps = [s.step(client=c()), good(), s.step(client=c(), poll=False, advance_s=6.0), good()]
    frames, _, _, _, _ = go(tmp_path, steps)
    assert frames[2]["static"]["frame"]["status"] == "rejected"
    assert frames[2]["static"]["frame"]["reason_code"] != "stage3_initialization_ledger_baseline"


# ---------------------------------------------------------------------------------------------- 31 bounded initialization
def test_initialization_is_bounded_and_one_frame_beyond_the_bound_stops_the_run(tmp_path):
    steps = [s.step(client=c())] + [s.step(client=c(), poll=False) for _ in range(4)]
    frames, summary, evidence, manifest, _ = go(tmp_path, steps, init=2)
    assert [frames[i]["static"]["frame"]["status"] for i in range(3)] == ["initialization", "initialization", "rejected"]
    assert frames[2]["static"]["frame"]["reason_code"] == "stage3_initialization_limit_exceeded"
    assert summary["native"]["initialization_limit_exceeded"] is True and summary["native"]["initialization_frame_count"] == 2
    assert summary["stop_reasons"] == ["initialization_limit_exceeded"] and manifest["status"] == "STOPPED_BY_SAFETY_LIMIT"
    assert summary["frames_processed"] == 3 < summary["frames_in_fixture"]                # the remaining frames were not consumed
    stop = [v for v in evidence["validation_records"] if v.get("status") == "stopped"][0]
    assert stop["max_initialization_frames"] == 2


def test_no_controller_decides_on_a_warm_up_frame_and_none_recommends_anything(tmp_path):
    steps = [s.step(client=c())] + [s.step(client=c(), poll=False) for _ in range(3)]
    frames, _, _, _, _ = go(tmp_path, steps, init=3)
    for index in (0, 1, 2):
        assert {r["outcome"] for r in frames[index].values()} == {"abstain"}
        assert all(r["recommended_config"] is None for r in frames[index].values())


# ---------------------------------------------------------------------------------------------- 32-33 consecutive invalid frames
def test_consecutive_invalid_frames_exhaust_the_operator_limit_and_stop_the_run(tmp_path):
    steps = [s.step(client=c()), good(), bad(), bad(), bad(), good(), good()]
    frames, summary, evidence, manifest, _ = go(tmp_path, steps, abstentions=3)
    assert summary["native"]["consecutive_abstention_limit_reached"] is True and summary["native"]["longest_consecutive_abstention_run"] == 3
    assert summary["stop_reasons"] == ["consecutive_abstention_limit_reached"]
    assert summary["frames_processed"] == 5 and set(frames) == {0, 1, 2, 3, 4}          # frames 5 and 6 were never consumed
    assert all(len(frames[i]) == 3 for i in frames)                                      # the stopping frame still reached all three
    stop = [v for v in evidence["validation_records"] if v.get("status") == "stopped"][0]
    assert stop["consecutive_abstained_frames"] == 3 and stop["max_consecutive_abstentions"] == 3
    assert manifest["status"] == "STOPPED_BY_SAFETY_LIMIT"


def test_the_limit_is_the_operator_supplied_one(tmp_path):
    steps = [s.step(client=c()), good(), bad(), bad(), bad(), good()]
    stopped = go(tmp_path, steps, abstentions=2, name="two")
    assert stopped[1]["stop_reasons"] == ["consecutive_abstention_limit_reached"] and stopped[1]["frames_processed"] == 4
    complete = go(tmp_path, steps, abstentions=4, name="four")
    assert complete[1]["stop_reasons"] == [] and complete[1]["frames_processed"] == 6 and complete[3]["status"] == "COMPLETED"


def test_a_limit_of_one_stops_on_the_first_invalid_frame_after_warm_up(tmp_path):
    frames, summary, _, _, _ = go(tmp_path, [s.step(client=c()), good(), bad(), good()], abstentions=1)
    assert summary["frames_processed"] == 3 and summary["stop_reasons"] == ["consecutive_abstention_limit_reached"]


def test_threshold_exhaustion_is_a_designed_stop_with_its_own_cli_exit_code(tmp_path, capsys):
    f = s.write(tmp_path / "f.json", s.fixture(steps=[s.step(client=c()), good(), bad(), bad(), bad()]))
    p = s.write(tmp_path / "p.json", stage3(3, 2))
    code = cli.main(["fixture", "--params", str(p), "--controller-policy", str(s.POLICY_FILE), "--fixture", str(f), "--out-dir", str(tmp_path / "o")])
    out = json.loads(capsys.readouterr().out)
    assert code == cli.EXIT_SAFETY_STOP == 4 and out["status"] == "STOPPED_BY_SAFETY_LIMIT"
    assert out["stop_reasons"] == ["consecutive_abstention_limit_reached"] and out["live_ready"] is False


# ---------------------------------------------------------------------------------------------- 34 recovery
def test_a_valid_frame_resets_the_consecutive_count_so_separated_failures_never_stop_the_run(tmp_path):
    steps = [s.step(client=c()), good(), bad(), bad(), good(), bad(), bad(), good()]
    frames, summary, _, manifest, _ = go(tmp_path, steps, abstentions=3)
    assert manifest["status"] == "COMPLETED" and summary["stop_reasons"] == []
    assert summary["native"]["longest_consecutive_abstention_run"] == 2 and summary["frames_processed"] == 8
    assert [frames[i]["static"]["frame"]["status"] for i in range(8)] == [
        "initialization", "accepted", "rejected", "rejected", "accepted", "rejected", "rejected", "accepted"]
    assert frames[4]["predictive"]["outcome"] in {"hold", "propose"} and frames[7]["reactive"]["outcome"] == "hold"


def test_after_recovery_the_reactive_confirmation_streak_is_not_carried_over_from_before_the_failure(tmp_path):
    def high():
        return s.step(client=c(latency=9000), ledger_valid_tx=4)
    # control: two consecutive over-threshold frames confirm congestion and propose
    control, _, _, _, _ = go(tmp_path, [s.step(client=c()), good(), high(), high()], abstentions=3, name="control")
    assert control[3]["reactive"]["outcome"] == "propose"
    # the same two over-threshold frames separated by an abstained frame must NOT confirm: the streak was broken
    split, _, _, _, _ = go(tmp_path, [s.step(client=c()), good(), high(), bad(), high()], abstentions=3, name="split")
    assert split[2]["reactive"]["outcome"] == "hold" and split[3]["reactive"]["outcome"] == "abstain"
    assert split[4]["reactive"]["outcome"] == "hold" and split[4]["reactive"]["observed"]["client_latency_mean_ms"] >= 500.0


# ---------------------------------------------------------------------------------------------- 35 reason propagation
def test_the_reason_recorded_for_each_controller_is_the_runner_reason_for_that_frame(tmp_path):
    steps = [s.step(client=c()), good(), bad(), s.step(client=c(), poll=False, advance_s=6.0), good()]
    frames, summary, evidence, _, _ = go(tmp_path, steps, abstentions=4)
    rejected = {v["decision_index"]: v for v in evidence["validation_records"] if v.get("status") == "rejected"}
    assert set(rejected) == {0, 2, 3}
    for index, native in rejected.items():
        for row in frames[index].values():
            assert row["frame"]["reason_code"] == native["reason_code"]
            assert row["frame"]["abstention_reason"] == native["abstention_reason"] == row["reason"]
            assert row["frame"]["reason_details"] == native["details"]
    assert frames[2]["static"]["frame"]["reason_code"] == "critical_telemetry_missing"
    assert frames[3]["static"]["frame"]["reason_code"] != frames[2]["static"]["frame"]["reason_code"]


def test_the_stop_reason_reaches_the_summary_the_manifest_status_and_the_validation_evidence(tmp_path):
    frames, summary, evidence, manifest, out = go(tmp_path, [s.step(client=c()), good(), bad(), bad(), bad()], abstentions=3)
    assert evidence["stop_reasons"] == summary["stop_reasons"] == ["consecutive_abstention_limit_reached"]
    assert manifest["status"] == summary["status"] == "STOPPED_BY_SAFETY_LIMIT"
    assert summary["live_ready"] is False and summary["configuration_changes_applied"] == 0
