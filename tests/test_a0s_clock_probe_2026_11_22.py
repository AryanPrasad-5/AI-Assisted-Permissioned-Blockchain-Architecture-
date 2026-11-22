"""A0s workstream D: the Windows <-> WSL clock probe collector and its compatibility with the A0e evaluator. Scripted clocks and a mock runner only."""

from __future__ import annotations

import copy
import json

import pytest

from tests import a0s_support as s
from tests.a0s_support import FakeClock, MockRunner, MS, T0

from fabric_perf.controllers import clock_preflight as cp      # noqa: E402
from fabric_perf.controllers import wsl_clock_probe as wp      # noqa: E402

ARGV = ["wsl.exe", "-d", "Ubuntu", "--exec", "date", "+%s%N"]
KW = dict(remote_resolution_ns=1000, timeout_s=5, reference_domain="windows-host:time_ns", remote_domain="wsl:Ubuntu:date_ns")
EVAL = dict(tolerance_ms=50, min_valid_samples=3, max_wall_monotonic_disagreement_ms=5)


def phase(clock: FakeClock, name: str = "pre_run", count: int = 5, runner: MockRunner | None = None, **kw) -> dict:
    runner = runner or MockRunner(clock)
    probe = wp.WslClockProbe(runner, wall_ns=clock.wall, mono_ns=clock.mono, argv=ARGV, phase=name, **{**KW, **kw})
    return wp.collect_phase(probe, count)


def test_01_valid_samples_have_exactly_the_a0e_shape_and_the_a0e_parser_accepts_them():
    record = phase(FakeClock())
    assert record["count_collected"] == 5 and record["invalid"] == []
    for sample in record["samples"]:
        parsed = cp.parse_sample(sample)                       # the A0e parser, unchanged
        assert parsed.remote_resolution_ns == 1000 and parsed.ref_wall_after_ns >= parsed.ref_wall_before_ns
        assert sample["unit"] == "ns" and sample["reference_domain"] == "windows-host:time_ns" and sample["remote_domain"] == "wsl:Ubuntu:date_ns"
        assert sample["round_trip_ns"] == sample["ref_wall_after_ns"] - sample["ref_wall_before_ns"]
    assert [x["seq"] for x in record["samples"]] == [1, 2, 3, 4, 5]
    assert wp.check_phase(record) == []


def test_02_valid_offset_interval_via_the_a0e_evaluator_agree_and_the_interval_contains_the_true_offset():
    clock = FakeClock(offset_ns=3 * MS)
    pre, post = phase(clock, "pre_run"), phase(clock, "post_run")
    verdict = wp.evaluate([pre, post], **EVAL)
    assert verdict["verdict"] == cp.AGREE
    for name in ("pre_run", "post_run"):
        interval = verdict["phases"][name]["interval_ms"]
        assert interval["low"] <= 3.0 <= interval["high"] and verdict["phases"][name]["valid_samples"] == 5


def test_03_a_real_offset_beyond_tolerance_is_disagree():
    clock = FakeClock(offset_ns=500 * MS)
    verdict = wp.evaluate([phase(clock)], **EVAL)
    assert verdict["verdict"] == cp.DISAGREE and verdict["phases"]["pre_run"]["reason"] == "offset_exceeds_tolerance_even_in_the_best_case"


def test_04_excessive_round_trip_uncertainty_is_inconclusive_not_agreement():
    clock = FakeClock(offset_ns=0, rtt_ns=400 * MS)            # an uncertainty of about 200 ms against a 50 ms tolerance
    record = phase(clock)
    assert all(x["round_trip_ns"] > 300 * MS for x in record["samples"])
    verdict = wp.evaluate([record], tolerance_ms=50, min_valid_samples=3, max_wall_monotonic_disagreement_ms=500)
    assert verdict["verdict"] == cp.INCONCLUSIVE and verdict["phases"]["pre_run"]["reason"] == "uncertainty_straddles_tolerance"


@pytest.mark.parametrize("text, code", [
    (b"", "remote_reading_missing"), (b"\n\n", "remote_reading_missing"), (b"%N\n", "remote_reading_malformed"), (b"1795000000\n", "remote_unit_unexpected"),
    (b"1795000000.123456789\n", "remote_reading_malformed"), (b"1795000000123456789 ns\n", "remote_reading_malformed"), (b"abc\n", "remote_reading_malformed"),
    (b"1795000000123456789\n1\n2\n", "remote_reading_malformed"), (b"\xff\xfe", "remote_reading_malformed"), (b"-1795000000123456789\n", "remote_reading_malformed"),
    (b"17950000001234567890\n", "remote_unit_unexpected")])
def test_05_malformed_or_wrong_unit_remote_readings_are_rejected_never_rescaled(text, code):
    with pytest.raises(wp.SampleInvalid) as e:
        wp.parse_remote_reading(text)
    assert e.value.code == code


def test_06_two_line_output_records_a_remote_monotonic_value_labelled_non_comparable_and_it_is_never_used():
    assert wp.parse_remote_reading(b"1795000000123456789\n123456789012\n") == (1795000000123456789, 123456789012)
    clock = FakeClock()
    runner = MockRunner(clock, rules=lambda argv: s.ok(clock.remote().strip() + b"\n42\n") if argv[:1] == ["wsl.exe"] else None)
    record = phase(clock, runner=runner)
    assert all(x["remote_mono_ns"] == 42 and x["remote_mono_comparable_across_hosts"] is False for x in record["samples"])
    assert wp.evaluate([record], **EVAL)["verdict"] == cp.AGREE
    bad = copy.deepcopy(record)
    del bad["samples"][0]["remote_mono_comparable_across_hosts"]
    assert any(p["code"] == "monotonic_cross_host_use" for p in wp.check_phase(bad))


def test_07_invalid_sample_ordering_is_rejected_at_collection_and_in_a_stored_record():
    clock = FakeClock()
    state = {"n": 0}

    def wall():
        state["n"] += 1
        return clock.wall() if state["n"] % 2 == 1 else clock.wall() - 50 * MS

    runner = MockRunner(clock)
    probe = wp.WslClockProbe(runner, wall_ns=wall, mono_ns=clock.mono, argv=ARGV, phase="pre_run", **KW)
    record = wp.collect_phase(probe, 3)
    assert record["count_collected"] == 0 and {x["code"] for x in record["invalid"]} == {"wall_clock_went_backwards"}
    good = phase(FakeClock())
    stored = copy.deepcopy(good)
    stored["samples"][1]["ref_wall_after_ns"] = stored["samples"][1]["ref_wall_before_ns"] - 1
    stored["samples"][2]["ref_mono_after_ns"] = stored["samples"][2]["ref_mono_before_ns"]
    codes = [p["code"] for p in wp.check_phase(stored)]
    assert codes.count("invalid_sample_ordering") == 2


def test_08_monotonic_clock_that_does_not_advance_is_invalid():
    clock = FakeClock()
    probe = wp.WslClockProbe(MockRunner(clock), wall_ns=clock.wall, mono_ns=lambda: 5, argv=ARGV, phase="pre_run", **KW)
    record = wp.collect_phase(probe, 2)
    assert record["count_collected"] == 0 and {x["code"] for x in record["invalid"]} == {"monotonic_not_advancing"}


def test_09_failed_remote_command_and_missing_sample_fields_are_reported():
    clock = FakeClock()
    runner = MockRunner(clock, rules=lambda argv: s.be.RunResult(1, b"", b"wsl: error") if argv[:1] == ["wsl.exe"] else None)
    record = phase(clock, runner=runner, count=2)
    assert record["count_collected"] == 0 and [x["code"] for x in record["invalid"]] == ["remote_command_failed"] * 2
    assert wp.evaluate([record], **EVAL)["verdict"] == cp.INCONCLUSIVE                 # zero usable samples never agree
    stored = phase(FakeClock())
    del stored["samples"][0]["remote_wall_ns"]
    assert any(p["code"] == "missing_sample" for p in wp.check_phase(stored))


def test_10_clock_domain_mismatch_inside_a_phase_and_between_phases_is_refused():
    pre = phase(FakeClock())
    bad = copy.deepcopy(pre)
    bad["samples"][2]["remote_domain"] = "wsl:Other:date_ns"
    assert any(p["code"] == "clock_domain_mismatch" for p in wp.check_phase(bad))
    with pytest.raises(wp.ProbeRefused):
        wp.to_evaluator_phases([bad])
    post = phase(FakeClock(), "post_run", remote_domain="wsl:Other:date_ns")
    with pytest.raises(wp.ProbeRefused) as e:
        wp.to_evaluator_phases([pre, post])
    assert "differ between phases" in str(e.value)
    with pytest.raises(wp.ProbeRefused):
        wp.to_evaluator_phases([pre, copy.deepcopy(pre)])                         # a phase name twice


def test_11_sampling_identity_count_and_sequence_are_checked():
    record = phase(FakeClock())
    other = copy.deepcopy(record)
    other["samples"][1]["sampling_id"] = "someone-else"
    assert any(p["code"] == "sampling_identity_mismatch" for p in wp.check_phase(other))
    other = copy.deepcopy(record)
    other["samples"][3]["seq"] = 1
    assert any(p["code"] == "sequence_not_increasing" for p in wp.check_phase(other))
    other = copy.deepcopy(record)
    other["count_collected"] = 4
    assert any(p["code"] == "count_mismatch" for p in wp.check_phase(other))
    other = copy.deepcopy(record)
    other["samples"][1]["ref_mono_before_ns"] = other["samples"][0]["ref_mono_after_ns"] - 1
    assert any(p["code"] == "overlapping_samples" for p in wp.check_phase(other))
    other = copy.deepcopy(record)
    other["samples"][0]["unit"] = "ms"
    assert any(p["code"] == "unit_mismatch" for p in wp.check_phase(other))
    assert record["sampling_id"] and len({x["sampling_id"] for x in record["samples"]}) == 1


def test_12_configuration_is_explicit_and_two_clocks_must_be_two_names():
    clock = FakeClock()
    runner = MockRunner(clock)
    for bad in (dict(remote_resolution_ns=0), dict(remote_resolution_ns=True), dict(timeout_s=0), dict(reference_domain=""), dict(remote_domain="has space"),
                dict(remote_domain=KW["reference_domain"])):
        with pytest.raises(wp.ProbeRefused):
            wp.WslClockProbe(runner, wall_ns=clock.wall, mono_ns=clock.mono, argv=ARGV, phase="pre_run", **{**KW, **bad})
    for bad in (None, "", "bad distro", "x" * 70):
        with pytest.raises(wp.ProbeRefused):
            wp.probe_plan(bad, remote_resolution_ns=1000, timeout_s=5)
    with pytest.raises(wp.ProbeRefused):
        wp.probe_plan("Ubuntu", remote_resolution_ns=None, timeout_s=5)
    with pytest.raises(wp.ProbeRefused):
        wp.collect_phase(wp.WslClockProbe(runner, wall_ns=clock.wall, mono_ns=clock.mono, argv=ARGV, phase="p", **KW), 0)


@pytest.mark.parametrize("kind", ["process", None, "live"])
def test_13_real_sampling_is_disabled_and_a_process_runner_is_refused(kind):
    clock = FakeClock()
    runner = MockRunner(clock)
    runner.kind = kind
    with pytest.raises(wp.ProbeRefused):
        wp.WslClockProbe(runner, wall_ns=clock.wall, mono_ns=clock.mono, argv=ARGV, phase="pre_run", **KW)
    assert wp.LIVE_CLOCK_SAMPLING_ENABLED is False
    plan = wp.probe_plan("Ubuntu", remote_resolution_ns=1000, timeout_s=5)
    assert plan["argv"] == ARGV and plan["verified_on_target"] is False and plan["live_sampling_enabled"] is False


def test_14_the_probe_object_satisfies_the_a0e_probe_interface_and_collect_samples_helper():
    clock = FakeClock()
    probe = wp.WslClockProbe(MockRunner(clock), wall_ns=clock.wall, mono_ns=clock.mono, argv=ARGV, phase="pre_run", **KW)
    samples = cp.collect_samples(probe, 4)                                    # the A0e helper takes any object with sample()
    assert len(samples) == 4 and cp.assess_clock_agreement(samples, phase="pre_run", **EVAL)["verdict"] == cp.AGREE


def test_15_a_utc_label_alone_is_never_evidence_the_record_says_so_and_the_verdict_comes_from_the_interval():
    record = phase(FakeClock(offset_ns=800 * MS))
    assert record["utc_label_is_not_evidence"] is True
    assert wp.evaluate([record], **EVAL)["verdict"] == cp.DISAGREE


def test_16_cli_plan_parse_check_evaluate_and_sample_refusal(tmp_path, capsys):
    import e1_a0s_clock_probe as cli

    clock = FakeClock()
    pre, post = tmp_path / "pre.json", tmp_path / "post.json"
    pre.write_text(json.dumps(phase(clock, "pre_run")), encoding="utf-8")
    post.write_text(json.dumps(phase(clock, "post_run")), encoding="utf-8")
    reading = tmp_path / "r.txt"
    reading.write_bytes(b"1795000000123456789\n")
    assert cli.main(["plan", "--distro", "Ubuntu", "--remote-resolution-ns", "1000", "--timeout-s", "5"]) == 0
    assert cli.main(["parse", "--reading-file", str(reading)]) == 0
    reading.write_bytes(b"%N\n")
    assert cli.main(["parse", "--reading-file", str(reading)]) == 2
    assert cli.main(["check", "--phases", str(pre), str(post)]) == 0
    assert cli.main(["evaluate", "--phases", str(pre), str(post), "--tolerance-ms", "50", "--min-valid-samples", "3", "--max-wall-monotonic-disagreement-ms", "5"]) == 0
    assert cli.main(["evaluate", "--phases", str(pre), str(post), "--tolerance-ms", "0.0001", "--min-valid-samples", "3", "--max-wall-monotonic-disagreement-ms", "5"]) == 3
    assert cli.main(["sample", "--anything"]) == 5
    assert "REFUSED" in capsys.readouterr().out
    assert T0 > 10 ** 18
