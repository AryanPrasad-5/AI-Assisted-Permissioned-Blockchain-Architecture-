"""Task A0e (2026-11-21), Workstream C: offline clock-agreement evidence. Every sample here is SYNTHETIC; nothing was measured."""

import ast
import json
import os
import socket
import subprocess
import urllib.request

import pytest

from fabric_perf.controllers import clock_preflight as cp
from tests.a0e_support import CLOCK_PARAMS, MS, REPO, clock_sample

BASE = 1_800_000_000_000_000_000


def assess(samples, **override):
    return cp.assess_clock_agreement(samples, **{**CLOCK_PARAMS, **override})


def good(n=3, **kw):
    return [clock_sample(base_ns=BASE + i * 10_000_000_000, mono_base_ns=5_000_000_000 + i * 10_000_000_000, **kw) for i in range(n)]


# ---------------------------------------------------------------- verdicts
def test_synthetic_agreement_is_reported_as_agree_with_the_interval():
    result = assess(good(offset_ns=0))
    assert result["verdict"] == cp.AGREE and result["reason"] == "offset_interval_within_tolerance"
    assert result["valid_samples"] == 3 and result["rejected"] == []
    assert abs(result["interval_ms"]["low"]) <= 10 and abs(result["interval_ms"]["high"]) <= 10
    assert result["evidence_origin"].startswith("operator-supplied") and result["utc_label_is_not_evidence"] is True


def test_an_excessive_offset_is_reported_as_disagree():
    result = assess(good(offset_ns=5_000 * MS))                  # remote is 5 s ahead, tolerance 10 ms
    assert result["verdict"] == cp.DISAGREE and result["reason"] == "offset_exceeds_tolerance_even_in_the_best_case"
    assert result["interval_ms"]["low"] > 10
    behind = assess(good(offset_ns=-5_000 * MS))
    assert behind["verdict"] == cp.DISAGREE and behind["interval_ms"]["high"] < -10


def test_an_offset_larger_than_the_tolerance_but_inside_the_uncertainty_is_inconclusive_not_disagree():
    # offset 12 ms, round trip 100 ms => +-50 ms uncertainty: the true offset could be anything from -38 to +62 ms
    result = assess(good(offset_ns=12 * MS, rtt_ns=100 * MS))
    assert result["verdict"] == cp.INCONCLUSIVE and result["reason"] == "uncertainty_straddles_tolerance"


def test_a_slow_round_trip_cannot_establish_agreement_at_a_tight_tolerance():
    slow = assess(good(offset_ns=0, rtt_ns=40 * MS), tolerance_ms=10.0)      # +-20 ms uncertainty > 10 ms tolerance
    assert slow["verdict"] == cp.INCONCLUSIVE and slow["best_sample_uncertainty_ms"] == pytest.approx(20.0, abs=0.01)
    assert assess(good(offset_ns=0, rtt_ns=40 * MS), tolerance_ms=25.0)["verdict"] == cp.AGREE   # same data, looser operator tolerance


def test_boundaries_of_the_tolerance_are_inclusive_for_agree():
    # zero true offset, 2 ms round trip, 1 us resolution: the estimate is +0.5 us (half the resolution) and the uncertainty
    # 1.0005 ms, so the interval is [-1.000, +1.001] ms and its larger end is exactly 1.001 ms
    exact = assess(good(offset_ns=0), tolerance_ms=1.001)
    assert exact["verdict"] == cp.AGREE and exact["interval_ms"]["high"] == pytest.approx(1.001) and exact["interval_ms"]["low"] == pytest.approx(-1.0)
    tighter = assess(good(offset_ns=0), tolerance_ms=1.0009)
    assert tighter["verdict"] == cp.INCONCLUSIVE


def test_the_offset_arithmetic_is_exact_for_a_known_sample():
    sample = cp.parse_sample({"ref_wall_before_ns": 1_000_000_000, "ref_wall_after_ns": 1_010_000_000, "remote_wall_ns": 1_105_000_000,
                              "ref_mono_before_ns": 0, "ref_mono_after_ns": 10_000_000, "remote_resolution_ns": 1_000_000})
    # midpoint 1.005 s; remote 1.105 s + half a 1 ms resolution = 1.1055 s => offset 100.5 ms; uncertainty 5 ms + 0.5 ms
    assert sample.offset_x2 / 2 / MS == pytest.approx(100.5) and sample.uncertainty_x2 / 2 / MS == pytest.approx(5.5)


def test_the_clock_resolution_widens_the_uncertainty():
    fine = assess(good(rtt_ns=2 * MS, resolution_ns=1_000))["widest_sample_uncertainty_ms"]
    coarse = assess(good(rtt_ns=2 * MS, resolution_ns=1_000_000))["widest_sample_uncertainty_ms"]
    assert coarse - fine == pytest.approx(0.4995, abs=1e-6)


# ---------------------------------------------------------------- missing, too few, contradictory
def test_no_samples_cannot_establish_agreement():
    for empty in ([], ()):
        result = assess(empty)
        assert result["verdict"] == cp.INCONCLUSIVE and result["reason"] == "no_samples" and result["valid_samples"] == 0
    assert assess(None)["verdict"] == cp.INCONCLUSIVE and assess("abc")["reason"] == "samples_not_a_list"


def test_fewer_valid_samples_than_the_operator_requires_is_inconclusive():
    result = assess(good(2))                                      # min_valid_samples is 3
    assert result["verdict"] == cp.INCONCLUSIVE and result["reason"] == "insufficient_valid_samples"
    assert assess(good(2), min_valid_samples=2)["verdict"] == cp.AGREE


def test_rejected_samples_do_not_count_toward_the_minimum_and_are_listed():
    samples = good(2) + [{"ref_wall_before_ns": 1}]
    result = assess(samples)
    assert result["verdict"] == cp.INCONCLUSIVE and result["valid_samples"] == 2
    assert result["rejected"][0]["index"] == 2 and result["rejected"][0]["code"] in ("missing_timestamp", "missing_field")


def test_samples_that_contradict_each_other_are_inconclusive_and_never_averaged():
    a = clock_sample(offset_ns=0, base_ns=BASE)
    b = clock_sample(offset_ns=500 * MS, base_ns=BASE + 10_000_000_000, mono_base_ns=15_000_000_000)
    c = clock_sample(offset_ns=0, base_ns=BASE + 20_000_000_000, mono_base_ns=25_000_000_000)
    result = assess([a, b, c], tolerance_ms=1000.0)
    assert result["verdict"] == cp.INCONCLUSIVE and result["reason"] == "samples_contradict_each_other" and result["interval_ms"] is None


# ---------------------------------------------------------------- malformed timestamps
BAD_FIELDS = [
    ("naive ISO", {"ref_wall_before_ns": None, "ref_wall_before_iso": "2027-01-15T08:00:00"}, "naive_timestamp"),
    ("garbage ISO", {"ref_wall_before_ns": None, "ref_wall_before_iso": "yesterday"}, "malformed_timestamp"),
    ("ISO is a number", {"ref_wall_before_ns": None, "ref_wall_before_iso": 5}, "malformed_timestamp"),
    ("ns is bool", {"ref_wall_before_ns": True}, "malformed_timestamp"),
    ("ns is float", {"ref_wall_before_ns": 1.5e18}, "malformed_timestamp"),
    ("ns is string", {"ref_wall_before_ns": "123"}, "malformed_timestamp"),
    ("ns negative", {"ref_wall_before_ns": -1}, "malformed_timestamp"),
    ("both forms", {"ref_wall_before_iso": "2027-01-15T08:00:00Z"}, "ambiguous_timestamp"),
    ("absent", {"ref_wall_before_ns": None}, "missing_timestamp"),
    ("mono missing", {"ref_mono_before_ns": None}, "missing_field"),
    ("mono bool", {"ref_mono_after_ns": True}, "malformed_field"),
    ("mono negative", {"ref_mono_after_ns": -5}, "malformed_field"),
    ("resolution zero", {"remote_resolution_ns": 0}, "malformed_field"),
    ("resolution float", {"remote_resolution_ns": 1.5}, "malformed_field"),
]


@pytest.mark.parametrize("label,changes,code", BAD_FIELDS)
def test_malformed_or_missing_timestamp_fields_are_rejected_with_a_stable_code(label, changes, code):
    sample = {**clock_sample(), **changes}
    sample = {k: v for k, v in sample.items() if v is not None}
    with pytest.raises(cp.ClockSampleError) as caught:
        cp.parse_sample(sample)
    assert caught.value.code == code, label
    assert assess(good(3) + [sample])["rejected"][-1]["code"] == code      # and the assessment reports it instead of crashing


def test_a_sample_that_is_not_an_object_is_rejected():
    for bad in (None, 5, "x", [1, 2]):
        with pytest.raises(cp.ClockSampleError) as caught:
            cp.parse_sample(bad)
        assert caught.value.code == "malformed_sample"


# ---------------------------------------------------------------- timezones and the "both are UTC" trap
def iso_sample(before, remote, after, **kw):
    return {"ref_wall_before_iso": before, "remote_wall_iso": remote, "ref_wall_after_iso": after, "ref_mono_before_ns": 0,
            "ref_mono_after_ns": 2 * MS, "remote_resolution_ns": 1_000, **kw}


def test_iso_strings_in_different_timezones_convert_to_the_same_instant():
    utc = cp.parse_sample(iso_sample("2027-01-15T08:00:00.000000Z", "2027-01-15T08:00:00.001000Z", "2027-01-15T08:00:00.002000Z"))
    ist = cp.parse_sample(iso_sample("2027-01-15T13:30:00.000000+05:30", "2027-01-15T13:30:00.001000+05:30", "2027-01-15T13:30:00.002000+05:30"))
    west = cp.parse_sample(iso_sample("2027-01-15T00:00:00.000000-08:00", "2027-01-15T00:00:00.001000-08:00", "2027-01-15T00:00:00.002000-08:00"))
    assert utc == ist == west
    assert utc.offset_x2 // 2 == 500                                       # remote midpoint + half resolution (0.5 us), in ns
    mixed = cp.parse_sample(iso_sample("2027-01-15T08:00:00Z", "2027-01-15T13:30:00.001+05:30", "2027-01-15T08:00:00.002+00:00"))
    assert mixed.remote_wall_ns - mixed.ref_wall_before_ns == 1 * MS       # a remote stamp in another zone is converted, not compared as text


def test_a_timezone_offset_error_is_a_large_disagreement_not_a_match():
    """Two clocks that both say 'UTC' but one is a time zone off (wrong label) are caught by the samples, not by the label."""
    samples = [iso_sample("2027-01-15T08:00:00.000000Z", "2027-01-15T13:30:00.001000Z", "2027-01-15T08:00:00.002000Z",
                          ref_mono_before_ns=i * 10**10, ref_mono_after_ns=i * 10**10 + 2 * MS) for i in range(3)]
    result = assess(samples)
    assert result["verdict"] == cp.DISAGREE and result["interval_ms"]["low"] == pytest.approx(5.5 * 3600 * 1000, rel=1e-3)


def test_both_hosts_claiming_utc_is_never_used_as_evidence_by_itself():
    assert assess([])["utc_label_is_not_evidence"] is True
    assert "utc" not in json.dumps(cp.assess_phases({"p": []}, **CLOCK_PARAMS)["phases"]["p"]["reason"]).lower()


def test_epoch_nanoseconds_and_iso_forms_agree():
    iso = cp.parse_sample(iso_sample("2027-01-15T08:00:00Z", "2027-01-15T08:00:00.001Z", "2027-01-15T08:00:00.002Z"))
    ns0 = int(__import__("datetime").datetime(2027, 1, 15, 8, tzinfo=__import__("datetime").timezone.utc).timestamp()) * 10**9
    as_ns = cp.parse_sample({"ref_wall_before_ns": ns0, "remote_wall_ns": ns0 + MS, "ref_wall_after_ns": ns0 + 2 * MS,
                             "ref_mono_before_ns": 0, "ref_mono_after_ns": 2 * MS, "remote_resolution_ns": 1_000})
    assert iso == as_ns


# ---------------------------------------------------------------- wall clock vs monotonic elapsed time
def test_a_wall_clock_step_during_a_sample_is_detected_with_the_monotonic_clock():
    stepped = clock_sample(rtt_ns=2 * MS, mono_rtt_ns=1_500 * MS)           # wall says 2 ms, monotonic says 1.5 s
    result = assess(good(3) + [stepped])
    assert result["rejected"][0]["code"] == "wall_clock_stepped_during_sample" and result["valid_samples"] == 3
    jumped = clock_sample(rtt_ns=900 * MS, mono_rtt_ns=2 * MS)               # wall clock jumped forward during the sample
    assert assess([jumped])["rejected"][0]["code"] == "wall_clock_stepped_during_sample"


def test_the_wall_monotonic_tolerance_is_operator_supplied_and_inclusive():
    sample = clock_sample(rtt_ns=2 * MS, mono_rtt_ns=3 * MS)                 # differ by exactly 1 ms
    assert assess([sample], min_valid_samples=1)["valid_samples"] == 1
    assert assess([sample], min_valid_samples=1, max_wall_monotonic_disagreement_ms=0.9)["valid_samples"] == 0


def test_a_wall_clock_that_went_backwards_or_a_stalled_monotonic_clock_is_rejected():
    back = clock_sample()
    back["ref_wall_after_ns"] = back["ref_wall_before_ns"] - 5
    assert assess([back])["rejected"][0]["code"] == "wall_clock_went_backwards"
    stalled = clock_sample(mono_rtt_ns=0)
    assert assess([stalled])["rejected"][0]["code"] == "monotonic_not_advancing"


def test_monotonic_values_are_used_only_for_elapsed_time_not_as_instants():
    a = good(3, offset_ns=0)
    shifted = [{**s, "ref_mono_before_ns": s["ref_mono_before_ns"] + 10**15, "ref_mono_after_ns": s["ref_mono_after_ns"] + 10**15} for s in a]
    assert assess(a)["interval_ms"] == assess(shifted)["interval_ms"]        # an arbitrary monotonic origin changes nothing


# ---------------------------------------------------------------- operator parameters have no defaults
@pytest.mark.parametrize("name", ["tolerance_ms", "max_wall_monotonic_disagreement_ms"])
@pytest.mark.parametrize("bad", [None, 0, -1, float("nan"), float("inf"), True, "10"])
def test_the_positive_numeric_parameters_refuse_unusable_values(name, bad):
    with pytest.raises(ValueError, match=name):
        assess(good(), **{name: bad})


@pytest.mark.parametrize("bad", [None, 0, -1, 1.5, True, "3"])
def test_min_valid_samples_refuses_unusable_values(bad):
    with pytest.raises(ValueError, match="min_valid_samples"):
        assess(good(), min_valid_samples=bad)


def test_the_parameters_have_no_defaults_in_the_signature_or_the_cli():
    with pytest.raises(TypeError):
        cp.assess_clock_agreement(good())                                     # keyword-only and required
    with pytest.raises(SystemExit) as caught:
        cp.main(["--samples", "x.json"])
    assert caught.value.code == 2


# ---------------------------------------------------------------- phases
def test_every_phase_must_agree_for_an_overall_agree():
    ok, bad, vague = good(offset_ns=0), good(offset_ns=5_000 * MS), good(offset_ns=0, rtt_ns=100 * MS)
    assert cp.assess_phases({"pre": ok, "post": ok}, **CLOCK_PARAMS)["verdict"] == cp.AGREE
    assert cp.assess_phases({"pre": ok, "post": bad}, **CLOCK_PARAMS)["verdict"] == cp.DISAGREE
    assert cp.assess_phases({"pre": ok, "post": vague}, **CLOCK_PARAMS)["verdict"] == cp.INCONCLUSIVE
    assert cp.assess_phases({"pre": vague, "post": bad}, **CLOCK_PARAMS)["verdict"] == cp.DISAGREE
    assert cp.assess_phases({"pre": ok, "post": []}, **CLOCK_PARAMS)["verdict"] == cp.INCONCLUSIVE
    with pytest.raises(ValueError):
        cp.assess_phases({}, **CLOCK_PARAMS)


# ---------------------------------------------------------------- the probe interface and the CLI
def test_there_is_no_built_in_probe_only_an_injected_one():
    class Synthetic:
        def __init__(self):
            self.n = 0

        def sample(self):
            self.n += 1
            return clock_sample(base_ns=BASE + self.n * 10**10, mono_base_ns=self.n * 10**10)

    taken = cp.collect_samples(Synthetic(), 3, sleep=lambda s: None, pause_s=0.5)
    assert len(taken) == 3 and assess(taken)["verdict"] == cp.AGREE
    with pytest.raises(NotImplementedError):
        cp.ClockProbe().sample()
    with pytest.raises(TypeError):
        cp.collect_samples(object(), 1)
    for bad in (0, -1, True, 1.5):
        with pytest.raises(ValueError):
            cp.collect_samples(Synthetic(), bad)
    assert not [n for n in dir(cp) if "wsl" in n.lower() or "probe_" in n.lower()]


def test_the_cli_reads_a_file_and_returns_distinct_exit_codes(tmp_path, capsys):
    path = tmp_path / "samples.json"
    args = ["--samples", str(path), "--tolerance-ms", "10", "--min-valid-samples", "3", "--max-wall-monotonic-disagreement-ms", "1"]
    for samples, code in ((good(offset_ns=0), 0), (good(offset_ns=5_000 * MS), 1), (good(rtt_ns=100 * MS), 3)):
        path.write_text(json.dumps({"pre_run": samples}), encoding="utf-8")
        assert cp.main(args) == code
        assert json.loads(capsys.readouterr().out)["verdict"] in (cp.AGREE, cp.DISAGREE, cp.INCONCLUSIVE)
    path.write_text(json.dumps(good()), encoding="utf-8")                      # a bare list is one phase
    assert cp.main(args) == 0
    path.write_text("not json", encoding="utf-8")
    assert cp.main(args) == 2
    assert cp.main(["--samples", str(tmp_path / "absent.json"), *args[2:]]) == 2


def test_the_required_measurements_are_documented_in_the_module():
    text = " ".join(cp.REQUIRED_MEASUREMENTS)
    for needle in ("reference wall clock", "monotonic", "resolution", "before the live round and one after", "tolerance"):
        assert needle in text


# ---------------------------------------------------------------- nothing is run
def test_assessment_starts_no_process_and_opens_no_connection(monkeypatch):
    def boom(*args, **kwargs):
        raise AssertionError("a process or network API was called")

    for owner, names in ((subprocess, ("run", "Popen", "call", "check_call", "check_output")), (os, ("system", "popen")),
                         (socket, ("create_connection", "getaddrinfo")), (urllib.request, ("urlopen",))):
        for name in names:
            if hasattr(owner, name):
                monkeypatch.setattr(owner, name, boom)
    monkeypatch.setattr(socket.socket, "connect", boom)
    assert assess(good())["verdict"] == cp.AGREE
    assert cp.assess_phases({"a": good(), "b": good(offset_ns=9_000 * MS)}, **CLOCK_PARAMS)["verdict"] == cp.DISAGREE


def test_the_clock_module_imports_nothing_that_can_start_a_process_or_open_a_socket():
    tree = ast.parse((REPO / "src/fabric_perf/controllers/clock_preflight.py").read_text(encoding="utf-8"))
    names = {a.name.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names} | \
            {n.module.split(".")[0] for n in ast.walk(tree) if isinstance(n, ast.ImportFrom) and n.module}
    assert not names & {"subprocess", "socket", "urllib", "http", "os", "shutil", "asyncio", "multiprocessing", "ssl", "docker"}
    source = (REPO / "src/fabric_perf/controllers/clock_preflight.py").read_text(encoding="utf-8")
    assert "os.environ" not in source and "getenv" not in source
