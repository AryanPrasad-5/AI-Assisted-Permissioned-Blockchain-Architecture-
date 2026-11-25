"""x0_decide (the pre-registered X0 -> M chain) and continue_check_v2 (integrity-only stop rule), on synthetic evidence."""
import hashlib
import json
import sys
from pathlib import Path

import pytest

from Fabric_Live_Testbed.closed_loop_v2 import protocol as P
from Fabric_Live_Testbed.closed_loop_v2 import x0_decide as XD

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ops"))
import continue_check_v2 as CC  # noqa: E402

RATES = P.X0_RUNG_RATES_TPS
P10 = [900, 900, 950, 1000, 1100, 1300, 2500, 9000, 20000]
P40 = [2000, 1950, 1900, 1800, 1700, 1600, 1500, 1500, 1500]


def rows_for(p10, p40, *, cls=None, failed=None):
    rows, run = [], 0
    for b, p in ((10, p10), (40, p40)):
        for rep in (1, 2):
            run += 1
            for rate, v in zip(RATES, p):
                c = (cls or {}).get((b, rate), "OK")
                rows.append({"run_id": f"cx{run:02d}", "B": b, "replicate": rep, "round": 1, "rate_tps": float(rate), "scheduled": 1000,
                             "success": 1000, "failed": (failed or {}).get((b, rate), 0), "pending_at_deactivate": 0, "mean_ms": v * 0.6,
                             "p50_ms": v * 0.5, "p95_ms": float(v), "p99_ms": v * 1.2, "round_wall_s": 62.0, "lag_p99_ms": 5.0,
                             "overflow_callbacks": 0, "class": c})
    return rows


RANKED = [{"start_s": 600 * i, "rank": i, "segment_rates_tps": [7.0, 8, 9, 10, 11, 12, 14, 17, 20, 25], "preroll_rate_tps": 10.0,
           "warmup_rates_tps": [10.0] * 10} for i in range(1, 6)]


def test_crossover_selects_window_k_reactive_threshold_and_surface_from_x0_only():
    d = XD.decide(rows_for(P10, P40), RANKED)
    assert d["status"] == "GO_M" and d["crossover"]["r_c"] == 130 and d["window"]["start_s"] == 600
    assert 2.0 <= d["k"] <= 25.0
    assert d["reactive"]["latency_high_ms"] == pytest.approx(2500 * 0.6)          # B=10 mean latency at r_c
    assert d["reactive"]["latency_low_ms"] == pytest.approx(2500 * 0.6 * 0.8)
    assert {p["B"] for p in d["surface_points"]} == {10, 40} and max(p["rate_tps"] for p in d["surface_points"]) == 200


def test_no_crossover_stops_with_a_negative_result_and_builds_nothing():
    d = XD.decide(rows_for([900] * 9, [1900] * 9), RANKED)
    assert d["status"] == "STOP_NEGATIVE_RESULT" and d["reason"] == "NO_CROSSOVER_WITHIN_CLIENT_CAPACITY"
    assert "window" not in d and "k" not in d and "adaptive comparison is not run" in d["meaning"]


def test_client_limited_high_rungs_stop_with_not_established():
    cls = {(10, 160.0): "CLIENT_LIMITED", (10, 200.0): "CLIENT_LIMITED"}
    d = XD.decide(rows_for(P10, P40, cls=cls), RANKED)
    assert d["status"] == "STOP_NEGATIVE_RESULT" and d["crossover"]["r_max"] == 130


def test_crossover_that_no_ranked_window_can_straddle_is_a_negative_result():
    ranked = [{**w, "segment_rates_tps": [8, 9, 10, 11, 12, 13, 14, 15, 16, 17.0]} for w in RANKED]
    p10_late = [900, 900, 950, 1000, 1100, 1300, 1400, 9000, 20000]       # B=10 only loses at 160 and 200 tx/s: r_c = 160
    d = XD.decide(rows_for(p10_late, P40), ranked)
    assert d["crossover"]["r_c"] == 160 and d["crossover"]["r_max"] == 200
    # capacity caps k at 0.9*200/17 = 10.59, so the top scaled segment is 180: only one segment reaches 1.1 * 160 = 176 (two are needed)
    assert d["status"] == "STOP_NEGATIVE_RESULT" and d["reason"] == "NO_FEASIBLE_WINDOW"


def test_outputs_are_written_once_and_never_overwritten(tmp_path):
    d = XD.decide(rows_for(P10, P40), RANKED)
    XD.write_outputs(d, tmp_path / "o")
    assert json.loads((tmp_path / "o" / "x0_decision.json").read_text())["status"] == "GO_M"
    assert (tmp_path / "o" / "rung_table.csv").exists() and (tmp_path / "o" / "surface_points.csv").exists()
    with pytest.raises(FileExistsError):
        XD.write_outputs(d, tmp_path / "o")


# ------------------------------------------------------------------------------------------------ continuation rule
def evidence(tmp_path, *, verdict="VALID_WITH_FLAGS", flags=("DISPATCH_LAG round 6 p99 120 ms",), failing=("runner verdict exactly VALID",
             "analysis verdict exactly VALID"), problems=(), step_failure=None, console="runner exit code 0", erro=0, restarts=0):
    out, stage, auth = tmp_path / "out", tmp_path / "stage", tmp_path / "AUTH.txt"
    (out / "cm01").mkdir(parents=True)
    (out / "cm01_verification").mkdir()
    (out / "cm01_fabric_logs").mkdir()
    stage.mkdir()
    (stage / "SHA256SUMS.txt").write_bytes(b"x\n")
    auth.write_bytes(b"a\n")
    (out / "cm01" / "run_result.json").write_text(json.dumps({"verdict": verdict, "step_failure": step_failure,
        "stage_sha256": hashlib.sha256(b"x\n").hexdigest(), "authorization_sha256": hashlib.sha256(b"a\n").hexdigest()}))
    (out / "cm01" / "analysis.json").write_text(json.dumps({"problems": list(problems), "flags": list(flags), "verdict": verdict}))
    checks = [{"check": n, "pass": False, "detail": ""} for n in failing] + [{"check": "some other check", "pass": True, "detail": ""}]
    (out / "cm01_verification" / "independent_verification.json").write_text(json.dumps({"checks": checks}))
    (out / "cm01_runner_console.log").write_text(console + "\n")
    (out / "cm01_fabric_logs" / "orderer.example.com.log").write_text("2026-11-25T07:00:00.000Z 2026-11-25 07:00:00.000 UTC 0042 ERRO [orderer] boom\n" * erro + "2026-11-25T07:00:01Z INFO fine\n")
    (out / "cm01_fabric_logs" / "containers_after.txt").write_text("".join(f"c{i} running=true restarts={restarts if i == 0 else 0}\n" for i in range(6)))
    return out, stage, auth


def test_flagged_run_continues_when_only_permitted_flags_and_verdict_checks_fail(tmp_path):
    out, stage, auth = evidence(tmp_path)
    r = CC.evaluate("cm01", out, stage, auth)
    assert r["decision"] == "CONTINUE" and all(c["pass"] for c in r["checks"])


def test_saturation_outcomes_are_data_not_a_stop(tmp_path):
    out, stage, auth = evidence(tmp_path, flags=("DISPATCH_LAG round 4 p99 900 ms", "FAILED_TRANSACTIONS 37"))
    assert CC.evaluate("cm01", out, stage, auth)["decision"] == "CONTINUE"


@pytest.mark.parametrize("kw", [
    {"flags": ("CLOCK_SUSPECT round 3 (80.0 ms)",)},
    {"failing": ("runner verdict exactly VALID", "analysis verdict exactly VALID", "every recorder success VALID on ledger")},
    {"verdict": "VALID", "failing": ("runner verdict exactly VALID",)},                   # verdict checks may fail only for VALID_WITH_FLAGS
    {"problems": ("round 2: 10 recorder rows for 11 scheduled",), "verdict": "INVALID_INCOMPLETE"},
    {"step_failure": {"step": "actuation", "detail": "x"}},
    {"console": "runner exit code 3"},
    {"erro": 2},
    {"restarts": 1},
])
def test_integrity_failures_stop(tmp_path, kw):
    out, stage, auth = evidence(tmp_path, **kw)
    assert CC.evaluate("cm01", out, stage, auth)["decision"] == "STOP"


def test_decision_file_is_written_once(tmp_path, capsys):
    out, stage, auth = evidence(tmp_path)
    assert CC.main(["cm01", "--out", str(out), "--stage", str(stage), "--auth", str(auth)]) == 0
    assert (out / "decisions_v2" / "cm01.json").exists()
    assert CC.main(["cm01", "--out", str(out), "--stage", str(stage), "--auth", str(auth)]) == 2
