                                                               
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))

import e1_analysis as ea              
import e1_schedule as es              


def test_schedule_is_balanced_seeded_and_never_repeats():
    rows = es.build()
    assert len(rows) == 40
    frame = pd.DataFrame(rows)
    assert (frame.groupby(["mode", "rate", "block"]).size() == 5).all()
    keys = list(zip(frame["mode"], frame.rate, frame.block))
    assert all(a != b for a, b in zip(keys, keys[1:]))
    assert rows == es.build()


def test_extended_schedule_has_sixty_runs():
    assert len(es.build(blocks=(40, 45, 500))) == 60


def _tx(rate, n, finish_lag, drain_extra=0.0, fail=0):
    create = np.arange(n) * 1000.0 / rate
    final = create + finish_lag
    final[-1] += drain_extra * 1000
    status = ["success"] * n
    for i in range(fail):
        status[i] = "failed"
    return pd.DataFrame({"create_ms": create, "final_ms": final, "status": status})


def test_summary_matches_caliper_formula():
    found = ea.summarise_run(_tx(100, 1000, 100.0))
    span = 999 / 100
    assert found["send_rate"] == pytest.approx(1000 / span)
    assert found["caliper_throughput"] == pytest.approx(1000 / (span + 0.1))
    assert found["drain_s"] == pytest.approx(0.1)


def test_windowed_throughput_excludes_warmup():
    assert ea.summarise_run(_tx(100, 12000, 100.0))["windowed_throughput"] == pytest.approx(100, abs=0.1)


def test_fixed_count_run_has_no_window():
    assert np.isnan(ea.summarise_run(_tx(100, 1000, 100.0))["windowed_throughput"])


def _runs(gap=40.0, duration_gap=2.5, jump=40.0):
    rng = np.random.default_rng(1)
    rows = []
    for rate in (160, 170):
        for block in (40, 45):
            for mode in ("txNumber", "txDuration"):
                for rep in range(5):
                    fixed = mode == "txNumber"
                    t = rate - (gap if fixed and block == 45 else 0) - (duration_gap if not fixed and block == 45 else 0)
                    rows.append({"mode": mode, "rate": rate, "block": block, "rep": rep,
                                 "send_rate": rate, "failure_rate": 0.0,
                                 "caliper_throughput": t + rng.normal(0, 0.3),
                                 "windowed_throughput": np.nan if fixed else rate + rng.normal(0, 0.2),
                                 "drain_s": (2.1 if fixed and block == 45 and gap else 0.2)})
    return pd.DataFrame(rows)


def test_planted_artefact_is_supported():
    found = ea.evaluate(_runs())
    assert found["verdict"] == "SUPPORTED"
    assert found["per_rate"][160]["exact_mann_whitney_p"] == pytest.approx(2 / 252)


def test_no_artefact_is_rejected():
    assert ea.evaluate(_runs(gap=0.0, duration_gap=0.0))["verdict"] == "REJECTED_FOR_THIS_DEPLOYMENT"


def test_artefact_surviving_in_duration_mode_is_not_supported():
    assert ea.evaluate(_runs(duration_gap=30.0))["verdict"] == "INCONCLUSIVE"


def test_runs_missing_target_rate_are_removed():
    runs = _runs()
    runs.loc[runs.index[:3], "send_rate"] = 100.0
    assert ea.evaluate(runs)["valid_runs"] == len(runs) - 3
