"""Pin the numbers reported in the 2026-11-06 audit against real inputs. Skipped if inputs are absent."""
import pytest

import cv_common as cc
import cv_identities as ident
import cv_mechanism as mech
import cv_mmbpd_observed as mm
import cv_protocol_stats as ps


def test_hfbtp_n_bound(hfbtp):
    rate = lambda n: ident.n_bound_violation_rate(hfbtp[cc.TPUT], hfbtp[cc.ACTUAL], hfbtp[cc.MAX_LAT], n)
    assert rate(1000) == 0
    assert rate(1050) == pytest.approx(967 / 24687)
    assert rate(1100) == pytest.approx(2409 / 24687)


def test_mmbpd_n_bound_uses_observed_columns_only(mmbpd):
    rate = lambda n: ident.n_bound_violation_rate(mmbpd[cc.TPUT], mmbpd[cc.M_SEND], mmbpd[cc.MAX_LAT], n)
    assert rate(1000) == 0
    assert rate(1050) == pytest.approx(0.0396, abs=1e-4)
    assert rate(1100) == pytest.approx(0.1149, abs=1e-4)


def test_divisor_scan_rank(hfbtp, mmbpd):
    assert ident.divisor_scan(hfbtp, cc.SET, cc.BLOCK, cc.TPUT)[0]["n"] == 1000
    assert ident.divisor_scan(mmbpd, cc.M_SET, cc.BLOCK, cc.TPUT)[0]["n"] == 1000


def test_level_165_counts(hfbtp):
    found = ident.level_165_structure(hfbtp)
    assert (found["below_130"], found["between_130_150"], found["at_least_150"]) == (251, 1, 381)


def test_heavy_gap(hfbtp):
    found = ident.heavy_decomposition(hfbtp)
    assert found["mean_t_heavy"] - found["mean_t_level_145"] == pytest.approx(34.75, abs=0.01)
    assert found["mae_copy_level_145"] == pytest.approx(34.803, abs=0.001)
    assert found["mae_identity"] == pytest.approx(26.103, abs=0.001)
    assert found["share_above_own_config_train_max"] == pytest.approx(0.99, abs=0.005)


def test_legacy_code_reproduces_reported_partition_scores(hfbtp, manifests):
    offset = mech.commit_offset(hfbtp)
    assert offset == pytest.approx(0.14)
    scores = mech.partition_scores(mech.legacy_prediction_table(hfbtp, offset), manifests)
    assert scores["HEAVY"]["throughput_mae"] == pytest.approx(8.661, abs=0.001)
    assert scores["T1"]["throughput_mae"] == pytest.approx(5.582, abs=0.001)
    assert scores["T2D3"]["throughput_mae"] == pytest.approx(3.681, abs=0.001)
    assert scores["HEAVY"]["latency_mae"] == pytest.approx(0.352, abs=0.001)


def test_exact_cutter_stays_close_to_legacy(hfbtp, manifests):
    offset = mech.commit_offset(hfbtp)
    legacy = mech.partition_scores(mech.legacy_prediction_table(hfbtp, offset), manifests)
    exact = mech.partition_scores(mech.prediction_table(hfbtp, offset), manifests)
    for name in cc.PARTITIONS:
        assert abs(legacy[name]["throughput_mae"] - exact[name]["throughput_mae"]) < 0.15


def test_fill_regime_selects_reported_parameters(hfbtp):
    best = mech.fill_regime_scan(hfbtp, mech.commit_offset(hfbtp)).iloc[0]
    assert (int(best["n"]), best["timeout"]) == (1000, "2")


def test_interpolation_reproduces_k1(hfbtp, manifests):
    scores = ident.interpolation_scores(hfbtp, manifests, cc.partition_keys)
    assert scores["T2D0"]["throughput_mae"] == pytest.approx(0.816, abs=0.001)
    assert scores["T2D3"]["throughput_mae"] == pytest.approx(4.163, abs=0.001)


def test_orderers_are_not_identifiable(hfbtp, mmbpd):
    gap = mm.orderer_gap(hfbtp, mmbpd)
    values = [row["throughput"] for row in gap["distance_by_assumed_orderers"].values()]
    assert max(values) - min(values) < 0.5
    assert gap["orderer_main_effect_variance_share"] < 1e-4


def test_warmup_gate_almost_always_fails_many_stable_runs():
    per_run = ps.warmup_gate_false_alarm(reps=200_000)
    assert per_run == pytest.approx(0.05, abs=0.003)
    assert (1 - per_run) ** 96 < 0.012


def test_e1_wilcoxon_limit():
    assert ps.minimum_wilcoxon_p(5) == pytest.approx(0.0625)
