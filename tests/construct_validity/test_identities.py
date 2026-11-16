import numpy as np
import pandas as pd
import pytest

from cv_identities import divisor_scan, n_bound_violation_rate


def test_bound_passes_true_n_and_fails_large_n():
    send = np.full(200, 100.0)
    drain = np.linspace(0.5, 1.5, 200)
    n = 1000
    throughput = np.round(n / (n / send + drain), 1)
    peak = np.round(drain, 2)
    assert n_bound_violation_rate(throughput, send, peak, n) == 0
    assert n_bound_violation_rate(throughput, send, peak, 2 * n) > 0.9


def test_divisor_scan_recovers_planted_n():
    rows = []
    for level in (100, 150):
        for block in range(10, 130, 5):
            rows.append({"s": level, "b": block, "t": level - (0 if 700 % block == 0 else 30)})
    assert divisor_scan(pd.DataFrame(rows), "s", "b", "t")[0]["n"] in (700, 1400, 2100, 2800)


def test_zero_drain_never_violates():
    assert n_bound_violation_rate([100.0], [100.0], [0.1], 5000) == pytest.approx(0)
