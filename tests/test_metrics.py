"""Tests for regression metric calculations against hand-computed examples."""
import sys
from pathlib import Path
import unittest

import numpy as np

src_dir = Path(__file__).resolve().parent.parent / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from fabric_perf.evaluation.metrics import calculate_regression_metrics


class TestRegressionMetrics(unittest.TestCase):
    def test_perfect_predictions(self):
        y_true = np.array([10.0, 20.0, 30.0])
        metrics = calculate_regression_metrics(y_true, y_true.copy(), prefix="t_")
        self.assertEqual(metrics["t_mae"], 0.0)
        self.assertEqual(metrics["t_rmse"], 0.0)
        self.assertAlmostEqual(metrics["t_r2"], 1.0)
        self.assertEqual(metrics["t_mape"], 0.0)

    def test_known_mae_rmse(self):
        y_true = np.array([1.0, 2.0, 3.0, 4.0])
        y_pred = np.array([2.0, 2.0, 2.0, 2.0])
        metrics = calculate_regression_metrics(y_true, y_pred, prefix="t_")
        # MAE = (1+0+1+2)/4 = 1.0 ; RMSE = sqrt((1+0+1+4)/4) = sqrt(1.5)
        self.assertAlmostEqual(metrics["t_mae"], 1.0)
        self.assertAlmostEqual(metrics["t_rmse"], np.sqrt(1.5), places=12)

    def test_known_r2(self):
        y_true = np.array([1.0, 2.0, 3.0, 4.0])
        y_pred = np.array([2.0, 2.0, 2.0, 2.0])
        metrics = calculate_regression_metrics(y_true, y_pred, prefix="t_")
        # Mean = 2.5; SS_res = 1+0+1+4 = 6 ; SS_tot = 2.25+0.25+0.25+2.25 = 5.0
        self.assertAlmostEqual(metrics["t_r2"], 1 - 6 / 5.0, places=12)

    def test_mape_ignores_zero_actuals(self):
        y_true = np.array([0.0, 100.0])
        y_pred = np.array([50.0, 90.0])
        metrics = calculate_regression_metrics(y_true, y_pred, prefix="t_")
        # Only the second point contributes: |(100-90)/100| = 10%.
        self.assertAlmostEqual(metrics["t_mape"], 10.0, places=9)

    def test_mape_nan_when_all_zero(self):
        y_true = np.array([0.0, 0.0])
        y_pred = np.array([1.0, 1.0])
        metrics = calculate_regression_metrics(y_true, y_pred, prefix="t_")
        self.assertTrue(np.isnan(metrics["t_mape"]))

    def test_prefix_applied(self):
        m = calculate_regression_metrics(np.arange(5.0), np.arange(5.0), prefix="throughput_")
        for key in m:
            self.assertTrue(key.startswith("throughput_"))


if __name__ == "__main__":
    unittest.main()
