import sys
from pathlib import Path
import unittest
import pandas as pd
import numpy as np

src_dir = Path(__file__).resolve().parent.parent / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from fabric_perf.data import BaseDatasetLoader, HFBTPDatasetLoader, MMBPDDatasetLoader
from fabric_perf.controllers import StaticController
from fabric_perf.evaluation import calculate_regression_metrics
from fabric_perf.utils import seed_everything


class TestDataContract(unittest.TestCase):
    def test_loader_inheritance(self):
        self.assertTrue(issubclass(HFBTPDatasetLoader, BaseDatasetLoader))
        self.assertTrue(issubclass(MMBPDDatasetLoader, BaseDatasetLoader))

    def test_loader_schema_audit(self):
        dummy_dir = Path("dummy")
        loader = HFBTPDatasetLoader(dummy_dir)
        self.assertEqual(loader.dataset_id, "hfbtp")

        # Exact HFBTP column headers.
        df = pd.DataFrame({
            "Set Transaction Arrival Rate": [10.0, 50.0, 100.0],
            "Actual Transaction Arrival Rate": [10.1, 50.2, 99.8],
            "Block Size": [10.0, 50.0, 100.0],
            "Orderers": [3, 5, 7],
            "Throughput": [9.8, 48.5, 95.1],
            "Avg Latency": [0.49, 0.76, 0.85],
            "Min Latency": [0.02, 0.05, 0.08],
            "Max Latency": [0.95, 1.82, 2.13],
        })
        report = loader.validate_schema(df)
        self.assertEqual(report["dataset_id"], "hfbtp")
        self.assertEqual(report["row_count"], 3)
        self.assertFalse(report["has_nulls"])
        self.assertIn("Throughput", report["columns"])
        self.assertIn("Avg Latency", report["columns"])
        self.assertTrue(report["schema_valid"])
        self.assertEqual(report["missing_required_columns"], [])


    def test_static_controller(self):
        cfg = {"batch_size": 50, "batch_timeout": 500}
        controller = StaticController(cfg)
        state = {"current_latency": 1.2, "tps": 200}
        decision = controller.select_configuration(state)
        self.assertEqual(decision, cfg)

    def test_metrics_calculation(self):
        y_true = np.array([10.0, 20.0, 30.0])
        y_pred = np.array([10.0, 20.0, 30.0])
        metrics = calculate_regression_metrics(y_true, y_pred, prefix="test_")
        self.assertEqual(metrics["test_mae"], 0.0)
        self.assertEqual(metrics["test_rmse"], 0.0)
        self.assertEqual(metrics["test_r2"], 1.0)

    def test_seed_reproducibility(self):
        s = seed_everything(42)
        self.assertEqual(s, 42)


if __name__ == "__main__":
    unittest.main()
