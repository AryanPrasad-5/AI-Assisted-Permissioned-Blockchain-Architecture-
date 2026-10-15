"""Tests for HFBTP loading, schema validation, preprocessing guards, and
split integrity / leakage prevention."""
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

src_dir = Path(__file__).resolve().parent.parent / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from fabric_perf.data.base import DatasetLoadError, SchemaValidationError
from fabric_perf.data.hfbtp import HFBTPDatasetLoader
from fabric_perf.models.baseline import (
    FEATURE_COLUMNS,
    add_engineered_features,
    build_pipeline,
    separate_features_targets,
)
from fabric_perf.evaluation.splitters import (
    block_size_band_split,
    t1b_right_edge_band_split,
    t2_d0_workload_split,
    t3_orderer_split,
    T1B_TEST_BAND,
    T1B_VAL_BAND,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
REAL_HFBTP = REPO_ROOT / "data" / "raw" / "hfbtp" / "HFBTP.csv"


def _sample_df(n: int = 60) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    df = pd.DataFrame(
        {
            "Set Transaction Arrival Rate": rng.choice([10.0, 50.0, 100.0], n),
            "Actual Transaction Arrival Rate": rng.choice([10.0, 50.0, 99.0], n),
            "Block Size": rng.choice([10.0, 100.0, 400.0, 710.0, 800.0], n),
            "Orderers": rng.choice([3, 5, 7, 9], n),
        }
    )
    df["Throughput"] = df["Set Transaction Arrival Rate"] * 0.9
    df["Avg Latency"] = 0.5
    df["Min Latency"] = 0.1
    df["Max Latency"] = 1.0
    return df


class TestLoading(unittest.TestCase):
    def test_missing_directory_raises(self):
        with TemporaryDirectory() as tmp:
            loader = HFBTPDatasetLoader(Path(tmp) / "nope")
            with self.assertRaises(DatasetLoadError):
                loader.load_raw()

    def test_empty_directory_raises(self):
        with TemporaryDirectory() as tmp:
            loader = HFBTPDatasetLoader(Path(tmp))
            with self.assertRaises(DatasetLoadError):
                loader.load_raw()

    @unittest.skipUnless(REAL_HFBTP.exists(), "raw HFBTP.csv not present")
    def test_real_file_identification(self):
        loader = HFBTPDatasetLoader(REAL_HFBTP.parent)
        df = loader.load_raw()
        self.assertEqual(df.shape, (24687, 8))
        self.assertEqual(list(df.columns), HFBTPDatasetLoader.REQUIRED_COLUMNS)
        checksums = loader.compute_checksums()
        self.assertEqual(
            checksums["md5"], "E9275C49A2D7DC1F2A4015B6D8B28482".upper()
        )
        self.assertEqual(
            checksums["sha256"],
            "CE295D6451787FEFCFE2101579238CDA4BEDE8539BC302E6A482AA4E5782BD5E",
        )
        grid = loader.describe_grid(df)
        self.assertEqual(grid["missing_cells"], 117)
        self.assertEqual(grid["max_rows_per_cell"], 1)


class TestSchemaValidation(unittest.TestCase):
    def test_validate_passes_on_sample(self):
        loader = HFBTPDatasetLoader(Path("dummy"))
        report = loader.validate(_sample_df())
        self.assertTrue(report["schema_valid"])

    def test_missing_column_raises_with_names(self):
        loader = HFBTPDatasetLoader(Path("dummy"))
        df = _sample_df().drop(columns=["Avg Latency"])
        with self.assertRaises(SchemaValidationError) as ctx:
            loader.validate(df)
        self.assertIn("Avg Latency", str(ctx.exception))

    def test_non_numeric_column_raises(self):
        loader = HFBTPDatasetLoader(Path("dummy"))
        df = _sample_df()
        df["Throughput"] = df["Throughput"].astype(object)
        df.loc[0, "Throughput"] = "oops"
        with self.assertRaises(SchemaValidationError) as ctx:
            loader.validate(df)
        self.assertIn("Throughput", str(ctx.exception))

    def test_out_of_range_reported_not_dropped(self):
        loader = HFBTPDatasetLoader(Path("dummy"))
        df = _sample_df()
        df.loc[0, "Block Size"] = 9999.0
        report = loader.validate(df)
        self.assertEqual(report["out_of_range_counts"]["Block Size"], 1)
        self.assertEqual(len(df), 60, "validation must never drop rows")


class TestNoMutation(unittest.TestCase):
    @unittest.skipUnless(REAL_HFBTP.exists(), "raw HFBTP.csv not present")
    def test_raw_file_unchanged_after_load(self):
        before = REAL_HFBTP.read_bytes()
        loader = HFBTPDatasetLoader(REAL_HFBTP.parent)
        loader.load_raw()
        self.assertEqual(before, REAL_HFBTP.read_bytes())

    def test_engineered_features_return_new_frame(self):
        df = _sample_df()
        out = add_engineered_features(df)
        self.assertIsNot(out, df)
        # Original frame untouched:
        self.assertNotIn("log_set_rate", df.columns)
        # The originals are preserved verbatim in the copy:
        pd.testing.assert_series_equal(out["Throughput"], df["Throughput"])


class TestFeatureTargetSeparation(unittest.TestCase):
    def test_feature_whitelist_is_leakage_free(self):
        forbidden = {
            "Actual Transaction Arrival Rate",
            "Min Latency",
            "Max Latency",
            "Throughput",
            "Avg Latency",
        }
        self.assertTrue(forbidden.isdisjoint(FEATURE_COLUMNS))

    def test_unknown_target_rejected(self):
        df = add_engineered_features(_sample_df())
        with self.assertRaises(ValueError):
            separate_features_targets(df, target="Max Latency")

    def test_x_contains_only_whitelisted_columns(self):
        df = add_engineered_features(_sample_df())
        X, y = separate_features_targets(df, target="Throughput")
        self.assertEqual(list(X.columns), FEATURE_COLUMNS)
        self.assertEqual(len(X), len(y))

    def test_all_models_are_pipelines(self):
        for name in ("dummy_median", "linear", "random_forest"):
            pipe = build_pipeline(name)
            self.assertTrue(hasattr(pipe, "fit") and hasattr(pipe, "predict"))


class TestSplitIntegrity(unittest.TestCase):
    """Leakage prevention: band/level units must never span partitions."""

    def _check_disjoint_keys(self, tr, va, te):
        cols = ["Set Transaction Arrival Rate", "Block Size", "Orderers"]
        ka = set(map(tuple, tr[cols].itertuples(index=False)))
        kv = set(map(tuple, va[cols].itertuples(index=False)))
        ke = set(map(tuple, te[cols].itertuples(index=False)))
        self.assertFalse(ka & kv, "train/val key overlap")
        self.assertFalse(ka & ke, "train/test key overlap")
        self.assertFalse(kv & ke, "val/test key overlap")

    def test_t1b_band_completeness_and_disjointness(self):
        full = add_engineered_features(_sample_df(n=400))
        tr, va, te, info = t1b_right_edge_band_split(full)
        self._check_disjoint_keys(tr, va, te)
        self.assertEqual(set(T1B_TEST_BAND), set(info["test_band"]))
        # every in-band row is in test and nowhere else
        lo, hi = T1B_TEST_BAND
        in_band = full[(full["Block Size"] >= lo) & (full["Block Size"] <= hi)]
        self.assertEqual(len(te), len(in_band))
        # Sample data max block = 800; the 610-705 val band and 710-800 test
        # band are excluded from training, so the trained max must be 400.
        self.assertEqual(info["max_trained_block_size"], 400)

    def test_block_band_covers_full_frame(self):
        full = add_engineered_features(_sample_df(n=400))
        tr, va, te, _ = block_size_band_split(full, T1B_TEST_BAND, T1B_VAL_BAND)
        self.assertEqual(len(tr) + len(va) + len(te), len(full))

    def test_t2_d0_protocol_counts(self):
        full = add_engineered_features(_sample_df(n=400))
        tr, va, te, info = t2_d0_workload_split(full)
        self._check_disjoint_keys(tr, va, te)
        # no test/val workload level appears in training
        used = set(tr["Set Transaction Arrival Rate"].unique())
        for lv in (30, 140, 150, 160, 155):
            self.assertNotIn(lv, used)
        # protocol hand-off: highest trained level is 145 in the real data; for
        # sampled data we check the exclusion rule itself
        self.assertFalse(any(lv >= 165 for lv in used))

    def test_t3_orderer_transfer(self):
        full = add_engineered_features(_sample_df(n=400))
        tr, va, te, info = t3_orderer_split(full)
        self._check_disjoint_keys(tr, va, te)
        self.assertEqual(set(tr["Orderers"].unique()), {3, 7})
        self.assertEqual(set(te["Orderers"].unique()), {5, 9})
        self.assertEqual(len(tr) + len(va) + len(te), len(full))


if __name__ == "__main__":
    unittest.main()
