"""Naming contract: identifiers stay stable, labels tell the two linear baselines apart."""
import sys
import unittest
from pathlib import Path

src_dir = Path(__file__).resolve().parent.parent / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from fabric_perf.models import (
    BASELINE_MODELS,
    FEATURE_COLUMNS,
    FROZEN_PHASE1C_MLR_LABEL,
    MODEL_LABELS,
    MODEL_SHORT_LABELS,
    RAW_FEATURES,
    build_pipeline,
)


class TestModelLabels(unittest.TestCase):
    def test_every_identifier_has_a_label(self):
        self.assertEqual(set(MODEL_LABELS), set(BASELINE_MODELS))
        self.assertEqual(set(MODEL_SHORT_LABELS), set(BASELINE_MODELS))

    def test_linear_label_names_the_eight_feature_model(self):
        self.assertEqual(MODEL_LABELS["linear"], "Multiple Linear Regression (8 Engineered Features)")
        self.assertNotIn("polynomial", MODEL_LABELS["linear"].lower())
        # The label promises 8 predictors: the 3 raw columns plus 5 engineered ones.
        self.assertEqual(len(FEATURE_COLUMNS), 8)
        self.assertEqual(FEATURE_COLUMNS[: len(RAW_FEATURES)], RAW_FEATURES)

    def test_frozen_baseline_label_is_distinct(self):
        self.assertIn("3 Raw Features", FROZEN_PHASE1C_MLR_LABEL)
        self.assertIn("Frozen Phase 1C", FROZEN_PHASE1C_MLR_LABEL)
        self.assertNotEqual(FROZEN_PHASE1C_MLR_LABEL, MODEL_LABELS["linear"])

    def test_identifiers_and_estimator_are_unchanged(self):
        self.assertEqual(BASELINE_MODELS, ("dummy_median", "linear", "random_forest"))
        self.assertEqual(len(RAW_FEATURES), 3)
        linear = build_pipeline("linear")
        self.assertEqual(type(linear.named_steps["model"]).__name__, "LinearRegression")


if __name__ == "__main__":
    unittest.main()
