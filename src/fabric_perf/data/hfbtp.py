"""HFBTP dataset loader, schema validation, and audit utilities.

Verified column headers (data/raw/hfbtp/HFBTP.csv, recomputed audit 2026-10-15):
    Features:  'Set Transaction Arrival Rate', 'Block Size', 'Orderers'
    Targets:   'Throughput' (tx/s), 'Avg Latency' (seconds)
    Excluded:  'Min Latency', 'Max Latency' (post-execution measurements)
    Caution:   'Actual Transaction Arrival Rate' (measured during the run; leakage
               risk — see docs/hfbtp_audit_report.md and docs/evaluation_protocol.md)

Audit reference values (verified against the actual file, NOT assumed):
    rows=24687, cols=8, nulls=0, duplicate rows=0, duplicate config cells=0,
    grid 39x159x4=24804 with 117 absent cells (99.528% coverage), one observation
    per configuration cell, no timestamps, no replications.
"""

from pathlib import Path
from typing import Dict, Any, List, Optional
import hashlib

import pandas as pd

from fabric_perf.data.base import (
    BaseDatasetLoader,
    DatasetLoadError,
    SchemaValidationError,
)


class HFBTPDatasetLoader(BaseDatasetLoader):
    """Loader and schema auditor for the Hyperledger Fabric Benchmark & Tuning
    Platform (HFBTP) dataset."""

    # All 8 columns that must be present in a valid HFBTP file (exact headers).
    REQUIRED_COLUMNS: List[str] = [
        "Set Transaction Arrival Rate",
        "Actual Transaction Arrival Rate",
        "Block Size",
        "Orderers",
        "Throughput",
        "Avg Latency",
        "Min Latency",
        "Max Latency",
    ]

    # Safe predictors: available before the benchmark run executes.
    FEATURE_COLUMNS: List[str] = [
        "Set Transaction Arrival Rate",
        "Block Size",
        "Orderers",
    ]

    TARGET_COLUMNS: List[str] = ["Throughput", "Avg Latency"]

    # Post-execution measurements: excluded from features AND targets of the
    # baseline pre-execution prediction model.
    EXCLUDED_COLUMNS: List[str] = ["Min Latency", "Max Latency"]

    # Measured during the run (telemetry). Corr(Throughput)=0.985. Excluded from
    # the primary feature set; documented leakage risk.
    CAUTION_COLUMNS: List[str] = ["Actual Transaction Arrival Rate"]

    # Plausible value ranges verified against the actual file (inclusive bounds).
    EXPECTED_RANGES: Dict[str, tuple] = {
        "Set Transaction Arrival Rate": (10.0, 200.0),
        "Actual Transaction Arrival Rate": (0.0, 200.0),
        "Block Size": (10.0, 800.0),
        "Orderers": (3, 9),
        "Throughput": (0.0, 250.0),
        "Avg Latency": (0.0, 30.0),
        "Min Latency": (0.0, 30.0),
        "Max Latency": (0.0, 60.0),
    }

    GRID_KEYS = [
        "Set Transaction Arrival Rate",
        "Block Size",
        "Orderers",
    ]

    @property
    def dataset_id(self) -> str:
        return "hfbtp"

    # ------------------------------------------------------------------ files

    def _find_raw_file(self) -> Path:
        """Locate the raw HFBTP CSV (HFBTP.csv preferred, else the sole .csv)."""
        if not self.data_dir.exists():
            raise DatasetLoadError(
                f"HFBTP data directory does not exist: {self.data_dir}. "
                "Place HFBTP.csv in data/raw/hfbtp/ (see data/raw/hfbtp/README.md)."
            )
        preferred = self.data_dir / "HFBTP.csv"
        if preferred.exists():
            return preferred
        csv_files = sorted(self.data_dir.glob("*.csv"))
        if not csv_files:
            raise DatasetLoadError(
                f"No CSV file found in {self.data_dir}. Expected 'HFBTP.csv'. "
                "Please follow data/README.md to register HFBTP raw files."
            )
        return csv_files[0]

    def compute_checksums(self) -> Dict[str, str]:
        """Compute MD5 and SHA-256 checksums of the raw file (integrity audit)."""
        path = self._find_raw_file()
        data = path.read_bytes()
        return {
            "filename": path.name,
            "file_size_bytes": len(data),
            "md5": hashlib.md5(data).hexdigest().upper(),
            "sha256": hashlib.sha256(data).hexdigest().upper(),
        }

    def load_raw(self) -> pd.DataFrame:
        """Load the raw HFBTP file as-is.

        Caches the dataframe after the first call (no mutation of the source CSV,
        no row/column drops at this stage).
        """
        if self._raw_data is not None:
            return self._raw_data
        path = self._find_raw_file()
        try:
            raw = pd.read_csv(path)
        except Exception as exc:  # unreadable/encoding/parsing problems
            raise DatasetLoadError(
                f"Failed to read HFBTP file {path}: {exc}"
            ) from exc
        self._raw_data = raw
        return raw

    # ------------------------------------------------------------- validation

    def validate_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Audit the HFBTP dataframe for column completeness, missing values,
        dtypes, duplicates, and grid coverage.

        Returns a report; does not mutate the dataframe and does not drop
        anything. Use :func:`validate` (or :func:`audit`) when a hard contract
        failure must raise instead.
        """
        present = list(df.columns)
        missing_required = [c for c in self.REQUIRED_COLUMNS if c not in present]
        unexpected = [c for c in present if c not in self.REQUIRED_COLUMNS]
        violation = self.check_value_ranges(df)
        report = {
            "dataset_id": self.dataset_id,
            "row_count": len(df),
            "col_count": len(df.columns),
            "columns": present,
            "missing_required_columns": missing_required,
            "unexpected_columns": unexpected,
            "schema_valid": len(missing_required) == 0 and not unexpected,
            "missing_values": df.isnull().sum().to_dict(),
            "has_nulls": bool(df.isnull().any().any()),
            "duplicate_rows": int(df.duplicated().sum()),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "out_of_range_counts": violation,
        }
        return report

    def validate(self, df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """Hard schema validation: raises :class:`SchemaValidationError` with an
        informative message when required columns are absent or dtypes are
        unusable for numeric modeling. Returns the audit report otherwise."""
        df = self._raw_data if df is None else df
        if df is None:
            df = self.load_raw()
        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise SchemaValidationError(
                f"HFBTP file is missing required column(s): {missing}. "
                f"Expected headers (exact): {self.REQUIRED_COLUMNS}. "
                f"Present headers: {list(df.columns)}."
            )
        non_numeric = []
        for col in self.REQUIRED_COLUMNS:
            if not pd.api.types.is_numeric_dtype(df[col]):
                non_numeric.append(col)
        if non_numeric:
            raise SchemaValidationError(
                f"HFBTP column(s) must be numeric but are not: {non_numeric}. "
                "Check for stray header rows, locale decimal separators, or "
                "corrupted values in the raw CSV."
            )
        return self.validate_schema(df)

    def check_value_ranges(self, df: pd.DataFrame) -> Dict[str, int]:
        """Count rows outside the plausible value range per column.

        Never removes rows — reporting only (audit discipline: no silent drops).
        """
        out: Dict[str, int] = {}
        for col, (lo, hi) in self.EXPECTED_RANGES.items():
            if col not in df.columns:
                continue
            if not pd.api.types.is_numeric_dtype(df[col]):
                continue
            out[col] = int(((df[col] < lo) | (df[col] > hi)).sum())
        return out

    # ------------------------------------------------------------------ audit

    def describe_grid(self, df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        """Describe the (Set Rate x Block Size x Orderers) configuration grid:
        unique levels, full factorial size, absent cells, coverage.

        With zero replicated trials, this is a cross-sectional response surface,
        NOT a time series; statistical claims about run-to-run variability are
        out of scope for this dataset (see docs/hfbtp_audit_report.md).
        """
        df = self._raw_data if df is None else df
        if df is None:
            df = self.load_raw()
        n_set = int(df["Set Transaction Arrival Rate"].nunique())
        n_block = int(df["Block Size"].nunique())
        n_ord = int(df["Orderers"].nunique())
        full_grid = n_set * n_block * n_ord
        observed_cells = int(df.groupby(self.GRID_KEYS).ngroups)
        cells_per_row = df.groupby(self.GRID_KEYS).size()
        return {
            "unique_set_arrival_rates": n_set,
            "unique_block_sizes": n_block,
            "unique_orderer_counts": n_ord,
            "full_grid_size": full_grid,
            "observed_cells": observed_cells,
            "missing_cells": full_grid - observed_cells,
            "coverage_pct": round(100.0 * observed_cells / full_grid, 3),
            "max_rows_per_cell": int(cells_per_row.max()),
            "duplicate_config_cells": int(cells_per_row.max() > 1) * int(
                (cells_per_row > 1).sum()
            ),
        }

    def audit(self) -> Dict[str, Any]:
        """Full reproducible audit: checksums, schema, quality, grid, extractive
        target ranges. Combines every check in one report dict."""
        df = self.load_raw()
        report = {"dataset_id": self.dataset_id}
        report.update(self.compute_checksums())
        report["schema"] = self.validate(df)
        report["grid"] = self.describe_grid(df)
        targets = {}
        for col in self.TARGET_COLUMNS:
            targets[col] = {
                "min": float(df[col].min()),
                "mean": float(df[col].mean()),
                "median": float(df[col].median()),
                "max": float(df[col].max()),
            }
        report["target_ranges"] = targets
        divergence = (
            df["Set Transaction Arrival Rate"] - df["Actual Transaction Arrival Rate"]
        ).abs()
        report["set_vs_actual_rate_divergence_rows_gt_5tx"] = int((divergence > 5).sum())
        report["set_vs_actual_rate_divergence_rows_gt_1tx"] = int((divergence > 1).sum())
        return report

    # ------------------------------------------------------------------ roles

    def get_feature_names(self) -> List[str]:
        """Safe predictors — available before the benchmark run executes.

        'Actual Transaction Arrival Rate' is intentionally excluded (measured
        during the run; leakage risk). 'Min/Max Latency' are post-execution
        measurements and never predictors.
        """
        return list(self.FEATURE_COLUMNS)

    def get_target_names(self) -> List[str]:
        """Primary regression targets: Throughput (tx/s) and Avg Latency (s)."""
        return list(self.TARGET_COLUMNS)

    def get_excluded_names(self) -> Dict[str, List[str]]:
        """Feature/target exclusions with their reasons (for documentation)."""
        return {
            "excluded_post_execution": list(self.EXCLUDED_COLUMNS),
            "caution_excluded_leakage_risk": list(self.CAUTION_COLUMNS),
        }
