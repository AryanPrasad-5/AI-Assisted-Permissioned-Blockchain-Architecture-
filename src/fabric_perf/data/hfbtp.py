from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
from fabric_perf.data.base import BaseDatasetLoader


class HFBTPDatasetLoader(BaseDatasetLoader):
    """Loader and schema auditor for the Hyperledger Fabric Benchmark & Tuning Platform (HFBTP) dataset."""

    @property
    def dataset_id(self) -> str:
        return "hfbtp"

    def load_raw(self) -> pd.DataFrame:
        """Loads the raw HFBTP data file.
        
        Note: Exact filename and format will be bound during the feature/dataset-audit milestone.
        """
        raw_files = list(self.data_dir.glob("*.csv")) + list(self.data_dir.glob("*.json"))
        if not raw_files:
            raise FileNotFoundError(
                f"No raw data files found in {self.data_dir}. "
                "Please follow data/README.md to register HFBTP raw files."
            )
        target_file = raw_files[0]
        if target_file.suffix == ".csv":
            self._raw_data = pd.read_csv(target_file)
        else:
            self._raw_data = pd.read_json(target_file)
        return self._raw_data

    def validate_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Audits HFBTP dataframe for column completeness, missing values, and data types."""
        audit_report = {
            "dataset_id": self.dataset_id,
            "row_count": len(df),
            "col_count": len(df.columns),
            "columns": list(df.columns),
            "missing_values": df.isnull().sum().to_dict(),
            "has_nulls": bool(df.isnull().any().any()),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        }
        return audit_report

    def get_feature_names(self) -> List[str]:
        """Placeholder: To be finalized after feature/dataset-audit."""
        return ["tx_rate", "batch_size", "batch_timeout"]

    def get_target_names(self) -> List[str]:
        """Primary targets: Throughput and Latency."""
        return ["throughput", "avg_latency"]
