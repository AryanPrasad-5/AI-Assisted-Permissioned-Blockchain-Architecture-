from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
from fabric_perf.data.base import BaseDatasetLoader


class MMBPDDatasetLoader(BaseDatasetLoader):
    """Loader and schema auditor for the Multi-scenario Permissioned Blockchain Dataset (MMBPD)."""

    @property
    def dataset_id(self) -> str:
        return "mmbpd"

    def load_raw(self) -> pd.DataFrame:
        raw_files = list(self.data_dir.glob("*.csv")) + list(self.data_dir.glob("*.json"))
        if not raw_files:
            raise FileNotFoundError(
                f"No raw data files found in {self.data_dir}. "
                "Please follow data/README.md to register MMBPD raw files."
            )
        target_file = raw_files[0]
        if target_file.suffix == ".csv":
            self._raw_data = pd.read_csv(target_file)
        else:
            self._raw_data = pd.read_json(target_file)
        return self._raw_data

    def validate_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "row_count": len(df),
            "col_count": len(df.columns),
            "columns": list(df.columns),
            "missing_values": df.isnull().sum().to_dict(),
            "has_nulls": bool(df.isnull().any().any()),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        }

    def get_feature_names(self) -> List[str]:
        return []

    def get_target_names(self) -> List[str]:
        return ["throughput", "avg_latency"]
