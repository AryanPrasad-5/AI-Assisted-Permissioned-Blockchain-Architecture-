from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd


class BaseDatasetLoader(ABC):
    """Abstract base class establishing the common interface for all research datasets.
    
    Ensures that different datasets (HFBTP, MMBPD, future benchmarks) implement
    consistent loading, metadata inspection, schema validation, and feature/target contracts.
    """

    def __init__(self, data_dir: Path, manifest_path: Optional[Path] = None):
        self.data_dir = Path(data_dir)
        self.manifest_path = Path(manifest_path) if manifest_path else None
        self._raw_data: Optional[pd.DataFrame] = None

    @property
    @abstractmethod
    def dataset_id(self) -> str:
        """Unique string identifier for the dataset (e.g., 'hfbtp', 'mmbpd')."""
        pass

    @abstractmethod
    def load_raw(self) -> pd.DataFrame:
        """Load raw dataset files from disk without applying preprocessing transformations.
        
        Returns:
            pd.DataFrame: Unmodified raw data.
        """
        pass

    @abstractmethod
    def validate_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Audit and validate the schema, checking required columns, data types, and nulls.
        
        Args:
            df (pd.DataFrame): Dataframe to audit.
            
        Returns:
            Dict[str, Any]: Audit report containing validation status and warnings.
        """
        pass

    @abstractmethod
    def get_feature_names(self) -> List[str]:
        """Return the list of predictor feature names."""
        pass

    @abstractmethod
    def get_target_names(self) -> List[str]:
        """Return the list of target variable names (e.g., throughput, latency)."""
        pass

    def get_metadata(self) -> Dict[str, Any]:
        """Return dataset provenance metadata."""
        return {
            "dataset_id": self.dataset_id,
            "data_dir": str(self.data_dir),
            "manifest_path": str(self.manifest_path) if self.manifest_path else None,
        }
