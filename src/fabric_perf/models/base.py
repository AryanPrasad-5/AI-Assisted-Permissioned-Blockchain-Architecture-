from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict
import numpy as np


class BasePerformanceModel(ABC):
    """Abstract interface for regression models predicting Hyperledger Fabric throughput and latency."""

    @abstractmethod
    def fit(self, X: np.ndarray, y: np.ndarray) -> "BasePerformanceModel":
        """Fit model on training data.
        
        Args:
            X (np.ndarray): Training features.
            y (np.ndarray): Target matrix/vector.
        """
        pass

    @abstractmethod
    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict performance targets given features.
        
        Args:
            X (np.ndarray): Evaluation features.
            
        Returns:
            np.ndarray: Predicted throughput and latency.
        """
        pass

    @abstractmethod
    def save(self, filepath: Path) -> None:
        """Serialize model artifact to disk."""
        pass

    @classmethod
    @abstractmethod
    def load(cls, filepath: Path) -> "BasePerformanceModel":
        """Load serialized model artifact from disk."""
        pass
