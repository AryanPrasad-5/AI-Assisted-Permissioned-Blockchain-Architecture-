from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseController(ABC):
    """Abstract base class for Hyperledger Fabric configuration controllers."""

    @abstractmethod
    def select_configuration(self, current_state: Dict[str, Any]) -> Dict[str, Any]:
        """Determine next blockchain configuration parameters given the system state.
        
        Args:
            current_state (Dict[str, Any]): Current workload metrics, telemetry, or lagged performance.
            
        Returns:
            Dict[str, Any]: Recommended or applied configuration (e.g. batch_size, batch_timeout).
        """
        pass


class StaticController(BaseController):
    """Fixed baseline controller holding predetermined static configuration parameters."""

    def __init__(self, fixed_config: Dict[str, Any]):
        self.fixed_config = fixed_config

    def select_configuration(self, current_state: Dict[str, Any]) -> Dict[str, Any]:
        return dict(self.fixed_config)


class ReactiveController(BaseController):
    """Rule-based reactive controller adjusting configuration after observed degradation."""

    def __init__(self, default_config: Dict[str, Any], latency_threshold: float):
        self.default_config = default_config
        self.latency_threshold = latency_threshold

    @abstractmethod
    def select_configuration(self, current_state: Dict[str, Any]) -> Dict[str, Any]:
        pass


class PredictiveController(BaseController):
    """ML-driven proactive controller predicting future load/degradation before selecting configuration."""

    def __init__(self, model: Any, default_config: Dict[str, Any]):
        self.model = model
        self.default_config = default_config

    @abstractmethod
    def select_configuration(self, current_state: Dict[str, Any]) -> Dict[str, Any]:
        pass
