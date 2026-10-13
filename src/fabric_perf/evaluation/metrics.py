from typing import Dict
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score


def calculate_regression_metrics(
    y_true: np.ndarray, y_pred: np.ndarray, prefix: str = ""
) -> Dict[str, float]:
    """Calculate standard regression metrics: MAE, RMSE, R2, and MAPE.
    
    Args:
        y_true (np.ndarray): Ground truth target values.
        y_pred (np.ndarray): Predicted target values.
        prefix (str, optional): Metric name prefix (e.g., 'throughput_' or 'latency_').
        
    Returns:
        Dict[str, float]: Computed metrics dictionary.
    """
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    r2 = float(r2_score(y_true, y_pred))
    
    # Safe MAPE (avoid division by zero)
    mask = y_true != 0
    if np.any(mask):
        mape = float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100.0)
    else:
        mape = float("nan")
        
    return {
        f"{prefix}mae": mae,
        f"{prefix}rmse": rmse,
        f"{prefix}r2": r2,
        f"{prefix}mape": mape,
    }
