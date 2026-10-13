import os
import random
import numpy as np


def seed_everything(seed: int = 42) -> int:
    """Set random seed across Python standard library, OS environment, and NumPy.
    
    Args:
        seed (int): Integer seed value. Default is 42.
        
    Returns:
        int: The set seed value.
    """
    random.seed(seed)
    os.environ["PYTHONHASHSEED"] = str(seed)
    np.random.seed(seed)
    return seed
