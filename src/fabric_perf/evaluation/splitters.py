from typing import Iterator, Tuple
import numpy as np
import pandas as pd
from sklearn.model_selection import GroupKFold, GroupShuffleSplit


def create_configuration_aware_split(
    df: pd.DataFrame,
    group_cols: list[str],
    test_size: float = 0.2,
    random_state: int = 42,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split dataset by configuration groups to ensure no parameter setting is shared between train and test.
    
    Args:
        df (pd.DataFrame): Input dataframe.
        group_cols (list[str]): Columns defining an experimental configuration cell.
        test_size (float): Fraction of groups reserved for testing.
        random_state (int): Seed for reproducibility.
        
    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: (train_df, test_df)
    """
    groups = df[group_cols].astype(str).agg("_".join, axis=1)
    gss = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=random_state)
    train_idx, test_idx = next(gss.split(df, groups=groups))
    return df.iloc[train_idx].copy(), df.iloc[test_idx].copy()
