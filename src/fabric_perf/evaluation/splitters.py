"""Leakage-aware split strategies for the HFBTP static configuration grid.

Implemented exactly as pre-registered in the approved evaluation protocol
(HFBTP_EVALUATION_PROTOCOL_FINAL.md, section 4). The band values below are
protocol constants, not tuning knobs:

  T1  — interior block-size transfer:   test band [210..305], val [610..705]
  T1b — right-edge block-size transfer: test band [710..800], val [610..705]
        (maximum trained block size therefore = 605)
  T2/D0 — primary draw of the headline unseen-workload experiment (the protocol's
        headline is the range across draws D0-D3; only D0 is implemented here):
        test {30,140}, val {150,160},
        training excludes the test/val levels, {150,155}, and all levels >=165
  T3  — orderer transfer: train orderers {3,7} (minus val levels {150,200}),
        val {150,200} @ orderers {3,7}, test orderers {5,9}

Design rule: no unit (block-size band, workload level or orderer value) spans
train, validation and test. Helper checks verify completeness (every band row
is in the test set), disjointness, and exact coverage of the input frame.

The constants are documented in docs/evaluation_protocol.md and were approved in
the research-planning workspace (D:\\Plan_research); they are not re-derived here.
"""

from typing import Dict, List, Optional, Tuple
import pandas as pd
# --- Protocol constants (approved ranges from the evaluation protocol) -------#

T1_TEST_BAND = (210, 305)
T1_VAL_BAND = (610, 705)
T1B_TEST_BAND = (710, 800)
T1B_VAL_BAND = (610, 705)

T2_D0_TEST_LEVELS = (30, 140)
T2_D0_VAL_LEVELS = (150, 160)
T2_D0_HEAVY_VAL_LEVELS = (150, 155)

T3_TRAIN_ORDERERS = (3, 7)
T3_TEST_ORDERERS = (5, 9)
T3_VAL_LEVELS = (150, 200)

WORKLOAD_COL = "Set Transaction Arrival Rate"
BLOCK_COL = "Block Size"
ORDERER_COL = "Orderers"


def _band_mask(df: pd.DataFrame, col: str, band: Tuple[float, float]) -> pd.Series:
    """Rows whose `col` value lies inside the inclusive band."""
    return (df[col] >= band[0]) & (df[col] <= band[1])


def _assert_disjoint(name: str, parts: List[pd.DataFrame]) -> None:
    for i in range(len(parts)):
        for j in range(i + 1, len(parts)):
            overlap = _row_overlap(parts[i], parts[j])
            if overlap:
                raise AssertionError(
                    f"Split integrity violation in {name}: {overlap} shared rows "
                    "between partitions."
                )


def _row_overlap(a: pd.DataFrame, b: pd.DataFrame) -> int:
    key_cols = [WORKLOAD_COL, BLOCK_COL, ORDERER_COL]
    ka = set(map(tuple, a[key_cols].itertuples(index=False)))
    kb = set(map(tuple, b[key_cols].itertuples(index=False)))
    return len(ka & kb)


def _report(
    name: str,
    train: pd.DataFrame,
    val: pd.DataFrame,
    test: pd.DataFrame,
) -> Dict[str, object]:
    return {
        "split": name,
        "train_rows": len(train),
        "val_rows": len(val),
        "test_rows": len(test),
        "total_rows": len(train) + len(val) + len(test),
        "train_max_block": int(train[BLOCK_COL].max()),
        "test_band": None,
    }


def block_size_band_split(
    df: pd.DataFrame,
    test_band: Tuple[float, float],
    val_band: Tuple[float, float],
    name: str = "block_size_band",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, object]]:
    """Pre-registered block-size holdout (T1/T1b protocol splits).

    Train = all rows whose block size is in NEITHER band; validation = val_band;
    test = test_band. Workload levels and orderer counts remain fully
    represented in test — this split makes NO unseen-workload and NO
    unseen-orderer claim.
    """
    test_mask = _band_mask(df, BLOCK_COL, test_band)
    val_mask = _band_mask(df, BLOCK_COL, val_band) & ~test_mask
    train_mask = ~(test_mask | val_mask)
    train, val, test = (
        df.loc[train_mask].copy(),
        df.loc[val_mask].copy(),
        df.loc[test_mask].copy(),
    )
    # Completeness: every test-band row must belong to test (and never train/val).
    n_test_rows_found = int(test_mask.sum())
    if len(test) != n_test_rows_found:
        raise AssertionError("Band rows did not map exactly to the test partition.")
    if (val_mask & test_mask).any():
        raise AssertionError("Validation and test band overlap for block-size split.")
    _assert_disjoint(name, [train, val, test])
    if len(train) + len(val) + len(test) != len(df):
        raise AssertionError("Partitions do not sum to full dataset (coverage failure).")
    info = {
        "split": name,
        "test_band": list(test_band),
        "val_band": list(val_band),
        "max_trained_block_size": int(train[BLOCK_COL].max()),
        "train_rows": len(train),
        "val_rows": len(val),
        "test_rows": len(test),
        "total_rows": len(df),
    }
    return train, val, test, info


def t1_interior_band_split(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, object]]:
    """T1: interior block-size transfer (test [210..305], val [610..705])."""
    return block_size_band_split(df, T1_TEST_BAND, T1_VAL_BAND, name="T1_interior")


def t1b_right_edge_band_split(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, object]]:
    """T1b: right-edge block-size transfer (test [710..800], val [610..705]).

    Maximum trained block size = 605 by protocol design, so there is a
    105-unit gap to the first test band row. Claim scope: right-edge
    block-size extrapolation WITHIN the measured grid.
    """
    return block_size_band_split(
        df, T1B_TEST_BAND, T1B_VAL_BAND, name="T1b_right_edge"
    )


def workload_level_split(
    df: pd.DataFrame,
    test_levels: Tuple[int, ...],
    val_levels: Tuple[int, ...],
    extra_excluded_levels: Tuple[int, ...],
    name: str = "workload_level",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, object]]:
    """Pre-registered unseen-workload split (T2 draw family, e.g. D0).

    Test rows have the test workload level; validation rows the val levels;
    training excludes the test and val levels AND the hand-picked extra
    excluded levels (e.g. {150,155} plus all levels >=165 for D0).
    """
    test_mask = df[WORKLOAD_COL].isin(test_levels)
    val_mask = df[WORKLOAD_COL].isin(val_levels) & ~test_mask
    excluded = set(test_levels) | set(val_levels) | set(extra_excluded_levels)
    # Protocol-aware exclusion: some levels are excluded from ALL partitions
    # (e.g. 155 and >=165 for D0), so train+val+test is deliberately < len(df).
    excluded_rows_mask = df[WORKLOAD_COL].isin(
        excluded - set(test_levels) - set(val_levels)
    ) & ~test_mask & ~val_mask
    train_mask = ~df[WORKLOAD_COL].isin(excluded)
    train, val, test = (
        df.loc[train_mask].copy(),
        df.loc[val_mask].copy(),
        df.loc[test_mask].copy(),
    )
    _assert_disjoint(name, [train, val, test])
    if (
        len(train) + len(val) + len(test) + int(excluded_rows_mask.sum())
        != len(df)
    ):
        raise AssertionError(
            "Partitions + protocol-excluded rows do not sum to full dataset."
        )
    info = {
        "split": name,
        "test_levels": list(test_levels),
        "val_levels": list(val_levels),
        "excluded_from_train": sorted(excluded),
        "excluded_from_all_partitions_levels": sorted(
            excluded - set(test_levels) - set(val_levels)
        ),
        "excluded_rows": int(excluded_rows_mask.sum()),
        "max_trained_workload_level": int(train[WORKLOAD_COL].max()),
        "train_rows": len(train),
        "val_rows": len(val),
        "test_rows": len(test),
        "total_rows": len(df),
    }
    return train, val, test, info


def t2_d0_workload_split(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, object]]:
    """T2 primary draw D0 (locked): test {30,140}, val {150,160}, training
    excludes test/val levels, {150,155}, and every level >=165."""
    df_lv = df[WORKLOAD_COL]
    heavy_mask = df_lv >= 165
    extra = set(df_lv.loc[heavy_mask].unique()) | {150, 155}
    return workload_level_split(
        df,
        test_levels=T2_D0_TEST_LEVELS,
        val_levels=T2_D0_VAL_LEVELS,
        extra_excluded_levels=tuple(extra),
        name="T2_D0_workload",
    )


def orderer_split(
    df: pd.DataFrame,
    train_orderers: Tuple[int, ...],
    test_orderers: Tuple[int, ...],
    val_levels: Tuple[int, ...],
    name: str = "orderer_transfer",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, object]]:
    """Pre-registered unseen-orderer transfer (T3 protocol split).

    Training keeps only the train orderer values and excludes the val
    workload levels (they are held out from train by design); validation is
    the val workload levels restricted to the train orderers; test is the
    test orderer values entirely.
    """
    train_mask = df[ORDERER_COL].isin(train_orderers) & ~df[WORKLOAD_COL].isin(
        val_levels
    )
    val_mask = df[ORDERER_COL].isin(train_orderers) & df[WORKLOAD_COL].isin(val_levels)
    test_mask = df[ORDERER_COL].isin(test_orderers)
    if (train_mask & val_mask).any() or (train_mask & test_mask).any():
        raise AssertionError("Orderer split partitions overlap on rows.")
    train, val, test = (
        df.loc[train_mask].copy(),
        df.loc[val_mask].copy(),
        df.loc[test_mask].copy(),
    )
    _assert_disjoint(name, [train, val, test])
    if len(train) + len(val) + len(test) != len(df):
        raise AssertionError("Partitions do not sum to full dataset (coverage failure).")
    info = {
        "split": name,
        "train_orderers": list(train_orderers),
        "test_orderers": list(test_orderers),
        "val_levels": list(val_levels),
        "train_rows": len(train),
        "val_rows": len(val),
        "test_rows": len(test),
        "total_rows": len(df),
    }
    return train, val, test, info


def t3_orderer_split(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, Dict[str, object]]:
    """T3: orderer transfer, train {3,7} minus val levels {150,200};
    val {150,200}@{3,7}; test orderers {5,9}."""
    return orderer_split(
        df,
        train_orderers=T3_TRAIN_ORDERERS,
        test_orderers=T3_TEST_ORDERERS,
        val_levels=T3_VAL_LEVELS,
        name="T3_orderer_transfer",
    )


def create_configuration_aware_split(
    df: pd.DataFrame,
    group_cols: List[str],
    test_size: float = 0.2,
    random_state: int = 42,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Generic group-aware shuffle split (GroupShuffleSplit) for exploratory
    validation only — NOT the approved HFBTP evaluation protocol.

    Kept for cross-checking experiments in notebooks; the pre-registered
    band/level splits above are the protocol.
    """
    from sklearn.model_selection import GroupShuffleSplit

    groups = df[group_cols].astype(str).agg("_".join, axis=1)
    gss = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=random_state)
    train_idx, test_idx = next(gss.split(df, groups=groups))
    return df.iloc[train_idx].copy(), df.iloc[test_idx].copy()
