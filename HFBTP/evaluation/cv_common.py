"""Shared inputs for the construct-validity analysis. Read-only by construction.

Inputs (all opened for reading only):
  HFBTP   data/raw/hfbtp/HFBTP.csv (gitignored, checksum-pinned)
  MMBPD   FABRIC_MMBPD_CSV, default D:/Plan_research/mmbpd/MMBPD.csv
  Phase1C FABRIC_PHASE1C_DIR, default D:/Plan_research/hfbtp_probe/outputs
  Models  FABRIC_MODEL_DIR, default D:/Reseach_Model/models/T2_D0

Outputs go only to the directory passed to run_construct_validity.py.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
HFBTP_CSV = Path(os.environ.get("FABRIC_HFBTP_CSV", REPO_ROOT / "data/raw/hfbtp/HFBTP.csv"))
MMBPD_CSV = Path(os.environ.get("FABRIC_MMBPD_CSV", "D:/Plan_research/mmbpd/MMBPD.csv"))
PHASE1C_DIR = Path(os.environ.get("FABRIC_PHASE1C_DIR", "D:/Plan_research/hfbtp_probe/outputs"))
MODEL_DIR = Path(os.environ.get("FABRIC_MODEL_DIR", "D:/Reseach_Model/models/T2_D0"))

HFBTP_MD5 = "e9275c49a2d7dc1f2a4015b6d8b28482"
MMBPD_MD5 = "f46797802478e553b3135a2c332d397e"

SET = "Set Transaction Arrival Rate"
ACTUAL = "Actual Transaction Arrival Rate"
BLOCK = "Block Size"
ORDERERS = "Orderers"
TPUT = "Throughput"
AVG_LAT = "Avg Latency"
MIN_LAT = "Min Latency"
MAX_LAT = "Max Latency"
M_SET = "Set Send Rates"
M_SEND = "Send Rates"

MANIFESTS = {
    "T1": "split_manifest_T1_unseen_blocksize_bands.json",
    "T1b": "split_manifest_T1b_rightedge_blocksize.json",
    "T2D0": "split_manifest_T2_unseen_workloads.json",
    "T2D1": "split_manifest_T2_D1_unseen_workloads.json",
    "T2D2": "split_manifest_T2_D2_unseen_workloads.json",
    "T2D3": "split_manifest_T2_D3_unseen_workloads.json",
    "T3": "split_manifest_T3_orderer_transfer.json",
}
PARTITIONS = ["T1", "T1b", "T2D0", "T2D1", "T2D2", "T2D3", "T3", "HEAVY"]


def md5_of(path: Path) -> str:
    digest = hashlib.md5()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load(path: Path, md5: str) -> pd.DataFrame:
    found = md5_of(path)
    if found != md5:
        raise ValueError(f"{path}: checksum {found} does not match pin {md5}")
    frame = pd.read_csv(path)
    frame.columns = [column.strip() for column in frame.columns]
    return frame


def load_hfbtp() -> pd.DataFrame:
    return _load(HFBTP_CSV, HFBTP_MD5)


def load_mmbpd() -> pd.DataFrame:
    """Observed MMBPD columns only. Never pass this frame to the block cutter."""
    return _load(MMBPD_CSV, MMBPD_MD5)


def load_manifests() -> dict:
    return {name: json.loads((PHASE1C_DIR / file).read_text()) for name, file in MANIFESTS.items()}


def partition_keys(manifests: dict, name: str) -> pd.DataFrame:
    source = manifests["T2D0"]
    columns = source["config_key_columns"]
    keys = source["heavy_test_keys"] if name == "HEAVY" else manifests[name]["test_keys"]
    return pd.DataFrame(keys, columns=columns)


def load_phase1c_results() -> pd.DataFrame:
    return pd.read_csv(PHASE1C_DIR / "phase1c" / "test_results.csv")
