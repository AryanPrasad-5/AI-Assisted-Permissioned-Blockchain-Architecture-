import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "HFBTP" / "evaluation"))

import cv_common as cc  # noqa: E402


@pytest.fixture(scope="session")
def hfbtp():
    if not cc.HFBTP_CSV.exists():
        pytest.skip("HFBTP.csv not available")
    return cc.load_hfbtp()


@pytest.fixture(scope="session")
def mmbpd():
    if not cc.MMBPD_CSV.exists():
        pytest.skip("MMBPD.csv not available")
    return cc.load_mmbpd()


@pytest.fixture(scope="session")
def manifests():
    if not cc.PHASE1C_DIR.exists():
        pytest.skip("Phase 1C outputs not available")
    return cc.load_manifests()
