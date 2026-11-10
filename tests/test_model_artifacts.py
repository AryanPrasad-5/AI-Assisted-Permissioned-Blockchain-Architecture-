from pathlib import Path

import pandas as pd

from fabric_perf.models.artifacts import predict_hfbtp, train_and_save_hfbtp


REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "data" / "raw" / "hfbtp"


def test_hfbtp_artifacts_round_trip(tmp_path):
    metadata = train_and_save_hfbtp(DATA, tmp_path)
    assert metadata["rows"] == 24687
    assert metadata["dataset_sha256"]
    sample = pd.read_csv(DATA / "HFBTP.csv").iloc[[0, 100, -1]]
    predictions = predict_hfbtp(tmp_path, sample)
    assert list(predictions.columns) == ["Throughput", "Avg Latency"]
    assert predictions.shape == (3, 2)
    assert predictions.notna().all().all()


def test_artifact_inference_rejects_missing_safe_feature(tmp_path):
    train_and_save_hfbtp(DATA, tmp_path)
    sample = pd.read_csv(DATA / "HFBTP.csv").drop(columns=["Orderers"]).iloc[:1]
    try:
        predict_hfbtp(tmp_path, sample)
    except KeyError:
        return
    raise AssertionError("missing decision-time feature was not rejected")
