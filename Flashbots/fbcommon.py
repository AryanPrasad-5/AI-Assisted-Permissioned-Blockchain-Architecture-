"""Shared helpers for the Flashbots research pipeline (paths, hashing, raw-archive streaming, plot style).

Every script in Flashbots/ imports this module. It only reads the registered raw archives; it never
writes outside Flashbots/.
"""

from __future__ import annotations

import hashlib
import json
import platform
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import numpy as np
import pandas as pd
import yaml

FB = Path(__file__).resolve().parent
REPO = FB.parent
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))

MANIFEST = REPO / "data" / "manifests" / "flashbots_mempool_manifest.yaml"
STORED_SERIES_DIR = REPO / "data" / "processed" / "flashbots"  # the six stored UTC series (read-only here)
STORED_ARCHIVE_AUDIT = REPO / "experiments" / "results" / "flashbots_archive_audit.json"
P2_DIR = REPO / "experiments" / "results" / "flashbots_horizon_benchmark_v2"  # frozen P2 benchmark (read-only)

DERIVED_DIR = FB / "data" / "derived"
TS_DIR = FB / "time_series"
VIZ_DIR = FB / "visualizations"

DATES = ("2026-09-21", "2026-10-12")
DAY_S = 86_400
DAY_MS = 86_400_000

EXPECTED_COLUMNS = [
    "timestamp_ms", "hash", "chain_id", "from", "to", "value", "nonce", "gas", "gas_price",
    "gas_tip_cap", "gas_fee_cap", "data_size", "data_4bytes", "sources", "included_at_block_height",
    "included_block_timestamp_ms", "inclusion_delay_ms", "tx_type",
]
NUMERIC_COLUMNS = [
    "timestamp_ms", "chain_id", "value", "nonce", "gas", "gas_price", "gas_tip_cap", "gas_fee_cap",
    "data_size", "included_at_block_height", "included_block_timestamp_ms", "inclusion_delay_ms", "tx_type",
]
# Fields known only after the transaction is included on-chain. They must never feed an arrival forecast.
POST_INCLUSION_COLUMNS = ["included_at_block_height", "included_block_timestamp_ms", "inclusion_delay_ms"]


def sha256_file(path: str | Path, chunk: int = 1 << 22) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(chunk), b""):
            digest.update(block)
    return digest.hexdigest().upper()


def load_manifest(path: Path = MANIFEST) -> dict[str, dict]:
    """date -> manifest entry for each registered raw archive."""
    manifest = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return {entry["archive_entry"].removesuffix(".csv"): entry for entry in manifest["raw_archives"]}


def verify_archive(entry: dict) -> str:
    """Refuse to read an archive whose SHA-256 differs from the manifest."""
    path = Path(entry["source_path"])
    if not path.exists():
        raise FileNotFoundError(f"registered archive missing: {path}")
    actual = sha256_file(path)
    if actual != entry["sha256"].upper():
        raise ValueError(f"checksum mismatch for {path}: {actual} != {entry['sha256']}")
    return actual


def csv_entry(archive_path: Path) -> str:
    with zipfile.ZipFile(archive_path) as archive:
        entries = [i.filename for i in archive.infolist() if not i.is_dir() and i.filename.lower().endswith(".csv")]
    if len(entries) != 1:
        raise ValueError(f"expected exactly one CSV in {archive_path}, found {entries}")
    return entries[0]


def iter_raw_chunks(archive_path: str | Path, usecols: list[str] | None = None, as_text: bool = False,
                    chunk_rows: int = 200_000) -> Iterator[pd.DataFrame]:
    """Stream the CSV inside a ZIP without extracting it. as_text=True keeps every field as the raw string
    (empty field -> NaN) so the audit can count parse failures itself."""
    archive_path = Path(archive_path)
    entry = csv_entry(archive_path)
    kwargs = {"dtype": str} if as_text else {"low_memory": False}
    with zipfile.ZipFile(archive_path) as archive, archive.open(entry) as raw:
        yield from pd.read_csv(raw, chunksize=chunk_rows, encoding="utf-8-sig", usecols=usecols, **kwargs)


def day_start_ms(date: str) -> int:
    return int(pd.Timestamp(date, tz="UTC").timestamp() * 1000)


def load_extract(date: str) -> dict[str, np.ndarray]:
    """Compact numeric per-transaction extract written by preprocessing/extract_transactions.py."""
    path = DERIVED_DIR / f"tx_extract_{date}.npz"
    if not path.exists():
        raise FileNotFoundError(f"{path} missing: run Flashbots/preprocessing/extract_transactions.py first")
    with np.load(path) as data:
        return {key: data[key] for key in data.files}


def load_target_series(date: str) -> pd.DataFrame:
    path = TS_DIR / f"target_series_{date}_1s.csv.gz"
    if not path.exists():
        raise FileNotFoundError(f"{path} missing: run Flashbots/time_series/build_target_series.py first")
    return pd.read_csv(path)


def environment() -> dict:
    import matplotlib
    import scipy
    import sklearn
    return {"python": platform.python_version(), "platform": platform.platform(), "numpy": np.__version__,
            "pandas": pd.__version__, "scipy": scipy.__version__, "scikit_learn": sklearn.__version__,
            "matplotlib": matplotlib.__version__, "pyyaml": yaml.__version__}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_json(path: Path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=_json_default) + "\n", encoding="utf-8")


def _json_default(value):
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"not JSON serialisable: {type(value)}")


def refuse_overwrite(paths: list[Path], force: bool) -> None:
    existing = [p for p in paths if p.exists()]
    if existing and not force:
        raise FileExistsError("refusing to overwrite existing outputs (pass --force to replace): "
                              + ", ".join(str(p) for p in existing))


# ---------------------------------------------------------------- plotting (reference palette, light mode)
DAY_COLOR = {"2026-09-21": "#2a78d6", "2026-10-12": "#eb6834"}
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
TEXT, TEXT2, GRID, SURFACE = "#0b0b0b", "#52514e", "#e4e3df", "#fcfcfb"


def plot_style() -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({
        "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
        "axes.edgecolor": TEXT2, "axes.labelcolor": TEXT, "xtick.color": TEXT2, "ytick.color": TEXT2,
        "text.color": TEXT, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
        "axes.spines.top": False, "axes.spines.right": False, "lines.linewidth": 1.6,
        "font.size": 10, "axes.titlesize": 11.5, "axes.titleweight": "bold", "legend.frameon": False,
        "figure.dpi": 110, "savefig.dpi": 150, "savefig.bbox": "tight",
    })


def save_figure(fig, name: str) -> Path:
    VIZ_DIR.mkdir(parents=True, exist_ok=True)
    path = VIZ_DIR / name
    fig.savefig(path)
    import matplotlib.pyplot as plt
    plt.close(fig)
    return path
