"""Full streaming audit of the two registered Flashbots Mempool Dumpster archives.

Reads every field of every row as raw text (no extraction, no modification of the archives) and writes
machine-readable audit outputs to Flashbots/data_audit/. Nothing is dropped, imputed or repaired: the
audit only counts.

    .venv/Scripts/python.exe -B Flashbots/data_audit/audit_raw_archives.py [--force]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fbcommon as fc  # noqa: E402

OUT = fc.FB / "data_audit"
QUANTILES = (0.0, 0.001, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 0.999, 1.0)
LOW_CARDINALITY = ("chain_id", "tx_type", "sources")
GWEI = 1e9


def audit_day(date: str, entry: dict) -> tuple:
    started = time.time()
    sha = fc.verify_archive(entry)
    path = Path(entry["source_path"])
    rows = 0
    header = None
    nulls = {c: 0 for c in fc.EXPECTED_COLUMNS}
    parse_fail = {c: 0 for c in fc.NUMERIC_COLUMNS}
    non_integer = {c: 0 for c in fc.NUMERIC_COLUMNS if c not in ("value",)}
    low_card = {c: {} for c in LOW_CARDINALITY}
    numeric = {c: [] for c in fc.NUMERIC_COLUMNS}
    hashes, sender_nonce, bad_hash_format = [], [], 0
    selector_mismatch = 0  # data_4bytes null although calldata has >= 4 bytes, or present although < 4 bytes
    for chunk in fc.iter_raw_chunks(path, as_text=True):
        if header is None:
            header = list(chunk.columns)
        rows += len(chunk)
        for column in fc.EXPECTED_COLUMNS:
            nulls[column] += int(chunk[column].isna().sum())
        for column in LOW_CARDINALITY:
            for key, count in chunk[column].fillna("<null>").value_counts().items():
                low_card[column][key] = low_card[column].get(key, 0) + int(count)
        for column in fc.NUMERIC_COLUMNS:
            raw = chunk[column]
            parsed = pd.to_numeric(raw, errors="coerce")
            parse_fail[column] += int((parsed.isna() & raw.notna()).sum())
            if column in non_integer:
                non_integer[column] += int(((parsed % 1) != 0).sum())
            numeric[column].append(parsed.to_numpy(dtype=float))
        size = pd.to_numeric(chunk["data_size"], errors="coerce")
        selector_mismatch += int(((chunk["data_4bytes"].isna()) & (size >= 4)).sum() + ((chunk["data_4bytes"].notna()) & (size < 4)).sum())
        h = chunk["hash"].fillna("")
        bad_hash_format += int((~h.str.fullmatch(r"0x[0-9a-fA-F]{64}")).sum())
        hashes.append(h.to_numpy(dtype="S66"))
        sender_nonce.append((chunk["from"].fillna("") + ":" + chunk["nonce"].fillna("")).to_numpy(dtype="S64"))
    arrays = {c: np.concatenate(v) for c, v in numeric.items()}
    hash_arr = np.concatenate(hashes)
    sn_arr = np.concatenate(sender_nonce)

    # ---------------- identity / duplicates
    _, hash_counts = np.unique(hash_arr, return_counts=True)
    _, sn_counts = np.unique(sn_arr, return_counts=True)
    # ---------------- timestamps
    ts = arrays["timestamp_ms"]
    start_ms = fc.day_start_ms(date)
    offsets = ts - start_ms
    gaps = np.diff(ts)
    per_second = np.bincount((offsets[(offsets >= 0) & (offsets < fc.DAY_MS)] // 1000).astype(np.int64), minlength=fc.DAY_S)
    zero_seconds = per_second == 0
    run_lengths = _runs(zero_seconds)
    order = np.argsort(gaps)[::-1][:10]
    top_gaps = [{"date_utc": date, "rank": i + 1, "gap_ms": float(gaps[k]),
                 "from_utc": _iso(ts[k]), "to_utc": _iso(ts[k + 1])} for i, k in enumerate(order)]
    # ---------------- consistency
    incl_ts, incl_h, delay = arrays["included_block_timestamp_ms"], arrays["included_at_block_height"], arrays["inclusion_delay_ms"]
    included = incl_ts > 0
    tx_type = arrays["tx_type"]
    checks = [
        ("columns_match_registered_schema", int(header != fc.EXPECTED_COLUMNS), 1, "header equals the 18 registered columns, in order"),
        ("hash_format_0x_64hex", bad_hash_format, rows, "tx hash is 0x + 64 hex chars"),
        ("duplicate_tx_hash_rows", int((hash_counts - 1).sum()), rows, "rows whose hash already appeared"),
        ("timestamp_outside_utc_day", int(((offsets < 0) | (offsets >= fc.DAY_MS)).sum()), rows, "timestamp_ms outside the file's UTC date"),
        ("timestamp_decreases", int((gaps < 0).sum()), rows - 1, "file order is chronological"),
        ("timestamp_exact_ties", int((gaps == 0).sum()), rows - 1, "consecutive rows with identical timestamp_ms (kept)"),
        ("chain_id_not_1", int((arrays["chain_id"] != 1).sum()), rows, "Ethereum mainnet only"),
        ("not_included_rows", int((~included).sum()), rows, "included_block_timestamp_ms == 0 (never landed); README: 0 = not included"),
        ("inclusion_delay_formula_mismatch", int((included & (np.abs(delay - (incl_ts - ts)) > 0)).sum()), int(included.sum()),
         "inclusion_delay_ms == included_block_timestamp_ms - timestamp_ms (README FAQ formula)"),
        ("negative_inclusion_delay", int((included & (delay < 0)).sum()), int(included.sum()), "block timestamp earlier than reception (allowed by README)"),
        ("inclusion_delay_over_1h", int((included & (delay > 3_600_000)).sum()), int(included.sum()), "landed more than 1 h after first seen"),
        ("tip_cap_gt_fee_cap", int((arrays["gas_tip_cap"] > arrays["gas_fee_cap"]).sum()), rows, "EIP-1559 requires tip cap <= fee cap"),
        ("legacy_type0_price_ne_feecap", int(((tx_type == 0) & (arrays["gas_price"] != arrays["gas_fee_cap"])).sum()), int((tx_type == 0).sum()),
         "for type-0 txs the client reports gas_price as both caps"),
        ("gas_limit_below_21000", int((arrays["gas"] < 21_000).sum()), rows, "intrinsic minimum gas for any tx"),
        ("gas_limit_above_block_limit_45M", int((arrays["gas"] > 45_000_000).sum()), rows, "cannot fit in one mainnet block (45M limit assumed, HYPOTHESIS)"),
        ("zero_gas_fee_cap", int((arrays["gas_fee_cap"] == 0).sum()), rows, "zero-fee transactions"),
        ("selector_vs_calldata_mismatch", selector_mismatch, rows, "data_4bytes present iff data_size >= 4"),
        ("empty_calldata_rows", int((arrays["data_size"] == 0).sum()), rows, "informational: plain transfers (no calldata)"),
    ]
    consistency = pd.DataFrame([{"date_utc": date, "check": c, "failing_rows": f, "rows_checked": n,
                                 "failing_share": (f / n if n else None), "definition": d} for c, f, n, d in checks])

    # ---------------- per-column profile
    profile = []
    for column in fc.EXPECTED_COLUMNS:
        row = {"date_utc": date, "column": column, "rows": rows, "nulls": nulls[column], "null_share": nulls[column] / rows,
               "role": _role(column)}
        if column in fc.NUMERIC_COLUMNS:
            values = arrays[column]
            finite = values[np.isfinite(values)]
            row.update({"inferred_type": "integer" if non_integer.get(column, 1) == 0 else "decimal",
                        "parse_failures": parse_fail[column]})
            if finite.size:
                qs = np.quantile(finite, QUANTILES)
                row.update({f"q{q:g}": v for q, v in zip(QUANTILES, qs)})
                row["mean"] = float(finite.mean())
        else:
            row["inferred_type"] = "text"
        if column in LOW_CARDINALITY:
            row["distinct_values"] = len(low_card[column])
        profile.append(row)
    profile = pd.DataFrame(profile)

    # ---------------- outliers (counted, never removed)
    gwei_price = arrays["gas_fee_cap"] / GWEI
    outliers = {
        "per_second_count_gt_p99_9": int((per_second > np.quantile(per_second, 0.999)).sum()),
        "per_second_count_max": int(per_second.max()),
        "per_second_count_max_at_utc": _iso(start_ms + int(per_second.argmax()) * 1000),
        "gas_fee_cap_gwei_gt_1000": int((gwei_price > 1000).sum()),
        "gas_fee_cap_gwei_max": float(gwei_price.max()),
        "gas_limit_ge_10M": int((arrays["gas"] >= 10_000_000).sum()),
        "data_size_gt_100kB": int((arrays["data_size"] > 100_000).sum()),
        "robust_z_gt_10_per_second_counts": int((np.abs(per_second - np.median(per_second))
                                                 / (1.4826 * _mad(per_second)) > 10).sum()),
    }

    report = {
        "date_utc": date, "archive": str(path), "archive_sha256_verified": sha, "csv_entry": fc.csv_entry(path),
        "compressed_bytes": path.stat().st_size, "rows": rows, "expected_rows_manifest": entry["rows"],
        "rows_match_manifest": rows == entry["rows"], "columns": header, "schema_matches_registered": header == fc.EXPECTED_COLUMNS,
        "null_counts": nulls, "numeric_parse_failures": parse_fail,
        "low_cardinality_value_counts": low_card,
        "duplicates": {"tx_hash_duplicate_rows": int((hash_counts - 1).sum()), "unique_tx_hashes": int(hash_counts.size),
                       "sender_nonce_pairs_with_multiple_hashes": int((sn_counts > 1).sum()),
                       "rows_in_repeated_sender_nonce_pairs": int(sn_counts[sn_counts > 1].sum()),
                       "note": "repeated (from, nonce) with distinct hashes are replacement / re-priced transactions; they are kept as separate arrivals"},
        "timestamps": {"timezone": "UTC (Unix epoch milliseconds)", "first_utc": _iso(ts.min()), "last_utc": _iso(ts.max()),
                       "span_s": float((ts.max() - ts.min()) / 1000), "decreases": int((gaps < 0).sum()), "exact_ties": int((gaps == 0).sum()),
                       "interarrival_ms_quantiles": dict(zip([f"q{q:g}" for q in QUANTILES], np.quantile(gaps, QUANTILES).tolist())),
                       "seconds_in_day": fc.DAY_S, "seconds_with_zero_arrivals": int(zero_seconds.sum()),
                       "longest_zero_arrival_run_s": int(run_lengths.max()) if run_lengths.size else 0,
                       "zero_arrival_runs_ge_3s": int((run_lengths >= 3).sum()),
                       "max_interarrival_ms": float(gaps.max())},
        "inclusion": {"included_rows": int(included.sum()), "not_included_rows": int((~included).sum()),
                      "included_block_height_min": float(incl_h[included].min()), "included_block_height_max": float(incl_h[included].max()),
                      "inclusion_delay_ms_quantiles_included": dict(zip([f"q{q:g}" for q in QUANTILES],
                                                                       np.quantile(delay[included], QUANTILES).tolist()))},
        "outliers_counted_not_removed": outliers,
        "audit_seconds": round(time.time() - started, 1),
    }
    keep = {"hash": hash_arr}
    return report, profile, consistency, keep, top_gaps


def _role(column: str) -> str:
    if column == "timestamp_ms":
        return "reception timestamp (arrival time) - forecasting basis"
    if column in fc.POST_INCLUSION_COLUMNS:
        return "post-inclusion (known only after landing) - excluded from forecasting inputs"
    if column in ("hash", "from", "to", "nonce", "data_4bytes"):
        return "identity / descriptive"
    if column in ("gas", "gas_price", "gas_tip_cap", "gas_fee_cap", "data_size", "value"):
        return "transaction attribute (known at reception)"
    return "metadata"


def _runs(mask: np.ndarray) -> np.ndarray:
    if not mask.any():
        return np.array([], dtype=int)
    padded = np.concatenate([[0], mask.astype(int), [0]])
    edges = np.diff(padded)
    return np.flatnonzero(edges == -1) - np.flatnonzero(edges == 1)


def _mad(values: np.ndarray) -> float:
    return float(np.median(np.abs(values - np.median(values)))) or 1.0


def _iso(ms: float) -> str:
    return pd.Timestamp(int(ms), unit="ms", tz="UTC").isoformat()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    outputs = [OUT / n for n in ("audit_report.json", "column_profile.csv", "consistency_checks.csv", "longest_gaps.csv")]
    fc.refuse_overwrite(outputs, args.force)
    manifest = fc.load_manifest()
    reports, profiles, checks, hashes, gaps = [], [], [], {}, []
    for date in fc.DATES:
        report, profile, consistency, keep, top = audit_day(date, manifest[date])
        reports.append(report)
        profiles.append(profile)
        checks.append(consistency)
        hashes[date] = keep["hash"]
        gaps.extend(top)
        print(f"{date}: rows={report['rows']} schema_ok={report['schema_matches_registered']} "
              f"dup_hash={report['duplicates']['tx_hash_duplicate_rows']} ({report['audit_seconds']} s)")
    cross_day = np.intersect1d(hashes[fc.DATES[0]], hashes[fc.DATES[1]]).size
    stored = fc.STORED_ARCHIVE_AUDIT
    combined = {
        "rows": sum(r["rows"] for r in reports),
        "schemas_identical": reports[0]["columns"] == reports[1]["columns"],
        "tx_hashes_present_on_both_dates": int(cross_day),
        "dates_contiguous": False,
        "note": "The two files are separate UTC days three weeks apart; they are never joined into one continuous series.",
    }
    payload = {"generated_utc": fc.utc_now(), "script": "Flashbots/data_audit/audit_raw_archives.py",
               "manifest": str(fc.MANIFEST), "reconciled_with_stored_audit": str(stored),
               "reconciliation": _reconcile(reports, stored), "combined": combined, "days": reports,
               "environment": fc.environment()}
    fc.write_json(OUT / "audit_report.json", payload)
    pd.concat(profiles).to_csv(OUT / "column_profile.csv", index=False)
    pd.concat(checks).to_csv(OUT / "consistency_checks.csv", index=False)
    pd.DataFrame(gaps).to_csv(OUT / "longest_gaps.csv", index=False)
    print(f"combined rows={combined['rows']} cross-day hashes={cross_day}; wrote {OUT}")


def _reconcile(reports: list[dict], stored_path: Path) -> list[dict]:
    """Compare this independent audit with the stored archive audit (experiments/results/flashbots_archive_audit.json)."""
    if not stored_path.exists():
        return [{"status": "stored audit not found"}]
    import json
    stored = {Path(a["archive"]).name: a for a in json.loads(stored_path.read_text(encoding="utf-8"))["archives"]}
    out = []
    for report in reports:
        old = stored.get(Path(report["archive"]).name, {})
        for key, new_value, old_value in (
            ("rows", report["rows"], old.get("rows")),
            ("sha256", report["archive_sha256_verified"], old.get("sha256")),
            ("duplicate_tx_hashes", report["duplicates"]["tx_hash_duplicate_rows"], old.get("duplicate_transaction_hashes")),
            ("timestamp_decreases", report["timestamps"]["decreases"], old.get("chronological_decreases")),
            ("exact_tie_gaps", report["timestamps"]["exact_ties"], (old.get("interarrival_ms") or {}).get("nonpositive")),
            ("null_counts", report["null_counts"], old.get("null_counts")),
        ):
            out.append({"date_utc": report["date_utc"], "field": key, "this_audit": new_value, "stored_audit": old_value,
                        "match": new_value == old_value})
    return out


if __name__ == "__main__":
    main()
