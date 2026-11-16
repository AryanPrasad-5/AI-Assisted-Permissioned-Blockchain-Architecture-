"""Offline tests of the evidence integrity manifest. Synthetic files only."""

from __future__ import annotations

import hashlib

import pytest

from e1_launch_prep import evidence as ev


def make_tree(tmp_path):
    a, b = tmp_path / "series", tmp_path / "caliper"
    (a / "run").mkdir(parents=True)
    (b / "dispatch").mkdir(parents=True)
    (a / "series_summary.json").write_bytes(b'{"status": "X"}')
    (a / "run" / "facts.json").write_bytes(b"facts")
    (b / "dispatch" / "dispatch_measured.csv").write_bytes(b"1,2,3\n")
    return {"series": a, "caliper": b}


def test_manifest_is_deterministic_sorted_and_hashes_every_file(tmp_path):
    roots = make_tree(tmp_path)
    text = ev.build_manifest(roots)
    assert text == ev.build_manifest(roots) and text.endswith("\n") and "\r" not in text
    lines = text.splitlines()
    assert [ln.split("  ", 1)[1] for ln in lines] == ["caliper/dispatch/dispatch_measured.csv", "series/run/facts.json", "series/series_summary.json"]
    assert lines[1].split("  ")[0] == hashlib.sha256(b"facts").hexdigest()


def test_verify_detects_a_changed_a_missing_and_an_unlisted_file(tmp_path):
    roots = make_tree(tmp_path)
    text = ev.build_manifest(roots)
    assert ev.verify_manifest(text, roots)["ok"] is True
    (roots["series"] / "run" / "facts.json").write_bytes(b"tampered")
    (roots["caliper"] / "dispatch" / "dispatch_measured.csv").unlink()
    (roots["series"] / "extra.txt").write_bytes(b"new")
    res = ev.verify_manifest(text, roots)
    assert res["ok"] is False and res["mismatched"] == ["series/run/facts.json"] and res["missing"] == ["caliper/dispatch/dispatch_measured.csv"] and res["unlisted"] == ["series/extra.txt"]


def test_output_is_create_exclusive_and_bad_input_is_refused(tmp_path):
    roots = make_tree(tmp_path)
    out = tmp_path / "SHA256SUMS.txt"
    assert ev.main(["--root", f"series={roots['series']}", "--root", f"caliper={roots['caliper']}", "--out", str(out)]) == 0
    before = out.read_bytes()
    assert ev.main(["--root", f"series={roots['series']}", "--out", str(out)]) == 2 and out.read_bytes() == before
    assert ev.main(["--root", f"series={roots['series']}", "--root", f"series={roots['caliper']}", "--out", str(tmp_path / "x")]) == 2
    with pytest.raises(ev.EvidenceError):
        ev.walk_files(tmp_path / "does_not_exist")
    with pytest.raises(ev.EvidenceError):
        ev.build_manifest({"bad/label": roots["series"]})
