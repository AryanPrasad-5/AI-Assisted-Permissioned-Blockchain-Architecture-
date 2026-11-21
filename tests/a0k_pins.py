"""Pin tolerance for the A0k successor manifest (2026-11-21). Standard library only, so the older integrity tests can import it without pulling in the code.

A0k had to edit eight files that the A0i and A0j candidate manifests list (all ``PENDING_OWNER_APPROVAL`` with no pin: candidate files, not frozen ones). Those
manifests are left byte-identical, so they still record what the eight files were. The rule, one level up from the A0h and A0i rules before it: a file may differ
from its entry in an OLDER manifest ONLY if the A0k manifest names it in ``changed_since_a0j`` AND the file on disk matches the A0k entry. Any other difference
still fails, and the set of tolerated files is itself pinned here so a ninth file cannot slip in.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
A0K_MANIFEST = REPO / "docs" / "research" / "e1" / "prelive_candidate_manifest_2026-11-21_a0k.json"
A0K_CHANGED = {
    "src/fabric_perf/controllers/workload_coverage.py", "src/fabric_perf/controllers/collector_health.py", "src/fabric_perf/controllers/captured_history.py",
    "src/fabric_perf/controllers/recorder_coverage.py", "src/fabric_perf/controllers/recorder_health_bridge.py", "scripts/e1/e1_a0i_coverage_demo.py",
    "scripts/e1/e1_a0j_recorder_health_demo.py", "scripts/e1/caliper/e1_per_tx_recorder_v3.js",
}
A0K_ADDED = {"src/fabric_perf/controllers/source_completeness.py", "src/fabric_perf/controllers/worker_coverage.py"}


def sha(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 22), b""):
            digest.update(chunk)
    return digest.hexdigest()


def a0k_manifest() -> dict | None:
    return json.loads(A0K_MANIFEST.read_text(encoding="utf-8")) if A0K_MANIFEST.is_file() else None


def changed_by_a0k() -> set:
    """The files A0k changed, each checked against its A0k entry. Empty when the A0k manifest does not exist yet."""
    manifest = a0k_manifest()
    if manifest is None:
        return set()
    current = {e["path"]: e["sha256"] for e in manifest["entries"]}
    paths = set(manifest["changed_since_a0j"])
    assert paths == A0K_CHANGED, sorted(paths ^ A0K_CHANGED)            # no other candidate file may have changed
    for path in paths:
        assert sha(REPO / path) == current[path], path                    # and each one must match its A0k entry
    return paths


def a0k_entry_paths() -> set:
    manifest = a0k_manifest()
    return set() if manifest is None else {e["path"] for e in manifest["entries"]}


def tolerated_in(entries) -> set:
    """The changed files that a given older manifest's ``entries`` actually list."""
    listed = {e["path"] for e in entries}
    return changed_by_a0k() & listed
