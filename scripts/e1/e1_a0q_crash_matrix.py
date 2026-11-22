"""Generator-route bridge crash matrix (task A0q, 2026-11-22): run every scripted process death against the uninterrupted baseline and write the outcomes. OFFLINE, SYNTHETIC.

Each case kills (or fails a write of) a ``RecoverableSourceCountReader`` at the n-th occurrence of a named boundary, rebuilds it from the files and compares the final journal, the
attempts with their original read times, the observed segments and the evidence and coverage at 27 cutoffs with the uninterrupted run. Scripted generator, scripted read times, a scratch
temporary directory; no process, network, container, Caliper, Fabric or model is used. The result file must not exist.

    python scripts/e1/e1_a0q_crash_matrix.py --out docs/research/e1/a0q_example_artifacts_2026-11-22/crash_matrix_results.json
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
for entry in (str(REPO), str(REPO / "src")):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from tests import a0q_support as q   # noqa: E402

VERSION = "a0q_crash_matrix 0.1 (2026-11-22)"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.out.exists():
        print(f"REFUSED: {args.out} exists; evidence is never overwritten")
        return 2
    with tempfile.TemporaryDirectory(prefix="a0q_matrix_") as scratch:
        result = q.run_matrix(Path(scratch))
    doc = {"schema": "a0q_crash_matrix_results/1", "tool": VERSION, "evidence_origin": "synthetic_fixture", "live_run": False,
           "boundaries": q.BOUNDARIES,
           "notes": ["every IDENTICAL case: final journal bytes, attempts with original read times, segments and per-cutoff evidence/coverage equal the uninterrupted run",
                     "the interrupted poll is re-issued at its SCHEDULED read time (synthetic schedule); a later restart time is tested separately and is not byte-comparable",
                     "REFUSED: recovery raised a coded refusal and wrote nothing; the journal is left as the crash left it",
                     "NOT_REACHED: the script never produces that n-th occurrence",
                     "not claimed: power loss, a storage device that misreports a flush, a second concurrent bridge, a real Caliper worker"],
           **result}
    with args.out.open("x", encoding="utf-8") as handle:
        json.dump(doc, handle, indent=2, sort_keys=True)
        handle.write("\n")
    print(f"crash matrix -> {args.out}: {result['counts']}")
    return 0 if result["counts"].get("DIFFERENT", 0) == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
