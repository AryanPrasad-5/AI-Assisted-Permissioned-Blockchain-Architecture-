"""verify_x1.py is closed_loop_v2/ops/verify_v2.py with EXACTLY the substitutions below (each must match once). Run as
    python -m Fabric_Live_Testbed.closed_loop_x1.derive_verify <verify_v2.py> <verify_x1.py>
The test suite re-derives the file and compares bytes, so the independent verifier of X1 differs from the X0 one only in the menu and
the output root. verify_v2.py itself is never edited."""

from __future__ import annotations

import sys
from pathlib import Path

SUBSTITUTIONS = (
    ('"""v2 (2026-11-25). Derived from',
     '"""X1 (2026-11-25): closed_loop_v2/ops/verify_v2.py with the configuration menu widened to {2,5,10} and the output root from X1_OUT. '
     'verify_v2 was derived from'),
    ('OUT = Path(os.environ.get("LPC_OUT", "/home/mailf18/e1/lpc-20261126"))',
     'OUT = Path(os.environ.get("X1_OUT", "/home/mailf18/e1/x1-block-20261126"))'),
    ('check("every config B in menu {10,40}", all(d["B"] in (10, 40) for d in dec.values()))',
     'check("every config B in menu {2,5,10}", all(d["B"] in (2, 5, 10) for d in dec.values()))'),
    ('check("every decision target in menu", all(d["target_B"] in (10, 40) for d in decs))',
     'check("every decision target in menu", all(d["target_B"] in (2, 5, 10) for d in decs))'),
)


def derive(text: str) -> str:
    for old, new in SUBSTITUTIONS:
        if text.count(old) != 1:
            raise ValueError(f"substitution does not match exactly once: {old[:60]!r} ({text.count(old)} matches)")
        text = text.replace(old, new)
    return text


def main(argv=None) -> int:
    src, dst = Path(sys.argv[1]), Path(sys.argv[2])
    dst.write_bytes(derive(src.read_bytes().decode("utf-8")).encode("utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
