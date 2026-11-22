"""Designated read-only channel-configuration inspection, A0s (2026-11-22). OFFLINE: text and stored files only.

    python scripts/e1/e1_a0s_config_inspect.py plan  --channel NAME --expect-json CELL.json --peer P --configtxlator C --out-dir DIR --orderer HOST:PORT \\
                                                     --orderer-tls-hostname H --orderer-ca CA [--label inspection]
    python scripts/e1/e1_a0s_config_inspect.py parse --channel NAME --expect-json CELL.json --block decoded_config.json --captured-utc ISO [--expected-sequence N] --out EVIDENCE.json
    python scripts/e1/e1_a0s_config_inspect.py compare --before A.json --after B.json
    python scripts/e1/e1_a0s_config_inspect.py inspect   (always refused)

``plan`` prints the exact read-only argument vectors (peer version, configtxlator version, peer channel fetch config, configtxlator proto_decode); every vector is validated
against an allow-list and no creating, joining, updating, signing, chaincode, commit, invoke or node verb can be built. ``parse`` reads a stored decoded CONFIG block. ``inspect``
(the live fetch) is refused: live inspection is disabled in this version and needs a separately authorized tool supplying a process runner.

Exit codes: 0 match / unchanged, 1 mismatch or changed, 2 invalid request or evidence (nothing created), 5 live inspection refused.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(REPO / "src"))

from fabric_perf.controllers import config_inspection as ci      # noqa: E402
from fabric_perf.controllers import prelive_integrity as pi       # noqa: E402

EXIT_REFUSED = 5


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "inspect":
        print("REFUSED: live inspection is disabled in this version (config_inspection.LIVE_INSPECTION_ENABLED is False). Nothing was contacted.")
        return EXIT_REFUSED
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    p = sub.add_parser("plan")
    for name in ("channel", "expect-json", "peer", "configtxlator", "out-dir", "orderer", "orderer-tls-hostname", "orderer-ca"):
        p.add_argument(f"--{name}", required=True)
    p.add_argument("--label", default="inspection")
    q = sub.add_parser("parse")
    for name in ("channel", "expect-json", "block", "captured-utc", "out"):
        q.add_argument(f"--{name}", required=True)
    q.add_argument("--expected-sequence", type=int)
    c = sub.add_parser("compare")
    c.add_argument("--before", required=True, type=Path)
    c.add_argument("--after", required=True, type=Path)
    sub.add_parser("inspect")
    args = parser.parse_args(argv)
    try:
        if args.phase == "plan":
            expected = json.loads(Path(args.expect_json).read_text(encoding="utf-8"))
            plan = ci.build_inspection_plan(args.channel, expected, peer=args.peer, configtxlator=args.configtxlator, out_dir=args.out_dir, orderer=args.orderer,
                                            orderer_tls_hostname=args.orderer_tls_hostname, orderer_ca=args.orderer_ca, label=args.label)
            print(json.dumps(plan, indent=2))
            return 0
        if args.phase == "parse":
            out = Path(args.out)
            if out.exists():
                print(f"REFUSED: {out} already exists")
                return 2
            expected = json.loads(Path(args.expect_json).read_text(encoding="utf-8"))
            block, digest = ci.load_decoded_block(Path(args.block))
            evidence = ci.inspect_decoded_block(block, channel=args.channel, expected=expected, captured_utc=args.captured_utc, decoded_sha256=digest,
                                                expected_sequence=args.expected_sequence)
            pi.write_new_json(out, evidence)
            print(f"{evidence['verdict']}: channel {evidence['channel']} block {evidence['config_block_number']} sequence {evidence['config_sequence']}; differing {evidence['differing_fields']}")
            return 0 if evidence["verdict"] == "MATCH" else 1
        result = ci.compare_inspections(json.loads(args.before.read_text(encoding="utf-8")), json.loads(args.after.read_text(encoding="utf-8")))
        print(json.dumps(result, indent=2))
        return 0 if result["unchanged"] else 1
    except (ci.InspectionError, ci.MutationRefused) as exc:
        print(f"INVALID: {getattr(exc, 'code', 'refused')}: {exc}")
        return 2
    except (OSError, ValueError) as exc:
        print(f"INVALID REQUEST OR UNREADABLE INPUT: {exc}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
