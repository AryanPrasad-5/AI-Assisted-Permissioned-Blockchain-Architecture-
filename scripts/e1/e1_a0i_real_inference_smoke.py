"""A0i real-inference smoke test: specification, input verification and a DISABLED run command (2026-11-21).

    python scripts/e1/e1_a0i_real_inference_smoke.py spec [--out NEW_FILE]
    python scripts/e1/e1_a0i_real_inference_smoke.py verify-inputs           # hashes the pinned inputs as bytes; opens no model
    python scripts/e1/e1_a0i_real_inference_smoke.py run --out-dir NEW_DIR --orderers N     # REFUSED (exit 6) until the owner authorizes

``run`` has no override: no flag and no environment variable enables it. It checks the single constant
``stage3_predictive.REAL_INFERENCE_AUTHORIZED`` first and, while that is False, exits 6 before reading any file, pin or model. The
arguments exist only so the future command has its final shape. Exit codes: 0 ok, 1 an input check failed, 2 usage or I/O refusal, 6 not authorized.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))

from fabric_perf.controllers import real_inference_smoke as smoke   # noqa: E402
from fabric_perf.controllers.stage3_predictive import RealInferenceNotAuthorized   # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="phase", required=True)
    spec = sub.add_parser("spec", help="print the smoke-test contract (reads no file)")
    spec.add_argument("--out", type=Path, default=None, help="write the contract to a NEW file")
    sub.add_parser("verify-inputs", help="hash every pinned input and compare it with the contract and the pin file")
    run = sub.add_parser("run", help="the real run: REFUSED while real inference is not authorized")
    run.add_argument("--out-dir", type=Path, required=True)
    run.add_argument("--orderers", type=int, required=True)
    args = parser.parse_args(argv)
    if args.phase == "run":
        try:
            smoke.run_real_smoke(out_dir=args.out_dir, orderers=args.orderers)
        except RealInferenceNotAuthorized as exc:
            print(f"REFUSED: {exc}")
            return 6
        except smoke.SmokeProcedureNotWired as exc:
            print(f"REFUSED: {exc}")
            return 6
        return 6
    if args.phase == "spec":
        text = json.dumps(smoke.build_spec(), indent=2, sort_keys=True) + "\n"
        if args.out is None:
            print(text, end="")
            return 0
        try:
            with args.out.open("x", encoding="utf-8", newline="\n") as stream:
                stream.write(text)
        except OSError as exc:
            print(f"REFUSED: {exc}")
            return 2
        print(f"smoke-test contract written to {args.out}")
        return 0
    report = smoke.verify_inputs(REPO)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["all_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
