"""WSL-side tree digest for the integrity step X13 (task A0u, 2026-11-22). READ-ONLY: it lists and reads files under ONE approved directory and prints ONE JSON document to stdout.

    python3 scripts/e1/e1_wsl_tree_digest.py digest --target /home/u/ws/runs/RUN --approved-root /home/u/ws/runs --expect directory --run-id RUN --variant VARIANT \\
        --plan-sha256 HEX64 --path-record-sha256 HEX64 --distro NAME --phase pre_workload|post_workload --operation wsl_integrity_digest \\
        --symlinks refuse|record_internal --max-entries N --max-bytes N --max-depth N

It is meant to be started by the Windows-host orchestrator as the structured vector ``wsl.exe -d <distro> --cd <repo> --exec python3 scripts/e1/e1_wsl_tree_digest.py digest ...`` (never through a
shell). It writes nothing, deletes nothing, starts no process, opens no network connection and touches no Docker, Fabric or Caliper component. Every argument is required (no default). The
library (``src/fabric_perf/controllers/tree_digest.py``) is loaded BY FILE PATH from this repository checkout so the package ``__init__`` (and the science stack) is never imported; both files are
listed, with hashes, in the candidate manifest and in the plan's addendum.

The only environment variable read is ``WSL_DISTRO_NAME``, to attest which distribution this process runs in; it can only make the tool REFUSE (distribution mismatch), never permit more.
Exit codes: 0 DIGESTED, 3 REFUSED (a refusal document is still printed), 2 invalid command line. The document is evidence about the directory at the moment of reading; it is not an approval.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
LIB = REPO / "src" / "fabric_perf" / "controllers" / "tree_digest.py"
EXIT_OK, EXIT_INVALID, EXIT_REFUSED = 0, 2, 3


def load_library(path: Path = LIB):
    spec = importlib.util.spec_from_file_location("a0u_tree_digest_lib", str(path))
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("digest")
    for name in ("target", "approved-root", "expect", "run-id", "variant", "plan-sha256", "path-record-sha256", "distro", "phase", "operation", "symlinks"):
        p.add_argument("--" + name, required=True)
    for name in ("max-entries", "max-bytes", "max-depth"):
        p.add_argument("--" + name, required=True, type=int)
    return parser


def main(argv=None, *, fs=None, env_distro=None, platform=None, now_utc=None) -> int:
    """``fs``, ``env_distro``, ``platform`` and ``now_utc`` exist so tests can inject a file-system view and the attested identity; the command line cannot set them."""
    try:
        args = build_parser().parse_args(sys.argv[1:] if argv is None else list(argv))
    except SystemExit:
        return EXIT_INVALID
    lib = load_library()
    request = {"run_id": args.run_id, "variant": args.variant, "plan_sha256": args.plan_sha256, "distro": args.distro, "phase": args.phase, "operation": args.operation,
               "approved_root": args.approved_root, "target": args.target, "expected_type": args.expect, "symlink_policy": args.symlinks, "max_entries": args.max_entries,
               "max_total_bytes": args.max_bytes, "max_depth": args.max_depth, "path_record_sha256": args.path_record_sha256}
    observed_platform = sys.platform if platform is None else platform
    if env_distro is None:
        env_distro = os.environ.get("WSL_DISTRO_NAME")
    if now_utc is None:
        stamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    else:
        stamp = now_utc
    document = lib.build_evidence(request, fs if fs is not None else lib.RealFs(), observed_distro=env_distro, observed_utc=stamp, platform=observed_platform,
                                  tool_sha256=_sha256_file(Path(__file__).resolve()), library_sha256=_sha256_file(LIB))
    if observed_platform != "linux" and document["result"]["verdict"] == "DIGESTED":
        # the digest was produced on a non-Linux host: never present it as a WSL observation (the orchestrator also rejects it by the recorded platform)
        document["result"] = {"verdict": "REFUSED", "code": "not_a_linux_process", "message": "this tool must run inside the WSL distribution (platform %s)" % observed_platform}
        document["tree"] = None
        document["document_sha256"] = lib._document_digest(document)
    sys.stdout.write(json.dumps(document, sort_keys=True, ensure_ascii=True) + "\n")
    return EXIT_OK if document["result"]["verdict"] == "DIGESTED" else EXIT_REFUSED


if __name__ == "__main__":
    sys.exit(main())
