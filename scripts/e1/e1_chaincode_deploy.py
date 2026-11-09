"""Chaincode build, package and install (LA-2(c); E0-0 "chaincode compilation").

Status: written 2026-11-09, tested with a fake go/peer CLI only. Never run. It assumes that vendoring
(`go mod tidy && go mod vendor`, environment Stage 7b, a module download) is already done; it never
downloads (GOPROXY=off, GOTOOLCHAIN=local, -mod=vendor). Approve/commit per channel is done by
e1_channel_run.py; this tool installs the package once on every peer.

Steps (each argv, exit code and output hashes are recorded in chaincode_record.json):
  0 preconditions  go.mod, go.sum, vendor/modules.txt, main.go, core/ present in --cc-dir (else exit 2, nothing run)
  1 go version     the toolchain (Stage 5c pins go1.26.4; ccenv 2.5.16 builds with the same Go)
  2 go build       go -C <cc-dir> build -mod=vendor -o <out>/build/e1asset .
  3 go test        go -C <cc-dir> test -mod=vendor -count=1 ./core/
  4 package        peer lifecycle chaincode package <out>/<label>.tar.gz --path <cc-dir> --lang golang --label <label>
  5 inspect        metadata.json (type golang, label) and code.tar.gz members: src/go.mod, src/go.sum, src/main.go,
                   src/core/dispatch.go, src/vendor/modules.txt, the vendored contract API
  6 package id     <label>:<SHA-256 of the tarball>, computed here, must equal `peer lifecycle chaincode
                   calculatepackageid`
  7 install        peer lifecycle chaincode install on every peer of network.json
  8 verify         peer lifecycle chaincode queryinstalled --output json lists the package id on every peer
Status / exit: INSTALLED 0; BUILD_FAILED, PACKAGE_INVALID, INSTALL_FAILED 1; PRECONDITION_FAILED or bad request 2.

Usage (dry run prints the plan; --execute needs the E0-0 approval):
  python3 -B scripts/e1/e1_chaincode_deploy.py --network network.json --cc-dir fabric-testbed/chaincode/e1asset
          --label e1asset_1.0 --out-dir <new dir> [--execute]
Standard library only.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import subprocess
import sys
import tarfile
import time
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_channel_run as cr  # noqa: E402
import e1_telemetry_common as tc  # noqa: E402

TOOL_VERSION = "e1_chaincode_deploy 0.1 (2026-11-09)"
LABEL_RE = re.compile(r"^[A-Za-z0-9_.+-]+$")
# -buildvcs=false: Go's VCS stamping runs git on the repo, which WSL's git refuses for a Windows-owned working tree
# (exit 128, seen in Stage 7b, 2026-11-09). It only drops a metadata stamp; ccenv builds from src/ without .git.
GO_ENV = {"GOTOOLCHAIN": "local", "GOPROXY": "off", "GOFLAGS": "-mod=vendor -buildvcs=false", "CGO_ENABLED": "0"}
REQUIRED_FILES = ["go.mod", "go.sum", "vendor/modules.txt", "main.go", "core/dispatch.go"]
REQUIRED_MEMBERS = ["src/go.mod", "src/go.sum", "src/main.go", "src/core/dispatch.go", "src/vendor/modules.txt"]
CONTRACT_API_PREFIX = "src/vendor/github.com/hyperledger/fabric-contract-api-go/v2/"
INSTALLED, BUILD_FAILED, PACKAGE_INVALID = "INSTALLED", "BUILD_FAILED", "PACKAGE_INVALID"
INSTALL_FAILED, PRECONDITION_FAILED = "INSTALL_FAILED", "PRECONDITION_FAILED"
EXIT = {INSTALLED: 0, BUILD_FAILED: 1, PACKAGE_INVALID: 1, INSTALL_FAILED: 1, PRECONDITION_FAILED: 2}
Runner = Callable[[list[str], dict], "tuple[int | None, bytes, bytes]"]


def inspect_package(data: bytes, label: str) -> tuple[list[str], dict]:
    """Return (problems, facts) for a Fabric chaincode package (tar.gz of metadata.json + code.tar.gz)."""
    problems, facts = [], {}
    try:
        outer = tarfile.open(fileobj=io.BytesIO(data), mode="r:gz")
        names = outer.getnames()
        facts["outer_members"] = names
        meta = json.loads(outer.extractfile("metadata.json").read().decode("utf-8"))
        code = outer.extractfile("code.tar.gz").read()
    except (tarfile.TarError, KeyError, AttributeError, json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
        return [f"package unreadable: {exc!r}"], facts
    facts["metadata"] = meta
    if meta.get("type") != "golang":
        problems.append(f"metadata type {meta.get('type')!r} != 'golang'")
    if meta.get("label") != label:
        problems.append(f"metadata label {meta.get('label')!r} != {label!r}")
    try:
        inner = tarfile.open(fileobj=io.BytesIO(code), mode="r:gz").getnames()
    except tarfile.TarError as exc:
        return problems + [f"code.tar.gz unreadable: {exc!r}"], facts
    facts["code_members"] = len(inner)
    facts["code_sha256"] = hashlib.sha256(code).hexdigest()
    missing = [m for m in REQUIRED_MEMBERS if m not in inner]
    if missing:
        problems.append(f"code.tar.gz lacks {missing}")
    if not any(m.startswith(CONTRACT_API_PREFIX) for m in inner):
        problems.append("vendored fabric-contract-api-go/v2 missing from code.tar.gz")
    facts["shim_adapter_included_but_ignored"] = "src/main_shim.go" in inner
    if any(m.startswith("src/.git/") or "/__pycache__/" in m for m in inner):
        problems.append("package contains .git or __pycache__")
    return problems, facts


def plan(net: cr.Network, cc_dir: str, label: str, out_dir: str) -> list[tuple[str, list[str], dict]]:
    tar = f"{out_dir}/{label}.tar.gz"
    steps = [("go_version", ["go", "version"], GO_ENV),
             ("go_build", ["go", "-C", cc_dir, "build", "-mod=vendor", "-o", f"{out_dir}/build/e1asset", "."], GO_ENV),
             ("go_test", ["go", "-C", cc_dir, "test", "-mod=vendor", "-count=1", "./core/"], GO_ENV),
             # the peer CLI's Go packager runs `go list` in cc_dir: same VCS-stamping refusal as the go steps
             # (E0-0 check 5, 2026-11-09), so the same flags, offline
             ("package", [net.tool("peer"), "lifecycle", "chaincode", "package", tar, "--path", cc_dir,
                          "--lang", "golang", "--label", label],
              {**net.peer_env(net.orgs()[0]), "GOFLAGS": "-buildvcs=false", "GOTOOLCHAIN": "local", "GOPROXY": "off"}),
             ("calculatepackageid", [net.tool("peer"), "lifecycle", "chaincode", "calculatepackageid", tar],
              net.peer_env(net.orgs()[0]))]
    for p in net.raw["peers"]:
        steps.append((f"install:{p['name']}", [net.tool("peer"), "lifecycle", "chaincode", "install", tar],
                      net.peer_env(p)))
    for p in net.raw["peers"]:
        steps.append((f"queryinstalled:{p['name']}", [net.tool("peer"), "lifecycle", "chaincode", "queryinstalled",
                                                      "--output", "json"], net.peer_env(p)))
    return steps


def run(net: cr.Network, cc_dir: Path, label: str, out_dir: Path, runner: Runner, *,
        wall_ns: Callable[[], int] = time.time_ns, argv: list[str] | None = None) -> dict:
    if not LABEL_RE.match(label):
        raise ValueError(f"label {label!r}")
    missing = [f for f in REQUIRED_FILES if not (cc_dir / f).exists()]
    if out_dir.exists():
        raise ValueError(f"out dir {out_dir} exists; refusing to reuse or overwrite")
    out_dir.mkdir(parents=True)
    (out_dir / "cmd").mkdir()
    (out_dir / "build").mkdir()
    rec = {"tool": TOOL_VERSION, "script": tc.provenance(__file__), "argv": argv, "label": label,
           "cc_dir": str(cc_dir), "source_sha256": {f: tc.sha256_file(cc_dir / f) for f in
                                                    ["go.mod", "go.sum", "main.go", "core/dispatch.go",
                                                     "core/e1_functions.go", "core/e0_diagnostics.go",
                                                     "core/validate.go", "vendor/modules.txt"]},
           "steps": [], "status": None, "detail": ""}
    if missing:
        rec.update(status=PRECONDITION_FAILED, detail=f"missing {missing}: vendoring (Stage 7b) not done")
        tc.write_json_x(out_dir / "chaincode_record.json", rec)
        return rec
    tar = out_dir / f"{label}.tar.gz"
    status = INSTALLED
    for k, (name, cmd, env) in enumerate(plan(net, cc_dir.as_posix(), label, out_dir.as_posix())):
        t0 = wall_ns()
        try:
            code, out, err = runner(cmd, env)
        except (OSError, subprocess.SubprocessError) as exc:
            code, out, err = None, b"", repr(exc).encode()
        stem = f"{k:02d}_{name.replace(':', '_')}"
        tc.write_bytes_x(out_dir / "cmd" / f"{stem}.stdout", out)
        tc.write_bytes_x(out_dir / "cmd" / f"{stem}.stderr", err)
        step = {"step": name, "argv": cmd, "env": env, "start_utc": tc.utc_iso(t0), "exit_code": code,
                "stdout_sha256": tc.sha256_bytes(out), "stderr_sha256": tc.sha256_bytes(err), "ok": code == 0}
        rec["steps"].append(step)
        text = out.decode("utf-8", "replace").strip()
        bad = None
        if code != 0:
            bad = {"go_version": BUILD_FAILED, "go_build": BUILD_FAILED, "go_test": BUILD_FAILED,
                   "package": PACKAGE_INVALID, "calculatepackageid": PACKAGE_INVALID}.get(name, INSTALL_FAILED)
            rec["detail"] = f"{name}: exit {code}: {err.decode('utf-8', 'replace').strip()[-300:]}"
        elif name == "go_version":
            rec["go_version"] = text
        elif name == "go_build" and not (out_dir / "build" / "e1asset").exists():
            bad, rec["detail"] = BUILD_FAILED, "go build exited 0 but wrote no binary"
        elif name == "package":
            if not tar.exists():
                bad, rec["detail"] = PACKAGE_INVALID, "package exited 0 but wrote no tarball"
            else:
                data = tar.read_bytes()
                problems, facts = inspect_package(data, label)
                rec["package"] = {"file": tar.name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
                                  "package_id": f"{label}:{hashlib.sha256(data).hexdigest()}", **facts}
                if problems:
                    bad, rec["detail"] = PACKAGE_INVALID, "; ".join(problems)
        elif name == "calculatepackageid":
            want = rec["package"]["package_id"]
            step["printed"] = text
            if text != want:
                bad, rec["detail"] = PACKAGE_INVALID, f"calculatepackageid {text!r} != computed {want!r}"
        elif name.startswith("queryinstalled:"):
            try:
                ids = {c["package_id"]: c.get("label") for c in json.loads(text).get("installed_chaincodes", [])}
            except (json.JSONDecodeError, AttributeError, TypeError, KeyError):
                ids = None
            want = rec["package"]["package_id"]
            if ids is None or ids.get(want) != label:
                bad, rec["detail"] = INSTALL_FAILED, f"{name}: package {want} not listed (got {ids})"
        if bad:
            step["ok"], status = False, bad
            break
    rec["status"] = status
    rec["finished_utc"] = tc.utc_iso(wall_ns())
    tc.write_json_x(out_dir / "chaincode_record.json", rec)
    return rec


def _subprocess_runner(argv: list[str], env: dict) -> tuple[int | None, bytes, bytes]:
    proc = subprocess.run(argv, env={**os.environ, **env}, capture_output=True, timeout=900, check=False)
    return proc.returncode, proc.stdout, proc.stderr


def main(argv: list[str] | None = None, runner: Runner | None = None, platform_name: str = sys.platform) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--network", required=True, type=Path)
    ap.add_argument("--cc-dir", required=True, type=Path)
    ap.add_argument("--label", required=True)
    ap.add_argument("--out-dir", required=True, type=Path)
    ap.add_argument("--execute", action="store_true", help="build, package and install (needs E0-0 approval)")
    args = ap.parse_args(argv)
    try:
        net = cr.Network(json.loads(args.network.read_text(encoding="utf-8")))
        if not args.execute:
            for name, cmd, env in plan(net, args.cc_dir.as_posix(), args.label, args.out_dir.as_posix()):
                print(f"PLAN {name}: {' '.join(cmd)}")
            print("dry run: nothing executed or written (add --execute once E0-0 is approved)")
            return 0
        if runner is None and not platform_name.startswith("linux"):
            raise ValueError(f"--execute runs go and the Linux peer CLI: run it inside the Ubuntu WSL distro "
                             f"(platform here: {platform_name})")
        rec = run(net, args.cc_dir, args.label, args.out_dir, runner or _subprocess_runner,
                  argv=sys.argv if argv is None else argv)
    except (OSError, ValueError, KeyError, cr.RequestError, json.JSONDecodeError) as err:
        print(f"INVALID REQUEST: {err}")
        return 2
    print(f"CHAINCODE {rec['status']} {rec.get('package', {}).get('package_id', '')} {rec['detail']}")
    return EXIT[rec["status"]]


if __name__ == "__main__":
    sys.exit(main())
