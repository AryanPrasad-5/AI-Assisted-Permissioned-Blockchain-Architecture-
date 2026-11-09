"""Chaincode build/package/install tool against a fake go and peer CLI. Synthetic packages only."""
import hashlib
import io
import json
import sys
import tarfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts" / "e1"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import e1_channel_run as cr  # noqa: E402
import e1_chaincode_deploy as cd  # noqa: E402
import synthetic_fabric as sf  # noqa: E402

LABEL = "e1asset_1.0"


def _tgz(files: dict) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as t:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            t.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def package_bytes(label=LABEL, ctype="golang", drop=(), extra=()):
    code = {m: b"x" for m in cd.REQUIRED_MEMBERS + [cd.CONTRACT_API_PREFIX + "contractapi/contract.go", "src/main_shim.go"]
            if m not in drop}
    code.update({m: b"x" for m in extra})
    meta = json.dumps({"path": "/cc", "type": ctype, "label": label}).encode()
    return _tgz({"metadata.json": meta, "code.tar.gz": _tgz(code)})


class FakeCli:
    def __init__(self, fail=None, pkg=None, calc=None, query_missing=None):
        self.fail, self.pkg, self.calc, self.qm = dict(fail or {}), pkg, calc, query_missing
        self.calls = []

    def __call__(self, argv, env):
        self.calls.append(argv)
        key = argv[3] if argv[0] == "go" and argv[1] == "-C" else argv[1] if argv[0] == "go" else argv[3]
        if self.fail.get(key) == env.get("CORE_PEER_LOCALMSPID", key) or self.fail.get(key) is True:
            return 1, b"", f"Error: {key} failed".encode()
        if argv[:2] == ["go", "version"]:
            assert env["GOPROXY"] == "off" and env["GOTOOLCHAIN"] == "local"
            return 0, b"go version go1.26.4 linux/amd64\n", b""
        if key == "build":
            Path(argv[argv.index("-o") + 1]).write_bytes(b"\x7fELF")
            return 0, b"", b""
        if key == "test":
            return 0, b"ok  \te1asset/core\t0.1s\n", b""
        if key == "package":
            # E0-0 check 5: the peer packager runs `go list`; VCS stamping must be off and the step offline
            assert env["GOFLAGS"] == "-buildvcs=false" and env["GOPROXY"] == "off" and env["GOTOOLCHAIN"] == "local"
            Path(argv[4]).write_bytes(self.pkg if self.pkg is not None else package_bytes())
            return 0, b"", b""
        if key == "calculatepackageid":
            data = Path(argv[4]).read_bytes()
            return 0, (self.calc or f"{LABEL}:{hashlib.sha256(data).hexdigest()}").encode() + b"\n", b""
        if key == "install":
            return 0, b"", b"Chaincode code package identifier: x\n"
        if key == "queryinstalled":
            if self.qm == env["CORE_PEER_LOCALMSPID"]:
                return 0, json.dumps({"installed_chaincodes": []}).encode(), b""
            tar = next(Path(c[4]) for c in self.calls if len(c) > 4 and c[3] == "package")
            pid = f"{LABEL}:{hashlib.sha256(tar.read_bytes()).hexdigest()}"
            return 0, json.dumps({"installed_chaincodes": [{"package_id": pid, "label": LABEL}]}).encode(), b""
        return 127, b"", b"unexpected"


def cc_dir(tmp_path, drop=()):
    d = tmp_path / "cc"
    for f in cd.REQUIRED_FILES + ["core/e1_functions.go", "core/e0_diagnostics.go", "core/validate.go"]:
        if f not in drop:
            (d / f).parent.mkdir(parents=True, exist_ok=True)
            (d / f).write_text("x\n", encoding="utf-8")
    return d


def deploy(tmp_path, cli, **kw):
    net = cr.Network(sf.network(tmp_path))
    return cd.run(net, kw.pop("cc", None) or cc_dir(tmp_path), LABEL, tmp_path / kw.pop("name", "out"), cli,
                  wall_ns=sf.Clock().wall_ns)


def test_full_path_installs_on_every_peer_with_an_independently_computed_package_id(tmp_path):
    cli = FakeCli()
    r = deploy(tmp_path, cli)
    assert r["status"] == "INSTALLED" and r["go_version"].startswith("go version go1.26.4")
    data = (tmp_path / "out" / f"{LABEL}.tar.gz").read_bytes()
    assert r["package"]["package_id"] == f"{LABEL}:{hashlib.sha256(data).hexdigest()}"
    assert r["package"]["metadata"]["type"] == "golang" and r["package"]["shim_adapter_included_but_ignored"]
    assert [s["step"] for s in r["steps"]][-4:] == ["install:peer0.org1.example.com", "install:peer0.org2.example.com",
                                                    "queryinstalled:peer0.org1.example.com",
                                                    "queryinstalled:peer0.org2.example.com"]
    assert all(s["ok"] for s in r["steps"]) and (tmp_path / "out" / "chaincode_record.json").exists()
    assert ["go", "-C", (tmp_path / "cc").as_posix(), "build", "-mod=vendor"] == cli.calls[1][:5]


def test_preconditions_build_and_package_failures(tmp_path):
    r = deploy(tmp_path, FakeCli(), cc=cc_dir(tmp_path, drop=("vendor/modules.txt",)))
    assert r["status"] == "PRECONDITION_FAILED" and "Stage 7b" in r["detail"] and r["steps"] == []
    r = deploy(tmp_path, FakeCli(fail={"build": True}), name="o2")
    assert r["status"] == "BUILD_FAILED" and r["steps"][-1]["step"] == "go_build" and len(r["steps"]) == 2
    for name, cli, why in [("o3", FakeCli(pkg=package_bytes(drop=("src/vendor/modules.txt",))), "lacks"),
                           ("o4", FakeCli(pkg=package_bytes(drop=(cd.CONTRACT_API_PREFIX + "contractapi/contract.go",))),
                            "contract-api"),
                           ("o5", FakeCli(pkg=package_bytes(label="other_1.0")), "label"),
                           ("o6", FakeCli(pkg=package_bytes(ctype="node")), "type"),
                           ("o7", FakeCli(pkg=package_bytes(extra=("src/.git/HEAD",))), ".git"),
                           ("o8", FakeCli(pkg=b"not a tarball"), "unreadable"),
                           ("o9", FakeCli(calc=f"{LABEL}:{'0' * 64}"), "calculatepackageid")]:
        r = deploy(tmp_path, cli, name=name)
        assert r["status"] == "PACKAGE_INVALID" and why in r["detail"], (name, r["detail"])
        assert not any(s["step"].startswith("install") for s in r["steps"])


def test_install_failures(tmp_path):
    r = deploy(tmp_path, FakeCli(fail={"install": "Org2MSP"}))
    assert r["status"] == "INSTALL_FAILED" and r["steps"][-1]["step"] == "install:peer0.org2.example.com"
    r = deploy(tmp_path, FakeCli(query_missing="Org1MSP"), name="o2")
    assert r["status"] == "INSTALL_FAILED" and "not listed" in r["detail"]


def test_cli_guards(tmp_path, capsys):
    netf = tmp_path / "network.json"
    netf.write_text(json.dumps(sf.network(tmp_path)), encoding="utf-8")
    base = ["--network", str(netf), "--cc-dir", str(cc_dir(tmp_path)), "--label", LABEL]
    assert cd.main(base + ["--out-dir", str(tmp_path / "dry")]) == 0
    assert "dry run" in capsys.readouterr().out and not (tmp_path / "dry").exists()
    (tmp_path / "exists").mkdir()
    assert cd.main(base + ["--out-dir", str(tmp_path / "exists"), "--execute"], runner=FakeCli()) == 2
    assert cd.main(["--network", str(netf), "--cc-dir", "x", "--label", "bad label", "--out-dir", str(tmp_path / "y"),
                    "--execute"], runner=FakeCli()) == 2
    assert cd.main(base + ["--out-dir", str(tmp_path / "ok"), "--execute"], runner=FakeCli()) == 0
