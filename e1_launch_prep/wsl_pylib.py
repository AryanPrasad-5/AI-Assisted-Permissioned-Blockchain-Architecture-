"""Create-exclusive bootstrap of an isolated library folder for WSL's python3 (standard library only; no pip, no sudo, no virtual environment, no system change).

Why: the live entry (`python3 -m p4_replay.live_entry launch`) must run inside WSL (the frozen P1b `Backend` reads /proc and uses Windows interop), and `p4_replay` imports numpy and
pandas, which WSL's system python3 (3.12) does not have. This script downloads exact, pinned wheels from PyPI, checks each file's SHA-256 against the digest PyPI publishes for it,
extracts them into a NEW folder (an existing one is refused), and writes INSTALLED.json (name, version, wheel, sha256). Use with `PYTHONPATH=<dest>`.

    python3 wsl_pylib.py --dest /home/mailf18/p4_replay_pylib --pkg numpy==2.5.4 --pkg pandas==3.0.6 --pkg python-dateutil==2.9.0.post0 --pkg six==1.17.0

Trust and limits: the digest comes from the same index as the wheel, so this detects corruption and a wrong file, not a compromised index. The wheels' code is third-party and runs inside
the experiment host's Python; record INSTALLED.json with the evidence. Nothing is installed outside --dest.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
import urllib.request
import zipfile
from pathlib import PurePosixPath

PYPI = "https://pypi.org/pypi/{name}/{version}/json"
PY_TAG = f"cp{sys.version_info.major}{sys.version_info.minor}"


class PylibError(ValueError):
    pass


def pick_wheel(urls: list[dict], py_tag: str = PY_TAG) -> dict:
    """Choose the wheel for this interpreter: a pure `none-any` wheel, or a `<py_tag>` manylinux x86_64 wheel. Several candidates: the lexicographically last filename. None: error."""
    cands = []
    for f in urls:
        fn = f.get("filename", "")
        if f.get("packagetype") != "bdist_wheel" or not fn.endswith(".whl") or f.get("yanked"):
            continue
        tags = fn[:-4].split("-")[-3:]                        # python tag, abi tag, platform tag
        py, abi, plat = tags
        pure = abi == "none" and plat == "any" and any(t.startswith("py3") or t.startswith("py2.py3") or t == py_tag for t in py.split("."))
        native = py == py_tag and "manylinux" in plat and "x86_64" in plat
        if pure or native:
            cands.append(f)
    if not cands:
        raise PylibError("no suitable wheel in the release")
    return sorted(cands, key=lambda f: f["filename"])[-1]


def safe_member(name: str) -> str:
    p = PurePosixPath(name)
    if p.is_absolute() or ".." in p.parts or "\\" in name or not p.parts:
        raise PylibError(f"unsafe path in wheel: {name!r}")
    return p.as_posix()


def extract(wheel_bytes: bytes, dest: str) -> int:
    n = 0
    with zipfile.ZipFile(io.BytesIO(wheel_bytes)) as zf:
        for info in zf.infolist():
            rel = safe_member(info.filename)
            target = os.path.join(dest, *rel.split("/"))
            if info.is_dir() or rel.endswith("/"):
                os.makedirs(target, exist_ok=True)
                continue
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with zf.open(info) as src, open(target, "xb") as out:        # create-exclusive: two packages never write the same file
                out.write(src.read())
            if info.external_attr >> 16 & 0o111:
                os.chmod(target, 0o755)
            n += 1
    return n


def fetch(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=120) as r:
        return r.read()


def install(dest: str, pins: list[str]) -> list[dict]:
    if os.path.exists(dest):
        raise FileExistsError(dest)
    os.makedirs(dest)
    done = []
    for pin in pins:
        name, sep, version = pin.partition("==")
        if not sep or not name or not version:
            raise PylibError(f"--pkg must be name==version, got {pin!r}")
        meta = json.loads(fetch(PYPI.format(name=name, version=version)).decode("utf-8"))
        wheel = pick_wheel(meta["urls"])
        data = fetch(wheel["url"])
        digest = hashlib.sha256(data).hexdigest()
        if digest != wheel["digests"]["sha256"]:
            raise PylibError(f"{wheel['filename']}: sha256 {digest} is not the published {wheel['digests']['sha256']}")
        files = extract(data, dest)
        done.append({"name": name, "version": version, "wheel": wheel["filename"], "sha256": digest, "files": files})
        print(f"installed {name}=={version}: {wheel['filename']} sha256 {digest} ({files} files)", flush=True)
    with open(os.path.join(dest, "INSTALLED.json"), "x", encoding="utf-8", newline="\n") as fh:
        json.dump({"python": sys.version.split()[0], "packages": done}, fh, indent=1)
    return done


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--dest", required=True)
    ap.add_argument("--pkg", action="append", required=True, metavar="NAME==VERSION")
    args = ap.parse_args(argv)
    try:
        install(args.dest, args.pkg)
    except (OSError, PylibError, ValueError, KeyError) as exc:
        print(f"INVALID: {exc!r}")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
