"""Offline tests of the WSL library bootstrap: wheel selection, path safety and extraction. No network."""

from __future__ import annotations

import io
import zipfile

import pytest

from e1_launch_prep import wsl_pylib as wp


def whl(fn: str, **kw) -> dict:
    return {"filename": fn, "packagetype": "bdist_wheel", "yanked": False, **kw}


def test_pick_wheel_prefers_this_interpreters_manylinux_x86_64_or_a_pure_wheel():
    urls = [whl("numpy-2.5.4-cp313-cp313-manylinux_2_27_x86_64.whl"), whl("numpy-2.5.4-cp312-cp312-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl"),
            whl("numpy-2.5.4-cp312-cp312-win_amd64.whl"), whl("numpy-2.5.4-cp312-cp312-manylinux_2_27_aarch64.whl"), {"filename": "numpy-2.5.4.tar.gz", "packagetype": "sdist"}]
    assert wp.pick_wheel(urls, "cp312")["filename"].startswith("numpy-2.5.4-cp312-cp312-manylinux_2_27_x86_64")
    pure = [whl("six-1.17.0-py2.py3-none-any.whl")]
    assert wp.pick_wheel(pure, "cp312")["filename"] == "six-1.17.0-py2.py3-none-any.whl"
    with pytest.raises(wp.PylibError):
        wp.pick_wheel([whl("numpy-2.5.4-cp313-cp313-manylinux_2_27_x86_64.whl"), whl("x-1-cp312-cp312-win_amd64.whl"), whl("y-1-cp312-cp312-manylinux_2_27_x86_64.whl", yanked=True)][:2], "cp312")


@pytest.mark.parametrize("bad", ["../evil.py", "/etc/passwd", "a/../../b", "a\\b", ""])
def test_unsafe_wheel_member_paths_are_refused(bad):
    with pytest.raises(wp.PylibError):
        wp.safe_member(bad)


def test_extract_writes_files_create_exclusively_and_refuses_a_traversal_wheel(tmp_path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("pkg/__init__.py", "X = 1\n")
        zf.writestr("pkg/sub/data.txt", "d")
    dest = tmp_path / "lib"
    dest.mkdir()
    assert wp.extract(buf.getvalue(), str(dest)) == 2 and (dest / "pkg" / "__init__.py").read_text() == "X = 1\n"
    with pytest.raises(FileExistsError):
        wp.extract(buf.getvalue(), str(dest))                               # a second package never overwrites a file of the first
    evil = io.BytesIO()
    with zipfile.ZipFile(evil, "w") as zf:
        zf.writestr("../outside.py", "bad")
    with pytest.raises(wp.PylibError):
        wp.extract(evil.getvalue(), str(tmp_path / "lib2"))
    assert not (tmp_path / "outside.py").exists()


def test_install_refuses_an_existing_destination_and_bad_pins(tmp_path):
    (tmp_path / "taken").mkdir()
    with pytest.raises(FileExistsError):
        wp.install(str(tmp_path / "taken"), ["six==1.17.0"])
    assert wp.main(["--dest", str(tmp_path / "taken"), "--pkg", "six==1.17.0"]) == 2
    with pytest.raises(wp.PylibError):
        wp.install(str(tmp_path / "new"), ["six"])                          # no version: refused before any download
