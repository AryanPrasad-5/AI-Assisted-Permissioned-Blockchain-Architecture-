"""Windows <-> WSL <-> container path mapping for the bounded execution plan (task A0t, 2026-11-22). OFFLINE: pure string rules plus optional reads of the LOCAL file system.

Why. The A0s execution plan mixes three path spaces without saying which step uses which: Windows host paths (the output directory, the rendered directory, the network
description in the A0m render), WSL paths (the repository under ``/mnt/c``, the Caliper workspace under ``/home/<user>``) and container paths (``/hyperledger/caliper/workspace``).
A WSL process cannot open ``C:\\...``, so a plan that hands a Windows path to a WSL tool would fail at run time. This module makes every translation explicit, testable and
recorded. It never calls ``wsl.exe``, never asks Docker, never reads ``/etc/wsl.conf``; it can therefore never SHOW that a path works inside a real distribution or container.

Rules (each failure is a stable ``PathMappingError.code``; a path is never repaired):

* The WSL automount root (normally ``/mnt``) is NOT defaulted. It is supplied by the operator or derived by :func:`derive_mount_root` from one declared Windows/WSL pair
  (for example the repository, which ``fabric-testbed/network/env-e1-caliper-benchmark.json`` declares as ``/mnt/c/OJT_SEM3/...``).
* Windows drive paths only (``C:\\x`` or ``C:/x``; mixed separators are normalised). UNC (``\\\\server\\share``, ``\\\\wsl$\\...``, ``\\\\?\\``), drive-relative (``C:x``),
  relative, empty, ``..``-containing and control-character paths are refused. Whitespace inside a path is allowed (arguments travel as a vector, never through a shell).
* Case is preserved and compared exactly (WSL is case-sensitive). A pair that differs only by case is reported as ``case_mismatch``.
* A WSL-native path (``/home/...``) has no drive equivalent; converting it to a Windows path is refused.
* ``within`` is segment-wise: ``/a/bc`` is not inside ``/a/b``.

Verification levels (the highest reachable offline is ``REPO_DECLARATION_CROSS_CHECKED``; ``RUNTIME_VERIFIED`` exists only to be refused):

    SYNTACTIC_RULE                      derived by the drive-mount rule or the container-mount rule; nothing else was consulted
    OPERATOR_SUPPLIED_CONSISTENT        the operator typed both sides and they agree with the rule
    REPO_DECLARATION_CROSS_CHECKED      the mapping equals a declaration that exists in a repository file (cited by path and line text)
    RUNTIME_VERIFIED                    refused: needs a real WSL distribution or container (never reachable in A0t)
"""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

PATH_MAPPING_VERSION = "path_mapping 0.1 (A0t, 2026-11-22)"
RECORD_SCHEMA = "a0t_path_mapping_record/1"
L_SYNTACTIC = "SYNTACTIC_RULE"
L_OPERATOR = "OPERATOR_SUPPLIED_CONSISTENT"
L_REPO = "REPO_DECLARATION_CROSS_CHECKED"
L_RUNTIME = "RUNTIME_VERIFIED"
OFFLINE_LEVELS = (L_SYNTACTIC, L_OPERATOR, L_REPO)
RUNTIME_STATUS = "UNVERIFIED_AT_RUNTIME"          # every record says this; no offline step can change it
_WIN_DRIVE = re.compile(r"([A-Za-z]):/(.*)", re.DOTALL)
_WIN_DRIVE_RELATIVE = re.compile(r"[A-Za-z]:(?![/\\])")
_WIN_BAD_CHARS = re.compile(r'[<>"|?*\x00-\x1f]')
_POSIX_BAD = re.compile(r"[\x00-\x1f]")
_DRIVE_TOKEN = re.compile(r"([A-Za-z]):[\\/]")


class PathMappingError(ValueError):
    """A path cannot be mapped or is not acceptable. ``code`` is a stable machine-readable cause; the path is reported, never repaired."""

    def __init__(self, code: str, message: str):
        super().__init__(f"{message} [{code}]")
        self.code = code


def _fail(code: str, message: str) -> PathMappingError:
    return PathMappingError(code, message)


# ------------------------------------------------------------------------------------------------ classification
def classify(path: Any) -> str:
    """One of ``windows_drive``, ``unc``, ``drive_relative``, ``wsl_posix``, ``relative``, ``empty``, ``invalid``. Pure text rule."""
    if not isinstance(path, str):
        return "invalid"
    if not path.strip():
        return "empty"
    flat = path.replace("\\", "/")
    if flat.startswith("//"):
        return "unc"
    if _WIN_DRIVE.fullmatch(flat):
        return "windows_drive"
    if _WIN_DRIVE_RELATIVE.match(path):
        return "drive_relative"
    if path.startswith("/"):
        return "wsl_posix"
    return "relative"


def _segments(flat: str, *, windows: bool) -> list[str]:
    out: list[str] = []
    for seg in flat.split("/"):
        if seg in ("", "."):
            continue
        if seg == "..":
            raise _fail("path_traversal", f"{flat!r} contains a '..' segment")
        if windows and (seg.endswith(".") or seg.endswith(" ")):
            raise _fail("windows_segment_invalid", f"{seg!r} ends with a dot or a space, which Windows silently strips")
        out.append(seg)
    return out


def normalize_windows(path: Any) -> tuple[str, list[str]]:
    """(drive letter in the case given, segments) of a Windows drive path, or :class:`PathMappingError`."""
    kind = classify(path)
    if kind != "windows_drive":
        code = {"unc": "unc_unsupported", "drive_relative": "drive_relative_unsupported", "relative": "relative_path", "empty": "empty_path", "wsl_posix": "not_a_windows_path",
                "invalid": "wrong_type"}[kind]
        raise _fail(code, f"{path!r} is not a Windows drive path ({kind})")
    if _WIN_BAD_CHARS.search(path):
        raise _fail("windows_character_invalid", f"{path!r} contains a character that is invalid in a Windows path")
    flat = path.replace("\\", "/")
    match = _WIN_DRIVE.fullmatch(flat)
    assert match is not None
    if ":" in match.group(2):
        raise _fail("windows_character_invalid", f"{path!r} contains a second colon (alternate data stream or malformed path)")
    return match.group(1), _segments(match.group(2), windows=True)


def normalize_posix(path: Any, *, what: str = "path") -> str:
    """The normalised absolute POSIX form (no '.', no empty or '..' segments), or :class:`PathMappingError`."""
    kind = classify(path)
    if kind != "wsl_posix":
        code = {"relative": "relative_path", "empty": "empty_path", "windows_drive": "not_a_posix_path", "unc": "unc_unsupported", "drive_relative": "drive_relative_unsupported",
                "invalid": "wrong_type"}[kind]
        raise _fail(code, f"{what} {path!r} is not an absolute POSIX path ({kind})")
    if _POSIX_BAD.search(path) or "\\" in path:
        raise _fail("posix_character_invalid", f"{what} {path!r} contains a control character or a backslash")
    return "/" + "/".join(_segments(path, windows=False))


def check_mount_root(mount_root: Any) -> str:
    if not isinstance(mount_root, str):
        raise _fail("mount_root_missing", "the WSL automount root is required (no default)")
    root = normalize_posix(mount_root, what="mount_root")
    if root == "/":
        raise _fail("mount_root_invalid", "the automount root may not be '/'")
    return root


# ------------------------------------------------------------------------------------------------ Windows <-> WSL
def windows_to_wsl(path: Any, *, mount_root: Any) -> str:
    root = check_mount_root(mount_root)
    drive, segs = normalize_windows(path)
    return posixpath.join(root, drive.lower(), *segs) if segs else posixpath.join(root, drive.lower())


def wsl_to_windows(path: Any, *, mount_root: Any) -> str:
    """The Windows form of ``<mount_root>/<drive>/...``; a WSL-native path has no drive equivalent and is refused."""
    root = check_mount_root(mount_root)
    norm = normalize_posix(path, what="WSL path")
    parts = norm.split("/")[1:]
    root_parts = root.split("/")[1:]
    if parts[: len(root_parts)] != root_parts or len(parts) <= len(root_parts) or not re.fullmatch(r"[a-z]", parts[len(root_parts)]):
        raise _fail("wsl_native_path_has_no_drive_equivalent", f"{norm!r} is not under {root}/<drive letter>; a Windows form would be a UNC path, which is unsupported")
    drive = parts[len(root_parts)].upper()
    rest = parts[len(root_parts) + 1:]
    return f"{drive}:\\" + "\\".join(rest) if rest else f"{drive}:\\"


def derive_mount_root(windows_path: Any, wsl_path: Any) -> str:
    """The automount root implied by ONE declared (Windows, WSL) pair. Comparison is exact (case-sensitive)."""
    drive, segs = normalize_windows(windows_path)
    norm = normalize_posix(wsl_path, what="WSL path")
    suffix = "/" + "/".join([drive.lower(), *segs])
    if norm.endswith(suffix):
        return check_mount_root(norm[: len(norm) - len(suffix)] or "/")
    if norm.lower().endswith(suffix.lower()):
        raise _fail("case_mismatch", f"{windows_path!r} and {wsl_path!r} differ only by case; WSL paths are case-sensitive")
    raise _fail("mount_root_mismatch", f"{wsl_path!r} does not end with {suffix!r}, the WSL form of {windows_path!r}")


def within(path: Any, root: Any) -> bool:
    """True when the normalised POSIX ``path`` equals or lies inside ``root`` (segment-wise, case-sensitive)."""
    p, r = normalize_posix(path).split("/")[1:], normalize_posix(root, what="root").split("/")[1:]
    r = [seg for seg in r if seg]
    p = [seg for seg in p if seg]
    return p[: len(r)] == r


def require_within(path: Any, roots: Sequence[Any], *, what: str) -> str:
    norm = normalize_posix(path, what=what)
    if any(within(norm, root) for root in roots):
        return norm
    raise _fail("outside_approved_roots", f"{what} {norm!r} is not inside any approved root {list(roots)}")


# ------------------------------------------------------------------------------------------------ container mount
@dataclass(frozen=True)
class ContainerMount:
    """One bind mount ``<wsl_root>:<container_root>``. ``source`` names the repository declaration the mapping was taken from."""

    wsl_root: str
    container_root: str
    container_name: str
    source: str

    def __post_init__(self):
        object.__setattr__(self, "wsl_root", normalize_posix(self.wsl_root, what="mount wsl_root"))
        object.__setattr__(self, "container_root", normalize_posix(self.container_root, what="mount container_root"))

    def to_container(self, wsl_path: Any) -> str:
        norm = normalize_posix(wsl_path, what="WSL path")
        if not within(norm, self.wsl_root):
            raise _fail("mount_root_mismatch", f"{norm!r} is not inside the mounted WSL directory {self.wsl_root!r}")
        rel = norm[len(self.wsl_root):].lstrip("/")
        return posixpath.join(self.container_root, rel) if rel else self.container_root

    def to_wsl(self, container_path: Any) -> str:
        norm = normalize_posix(container_path, what="container path")
        if not within(norm, self.container_root):
            raise _fail("mount_root_mismatch", f"{norm!r} is not inside the container mount {self.container_root!r}")
        rel = norm[len(self.container_root):].lstrip("/")
        return posixpath.join(self.wsl_root, rel) if rel else self.wsl_root


# ------------------------------------------------------------------------------------------------ local file system checks (host side only)
def check_symlink_escape(path: Any, root: Any) -> dict[str, Any]:
    """If ``path`` exists on THIS machine, its resolved location must stay inside the resolved ``root``. A missing path is reported, not accepted as verified."""
    p, r = Path(str(path)), Path(str(root))
    if not r.exists():
        raise _fail("root_missing", f"the approved root {root!r} does not exist on this machine")
    if not p.exists() and not p.is_symlink():
        return {"path": str(path), "exists": False, "escape_checked": False, "note": "the target does not exist, so a symlink escape cannot be assessed"}
    resolved_p, resolved_r = p.resolve(), r.resolve()
    try:
        resolved_p.relative_to(resolved_r)
    except ValueError:
        raise _fail("symlink_escape", f"{path!r} resolves to {str(resolved_p)!r}, outside {str(resolved_r)!r}") from None
    return {"path": str(path), "exists": True, "escape_checked": True, "resolved": str(resolved_p)}


# ------------------------------------------------------------------------------------------------ argv translation for a WSL-side step
def translate_token_for_wsl(token: str, *, mount_root: str) -> str:
    """A token that is a Windows drive path (or ``--flag=<drive path>``) becomes its WSL form; every other token is returned unchanged. UNC and ``..`` tokens are refused."""
    if not isinstance(token, str):
        raise _fail("wrong_type", f"argument {token!r} is not text")
    flag, value = "", token
    match = re.fullmatch(r"(--?[A-Za-z0-9][\w.-]*=)(.*)", token, flags=re.DOTALL)
    if match:
        flag, value = match.group(1), match.group(2)
    kind = classify(value)
    if kind == "unc":
        raise _fail("unc_unsupported", f"argument {token!r} is a UNC path")
    if kind == "windows_drive":
        return flag + windows_to_wsl(value, mount_root=mount_root)
    if kind == "drive_relative":
        raise _fail("drive_relative_unsupported", f"argument {token!r} is a drive-relative path")
    if ("/" in value or "\\" in value) and ".." in re.split(r"[\\/]", value):
        raise _fail("path_traversal", f"argument {token!r} contains a '..' segment")
    return token


def translate_argv_for_wsl(argv: Sequence[str], *, mount_root: str) -> list[str]:
    return [translate_token_for_wsl(t, mount_root=mount_root) for t in argv]


def path_like_tokens(argv: Sequence[str]) -> list[str]:
    """Tokens that name an absolute location (Windows drive path or POSIX absolute path); relative-looking tokens are not paths we can bound."""
    out = []
    for token in argv:
        match = re.fullmatch(r"(--?[A-Za-z0-9][\w.-]*=)(.*)", token, flags=re.DOTALL)
        value = match.group(2) if match else token
        if classify(value) in ("windows_drive", "wsl_posix", "unc", "drive_relative"):
            out.append(value)
    return out


# ------------------------------------------------------------------------------------------------ the machine-readable record
def _digest(document: Mapping[str, Any]) -> str:
    return hashlib.sha256(json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")).hexdigest()


def _entry(role: str, *, host: str | None, wsl: str | None, container: str | None, source: str, level: str, note: str = "") -> dict[str, Any]:
    if level not in OFFLINE_LEVELS:
        raise _fail("verification_level_unreachable", f"{level!r} cannot be claimed offline; allowed {list(OFFLINE_LEVELS)}")
    return {"role": role, "host_path": host, "wsl_path": wsl, "container_path": container, "mapping_source": source, "verification_level": level, "runtime_status": RUNTIME_STATUS,
            "note": note}


def build_path_record(plan: Mapping[str, Any], *, repo_root_windows: str, repo_declared_wsl: str, repo_declaration_source: str, mount_root: str | None,
                      container_mount: ContainerMount | None, workspace_declaration_source: str, extra_artifact_hashes: Mapping[str, str] | None = None) -> dict[str, Any]:
    """The path-mapping record of one plan. ``mount_root`` of ``None`` means 'derive it from the declared repository pair'.

    Every pair the plan or the request names is checked in the direction that matters: the operator-supplied WSL form must equal the rule's form of the host path.
    A disagreement raises (``operator_path_disagrees_with_rule``): the record is not written with a known-wrong pair.
    """
    request, layout, tools = plan["request"], plan["layout"], plan["request"]["tools"]
    derived = derive_mount_root(repo_root_windows, repo_declared_wsl)
    if mount_root is not None and check_mount_root(mount_root) != derived:
        raise _fail("mount_root_mismatch", f"the supplied automount root {mount_root!r} differs from the root {derived!r} implied by the repository declaration")
    root = derived
    entries: list[dict[str, Any]] = []

    def pair(role: str, host_path: str, operator_wsl: str, source: str) -> None:
        wanted = windows_to_wsl(host_path, mount_root=root)
        if normalize_posix(operator_wsl, what=f"{role} (operator WSL path)") != wanted:
            raise _fail("operator_path_disagrees_with_rule", f"{role}: operator WSL path {operator_wsl!r} differs from {wanted!r}, the WSL form of {host_path!r}")
        entries.append(_entry(role, host=host_path, wsl=wanted, container=None, source=source, level=L_OPERATOR, note="the operator typed both sides; both agree with the drive-mount rule"))

    entries.append(_entry("repository", host=repo_root_windows, wsl=windows_to_wsl(repo_root_windows, mount_root=root), container=None, source=repo_declaration_source, level=L_REPO,
                          note="the WSL form equals the repository declaration; that /mnt/<drive> is the automount root in the target distro is not read from the distro"))
    pair("run_output_directory", layout["output_dir"], tools["output_dir_wsl"], "request.output_dir vs request.tools.output_dir_wsl")
    pair("rendered_overlay_directory", layout["launch_authorizing_directory"], tools["rendered_dir_wsl"], "plan.layout.launch_authorizing_directory vs request.tools.rendered_dir_wsl")
    pair("network_description", layout["network_description"], tools["network_description_wsl"], "plan.layout.network_description (A0m render plan.json) vs request.tools.network_description_wsl")
    if normalize_posix(tools["wsl_repo_path"], what="wsl_repo_path") != normalize_posix(repo_declared_wsl, what="repo declaration"):
        raise _fail("operator_path_disagrees_with_rule", f"request.tools.wsl_repo_path {tools['wsl_repo_path']!r} differs from the declared repository path {repo_declared_wsl!r}")
    ws_wsl = layout["caliper_workspace_host"]
    run_wsl = layout["worker_evidence_root_host"]
    if container_mount is not None:
        if normalize_posix(ws_wsl) != container_mount.wsl_root:
            raise _fail("mount_root_mismatch", f"the plan's Caliper workspace {ws_wsl!r} is not the mounted WSL directory {container_mount.wsl_root!r}")
        if container_mount.container_root != plan["mounts"]["container_workspace"]:
            raise _fail("mount_root_mismatch", "the plan's container workspace differs from the declared container mount")
        if container_mount.to_container(run_wsl) != layout["container_run_dir"]:
            raise _fail("mount_root_mismatch", f"{run_wsl!r} maps to {container_mount.to_container(run_wsl)!r}, the plan says {layout['container_run_dir']!r}")
        entries.append(_entry("caliper_workspace", host=None, wsl=container_mount.wsl_root, container=container_mount.container_root, source=workspace_declaration_source, level=L_REPO,
                              note=f"container {container_mount.container_name!r}: bind mount taken from a repository declaration; whether the container exists and mounts this is NOT verified"))
        entries.append(_entry("worker_run_directory", host=None, wsl=run_wsl, container=layout["container_run_dir"], source="plan.layout (derived from the render's recorder directories) + the bind mount",
                              level=L_REPO, note="WSL-native path: it has no Windows drive equivalent, so a Windows-side reader cannot open it without a UNC path (unsupported)"))
    else:
        entries.append(_entry("caliper_workspace", host=None, wsl=ws_wsl, container=plan["mounts"]["container_workspace"], source="plan.layout only; no mount declaration supplied",
                              level=L_SYNTACTIC, note="UNVERIFIED: an explicit operator-supplied mount mapping is required before this can be trusted"))
    for rnd in layout["rounds"]:
        entries.append(_entry(f"round_{rnd['round_index']}_recorder_directory", host=None, wsl=posixpath.dirname(rnd["recorder_files"][".csv"]),
                              container=(container_mount.to_container(posixpath.dirname(rnd["recorder_files"][".csv"])) if container_mount is not None else None),
                              source="plan.layout rounds[].recorder_files", level=(L_REPO if container_mount is not None else L_SYNTACTIC)))
    record = {"schema": RECORD_SCHEMA, "version": PATH_MAPPING_VERSION, "run_id": plan["run_id"], "variant": plan["variant"], "channel": plan["channel"], "plan_sha256": plan["plan_sha256"],
              "automount_root": root, "automount_root_source": f"derived from the declared pair {repo_root_windows!r} <-> {repo_declared_wsl!r} ({repo_declaration_source})",
              "entries": entries, "artifact_hashes": dict(plan["artifact_hashes"]), "extra_artifact_hashes": dict(extra_artifact_hashes or {}),
              "highest_offline_level_reached": L_REPO, "runtime_verified": False,
              "claims_not_made": ["any path was opened inside a real WSL distribution or container", "the automount root in the target distribution equals the derived one",
                                  "the Caliper container exists, is running, or has the declared bind mount", "WSL-native directories are visible from the Windows host"]}
    record["record_sha256"] = _digest({k: v for k, v in record.items() if k != "record_sha256"})
    return record


def verify_path_record(record: Mapping[str, Any]) -> list[dict[str, str]]:
    problems: list[dict[str, str]] = []
    if record.get("schema") != RECORD_SCHEMA:
        return [{"code": "record_schema", "message": f"schema {record.get('schema')!r}"}]
    if _digest({k: v for k, v in record.items() if k != "record_sha256"}) != record.get("record_sha256"):
        problems.append({"code": "record_digest", "message": "the record was edited after it was written"})
    if record.get("runtime_verified") is not False:
        problems.append({"code": "runtime_claim", "message": "an offline record may not claim runtime verification"})
    for entry in record.get("entries", []):
        if entry.get("verification_level") not in OFFLINE_LEVELS:
            problems.append({"code": "level_unreachable", "message": f"{entry.get('role')}: {entry.get('verification_level')!r}"})
        if entry.get("runtime_status") != RUNTIME_STATUS:
            problems.append({"code": "runtime_status", "message": f"{entry.get('role')}: runtime_status must be {RUNTIME_STATUS!r}"})
    return problems
