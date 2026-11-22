"""Deterministic file / directory-tree digest for the WSL-side integrity step (task A0u, 2026-11-22). OFFLINE library; standard library only (no ``os``, no process, no network).

Why. The A0s plan's step X13 hashes the worker evidence root, a WSL-native directory (``/home/<user>/...``), through ``Deps.hash_tree`` on the WINDOWS host. A Windows process cannot open
that path (UNC forms are refused by ``path_mapping``); ``Path('/home/x').resolve()`` becomes ``C:\\home\\x``, which does not exist, and the reference ``hash_tree`` then returns ``{}``: an EMPTY
manifest that looks like a successful verification (reproduced in ``tests/test_a0u_x13_2026_11_22.py``). This module is the part of the repair that runs INSIDE the WSL distribution (through
``scripts/e1/e1_wsl_tree_digest.py``, a structured ``wsl.exe --exec`` vector); it is also what the Windows side uses to re-derive and verify what came back.

What X13 means (read from ``bounded_execution.py``): after the run, "hash every file under the output directory and the worker evidence root" so the evidence has a content manifest. It is a
post-run evidence digest, not a pre/post comparison. A post-run digest alone cannot show that the staged files were the authorized ones, so the successor (``wsl_integrity``) also digests the
staged run directory BEFORE the workload and compares both with the authorized overlay hashes.

Definition ``a0u_tree_digest/1`` (nothing is substituted silently):

* the TARGET is a regular file or a directory; the caller states which (``expected_type``) and a mismatch is a refusal;
* for a directory every descendant is an entry: ``{"path", "type": "dir"}``, ``{"path", "type": "file", "size", "sha256"}`` (SHA-256 of the file CONTENT) or, only with the policy
  ``record_internal``, ``{"path", "type": "symlink", "link_target"}`` (never followed); for a file the single entry has the path ``"."``;
* ``path`` is relative to the target, ``/``-separated, with no empty, ``.`` or ``..`` segment; entries are sorted by the UTF-8 bytes of the path;
* ``tree_digest`` = SHA-256 of the canonical JSON (sorted keys, no spaces, ASCII) of ``{"schema": "a0u_tree_digest/1", "entries": [...]}``;
* modification times are NEVER hashed or used as a substitute for content; they (with size, inode and device) are only compared before and after reading a file to detect a change.

Refused, never repaired (stable ``DigestRefused.code``): a missing target or ancestor, a wrong type, a symlink as target, as an ancestor or (default policy) inside the tree, a symlink that
escapes the tree, a special file (device, FIFO, socket), a permission error, any other I/O error, a name that is not UTF-8 or holds a control character or a backslash, a bound exceeded
(entries, bytes, depth; every bound is required), a file whose size, mtime, inode or device changed while it was read, a directory whose listing changed while it was walked.

Kept compatible with Python 3.9 (the WSL system interpreter is unknown): no ``match``, no ``X | Y`` outside annotations.
"""

from __future__ import annotations

import hashlib
import json
import posixpath
import re
import stat as stat_mod
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence

TREE_DIGEST_VERSION = "tree_digest 0.1 (A0u, 2026-11-22)"
TREE_SCHEMA = "a0u_tree_digest/1"
EVIDENCE_SCHEMA = "a0u_tree_digest_evidence/1"
HASH_ALGORITHM = "sha256"
OPERATION = "wsl_integrity_digest"
EXPECTED_TYPES = ("file", "directory")
SYMLINK_POLICIES = ("refuse", "record_internal")
PHASES = ("pre_workload", "post_workload")
HARD_MAX_ENTRIES, HARD_MAX_BYTES, HARD_MAX_DEPTH = 1_000_000, 1 << 40, 64
RUN_ID = re.compile(r"[A-Za-z0-9_.-]{1,40}")
DISTRO = re.compile(r"[A-Za-z0-9_.-]{1,64}")
SHA = re.compile(r"[0-9a-f]{64}")
_BAD_NAME = re.compile(r"[\x00-\x1f\x7f\\/]")
_BAD_PATH = re.compile(r"[\x00-\x1f\x7f\\]")
READ_CHUNK = 1 << 20
DIGEST_DEFINITION = ("a0u_tree_digest/1: sha256 over canonical JSON {schema, entries}; entries sorted by UTF-8 path bytes; file entries carry the SHA-256 of the file content; "
                     "mtime is never hashed; symlinks refused unless policy record_internal (recorded, never followed)")
REQUEST_KEYS = ("run_id", "variant", "plan_sha256", "distro", "phase", "operation", "approved_root", "target", "expected_type", "symlink_policy", "max_entries", "max_total_bytes",
                "max_depth", "path_record_sha256")


class DigestRefused(ValueError):
    """The digest cannot be produced. ``code`` is a stable machine-readable cause; the tree is reported, never repaired."""

    def __init__(self, code: str, message: str):
        super().__init__("%s [%s]" % (message, code))
        self.code = code
        self.detail = message


# ------------------------------------------------------------------------------------------------ POSIX string rules
def posix_segments(path: Any, *, what: str = "path") -> List[str]:
    """The segments of an absolute, already-normalised POSIX path, or :class:`DigestRefused`. Nothing is normalised for the caller."""
    if not isinstance(path, str) or not path.startswith("/"):
        raise DigestRefused("path_not_absolute", "%s %r is not an absolute POSIX path" % (what, path))
    if _BAD_PATH.search(path):
        raise DigestRefused("path_character_invalid", "%s %r contains a control character or a backslash" % (what, path))
    if path != "/" and path.endswith("/"):
        raise DigestRefused("path_not_normalised", "%s %r ends with a slash" % (what, path))
    segments = path.split("/")[1:] if path != "/" else []
    for seg in segments:
        if seg == "..":
            raise DigestRefused("path_traversal", "%s %r contains a '..' segment" % (what, path))
        if seg in ("", "."):
            raise DigestRefused("path_not_normalised", "%s %r contains an empty or '.' segment" % (what, path))
    return segments


def is_within(path: str, root: str) -> bool:
    p, r = posix_segments(path), posix_segments(root)
    return p[: len(r)] == r


# ------------------------------------------------------------------------------------------------ file-system view
class RealFs:
    """pathlib view of the local file system. ``path_factory`` maps a POSIX string to a ``Path`` (default ``Path``); tests map into a temporary directory."""

    def __init__(self, path_factory: Optional[Callable[[str], Path]] = None):
        self._p = path_factory or Path

    def lstat(self, path: str) -> Any:
        return self._p(path).lstat()

    def listdir(self, path: str) -> List[str]:
        return [child.name for child in self._p(path).iterdir()]

    def open_read(self, path: str) -> Any:
        return self._p(path).open("rb")

    def readlink(self, path: str) -> str:
        return str(self._p(path).readlink())


def _signature(st: Any) -> tuple:
    return (getattr(st, "st_mode", None), getattr(st, "st_size", None), getattr(st, "st_mtime_ns", None), getattr(st, "st_ino", None), getattr(st, "st_dev", None))


def _io(call: Callable[[], Any], path: str, *, missing_code: str = "target_missing") -> Any:
    try:
        return call()
    except FileNotFoundError:
        raise DigestRefused(missing_code, "%s does not exist" % path)
    except PermissionError:
        raise DigestRefused("permission_denied", "permission denied: %s" % path)
    except NotADirectoryError:
        raise DigestRefused("not_a_directory", "%s is not a directory" % path)
    except OSError as exc:
        raise DigestRefused("io_error", "%s: %s" % (path, type(exc).__name__))


def _kind(st: Any, path: str) -> str:
    mode = getattr(st, "st_mode", 0)
    if stat_mod.S_ISLNK(mode):
        return "symlink"
    if stat_mod.S_ISDIR(mode):
        return "dir"
    if stat_mod.S_ISREG(mode):
        return "file"
    raise DigestRefused("special_file", "%s is not a regular file, directory or symlink (device, FIFO or socket)" % path)


def _name_ok(name: str, parent: str) -> str:
    if not name or name in (".", "..") or _BAD_NAME.search(name):
        raise DigestRefused("name_invalid", "an entry of %s has an empty name or a control character, slash or backslash" % parent)
    try:
        name.encode("utf-8")
    except UnicodeEncodeError:
        raise DigestRefused("path_not_utf8", "an entry of %s is not valid UTF-8" % parent)
    return name


def canonical_json(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def tree_digest_of(entries: Sequence[Mapping[str, Any]]) -> str:
    return hashlib.sha256(canonical_json({"schema": TREE_SCHEMA, "entries": list(entries)})).hexdigest()


def _hash_file(fs: Any, path: str, st: Any, counters: Dict[str, int], max_total_bytes: int) -> Dict[str, Any]:
    size = getattr(st, "st_size", None)
    if not isinstance(size, int) or size < 0:
        raise DigestRefused("io_error", "%s has no usable size" % path)
    counters["bytes"] += size
    if counters["bytes"] > max_total_bytes:
        raise DigestRefused("bound_exceeded", "total size exceeds max_total_bytes (%d)" % max_total_bytes)
    digest, read = hashlib.sha256(), 0
    handle = _io(lambda: fs.open_read(path), path)
    try:
        while True:
            chunk = _io(lambda: handle.read(READ_CHUNK), path)
            if not chunk:
                break
            digest.update(chunk)
            read += len(chunk)
    finally:
        handle.close()
    after = _io(lambda: fs.lstat(path), path, missing_code="changed_during_enumeration")
    if _signature(after) != _signature(st) or read != size:
        raise DigestRefused("changed_during_enumeration", "%s changed while it was read (size, mtime, inode or device differ, or %d bytes read of %d)" % (path, read, size))
    return {"type": "file", "size": size, "sha256": digest.hexdigest()}


def _walk(fs: Any, abs_dir: str, rel: str, depth: int, root_abs: str, policy: str, limits: Mapping[str, int], counters: Dict[str, int], out: List[Dict[str, Any]]) -> None:
    if depth > limits["max_depth"]:
        raise DigestRefused("bound_exceeded", "directory depth exceeds max_depth (%d)" % limits["max_depth"])
    names = [_name_ok(n, abs_dir) for n in _io(lambda: fs.listdir(abs_dir), abs_dir)]
    if len(set(names)) != len(names):
        raise DigestRefused("name_invalid", "%s lists a name twice" % abs_dir)
    names.sort(key=lambda n: n.encode("utf-8"))
    for name in names:
        child_abs = abs_dir.rstrip("/") + "/" + name
        child_rel = name if not rel else rel + "/" + name
        counters["entries"] += 1
        if counters["entries"] > limits["max_entries"]:
            raise DigestRefused("bound_exceeded", "entry count exceeds max_entries (%d)" % limits["max_entries"])
        st = _io(lambda: fs.lstat(child_abs), child_abs, missing_code="changed_during_enumeration")
        kind = _kind(st, child_abs)
        if kind == "symlink":
            if policy != "record_internal":
                raise DigestRefused("symlink_refused", "%s is a symbolic link; the policy is %r" % (child_abs, policy))
            link = _io(lambda: fs.readlink(child_abs), child_abs)
            resolved = posixpath.normpath(link if link.startswith("/") else posixpath.join(abs_dir, link))
            if not (resolved == root_abs or resolved.startswith(root_abs.rstrip("/") + "/")):
                raise DigestRefused("symlink_escapes_root", "%s points outside the digested tree" % child_abs)
            out.append({"path": child_rel, "type": "symlink", "link_target": link})
        elif kind == "dir":
            out.append({"path": child_rel, "type": "dir"})
            _walk(fs, child_abs, child_rel, depth + 1, root_abs, policy, limits, counters, out)
        else:
            entry = _hash_file(fs, child_abs, st, counters, limits["max_total_bytes"])
            out.append({"path": child_rel, **entry})
    again = sorted(_name_ok(n, abs_dir) for n in _io(lambda: fs.listdir(abs_dir), abs_dir))
    if again != sorted(names):
        raise DigestRefused("changed_during_enumeration", "the listing of %s changed while it was walked" % abs_dir)


def check_ancestors(fs: Any, approved_root: str, target: str) -> None:
    """Every directory from ``approved_root`` down to the parent of ``target`` must exist and be a plain directory (no symlink): a symlinked ancestor could redirect the walk."""
    root_segments, target_segments = posix_segments(approved_root, what="approved_root"), posix_segments(target, what="target")
    if target_segments[: len(root_segments)] != root_segments:
        raise DigestRefused("outside_approved_root", "target %r is not inside the approved root %r" % (target, approved_root))
    for depth in range(len(root_segments), len(target_segments)):
        path = "/" + "/".join(target_segments[:depth])
        st = _io(lambda: fs.lstat(path), path, missing_code="ancestor_missing")
        kind = _kind(st, path)
        if kind != "dir":
            raise DigestRefused("ancestor_symlink" if kind == "symlink" else "ancestor_not_a_directory", "%s is a %s, not a plain directory" % (path, kind))


def digest_tree(fs: Any, target: str, *, approved_root: str, expected_type: str, symlink_policy: str, max_entries: int, max_total_bytes: int, max_depth: int) -> Dict[str, Any]:
    """The tree record of ``target``. Raises :class:`DigestRefused`. Reads only; writes nothing."""
    if expected_type not in EXPECTED_TYPES or symlink_policy not in SYMLINK_POLICIES:
        raise DigestRefused("request_invalid", "expected_type or symlink_policy is not one of the allowed values")
    for name, value, ceiling in (("max_entries", max_entries, HARD_MAX_ENTRIES), ("max_total_bytes", max_total_bytes, HARD_MAX_BYTES), ("max_depth", max_depth, HARD_MAX_DEPTH)):
        if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= ceiling:
            raise DigestRefused("bound_invalid", "%s must be an integer in 1..%d (required, no default)" % (name, ceiling))
    posix_segments(target, what="target")
    check_ancestors(fs, approved_root, target)
    st = _io(lambda: fs.lstat(target), target)
    kind = _kind(st, target)
    if kind == "symlink":
        raise DigestRefused("target_is_symlink", "%s is a symbolic link; the target must be the real file or directory" % target)
    if (expected_type == "file") != (kind == "file"):
        raise DigestRefused("type_mismatch", "%s is a %s, expected a %s" % (target, kind, expected_type))
    limits = {"max_entries": max_entries, "max_total_bytes": max_total_bytes, "max_depth": max_depth}
    counters = {"entries": 0, "bytes": 0}
    entries: List[Dict[str, Any]] = []
    if kind == "file":
        counters["entries"] = 1
        entries.append({"path": ".", **_hash_file(fs, target, st, counters, max_total_bytes)})
    else:
        _walk(fs, target, "", 1, target, symlink_policy, limits, counters, entries)
        after = _io(lambda: fs.lstat(target), target, missing_code="changed_during_enumeration")
        if _kind(after, target) != "dir":
            raise DigestRefused("changed_during_enumeration", "%s changed type while it was walked" % target)
    entries.sort(key=lambda e: e["path"].encode("utf-8"))
    kinds = [e["type"] for e in entries]
    return {"entries": entries, "entry_count": len(entries), "file_count": kinds.count("file"), "dir_count": kinds.count("dir"), "symlink_count": kinds.count("symlink"),
            "total_bytes": sum(e.get("size", 0) for e in entries), "tree_digest": tree_digest_of(entries)}


# ------------------------------------------------------------------------------------------------ request and evidence document
def validate_request(request: Any) -> List[Dict[str, str]]:
    """Every problem with a digest request (an empty list means none). There is no default for any field."""
    issues: List[Dict[str, str]] = []

    def bad(code: str, field: str, message: str) -> None:
        issues.append({"code": code, "field": field, "message": message})

    if not isinstance(request, Mapping):
        return [{"code": "not_an_object", "field": "<request>", "message": "the request must be an object"}]
    for key in REQUEST_KEYS:
        if request.get(key) is None:
            bad("missing", key, "no value was supplied; there is no default")
    extra = sorted(set(request) - set(REQUEST_KEYS))
    if extra:
        bad("unknown_key", "<request>", "unknown key(s) %s" % extra)
    if issues:
        return issues
    if not (isinstance(request["run_id"], str) and RUN_ID.fullmatch(request["run_id"])):
        bad("run_id_invalid", "run_id", "must match %s" % RUN_ID.pattern)
    if not (isinstance(request["variant"], str) and re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", request["variant"])):
        bad("variant_invalid", "variant", "must be a short identifier")
    for key in ("plan_sha256", "path_record_sha256"):
        if not (isinstance(request[key], str) and SHA.fullmatch(request[key])):
            bad("hash_invalid", key, "must be 64 lowercase hex characters")
    if not (isinstance(request["distro"], str) and DISTRO.fullmatch(request["distro"])):
        bad("distro_invalid", "distro", "the WSL distribution name is required (no default)")
    if request["phase"] not in PHASES:
        bad("phase_invalid", "phase", "must be one of %s" % list(PHASES))
    if request["operation"] != OPERATION:
        bad("operation_invalid", "operation", "must be %r" % OPERATION)
    if request["expected_type"] not in EXPECTED_TYPES:
        bad("expected_type_invalid", "expected_type", "must be one of %s" % list(EXPECTED_TYPES))
    if request["symlink_policy"] not in SYMLINK_POLICIES:
        bad("symlink_policy_invalid", "symlink_policy", "must be one of %s" % list(SYMLINK_POLICIES))
    for key, ceiling in (("max_entries", HARD_MAX_ENTRIES), ("max_total_bytes", HARD_MAX_BYTES), ("max_depth", HARD_MAX_DEPTH)):
        value = request[key]
        if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= ceiling:
            bad("bound_invalid", key, "must be an integer in 1..%d" % ceiling)
    try:
        root, target = posix_segments(request["approved_root"], what="approved_root"), posix_segments(request["target"], what="target")
        if len(root) < 2:
            bad("approved_root_too_broad", "approved_root", "an approved root needs at least two segments")
        if target[: len(root)] != root:
            bad("outside_approved_root", "target", "the target is not inside the approved root")
    except DigestRefused as exc:
        bad(exc.code, "approved_root/target", exc.detail)
    return issues


def _document_digest(document: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json({k: v for k, v in document.items() if k != "document_sha256"})).hexdigest()


def build_evidence(request: Mapping[str, Any], fs: Any, *, observed_distro: Optional[str], observed_utc: str, platform: str, tool_sha256: Optional[str] = None,
                   library_sha256: Optional[str] = None) -> Dict[str, Any]:
    """The evidence document for one request. A refusal is a document too (``result.verdict == 'REFUSED'``), so the caller always has something to store and cannot mistake it for success."""
    binding = {key: request.get(key) for key in REQUEST_KEYS} if isinstance(request, Mapping) else {}
    document: Dict[str, Any] = {
        "schema": EVIDENCE_SCHEMA, "tool_version": TREE_DIGEST_VERSION, "hash_algorithm": HASH_ALGORITHM, "digest_definition": DIGEST_DEFINITION, "binding": binding,
        "observation": {"observed_utc": observed_utc, "observed_distro": observed_distro, "platform": platform, "tool_sha256": tool_sha256, "library_sha256": library_sha256, "encoding": "utf-8",
                        "mtime_used_as_hash": False, "mtime_used_only_for_change_detection": True},
    }
    issues = validate_request(request)
    try:
        if issues:
            raise DigestRefused("request_invalid", "; ".join("%s: %s [%s]" % (i["field"], i["message"], i["code"]) for i in issues))
        if observed_distro != request["distro"]:
            raise DigestRefused("distro_mismatch", "this process runs in distribution %r, the request names %r" % (observed_distro, request["distro"]))
        document["tree"] = digest_tree(fs, request["target"], approved_root=request["approved_root"], expected_type=request["expected_type"], symlink_policy=request["symlink_policy"],
                                       max_entries=request["max_entries"], max_total_bytes=request["max_total_bytes"], max_depth=request["max_depth"])
        document["result"] = {"verdict": "DIGESTED", "code": None, "message": None}
    except DigestRefused as exc:
        document["tree"] = None
        document["result"] = {"verdict": "REFUSED", "code": exc.code, "message": exc.detail}
    document["document_sha256"] = _document_digest(document)
    return document


def verify_evidence(document: Any) -> List[Dict[str, str]]:
    """Structural problems of an evidence document, including a recomputation of its own digests. An empty list means the document is internally consistent (not that it is genuine)."""
    problems: List[Dict[str, str]] = []

    def bad(code: str, message: str) -> None:
        problems.append({"code": code, "message": message})

    if not isinstance(document, Mapping) or document.get("schema") != EVIDENCE_SCHEMA:
        return [{"code": "evidence_schema", "message": "not an %s document" % EVIDENCE_SCHEMA}]
    if document.get("hash_algorithm") != HASH_ALGORITHM:
        bad("hash_algorithm", "hash_algorithm is %r" % document.get("hash_algorithm"))
    if document.get("document_sha256") != _document_digest(document):
        bad("document_digest", "the document was edited after it was written")
    result = document.get("result") or {}
    if result.get("verdict") != "DIGESTED":
        bad("not_digested", "verdict %r (%r)" % (result.get("verdict"), result.get("code")))
        return problems
    tree = document.get("tree")
    if not isinstance(tree, Mapping) or not isinstance(tree.get("entries"), list):
        bad("tree_missing", "a DIGESTED document needs a tree")
        return problems
    entries = tree["entries"]
    paths = [e.get("path") for e in entries if isinstance(e, Mapping)]
    if len(paths) != len(entries) or any(not isinstance(p, str) for p in paths):
        bad("entry_malformed", "every entry needs a text path")
        return problems
    if paths != sorted(paths, key=lambda p: p.encode("utf-8")) or len(set(paths)) != len(paths):
        bad("entries_not_sorted_or_duplicated", "entries must be unique and sorted by UTF-8 path bytes")
    expected_keys = {"file": {"path", "type", "size", "sha256"}, "dir": {"path", "type"}, "symlink": {"path", "type", "link_target"}}
    for entry in entries:
        keys = expected_keys.get(entry.get("type"))
        if keys is None or set(entry) != keys:
            bad("entry_malformed", "entry %r has the wrong type or keys" % entry.get("path"))
        elif entry["type"] == "file" and not (isinstance(entry["sha256"], str) and SHA.fullmatch(entry["sha256"]) and isinstance(entry["size"], int) and entry["size"] >= 0):
            bad("entry_malformed", "file entry %r has a bad hash or size" % entry.get("path"))
        if entry.get("path") not in (None, ".") and (entry["path"].startswith("/") or ".." in entry["path"].split("/") or "" in entry["path"].split("/")):
            bad("entry_path_invalid", "entry path %r is not relative and normalised" % entry["path"])
    if tree.get("tree_digest") != tree_digest_of(entries):
        bad("tree_digest", "tree_digest does not match the entries")
    kinds = [e.get("type") for e in entries]
    for key, want in (("entry_count", len(entries)), ("file_count", kinds.count("file")), ("dir_count", kinds.count("dir")), ("symlink_count", kinds.count("symlink")),
                      ("total_bytes", sum(e.get("size", 0) for e in entries if isinstance(e.get("size", 0), int)))):
        if tree.get(key) != want:
            bad("count_mismatch", "%s is %r, recomputed %r" % (key, tree.get(key), want))
    binding = document.get("binding") or {}
    if len(entries) > binding.get("max_entries", 0) or tree.get("total_bytes", 0) > binding.get("max_total_bytes", 0):
        bad("bound_exceeded", "the tree exceeds the bounds recorded in its own binding")
    return problems


def flat_file_map(document: Mapping[str, Any]) -> Dict[str, str]:
    """``{relative path: sha256}`` of the file entries of a verified DIGESTED document (the shape the A0s integrity manifest uses)."""
    return {e["path"]: e["sha256"] for e in document["tree"]["entries"] if e["type"] == "file"}
