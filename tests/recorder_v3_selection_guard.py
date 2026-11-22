"""Recorder-v3 selection guard (task A0q, 2026-11-22). Replaces the filename/text scan of ``test_a0j_integrity::test_42`` with a syntax-aware one.

What the guard protects: nothing may SELECT recorder v3 (``e1_per_tx_recorder_v3``) implicitly. A launcher, a legacy script, a configuration file or an environment variable that
makes v3 the recorder without an explicit, versioned, fail-closed choice is a defect. A file that merely MENTIONS v3 (a docstring, a comment, a recorded evidence document, a test)
is not a selection and is not forbidden.

How it decides (it does not whitelist directories):

* PYTHON is parsed. A mention counts only as a string LITERAL in executable code. Docstrings and comments are not literals in the syntax tree, so descriptive mentions need no
  allow-list entry. A literal that sits in a default argument, a dynamic ``"e1_per_tx_recorder_" + version`` construction, or an environment-variable lookup that names a recorder is
  a finding wherever it is. Every remaining literal must be in ``REGISTERED_PYTHON`` (three files, each with its stated, tested role); anything else is a finding.
* JAVASCRIPT, shell and YAML are read with their comments removed; any remaining mention outside the recorder's own file is a finding, and so is a Caliper ``module:`` line
  naming v3 anywhere.
* JSON is parsed. A document shaped like a Caliper benchmark (``monitors`` or ``test.rounds``) that names v3, or any ``module`` key whose value names v3, is a finding. A recorded
  evidence document that names v3 under other keys (``monitor_module``, ``selected_recorder_file``, prose) is a record, not a selection, and is listed but not forbidden.

``scan_tree(root)`` takes the root so a test can run the SAME scan over a synthetic tree containing an injected unsafe selector and prove it is caught.
"""

from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

V3_STEM = "e1_per_tx_recorder_v3"
V3_FILE = "scripts/e1/caliper/e1_per_tx_recorder_v3.js"
SCAN_ROOTS = ("src", "scripts", "Fabric_Live_Testbed", "Integration", "p4_replay", "fabric-testbed")
SUFFIXES = {".py", ".js", ".yaml", ".yml", ".json", ".sh"}
SKIP_PARTS = ("__pycache__", "node_modules")

# Python files allowed to hold the v3 name as a string literal, and why. Each role is exercised by a behavioural test in test_a0q_recorder_v3_guard_2026_11_22.py.
REGISTERED_PYTHON = {
    "scripts/e1/e1_a0m_dry_render.py": "EXPLICIT SELECTOR: its parameter file must name recorder v3; the field is required, has no default and only this one value is accepted; no execute phase",
    "src/fabric_perf/controllers/launch_variant.py": "VARIANT CLASSIFIER: derives the variant from a generated benchmark and refuses a mixed or unknown one; it selects nothing and has no fallback",
    "scripts/e1/e1_a0j_candidate_manifest.py": "INTEGRITY INVENTORY: lists the recorder's path in a candidate manifest; it loads and selects nothing",
    "Integration/system_integration/a0j_2026-11-21/mutation_check.py": "MUTATION TARGET LIST (A0j evidence tool): names the recorder file it temporarily mutates and restores, and the test that must kill each mutant; it launches nothing",
}
ENV_READERS = {"getenv", "get"}
RECORDER_WORD = re.compile(r"recorder", re.I)


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    kind: str
    text: str

    def as_tuple(self) -> tuple:
        return (self.path, self.kind)


def _is_v3(text: str) -> bool:
    return V3_STEM in text


def _docstring_nodes(tree: ast.AST) -> set[int]:
    out: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", [])
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant) and isinstance(body[0].value.value, str):
                out.add(id(body[0].value))
    return out


def _default_nodes(tree: ast.AST) -> set[int]:
    out: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            for default in list(node.args.defaults) + [d for d in node.args.kw_defaults if d is not None]:
                for inner in ast.walk(default):
                    out.add(id(inner))
    return out


def scan_python(rel: str, text: str) -> list[Finding]:
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return [Finding(rel, 0, "unparseable_python_naming_v3", "") ] if _is_v3(text) else []
    docs, defaults = _docstring_nodes(tree), _default_nodes(tree)
    found: list[Finding] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docs:
            if _is_v3(node.value):
                kind = "default_argument" if id(node) in defaults else "python_literal"
                found.append(Finding(rel, node.lineno, kind, node.value[:80]))
        if isinstance(node, ast.JoinedStr):
            parts = [v.value for v in node.values if isinstance(v, ast.Constant) and isinstance(v.value, str)]
            if any("e1_per_tx_recorder_" in p for p in parts) and any(isinstance(v, ast.FormattedValue) for v in node.values):
                found.append(Finding(rel, node.lineno, "dynamic_recorder_selection", "".join(parts)[:80]))
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            side = [n.value for n in (node.left, node.right) if isinstance(n, ast.Constant) and isinstance(n.value, str)]
            if any(s.startswith("e1_per_tx_recorder_") and not s.endswith(".js") and s != V3_STEM for s in side):
                found.append(Finding(rel, node.lineno, "dynamic_recorder_selection", side[0][:80]))
        # an environment lookup whose key names a recorder is a hidden selector
        if isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
            owner = ast.unparse(func.value) if isinstance(func, ast.Attribute) else ""
            if name in ENV_READERS and ("environ" in owner or name == "getenv" or owner == "os"):
                for arg in node.args[:1]:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and RECORDER_WORD.search(arg.value):
                        found.append(Finding(rel, node.lineno, "environment_selection", arg.value))
        if isinstance(node, ast.Subscript) and "environ" in ast.unparse(node.value):
            key = node.slice
            if isinstance(key, ast.Constant) and isinstance(key.value, str) and RECORDER_WORD.search(key.value):
                found.append(Finding(rel, node.lineno, "environment_selection", key.value))
    return found


def _strip_js_comments(text: str) -> str:
    out, i, n, quote = [], 0, len(text), ""
    while i < n:
        ch = text[i]
        if quote:
            out.append(ch)
            if ch == "\\" and i + 1 < n:
                out.append(text[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = ""
            i += 1
        elif ch in "'\"`":
            quote = ch
            out.append(ch)
            i += 1
        elif text.startswith("//", i):
            while i < n and text[i] != "\n":
                i += 1
        elif text.startswith("/*", i):
            end = text.find("*/", i + 2)
            i = n if end < 0 else end + 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _strip_hash_comments(text: str) -> str:
    return "\n".join(re.sub(r"(^|\s)#.*$", "", line) for line in text.splitlines())


def _walk_json(value, path=()):
    if isinstance(value, dict):
        for key, inner in value.items():
            if isinstance(key, str) and _is_v3(key):
                yield path + (key,), key, True
            yield from _walk_json(inner, path + (key,))
    elif isinstance(value, list):
        for index, inner in enumerate(value):
            yield from _walk_json(inner, path + (index,))
    elif isinstance(value, str) and _is_v3(value):
        yield path, value, False


def scan_json(rel: str, text: str) -> list[Finding]:
    if not _is_v3(text):
        return []
    try:
        doc = json.loads(text)
    except ValueError:
        return [Finding(rel, 0, "unparseable_json_naming_v3", "")]
    caliper_shaped = isinstance(doc, dict) and ("monitors" in doc or (isinstance(doc.get("test"), dict) and "rounds" in doc["test"]))
    found: list[Finding] = []
    for path, value, _is_key in _walk_json(doc):
        last = next((p for p in reversed(path) if isinstance(p, str)), "")
        if last == "module":
            found.append(Finding(rel, 0, "caliper_module_selection", value[:80]))
        elif caliper_shaped:
            found.append(Finding(rel, 0, "benchmark_config_naming_v3", value[:80]))
    return found


def scan_text(rel: str, text: str, suffix: str) -> list[Finding]:
    if suffix == ".json":
        return scan_json(rel, text)
    if suffix == ".py":
        return scan_python(rel, text)
    body = _strip_js_comments(text) if suffix == ".js" else _strip_hash_comments(text)
    found = [Finding(rel, 0, "text_mention", V3_STEM)] if _is_v3(body) else []
    if suffix in (".yaml", ".yml", ".sh") and re.search(r"module\s*:\s*\S*" + V3_STEM, body):
        found.append(Finding(rel, 0, "caliper_module_selection", V3_STEM))
    return found


def iter_files(root: Path, roots: Iterable[str] = SCAN_ROOTS):
    for sub in roots:
        base = Path(root) / sub
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if path.is_file() and path.suffix in SUFFIXES and not any(part in path.as_posix() for part in SKIP_PARTS):
                yield path


def scan_tree(root: Path, *, registered: dict[str, str] | None = None, roots: Iterable[str] = SCAN_ROOTS) -> tuple[list[Finding], list[str]]:
    """Return ``(violations, recorded_mentions)``. A violation is a selection-shaped mention; ``recorded_mentions`` are evidence documents that only record the name."""
    registered = REGISTERED_PYTHON if registered is None else registered
    violations: list[Finding] = []
    records: list[str] = []
    for path in iter_files(root, roots):
        rel = path.relative_to(root).as_posix()
        if rel == V3_FILE:
            continue                                                 # the recorder itself
        text = path.read_text(encoding="utf-8", errors="replace")
        if V3_STEM not in text and "e1_per_tx_recorder_" not in text and "environ" not in text and "getenv" not in text:
            continue
        for finding in scan_text(rel, text, path.suffix):
            if finding.kind == "python_literal" and rel in registered:
                continue
            violations.append(finding)
        if path.suffix == ".json" and V3_STEM in text and not any(f.path == rel for f in violations):
            records.append(rel)
    return violations, sorted(records)
