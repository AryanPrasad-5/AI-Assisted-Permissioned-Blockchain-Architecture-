"""Shared types for the replay package: integrity errors and named checks."""

from __future__ import annotations

from dataclasses import dataclass, field

DAY_S = 86_400
DAY_MS = DAY_S * 1000
FLOAT_TOL = 1e-9


class IntegrityError(RuntimeError):
    """A source or derived-data integrity check failed. Downstream work must stop."""


@dataclass
class Check:
    name: str
    expected: object
    observed: object
    ok: bool


@dataclass
class CheckReport:
    label: str
    checks: list[Check] = field(default_factory=list)

    def add(self, name: str, expected: object, observed: object, tol: float = 0.0) -> bool:
        """Record one comparison. Numbers compare within tol (default exact); everything else compares with ==."""
        if isinstance(expected, bool) or isinstance(observed, bool) or not (
            isinstance(expected, (int, float)) and isinstance(observed, (int, float))
        ):
            ok = bool(expected == observed)
        else:
            ok = abs(float(expected) - float(observed)) <= tol
        self.checks.append(Check(name, expected, observed, ok))
        return ok

    def add_flag(self, name: str, ok: bool, observed: object = None) -> bool:
        """Record a pass/fail check whose expectation is simply True."""
        self.checks.append(Check(name, True, observed if observed is not None else bool(ok), bool(ok)))
        return bool(ok)

    @property
    def ok(self) -> bool:
        return all(c.ok for c in self.checks)

    @property
    def failures(self) -> list[Check]:
        return [c for c in self.checks if not c.ok]

    def as_dicts(self) -> list[dict]:
        return [
            {"name": c.name, "expected": _plain(c.expected), "observed": _plain(c.observed), "ok": c.ok}
            for c in self.checks
        ]

    def render(self) -> str:
        lines = [f"{self.label}: {'PASS' if self.ok else 'FAIL'} ({len(self.checks)} checks)"]
        for c in self.checks:
            lines.append(f"  [{'ok' if c.ok else 'FAIL'}] {c.name}: expected={_plain(c.expected)} observed={_plain(c.observed)}")
        return "\n".join(lines)


def _plain(value: object) -> object:
    """JSON-safe scalar."""
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, AttributeError):
            return str(value)
    return value
