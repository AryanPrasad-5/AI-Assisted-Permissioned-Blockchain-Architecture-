"""Structured, sanitized telemetry read failures (live-shadow integration fix 3, 2026-11-17).

Before this module an adapter read error was either flattened into one string
(``"TimeoutError: ..."``) inside ``health["endpoint_errors"]``, or stored in
``health["read_errors"]`` and never looked at again by the frame provider. A failed
read could therefore reach a controller as an apparently healthy frame. This module
gives every failure

* a stable ``category`` (timeout, connection_refused, malformed_response, ...),
* the original exception type and a bounded, sanitized message,
* the source it came from, so the context survives into abstention records, and
* one of four observation statuses: SUCCESSFUL, PARTIAL, FAILED, INVALID.

Nothing here substitutes a value for a missing one. The module uses only the standard
library and no package-relative imports, so it can also be loaded by plain scripts.
"""

from __future__ import annotations

import http.client
import re
import socket
import urllib.error
from dataclasses import dataclass
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit

# A source could not be read at all (transport, protocol, filesystem).
FAILED_CATEGORIES = frozenset({
    "timeout", "connection_refused", "connection_error", "http_error",
    "file_missing", "file_read_error", "internal_error",
})
# A source answered, but what it returned cannot be used as an observation.
INVALID_CATEGORIES = frozenset({
    "malformed_response", "empty_response", "schema_error", "invalid_value",
    "missing_timestamp", "timestamp_conflict",
})
# A source answered validly but lacks content the consumer declared required.
MISSING_CATEGORIES = frozenset({"missing_required_metric"})
CATEGORIES = FAILED_CATEGORIES | INVALID_CATEGORIES | MISSING_CATEGORIES

READ_STATUSES = ("SUCCESSFUL", "PARTIAL", "FAILED", "INVALID")

MAX_MESSAGE_CHARS = 300
MAX_DETAILS_IN_FRAME = 10

_PEM = re.compile(r"-----BEGIN [A-Z0-9 ]+-----.*?(?:-----END [A-Z0-9 ]+-----|$)", re.DOTALL)
_URL_USERINFO = re.compile(r"(?i)\b([a-z][a-z0-9+.\-]*://)[^/\s:@]+(?::[^/\s@]*)?@")
_URL_SECRET_QUERY = re.compile(
    r"(?i)([?&;](?:token|access_token|id_token|api[_\-]?key|key|secret|password|passwd|pwd|auth|"
    r"authorization|signature|sig|credential)=)[^&\s#]*")
_AUTH_SCHEME = re.compile(r"(?i)\b(bearer|basic|token)\s+[A-Za-z0-9._~+/=\-]{8,}")
_ENV_SECRET = re.compile(
    r"(?i)\b([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|PASSWD|API_?KEY|PRIVATE_?KEY|CREDENTIAL)[A-Z0-9_]*)"
    r"\s*[=:]\s*[^\s,;&]+")
_JSON_SECRET = re.compile(
    r"(?i)(\"(?:password|passwd|secret|token|api_?key|private_?key|authorization|credential)\"\s*:\s*)\"[^\"]*\"")
_JWT = re.compile(r"\beyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}")


def sanitize_text(text: Any, *, max_chars: int = MAX_MESSAGE_CHARS) -> str:
    """Remove credentials, key material and tokens from diagnostic text and bound its length.

    The redaction is deliberately conservative: it keeps the exception's meaning (what
    failed and why) and drops anything that looks like a secret. It is not a guarantee
    for arbitrary free text, so callers pass only exception messages and file paths.
    """
    value = str(text)
    value = _PEM.sub("[REDACTED PEM BLOCK]", value)
    value = _URL_USERINFO.sub(r"\1[REDACTED]@", value)
    value = _URL_SECRET_QUERY.sub(r"\1[REDACTED]", value)
    value = _JSON_SECRET.sub(r'\1"[REDACTED]"', value)
    value = _AUTH_SCHEME.sub(lambda m: f"{m.group(1)} [REDACTED]", value)
    value = _ENV_SECRET.sub(lambda m: f"{m.group(1)}=[REDACTED]", value)
    value = _JWT.sub("[REDACTED JWT]", value)
    value = " ".join(value.split())
    if len(value) > max_chars:
        value = f"{value[:max_chars]}...[truncated {len(value) - max_chars} chars]"
    return value


def sanitize_url(url: Any) -> str:
    """Return scheme://host:port/path without user information or query values."""
    text = str(url)
    try:
        parts = urlsplit(text)
        host = parts.hostname or ""
        if not parts.scheme or not host:
            return sanitize_text(text, max_chars=200)
        port = f":{parts.port}" if parts.port else ""
        query = "?[query omitted]" if parts.query else ""
        return f"{parts.scheme}://{host}{port}{parts.path}{query}"
    except ValueError:
        return sanitize_text(text, max_chars=200)


def classify_exception(exc: BaseException, *, kind: str = "network") -> str:
    """Map an exception to a stable category.

    ``kind`` is ``"network"`` for an HTTP/Prometheus read and ``"file"`` for a CSV or JSON
    file read: the same ``OSError`` means a broken connection in one and an unreadable file
    in the other. An exception class may carry its own ``telemetry_category``.
    """
    if kind not in {"network", "file"}:
        raise ValueError("kind must be network or file")
    declared = getattr(exc, "telemetry_category", None)
    if declared in CATEGORIES:
        return declared
    reason = getattr(exc, "reason", None)
    for candidate in (exc, reason if isinstance(reason, BaseException) else None):
        if candidate is None:
            continue
        if isinstance(candidate, (TimeoutError, socket.timeout)):
            return "timeout"
        if isinstance(candidate, ConnectionRefusedError):
            return "connection_refused"
        if isinstance(candidate, (ConnectionError, socket.gaierror, http.client.HTTPException)):
            return "connection_error"
    if isinstance(exc, urllib.error.HTTPError):
        return "http_error"
    if isinstance(exc, urllib.error.URLError):
        text = str(exc.reason).lower()
        if "timed out" in text or "timeout" in text:
            return "timeout"
        if "refused" in text:
            return "connection_refused"
        return "connection_error"
    if isinstance(exc, FileNotFoundError):
        return "file_missing"
    if isinstance(exc, UnicodeError):
        return "malformed_response" if kind == "network" else "invalid_value"
    if isinstance(exc, KeyError):
        return "schema_error"
    if isinstance(exc, OSError):
        return "connection_error" if kind == "network" else "file_read_error"
    if isinstance(exc, ValueError):
        return "malformed_response" if kind == "network" else "invalid_value"
    return "internal_error"


@dataclass(frozen=True)
class TelemetryReadFailure:
    """One failed, invalid or incomplete read, with enough context to investigate it."""

    source: str
    category: str
    error_type: str
    message: str
    endpoint: str | None = None
    http_status: int | None = None

    def __post_init__(self) -> None:
        if not self.source:
            raise ValueError("a read failure names its source")
        if self.category not in CATEGORIES:
            raise ValueError(f"unknown failure category {self.category!r}")

    @property
    def outcome(self) -> str:
        """``failed`` (unreadable), ``invalid`` (unusable content) or ``partial`` (required content absent)."""
        if self.category in FAILED_CATEGORIES:
            return "failed"
        if self.category in INVALID_CATEGORIES:
            return "invalid"
        return "partial"

    @property
    def code(self) -> str:
        return f"{self.source}:{self.category}"

    def as_dict(self) -> dict[str, Any]:
        return {"source": self.source, "category": self.category, "outcome": self.outcome,
                "error_type": self.error_type, "message": self.message,
                "endpoint": self.endpoint, "http_status": self.http_status}


def failure_from_exception(source: str, exc: BaseException, *, kind: str = "network",
                           endpoint: str | None = None) -> TelemetryReadFailure:
    """Build a sanitized failure from an exception, keeping its original type name."""
    status = exc.code if isinstance(exc, urllib.error.HTTPError) else None
    return TelemetryReadFailure(
        source=source, category=classify_exception(exc, kind=kind), error_type=type(exc).__name__,
        message=sanitize_text(exc), endpoint=None if endpoint is None else sanitize_url(endpoint),
        http_status=status)


def failure_from_category(source: str, category: str, message: str, *, error_type: str = "TelemetryDataError",
                          endpoint: str | None = None) -> TelemetryReadFailure:
    """Build a failure that is not an exception (for example a required metric that is absent)."""
    return TelemetryReadFailure(
        source=source, category=category, error_type=error_type, message=sanitize_text(message),
        endpoint=None if endpoint is None else sanitize_url(endpoint))


def aggregate_read_status(source_outcomes: Mapping[str, str], missing_required: Iterable[str] = ()) -> dict[str, Any]:
    """Combine per-source outcomes (``ok``/``failed``/``invalid``) into one observation status.

    * SUCCESSFUL: every source was read and validated, and nothing required is missing.
    * PARTIAL: at least one source is usable, but another failed, was invalid, or required
      content is missing. A partial observation is never a healthy one.
    * FAILED: no source is usable and none returned invalid content.
    * INVALID: no source is usable and at least one returned invalid content.
    An empty set of sources is FAILED: there is no observation at all.
    """
    outcomes = dict(source_outcomes)
    bad = set(outcomes.values()) - {"ok", "failed", "invalid"}
    if bad:
        raise ValueError(f"unknown source outcomes: {sorted(bad)}")
    missing = sorted(set(missing_required))
    ok = sorted(name for name, value in outcomes.items() if value == "ok")
    failed = sorted(name for name, value in outcomes.items() if value == "failed")
    invalid = sorted(name for name, value in outcomes.items() if value == "invalid")
    if not outcomes:
        status = "FAILED"
    elif len(ok) == len(outcomes) and not missing:
        status = "SUCCESSFUL"
    elif not ok:
        status = "INVALID" if invalid else "FAILED"
    else:
        status = "PARTIAL"
    return {"status": status, "sources_ok": ok, "sources_failed": failed, "sources_invalid": invalid,
            "missing_required": missing}


def bounded_details(failures: Iterable[TelemetryReadFailure], limit: int = MAX_DETAILS_IN_FRAME) -> dict[str, Any]:
    """Serialize failures for a frame or record: at most ``limit`` entries, never silently fewer."""
    items = list(failures)
    return {"count": len(items), "truncated": max(0, len(items) - limit),
            "items": [item.as_dict() for item in items[:limit]]}
