"""Core data structures shared across the IceForget pipeline.

These are deliberately plain and serializable: every report and result can be
dumped to JSON so it can live in an audit trail forever, independent of the
Python objects that produced it.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _literal(value: Any) -> str:
    """Render a Python value as an Iceberg row-filter literal."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    return str(value)


# ---------------------------------------------------------------------------
# Request
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ErasureRequest:
    """A single subject-erasure request against one table.

    ``key`` maps PII identifier columns to the value(s) to erase, e.g.
    ``{"user_id": 42}`` or ``{"email": "a@b.com"}``. Multiple entries are
    AND-ed together into a row predicate.
    """

    table: str
    key: dict[str, Any]
    request_id: str = ""
    subject: str = ""  # optional free-text subject label for the audit trail
    received_at: str = ""

    def __post_init__(self) -> None:
        if not self.key:
            raise ValueError("ErasureRequest.key must not be empty")
        if not self.request_id:
            object.__setattr__(self, "request_id", self._derive_request_id())
        if not self.received_at:
            object.__setattr__(self, "received_at", _utcnow())

    def _derive_request_id(self) -> str:
        digest = hashlib.sha256(
            (self.table + json.dumps(self.key, sort_keys=True)).encode()
        ).hexdigest()[:12]
        return f"erasure-{digest}"

    def row_filter(self) -> str:
        """Render the key as a PyIceberg row-filter expression string."""
        return " AND ".join(f"{col} = {_literal(val)}" for col, val in sorted(self.key.items()))


# ---------------------------------------------------------------------------
# Indexing (blast-radius analysis, before any mutation)
# ---------------------------------------------------------------------------


@dataclass
class FileMatch:
    """A data file that the subject's rows are read from, in some snapshot."""

    snapshot_id: int
    file_path: str
    record_count: int
    file_size_bytes: int
    is_current: bool


@dataclass
class IndexReport:
    """The blast radius: every file across snapshot history that serves the key."""

    request_id: str
    table: str
    row_filter: str
    current_snapshot_id: int | None
    matches: list[FileMatch] = field(default_factory=list)
    scanned_snapshots: int = 0
    generated_at: str = field(default_factory=_utcnow)

    @property
    def matched_files(self) -> int:
        """Distinct physical data files that serve the subject (deduped by path).

        A single file is often referenced by several snapshots; those are one
        file here, but several :attr:`file_references`."""
        return len({m.file_path for m in self.matches})

    @property
    def file_references(self) -> int:
        """(snapshot, file) references — how many snapshot slots must be cleared."""
        return len({(m.snapshot_id, m.file_path) for m in self.matches})

    @property
    def snapshots_with_matches(self) -> int:
        return len({m.snapshot_id for m in self.matches})

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Verification (after erasure)
# ---------------------------------------------------------------------------


@dataclass
class VerifyReport:
    """Result of re-scanning every reachable snapshot for residual rows.

    ``residual_rows`` is summed across snapshots: a subject reachable from N
    snapshots contributes its matching rows N times. Zero means the subject is
    unreachable from any snapshot the catalog can see, which is what ``clean``
    reports and what the certificate attests.
    """

    request_id: str
    table: str
    row_filter: str
    residual_rows: int
    residual_snapshots: list[int] = field(default_factory=list)
    scanned_snapshots: int = 0
    generated_at: str = field(default_factory=_utcnow)

    @property
    def clean(self) -> bool:
        return self.residual_rows == 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Erasure result + certificate
# ---------------------------------------------------------------------------


@dataclass
class ErasureResult:
    """Everything that happened during one erasure run."""

    request: ErasureRequest
    index: IndexReport
    rows_deleted: int
    delete_snapshot_id: int | None
    compacted: bool
    expired_snapshot_ids: list[int]
    verify: VerifyReport
    dry_run: bool
    started_at: str
    finished_at: str

    @property
    def success(self) -> bool:
        return self.verify.clean


@dataclass
class ErasureCertificate:
    """Tamper-evident audit record. Hash covers the full serialized body."""

    request_id: str
    table: str
    subject: str
    key_columns: list[str]
    processing_mode: str
    tool_version: str
    outcome: str  # "erased" | "residual-detected" | "dry-run"
    rows_deleted: int
    files_in_blast_radius: int
    snapshots_expired: list[int]
    residual_rows: int
    received_at: str
    completed_at: str
    body_sha256: str = ""

    @classmethod
    def from_result(
        cls, result: ErasureResult, *, tool_version: str, mode: str
    ) -> ErasureCertificate:
        if result.dry_run:
            outcome = "dry-run"
        elif result.verify.clean:
            outcome = "erased"
        else:
            outcome = "residual-detected"

        cert = cls(
            request_id=result.request.request_id,
            table=result.request.table,
            subject=result.request.subject,
            key_columns=sorted(result.request.key.keys()),
            processing_mode=mode,
            tool_version=tool_version,
            outcome=outcome,
            rows_deleted=result.rows_deleted,
            files_in_blast_radius=result.index.matched_files,
            snapshots_expired=result.expired_snapshot_ids,
            residual_rows=result.verify.residual_rows,
            received_at=result.request.received_at,
            completed_at=result.finished_at,
        )
        cert.body_sha256 = cert._compute_hash()
        return cert

    def _compute_hash(self) -> str:
        body = {k: v for k, v in asdict(self).items() if k != "body_sha256"}
        return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()

    def verify_integrity(self) -> bool:
        return self.body_sha256 == self._compute_hash()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True)
