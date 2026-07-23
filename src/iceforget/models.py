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


def _predicate(col: str, value: Any) -> str:
    """Render one column's predicate: ``col = x``, or ``col IN (x, y)``.

    A single value — including a one-element list — always renders as equality,
    so the predicate for a given subject doesn't depend on how the caller
    happened to wrap it.
    """
    # Sets are deliberately not accepted: they render in an arbitrary order and
    # aren't JSON-serializable for the request id.
    if isinstance(value, (list, tuple)):
        values = list(value)
        if not values:
            raise ValueError(f"key column {col!r} has an empty value list")
        if len(values) > 1:
            return f"{col} IN ({', '.join(_literal(v) for v in values)})"
        value = values[0]
    return f"{col} = {_literal(value)}"


def render_row_filter(key: dict[str, Any]) -> str:
    """Render a subject key as an Iceberg row-filter expression.

    The single source of truth for turning a key into a predicate: both
    :meth:`ErasureRequest.row_filter` and the surgical rewriter go through here,
    so the orchestrate and surgical paths can never disagree about which rows a
    request covers. Columns are sorted for a stable, reproducible predicate.
    """
    return " AND ".join(_predicate(col, val) for col, val in sorted(key.items()))


# ---------------------------------------------------------------------------
# Request
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ErasureRequest:
    """A single subject-erasure request against one table.

    ``key`` maps PII identifier columns to the value(s) to erase, e.g.
    ``{"user_id": 42}`` or ``{"email": "a@b.com"}``. Multiple entries are
    AND-ed together into a row predicate.

    A column may also carry a list of values — ``{"user_id": [42, 43]}`` — which
    renders as an ``IN`` predicate, so one request can erase a batch of subjects
    on the same column.
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
        return render_row_filter(self.key)


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
# Erasure levels
# ---------------------------------------------------------------------------


class ErasureLevel:
    """How complete an erasure is, as verifiable layers.

    "Deleted" is not one thing, and most erasure tooling silently stops at the
    first layer. Naming the layers lets a certificate state exactly how far a
    run got, instead of asserting a bare "erased" that means different things to
    the operator and the auditor.

    Each level must be *checked*, never assumed — see :func:`erasure_level`.
    """

    NONE = 0
    CURRENT_SNAPSHOT = 1
    ALL_SNAPSHOTS = 2
    BYTES_REMOVED = 3


LEVEL_ATTESTS: dict[int, str] = {
    ErasureLevel.NONE: "nothing was erased",
    ErasureLevel.CURRENT_SNAPSHOT: (
        "the subject is gone from the current snapshot, but is still readable "
        "through at least one historical snapshot"
    ),
    ErasureLevel.ALL_SNAPSHOTS: (
        "no snapshot reachable from the catalog serves the subject, but at least "
        "one file that held them is still present in the warehouse"
    ),
    ErasureLevel.BYTES_REMOVED: (
        "no snapshot reachable from the catalog serves the subject, and no file "
        "that held them remains in the warehouse"
    ),
}

# Things no level of this tool covers. Stated on every certificate: an auditor's
# first question is what the document does *not* cover, and a certificate that
# cannot answer that is worth less than one that can.
OUT_OF_SCOPE: list[str] = [
    "backups and snapshots taken outside this table",
    "exports and downstream systems fed from this table",
    "physical media recovery of already-deleted files",
]


def erasure_level(result: ErasureResult) -> int:
    """Determine, by inspection, how far an erasure actually got."""
    if result.dry_run:
        return ErasureLevel.NONE
    if not result.verify.clean:
        # Some snapshot still serves the subject. The current one counts as
        # cleared only if it is not among those still holding residual rows.
        current_cleared = result.delete_snapshot_id not in result.verify.residual_snapshots
        return ErasureLevel.CURRENT_SNAPSHOT if current_cleared else ErasureLevel.NONE
    if not result.bytes_erased:
        return ErasureLevel.ALL_SNAPSHOTS
    return ErasureLevel.BYTES_REMOVED


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
    # Set by the surgical path; defaults describe the orchestrate path.
    method: str = "orchestrate"
    snapshots_rewritten: list[int] = field(default_factory=list)
    files_rewritten: int = 0
    time_travel_preserved: bool | None = None
    # Physical deletion of the subject's data files.
    files_purged: list[str] = field(default_factory=list)
    files_left_on_disk: int = 0
    purge_requested: bool = True

    @property
    def bytes_erased(self) -> bool:
        """True when no file that held the subject is still on disk.

        Distinct from :attr:`VerifyReport.clean`, which only proves the subject
        is unreachable through the catalog. A file can be unreferenced and still
        sitting in the warehouse — for an erasure obligation those are not the
        same thing.
        """
        return self.files_left_on_disk == 0

    @property
    def success(self) -> bool:
        if not self.verify.clean:
            return False
        # A surgical rewrite that lost a snapshot id failed its core guarantee.
        if self.method == "surgical" and self.time_travel_preserved is False:
            return False
        # Bytes still in the warehouse is not an erasure — whether they remain
        # because purging failed or because the policy opted out. Opting out is
        # a legitimate choice (an external GC may own deletion), but it does not
        # make this run a completed erasure.
        return self.bytes_erased


@dataclass
class ErasureCertificate:
    """Tamper-evident audit record. Hash covers the full serialized body."""

    request_id: str
    table: str
    subject: str
    key_columns: list[str]
    processing_mode: str
    tool_version: str
    outcome: str  # "erased" | "residual-detected" | "bytes-on-disk" | "dry-run"
    rows_deleted: int
    files_in_blast_radius: int
    snapshots_expired: list[int]
    residual_rows: int
    received_at: str
    completed_at: str
    method: str = "orchestrate"
    snapshots_rewritten: list[int] = field(default_factory=list)
    time_travel_preserved: bool | None = None
    # Physical deletion: what the certificate can actually attest about bytes.
    files_purged: int = 0
    files_left_on_disk: int = 0
    # How far the erasure got, in plain words, plus what no level covers.
    erasure_level: int = ErasureLevel.NONE
    attests: str = ""
    out_of_scope: list[str] = field(default_factory=list)
    body_sha256: str = ""

    @classmethod
    def from_result(
        cls, result: ErasureResult, *, tool_version: str, mode: str
    ) -> ErasureCertificate:
        level = erasure_level(result)

        if result.dry_run:
            outcome = "dry-run"
        elif not result.verify.clean:
            outcome = "residual-detected"
        elif result.method == "surgical" and result.time_travel_preserved is False:
            outcome = "integrity-failed"
        elif not result.bytes_erased:
            # Unreachable through the catalog, but the bytes are still there.
            # Saying "erased" here is exactly the lie this field exists to stop,
            # so a policy that opted out of purging gets no exemption.
            outcome = "bytes-on-disk"
        else:
            outcome = "erased"

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
            files_purged=len(result.files_purged),
            files_left_on_disk=result.files_left_on_disk,
            erasure_level=level,
            attests=LEVEL_ATTESTS[level],
            out_of_scope=list(OUT_OF_SCOPE),
            method=result.method,
            snapshots_rewritten=result.snapshots_rewritten,
            time_travel_preserved=result.time_travel_preserved,
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
