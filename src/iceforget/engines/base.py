"""The Engine protocol: the single seam between IceForget and a compute layer.

Keeping this surface small is what lets a Spark or iceberg-rust engine drop in
later without touching the coordinator. An engine is responsible for four
things and nothing else:

    1. plan_files  - which data files does ``row_filter`` read, in a snapshot?
    2. delete_rows - remove matching rows from the *current* table state
    3. compact     - consolidate files (best effort; may be a no-op)
    4. expire_snapshots - drop old snapshots so orphaned PII files are GC'd

Everything policy-, audit-, and verification-related lives above this line.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class PlannedFile:
    """A data file a scan would read, plus the snapshot it was planned against."""

    snapshot_id: int
    file_path: str
    record_count: int
    file_size_bytes: int


@dataclass(frozen=True)
class DeleteResult:
    rows_deleted: int
    new_snapshot_id: int | None


@dataclass(frozen=True)
class ExpireResult:
    expired_snapshot_ids: list[int]


@runtime_checkable
class Engine(Protocol):
    """Compute-layer operations IceForget needs. Implementations must be idempotent
    where the underlying Iceberg operation is (planning, scanning) and transactional
    where it mutates (delete, expire)."""

    name: str

    def load_table(self, identifier: str) -> Any:
        """Return an opaque table handle understood by this engine's other methods."""
        ...

    def current_snapshot_id(self, table: Any) -> int | None:
        ...

    def snapshot_ids(self, table: Any) -> list[int]:
        """All snapshot ids currently reachable from table metadata, newest last."""
        ...

    def plan_files(
        self, table: Any, row_filter: str, snapshot_id: int | None = None
    ) -> list[PlannedFile]:
        """Files the ``row_filter`` scan would read against ``snapshot_id``
        (current snapshot when ``None``)."""
        ...

    def count_rows(self, table: Any, row_filter: str, snapshot_id: int | None = None) -> int:
        """Exact count of rows matching ``row_filter`` in the given snapshot."""
        ...

    def delete_rows(self, table: Any, row_filter: str) -> DeleteResult:
        """Delete matching rows from the current table state (copy-on-write)."""
        ...

    def compact(self, table: Any) -> bool:
        """Consolidate small/rewritten files. Returns True if it did work."""
        ...

    def expire_snapshots(
        self, table: Any, *, older_than_ms: int | None = None, retain_last: int = 1
    ) -> ExpireResult:
        """Expire snapshots so files no longer referenced by any retained snapshot
        become eligible for physical deletion."""
        ...

    def referenced_files(self, table: Any) -> set[str]:
        """Every data file path reachable from any snapshot in table metadata.

        The complement of this set is what may be physically deleted. Engines
        must be conservative: a path that *might* still be referenced belongs in
        the result, because the caller uses this to authorize deletion."""
        ...

    def delete_files(self, table: Any, paths: set[str]) -> list[str]:
        """Physically delete the given data files. Returns the paths deleted.

        Expiring a snapshot only unlinks a file from metadata; the bytes survive
        until something removes them. For an erasure tool that gap is the whole
        ballgame, so this is a first-class engine operation."""
        ...
