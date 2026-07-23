"""PyIceberg-backed :class:`~iceforget.engines.base.Engine`.

This is the reference engine and the only one required for the MVP. It talks to
any catalog PyIceberg supports (REST first, then SQL/Glue/Hive) and needs no
external compute cluster — planning and counting run in-process, deletes use
Iceberg copy-on-write, and expiry uses the catalog's maintenance API.

PyIceberg's surface still shifts between releases, so every call that has
moved across versions is funnelled through a small guarded helper here rather
than sprinkled through the codebase.
"""

from __future__ import annotations

from typing import Any

from pyiceberg.catalog import Catalog, load_catalog

from iceforget.engines.base import DeleteResult, ExpireResult, PlannedFile


class PyIcebergEngine:
    name = "pyiceberg"

    def __init__(self, catalog: Catalog):
        self._catalog = catalog

    @classmethod
    def from_config(cls, catalog_name: str, properties: dict[str, Any]) -> PyIcebergEngine:
        """Build an engine from a catalog name + PyIceberg connection properties."""
        return cls(load_catalog(catalog_name, **properties))

    @property
    def catalog(self) -> Catalog:
        """The underlying PyIceberg catalog (needed for surgical commits)."""
        return self._catalog

    # -- lookups -----------------------------------------------------------

    def load_table(self, identifier: str) -> Any:
        return self._catalog.load_table(identifier)

    def current_snapshot_id(self, table: Any) -> int | None:
        snap = table.current_snapshot()
        return snap.snapshot_id if snap is not None else None

    def snapshot_ids(self, table: Any) -> list[int]:
        snapshots = list(table.metadata.snapshots)
        snapshots.sort(key=lambda s: s.timestamp_ms)
        return [s.snapshot_id for s in snapshots]

    # -- planning / counting ----------------------------------------------

    def plan_files(
        self, table: Any, row_filter: str, snapshot_id: int | None = None
    ) -> list[PlannedFile]:
        scan = self._scan(table, row_filter, snapshot_id)
        resolved = snapshot_id if snapshot_id is not None else self.current_snapshot_id(table)
        planned: list[PlannedFile] = []
        seen: set[str] = set()
        for task in scan.plan_files():
            data_file = task.file
            path = data_file.file_path
            if path in seen:  # a file can back multiple tasks; report it once
                continue
            seen.add(path)
            planned.append(
                PlannedFile(
                    snapshot_id=resolved if resolved is not None else -1,
                    file_path=path,
                    record_count=int(data_file.record_count or 0),
                    file_size_bytes=int(data_file.file_size_in_bytes or 0),
                )
            )
        return planned

    def count_rows(self, table: Any, row_filter: str, snapshot_id: int | None = None) -> int:
        scan = self._scan(table, row_filter, snapshot_id)
        # Prefer a metadata/streaming count when the installed PyIceberg exposes it.
        count = getattr(scan, "count", None)
        if callable(count):
            return int(count())
        # Fall back to materializing matching rows (erasure keys match few rows).
        total = 0
        for batch in scan.to_arrow_batch_reader():
            total += batch.num_rows
        return total

    # -- mutations ---------------------------------------------------------

    def delete_rows(self, table: Any, row_filter: str) -> DeleteResult:
        before = self.count_rows(table, row_filter)
        if before == 0:
            return DeleteResult(rows_deleted=0, new_snapshot_id=self.current_snapshot_id(table))
        table.delete(delete_filter=row_filter)
        table.refresh()
        return DeleteResult(rows_deleted=before, new_snapshot_id=self.current_snapshot_id(table))

    def compact(self, table: Any) -> bool:
        # PyIceberg has no stable public compaction API yet; the copy-on-write
        # delete above already rewrote every affected file, so there is nothing
        # left to consolidate for correctness. A Spark engine will override this
        # to run rewrite_data_files for storage efficiency.
        return False

    def expire_snapshots(
        self, table: Any, *, older_than_ms: int | None = None, retain_last: int = 1
    ) -> ExpireResult:
        targets = self._snapshots_to_expire(table, older_than_ms, retain_last)
        if not targets:
            return ExpireResult(expired_snapshot_ids=[])

        maintenance = getattr(table, "maintenance", None)
        if maintenance is None or not hasattr(maintenance, "expire_snapshots"):
            raise NotImplementedError(
                "The installed PyIceberg does not expose Table.maintenance.expire_snapshots(). "
                "Upgrade to pyiceberg>=0.9.0, or run expiry via your query engine."
            )
        maintenance.expire_snapshots().by_ids(targets).commit()
        table.refresh()
        return ExpireResult(expired_snapshot_ids=targets)

    # -- internals ---------------------------------------------------------

    def _scan(self, table: Any, row_filter: str, snapshot_id: int | None):
        if snapshot_id is not None:
            return table.scan(row_filter=row_filter, snapshot_id=snapshot_id)
        return table.scan(row_filter=row_filter)

    def _snapshots_to_expire(
        self, table: Any, older_than_ms: int | None, retain_last: int
    ) -> list[int]:
        snapshots = sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms)
        current = self.current_snapshot_id(table)
        # Never expire the current snapshot or the newest `retain_last`.
        protected: set[int] = {s.snapshot_id for s in snapshots[-max(retain_last, 1):]}
        if current is not None:
            protected.add(current)
        targets = [
            s.snapshot_id
            for s in snapshots
            if s.snapshot_id not in protected
            and (older_than_ms is None or s.timestamp_ms < older_than_ms)
        ]
        return targets
