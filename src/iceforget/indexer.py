"""Indexer: compute the blast radius of an erasure *before* touching anything.

For a given subject key, this answers "which physical data files, across the
entire snapshot history, do these rows live in?" using nothing but Iceberg
metadata and scan planning — no full-table read. That's the report an operator
reviews before authorizing an irreversible erasure, and the baseline the
verifier is checked against afterwards.
"""

from __future__ import annotations

from iceforget.engines.base import Engine
from iceforget.models import ErasureRequest, FileMatch, IndexReport


class Indexer:
    def __init__(self, engine: Engine):
        self._engine = engine

    def index(self, table_handle, request: ErasureRequest) -> IndexReport:
        row_filter = request.row_filter()
        current = self._engine.current_snapshot_id(table_handle)
        snapshot_ids = self._engine.snapshot_ids(table_handle)

        matches: list[FileMatch] = []
        for snap_id in snapshot_ids:
            for pf in self._engine.plan_files(table_handle, row_filter, snapshot_id=snap_id):
                matches.append(
                    FileMatch(
                        snapshot_id=snap_id,
                        file_path=pf.file_path,
                        record_count=pf.record_count,
                        file_size_bytes=pf.file_size_bytes,
                        is_current=(snap_id == current),
                    )
                )

        return IndexReport(
            request_id=request.request_id,
            table=request.table,
            row_filter=row_filter,
            current_snapshot_id=current,
            matches=matches,
            scanned_snapshots=len(snapshot_ids),
        )
