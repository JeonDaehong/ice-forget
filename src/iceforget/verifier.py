"""Verifier: prove the subject is gone from every reachable snapshot.

After erasure, IceForget re-scans each snapshot the catalog can still reach and
counts residual rows matching the subject key. A clean run (zero residual rows)
is the technical evidence that backs the erasure certificate; a dirty run names
exactly which snapshots still serve the data so an operator can act.

Note the scope: this checks what the *catalog* can reach. Files already
unreferenced but not yet physically deleted, or external copies, are out of
scope and called out as such in the docs.
"""

from __future__ import annotations

from iceforget.engines.base import Engine
from iceforget.models import ErasureRequest, VerifyReport


class Verifier:
    def __init__(self, engine: Engine):
        self._engine = engine

    def verify(self, table_handle, request: ErasureRequest) -> VerifyReport:
        row_filter = request.row_filter()
        snapshot_ids = self._engine.snapshot_ids(table_handle)

        residual_total = 0
        residual_snapshots: list[int] = []
        for snap_id in snapshot_ids:
            count = self._engine.count_rows(table_handle, row_filter, snapshot_id=snap_id)
            if count > 0:
                residual_total += count
                residual_snapshots.append(snap_id)

        return VerifyReport(
            request_id=request.request_id,
            table=request.table,
            row_filter=row_filter,
            residual_rows=residual_total,
            residual_snapshots=residual_snapshots,
            scanned_snapshots=len(snapshot_ids),
        )
