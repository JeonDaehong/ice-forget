"""ErasureCoordinator: the orchestration at the heart of IceForget.

It runs the industry-standard erasure pipeline as one governed, audited
operation:

    index (blast radius)
      -> delete rows      (copy-on-write, current snapshot)
      -> compact          (best effort)
      -> expire snapshots (drop history that still references the PII)
      -> verify           (re-scan every reachable snapshot)
      -> certify          (tamper-evident audit record)

MVP scope, per the design's risk note: this is orchestration + proof, not the
time-travel-preserving "surgical history rewrite" (that's the roadmap). Expiry
here does remove reachable history down to the table's retention budget, which
is exactly the trade-off the compliance pipeline is supposed to make. Nothing
mutates when ``dry_run=True`` — you get the blast radius and a projected
outcome only.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from iceforget import __version__
from iceforget.auditor import Auditor
from iceforget.engines.base import Engine
from iceforget.engines.pyiceberg_engine import PyIcebergEngine
from iceforget.indexer import Indexer
from iceforget.models import (
    ErasureCertificate,
    ErasureRequest,
    ErasureResult,
    VerifyReport,
)
from iceforget.policy import Policy, TablePolicy
from iceforget.verifier import Verifier


class ErasureCoordinator:
    def __init__(self, engine: Engine, policy: Policy):
        self._engine = engine
        self._policy = policy
        self._indexer = Indexer(engine)
        self._verifier = Verifier(engine)
        self._auditor = Auditor(tool_version=__version__)

    @classmethod
    def from_policy(cls, policy: Policy) -> ErasureCoordinator:
        """Build a coordinator with the default PyIceberg engine from a policy."""
        engine = PyIcebergEngine.from_config(
            policy.catalog.name, policy.catalog.properties
        )
        return cls(engine, policy)

    # ------------------------------------------------------------------

    def erase(
        self,
        table: str,
        key: dict,
        *,
        subject: str = "",
        request_id: str = "",
        dry_run: bool = False,
    ) -> ErasureResult:
        table_policy = self._policy.for_table(table)
        self._validate_key(table_policy, key)

        request = ErasureRequest(
            table=table, key=key, subject=subject, request_id=request_id
        )
        started = _utcnow()
        t0 = time.monotonic()

        handle = self._engine.load_table(table)
        index = self._indexer.index(handle, request)

        if dry_run:
            # Project the outcome without mutating: verify reflects current state.
            verify = self._verifier.verify(handle, request)
            return ErasureResult(
                request=request,
                index=index,
                rows_deleted=0,
                delete_snapshot_id=self._engine.current_snapshot_id(handle),
                compacted=False,
                expired_snapshot_ids=[],
                verify=verify,
                dry_run=True,
                started_at=started,
                finished_at=_utcnow(),
            )

        row_filter = request.row_filter()

        # 1. delete rows from the live table (copy-on-write rewrite).
        delete = self._engine.delete_rows(handle, row_filter)

        # 2. compact (best effort — a no-op on the copy-on-write MVP engine).
        compacted = self._engine.compact(handle)

        # 3. expire history so snapshots still referencing the PII drop out,
        #    letting their orphaned files be physically deleted.
        expire = self._engine.expire_snapshots(
            handle,
            older_than_ms=_older_than_ms(table_policy),
            retain_last=table_policy.retain_last_snapshots,
        )

        # 4. verify residual rows across everything still reachable.
        verify: VerifyReport = self._verifier.verify(handle, request)

        _ = t0  # duration is available if we later want it on the result
        return ErasureResult(
            request=request,
            index=index,
            rows_deleted=delete.rows_deleted,
            delete_snapshot_id=delete.new_snapshot_id,
            compacted=compacted,
            expired_snapshot_ids=expire.expired_snapshot_ids,
            verify=verify,
            dry_run=False,
            started_at=started,
            finished_at=_utcnow(),
        )

    def certify(self, result: ErasureResult) -> ErasureCertificate:
        mode = self._policy.for_table(result.request.table).mode
        return self._auditor.certify(result, mode=mode)

    def write_certificate(self, certificate: ErasureCertificate, directory: str) -> str:
        return str(self._auditor.write(certificate, directory))

    # ------------------------------------------------------------------

    @staticmethod
    def _validate_key(table_policy: TablePolicy, key: dict) -> None:
        allowed = set(table_policy.identifier_columns)
        unknown = set(key) - allowed
        if unknown:
            raise ValueError(
                f"Key columns {sorted(unknown)} are not identifier_columns for "
                f"{table_policy.table} (allowed: {sorted(allowed)}). Refusing to erase on "
                f"non-identifier columns to avoid over-deletion."
            )


def _older_than_ms(table_policy: TablePolicy) -> int | None:
    if table_policy.expire_older_than_days is None:
        return None
    cutoff = time.time() - table_policy.expire_older_than_days * 86400
    return int(cutoff * 1000)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()
