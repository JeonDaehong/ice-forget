"""ErasureCoordinator: the orchestration at the heart of IceForget.

It runs the industry-standard erasure pipeline as one governed, audited
operation:

    index (blast radius)
      -> delete rows      (copy-on-write, current snapshot)
      -> compact          (best effort)
      -> expire snapshots (drop history that still references the PII)
      -> verify           (re-scan every reachable snapshot)
      -> certify          (tamper-evident audit record)

The table's policy ``mode`` selects how the erasure is carried out:

``orchestrate``
    The pipeline above. Expiry removes reachable history down to the table's
    retention budget — the trade-off the compliance pipeline is meant to make,
    but it *does* shorten time travel.

``surgical``
    RFC 0001 Method A: rewrite every snapshot minus the subject, preserving
    every snapshot id. Nothing is expired, so time travel survives intact.

A mode that isn't implemented is refused at policy-load time rather than here,
because the certificate attests ``processing_mode``: running a different mode
than the one we sign would make the audit record untrue.

Nothing mutates when ``dry_run=True`` — you get the blast radius and a
projected outcome only.
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

        handle = self._engine.load_table(table)
        index = self._indexer.index(handle, request)
        mode = table_policy.mode

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
                method=mode,
            )

        if mode == "surgical":
            result = self._erase_surgical(handle, request, index, started)
        else:
            result = self._erase_orchestrate(handle, request, index, table_policy, started)
        return self._purge(handle, index, result, table_policy)

    def _purge(self, handle, index, result: ErasureResult, table_policy: TablePolicy):
        """Physically delete the subject's data files, then record what remains.

        Expiring or repointing a snapshot only unlinks a file from metadata —
        the bytes survive in the warehouse. Rather than sweeping the warehouse
        for orphans (which can race a concurrent writer's in-flight files), this
        deletes exactly the blast-radius files, and only those no longer
        referenced by any snapshot.
        """
        result.purge_requested = table_policy.purge_data_files
        blast_radius = {m.file_path for m in index.matches}
        # Re-load: the delete/expire/rewrite above moved the metadata pointer,
        # and a stale handle would report the pre-erasure reference set — which
        # still lists the very files we mean to delete.
        handle = self._engine.load_table(table_policy.table)
        if not table_policy.purge_data_files:
            # Not purging is a policy choice, but the certificate must still say
            # how many files were left behind rather than implying none were.
            still_live = self._engine.referenced_files(handle)
            result.files_left_on_disk = len(blast_radius - still_live)
            return result

        result.files_purged = self._engine.delete_files(handle, blast_radius)
        # Anything in the blast radius we did not delete is still referenced by
        # a live snapshot, so its bytes remain readable.
        result.files_left_on_disk = len(blast_radius) - len(result.files_purged)
        return result

    # ------------------------------------------------------------------

    def _erase_orchestrate(
        self, handle, request: ErasureRequest, index, table_policy: TablePolicy, started: str
    ) -> ErasureResult:
        """delete -> compact -> expire -> verify. Shortens time travel by design."""
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
            method="orchestrate",
        )

    def _erase_surgical(
        self, handle, request: ErasureRequest, index, started: str
    ) -> ErasureResult:
        """Rewrite every snapshot minus the subject, preserving time travel.

        Unlike the orchestrate path this expires nothing: every snapshot id stays
        resolvable, which is the guarantee the certificate records via
        ``time_travel_preserved``.
        """
        from iceforget.surgical import SurgicalRewriter

        catalog = getattr(self._engine, "catalog", None)
        if catalog is None:
            raise NotImplementedError(
                f"mode 'surgical' needs an engine exposing a PyIceberg catalog; "
                f"{type(self._engine).__name__} does not. Use mode 'orchestrate'."
            )

        outcome = SurgicalRewriter(catalog).rewrite(handle, request.key)

        # The commit swapped the catalog pointer under us; re-load so the
        # verification scan reads the rewritten metadata, not the stale handle.
        handle = self._engine.load_table(request.table)
        verify: VerifyReport = self._verifier.verify(handle, request)

        return ErasureResult(
            request=request,
            index=index,
            rows_deleted=outcome.rows_deleted,
            delete_snapshot_id=self._engine.current_snapshot_id(handle),
            compacted=False,
            expired_snapshot_ids=[],
            verify=verify,
            dry_run=False,
            started_at=started,
            finished_at=_utcnow(),
            method="surgical",
            snapshots_rewritten=outcome.snapshots_rewritten,
            files_rewritten=outcome.files_rewritten,
            time_travel_preserved=outcome.time_travel_preserved,
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
