"""Surgical history rewrite (RFC 0001, Method A — metadata surgery).

Erase a subject's rows from **every** snapshot of an Iceberg table while keeping
every snapshot id and timestamp resolvable, so time travel still works — you can
read any historical snapshot, minus the subject.

The mechanic, per erasure request:

    1. blast radius   - which data files does any snapshot read the subject from?
    2. rewrite files  - write each affected file minus the subject's rows
    3. rewrite tree   - rebuild the manifests / manifest lists that pointed at
                        them, preserving snapshot-id / sequence-number / timestamp
    4. commit         - atomic compare-and-swap of the catalog metadata pointer

This is the Phase 1 implementation and is intentionally scoped (see the guards
in :meth:`SurgicalRewriter.rewrite`): copy-on-write, unpartitioned tables, on a
catalog whose pointer swap we support. Partitioning, merge-on-read deletes, v3
row-lineage, and REST/Glue commits are tracked as follow-ups in RFC 0001.

.. warning::
   This reaches into a few PyIceberg internals (manifest writers, the SQL
   catalog table) that are not part of its public API and may shift between
   releases. They are all funnelled through this one module on purpose.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from itertools import count
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog

# --- PyIceberg internals, centralized here (see module warning) --------------
from pyiceberg.io.pyarrow import _dataframe_to_data_files
from pyiceberg.manifest import (
    ManifestContent,
    ManifestEntry,
    write_manifest,
    write_manifest_list,
)
from pyiceberg.table.metadata import MetadataLogEntry
from pyiceberg.table.update.snapshot import (
    _new_manifest_file_name,
    _new_manifest_list_file_name,
)

from iceforget.models import render_row_filter


@dataclass
class SurgicalResult:
    rows_deleted: int
    files_rewritten: int
    snapshots_rewritten: list[int]
    original_snapshot_ids: list[int]
    resulting_snapshot_ids: list[int]
    new_metadata_location: str

    @property
    def time_travel_preserved(self) -> bool:
        """True iff every original snapshot id survived unchanged."""
        return self.original_snapshot_ids == self.resulting_snapshot_ids


class SurgicalRewriteError(RuntimeError):
    """Raised when a table is outside the Phase 1 supported scope, or a commit
    conflicts with a concurrent writer."""


class SurgicalRewriter:
    """Performs Method A of RFC 0001 against a PyIceberg table + catalog."""

    def __init__(self, catalog: Catalog):
        self._catalog = catalog
        self._compression = "gzip"

    def rewrite(self, table: Any, key: dict) -> SurgicalResult:
        io = table.io
        metadata = table.metadata
        self._guard_supported(table)

        original_ids = [s.snapshot_id for s in metadata.snapshots]
        blast_radius = self._blast_radius(table, key)

        if not blast_radius:
            # Nothing references the subject; the table is already clean.
            return SurgicalResult(
                rows_deleted=0,
                files_rewritten=0,
                snapshots_rewritten=[],
                original_snapshot_ids=original_ids,
                resulting_snapshot_ids=original_ids,
                new_metadata_location=table.metadata_location,
            )

        # 2. rewrite each affected data file minus the subject's rows.
        rewritten_data, rows_deleted = self._rewrite_data_files(table, blast_radius, key)

        # 3. rebuild the manifest tree, preserving snapshot identity.
        commit_uuid = uuid.uuid4()
        location = table.location_provider()
        manifest_num = count()
        rewritten_manifest: dict[str, Any] = {}

        new_snapshots = []
        rewritten_snapshot_ids: list[int] = []
        for snap in metadata.snapshots:
            new_list = []
            changed = False
            for m in snap.manifests(io):
                rm = self._rewrite_manifest(
                    table, m, blast_radius, rewritten_data, rewritten_manifest,
                    location, manifest_num, commit_uuid,
                )
                if rm is not None:
                    new_list.append(rm)
                    changed = True
                else:
                    new_list.append(m)
            if not changed:
                new_snapshots.append(snap)
                continue
            ml_path = location.new_metadata_location(
                _new_manifest_list_file_name(
                    snapshot_id=snap.snapshot_id, attempt=0, commit_uuid=commit_uuid
                )
            )
            with write_manifest_list(
                format_version=metadata.format_version,
                output_file=io.new_output(ml_path),
                snapshot_id=snap.snapshot_id,
                parent_snapshot_id=snap.parent_snapshot_id,
                sequence_number=snap.sequence_number,
                avro_compression=self._compression,
            ) as writer:
                writer.add_manifests(new_list)
            new_snapshots.append(snap.model_copy(update={"manifest_list": ml_path}))
            rewritten_snapshot_ids.append(snap.snapshot_id)

        # 5. new metadata: snapshots repointed, ids/timestamps/refs preserved.
        old_location = table.metadata_location
        previous_entry = MetadataLogEntry(
            metadata_file=old_location, timestamp_ms=metadata.last_updated_ms
        )
        new_metadata = metadata.model_copy(
            update={
                "snapshots": new_snapshots,
                "last_updated_ms": int(time.time() * 1000),
                "metadata_log": list(metadata.metadata_log) + [previous_entry],
            }
        )
        new_location = self._next_metadata_location(table)
        self._catalog._write_metadata(new_metadata, io, new_location)

        # 6. atomic compare-and-swap of the catalog pointer.
        self._swap_pointer(table, old_location, new_location)

        return SurgicalResult(
            rows_deleted=rows_deleted,
            files_rewritten=len(rewritten_data),
            snapshots_rewritten=rewritten_snapshot_ids,
            original_snapshot_ids=original_ids,
            resulting_snapshot_ids=[s.snapshot_id for s in new_snapshots],
            new_metadata_location=new_location,
        )

    # -- steps -------------------------------------------------------------

    def _blast_radius(self, table: Any, key: dict) -> set[str]:
        row_filter = render_row_filter(key)
        paths: set[str] = set()
        for snap in table.metadata.snapshots:
            scan = table.scan(row_filter=row_filter, snapshot_id=snap.snapshot_id)
            for task in scan.plan_files():
                paths.add(task.file.file_path)
        return paths

    def _rewrite_data_files(
        self, table: Any, blast_radius: set[str], key: dict
    ) -> tuple[dict[str, Any], int]:
        io = table.io
        rewritten: dict[str, Any] = {}
        rows_deleted = 0
        for path in blast_radius:
            with io.new_input(path).open() as f:
                data = pq.read_table(f)
            mask = _subject_mask(data, key)
            kept = data.filter(pc.invert(mask))
            rows_deleted += data.num_rows - kept.num_rows
            if kept.num_rows == 0:
                rewritten[path] = None  # file emptied -> drop it from the tree
                continue
            new_files = list(_dataframe_to_data_files(table.metadata, df=kept, io=io))
            if len(new_files) != 1:
                raise SurgicalRewriteError(
                    f"expected one rewritten file for {path}, got {len(new_files)} "
                    "(partitioned tables are not yet supported — RFC 0001 Phase 1.x)"
                )
            rewritten[path] = new_files[0]
        return rewritten, rows_deleted

    def _rewrite_manifest(
        self,
        table,
        manifest,
        blast_radius,
        rewritten_data,
        cache,
        location,
        manifest_num,
        commit_uuid,
    ):
        if manifest.manifest_path in cache:
            return cache[manifest.manifest_path]
        entries = manifest.fetch_manifest_entry(table.io, discard_deleted=False)
        if not any(e.data_file.file_path in blast_radius for e in entries):
            return None  # untouched -> reuse the original manifest as-is

        metadata = table.metadata
        output = table.io.new_output(
            location.new_metadata_location(
                _new_manifest_file_name(num=next(manifest_num), commit_uuid=commit_uuid)
            )
        )
        with write_manifest(
            format_version=metadata.format_version,
            spec=metadata.specs()[manifest.partition_spec_id],
            schema=metadata.schema(),
            output_file=output,
            snapshot_id=manifest.added_snapshot_id,
            avro_compression=self._compression,
        ) as writer:
            for e in entries:
                fp = e.data_file.file_path
                if fp in blast_radius:
                    new_df = rewritten_data[fp]
                    if new_df is None:
                        continue  # emptied file: drop the entry
                    data_file = new_df
                else:
                    data_file = e.data_file
                writer.add(
                    ManifestEntry.from_args(
                        status=e.status,
                        snapshot_id=e.snapshot_id,
                        sequence_number=e.sequence_number,
                        file_sequence_number=e.file_sequence_number,
                        data_file=data_file,
                    )
                )
        manifest_file = writer.to_manifest_file()
        cache[manifest.manifest_path] = manifest_file
        return manifest_file

    # -- commit ------------------------------------------------------------

    def _next_metadata_location(self, table: Any) -> str:
        version = self._catalog._parse_metadata_version(table.metadata_location) + 1
        return f"{table.metadata.location}/metadata/{version:05d}-{uuid.uuid4()}.metadata.json"

    def _swap_pointer(self, table: Any, old_location: str, new_location: str) -> None:
        """Atomic compare-and-swap of the catalog metadata pointer.

        Phase 1 supports the SQL catalog only; other catalogs raise so we never
        silently do a non-atomic write."""
        from pyiceberg.catalog.sql import IcebergTables, SqlCatalog

        if not isinstance(self._catalog, SqlCatalog):
            raise SurgicalRewriteError(
                f"surgical rewrite currently supports the SQL catalog only; got "
                f"{type(self._catalog).__name__}. REST/Glue commits are tracked in RFC 0001."
            )
        from sqlalchemy import update
        from sqlalchemy.orm import Session

        identifier = table.name()
        namespace = Catalog.namespace_to_string(Catalog.namespace_from(identifier))
        table_name = Catalog.table_name_from(identifier)
        with Session(self._catalog.engine) as session:
            result = session.execute(
                update(IcebergTables)
                .where(
                    IcebergTables.catalog_name == self._catalog.name,
                    IcebergTables.table_namespace == namespace,
                    IcebergTables.table_name == table_name,
                    IcebergTables.metadata_location == old_location,
                )
                .values(metadata_location=new_location, previous_metadata_location=old_location)
            )
            if result.rowcount < 1:
                raise SurgicalRewriteError(
                    "metadata pointer changed under us (concurrent commit); retry the erasure."
                )
            session.commit()

    # -- scope guards ------------------------------------------------------

    def _guard_supported(self, table: Any) -> None:
        metadata = table.metadata
        if not metadata.spec().is_unpartitioned():
            raise SurgicalRewriteError(
                "surgical rewrite Phase 1 supports unpartitioned tables only "
                "(partition-aware rewrite is a follow-up in RFC 0001)."
            )
        for snap in metadata.snapshots:
            for m in snap.manifests(table.io):
                if m.content != ManifestContent.DATA:
                    raise SurgicalRewriteError(
                        "table uses merge-on-read delete files; surgical rewrite of "
                        "deletes is RFC 0001 Phase 2. Use mode: orchestrate for now."
                    )


def _subject_mask(data, key: dict):
    """Arrow mask selecting the subject's rows — the in-memory twin of
    :func:`iceforget.models.render_row_filter`.

    The two must agree on which rows a key covers: the row filter decides which
    *files* get rewritten, this mask decides which *rows* are dropped from them.
    A disagreement would silently leave the subject in place.
    """
    mask = None
    for col, val in key.items():
        field_type = data.schema.field(col).type
        if isinstance(val, (list, tuple)):
            values = list(val)
            if not values:
                raise ValueError(f"key column {col!r} has an empty value list")
            if len(values) > 1:
                match = pc.is_in(data[col], value_set=pa.array(values, type=field_type))
            else:
                match = pc.equal(data[col], pa.scalar(values[0], field_type))
        else:
            match = pc.equal(data[col], pa.scalar(val, field_type))
        mask = match if mask is None else pc.and_(mask, match)
    return mask
