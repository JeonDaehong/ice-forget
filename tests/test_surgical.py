"""Surgical history rewrite (RFC 0001 Method A).

The distinguishing claim over the orchestrate path: the subject is gone from
*every* snapshot, yet every snapshot id still resolves. These tests hold both
halves of that claim at once — a rewrite that erased the subject by dropping
history would pass a residual scan but fail here.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyiceberg")

from iceforget.surgical import SurgicalRewriteError, SurgicalRewriter


def _ids(table):
    return [s.snapshot_id for s in table.metadata.snapshots]


def test_surgical_erase_removes_subject_and_keeps_every_snapshot(surgical_coordinator, catalog):
    before = _ids(catalog.load_table("db.users"))
    assert len(before) == 3

    result = surgical_coordinator.erase("db.users", {"user_id": 42})

    assert result.method == "surgical"
    assert result.rows_deleted == 1
    assert result.verify.residual_rows == 0
    assert result.success
    # The whole point: no snapshot was expired to achieve the erasure.
    assert result.expired_snapshot_ids == []
    assert result.time_travel_preserved is True
    assert _ids(catalog.load_table("db.users")) == before


def test_time_travel_still_reads_history_minus_the_subject(surgical_coordinator, catalog):
    """The oldest snapshot must still be readable, and still hold its bystanders."""
    table = catalog.load_table("db.users")
    oldest = sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms)[0].snapshot_id

    surgical_coordinator.erase("db.users", {"user_id": 42})

    reloaded = catalog.load_table("db.users")
    rows = reloaded.scan(snapshot_id=oldest).to_arrow()
    ids = set(rows["user_id"].to_pylist())
    assert 42 not in ids  # subject erased from history
    assert {7, 13} <= ids  # its snapshot-mates survived


def test_surgical_erase_leaves_other_subjects_intact(surgical_coordinator):
    surgical_coordinator.erase("db.users", {"user_id": 42})
    other = surgical_coordinator.erase("db.users", {"user_id": 7}, dry_run=True)
    assert other.verify.residual_rows > 0


def test_surgical_erase_of_absent_subject_is_a_noop(surgical_coordinator, catalog):
    before = _ids(catalog.load_table("db.users"))
    result = surgical_coordinator.erase("db.users", {"user_id": 999})
    assert result.rows_deleted == 0
    assert result.success
    assert result.files_rewritten == 0
    assert _ids(catalog.load_table("db.users")) == before


def test_surgical_erase_handles_multiple_subjects(surgical_coordinator, catalog):
    """The IN predicate must reach the Arrow mask, not just the file filter."""
    before = _ids(catalog.load_table("db.users"))
    result = surgical_coordinator.erase("db.users", {"user_id": [42, 13]})
    assert result.rows_deleted == 2
    assert result.verify.residual_rows == 0
    assert result.time_travel_preserved is True
    assert _ids(catalog.load_table("db.users")) == before


def test_certificate_records_the_surgical_guarantee(surgical_coordinator):
    result = surgical_coordinator.erase("db.users", {"user_id": 42}, subject="dana")
    cert = surgical_coordinator.certify(result)
    assert cert.processing_mode == "surgical"
    assert cert.method == "surgical"
    assert cert.outcome == "erased"
    assert cert.time_travel_preserved is True
    assert cert.snapshots_expired == []
    assert cert.verify_integrity()


def test_partitioned_table_is_refused(catalog, make_batch):
    """Phase 1 is unpartitioned-only; it must refuse rather than corrupt."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import IdentityTransform
    from pyiceberg.types import LongType, NestedField, StringType

    # An explicit Iceberg schema, so the spec's source_id resolves to a real field.
    iceberg_schema = Schema(
        NestedField(field_id=1, name="user_id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="email", field_type=StringType(), required=False),
        NestedField(field_id=4, name="country", field_type=StringType(), required=False),
    )
    table = catalog.create_table(
        "db.parted",
        schema=iceberg_schema,
        partition_spec=PartitionSpec(
            PartitionField(
                source_id=4, field_id=1000, transform=IdentityTransform(), name="country"
            )
        ),
    )
    table.append(make_batch([1, 2], ["A", "B"]))

    with pytest.raises(SurgicalRewriteError, match="unpartitioned"):
        SurgicalRewriter(catalog).rewrite(table, {"user_id": 1})
