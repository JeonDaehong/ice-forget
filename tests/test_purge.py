"""Physical deletion of the subject's data files.

Unlinking a file from metadata is not erasure — the bytes survive in the
warehouse until something deletes them. These tests look at the filesystem
rather than the catalog, because that is the only place the guarantee is real.
"""

from __future__ import annotations

import pytest

from iceforget.coordinator import ErasureCoordinator
from iceforget.engines.pyiceberg_engine import PyIcebergEngine
from iceforget.policy import CatalogConfig, Policy, TablePolicy


def _parquet_names(warehouse):
    return {p.name for p in warehouse.rglob("*.parquet")}


def _blast_radius_names(coordinator, key):
    """Basenames of the files holding the subject, taken before any mutation."""
    preview = coordinator.erase("db.users", key, dry_run=True)
    return {m.file_path.rsplit("/", 1)[-1] for m in preview.index.matches}


def _policy(**overrides):
    return Policy(
        catalog=CatalogConfig(name="test"),
        tables=[
            TablePolicy(
                table="db.users",
                identifier_columns=["user_id"],
                retain_last_snapshots=1,
                **overrides,
            )
        ],
    )


@pytest.mark.parametrize("mode", ["orchestrate", "surgical"])
def test_subject_files_are_physically_deleted(catalog, users_table, tmp_path, mode):
    warehouse = tmp_path / "warehouse"
    coordinator = ErasureCoordinator(PyIcebergEngine(catalog), _policy(mode=mode))

    doomed = _blast_radius_names(coordinator, {"user_id": 42})
    assert doomed, "test needs a non-empty blast radius to be meaningful"
    assert doomed <= _parquet_names(warehouse)

    result = coordinator.erase("db.users", {"user_id": 42})

    assert result.files_purged, "nothing was purged"
    assert result.files_left_on_disk == 0
    assert result.bytes_erased
    assert result.success
    # The actual guarantee: those files are gone from the warehouse.
    assert doomed & _parquet_names(warehouse) == set()


def test_purge_leaves_other_subjects_files_alone(catalog, users_table, tmp_path):
    """Only the blast radius is deleted, never a sweep of the warehouse."""
    warehouse = tmp_path / "warehouse"
    coordinator = ErasureCoordinator(PyIcebergEngine(catalog), _policy())

    doomed = _blast_radius_names(coordinator, {"user_id": 42})
    before = _parquet_names(warehouse)

    coordinator.erase("db.users", {"user_id": 42})
    after = _parquet_names(warehouse)

    # Files that never held the subject survive (minus none), and the rewrite
    # may have added one. Nothing outside the blast radius was removed.
    assert (before - doomed) <= after


def test_purge_disabled_reports_bytes_on_disk(catalog, users_table, tmp_path):
    """Opting out is allowed, but the certificate must not claim 'erased'."""
    warehouse = tmp_path / "warehouse"
    coordinator = ErasureCoordinator(
        PyIcebergEngine(catalog), _policy(purge_data_files=False)
    )

    doomed = _blast_radius_names(coordinator, {"user_id": 42})
    result = coordinator.erase("db.users", {"user_id": 42})

    assert result.files_purged == []
    assert result.files_left_on_disk > 0
    assert not result.bytes_erased
    # Catalog-clean but bytes present: this must not read as a success.
    assert result.verify.clean
    assert not result.success

    cert = coordinator.certify(result)
    assert cert.outcome == "bytes-on-disk"
    assert cert.files_purged == 0
    assert cert.files_left_on_disk > 0
    assert cert.verify_integrity()

    # And the bytes really are still there.
    assert doomed & _parquet_names(warehouse) == doomed


def test_certificate_records_purged_files(catalog, users_table):
    coordinator = ErasureCoordinator(PyIcebergEngine(catalog), _policy())
    result = coordinator.erase("db.users", {"user_id": 42})
    cert = coordinator.certify(result)
    assert cert.outcome == "erased"
    assert cert.files_purged == len(result.files_purged) > 0
    assert cert.files_left_on_disk == 0
    assert cert.verify_integrity()


def test_delete_files_refuses_to_delete_a_live_file(catalog, users_table, tmp_path):
    """The safety guard: a still-referenced file is never removed."""
    warehouse = tmp_path / "warehouse"
    engine = PyIcebergEngine(catalog)
    table = engine.load_table("db.users")

    live = engine.referenced_files(table)
    assert live, "table should reference some files"

    deleted = engine.delete_files(table, live)
    assert deleted == []
    # Every live file is still on disk.
    assert {p.rsplit("/", 1)[-1] for p in live} <= _parquet_names(warehouse)


def test_dry_run_purges_nothing(catalog, users_table, tmp_path):
    warehouse = tmp_path / "warehouse"
    coordinator = ErasureCoordinator(PyIcebergEngine(catalog), _policy())
    before = _parquet_names(warehouse)

    result = coordinator.erase("db.users", {"user_id": 42}, dry_run=True)

    assert result.files_purged == []
    assert _parquet_names(warehouse) == before
