"""End-to-end pipeline behavior against a real local Iceberg table."""

from __future__ import annotations

import pytest

from iceforget.models import ErasureCertificate, ErasureRequest


def test_index_finds_subject_across_history(coordinator):
    result = coordinator.erase("db.users", {"user_id": 42}, dry_run=True)
    idx = result.index
    # Subject 42 lives in one physical file, referenced by all 3 snapshots.
    assert idx.matched_files == 1
    assert idx.snapshots_with_matches == 3
    assert idx.scanned_snapshots == 3
    assert idx.row_filter == "user_id = 42"


def test_dry_run_does_not_mutate(coordinator):
    coordinator.erase("db.users", {"user_id": 42}, dry_run=True)
    # A second dry-run still sees the subject present -> nothing was deleted.
    # residual_rows sums per-snapshot occurrences: one file, reachable from 3
    # snapshots, counts as 3.
    again = coordinator.erase("db.users", {"user_id": 42}, dry_run=True)
    assert again.verify.residual_rows == 3
    assert again.rows_deleted == 0


def test_erase_removes_subject_and_verifies_clean(coordinator):
    result = coordinator.erase("db.users", {"user_id": 42})
    assert result.rows_deleted == 1
    assert result.success
    assert result.verify.residual_rows == 0
    # Old snapshots that referenced the PII were expired down to retention.
    assert len(result.expired_snapshot_ids) >= 1


def test_erase_leaves_other_subjects_intact(coordinator):
    coordinator.erase("db.users", {"user_id": 42})
    # A different subject is still fully present after the erasure.
    other = coordinator.erase("db.users", {"user_id": 7}, dry_run=True)
    assert other.verify.residual_rows == 1


def test_erase_multiple_subjects_via_in_predicate(coordinator):
    """PyIceberg must accept the IN predicate, and both subjects must go."""
    result = coordinator.erase("db.users", {"user_id": [42, 7]})
    assert result.rows_deleted == 2
    assert result.success
    assert result.verify.residual_rows == 0
    # Each subject is independently gone, and a bystander is untouched.
    assert coordinator.erase("db.users", {"user_id": 42}, dry_run=True).verify.residual_rows == 0
    assert coordinator.erase("db.users", {"user_id": 7}, dry_run=True).verify.residual_rows == 0
    assert coordinator.erase("db.users", {"user_id": 13}, dry_run=True).verify.residual_rows == 1


def test_index_finds_multiple_subjects_via_in_predicate(coordinator):
    result = coordinator.erase("db.users", {"user_id": [42, 21]}, dry_run=True)
    assert result.index.row_filter == "user_id IN (42, 21)"
    # 42 and 21 sit in different snapshots' files, so both files are in range.
    assert result.index.matched_files == 2


def test_erase_of_absent_subject_is_a_noop(coordinator):
    result = coordinator.erase("db.users", {"user_id": 999})
    assert result.rows_deleted == 0
    assert result.success  # nothing to erase => already clean


def test_certificate_is_tamper_evident(coordinator):
    result = coordinator.erase("db.users", {"user_id": 42}, subject="alice")
    cert = coordinator.certify(result)
    assert cert.outcome == "erased"
    assert cert.rows_deleted == 1
    assert cert.subject == "alice"
    assert cert.verify_integrity()

    # Any post-hoc edit breaks the hash.
    tampered = ErasureCertificate(**{**cert.to_dict(), "rows_deleted": 0})
    assert not tampered.verify_integrity()


def test_certificate_written_to_disk(coordinator, tmp_path):
    result = coordinator.erase("db.users", {"user_id": 42})
    cert = coordinator.certify(result)
    path = coordinator.write_certificate(cert, str(tmp_path / "certs"))
    assert path.endswith(".json")


def test_erase_on_non_identifier_column_is_refused(coordinator):
    with pytest.raises(ValueError, match="identifier_columns"):
        coordinator.erase("db.users", {"email": "dana@x.io"})


def test_unknown_table_raises(coordinator):
    with pytest.raises(KeyError):
        coordinator.erase("db.nonexistent", {"user_id": 1})


def test_request_id_is_deterministic():
    a = ErasureRequest(table="db.users", key={"user_id": 42})
    b = ErasureRequest(table="db.users", key={"user_id": 42})
    assert a.request_id == b.request_id
    c = ErasureRequest(table="db.users", key={"user_id": 43})
    assert a.request_id != c.request_id


def test_empty_key_rejected():
    with pytest.raises(ValueError):
        ErasureRequest(table="db.users", key={})
