"""What the certificate is allowed to claim.

The certificate is the product: if it overstates, everything else is worthless.
Each level here is asserted against an independently observable fact, not
against the flag the code set for itself.
"""

from __future__ import annotations

import json

import pytest

from iceforget.coordinator import ErasureCoordinator
from iceforget.engines.pyiceberg_engine import PyIcebergEngine
from iceforget.models import (
    LEVEL_ATTESTS,
    OUT_OF_SCOPE,
    ErasureCertificate,
    ErasureLevel,
    ErasureRequest,
    ErasureResult,
    IndexReport,
    VerifyReport,
    erasure_level,
)
from iceforget.policy import CatalogConfig, Policy, TablePolicy


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


def _coord(catalog, **overrides):
    return ErasureCoordinator(PyIcebergEngine(catalog), _policy(**overrides))


# -- levels, against observable state ---------------------------------------


def test_dry_run_claims_nothing(catalog, users_table):
    result = _coord(catalog).erase("db.users", {"user_id": 42}, dry_run=True)
    cert = _coord(catalog).certify(result)
    assert cert.erasure_level == ErasureLevel.NONE
    assert cert.outcome == "dry-run"
    assert cert.attests == LEVEL_ATTESTS[ErasureLevel.NONE]


def test_full_erasure_claims_bytes_removed(catalog, users_table, tmp_path):
    warehouse = tmp_path / "warehouse"
    coord = _coord(catalog)
    doomed = {
        m.file_path.rsplit("/", 1)[-1]
        for m in coord.erase("db.users", {"user_id": 42}, dry_run=True).index.matches
    }

    result = coord.erase("db.users", {"user_id": 42})
    cert = coord.certify(result)

    assert cert.erasure_level == ErasureLevel.BYTES_REMOVED
    # Independently true: those files really are gone from the warehouse.
    assert doomed & {p.name for p in warehouse.rglob("*.parquet")} == set()
    assert "no file that held them remains" in cert.attests


def test_unpurged_erasure_claims_only_all_snapshots(catalog, users_table, tmp_path):
    warehouse = tmp_path / "warehouse"
    coord = _coord(catalog, purge_data_files=False)
    doomed = {
        m.file_path.rsplit("/", 1)[-1]
        for m in coord.erase("db.users", {"user_id": 42}, dry_run=True).index.matches
    }

    result = coord.erase("db.users", {"user_id": 42})
    cert = coord.certify(result)

    assert cert.erasure_level == ErasureLevel.ALL_SNAPSHOTS
    # Independently true: the bytes are still there, which is what level 2 says.
    assert doomed <= {p.name for p in warehouse.rglob("*.parquet")}
    assert "still present in the warehouse" in cert.attests
    assert cert.outcome == "bytes-on-disk"


def test_residual_history_claims_only_current_snapshot():
    """A run that cleared the live snapshot but left history must say so."""
    request = ErasureRequest(table="db.users", key={"user_id": 42})
    result = ErasureResult(
        request=request,
        index=IndexReport(
            request_id=request.request_id, table="db.users",
            row_filter="user_id = 42", current_snapshot_id=999,
        ),
        rows_deleted=1,
        delete_snapshot_id=999,
        compacted=False,
        expired_snapshot_ids=[],
        verify=VerifyReport(
            request_id=request.request_id, table="db.users",
            row_filter="user_id = 42", residual_rows=2,
            residual_snapshots=[111, 222],  # history, not the live snapshot
            scanned_snapshots=3,
        ),
        dry_run=False,
        started_at="", finished_at="",
    )
    assert erasure_level(result) == ErasureLevel.CURRENT_SNAPSHOT
    assert not result.success


def test_level_is_none_when_even_the_live_snapshot_still_serves():
    request = ErasureRequest(table="db.users", key={"user_id": 42})
    result = ErasureResult(
        request=request,
        index=IndexReport(
            request_id=request.request_id, table="db.users",
            row_filter="user_id = 42", current_snapshot_id=999,
        ),
        rows_deleted=0,
        delete_snapshot_id=999,
        compacted=False,
        expired_snapshot_ids=[],
        verify=VerifyReport(
            request_id=request.request_id, table="db.users",
            row_filter="user_id = 42", residual_rows=1,
            residual_snapshots=[999],  # the live snapshot itself
            scanned_snapshots=1,
        ),
        dry_run=False,
        started_at="", finished_at="",
    )
    assert erasure_level(result) == ErasureLevel.NONE


# -- scope disclosure --------------------------------------------------------


def test_certificate_always_discloses_what_it_does_not_cover(catalog, users_table):
    coord = _coord(catalog)
    cert = coord.certify(coord.erase("db.users", {"user_id": 42}))
    assert cert.out_of_scope == OUT_OF_SCOPE
    assert any("backup" in s for s in cert.out_of_scope)


@pytest.mark.parametrize("level", sorted(LEVEL_ATTESTS))
def test_every_level_has_a_plain_language_claim(level):
    assert LEVEL_ATTESTS[level].strip()


# -- tamper evidence covers the new claims -----------------------------------


@pytest.mark.parametrize(
    "field,value",
    [
        ("erasure_level", ErasureLevel.BYTES_REMOVED),
        ("attests", "everything is definitely gone"),
        ("out_of_scope", []),
        ("files_left_on_disk", 0),
    ],
)
def test_editing_a_claim_breaks_the_hash(catalog, users_table, field, value):
    """Upgrading the claim after the fact must be detectable."""
    coord = _coord(catalog, purge_data_files=False)
    cert = coord.certify(coord.erase("db.users", {"user_id": 42}))
    assert cert.verify_integrity()

    tampered = ErasureCertificate(**{**cert.to_dict(), field: value})
    assert not tampered.verify_integrity()


def test_certificate_json_round_trips(catalog, users_table):
    coord = _coord(catalog)
    cert = coord.certify(coord.erase("db.users", {"user_id": 42}))
    restored = ErasureCertificate(**json.loads(cert.to_json()))
    assert restored.verify_integrity()
    assert restored.erasure_level == cert.erasure_level
