"""Shared fixtures: a populated Iceberg table on a local SQLite catalog.

Every test gets its own temp warehouse so runs are isolated and need no
external services.
"""

from __future__ import annotations

import pytest

pa = pytest.importorskip("pyarrow")
SqlCatalog = pytest.importorskip("pyiceberg.catalog.sql").SqlCatalog

from iceforget.coordinator import ErasureCoordinator
from iceforget.engines.pyiceberg_engine import PyIcebergEngine
from iceforget.policy import CatalogConfig, Policy, TablePolicy

SCHEMA = pa.schema(
    [
        ("user_id", pa.int64()),
        ("name", pa.string()),
        ("email", pa.string()),
        ("country", pa.string()),
    ]
)


def _batch(user_ids, names):
    n = len(user_ids)
    return pa.table(
        {
            "user_id": user_ids,
            "name": names,
            "email": [f"{x}@x.io" for x in names],
            "country": ["DE"] * n,
        },
        schema=SCHEMA,
    )


@pytest.fixture
def catalog(tmp_path):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    cat = SqlCatalog(
        "test",
        uri=f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
        warehouse=f"file://{warehouse.as_posix()}",
    )
    cat.create_namespace("db")
    return cat


@pytest.fixture
def users_table(catalog):
    """db.users with subject 42 in the oldest of three snapshots."""
    table = catalog.create_table("db.users", schema=SCHEMA)
    table.append(_batch([42, 7, 13], ["Dana", "Amir", "Bo"]))  # snapshot 1: 42 lives here
    table.append(_batch([21, 34], ["Cleo", "Eli"]))  # snapshot 2
    table.append(_batch([55, 89], ["Fin", "Gia"]))  # snapshot 3
    return table


@pytest.fixture
def policy():
    return Policy(
        catalog=CatalogConfig(name="test"),
        tables=[
            TablePolicy(
                table="db.users",
                identifier_columns=["user_id"],
                retain_last_snapshots=1,
            )
        ],
    )


@pytest.fixture
def coordinator(catalog, users_table, policy):
    return ErasureCoordinator(PyIcebergEngine(catalog), policy)


@pytest.fixture
def surgical_policy():
    """Same table, but erased via the time-travel-preserving rewrite."""
    return Policy(
        catalog=CatalogConfig(name="test"),
        tables=[
            TablePolicy(
                table="db.users",
                identifier_columns=["user_id"],
                mode="surgical",
            )
        ],
    )


@pytest.fixture
def surgical_coordinator(catalog, users_table, surgical_policy):
    return ErasureCoordinator(PyIcebergEngine(catalog), surgical_policy)
