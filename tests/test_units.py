"""Fast unit tests that need no catalog."""

from __future__ import annotations

import pytest

from iceforget.models import ErasureRequest
from iceforget.policy import Policy, TablePolicy, load_policy


def test_row_filter_renders_and_ands_keys():
    req = ErasureRequest(table="t", key={"user_id": 42, "tenant": "acme"})
    # keys are sorted for a stable, reproducible predicate
    assert req.row_filter() == "tenant = 'acme' AND user_id = 42"


def test_row_filter_escapes_quotes():
    req = ErasureRequest(table="t", key={"name": "O'Brien"})
    assert req.row_filter() == "name = 'O''Brien'"


def test_row_filter_renders_bool_and_float():
    assert ErasureRequest(table="t", key={"active": True}).row_filter() == "active = true"
    assert ErasureRequest(table="t", key={"score": 1.5}).row_filter() == "score = 1.5"


def test_policy_lookup_and_missing():
    p = Policy(tables=[TablePolicy(table="db.users", identifier_columns=["user_id"])])
    assert p.has_table("db.users")
    assert p.for_table("db.users").sla_days == 30
    with pytest.raises(KeyError):
        p.for_table("db.other")


def test_policy_rejects_empty_identifier_columns():
    with pytest.raises(ValueError):
        TablePolicy(table="db.users", identifier_columns=[])


def test_policy_retain_must_be_at_least_one():
    with pytest.raises(ValueError):
        TablePolicy(table="db.users", identifier_columns=["user_id"], retain_last_snapshots=0)


def test_load_policy_from_yaml(tmp_path):
    path = tmp_path / "policy.yaml"
    path.write_text(
        """
catalog:
  name: prod
  properties:
    type: rest
    uri: https://example.com
tables:
  - table: db.users
    identifier_columns: [user_id]
    retain_last_snapshots: 2
    sla_days: 30
""",
        encoding="utf-8",
    )
    policy = load_policy(path)
    assert policy.catalog.name == "prod"
    assert policy.for_table("db.users").retain_last_snapshots == 2
