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


def test_row_filter_renders_in_for_multiple_values():
    req = ErasureRequest(table="t", key={"user_id": [42, 7, 13]})
    assert req.row_filter() == "user_id IN (42, 7, 13)"


def test_row_filter_in_escapes_string_literals():
    req = ErasureRequest(table="t", key={"name": ["O'Brien", "Bo"]})
    assert req.row_filter() == "name IN ('O''Brien', 'Bo')"


def test_row_filter_single_element_list_renders_as_equality():
    # How the caller wrapped one value must not change the predicate.
    assert ErasureRequest(table="t", key={"user_id": [42]}).row_filter() == "user_id = 42"
    assert ErasureRequest(table="t", key={"user_id": (42,)}).row_filter() == "user_id = 42"


def test_row_filter_mixes_in_and_equality_across_columns():
    req = ErasureRequest(table="t", key={"user_id": [1, 2], "tenant": "acme"})
    assert req.row_filter() == "tenant = 'acme' AND user_id IN (1, 2)"


def test_row_filter_rejects_empty_value_list():
    with pytest.raises(ValueError, match="empty value list"):
        ErasureRequest(table="t", key={"user_id": []}).row_filter()


def test_parse_key_collects_repeated_column_into_a_list():
    from iceforget.cli import _parse_key

    assert _parse_key(["user_id=1", "user_id=2"]) == {"user_id": [1, 2]}
    # A single occurrence stays a scalar; distinct columns are unaffected.
    assert _parse_key(["user_id=1", "tenant=acme"]) == {"user_id": 1, "tenant": "acme"}


def test_parse_key_repeated_column_renders_as_in():
    from iceforget.cli import _parse_key

    req = ErasureRequest(table="t", key=_parse_key(["user_id=1", "user_id=2"]))
    assert req.row_filter() == "user_id IN (1, 2)"


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
