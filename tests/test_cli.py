"""CLI-level tests, driven through Typer's runner against a real local table."""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from iceforget.cli import app

runner = CliRunner()


@pytest.fixture
def policy_file(tmp_path, users_table):
    """A policy pointing at the same SQLite catalog the fixtures built.

    ``users_table`` is requested so the table exists before the CLI loads it;
    the paths mirror the ``catalog`` fixture in conftest.
    """
    path = tmp_path / "policy.yaml"
    path.write_text(
        f"""
catalog:
  name: test
  properties:
    type: sql
    uri: sqlite:///{(tmp_path / "catalog.db").as_posix()}
    warehouse: file://{(tmp_path / "warehouse").as_posix()}
tables:
  - table: db.users
    identifier_columns: [user_id]
    retain_last_snapshots: 1
""",
        encoding="utf-8",
    )
    return str(path)


def _run(*args):
    return runner.invoke(app, list(args))


def test_index_json_is_parseable_and_has_expected_keys(policy_file):
    result = _run("index", "--table", "db.users", "-k", "user_id=42", "-p", policy_file, "--json")
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["table"] == "db.users"
    assert payload["row_filter"] == "user_id = 42"
    assert payload["scanned_snapshots"] == 3
    # Subject 42 lives in one file, referenced by all three snapshots.
    assert len(payload["matches"]) == 3
    assert {"snapshot_id", "file_path", "record_count"} <= set(payload["matches"][0])


def test_verify_json_is_parseable_and_flags_residual(policy_file):
    result = _run("verify", "--table", "db.users", "-k", "user_id=42", "-p", policy_file, "--json")
    # Subject is still present, so the CI-gate exit code survives --json.
    assert result.exit_code == 2
    payload = json.loads(result.stdout)
    assert payload["table"] == "db.users"
    assert payload["residual_rows"] == 3
    assert payload["scanned_snapshots"] == 3


def test_verify_json_clean_subject_exits_zero(policy_file):
    result = _run("verify", "--table", "db.users", "-k", "user_id=999", "-p", policy_file, "--json")
    assert result.exit_code == 0
    assert json.loads(result.stdout)["residual_rows"] == 0


def test_repeated_key_for_one_column_becomes_an_in_predicate(policy_file):
    result = _run(
        "index", "--table", "db.users",
        "-k", "user_id=42", "-k", "user_id=21",
        "-p", policy_file, "--json",
    )
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["row_filter"] == "user_id IN (42, 21)"
    # Both subjects are in range, not just the last -k.
    assert len({m["file_path"] for m in payload["matches"]}) == 2


def test_default_output_is_still_rich(policy_file):
    """No --json => the human-readable rendering, and no JSON on stdout."""
    result = _run("index", "--table", "db.users", "-k", "user_id=42", "-p", policy_file)
    assert result.exit_code == 0
    assert "blast radius" in result.stdout
    with pytest.raises(json.JSONDecodeError):
        json.loads(result.stdout)
