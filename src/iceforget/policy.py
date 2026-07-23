"""Policy store: how each table is allowed to be erased.

A policy file binds the moving parts an operator must decide *once* per table —
which columns identify a subject, how aggressively to expire history, which
processing mode applies — so that individual erasure requests stay a one-liner
and the rules live in version control next to the rest of your data contracts.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator

ProcessingMode = Literal["orchestrate", "surgical", "crypto-shred"]

# Modes the coordinator can actually dispatch. A mode that is declared but not
# implemented must be refused at load time: the erasure certificate attests
# `processing_mode`, so accepting one we don't run would make the certificate
# claim something untrue.
IMPLEMENTED_MODES = frozenset({"orchestrate", "surgical"})


class CatalogConfig(BaseModel):
    """PyIceberg catalog connection. ``properties`` is passed straight to
    ``pyiceberg.catalog.load_catalog``, so anything PyIceberg understands works
    (``uri``, ``warehouse``, ``token``, ``s3.*`` credentials, ...)."""

    name: str = "default"
    properties: dict[str, Any] = Field(default_factory=dict)


class TablePolicy(BaseModel):
    """Erasure rules for one Iceberg table."""

    table: str
    identifier_columns: list[str]
    mode: ProcessingMode = "orchestrate"
    # Keep this many of the newest snapshots after erasure (time-travel budget).
    retain_last_snapshots: int = 1
    # Only expire snapshots older than this many days; None = ignore age.
    expire_older_than_days: int | None = None
    # Compliance deadline used by the SLA tracker, in days from receipt.
    sla_days: int = 30

    @field_validator("identifier_columns")
    @classmethod
    def _non_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("identifier_columns must list at least one column")
        return v

    @field_validator("retain_last_snapshots")
    @classmethod
    def _retain_at_least_one(cls, v: int) -> int:
        if v < 1:
            raise ValueError("retain_last_snapshots must be >= 1 (the live snapshot)")
        return v

    @field_validator("mode")
    @classmethod
    def _mode_is_implemented(cls, v: str) -> str:
        if v not in IMPLEMENTED_MODES:
            raise ValueError(
                f"processing mode {v!r} is not implemented yet. "
                f"Supported: {sorted(IMPLEMENTED_MODES)}. Refusing rather than running a "
                f"different mode than the erasure certificate would attest."
            )
        return v

    @field_validator("expire_older_than_days")
    @classmethod
    def _expire_days_non_negative(cls, v: int | None) -> int | None:
        if v is not None and v < 0:
            raise ValueError("expire_older_than_days must be >= 0, or null to ignore age")
        return v


class Policy(BaseModel):
    """The whole policy document: one catalog, many table rules."""

    catalog: CatalogConfig = Field(default_factory=CatalogConfig)
    tables: list[TablePolicy] = Field(default_factory=list)

    def for_table(self, identifier: str) -> TablePolicy:
        for tp in self.tables:
            if tp.table == identifier:
                return tp
        raise KeyError(
            f"No policy for table {identifier!r}. Add it to the policy file under `tables:`."
        )

    def has_table(self, identifier: str) -> bool:
        return any(tp.table == identifier for tp in self.tables)


def load_policy(path: str | Path) -> Policy:
    """Load and validate a policy file (YAML or JSON)."""
    raw = Path(path).read_text(encoding="utf-8")
    data = yaml.safe_load(raw) or {}
    return Policy.model_validate(data)
