"""Pluggable execution engines.

An engine knows how to talk to a specific compute layer (PyIceberg today;
Spark and iceberg-rust are on the roadmap). Everything above the engine —
the coordinator, indexer, verifier, auditor — is engine-agnostic and speaks
only the :class:`~iceforget.engines.base.Engine` protocol.
"""

from iceforget.engines.base import DeleteResult, Engine, ExpireResult, PlannedFile

__all__ = ["Engine", "PlannedFile", "DeleteResult", "ExpireResult"]
