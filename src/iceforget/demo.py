"""Self-contained end-to-end demo: ``iceforget demo``.

Builds a throwaway Iceberg table in a temp dir (SQLite catalog + local
warehouse), writes a few subjects across several snapshots so there *is* a
history to erase from, then runs the full pipeline against subject
``user_id = 42`` and prints the before/after. No external catalog, cloud
credentials, or Spark cluster required.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from iceforget import __version__


def run_demo(console) -> None:
    try:
        import pyarrow as pa
        from pyiceberg.catalog.sql import SqlCatalog
    except ImportError as exc:  # pragma: no cover
        console.print(
            f"[red]demo needs the 'demo' extra:[/red] pip install 'iceforget[demo]'  ({exc})"
        )
        raise SystemExit(1) from exc

    from iceforget.coordinator import ErasureCoordinator
    from iceforget.engines.pyiceberg_engine import PyIcebergEngine
    from iceforget.policy import CatalogConfig, Policy, TablePolicy

    # ignore_cleanup_errors: on Windows the SQLite catalog file stays memory-mapped
    # by the process until exit, which would otherwise break temp-dir teardown.
    with tempfile.TemporaryDirectory(prefix="iceforget-demo-", ignore_cleanup_errors=True) as tmp:
        tmp_path = Path(tmp)
        warehouse = tmp_path / "warehouse"
        warehouse.mkdir()
        db_uri = f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}"

        catalog = SqlCatalog(
            "demo",
            uri=db_uri,
            warehouse=f"file://{warehouse.as_posix()}",
        )
        catalog.create_namespace("db")

        schema = pa.schema(
            [
                ("user_id", pa.int64()),
                ("name", pa.string()),
                ("email", pa.string()),
                ("country", pa.string()),
            ]
        )

        # Snapshot 1: subject 42 is born here, in an old data file.
        table = catalog.create_table("db.users", schema=schema)
        table.append(
            pa.table(
                {
                    "user_id": [42, 7, 13],
                    "name": ["Dana", "Amir", "Bo"],
                    "email": ["dana@x.io", "amir@x.io", "bo@x.io"],
                    "country": ["DE", "FR", "KR"],
                },
                schema=schema,
            )
        )
        # Snapshots 2 & 3: more history accrues; 42 stays put in the old file.
        table.append(
            pa.table(
                {"user_id": [21, 34], "name": ["Cleo", "Eli"],
                 "email": ["cleo@x.io", "eli@x.io"], "country": ["IT", "ES"]},
                schema=schema,
            )
        )
        table.append(
            pa.table(
                {"user_id": [55, 89], "name": ["Fin", "Gia"],
                 "email": ["fin@x.io", "gia@x.io"], "country": ["NL", "PT"]},
                schema=schema,
            )
        )

        policy = Policy(
            catalog=CatalogConfig(name="demo"),
            tables=[
                TablePolicy(
                    table="db.users",
                    identifier_columns=["user_id"],
                    retain_last_snapshots=1,
                )
            ],
        )
        coordinator = ErasureCoordinator(PyIcebergEngine(catalog), policy)

        console.rule(f"IceForget demo  (v{__version__})")
        console.print("Built db.users with 3 snapshots; subject [bold]user_id=42[/bold] lives in "
                      "the oldest data file.\n")

        # Blast radius first.
        preview = coordinator.erase("db.users", {"user_id": 42}, dry_run=True)
        console.print(
            f"[cyan]blast radius[/cyan]: {preview.index.matched_files} file(s) across "
            f"{preview.index.snapshots_with_matches} snapshot(s) reference user 42."
        )

        # Real erasure.
        result = coordinator.erase("db.users", {"user_id": 42}, subject="demo-subject")
        console.print(
            f"[cyan]erasure[/cyan]: deleted {result.rows_deleted} row(s), "
            f"expired {len(result.expired_snapshot_ids)} snapshot(s)."
        )

        verdict = "[green]ERASED - 0 residual rows[/green]" if result.success else (
            f"[red]RESIDUAL: {result.verify.residual_rows} row(s) remain[/red]"
        )
        console.print(f"[cyan]verify[/cyan]: {verdict}")

        cert = coordinator.certify(result)
        console.print(
            f"[cyan]certificate[/cyan]: {cert.request_id}  outcome={cert.outcome}  "
            f"sha256={cert.body_sha256[:16]}...  integrity_ok={cert.verify_integrity()}"
        )
        console.rule("done")
