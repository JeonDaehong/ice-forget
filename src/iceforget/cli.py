"""``iceforget`` command-line interface.

    iceforget index  --policy p.yaml --table db.users --key user_id=42
    iceforget erase  --policy p.yaml --table db.users --key user_id=42 --cert-dir ./certs
    iceforget verify --policy p.yaml --table db.users --key user_id=42
    iceforget demo   # self-contained end-to-end run, no catalog required
"""

from __future__ import annotations

import json

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table as RichTable

from iceforget import __version__

app = typer.Typer(
    name="iceforget",
    help="Right-to-be-forgotten compliance engine for Apache Iceberg.",
    no_args_is_help=True,
    add_completion=False,
)
console = Console()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _parse_key(pairs: list[str]) -> dict:
    """Parse repeated ``col=value`` options into a typed key dict.

    Repeating the *same* column collects its values into a list, which renders
    as an ``IN`` predicate — ``-k user_id=1 -k user_id=2`` erases both. Distinct
    columns are AND-ed as before.
    """
    key: dict = {}
    for pair in pairs:
        if "=" not in pair:
            raise typer.BadParameter(f"--key must be col=value, got {pair!r}")
        col, _, raw = pair.partition("=")
        col = col.strip()
        value = _coerce(raw.strip())
        if col in key:
            key[col].append(value)
        else:
            key[col] = [value]
    # Unwrap single-valued columns so the common case stays a plain scalar.
    return {col: vals[0] if len(vals) == 1 else vals for col, vals in key.items()}


def _coerce(raw: str):
    low = raw.lower()
    if low in ("true", "false"):
        return low == "true"
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return float(raw)
    except ValueError:
        pass
    return raw


def _load(policy_path: str):
    from iceforget.coordinator import ErasureCoordinator
    from iceforget.policy import load_policy

    policy = load_policy(policy_path)
    return ErasureCoordinator.from_policy(policy)


def _print_index(index) -> None:
    console.print(
        Panel.fit(
            f"[bold]{index.table}[/bold]   filter: [cyan]{index.row_filter}[/cyan]\n"
            f"snapshots scanned: {index.scanned_snapshots}    "
            f"snapshots with matches: [yellow]{index.snapshots_with_matches}[/yellow]    "
            f"files in blast radius: [yellow]{index.matched_files}[/yellow]",
            title="blast radius",
        )
    )
    if not index.matches:
        console.print("  [green]no data files reference this subject.[/green]")
        return
    tbl = RichTable(show_header=True, header_style="bold")
    tbl.add_column("snapshot")
    tbl.add_column("current")
    tbl.add_column("rows", justify="right")
    tbl.add_column("bytes", justify="right")
    # overflow="ignore" so Rich clips instead of inserting a unicode ellipsis,
    # which Windows legacy code pages (e.g. cp949) can't render.
    tbl.add_column("file", overflow="ignore", no_wrap=True)
    for m in index.matches:
        tbl.add_row(
            str(m.snapshot_id),
            "*" if m.is_current else "",
            str(m.record_count),
            str(m.file_size_bytes),
            _short(m.file_path),
        )
    console.print(tbl)


def _short(path: str, width: int = 48) -> str:
    return path if len(path) <= width else "..." + path[-(width - 3) :]


def _print_json(report) -> None:
    """Emit a report as JSON on stdout.

    Uses the builtin ``print`` rather than the Rich console on purpose: Rich
    would wrap long lines and interpret square brackets as markup, which would
    corrupt the payload a downstream parser reads.
    """
    print(json.dumps(report.to_dict(), indent=2))


# ---------------------------------------------------------------------------
# commands
# ---------------------------------------------------------------------------


@app.command()
def version() -> None:
    """Print the IceForget version."""
    console.print(f"iceforget {__version__}")


@app.command()
def index(
    table: str = typer.Option(..., help="Table identifier, e.g. db.users"),
    key: list[str] = typer.Option(..., "--key", "-k", help="Identifier col=value (repeatable)"),
    policy: str = typer.Option(..., "--policy", "-p", help="Path to policy file"),
    json_output: bool = typer.Option(
        False, "--json", help="Print the raw IndexReport as JSON instead of a table"
    ),
) -> None:
    """Show the blast radius of an erasure without mutating anything."""
    coordinator = _load(policy)
    request_key = _parse_key(key)
    result = coordinator.erase(table, request_key, dry_run=True)
    if json_output:
        _print_json(result.index)
        return
    _print_index(result.index)


@app.command()
def erase(
    table: str = typer.Option(..., help="Table identifier, e.g. db.users"),
    key: list[str] = typer.Option(..., "--key", "-k", help="Identifier col=value (repeatable)"),
    policy: str = typer.Option(..., "--policy", "-p", help="Path to policy file"),
    subject: str = typer.Option("", help="Optional subject label for the audit trail"),
    cert_dir: str | None = typer.Option(None, help="Directory to write the erasure certificate"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Plan only; do not mutate"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt"),
) -> None:
    """Erase a subject: delete → compact → expire → verify → certify."""
    coordinator = _load(policy)
    request_key = _parse_key(key)

    # Always show the blast radius first, even for a real run.
    preview = coordinator.erase(table, request_key, subject=subject, dry_run=True)
    _print_index(preview.index)

    if dry_run:
        console.print("[dim]dry-run: no changes made.[/dim]")
        return

    if not yes:
        confirm = typer.confirm(
            f"Irreversibly erase {request_key} from {table} "
            f"(expires history down to retention)?"
        )
        if not confirm:
            console.print("[yellow]aborted.[/yellow]")
            raise typer.Exit(code=1)

    result = coordinator.erase(table, request_key, subject=subject)
    _print_result(result)

    certificate = coordinator.certify(result)
    if cert_dir:
        path = coordinator.write_certificate(certificate, cert_dir)
        console.print(f"certificate: [green]{path}[/green]")
    if not result.success:
        raise typer.Exit(code=2)


@app.command()
def verify(
    table: str = typer.Option(..., help="Table identifier, e.g. db.users"),
    key: list[str] = typer.Option(..., "--key", "-k", help="Identifier col=value (repeatable)"),
    policy: str = typer.Option(..., "--policy", "-p", help="Path to policy file"),
    json_output: bool = typer.Option(
        False, "--json", help="Print the raw VerifyReport as JSON instead of a summary"
    ),
) -> None:
    """Scan every reachable snapshot for residual rows of a subject."""
    coordinator = _load(policy)
    request_key = _parse_key(key)
    result = coordinator.erase(table, request_key, dry_run=True)
    v = result.verify
    if json_output:
        # Exit code still signals the verdict, so `--json` stays usable as a
        # CI gate without having to parse the payload first.
        _print_json(v)
        if not v.clean:
            raise typer.Exit(code=2)
        return
    if v.clean:
        console.print(
            f"[green]clean[/green]: 0 residual rows across {v.scanned_snapshots} snapshots."
        )
    else:
        console.print(
            f"[red]residual[/red]: {v.residual_rows} rows in snapshots "
            f"{v.residual_snapshots} ({v.scanned_snapshots} scanned)."
        )
        raise typer.Exit(code=2)


@app.command()
def demo() -> None:
    """Run a self-contained end-to-end erasure against a temporary local table."""
    from iceforget.demo import run_demo

    run_demo(console)


def _print_result(result) -> None:
    v = result.verify
    if not v.clean:
        verdict = "[red]RESIDUAL DETECTED[/red]"
    elif not result.bytes_erased:
        # Catalog-clean but the bytes survive: not an erasure, and the verdict
        # must not imply otherwise.
        verdict = "[red]BYTES ON DISK[/red]"
    else:
        verdict = "[green]ERASED[/green]"
    lines = [
        f"mode: [cyan]{result.method}[/cyan]",
        f"rows deleted: [bold]{result.rows_deleted}[/bold]",
    ]
    if result.method == "surgical":
        # The surgical path's whole claim is that history survived; show it
        # next to the verdict rather than burying it in the certificate.
        preserved = (
            "[green]preserved[/green]"
            if result.time_travel_preserved
            else "[red]LOST[/red]"
        )
        lines += [
            f"snapshots rewritten: {len(result.snapshots_rewritten)}",
            f"files rewritten: {result.files_rewritten}",
            f"time travel: {preserved}",
        ]
    else:
        lines.append(f"snapshots expired: {len(result.expired_snapshot_ids)}")
    bytes_state = (
        f"[green]{len(result.files_purged)} file(s) deleted[/green]"
        if result.bytes_erased
        else f"[red]{result.files_left_on_disk} file(s) STILL ON DISK[/red]"
    )
    lines += [
        f"residual rows after erasure: {v.residual_rows}",
        f"bytes on disk: {bytes_state}",
        f"verdict: {verdict}",
    ]
    console.print(Panel.fit("\n".join(lines), title="erasure result"))


if __name__ == "__main__":
    app()
