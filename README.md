# IceForget

**Right-to-be-forgotten compliance engine for [Apache Iceberg](https://iceberg.apache.org/).**

> Iceberg time travel is a compliance liability. Deleting a row from the current
> snapshot leaves it physically present in every historical data file. IceForget
> runs the erasure the right way — and **proves** the data is gone.

Iceberg's snapshot history is a double-edged sword: a `DELETE` on the live table
removes rows from the current view, but the same rows sit untouched in the data
files that older snapshots still point at. Meeting GDPR / CCPA / Korea PIPA
erasure obligations therefore means a manual `DELETE → compact → expire`
pipeline, run per request, per table, and then *somehow* proving it worked.

There is no dedicated open-source tool for this. IceForget is that tool.

```console
$ iceforget erase --policy policy.yaml --table db.users --key user_id=42 --cert-dir ./certs
blast radius: 1 file(s) across 3 snapshot(s) reference user 42
Irreversibly erase {'user_id': 42} from db.users ...? [y/N]: y
rows deleted: 1   snapshots expired: 2   residual rows after erasure: 0
verdict: ERASED
certificate: certs/erasure-a1b2c3d4e5f6.json
```

---

## Status

**Alpha — v0.0.1.** The MVP does one thing end to end and does it honestly:
orchestrated erasure plus verifiable proof. See [Scope](#scope--honest-limits)
before relying on it for a real obligation.

## Install

```bash
pipx install iceforget          # or: pip install iceforget
```

Try it with zero setup — no catalog, no cloud, no Spark:

```bash
pip install 'iceforget[demo]'
iceforget demo
```

`iceforget demo` builds a throwaway Iceberg table (SQLite catalog + local
warehouse) with a subject buried in an old snapshot, erases it, and prints the
before/after plus a certificate.

## How it works

For one subject key, IceForget runs a single governed, audited operation:

| Stage | What it does |
|-------|--------------|
| **index** | Uses Iceberg scan-planning (no full read) to find every data file, across *all* snapshots, that serves the subject — the "blast radius". |
| **delete** | Copy-on-write delete of matching rows from the live snapshot. |
| **compact** | Consolidate rewritten files (best effort; a no-op on the PyIceberg engine, which rewrites during delete). |
| **expire** | Expire snapshots down to the table's retention budget so files still referencing the PII become orphaned and physically deletable. |
| **verify** | Re-scan **every reachable snapshot** and count residual rows. Zero = clean. |
| **certify** | Emit a tamper-evident JSON [erasure certificate](#the-erasure-certificate): who, what table, when received/completed, rows, files, snapshots expired, residual count, SHA-256 over the body. |

Everything above the compute layer is engine-agnostic; the
[`Engine`](src/iceforget/engines/base.py) protocol is the single seam, so a
Spark or `iceberg-rust` engine can drop in later without touching the pipeline.

## Policy

Bind the per-table rules once, in version control, next to your data contracts:

```yaml
# policy.yaml
catalog:
  name: prod
  properties:                       # passed straight to pyiceberg.load_catalog
    type: rest
    uri: https://catalog.example.com
    warehouse: s3://lake/warehouse

tables:
  - table: db.users
    identifier_columns: [user_id]   # erasure keys must be one of these
    retain_last_snapshots: 1        # time-travel budget kept after erasure
    expire_older_than_days: 7       # only expire snapshots older than this
    sla_days: 30                    # GDPR deadline, for the SLA tracker
```

Erasing on a column that isn't an `identifier_column` is refused — a guardrail
against over-deletion.

## CLI

```bash
iceforget index  -p policy.yaml --table db.users -k user_id=42   # blast radius, no mutation
iceforget erase  -p policy.yaml --table db.users -k user_id=42 --cert-dir ./certs
iceforget verify -p policy.yaml --table db.users -k user_id=42   # residual scan only
iceforget demo                                                   # self-contained run
```

`--key/-k` is repeatable and AND-ed: `-k tenant=acme -k user_id=42`.
`--dry-run` on `erase` shows the blast radius and projected outcome without
mutating.

## The erasure certificate

```json
{
  "request_id": "erasure-a1b2c3d4e5f6",
  "table": "db.users",
  "key_columns": ["user_id"],
  "processing_mode": "orchestrate",
  "outcome": "erased",
  "rows_deleted": 1,
  "files_in_blast_radius": 1,
  "snapshots_expired": [123, 456],
  "residual_rows": 0,
  "received_at": "2026-07-20T09:00:00+00:00",
  "completed_at": "2026-07-20T09:00:04+00:00",
  "tool_version": "0.0.1",
  "body_sha256": "…"
}
```

`body_sha256` covers the full body; `ErasureCertificate.verify_integrity()`
detects any later edit.

## Scope & honest limits

IceForget is a **technical measure**, not legal advice. The MVP deliberately
implements the standard *orchestrate-and-prove* pipeline, **not** the
time-travel-preserving "surgical history rewrite" (that's on the roadmap).
Concretely:

- **Expiry removes reachable history** down to `retain_last_snapshots`. That is
  the intended compliance trade-off, but it *does* shorten time travel.
- **Verification checks catalog-reachable snapshots.** Files already unreferenced
  but not yet garbage-collected, and copies outside the table (backups, external
  exports, downstream systems), are out of scope and must be handled separately.
- **No warranty of legal sufficiency.** Crypto-shredding's legal validity varies
  by jurisdiction; consult counsel.

## Roadmap

- Surgical history rewrite (erase a subject while preserving time travel) —
  design in [RFC 0001](docs/rfcs/0001-surgical-history-rewrite.md)
- Crypto-shredding mode (per-subject KMS keys: AWS KMS / GCP KMS / Vault)
- Deletion-request queue + SLA tracker (30-day deadline monitoring)
- Spark and `iceberg-rust` engines
- PDF certificates
- Glue / Hive catalog adapters

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). IceForget is developed in the open under
[Apache License 2.0](LICENSE) with an eye toward the Apache Incubator; design
decisions run as public RFCs in [`docs/rfcs/`](docs/rfcs/).

## License

[Apache License 2.0](LICENSE).
