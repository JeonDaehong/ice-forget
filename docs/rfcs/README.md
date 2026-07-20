# IceForget RFCs

Non-trivial changes to IceForget are designed in the open as **RFCs** (Requests
for Comments) before they are implemented. This is how we practice the
"design in public" habit the Apache Way expects — and how we make sure the hard
compliance/correctness questions are settled *before* code, not after.

## When you need an RFC

Open an RFC for anything that:

- changes on-disk or catalog semantics (how we mutate Iceberg metadata/data),
- adds a new processing mode, engine, or public API surface,
- has correctness, legal-interpretation, or irreversibility implications, or
- is large enough that reviewers would want to agree on the shape first.

Small, self-contained changes (a bug fix, a CLI flag, docs) do **not** need an
RFC — just open a PR.

## Statuses

| Status | Meaning |
|--------|---------|
| `Draft` | Being written; not yet ready for broad review. |
| `Discussion` | Open for community feedback (via the linked issue/PR). |
| `Accepted` | Design agreed; ready to implement (tracked by an issue). |
| `Implemented` | Landed; the RFC is now historical record. |
| `Rejected` | Decided against; kept for the record and rationale. |
| `Superseded` | Replaced by a later RFC (link it). |

## Process

1. Copy [`0000-template.md`](0000-template.md) to `NNNN-short-title.md`, where
   `NNNN` is the next unused 4-digit number.
2. Open it as a PR with status `Draft`, then flip to `Discussion` and open a
   companion issue for the conversation (so discussion is searchable).
3. Iterate. When consensus is reached, a maintainer sets the status to
   `Accepted` (or `Rejected`, with reasons) and merges.
4. Implementation happens in follow-up PRs that link back to the RFC. When it
   lands, set the status to `Implemented`.

## Index

| # | Title | Status |
|---|-------|--------|
| [0001](0001-surgical-history-rewrite.md) | Surgical History Rewrite | Discussion |
