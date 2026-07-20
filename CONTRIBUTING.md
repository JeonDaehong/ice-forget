# Contributing to IceForget

Thanks for your interest! IceForget is developed in the open, with an eye toward
the Apache Incubator. That shapes how we work: design in public, keep the
project vendor-neutral, and welcome contributors from many organizations.

## Ground rules

- **Code of Conduct.** Participation is governed by our
  [Code of Conduct](CODE_OF_CONDUCT.md). Be kind.
- **License.** All contributions are under [Apache License 2.0](LICENSE). By
  opening a PR you agree your contribution is licensed under it.
- **Design in public.** Non-trivial changes start as an RFC (a discussion or a
  doc under `docs/rfcs/`) before implementation. "If it isn't written down, it
  didn't happen."
- **No category-X dependencies.** Do not add GPL/AGPL or other Apache
  category-X dependencies — it would block the incubation path.

## Development

```bash
python -m venv .venv && . .venv/Scripts/activate   # or bin/activate on *nix
pip install -e ".[dev,demo]"
pytest
ruff check src tests
```

`iceforget demo` is the fastest way to exercise the whole pipeline end to end.

## Architecture in one breath

The pipeline (`index → delete → compact → expire → verify → certify`) lives in
`coordinator.py` and speaks only to the [`Engine`](src/iceforget/engines/base.py)
protocol. Adding support for a new compute layer (Spark, iceberg-rust) means
implementing that one protocol — nothing above it should need to change. Keep
that seam clean.

## Good first issues

We aim to keep 15+ `good first issue`s open at all times, with a 48-hour
first-review SLA. If you don't see one that fits, open a discussion.
