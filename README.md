# chdh-simpledb

AWS SimpleDB convenience wrapper. Provides `Record`, `QuerySelect`, `Paginator`,
`BatchUpdate`, `Domain`, and `SimpleDatabase` classes with automatic large-value
offloading to S3.

## Versions

This repo maintains **two parallel lineages**, not a linear history:

- **`1.x`** — Lightweight version. Used by experiments in the LambdaExpt framework.
  Includes `Domain.backup()` / `Domain.restore()` and an `SIMPLEDB_S3_BUCKET`
  environment variable fallback.

- **`2.x`** — Full-featured version. Adds chunked `_attrs_type` storage for
  records whose type-map exceeds 1KB, `uuid4` key generation, and stricter
  validation. Stricter `s3_bucket` requirement (no env-var fallback).

Pick whichever version matches your project's needs. Choosing `2.x` over `1.x`
is **not** an upgrade — they are parallel codebases that have intentionally
diverged.

## Installation

In your project's `requirements.txt`:

```
# 1.x lineage (LambdaExpt experiments)
chdh-simpledb @ git+https://github.com/complex-human-data-hub/SimpleDB.git@v1.0.0

# 2.x lineage (other projects)
chdh-simpledb @ git+https://github.com/complex-human-data-hub/SimpleDB.git@v2.0.0
```

The Python import is the same regardless of version:

```python
from simpledb import Record, QuerySelect, Domain
```

## Branches

| Branch  | Lineage | Purpose                                  |
|---------|---------|------------------------------------------|
| `1.x`   | V2      | v1 lineage maintenance                   |
| `2.x`   | V5      | v2 lineage maintenance                   |
| `main`  | -       | Future home of merged v3 (not yet built) |

Bug fixes that affect both lineages should be applied to both branches.

## Development

Editable install for local development:

```
pip install -e /home/bens/SimpleDB/simpledb
```

## License

MIT
