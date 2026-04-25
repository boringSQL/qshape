# qShape

AST-level canonicalization and fingerprinting of PostgreSQL queries,
plus a `pg_stat_statements` capture command that ranks workload by
execution time, not just call count.

`pg_stat_statements.queryid` hashes the parse tree after literal-to-`$N`
substitution but before any AST normalization. ORMs (Rails,
ActiveRecord, SQLAlchemy, Prisma, Sequelize) produce many queryids for
one logical query shape. `qshape` collapses those variants to a single
canonical fingerprint and aggregates their timing.

## Measured corpus reduction

Typical reduction on an ORM-heavy `pg_stat_statements` snapshot is
**30–50%**. Enough that per-shape timing aggregates stop fragmenting
across near-duplicate `queryid`s. The upper bound depends on how much
of the workload is alias / predicate-order variation vs. genuinely
distinct shapes.

On one real production snapshot we measured **4,716 distinct queryids
to 177 canonical fingerprints (96.2% reduction)** — an outlier, driven
by heavy ORM alias churn on a small set of underlying query shapes.
See `crates/qshape-core/src/reshape.rs` for the AST transformations that drive the collapse.

## Install

Homebrew:

```
brew install boringsql/boringsql/qshape
```

From source:

```
cargo install --path crates/qshape-cli
```

Pre-built binaries for macOS and Linux (amd64 + arm64) are published on
each release via [GoReleaser](https://github.com/boringsql/qshape/releases).

## CLI

```
qshape normalize "SELECT u.id FROM users u WHERE u.id = 1"
SELECT.id FROM users u WHERE id = 1

qshape fingerprint "SELECT id FROM users WHERE id = 1"
sha1:63fe28385e8b4d95

qshape capture "postgres://user:pass@host/db" > queries.json

qshape attribute --in clusters.json --conn "$DATABASE_URL" > queries-attributed.json
```

`capture` connects directly to a PostgreSQL node and reads
`pg_stat_statements` with original whitespace and timing intact, then
writes `{"clusters":[...]}` to stdout sorted by descending
`total_exec_time_ms`. Passing `-` (or omitting the SQL arg) to
`normalize` / `fingerprint` reads stdin.

## License

BSD 2-Clause.
