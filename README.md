# qShape

AST-level canonicalization and fingerprinting of PostgreSQL queries,
plus a `pg_stat_statements` capture command that ranks workload by
execution time, not just call count.

`pg_stat_statements.queryid` hashes the parse tree after literal-to-`$N`
substitution but before any AST normalization. ORMs (Rails,
ActiveRecord, SQLAlchemy, Prisma, Sequelize) produce many queryids for
one logical query shape. `qshape` collapses those variants to a single
canonical fingerprint and aggregates their timing.

## Install

Homebrew:

```
brew install boringsql/boringsql/qshape
```

From source:

```
go install github.com/boringsql/qshape/cmd/qshape@latest
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
