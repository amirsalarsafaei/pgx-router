# pgx-router

![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)
[![Latest Release](https://img.shields.io/github/v/release/amirsalarsafaei/pgx-router)](https://github.com/amirsalarsafaei/pgx-router/releases/latest)
[![codecov](https://codecov.io/github/amirsalarsafaei/pgx-router/graph/badge.svg?token=7ue7iNJkCH)](https://codecov.io/github/amirsalarsafaei/pgx-router)

`pgx-router` is a Go library that automatically routes PostgreSQL queries to a primary (read-write) or replica (read-only) connection pool based on the type of SQL statement. It wraps [`pgxpool.Pool`](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool) from [pgx](https://github.com/jackc/pgx) and is designed to be a drop-in addition for applications that want to offload read traffic to replicas without changing query code.

## Features

- **Automatic read/write routing** — uses a full PostgreSQL query parser ([pg_query_go](https://github.com/pganalyze/pg_query_go)) to classify every statement. `SELECT` queries go to the replica; `INSERT`, `UPDATE`, `DELETE`, and other mutating statements go to the primary.
- **Comment-based overrides** — prepend a SQL comment to force a specific pool for any individual query (useful for transactions, read-your-writes scenarios, etc.).
- **Locking clause detection** — `SELECT … FOR UPDATE / FOR SHARE` is treated as a write and routed to the primary.
- **Writable CTE detection** — `WITH … INSERT/UPDATE/DELETE … SELECT` is correctly classified as a write.
- **Automatic fallback on read-only errors** — if the replica returns a PostgreSQL `read_only_sql_transaction` error (e.g. after a failover), the query is transparently retried on the primary.
- **Custom retry hook** — supply a `WithRetryOnError` callback to implement your own fallback policy in addition to the built-in detection (e.g. retry on `pgx.ErrNoRows` or connection errors).
- **Single-pool mode** — pass `nil` as the read pool and the primary is used for all queries (no-op routing).

## Installation

```sh
go get github.com/amirsalarsafaei/pgx-router
```

## Quick Start

```go
import (
    pgxrouter "github.com/amirsalarsafaei/pgx-router"
    "github.com/jackc/pgx/v5/pgxpool"
)

// Create your primary and replica pools as usual.
primaryPool, _ := pgxpool.New(ctx, "postgres://user:pass@primary/db")
replicaPool, _ := pgxpool.New(ctx, "postgres://user:pass@replica/db")

// Wrap them in a router pool.
pool := pgxrouter.New(primaryPool, replicaPool)
defer pool.Close()

// Use pool just like *pgxpool.Pool.
// This SELECT is automatically sent to the replica.
rows, err := pool.Query(ctx, "SELECT id, name FROM users WHERE active = true")

// This INSERT is automatically sent to the primary.
_, err = pool.Exec(ctx, "INSERT INTO events (type) VALUES ($1)", "signup")
```

## API

### `New`

```go
func New(main, read *pgxpool.Pool, opts ...Option) *Pool
```

Creates a new routing pool. If `read` is `nil` the primary pool is used for all queries.

### `Pool` methods

`Pool` embeds `*pgxpool.Pool`, so all methods of the underlying pool are available. The following methods have routing logic applied:

| Method | Behaviour |
|--------|-----------|
| `Exec(ctx, sql, args...)` | Routes to read or primary based on the SQL statement. |
| `Query(ctx, sql, args...)` | Routes to read or primary. Retries on primary if a read-only error is encountered during row iteration. |
| `QueryRow(ctx, sql, args...)` | Routes to read or primary. Retries on primary when `Scan` returns a read-only error. |
| `Close()` | Closes both pools (or just the primary if both are the same). |
| `Reset()` | Resets both pools (or just the primary if both are the same). |

### Accessors

```go
pool.MainPool() *pgxpool.Pool  // returns the primary pool
pool.ReadPool()  *pgxpool.Pool  // returns the replica pool (or primary if no replica was provided)
```

### Options

#### `WithRetryOnError`

```go
func WithRetryOnError(fn func(error) bool) Option
```

Registers a custom function that decides whether a failed read-replica query should be retried on the primary pool. The function receives the error returned by the replica and returns `true` to trigger a retry. This is evaluated _in addition to_ the built-in `read_only_sql_transaction` detection, so either condition can trigger the fallback.

```go
pool := pgxrouter.New(primary, replica,
    pgxrouter.WithRetryOnError(func(err error) bool {
        // Retry on not-found: the replica may lag behind the primary.
        if errors.Is(err, pgx.ErrNoRows) {
            return true
        }
        // Also retry on connection-level errors.
        var netErr *net.OpError
        return errors.As(err, &netErr)
    }),
)
```

## Routing Rules

### Automatic classification

Queries are classified using the PostgreSQL parser. The default rules are:

| Statement type | Pool |
|----------------|------|
| `SELECT` (no locking clause) | Replica |
| `SELECT … FOR UPDATE / FOR SHARE / FOR NO KEY UPDATE / FOR KEY SHARE` | Primary |
| `INSERT`, `UPDATE`, `DELETE` | Primary |
| `WITH … (mutating CTE) … SELECT` | Primary |
| `EXPLAIN …` | Primary |
| Any statement that fails to parse | Primary (safe fallback) |

### Comment overrides

Prepend a line comment or block comment with `rw: read` / `rw: write` (or the long form `rw_mode: read` / `rw_mode: write`) to override routing for a specific query:

```sql
-- rw: write
SELECT * FROM users WHERE id = $1
```

```sql
-- rw: read
INSERT INTO audit_log (event) VALUES ($1) RETURNING id
```

```sql
/* rw_mode: write */
SELECT pg_advisory_lock(1)
```

The keyword match is **case-insensitive**.

## sqlc Integration

If you use [sqlc](https://github.com/sqlc-dev/sqlc) for query code generation, consider [**sqlc-pgx-route**](https://github.com/amirsalarsafaei/sqlc-pgx-route) — a drop-in replacement for `sqlc-gen-go` that determines read/write routing **at code generation time** rather than at runtime.

Instead of parsing SQL on every query execution, `sqlc-pgx-route` uses the PostgreSQL parser once during code generation to classify each query and emits a `PoolRouteQueries` wrapper that calls the correct pool directly. This gives you:

- **Zero per-query routing overhead** — no runtime SQL parsing.
- **Auditable routing** — because the pool assignment is part of the generated code, code reviews and pull request diffs make it immediately visible which pool each query will use, before any code is merged.

See the [sqlc-pgx-route repository](https://github.com/amirsalarsafaei/sqlc-pgx-route) for installation and configuration instructions.

## License

[MIT](LICENSE)
