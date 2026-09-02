# doltlite-driver

[DoltLite](https://github.com/dolthub/doltlite) for Go: SQLite with Git-style
version control — branch, commit, merge, and diff your relational data,
exposed as a `database/sql` driver. The engine is compiled into the package
from a vendored amalgamation, so there is no system library to install.

## Install

```sh
go get github.com/dolthub/doltlite-driver
```

Requires cgo (`CGO_ENABLED=1`, the default when a C compiler is present) and
zlib. The first build compiles the engine and is then cached.

## Use

```go
import (
    "database/sql"
    _ "github.com/dolthub/doltlite-driver"
)

db, err := sql.Open("doltlite", "app.db")
if err != nil { return err }
defer db.Close()

if _, err := db.Exec(`CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)`); err != nil {
    return err
}
if _, err := db.Exec(`INSERT INTO users(name) VALUES (?)`, "Ada"); err != nil {
    return err
}

// Version control is plain SQL on the same connection.
var commit string
if err := db.QueryRow(`SELECT dolt_commit('-A', '-m', ?)`, "add ada").Scan(&commit); err != nil {
    return err
}

rows, err := db.Query(`SELECT commit_hash, message FROM dolt_log`)
```

Every DoltLite feature — branches, merges, `dolt_log`/`dolt_diff`/`dolt_branches`,
remotes — is reachable through SQL; there is no separate API. See the
[DoltLite documentation](https://github.com/dolthub/doltlite).

## Concurrency

Safe to use from multiple goroutines through `*sql.DB`, including its
connection pool.

Writers to one database still serialize, as in SQLite. A connection waits up
to five seconds for a contended write before returning "database is locked";
change that per connection with `PRAGMA busy_timeout = <ms>`. Transactions
begin as `BEGIN IMMEDIATE`, so contention surfaces at `Begin` where it can be
waited on, rather than partway through a transaction where it cannot.

## Why not embedded Dolt?

If you want versioned SQL in a new Go program, prefer
[`github.com/dolthub/driver`](https://github.com/dolthub/driver): it embeds
Dolt itself in pure Go, needs no cgo, and gives you the full Dolt feature set
with MySQL dialect.

Use this package when you specifically need **DoltLite databases** — a
single-file chunk store that embedded Dolt cannot open — or SQLite dialect
compatibility for an existing SQLite application, or a small C library rather
than Dolt's dependency tree.

## Platforms

Linux and macOS. Windows is not supported yet: the build links the platform's
zlib, which the MSVC toolchain does not provide.

## Source

The package source lives in
[dolthub/doltlite](https://github.com/dolthub/doltlite) under `packaging/go/`;
send issues and pull requests there.
