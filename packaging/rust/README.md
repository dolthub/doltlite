# doltlite

[DoltLite](https://github.com/dolthub/doltlite) for Rust: SQLite with
Git-style version control — branch, commit, merge, and diff your relational
data. The engine is compiled into the crate from a vendored amalgamation, so
there is no system library to install, link, or point at.

## Install

```sh
cargo add doltlite
```

Building the crate compiles the engine, which takes a few minutes the first
time and is then cached like any other dependency. It needs a C compiler and
zlib.

Linux and macOS are supported and covered by CI. Windows is not yet: the
build links the platform's zlib, which an MSVC toolchain does not provide.

## Use

```rust
use doltlite::{Connection, Value};

let db = Connection::open("app.db")?;
db.execute_batch("CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)")?;
db.execute("INSERT INTO users(name) VALUES (?)", &[Value::from("Ada")])?;

// Version control is plain SQL on the same connection.
db.query_row("SELECT dolt_commit('-A', '-m', 'add ada')", &[])?;
db.query_row("SELECT dolt_checkout('-b', 'experiment')", &[])?;
db.execute("UPDATE users SET name = ? WHERE id = 1", &[Value::from("Grace")])?;
db.query_row("SELECT dolt_commit('-A', '-m', 'rename')", &[])?;
db.query_row("SELECT dolt_checkout('main')", &[])?;
db.query_row("SELECT dolt_merge('experiment')", &[])?;

for row in db.query("SELECT commit_hash, message FROM dolt_log", &[])? {
    println!("{:?} {:?}", row.get(0), row.get(1));
}
# Ok::<(), doltlite::Error>(())
```

Every DoltLite feature — branches, merges, diffs (`dolt_log`, `dolt_diff`,
`dolt_branches`), tags — is reachable through SQL; there is no separate API to
learn. See the [DoltLite documentation](https://github.com/dolthub/doltlite).

## Using rusqlite instead

If you would rather use [`rusqlite`](https://crates.io/crates/rusqlite) and its
ecosystem, you do not need this crate: point `libsqlite3-sys` at a DoltLite
build, which installs SQLite-named artifacts for exactly this purpose.

```sh
SQLITE3_LIB_DIR=/path/to/doltlite/build \
SQLITE3_INCLUDE_DIR=/path/to/doltlite/build \
cargo build
```

`rusqlite` then runs on DoltLite unchanged, and the `dolt_*` functions are
available through its normal query API. This crate exists for the case where
vendoring the engine and skipping the system-library setup is preferable.

## Remotes

Remote support is compiled in: `dolt_clone`, `dolt_fetch`, `dolt_push`, and
`dolt_pull` work against both `file://` and `https://` remotes, TLS included.

## Source

The crate source lives in
[dolthub/doltlite](https://github.com/dolthub/doltlite) under
`packaging/rust/`; send issues and pull requests there.
