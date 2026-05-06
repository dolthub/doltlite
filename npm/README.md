# doltlite

Node.js native binding for [DoltLite](https://github.com/dolthub/doltlite) — SQLite with Dolt-style version control.

API mirrors a subset of [`better-sqlite3`](https://github.com/WiseLibs/better-sqlite3): synchronous, prepared-statement-driven, no callbacks.

## Status

Pre-`1.0`. Released alongside the parent DoltLite repo on tag pushes; each tag triggers prebuilds for all supported platforms and an `npm publish`.

## Install

```bash
npm install @dolthub/doltlite
```

Pre-built binaries ship for:

- `linux-x64`
- `linux-arm64`
- `darwin-arm64`  (Apple Silicon)
- `win32-x64`     (built with MSYS2/MinGW; loads in stock Node for Windows)

If you're on one of those, no toolchain is needed.

For other platforms (e.g. `darwin-x64`, `linux-musl`, BSD), the package falls back to building from source via `node-gyp`. That requires a checkout of the DoltLite source tree at `../build` (or path set via `DOLTLITE_BUILD`), with `libdoltlite.a` already built — `make doltlite-lib` from a configured doltlite build dir.

## Usage

```javascript
const Database = require('@dolthub/doltlite');

const db = new Database('app.db');

db.exec(`CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)`);
db.prepare(`INSERT INTO users(name) VALUES (?)`).run('alice');

// Standard SQLite queries.
const rows = db.prepare(`SELECT * FROM users`).all();
console.log(rows);  // [{ id: 1, name: 'alice' }]

// DoltLite version-control procedures, exposed via the same surface.
db.exec(`SELECT dolt_commit('-A', '-m', 'initial users')`);
db.exec(`SELECT dolt_branch('feat')`);
db.exec(`SELECT dolt_checkout('feat')`);

db.prepare(`INSERT INTO users(name) VALUES (?)`).run('bob');
db.exec(`SELECT dolt_commit('-A', '-m', 'add bob')`);

db.exec(`SELECT dolt_checkout('main')`);
db.exec(`SELECT dolt_merge('feat')`);

// Inspect the log.
const log = db.prepare(`SELECT message FROM dolt_log ORDER BY date DESC`).all();
```

## API

### `new Database(path)`

Opens or creates a database at the given filesystem path. Use `':memory:'` for an in-memory database.

### `db.prepare(sql)` → `Statement`

Compile a SQL statement. The returned `Statement` has `.run()`, `.get()`, `.all()`, and `.finalize()`.

### `db.exec(sql)` → `db`

Execute one or more SQL statements without returning results. Useful for DDL and `SELECT dolt_commit(...)`-style fire-and-forget calls.

### `db.close()` → `db`

Close the underlying handle.

### `stmt.run(...params)` → `{ changes, lastInsertRowid }`

Execute a write statement with positional `?` bindings.

### `stmt.get(...params)` → `Object | undefined`

Execute a read statement and return the first row as a plain object, or `undefined` if no rows match.

### `stmt.all(...params)` → `Array<Object>`

Execute a read statement and return all rows as an array of plain objects.

## License

Apache-2.0. See [LICENSE](https://github.com/dolthub/doltlite/blob/master/LICENSE) in the parent repo.
