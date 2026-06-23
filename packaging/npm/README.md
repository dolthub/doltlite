# @dolthub/doltlite-wasm

[DoltLite](https://github.com/dolthub/doltlite) compiled to WebAssembly: a
drop-in SQLite build with Dolt-style version control — `dolt_commit`,
`dolt_branch`, `dolt_merge`, `dolt_diff`, and the rest — that runs in the
browser and in any JS runtime.

This is the WebAssembly distribution. For a native Node/Bun addon, use
[`@dolthub/doltlite`](https://github.com/dolthub/doltlite-node) instead.

## Install

```bash
npm install @dolthub/doltlite-wasm
```

## Browser (OPFS-backed, persistent)

OPFS persistence requires the page be served cross-origin isolated (the
`COOP`/`COEP` headers below); without them you still get an in-memory database.

```js
import { sqlite3InitModule } from '@dolthub/doltlite-wasm';

const sqlite3 = await sqlite3InitModule();

// Persistent across reloads when OPFS is available, in-memory otherwise.
const db = ('opfs' in sqlite3)
  ? new sqlite3.oo1.OpfsDb('/app.db')
  : new sqlite3.oo1.DB(':memory:');

db.exec(`CREATE TABLE IF NOT EXISTS t(id INTEGER PRIMARY KEY, msg TEXT)`);
db.exec(`INSERT INTO t(msg) VALUES('hello')`);
db.exec(`SELECT dolt_commit('-A', '-m', 'first commit')`);

for (const row of db.selectObjects(`SELECT * FROM dolt_log`)) {
  console.log(row.commit_hash, row.message);
}
```

Required headers for OPFS (cross-origin isolation):

```
Cross-Origin-Opener-Policy: same-origin
Cross-Origin-Embedder-Policy: require-corp
```

## Node / Bun

```js
import { sqlite3InitModule } from '@dolthub/doltlite-wasm';

const sqlite3 = await sqlite3InitModule();
const db = new sqlite3.oo1.DB('/tmp/app.db');
db.exec(`CREATE TABLE t(x)`);
db.exec(`SELECT dolt_commit('-A', '-m', 'init')`);
```

## Entry points

| Import | Use |
| --- | --- |
| `@dolthub/doltlite-wasm` | Default; bundler-friendly ESM init |
| `@dolthub/doltlite-wasm/sqlite3.mjs` | Plain ESM (no bundler) |
| `@dolthub/doltlite-wasm/sqlite3-node.mjs` | Node-specific entry |
| `@dolthub/doltlite-wasm/sqlite3-worker1-promiser.mjs` | Worker promiser API |

The JavaScript API is the upstream SQLite WASM API — see the
[sqlite3-wasm docs](https://sqlite.org/wasm). Everything there works; DoltLite
adds the `dolt_*` version-control functions and virtual tables on top.

## License

Apache-2.0. See [LICENSE.md](./LICENSE.md). SQLite itself is public domain.
