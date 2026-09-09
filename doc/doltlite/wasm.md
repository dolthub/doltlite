# WebAssembly

`@dolthub/doltlite-wasm` is SQLite's own wasm build (`ext/wasm`) compiled
against the DoltLite engine. The JavaScript API is unchanged from
[sqlite.org/wasm](https://sqlite.org/wasm); the `dolt_*` functions and
tables are simply there. Install and first steps are in the package README
at [`packaging/npm`](../../packaging/npm/README.md). For a native Node addon
use `@dolthub/doltlite` instead.

## Persistence

| Runtime | Storage |
|---|---|
| Browser, cross-origin isolated | OPFS via `sqlite3.oo1.OpfsDb`. Needs `Cross-Origin-Opener-Policy: same-origin` and `Cross-Origin-Embedder-Policy: require-corp`. |
| Browser, not isolated | In-memory only |
| Node / Bun | Ordinary files through the wasm filesystem |

A database is one file in either case, in the same format native DoltLite
reads and writes. `sqlite3_serialize()` / `deserialize()` move a whole
database, history included, in and out as bytes.

## Remotes

`dolt_clone`, `dolt_fetch`, `dolt_pull`, and `dolt_push` work over HTTP and
HTTPS. Emscripten has no sockets, so the transport is host-provided:

- **Browser**: a synchronous `XMLHttpRequest` per request. The remote must
  allow the page's origin (CORS), and the call blocks the thread it runs on,
  so run it in a worker.
- **Node**: a worker thread that performs the fetch while the main thread
  waits on `Atomics`.

Filesystem remotes (`file://`) work only where the wasm filesystem can reach
the path. Credentials use the same functions as native and are stored under
`$HOME/.doltlite/creds` in the wasm filesystem, which a browser does not
persist unless you arrange it. See [auth.md](auth.md).

## Build flags

```
make -C ext/wasm                              # DoltLite engine (default)
make -C ext/wasm DOLTLITE_WASM=0              # upstream SQLite wasm
make -C ext/wasm DOLTLITE_ENABLE_REMOTES=0    # no clone/fetch/pull/push, HTTP, TLS, creds
make -C ext/wasm DOLTLITE_ENABLE_CHUNK_SOURCE=0
```

With remotes disabled, the remote functions return `DoltLite remotes are
disabled in this build`. `DOLTLITE_ENABLE_CHUNK_SOURCE` controls the
host-provided lazy chunk source (`doltlite_set_chunk_source`,
`doltlite_init_lazy` in `doltlite.h`), a C API for embedders that supply
chunks themselves. The published npm package has everything enabled.

Full build setup, including the Emscripten SDK, is in
[building.md](building.md) and the upstream notes in
[`ext/wasm/README.md`](../../ext/wasm/README.md).
