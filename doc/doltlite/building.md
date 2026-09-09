# Building

## macOS / Linux

```
cd build
../configure
make
./doltlite :memory:
```

## Windows (MSYS2 / MINGW64)

```
pacman -S mingw-w64-x86_64-gcc mingw-w64-x86_64-zlib make tcl
mkdir -p build && cd build
../configure
make doltlite.exe
./doltlite.exe :memory:
```

To verify the engine:

```sql
SELECT doltlite_engine();
-- prolly
```

To build stock SQLite instead (for comparison):

```
cd ..
bash test/build_stock_reference.sh build-stock build/doltlite
./build-stock/sqlite3
```

Vec1 is built into native DoltLite by default. Use `make DOLTLITE_VEC1=0` to
omit it. Compile the DoltLite amalgamation with `-DDOLTLITE_VEC1=1` to include
vec1; otherwise it can be built and loaded as an extension.

## WebAssembly (`ext/wasm`)

Vendored SQLite `ext/wasm`, defaulting to the DoltLite engine. Build generated
SQLite sources first, then wasm:

```bash
./configure
make sqlite3.c sqlite3.h sqlite3ext.h
make -C ext/wasm
# → ext/wasm/jswasm/{sqlite3.js,sqlite3.mjs,sqlite3.wasm}
make -C ext/wasm DOLTLITE_WASM=0   # upstream SQLite wasm instead
make -C ext/wasm DOLTLITE_ENABLE_REMOTES=0 # DoltLite without remote clients
make -C ext/wasm dist             # zip package
```

`DOLTLITE_ENABLE_REMOTES=0` omits clone, fetch, pull, push, HTTP, TLS, and
credential code. Calls to the remote SQL functions then return `DoltLite
remotes are disabled in this build`. Remotes use browser XHR/CORS or Node
worker/fetch. See [wasm.md](wasm.md).

`DOLTLITE_ENABLE_CHUNK_SOURCE=0` omits host-provided and origin-backed lazy
chunk fetching. The feature is enabled by default.
