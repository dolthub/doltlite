# Embedding DoltLite in C

Public C API is the bundled SQLite declarations under `sqlite3_*` names plus
the DoltLite-specific declarations in `doltlite.h`. Port supported programs by
switching the include/link to `libdoltlite`; APIs tied to SQLite's pager, page
format, or journaling differ (see [SQLite Compatibility](sqlite-compatibility.md)).
Dolt features are SQL functions (`dolt_commit`, `dolt_branch`, …) and virtual
tables (`dolt_log`, `dolt_diff_<table>`, …).

`doltlite_set_chunk_source()` registers synchronous `xGet` and `xGetMany`
callbacks for one attached database. `doltlite_init_lazy()` installs a refs
blob into a fresh or existing main database, allowing missing graph chunks to
be fetched and cached on demand. Source objects remain owned by the host and
must outlive their registrations.

Loadable extensions use `doltliteext.h` (rebranded `sqlite3ext.h`, shipped in
the amalgamation zip). The shared library exports only `sqlite3_*`,
`doltliteServe*` (`doltlite_remotesrv.h`), `doltlite_set_chunk_source`, and
`doltlite_init_lazy`; prolly/chunk-store internals, other `doltlite*` symbols,
and vendored crypto are hidden. The static archive is unfiltered for tests and
tooling.

```bash
cd build
../configure
make doltlite-lib   # libdoltlite.a and libdoltlite.dylib/.so

# Static (recommended) or dynamic
gcc -o myapp myapp.c -I/path/to/build libdoltlite.a -lpthread -lz -lm
gcc -o myapp myapp.c -I/path/to/build -L/path/to/build -ldoltlite -lpthread -lz -lm

sudo make install   # honours --prefix / DESTDIR; then:
gcc -o myapp myapp.c -ldoltlite -lpthread -lz -lm
```

`make install` also installs SQLite-named artifacts (`sqlite3.h`,
`libsqlite3.*`, …) from this tree — release packages omit those so they do not
collide with system SQLite. Use a private `--prefix` if that matters.

## Quickstart Examples

Same flow (commits, branches, merges, diffs, tags) in each language.

**C** ([`examples/quickstart.c`](../../examples/quickstart.c)) — based on the
[SQLite quickstart](https://sqlite.org/quickstart.html):

```bash
cd build
gcc -o quickstart ../examples/quickstart.c -I. libdoltlite.a -lpthread -lz -lm
./quickstart
```

**Python** ([`examples/quickstart.py`](../../examples/quickstart.py)) — stdlib
`sqlite3` with the [`doltlite`](https://github.com/dolthub/doltlite-python)
package (bundles libdoltlite):

```bash
pip install doltlite
python3 examples/quickstart.py
```

Needs a Python whose `_sqlite3` links a shared `libsqlite3` (distro,
Homebrew, pyenv, or conda). Avoid python-build-standalone (`uv python install`
defaults), the python.org macOS installer, and Apple system Python — they
static-link SQLite and cannot preload libdoltlite. Local-build preload
recipes (including macOS) are in the
[doltlite-python](https://github.com/dolthub/doltlite-python) README.

**Go** ([`examples/go/main.go`](../../examples/go/main.go)) — uses
[mattn/go-sqlite3](https://github.com/mattn/go-sqlite3) with the `libsqlite3`
build tag:

```bash
cd examples/go
CGO_CFLAGS="-I../../build" CGO_LDFLAGS="../../build/libdoltlite.a -lz -lpthread -lm" \
    go build -tags libsqlite3 -o quickstart .
./quickstart
```
