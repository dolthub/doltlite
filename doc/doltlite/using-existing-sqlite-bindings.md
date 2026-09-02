# Pointing an existing SQLite binding at DoltLite

DoltLite exports the SQLite C API under SQLite's own symbol names, so a
program that already talks to SQLite can be pointed at DoltLite instead of
being rewritten. Your queries keep working, and the `dolt_*` functions and
version-control tables become available through whatever query API you
already use.

This matters because the layer people assume is the engine usually is not.
`Microsoft.Data.Sqlite`, `rusqlite`, `sqlite3` for Dart, and PHP's `pdo_sqlite`
are all *bindings* that call a native SQLite library, and most of them have a
documented way to be handed a different one. Where that hook exists, adopting
DoltLite is a configuration change rather than a port.

If your language has a
[packaged binding](../../README.md#bindings), use it — it bundles the engine and
needs none of this. This document is for everything else.

## What you need

A DoltLite shared library, from a
[release](https://github.com/dolthub/doltlite/releases) (`doltlite-lib-*.zip`,
or the `.deb` packages) or built from source. Bindings that require the
library to be named `libsqlite3` need the full source build:

```sh
cd build && ../configure && make
# → libdoltlite.{so,dylib}, libdoltlite.a, and SQLite-named copies
#   (libsqlite3.*) of the same engine
```

The full `make` target creates the SQLite-named artifacts so bindings that
hardcode `-lsqlite3` can link them without knowing the difference.
`make doltlite-lib` creates only the DoltLite-named libraries, and release
packages omit the aliases to avoid colliding with system SQLite.

## Confirming you got DoltLite

Whatever the mechanism, verify it took effect rather than assuming:

```sql
SELECT doltlite_engine();   -- prolly
```

Stock SQLite fails that query with "no such function", which is the signal
you are still on the old engine. `SELECT sqlite_version()` is *not* a useful
check — DoltLite reports the SQLite version it is derived from.

## Rust (rusqlite)

`libsqlite3-sys` links an external SQLite when told where it is:

```sh
SQLITE3_LIB_DIR=/path/to/doltlite/build \
SQLITE3_INCLUDE_DIR=/path/to/doltlite/build \
cargo build
```

Do not enable its `bundled` feature — that compiles its own copy of SQLite and
ignores yours. There is also a [`doltlite` crate](../../packaging/rust) that
vendors the engine if you would rather not manage the library.

## Go (mattn/go-sqlite3)

The `libsqlite3` build tag links a system library through `pkg-config` instead
of compiling the bundled amalgamation:

```sh
sudo make install        # the shipped sqlite3.pc resolves against the
                         # install prefix, not the build directory
go build -tags libsqlite3 ./...
```

Note that Go already has a better option for most cases: Dolt itself embeds
in-process via [`github.com/dolthub/driver`](https://github.com/dolthub/driver),
in pure Go with no cgo and the full Dolt feature set. Reach for DoltLite when
you specifically need SQLite dialect compatibility for an existing SQLite
application, or a small C library rather than Dolt's dependency tree.

## Dart / Flutter (`sqlite3` package)

The package takes an explicit override:

```dart
import 'dart:ffi';
import 'package:sqlite3/open.dart';
import 'package:sqlite3/sqlite3.dart';

void main() {
  open.overrideFor(
    OperatingSystem.linux,
    () => DynamicLibrary.open('libdoltlite.so'),
  );
  final db = sqlite3.open('app.db');
  db.select("SELECT dolt_commit('-A', '-m', 'checkpoint')");
}
```

Register the override before the first `sqlite3` use, once per platform you
ship. For Android and iOS specifically, prefer the packaged
[Android](https://github.com/dolthub/doltlite-android) and
[Swift](https://github.com/dolthub/doltlite-swift) bindings, which already
carry the native library.

## .NET

Covered by the [`DoltHub.Doltlite`](../../packaging/nuget) package: it plugs the
engine into SQLitePCLRaw, under `Microsoft.Data.Sqlite.Core`, EF Core, and
Dapper. Use `Microsoft.Data.Sqlite.Core` rather than `Microsoft.Data.Sqlite`,
which pins its own bundled engine.

## Python, PHP, Node, Ruby

All have packaged bindings — see the
[bindings table](../../README.md#bindings). Python additionally works with the
stdlib `sqlite3` module when the interpreter links a shared `libsqlite3`; the
[`doltlite`](https://github.com/dolthub/doltlite-python) package handles that
for you.

## Any language with a C FFI

If your binding has no override hook, two general mechanisms remain.

**Load the library directly.** Anything that can `dlopen` and call C functions
can drive DoltLite: open `libdoltlite`, then call `sqlite3_open_v2`,
`sqlite3_prepare_v2`, `sqlite3_step`, and friends. That is exactly how the PHP
binding works.

**Substitute it at load time.** Where a binding hardcodes a library name, the
dynamic linker can be told to resolve those symbols from DoltLite first:

```sh
LD_PRELOAD=/path/to/libdoltlite.so ./your-program              # Linux
DYLD_INSERT_LIBRARIES=/path/to/libdoltlite.dylib ./your-program # macOS
```

Treat this as a last resort and a development aid rather than a deployment
strategy: it applies process-wide, so every component that uses SQLite gets
DoltLite, and on macOS it additionally requires the library's install name to
match what the program looked for. It is genuinely useful for answering "would
this work?" in a few seconds before committing to a real integration.

## What changes, and what does not

The SQL surface, the C API, and your queries stay the same, with the
[storage-engine exceptions](../../README.md#sqlite-compatibility) — those are
about page-level and journaling APIs, not about SQL. What you gain is that
every write can be committed, branched, diffed, and merged.

Two practical notes. `PRAGMA journal_mode` is inert: the chunk store has its
own durability mechanism, so on a file database the pragma always reports
`wal` and setting it changes nothing. And a DoltLite database is not a SQLite
file — existing `.db` files must be migrated by dumping and reloading, not
opened in place.
