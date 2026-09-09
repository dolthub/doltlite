# Language bindings

Every binding wraps the same `libdoltlite`: the bundled SQLite's `sqlite3_*`
API plus the `dolt_*` functions and tables, subject to the
[storage-engine exceptions](sqlite-compatibility.md). Each section says what
the package bundles, which platforms it covers, and what tends to go wrong.

| Language | Install | Bundled engine | Source |
|---|---|---|---|
| Python | `pip install doltlite` | Linux x86_64/arm64, macOS arm64 | [doltlite-python](https://github.com/dolthub/doltlite-python) |
| Ruby | `gem install doltlite` | Linux x86_64/arm64, macOS arm64/x86_64 | [doltlite-ruby](https://github.com/dolthub/doltlite-ruby) |
| Node.js / Bun | `npm install @dolthub/doltlite` | Linux x64/arm64, macOS x64/arm64, Windows x64 | [doltlite-node](https://github.com/dolthub/doltlite-node) |
| Browser / WASM | `npm install @dolthub/doltlite-wasm` | any wasm runtime | [`packaging/npm`](../../packaging/npm) |
| PHP | `composer require dolthub/doltlite-php` | Linux x86_64/arm64, macOS arm64 | [`packaging/composer`](../../packaging/composer) |
| .NET | `dotnet add package DoltHub.Doltlite` | linux-x64, linux-arm64, osx-arm64, win-x64 | [`packaging/nuget`](../../packaging/nuget) |
| Rust | `cargo add doltlite` | compiled from source; Linux, macOS | [`packaging/rust`](../../packaging/rust) |
| Go | `go get github.com/dolthub/doltlite-driver` | compiled from source; Linux, macOS | [`packaging/go`](../../packaging/go) |
| Swift | SwiftPM `https://github.com/dolthub/doltlite-swift` | iOS 14+, macOS 11+, Catalyst 14+ | [doltlite-swift](https://github.com/dolthub/doltlite-swift) |
| Android | Gradle `com.dolthub:doltlite-android` | arm64-v8a, armeabi-v7a, x86_64, x86 | [doltlite-android](https://github.com/dolthub/doltlite-android) |

Any language with a C FFI can load `libdoltlite` directly; see
[using-existing-sqlite-bindings.md](using-existing-sqlite-bindings.md) for
pointing an existing SQLite binding at it.

## Python

Rides on the stdlib `sqlite3` module by loading `libdoltlite` in place of
the system SQLite before `_sqlite3` initializes. That only works when the
interpreter links SQLite as a shared library: distro or system Python,
Homebrew, pyenv, and conda do. python-build-standalone interpreters, the
default for `uv python install`, mise, and Rye, statically link SQLite and
fail with `DoltliteLoadError`; point `uv venv --python` at a supported
interpreter instead. On macOS the bootstrap re-executes the interpreter with
an `install_name_tool` shim, so Xcode Command Line Tools are needed. Intel
Mac wheels are not shipped; build locally and set `DOLTLITE_LIB`.

## Ruby

Precompiled gems bundle the engine; the plain gem falls back to a
system-installed `libdoltlite`. `db.commit` in the README examples is a Dolt
commit, not a SQL `COMMIT`.

## Node.js and Bun

A drop-in for `node:sqlite` (`DatabaseSync`, `StatementSync`) with `dolt_*`
methods added, plus `binPath()` for the bundled `doltlite` CLI. Prebuilt
binaries cover Linux, macOS, and Windows x64; elsewhere it builds from source
with `node-gyp`, which needs Python 3 and a C++ toolchain. For the browser use
the wasm package below.

## Browser and WASM

Upstream SQLite's wasm build on the DoltLite engine, same JavaScript API.
Persistent storage needs OPFS and cross-origin isolation headers; remotes work
over HTTP through the host's XHR or worker thread. Details: [wasm.md](wasm.md).

## PHP

A `Doltlite3` class shaped like `SQLite3`, over PHP's `ffi` extension (PHP 8.1+,
`ffi.enable` permitting). Differences from `SQLite3`: errors always throw
`Doltlite\Exception`; `openBlob`, `createFunction`, `createAggregate`,
`createCollation`, and `setAuthorizer` are not implemented.

Windows is not supported. Composer will install the package there because
its metadata has no platform restriction, but releases do not reliably
include `libdoltlite.dll` and the package is not tested on Windows, so the
first `new Doltlite3(...)` fails inside `FFI::cdef()` with a loader error.
`DOLTLITE_PHP_LIB=/path/to/libdoltlite.<ext>` overrides library resolution
on any platform; on Windows that is an untested escape hatch, not support.

## .NET

Works under `Microsoft.Data.Sqlite`, EF Core, and Dapper through
SQLitePCLRaw: call `Doltlite.Init()` once before the first connection, then
version control is plain SQL on the same connection. Bundles linux-x64,
linux-arm64, osx-arm64, and win-x64; set `DOLTLITE_NET_LIB` to a library you
built for anything else. .NET 6.0 or later.

## Rust

The crate vendors the amalgamation and compiles it in `build.rs`, so the
first build takes a few minutes and needs a C compiler and zlib. Linux and
macOS are covered by CI; Windows is not, because the build links the
platform's zlib, which MSVC toolchains lack. Remotes, TLS included, are
compiled in. To use `rusqlite` instead, point `libsqlite3-sys` at a full
DoltLite build (`make`, not `make doltlite-lib`) with `SQLITE3_LIB_DIR` and
`SQLITE3_INCLUDE_DIR`.

## Go

A `database/sql` driver with the engine vendored via cgo; needs a C compiler
and zlib, Linux and macOS only for the same zlib reason as Rust. Safe across
goroutines through `*sql.DB`; transactions begin as `BEGIN IMMEDIATE` so lock
contention surfaces at `Begin`, with a five-second default wait. This is
`doltlite-driver`; `dolthub/doltlite-go` is the pure-Go sync protocol library,
and `dolthub/driver` embeds Dolt itself if you want MySQL dialect without cgo.

## Swift

An XCFramework for iOS 14+, macOS 11+, and Mac Catalyst 14+, exposing the full
C API. Dolt cannot run on iOS; this is the way to ship versioned SQL there.

## Android

Android's framework SQLite cannot be swapped, so the AAR vendors one
`libdoltlite.so` per ABI (arm64-v8a, armeabi-v7a, x86_64, x86) and reaches
it through JNA.

## See also

[embedding.md](embedding.md) for the C API, [cli.md](cli.md) for `db@branch`
open syntax that every binding accepts as the database path.
