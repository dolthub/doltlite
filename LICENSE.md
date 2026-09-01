License Information
===================

Doltlite Extensions — Apache License 2.0
-----------------------------------------

The Doltlite extensions to SQLite — including the prolly tree storage
engine, chunk store, version control functions (dolt_commit, dolt_merge,
dolt_diff, etc.), and all related code in the following files — are
licensed under the Apache License, Version 2.0:

  * `src/prolly_*.c` and `src/prolly_*.h` — Prolly tree implementation
  * `src/chunk_store.c` and `src/chunk_store.h` — Content-addressed chunk store
  * `src/pager_shim.c` — Pager shim for SQLite integration
  * `src/doltlite*.c` and `src/doltlite*.h` — Version control functions
  * `.github/` — CI/CD workflows

You may obtain a copy of the Apache License at:
https://www.apache.org/licenses/LICENSE-2.0

Copyright 2024-2026 DoltHub, Inc.

SQLite Is Public Domain
-----------------------

The SQLite source code, including all of the files in the directories
listed in the bullets below are 
[Public Domain](https://sqlite.org/copyright.html).
The authors have submitted written affidavits releasing their work to
the public for any use.  Every byte of the public-domain code can be
traced back to the original authors.  The files of this repository
that are public domain include the following:

  *  All of the primary SQLite source code files found in the
     [src/ directory](https://sqlite.org/src/tree/src?type=tree&expand)
  *  All of the test cases and testing code in the
     [test/ directory](https://sqlite.org/src/tree/test?type=tree&expand)
  *  All of the SQLite extension source code and test cases in the
     [ext/ directory](https://sqlite.org/src/tree/ext?type=tree&expand)
  *  All code that ends up in the "sqlite3.c" and "sqlite3.h" build products
     that actually implement the SQLite RDBMS.
  *  All of the code used to compile the
     [command-line interface](https://sqlite.org/cli.html)
  *  All of the code used to build various utility programs such as
     "sqldiff", "sqlite3_rsync", and "sqlite3_analyzer".


The public domain source files usually contain a header comment
similar to the following to make it clear that the software is
public domain.

> The author disclaims copyright to this source code.  In place of
> a legal notice, here is a blessing:
> 
>   *   May you do good and not evil.
>   *   May you find forgiveness for yourself and forgive others.
>   *   May you share freely, never taking more than you give.

SQLite's own sources in this repository are public domain. DoltLite
code and the vendored libraries below are not. Other exceptions:

Non-Public-Domain Code Included With This Source Repository AS A Convenience
----------------------------------------------------------------------------

This repository contains a (relatively) small amount of non-public-domain
code used to help implement the configuration and build logic.  In other
words, there are some non-public-domain files used to implement:

> ./configure && make

SQLite's configure/build scripts (autosetup, legacy autoconf) have
BSD-style licenses and do not reach the DoltLite build products. They
are included as a convenience so a source checkout can `./configure &&
make` without fetching third-party build tools. Other non-public-domain
code in this tree — DoltLite itself (Apache-2.0) and the vendored
libraries below — **does** reach `libdoltlite` and the CLI.

Non-public-domain code included in this respository includes:

  *  The ["autosetup"](http://msteveb.github.io/autosetup/) configuration
     system that is contained (mostly) in the autosetup/ directory, but also
     includes the "./configure" script at the top-level of this archive.
     Autosetup has a separate BSD-style license.  See the
     [autosetup/LICENSE](http://msteveb.github.io/autosetup/license/)
     for details.

  *  There are BSD-style licenses on some of the configuration
     software found in the legacy autoconf/ directory and its
     subdirectories.

  *  The vendored BLAKE3 reference implementation under `ext/blake3/`,
     used by the prolly tree's content-addressing layer. Upstream is
     dual-licensed under Apache License 2.0 (with LLVM exception) or
     CC0 1.0 Universal — DoltLite redistributes under Apache 2.0
     (the project-wide license). Source: BLAKE3 v1.8.5 from
     https://github.com/BLAKE3-team/BLAKE3. See `ext/blake3/LICENSE`
     and `ext/blake3/README.md` for the full license texts and a
     description of DoltLite-specific modifications.

  *  Vendored [Mbed TLS](https://www.trustedfirmware.org/projects/mbed-tls/)
     under `ext/mbedtls/`, used for HTTPS remotes and `doltlite-remotesrv`
     TLS. Dual-licensed Apache-2.0 OR GPL-2.0-or-later; see
     `ext/mbedtls/LICENSE`. This code is linked into builds that include
     remotes.

  *  Vendored Ed25519 (Orson Peters) under `ext/ed25519/`, used for JWT
     signing and verification. zlib-style license; see
     `ext/ed25519/LICENSE`. This code is linked into credential and
     remotesrv builds.

Autosetup and the legacy autoconf scripts do not reach `libdoltlite` or
the CLI. BLAKE3, Mbed TLS, and Ed25519 do: they are in the DoltLite
build products, not "build scripts only."

The following unix shell command removes the configure/build-script
exceptions:

> rm -rf configure autosetup autoconf

That does **not** leave a 100% public-domain tree. What remains is
public-domain SQLite, Apache-2.0 DoltLite code, and the vendored
libraries listed above.
