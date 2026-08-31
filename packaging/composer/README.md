# dolthub/doltlite-php

> The installable package is distributed through
> [dolthub/doltlite-php](https://github.com/dolthub/doltlite-php), whose
> contents — including the prebuilt `lib/` binaries — are pushed by
> [dolthub/doltlite](https://github.com/dolthub/doltlite)'s release workflow
> on every tagged release. The package source lives in that repository under
> `packaging/composer/`; send issues and pull requests there.

[DoltLite](https://github.com/dolthub/doltlite) for PHP: SQLite with Git-style
version control — branch, commit, merge, and diff your relational data. The
binding runs over PHP's FFI extension against the `libdoltlite` shared library
bundled with this package, and mirrors the shape of PHP's built-in `SQLite3`
classes so existing code ports by search-and-replace.

## Requirements

- PHP >= 8.1 with the `ffi` extension enabled (`php -m | grep FFI`). On
  hardened hosts check `ffi.enable`; the CLI default allows FFI.
- Linux (x86_64/arm64) or macOS (x86_64/arm64). The package bundles a
  prebuilt `libdoltlite` per platform; `DOLTLITE_PHP_LIB=/path/to/lib`
  overrides resolution for anything else.

## Install

```sh
composer require dolthub/doltlite-php
```

## Use

The API is `SQLite3` with the prefix swapped: `Doltlite3`, `Doltlite3Stmt`,
`Doltlite3Result`, and `DOLTLITE3_*` constants. Errors always raise
`Doltlite\Exception`.

```php
use Doltlite\Doltlite3;

$db = new Doltlite3('app.db');
$db->exec('CREATE TABLE users(id INTEGER PRIMARY KEY, name TEXT)');

$stmt = $db->prepare('INSERT INTO users(name) VALUES (:name)');
$stmt->bindValue(':name', 'Ada');
$stmt->execute();

// Version control is plain SQL on the same connection.
$db->querySingle("SELECT dolt_commit('-A', '-m', 'add ada')");
$db->querySingle("SELECT dolt_checkout('-b', 'experiment')");
$db->exec("UPDATE users SET name = 'Grace' WHERE id = 1");
$db->querySingle("SELECT dolt_commit('-A', '-m', 'rename')");
$db->querySingle("SELECT dolt_checkout('main')");
$db->querySingle("SELECT dolt_merge('experiment')");

$log = $db->query('SELECT commit_hash, message FROM dolt_log');
while (($commit = $log->fetchArray(DOLTLITE3_ASSOC)) !== false) {
    echo "{$commit['commit_hash']} {$commit['message']}\n";
}
```

Every DoltLite feature — branches, merges, diffs (`dolt_diff`, `dolt_log`,
`dolt_branches` tables), remotes (`dolt_push`/`dolt_pull`/`dolt_clone`) — is
reachable through SQL; no binding-specific API is needed. See the
[DoltLite documentation](https://github.com/dolthub/doltlite) for the SQL
surface.

## Differences from SQLite3

- Errors always throw `Doltlite\Exception`; there is no warnings mode
  (matching where PHP itself is headed with `SQLite3`).
- `openBlob`, `createFunction`, `createAggregate`, `createCollation`, and
  `setAuthorizer` are not implemented. File an issue if you need them.
