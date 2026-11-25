#!/usr/bin/env bash
set -euo pipefail

archive="${1:?usage: release_pdo_sqlite.sh <release library archive>}"
directory=$(basename "$archive" .zip)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

unzip -p "$archive" "$directory/libdoltlite.so" > "$work/libsqlite3.so.0"
env -u LD_PRELOAD LD_LIBRARY_PATH="$work${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" php <<'PHP'
<?php
$pdo = new PDO('sqlite::memory:');
if ($pdo->query('SELECT doltlite_engine()')->fetchColumn() !== 'prolly') {
    throw new RuntimeException('PDO did not open DoltLite');
}
$pdo->exec("CREATE TABLE accounts(id INTEGER PRIMARY KEY, label TEXT)");
$pdo->exec("INSERT INTO accounts VALUES (1, 'first')");
$statement = $pdo->query('SELECT label AS display_name FROM accounts');
$metadata = $statement->getColumnMeta(0);
if (($metadata['table'] ?? null) !== 'accounts' || ($metadata['name'] ?? null) !== 'display_name') {
    throw new RuntimeException('Unexpected PDO column metadata');
}
echo "PDO reports column metadata from the packaged DoltLite library\n";
PHP
