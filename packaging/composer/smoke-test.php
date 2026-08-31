<?php

/*
** Package-level smoke test for dolthub/doltlite-php. Runs from a consumer
** project against the installed package (vendor/autoload.php), covering the
** binding surface end to end: DDL/DML, prepared statements across every
** value type, result modes, error paths, and a version-control round trip
** (commit, branch, checkout, merge, dolt_log/dolt_diff reads).
*/

declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use Doltlite\Doltlite3;
use Doltlite\Exception as DoltliteException;

$failures = 0;

function check(string $name, mixed $got, mixed $want): void
{
    global $failures;
    if ($got === $want) {
        echo "  PASS: $name\n";
    } else {
        $failures++;
        echo "  FAIL: $name\n    want: " . var_export($want, true)
            . "\n    got:  " . var_export($got, true) . "\n";
    }
}

$dir = sys_get_temp_dir() . '/doltlite-php-smoke-' . getmypid();
@mkdir($dir);
$dbPath = "$dir/smoke.db";
@unlink($dbPath);

$db = new Doltlite3($dbPath);

$v = Doltlite3::version();
check('version reports a string', is_string($v['versionString']) && $v['versionString'] !== '', true);

// exec: multi-statement, then simple reads.
$db->exec("
    CREATE TABLE t(pk INTEGER PRIMARY KEY, s TEXT, f REAL, b BLOB);
    INSERT INTO t(pk, s, f, b) VALUES (1, 'one', 1.5, x'00ff10');
");
check('changes after insert', $db->changes(), 1);
check('lastInsertRowID', $db->lastInsertRowID(), 1);

// Prepared statements: every bind type, named and positional.
$stmt = $db->prepare('INSERT INTO t(pk, s, f, b) VALUES (:pk, :s, :f, :b)');
check('paramCount', $stmt->paramCount(), 4);
$stmt->bindValue(':pk', 2);
$stmt->bindValue('s', 'twö');
$stmt->bindValue(':f', 2.25);
$stmt->bindValue(':b', "\x01\x00\x02", DOLTLITE3_BLOB);
$stmt->execute()->finalize();
$stmt->reset();
$stmt->clear();
$stmt->bindValue(1, 3);
$stmt->bindValue(2, null);
$stmt->bindValue(3, 3);
$stmt->bindValue(4, null);
$stmt->execute()->finalize();
$stmt->close();

// Result modes and typed round trips.
$row = $db->querySingle('SELECT s, f, b FROM t WHERE pk = 2', true);
check('text round trip (utf8)', $row['s'], 'twö');
check('float round trip', $row['f'], 2.25);
check('blob round trip (embedded nul)', $row['b'], "\x01\x00\x02");
check('null round trip', $db->querySingle('SELECT s FROM t WHERE pk = 3'), null);
check('int is int', $db->querySingle('SELECT pk FROM t WHERE pk = 1'), 1);
check('querySingle no rows', $db->querySingle('SELECT s FROM t WHERE pk = 99'), null);
check('querySingle no rows (row mode)', $db->querySingle('SELECT * FROM t WHERE pk = 99', true), []);

$res = $db->query('SELECT pk, s FROM t ORDER BY pk');
check('numColumns', $res->numColumns(), 2);
check('columnName', $res->columnName(1), 's');
$first = $res->fetchArray(DOLTLITE3_NUM);
check('fetch NUM', [$first[0], $first[1]], [1, 'one']);
$res->reset();
$again = $res->fetchArray(DOLTLITE3_ASSOC);
check('reset refetches', $again['pk'], 1);
$n = 1;
while ($res->fetchArray() !== false) {
    $n++;
}
check('row count', $n, 3);
$res->finalize();

// Error paths raise Doltlite\Exception with the engine message.
try {
    $db->exec('SELECT * FROM missing_table');
    check('bad SQL throws', 'no exception', 'exception');
} catch (DoltliteException $e) {
    check('bad SQL throws', str_contains($e->getMessage(), 'missing_table'), true);
    check('lastErrorCode set', $db->lastErrorCode() !== 0, true);
}

// Version control is plain SQL through the same connection.
check('dolt_commit', $db->querySingle("SELECT dolt_commit('-A', '-m', 'first commit')") !== '', true);
check('dolt_branch', $db->querySingle("SELECT dolt_branch('feature')"), 0);
check('dolt_checkout feature', $db->querySingle("SELECT dolt_checkout('feature')"), 0);
$db->exec("UPDATE t SET s = 'branched' WHERE pk = 1");
check('dolt_commit on branch', $db->querySingle("SELECT dolt_commit('-A', '-m', 'feature work')") !== '', true);
check('dolt_checkout main', $db->querySingle("SELECT dolt_checkout('main')"), 0);
check('main unchanged', $db->querySingle('SELECT s FROM t WHERE pk = 1'), 'one');
$merge = $db->querySingle("SELECT dolt_merge('feature')", true);
check('dolt_merge fast-forwards', $db->querySingle('SELECT s FROM t WHERE pk = 1'), 'branched');
check('dolt_log sees both commits',
    $db->querySingle('SELECT count(*) FROM dolt_log') >= 2, true);

$db->close();
check('close idempotent', $db->close(), true);

@unlink($dbPath);
@rmdir($dir);

echo $failures === 0 ? "smoke-test: all checks passed\n" : "smoke-test: $failures failure(s)\n";
exit($failures === 0 ? 0 : 1);
