<?php

declare(strict_types=1);

namespace Doltlite;

/**
 * A DoltLite database connection with the shape of PHP's SQLite3 class.
 * Errors always raise Doltlite\Exception; there is no warnings mode.
 * DoltLite's version-control surface is plain SQL — e.g.
 * $db->querySingle("SELECT dolt_commit('-A', '-m', 'message')").
 */
class Doltlite3
{
    private ?\FFI\CData $db = null;

    public function __construct(
        string $filename,
        int $flags = DOLTLITE3_OPEN_READWRITE | DOLTLITE3_OPEN_CREATE,
    ) {
        $ffi = Lib::get();
        $pDb = $ffi->new('sqlite3*[1]');
        $rc = $ffi->sqlite3_open_v2($filename, $pDb, $flags, null);
        if ($rc !== Lib::OK) {
            $msg = $pDb[0] !== null ? $ffi->sqlite3_errmsg($pDb[0]) : "open failed ($rc)";
            if ($pDb[0] !== null) {
                $ffi->sqlite3_close_v2($pDb[0]);
            }
            throw new Exception("Unable to open database: $msg", $rc);
        }
        $this->db = $pDb[0];
    }

    public function __destruct()
    {
        $this->close();
    }

    public function close(): bool
    {
        if ($this->db !== null) {
            Lib::get()->sqlite3_close_v2($this->db);
            $this->db = null;
        }
        return true;
    }

    /** Runs one or more semicolon-separated statements, discarding rows. */
    public function exec(string $query): bool
    {
        $ffi = Lib::get();
        $handle = $this->handle();
        $sql = $query;
        while (trim($sql) !== '') {
            $pStmt = $ffi->new('sqlite3_stmt*[1]');
            $pTail = $ffi->new('const char*[1]');
            $rc = $ffi->sqlite3_prepare_v2($handle, $sql, -1, $pStmt, $pTail);
            if ($rc !== Lib::OK) {
                throw $this->error();
            }
            $tail = $pTail[0];
            $consumed = is_string($tail) ? $tail
                : ($tail === null ? '' : \FFI::string($tail));
            if ($pStmt[0] !== null) {
                do {
                    $rc = $ffi->sqlite3_step($pStmt[0]);
                } while ($rc === Lib::ROW);
                $ffi->sqlite3_finalize($pStmt[0]);
                if ($rc !== Lib::DONE) {
                    throw $this->error();
                }
            }
            $sql = $consumed;
        }
        return true;
    }

    public function prepare(string $query): Doltlite3Stmt
    {
        return new Doltlite3Stmt($this, $query);
    }

    public function query(string $query): Doltlite3Result
    {
        return new Doltlite3Result($this, $this->prepare($query), true);
    }

    /**
     * First column of the first row (null when there are no rows), or with
     * $entireRow the whole first row as an associative array ([] when empty).
     */
    public function querySingle(string $query, bool $entireRow = false): mixed
    {
        $result = $this->query($query);
        $row = $result->fetchArray($entireRow ? DOLTLITE3_ASSOC : DOLTLITE3_NUM);
        $result->finalize();
        if ($row === false) {
            return $entireRow ? [] : null;
        }
        return $entireRow ? $row : $row[0];
    }

    public function lastInsertRowID(): int
    {
        return Lib::get()->sqlite3_last_insert_rowid($this->handle());
    }

    public function changes(): int
    {
        return Lib::get()->sqlite3_changes($this->handle());
    }

    public function lastErrorCode(): int
    {
        return $this->db === null ? 0 : Lib::get()->sqlite3_errcode($this->db);
    }

    public function lastErrorMsg(): string
    {
        return $this->db === null ? '' : Lib::get()->sqlite3_errmsg($this->db);
    }

    public function busyTimeout(int $milliseconds): bool
    {
        return Lib::get()->sqlite3_busy_timeout($this->handle(), $milliseconds) === Lib::OK;
    }

    public static function escapeString(string $value): string
    {
        return str_replace("'", "''", $value);
    }

    /** @return array{versionString: string, versionNumber: int} */
    public static function version(): array
    {
        $ffi = Lib::get();
        return [
            'versionString' => $ffi->sqlite3_libversion(),
            'versionNumber' => $ffi->sqlite3_libversion_number(),
        ];
    }

    /** @internal */
    public function handle(): \FFI\CData
    {
        if ($this->db === null) {
            throw new Exception('Database is closed');
        }
        return $this->db;
    }

    /** @internal */
    public function error(): Exception
    {
        return new Exception($this->lastErrorMsg(), $this->lastErrorCode());
    }
}
