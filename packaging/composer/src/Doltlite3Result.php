<?php

declare(strict_types=1);

namespace Doltlite;

class Doltlite3Result
{
    private Doltlite3 $db;
    /** Kept as an object reference so the statement outlives query(). */
    private ?Doltlite3Stmt $stmt;
    /** Results from query() own their statement; execute()'s do not. */
    private bool $ownsStmt;
    /** A row produced by the eager first step, not yet fetched. */
    private bool $pendingRow;
    private bool $done;

    /** @internal use Doltlite3::query() or Doltlite3Stmt::execute() */
    public function __construct(Doltlite3 $db, Doltlite3Stmt $stmt, bool $ownsStmt)
    {
        $this->db = $db;
        $this->stmt = $stmt;
        $this->ownsStmt = $ownsStmt;
        // Step eagerly so writes run even if the caller never fetches.
        $rc = Lib::get()->sqlite3_step($stmt->cHandle());
        if ($rc !== Lib::ROW && $rc !== Lib::DONE) {
            Lib::get()->sqlite3_reset($stmt->cHandle());
            throw $db->error();
        }
        $this->pendingRow = ($rc === Lib::ROW);
        $this->done = ($rc === Lib::DONE);
    }

    /** @return array<int|string, mixed>|false */
    public function fetchArray(int $mode = DOLTLITE3_BOTH): array|false
    {
        $ffi = Lib::get();
        $stmt = $this->handle();
        if ($this->pendingRow) {
            $this->pendingRow = false;
        } else {
            if ($this->done) {
                return false;
            }
            $rc = $ffi->sqlite3_step($stmt);
            if ($rc === Lib::DONE) {
                $this->done = true;
                return false;
            }
            if ($rc !== Lib::ROW) {
                throw $this->db->error();
            }
        }

        $row = [];
        $n = $ffi->sqlite3_column_count($stmt);
        for ($i = 0; $i < $n; $i++) {
            $value = $this->columnValue($i);
            if ($mode & DOLTLITE3_NUM) {
                $row[$i] = $value;
            }
            if ($mode & DOLTLITE3_ASSOC) {
                $row[$ffi->sqlite3_column_name($stmt, $i)] = $value;
            }
        }
        return $row;
    }

    public function numColumns(): int
    {
        return Lib::get()->sqlite3_column_count($this->handle());
    }

    public function columnName(int $column): string
    {
        return Lib::get()->sqlite3_column_name($this->handle(), $column);
    }

    public function columnType(int $column): int
    {
        return Lib::get()->sqlite3_column_type($this->handle(), $column);
    }

    /** Rewinds so the rows can be fetched again. */
    public function reset(): bool
    {
        $stmt = $this->handle();
        Lib::get()->sqlite3_reset($stmt);
        $rc = Lib::get()->sqlite3_step($stmt);
        if ($rc !== Lib::ROW && $rc !== Lib::DONE) {
            throw $this->db->error();
        }
        $this->pendingRow = ($rc === Lib::ROW);
        $this->done = ($rc === Lib::DONE);
        return true;
    }

    public function finalize(): bool
    {
        if ($this->stmt !== null) {
            if ($this->ownsStmt) {
                $this->stmt->close();
            } else {
                Lib::get()->sqlite3_reset($this->stmt->cHandle());
            }
            $this->stmt = null;
        }
        return true;
    }

    public function __destruct()
    {
        $this->finalize();
    }

    private function columnValue(int $i): mixed
    {
        $ffi = Lib::get();
        $stmt = $this->handle();
        return match ($ffi->sqlite3_column_type($stmt, $i)) {
            DOLTLITE3_NULL => null,
            DOLTLITE3_INTEGER => $ffi->sqlite3_column_int64($stmt, $i),
            DOLTLITE3_FLOAT => $ffi->sqlite3_column_double($stmt, $i),
            DOLTLITE3_BLOB => $this->bytes($ffi->sqlite3_column_blob($stmt, $i), $i),
            default => $this->bytes($ffi->sqlite3_column_text($stmt, $i), $i),
        };
    }

    private function bytes(?\FFI\CData $ptr, int $i): string
    {
        $len = Lib::get()->sqlite3_column_bytes($this->handle(), $i);
        if ($ptr === null || $len === 0) {
            return '';
        }
        return \FFI::string($ptr, $len);
    }

    private function handle(): \FFI\CData
    {
        if ($this->stmt === null) {
            throw new Exception('Result is finalized');
        }
        return $this->stmt->cHandle();
    }
}
