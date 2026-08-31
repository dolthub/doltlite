<?php

declare(strict_types=1);

namespace Doltlite;

class Doltlite3Stmt
{
    private Doltlite3 $db;
    private ?\FFI\CData $stmt = null;

    /** @internal use Doltlite3::prepare() */
    public function __construct(Doltlite3 $db, string $query)
    {
        $ffi = Lib::get();
        $pStmt = $ffi->new('sqlite3_stmt*[1]');
        $rc = $ffi->sqlite3_prepare_v2($db->handle(), $query, -1, $pStmt, null);
        if ($rc !== Lib::OK) {
            throw $db->error();
        }
        if ($pStmt[0] === null) {
            throw new Exception('Query contains no SQL statement');
        }
        $this->db = $db;
        $this->stmt = $pStmt[0];
    }

    public function __destruct()
    {
        $this->close();
    }

    public function bindValue(string|int $param, mixed $value, ?int $type = null): bool
    {
        $ffi = Lib::get();
        $stmt = $this->handle();
        $index = $this->paramIndex($param);

        if ($type === null) {
            $type = match (true) {
                $value === null => DOLTLITE3_NULL,
                is_int($value), is_bool($value) => DOLTLITE3_INTEGER,
                is_float($value) => DOLTLITE3_FLOAT,
                default => DOLTLITE3_TEXT,
            };
        }
        $rc = match ($type) {
            DOLTLITE3_NULL => $ffi->sqlite3_bind_null($stmt, $index),
            DOLTLITE3_INTEGER => $ffi->sqlite3_bind_int64($stmt, $index, (int) $value),
            DOLTLITE3_FLOAT => $ffi->sqlite3_bind_double($stmt, $index, (float) $value),
            DOLTLITE3_BLOB => $ffi->sqlite3_bind_blob(
                $stmt, $index, (string) $value, strlen((string) $value), Lib::TRANSIENT),
            default => $ffi->sqlite3_bind_text(
                $stmt, $index, (string) $value, strlen((string) $value), Lib::TRANSIENT),
        };
        if ($rc !== Lib::OK) {
            throw $this->db->error();
        }
        return true;
    }

    public function execute(): Doltlite3Result
    {
        Lib::get()->sqlite3_reset($this->handle());
        return new Doltlite3Result($this->db, $this, false);
    }

    public function reset(): bool
    {
        Lib::get()->sqlite3_reset($this->handle());
        return true;
    }

    public function clear(): bool
    {
        Lib::get()->sqlite3_clear_bindings($this->handle());
        return true;
    }

    public function paramCount(): int
    {
        return Lib::get()->sqlite3_bind_parameter_count($this->handle());
    }

    public function close(): bool
    {
        if ($this->stmt !== null) {
            Lib::get()->sqlite3_finalize($this->stmt);
            $this->stmt = null;
        }
        return true;
    }

    private function paramIndex(string|int $param): int
    {
        if (is_int($param)) {
            return $param;
        }
        $ffi = Lib::get();
        $index = $ffi->sqlite3_bind_parameter_index($this->handle(), $param);
        if ($index === 0 && $param !== '' && $param[0] !== ':' && $param[0] !== '@') {
            $index = $ffi->sqlite3_bind_parameter_index($this->handle(), ':' . $param);
        }
        if ($index === 0) {
            throw new Exception("Unknown parameter: $param");
        }
        return $index;
    }

    /** @internal */
    public function cHandle(): \FFI\CData
    {
        return $this->handle();
    }

    private function handle(): \FFI\CData
    {
        if ($this->stmt === null) {
            throw new Exception('Statement is closed');
        }
        return $this->stmt;
    }
}
