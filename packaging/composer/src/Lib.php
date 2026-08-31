<?php

declare(strict_types=1);

namespace Doltlite;

/**
 * Process-wide FFI handle to libdoltlite. The library keeps SQLite's C API
 * under SQLite's own symbol names, so the declarations below are the plain
 * sqlite3_* subset the classes in this package call.
 */
final class Lib
{
    private static ?\FFI $ffi = null;

    /** SQLITE_TRANSIENT: the library copies bound text/blobs immediately. */
    public const TRANSIENT = -1;

    public const OK = 0;
    public const ROW = 100;
    public const DONE = 101;

    private const CDEF = <<<'C'
        typedef struct sqlite3 sqlite3;
        typedef struct sqlite3_stmt sqlite3_stmt;
        int sqlite3_open_v2(const char *filename, sqlite3 **ppDb, int flags,
                            const char *zVfs);
        int sqlite3_close_v2(sqlite3 *db);
        int sqlite3_prepare_v2(sqlite3 *db, const char *zSql, int nByte,
                               sqlite3_stmt **ppStmt, const char **pzTail);
        int sqlite3_step(sqlite3_stmt *pStmt);
        int sqlite3_reset(sqlite3_stmt *pStmt);
        int sqlite3_finalize(sqlite3_stmt *pStmt);
        int sqlite3_clear_bindings(sqlite3_stmt *pStmt);
        int sqlite3_bind_parameter_count(sqlite3_stmt *pStmt);
        int sqlite3_bind_parameter_index(sqlite3_stmt *pStmt, const char *zName);
        int sqlite3_bind_int64(sqlite3_stmt *pStmt, int i, int64_t v);
        int sqlite3_bind_double(sqlite3_stmt *pStmt, int i, double v);
        int sqlite3_bind_null(sqlite3_stmt *pStmt, int i);
        int sqlite3_bind_text(sqlite3_stmt *pStmt, int i, const char *z, int n,
                              intptr_t destructor);
        int sqlite3_bind_blob(sqlite3_stmt *pStmt, int i, const void *p, int n,
                              intptr_t destructor);
        int sqlite3_column_count(sqlite3_stmt *pStmt);
        const char *sqlite3_column_name(sqlite3_stmt *pStmt, int i);
        int sqlite3_column_type(sqlite3_stmt *pStmt, int i);
        int64_t sqlite3_column_int64(sqlite3_stmt *pStmt, int i);
        double sqlite3_column_double(sqlite3_stmt *pStmt, int i);
        const unsigned char *sqlite3_column_text(sqlite3_stmt *pStmt, int i);
        const void *sqlite3_column_blob(sqlite3_stmt *pStmt, int i);
        int sqlite3_column_bytes(sqlite3_stmt *pStmt, int i);
        const char *sqlite3_errmsg(sqlite3 *db);
        int sqlite3_errcode(sqlite3 *db);
        int64_t sqlite3_last_insert_rowid(sqlite3 *db);
        int sqlite3_changes(sqlite3 *db);
        int sqlite3_busy_timeout(sqlite3 *db, int ms);
        const char *sqlite3_libversion(void);
        int sqlite3_libversion_number(void);
        C;

    public static function get(): \FFI
    {
        if (self::$ffi === null) {
            self::$ffi = \FFI::cdef(self::CDEF, self::resolveLibrary());
        }
        return self::$ffi;
    }

    /**
     * Resolution order: explicit override, the binary bundled with this
     * package for the current platform, then the system search path.
     */
    private static function resolveLibrary(): string
    {
        $override = getenv('DOLTLITE_PHP_LIB');
        if ($override !== false && $override !== '') {
            return $override;
        }

        $ext = match (PHP_OS_FAMILY) {
            'Darwin' => 'dylib',
            'Windows' => 'dll',
            default => 'so',
        };
        $os = strtolower(PHP_OS_FAMILY);
        $machine = strtolower(php_uname('m'));
        $arch = match ($machine) {
            'arm64', 'aarch64' => 'arm64',
            'x86_64', 'amd64' => 'x86_64',
            default => $machine,
        };

        $bundled = dirname(__DIR__) . "/lib/$os-$arch/libdoltlite.$ext";
        if (is_file($bundled)) {
            return $bundled;
        }
        return "libdoltlite.$ext";
    }
}
