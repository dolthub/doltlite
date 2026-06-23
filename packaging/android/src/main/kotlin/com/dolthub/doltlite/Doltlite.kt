package com.dolthub.doltlite

import com.sun.jna.Pointer
import com.sun.jna.ptr.PointerByReference

/** Error from a doltlite operation. */
class DoltliteException(message: String) : Exception(message)

/**
 * A DoltLite database connection: SQLite plus Dolt version control.
 *
 * ```
 * Doltlite(":memory:").use { db ->
 *     db.execute("CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)")
 *     db.execute("INSERT INTO t(v) VALUES (?)", "hello")
 *     db.doltCommit("first commit")
 *     db.query("SELECT commit_hash, message FROM dolt_log")
 * }
 * ```
 *
 * Generic methods (execute/query/close) are ordinary SQLite operations; the
 * dolt-prefixed ones wrap the Dolt version-control functions, which are reached
 * through SQL.
 */
class Doltlite(path: String = ":memory:") : AutoCloseable {
    private val c = CDoltlite.INSTANCE
    private var handle: Pointer?

    init {
        val ref = PointerByReference()
        val flags = CDoltlite.OPEN_READWRITE or CDoltlite.OPEN_CREATE
        val rc = c.sqlite3_open_v2(path, ref, flags, null)
        handle = ref.value
        if (rc != CDoltlite.OK) {
            val message = handle?.let { c.sqlite3_errmsg(it) } ?: "unable to open $path"
            handle?.let { c.sqlite3_close_v2(it) }
            handle = null
            throw DoltliteException("open failed ($rc): $message")
        }
    }

    /** Run SQL with optional positional bind values; returns result rows. */
    fun execute(sql: String, vararg binds: Any?): List<List<Any?>> {
        val ref = PointerByReference()
        if (c.sqlite3_prepare_v2(handle, sql, -1, ref, null) != CDoltlite.OK) {
            throw DoltliteException("prepare failed: ${c.sqlite3_errmsg(handle)}")
        }
        val stmt = ref.value
        try {
            bindAll(stmt, binds)
            return collectRows(stmt)
        } finally {
            c.sqlite3_finalize(stmt)
        }
    }

    /** Alias for [execute], for read queries. */
    fun query(sql: String, vararg binds: Any?): List<List<Any?>> = execute(sql, *binds)

    /**
     * Make a Dolt version-control commit (a new entry in dolt_log) — not a SQL
     * transaction commit. Stages everything when [all] is true. Returns the new
     * commit hash.
     */
    fun doltCommit(message: String, all: Boolean = true): String? {
        val args = if (all) arrayOf<Any?>("-A", "-m", message) else arrayOf<Any?>("-m", message)
        val placeholders = args.joinToString(", ") { "?" }
        return execute("SELECT dolt_commit($placeholders)", *args).firstOrNull()?.firstOrNull() as String?
    }

    /** The doltlite version string (SELECT dolt_version()). */
    fun doltVersion(): String? =
        execute("SELECT dolt_version()").firstOrNull()?.firstOrNull() as String?

    override fun close() {
        handle?.let { c.sqlite3_close_v2(it) }
        handle = null
    }

    private fun bindAll(stmt: Pointer?, binds: Array<out Any?>) {
        binds.forEachIndexed { i, value ->
            val index = i + 1
            when (value) {
                null -> c.sqlite3_bind_null(stmt, index)
                is Int -> c.sqlite3_bind_int64(stmt, index, value.toLong())
                is Long -> c.sqlite3_bind_int64(stmt, index, value)
                is Double -> c.sqlite3_bind_double(stmt, index, value)
                is Float -> c.sqlite3_bind_double(stmt, index, value.toDouble())
                else -> {
                    val s = value.toString()
                    c.sqlite3_bind_text(stmt, index, s, s.toByteArray(Charsets.UTF_8).size, CDoltlite.TRANSIENT)
                }
            }
        }
    }

    private fun collectRows(stmt: Pointer?): List<List<Any?>> {
        val rows = mutableListOf<List<Any?>>()
        val ncols = c.sqlite3_column_count(stmt)
        while (true) {
            when (c.sqlite3_step(stmt)) {
                CDoltlite.DONE -> break
                CDoltlite.ROW -> rows.add((0 until ncols).map { columnValue(stmt, it) })
                else -> throw DoltliteException("step failed: ${c.sqlite3_errmsg(handle)}")
            }
        }
        return rows
    }

    private fun columnValue(stmt: Pointer?, col: Int): Any? = when (c.sqlite3_column_type(stmt, col)) {
        CDoltlite.INTEGER -> c.sqlite3_column_int64(stmt, col)
        CDoltlite.FLOAT -> c.sqlite3_column_double(stmt, col)
        CDoltlite.NULL -> null
        else -> c.sqlite3_column_text(stmt, col)
    }
}
