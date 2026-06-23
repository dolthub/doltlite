package com.dolthub.doltlite

import com.sun.jna.Library
import com.sun.jna.Native
import com.sun.jna.Pointer
import com.sun.jna.ptr.PointerByReference

/**
 * JNA bindings to the doltlite C API (the SQLite C API; the dolt_* functions are
 * reached through SQL). The bundled libdoltlite.so is loaded by name from the
 * AAR's jniLibs.
 */
internal interface CDoltlite : Library {
    fun sqlite3_open_v2(filename: String, ppDb: PointerByReference, flags: Int, zVfs: String?): Int
    fun sqlite3_close_v2(db: Pointer?): Int
    fun sqlite3_errmsg(db: Pointer?): String?
    fun sqlite3_exec(db: Pointer?, sql: String, callback: Pointer?, arg: Pointer?, errmsg: PointerByReference?): Int
    fun sqlite3_prepare_v2(db: Pointer?, sql: String, nByte: Int, ppStmt: PointerByReference, pzTail: Pointer?): Int
    fun sqlite3_step(stmt: Pointer?): Int
    fun sqlite3_finalize(stmt: Pointer?): Int
    fun sqlite3_column_count(stmt: Pointer?): Int
    fun sqlite3_column_type(stmt: Pointer?, col: Int): Int
    fun sqlite3_column_int64(stmt: Pointer?, col: Int): Long
    fun sqlite3_column_double(stmt: Pointer?, col: Int): Double
    fun sqlite3_column_text(stmt: Pointer?, col: Int): String?
    fun sqlite3_bind_int64(stmt: Pointer?, index: Int, value: Long): Int
    fun sqlite3_bind_double(stmt: Pointer?, index: Int, value: Double): Int
    fun sqlite3_bind_text(stmt: Pointer?, index: Int, value: String, nByte: Int, destructor: Pointer?): Int
    fun sqlite3_bind_null(stmt: Pointer?, index: Int): Int
    fun sqlite3_free(ptr: Pointer?)

    companion object {
        const val OK = 0
        const val ROW = 100
        const val DONE = 101

        const val INTEGER = 1
        const val FLOAT = 2
        const val TEXT = 3
        const val BLOB = 4
        const val NULL = 5

        const val OPEN_READWRITE = 0x00000002
        const val OPEN_CREATE = 0x00000004

        // SQLITE_TRANSIENT (-1): tell the engine to copy bound buffers.
        val TRANSIENT: Pointer = Pointer(-1L)

        val INSTANCE: CDoltlite = Native.load("doltlite", CDoltlite::class.java)
    }
}
