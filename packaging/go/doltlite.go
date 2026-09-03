// Package doltlite registers a database/sql driver for DoltLite: SQLite with
// Git-style version control. Version control is plain SQL on the connection,
// e.g. SELECT dolt_commit('-A', '-m', 'message').
//
// The engine is compiled from the amalgamation vendored alongside this file,
// so no system library is required.
package doltlite

/*
#cgo CFLAGS: -DDOLTLITE_PROLLY=1 -DSQLITE_THREADSAFE=1
#cgo CFLAGS: -DSQLITE_ENABLE_MATH_FUNCTIONS -DSQLITE_ENABLE_FTS5
#cgo CFLAGS: -DSQLITE_ENABLE_RTREE -DSQLITE_ENABLE_DBSTAT_VTAB
#cgo CFLAGS: -DSQLITE_ENABLE_COLUMN_METADATA -w
#cgo LDFLAGS: -lz -lm
#cgo linux LDFLAGS: -lpthread

#include <stdlib.h>
#include <string.h>
#include "doltlite.h"

// The engine copies bound text and blobs rather than borrowing them, which
// cgo requires: Go memory must not be retained by C after the call returns.
static int dl_bind_text(sqlite3_stmt *s, int i, const char *p, int n) {
  return sqlite3_bind_text(s, i, p, n, SQLITE_TRANSIENT);
}
static int dl_bind_blob(sqlite3_stmt *s, int i, const void *p, int n) {
  return sqlite3_bind_blob(s, i, p, n, SQLITE_TRANSIENT);
}
*/
import "C"

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"
	"unsafe"
)

func init() {
	sql.Register("doltlite", Driver{})
}

// Version reports the SQLite version the bundled engine derives from.
func Version() string {
	return C.GoString(C.sqlite3_libversion())
}

// defaultBusyTimeoutMs is how long a connection waits for a contended write
// before giving up. Overridable per connection with PRAGMA busy_timeout.
const defaultBusyTimeoutMs = 5000

// Error is an engine error carrying its result code.
type Error struct {
	Code int
	Msg  string
}

func (e *Error) Error() string { return fmt.Sprintf("doltlite: %s (code %d)", e.Msg, e.Code) }

// Driver implements driver.Driver.
type Driver struct{}

func (Driver) Open(name string) (driver.Conn, error) {
	c := newConn()

	var db *C.sqlite3
	var rc C.int
	var msg string
	c.run(func() {
		cname := C.CString(name)
		defer C.free(unsafe.Pointer(cname))
		rc = C.sqlite3_open_v2(cname, &db,
			C.SQLITE_OPEN_READWRITE|C.SQLITE_OPEN_CREATE|C.SQLITE_OPEN_URI, nil)
		if rc != C.SQLITE_OK {
			msg = "unable to open database"
			if db != nil {
				msg = C.GoString(C.sqlite3_errmsg(db))
				C.sqlite3_close_v2(db)
			}
		}
	})
	if rc != C.SQLITE_OK {
		c.stop()
		return nil, &Error{Code: int(rc), Msg: msg}
	}
	// database/sql hands out a pool of connections, and every one of them is a
	// separate writer to the same store. Without a busy handler the loser of a
	// write race fails immediately with SQLITE_BUSY, so ordinary concurrent use
	// through the pool drops writes. Wait instead; callers who want different
	// behaviour can set their own with PRAGMA busy_timeout.
	c.db = db
	c.run(func() { C.sqlite3_busy_timeout(db, C.int(defaultBusyTimeoutMs)) })
	return c, nil
}

type conn struct {
	mu     sync.Mutex
	db     *C.sqlite3
	closed bool
	stmts  map[*stmt]struct{}

	// runMu makes the retired check and the call that follows it atomic, so a
	// statement or Rows finishing on another goroutine cannot reach the handle
	// while Close is tearing it down.
	runMu   sync.Mutex
	retired bool
}

// errConnClosed is returned by run once the connection is torn down.
var errConnClosed = errors.New("doltlite: connection is closed")

func newConn() *conn {
	return &conn{stmts: map[*stmt]struct{}{}}
}

// run performs an engine call unless the connection is already torn down.
func (c *conn) run(fn func()) error {
	c.runMu.Lock()
	defer c.runMu.Unlock()
	if c.retired {
		return errConnClosed
	}
	fn()
	return nil
}

// stop marks the connection torn down. Serialized with run, so a call already
// under way finishes first.
func (c *conn) stop() {
	c.runMu.Lock()
	defer c.runMu.Unlock()
	c.retired = true
}

func (c *conn) err(rc C.int) error {
	code := C.sqlite3_extended_errcode(c.db)
	if code == 0 {
		code = rc
	}
	return &Error{Code: int(code), Msg: C.GoString(C.sqlite3_errmsg(c.db))}
}

func (c *conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	// Finalize what is still open before the handle goes. sqlite3_close_v2
	// would otherwise keep it alive waiting for statements that will never be
	// finalized, because their Close short-circuits on a closed connection.
	live := make([]*stmt, 0, len(c.stmts))
	for s := range c.stmts {
		live = append(live, s)
	}
	c.stmts = nil
	c.run(func() {
		for _, s := range live {
			if !s.closed {
				s.closed = true
				C.sqlite3_finalize(s.st)
			}
		}
		C.sqlite3_close_v2(c.db)
	})
	c.stop()
	return nil
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, driver.ErrBadConn
	}
	var st *C.sqlite3_stmt
	var rc C.int
	var nInput int
	if err := c.run(func() {
		cq := C.CString(query)
		defer C.free(unsafe.Pointer(cq))
		rc = C.sqlite3_prepare_v2(c.db, cq, -1, &st, nil)
		if rc == C.SQLITE_OK && st != nil {
			nInput = int(C.sqlite3_bind_parameter_count(st))
		}
	}); err != nil {
		return nil, driver.ErrBadConn
	}
	if rc != C.SQLITE_OK {
		return nil, c.err(rc)
	}
	if st == nil {
		return nil, errors.New("doltlite: query contains no statement")
	}
	s := &stmt{c: c, st: st, nInput: nInput}
	c.stmts[s] = struct{}{}
	return s, nil
}

// Begin starts a write transaction. IMMEDIATE, not the default DEFERRED: a
// deferred transaction takes its write lock at the first write, and losing
// that upgrade returns SQLITE_BUSY without consulting the busy handler,
// because waiting there could deadlock. Taking the lock up front puts the
// contention where the handler can wait on it, which is what a pooled
// database/sql caller needs.
func (c *conn) Begin() (driver.Tx, error) {
	if err := c.exec("BEGIN IMMEDIATE"); err != nil {
		return nil, err
	}
	return &tx{c: c}, nil
}

// exec runs a statement that takes no parameters and returns no rows.
func (c *conn) exec(query string) error {
	s, err := c.Prepare(query)
	if err != nil {
		return err
	}
	defer s.Close()
	_, err = s.(*stmt).Exec(nil)
	return err
}

type tx struct{ c *conn }

func (t *tx) Commit() error   { return t.c.exec("COMMIT") }
func (t *tx) Rollback() error { return t.c.exec("ROLLBACK") }

type stmt struct {
	c      *conn
	st     *C.sqlite3_stmt
	nInput int
	closed bool
}

func (s *stmt) Close() error {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	delete(s.c.stmts, s)
	// A closed connection finalized this already; there is nothing to run and
	// nowhere to run it.
	_ = s.c.run(func() { C.sqlite3_finalize(s.st) })
	return nil
}

func (s *stmt) NumInput() int { return s.nInput }

func (s *stmt) bind(args []driver.Value) error {
	C.sqlite3_reset(s.st)
	C.sqlite3_clear_bindings(s.st)
	for i, a := range args {
		idx := C.int(i + 1)
		var rc C.int
		switch v := a.(type) {
		case nil:
			rc = C.sqlite3_bind_null(s.st, idx)
		case int64:
			rc = C.sqlite3_bind_int64(s.st, idx, C.sqlite3_int64(v))
		case float64:
			rc = C.sqlite3_bind_double(s.st, idx, C.double(v))
		case bool:
			n := C.sqlite3_int64(0)
			if v {
				n = 1
			}
			rc = C.sqlite3_bind_int64(s.st, idx, n)
		case string:
			// An empty string still needs a valid pointer for the bind.
			if len(v) == 0 {
				rc = C.dl_bind_text(s.st, idx, (*C.char)(unsafe.Pointer(&[]byte{0}[0])), 0)
			} else {
				b := []byte(v)
				rc = C.dl_bind_text(s.st, idx,
					(*C.char)(unsafe.Pointer(&b[0])), C.int(len(b)))
			}
		case []byte:
			if len(v) == 0 {
				rc = C.dl_bind_blob(s.st, idx, unsafe.Pointer(&[]byte{0}[0]), 0)
			} else {
				rc = C.dl_bind_blob(s.st, idx, unsafe.Pointer(&v[0]), C.int(len(v)))
			}
		default:
			return fmt.Errorf("doltlite: unsupported argument type %T", a)
		}
		if rc != C.SQLITE_OK {
			return s.c.err(rc)
		}
	}
	return nil
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	if s.closed {
		return nil, driver.ErrBadConn
	}
	var res result
	var bindErr error
	var stepRc C.int
	if err := s.c.run(func() {
		if bindErr = s.bind(args); bindErr != nil {
			return
		}
		for {
			stepRc = C.sqlite3_step(s.st)
			if stepRc == C.SQLITE_ROW {
				continue
			}
			break
		}
		if stepRc == C.SQLITE_DONE {
			res = result{
				lastID:  int64(C.sqlite3_last_insert_rowid(s.c.db)),
				changes: int64(C.sqlite3_changes(s.c.db)),
			}
		}
		C.sqlite3_reset(s.st)
	}); err != nil {
		return nil, driver.ErrBadConn
	}
	if bindErr != nil {
		return nil, bindErr
	}
	if stepRc != C.SQLITE_DONE {
		return nil, s.c.err(stepRc)
	}
	return res, nil
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	if s.closed {
		return nil, driver.ErrBadConn
	}
	var cols []string
	var bindErr error
	if err := s.c.run(func() {
		if bindErr = s.bind(args); bindErr != nil {
			return
		}
		n := int(C.sqlite3_column_count(s.st))
		cols = make([]string, n)
		for i := 0; i < n; i++ {
			cols[i] = C.GoString(C.sqlite3_column_name(s.st, C.int(i)))
		}
	}); err != nil {
		return nil, driver.ErrBadConn
	}
	if bindErr != nil {
		return nil, bindErr
	}
	return &rows{s: s, cols: cols}, nil
}

type result struct {
	lastID  int64
	changes int64
}

func (r result) LastInsertId() (int64, error) { return r.lastID, nil }
func (r result) RowsAffected() (int64, error) { return r.changes, nil }

type rows struct {
	s      *stmt
	cols   []string
	closed bool
}

func (r *rows) Columns() []string { return r.cols }

func (r *rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.s.c.mu.Lock()
	defer r.s.c.mu.Unlock()
	if r.s.closed {
		return nil
	}
	_ = r.s.c.run(func() { C.sqlite3_reset(r.s.st) })
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	r.s.c.mu.Lock()
	defer r.s.c.mu.Unlock()
	// Closing the connection finalizes its statements, so a Rows held past
	// that point must not touch one.
	if r.s.closed {
		return driver.ErrBadConn
	}

	var rc C.int
	if err := r.s.c.run(func() { rc = C.sqlite3_step(r.s.st) }); err != nil {
		return driver.ErrBadConn
	}
	if rc == C.SQLITE_DONE {
		return io.EOF
	}
	if rc != C.SQLITE_ROW {
		return r.s.c.err(rc)
	}
	if err := r.s.c.run(func() {
		for i := range dest {
			idx := C.int(i)
			switch C.sqlite3_column_type(r.s.st, idx) {
			case C.SQLITE_NULL:
				dest[i] = nil
			case C.SQLITE_INTEGER:
				dest[i] = int64(C.sqlite3_column_int64(r.s.st, idx))
			case C.SQLITE_FLOAT:
				dest[i] = float64(C.sqlite3_column_double(r.s.st, idx))
			case C.SQLITE_BLOB:
				dest[i] = columnBytes(r.s.st, idx, C.sqlite3_column_blob(r.s.st, idx))
			default:
				// Text: copied by length rather than as a C string, since a value
				// may contain embedded NULs.
				b := columnBytes(r.s.st, idx,
					unsafe.Pointer(C.sqlite3_column_text(r.s.st, idx)))
				dest[i] = string(b)
			}
		}
	}); err != nil {
		return driver.ErrBadConn
	}
	return nil
}

func columnBytes(st *C.sqlite3_stmt, idx C.int, p unsafe.Pointer) []byte {
	n := C.sqlite3_column_bytes(st, idx)
	if p == nil || n == 0 {
		return []byte{}
	}
	return C.GoBytes(p, n)
}
