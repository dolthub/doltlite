package doltlite

/*
#include <stdint.h>
#include "doltlite.h"

extern int dl_go_authorize(uintptr_t h, int action, char *arg1, char *arg2,
                           char *database, char *trigger);

static int dl_xauth(void *pCtx, int action, const char *arg1, const char *arg2,
                    const char *database, const char *trigger) {
  return dl_go_authorize((uintptr_t)pCtx, action, (char *)arg1, (char *)arg2,
                         (char *)database, (char *)trigger);
}
static int dl_set_authorizer(sqlite3 *db, uintptr_t h) {
  return sqlite3_set_authorizer(db, h ? dl_xauth : 0, (void *)h);
}
*/
import "C"

import (
	"runtime/cgo"
)

// Authorizer actions, as SQLite reports them while compiling a statement.
const (
	AuthCreateIndex       = int(C.SQLITE_CREATE_INDEX)
	AuthCreateTable       = int(C.SQLITE_CREATE_TABLE)
	AuthCreateTempIndex   = int(C.SQLITE_CREATE_TEMP_INDEX)
	AuthCreateTempTable   = int(C.SQLITE_CREATE_TEMP_TABLE)
	AuthCreateTempTrigger = int(C.SQLITE_CREATE_TEMP_TRIGGER)
	AuthCreateTempView    = int(C.SQLITE_CREATE_TEMP_VIEW)
	AuthCreateTrigger     = int(C.SQLITE_CREATE_TRIGGER)
	AuthCreateView        = int(C.SQLITE_CREATE_VIEW)
	AuthDelete            = int(C.SQLITE_DELETE)
	AuthDropIndex         = int(C.SQLITE_DROP_INDEX)
	AuthDropTable         = int(C.SQLITE_DROP_TABLE)
	AuthDropTempIndex     = int(C.SQLITE_DROP_TEMP_INDEX)
	AuthDropTempTable     = int(C.SQLITE_DROP_TEMP_TABLE)
	AuthDropTempTrigger   = int(C.SQLITE_DROP_TEMP_TRIGGER)
	AuthDropTempView      = int(C.SQLITE_DROP_TEMP_VIEW)
	AuthDropTrigger       = int(C.SQLITE_DROP_TRIGGER)
	AuthDropView          = int(C.SQLITE_DROP_VIEW)
	AuthInsert            = int(C.SQLITE_INSERT)
	AuthPragma            = int(C.SQLITE_PRAGMA)
	AuthRead              = int(C.SQLITE_READ)
	AuthSelect            = int(C.SQLITE_SELECT)
	AuthTransaction       = int(C.SQLITE_TRANSACTION)
	AuthUpdate            = int(C.SQLITE_UPDATE)
	AuthAttach            = int(C.SQLITE_ATTACH)
	AuthDetach            = int(C.SQLITE_DETACH)
	AuthAlterTable        = int(C.SQLITE_ALTER_TABLE)
	AuthReindex           = int(C.SQLITE_REINDEX)
	AuthAnalyze           = int(C.SQLITE_ANALYZE)
	AuthCreateVtable      = int(C.SQLITE_CREATE_VTABLE)
	AuthDropVtable        = int(C.SQLITE_DROP_VTABLE)
	AuthFunction          = int(C.SQLITE_FUNCTION)
	AuthSavepoint         = int(C.SQLITE_SAVEPOINT)
	AuthRecursive         = int(C.SQLITE_RECURSIVE)
)

// Authorizer results: allow the action, fail the statement, or (for a read) read NULL instead.
const (
	AuthOK     = int(C.SQLITE_OK)
	AuthDeny   = int(C.SQLITE_DENY)
	AuthIgnore = int(C.SQLITE_IGNORE)
)

// Authorizer is consulted for each action a statement takes while SQLite compiles it,
// with the action's arguments as sqlite3_set_authorizer documents them ("" for NULL).
// It runs on the compiling goroutine and must not use the connection.
type Authorizer func(action int, arg1, arg2, database, trigger string) int

// AuthorizerSetter is implemented by this driver's connections. Reach it with
// sql.Conn.Raw:
//
//	raw.(doltlite.AuthorizerSetter).SetAuthorizer(fn)
//
// Pass nil to remove the authorizer.
type AuthorizerSetter interface {
	SetAuthorizer(Authorizer) error
}

func (c *conn) SetAuthorizer(fn Authorizer) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errConnClosed
	}

	var h cgo.Handle
	if fn != nil {
		h = cgo.NewHandle(fn)
	}
	var rc C.int
	if err := c.run(func() { rc = C.dl_set_authorizer(c.db, C.uintptr_t(h)) }); err != nil {
		if fn != nil {
			h.Delete()
		}
		return err
	}
	if rc != C.SQLITE_OK {
		if fn != nil {
			h.Delete()
		}
		return c.err(rc)
	}
	if c.hasAuthorizer {
		c.authorizer.Delete()
	}
	c.authorizer, c.hasAuthorizer = h, fn != nil
	return nil
}

//export dl_go_authorize
func dl_go_authorize(h C.uintptr_t, action C.int, arg1, arg2, database, trigger *C.char) C.int {
	fn, ok := cgo.Handle(h).Value().(Authorizer)
	if !ok {
		return C.SQLITE_DENY
	}
	return C.int(fn(int(action), C.GoString(arg1), C.GoString(arg2), C.GoString(database), C.GoString(trigger)))
}

// Limit categories for SetLimit.
const (
	LimitLength            = int(C.SQLITE_LIMIT_LENGTH)
	LimitSQLLength         = int(C.SQLITE_LIMIT_SQL_LENGTH)
	LimitColumn            = int(C.SQLITE_LIMIT_COLUMN)
	LimitExprDepth         = int(C.SQLITE_LIMIT_EXPR_DEPTH)
	LimitCompoundSelect    = int(C.SQLITE_LIMIT_COMPOUND_SELECT)
	LimitVdbeOp            = int(C.SQLITE_LIMIT_VDBE_OP)
	LimitFunctionArg       = int(C.SQLITE_LIMIT_FUNCTION_ARG)
	LimitAttached          = int(C.SQLITE_LIMIT_ATTACHED)
	LimitLikePatternLength = int(C.SQLITE_LIMIT_LIKE_PATTERN_LENGTH)
	LimitVariableNumber    = int(C.SQLITE_LIMIT_VARIABLE_NUMBER)
	LimitTriggerDepth      = int(C.SQLITE_LIMIT_TRIGGER_DEPTH)
	LimitWorkerThreads     = int(C.SQLITE_LIMIT_WORKER_THREADS)
)

// Limiter is implemented by this driver's connections. Reach it with
// sql.Conn.Raw:
//
//	raw.(doltlite.Limiter).SetLimit(doltlite.LimitLength, 16<<20)
//
// SetLimit returns the previous value; a negative value only reads it.
type Limiter interface {
	SetLimit(id, value int) (int, error)
}

func (c *conn) SetLimit(id, value int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, errConnClosed
	}
	var prev C.int
	if err := c.run(func() { prev = C.sqlite3_limit(c.db, C.int(id), C.int(value)) }); err != nil {
		return 0, err
	}
	return int(prev), nil
}
