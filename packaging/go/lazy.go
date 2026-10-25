package doltlite

/*
#include <stdlib.h>
#include "doltlite.h"
*/
import "C"

import "unsafe"

// LazyInitializer is implemented by this driver's connections. Reach it with
// sql.Conn.Raw:
//
//	raw.(doltlite.LazyInitializer).InitLazy(refs)
type LazyInitializer interface {
	InitLazy(refs []byte) error
}

// InitLazy installs a serialized DoltLite refs blob as the refs of this
// connection's main database, as doltlite_init_lazy does. The database may be
// fresh or may already hold chunks cached from earlier refs. Missing chunks
// are read through a registered ChunkSource, or from origin when the database
// is opened with lazy_origin=1. It must run outside a transaction.
func (c *conn) InitLazy(refs []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errConnClosed
	}
	var p unsafe.Pointer
	if len(refs) > 0 {
		p = C.CBytes(refs)
		defer C.free(p)
	}
	rc := C.int(C.SQLITE_OK)
	if err := c.run(func() { rc = C.doltlite_init_lazy(c.db, p, C.int(len(refs))) }); err != nil {
		return err
	}
	if rc != C.SQLITE_OK {
		return c.err(rc)
	}
	return nil
}
