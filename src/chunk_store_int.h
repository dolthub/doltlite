#ifndef CHUNK_STORE_INT_H
#define CHUNK_STORE_INT_H

/* Private declarations shared by the chunk_store implementation modules. */

#include "chunk_store.h"
#include "prolly_hash.h"
#include "prolly_encoding.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

typedef sqlite3_file *CsFileLock;
# define CS_FILE_LOCK_INIT 0
# define CS_GRAPH_LOCK(cs) ((cs)->pGraphLockFile)

#define CS_RECENT_FAST_PATH_MAX 16384
#define CS_WRITEBUF_RETAIN_MAX (64*1024)
#define CS_PENDING_DRAIN_LIMIT (64*1024*1024)

#endif /* CHUNK_STORE_INT_H */
