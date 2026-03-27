/*
** Compile the original SQLite btree.c with renamed symbols so it can
** coexist with prolly_btree.c.  All exported sqlite3Btree* functions
** become orig_sqlite3Btree* via the prefix header.
*/
#include "btree_orig_prefix.h"
#include "btree.c"
