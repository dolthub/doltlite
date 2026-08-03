/*
** Compile the original SQLite backup.c with renamed symbols. DOLTLITE_ORIG_BACKUP
** makes findBtree() resolve schema names to the stock btree behind doltlite's
** Btree wrapper, since Db.pBt is a wrapper in every build.
*/
#define DOLTLITE_ORIG_BACKUP 1
#include "btree_orig_prefix.h"
#include "backup.c"
