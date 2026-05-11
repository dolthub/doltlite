
#ifndef DOLTLITE_IGNORE_H
#define DOLTLITE_IGNORE_H

typedef struct sqlite3 sqlite3;

int doltliteCheckIgnore(sqlite3 *db, const char *zTable,
                        int *pIgnored, char **pzErr);

#endif
