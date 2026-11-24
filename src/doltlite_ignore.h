
#ifndef DOLTLITE_IGNORE_H
#define DOLTLITE_IGNORE_H

typedef struct sqlite3 sqlite3;
typedef struct sqlite3_vtab sqlite3_vtab;
typedef struct ProllyHash ProllyHash;

int doltliteCheckIgnore(sqlite3 *db, const char *zTable,
                        int *pIgnored, char **pzErr);
int doltliteVtabSkipIgnored(sqlite3 *db, sqlite3_vtab *pVtab,
                            const char *zName, int *pSkip);

typedef struct DoltliteIgnoreSet DoltliteIgnoreSet;
int doltliteIgnoreSetLoad(sqlite3*, const ProllyHash *pCatalog,
                          DoltliteIgnoreSet**, char **pzErr);
int doltliteIgnoreSetCheck(const DoltliteIgnoreSet*, const char *zTable,
                           int *pIgnored, char **pzErr);
void doltliteIgnoreSetFree(DoltliteIgnoreSet*);
int doltliteIgnoreRegister(sqlite3 *db);

#endif
