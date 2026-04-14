
#ifndef DOLTLITE_IGNORE_H
#define DOLTLITE_IGNORE_H

typedef struct sqlite3 sqlite3;

/* Create dolt_ignore if it doesn't already exist. Idempotent. Call
** before the initial seed commit so the table is part of the repo
** from commit 0 with schema-only content. */
int doltliteEnsureIgnoreTable(sqlite3 *db);

/* Look up zTable against the patterns in dolt_ignore and decide
** whether it should be hidden from status/add.
**
** Pattern syntax:
**   '*' or '%'  → zero or more characters
**   '?'         → exactly one character
**   anything else (including '_') → literal
**
** Among matching patterns the most specific wins (longest
** literal-character count). If two equally-specific patterns
** disagree on `ignored`, this is a conflict and returns
** SQLITE_CONSTRAINT with a descriptive error in *pzErr.
**
** Returns:
**   SQLITE_OK          — *pIgnored set to 0 or 1. 0 means not ignored
**                        (either no match or the winning pattern says
**                        ignored=0). 1 means the table should be
**                        hidden.
**   SQLITE_CONSTRAINT  — conflicting patterns; *pzErr owned by caller
**                        via sqlite3_free (may be NULL if allocation
**                        failed).
**   other              — storage errors while reading dolt_ignore.
**
** If dolt_ignore doesn't exist yet (legacy pre-0.7.3 repos re-opened
** by a new binary), returns SQLITE_OK with *pIgnored=0 — no patterns,
** no filtering. */
int doltliteCheckIgnore(sqlite3 *db, const char *zTable,
                        int *pIgnored, char **pzErr);

#endif
