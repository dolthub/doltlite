
#ifndef DOLTLITE_IGNORE_H
#define DOLTLITE_IGNORE_H

typedef struct sqlite3 sqlite3;

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
**                        (either no match, the winning pattern says
**                        ignored=0, or dolt_ignore doesn't exist yet
**                        — no table, no patterns, no filtering).
**                        1 means the table should be hidden.
**   SQLITE_CONSTRAINT  — conflicting patterns; *pzErr owned by caller
**                        via sqlite3_free (may be NULL if allocation
**                        failed).
**   other              — storage errors while reading dolt_ignore.
**
** dolt_ignore is a regular user table created by the user (or by
** seed-data on first use). No auto-materialization. */
int doltliteCheckIgnore(sqlite3 *db, const char *zTable,
                        int *pIgnored, char **pzErr);

#endif
