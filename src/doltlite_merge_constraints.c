
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_cursor.h"
#include "prolly_cache.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include "doltlite_constraint_violations.h"

#include <string.h>

/* Post-merge constraint detection.
**
** Called from doltliteMergeFunc AFTER the merged catalog has been
** installed into the session via doltliteSwitchCatalog, so
** sqlite_schema reflects the merged DDL and each Table* has its
** pFKey / pCheck / aIndex chains loaded.
**
** For now scoped to FK orphans only: for each table in the
** merged catalog with at least one foreign key, run PRAGMA
** foreign_key_check to get the orphan rowids, then walk the
** child table's prolly tree to find each orphan row's raw key
** and value bytes for persisting into
** dolt_constraint_violations_<table>.
**
** Unique-index and check-constraint violations follow in
** Phase 6 — they need diff-based detection because the merge
** already collapses the table into a single prolly tree and
** SQLite's PRAGMA integrity_check doesn't distinguish what was
** present on each side of the merge. */

/* Walk pRoot until we find the row whose intKey matches
** targetRowid. Copies the row's key + value bytes into the
** caller's out buffers (sqlite3_malloc). Returns SQLITE_NOTFOUND
** if no row matches, SQLITE_OK otherwise. */
static int fetchRowByRowid(
  ChunkStore *cs,
  ProllyCache *pCache,
  const ProllyHash *pRoot,
  u8 flags,
  i64 targetRowid,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  ProllyCursor cur;
  int res, rc;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;

  if( prollyHashIsEmpty(pRoot) ) return SQLITE_NOTFOUND;

  prollyCursorInit(&cur, cs, pCache, pRoot, flags);
  rc = prollyCursorFirst(&cur, &res);
  if( rc!=SQLITE_OK || res ){
    prollyCursorClose(&cur);
    return rc==SQLITE_OK ? SQLITE_NOTFOUND : rc;
  }

  while( prollyCursorIsValid(&cur) ){
    i64 rowid = prollyCursorIntKey(&cur);
    if( rowid == targetRowid ){
      const u8 *pKey, *pVal;
      int nKey, nVal;
      prollyCursorKey(&cur, &pKey, &nKey);
      prollyCursorValue(&cur, &pVal, &nVal);
      if( pKey && nKey > 0 ){
        *ppKey = sqlite3_malloc(nKey);
        if( !*ppKey ){
          prollyCursorClose(&cur);
          return SQLITE_NOMEM;
        }
        memcpy(*ppKey, pKey, nKey);
        *pnKey = nKey;
      }
      if( pVal && nVal > 0 ){
        *ppVal = sqlite3_malloc(nVal);
        if( !*ppVal ){
          sqlite3_free(*ppKey);
          *ppKey = 0; *pnKey = 0;
          prollyCursorClose(&cur);
          return SQLITE_NOMEM;
        }
        memcpy(*ppVal, pVal, nVal);
        *pnVal = nVal;
      }
      prollyCursorClose(&cur);
      return SQLITE_OK;
    }
    rc = prollyCursorNext(&cur);
    if( rc != SQLITE_OK ) break;
  }

  prollyCursorClose(&cur);
  return SQLITE_NOTFOUND;
}

/* Build the violation_info JSON string for one FK orphan. The
** shape deliberately matches Dolt's output format (same top-level
** keys so oracle tests and external consumers see one schema).
** Caller owns the returned string. */
static char *buildFkViolationInfo(
  sqlite3 *db,
  const char *zChildTable,
  int fkid
){
  sqlite3_stmt *pStmt = 0;
  sqlite3_str *pJson;
  sqlite3_str *pCols;
  sqlite3_str *pRefCols;
  char *zColsBuf = 0;
  char *zRefColsBuf = 0;
  char *zParentBuf = 0;
  char *zOnUpBuf = 0;
  char *zOnDelBuf = 0;
  char *zQuery;
  char *zResult;
  int rc;
  int nMatches = 0;
  int fatal = 0;

  /* sqlite3_str_new() always returns a non-NULL handle; OOM is
  ** surfaced later via sqlite3_str_errcode() / the final
  ** sqlite3_str_finish() returning NULL. */
  pJson = sqlite3_str_new(0);
  pCols = sqlite3_str_new(0);
  pRefCols = sqlite3_str_new(0);

  zQuery = sqlite3_mprintf("PRAGMA foreign_key_list(%Q)", zChildTable);
  if( !zQuery ){
    fatal = 1;
  }else{
    rc = sqlite3_prepare_v2(db, zQuery, -1, &pStmt, 0);
    sqlite3_free(zQuery);
    if( rc != SQLITE_OK ){
      fatal = 1;
    }else{
      /* Walk every row belonging to this fkid, accumulating the
      ** child column list and the referenced column list into
      ** separate buffers. Scalar fields (ReferencedTable /
      ** OnUpdate / OnDelete) only need the first matching row —
      ** PRAGMA foreign_key_list repeats them identically for each
      ** column of a composite FK. */
      while( sqlite3_step(pStmt) == SQLITE_ROW ){
        int id = sqlite3_column_int(pStmt, 0);
        const char *zParent, *zFrom, *zTo, *zOnUp, *zOnDel;
        if( id != fkid ) continue;
        zParent = (const char*)sqlite3_column_text(pStmt, 2);
        zFrom   = (const char*)sqlite3_column_text(pStmt, 3);
        zTo     = (const char*)sqlite3_column_text(pStmt, 4);
        zOnUp   = (const char*)sqlite3_column_text(pStmt, 5);
        zOnDel  = (const char*)sqlite3_column_text(pStmt, 6);
        if( nMatches>0 ){
          sqlite3_str_appendall(pCols, ", ");
          sqlite3_str_appendall(pRefCols, ", ");
        }
        sqlite3_str_appendf(pCols, "\"%w\"", zFrom ? zFrom : "");
        sqlite3_str_appendf(pRefCols, "\"%w\"", zTo ? zTo : "");
        if( nMatches==0 ){
          if( zParent ) zParentBuf = sqlite3_mprintf("%s", zParent);
          zOnUpBuf  = sqlite3_mprintf("%s", zOnUp  ? zOnUp  : "NO ACTION");
          zOnDelBuf = sqlite3_mprintf("%s", zOnDel ? zOnDel : "NO ACTION");
        }
        nMatches++;
      }
    }
  }
  sqlite3_finalize(pStmt);

  /* Finalizing the two column-list builders releases them
  ** regardless of whether the walk ran. If OOM occurred during
  ** any append, finish returns NULL and the result propagates. */
  zColsBuf    = sqlite3_str_finish(pCols);
  zRefColsBuf = sqlite3_str_finish(pRefCols);

  if( fatal ){
    /* Fail the whole object rather than emit a degraded JSON
    ** with empty columns — the caller treats NULL as "no info". */
    sqlite3_free(sqlite3_str_finish(pJson));
    sqlite3_free(zColsBuf);
    sqlite3_free(zRefColsBuf);
    sqlite3_free(zParentBuf);
    sqlite3_free(zOnUpBuf);
    sqlite3_free(zOnDelBuf);
    return 0;
  }

  sqlite3_str_appendall(pJson, "{");
  sqlite3_str_appendf(pJson,
      "\"Columns\": [%s], \"ReferencedTable\": \"%w\", "
      "\"ReferencedColumns\": [%s], "
      "\"OnUpdate\": \"%w\", \"OnDelete\": \"%w\"}",
      zColsBuf ? zColsBuf : "",
      zParentBuf ? zParentBuf : "",
      zRefColsBuf ? zRefColsBuf : "",
      zOnUpBuf ? zOnUpBuf : "NO ACTION",
      zOnDelBuf ? zOnDelBuf : "NO ACTION");
  zResult = sqlite3_str_finish(pJson);
  sqlite3_free(zColsBuf);
  sqlite3_free(zRefColsBuf);
  sqlite3_free(zParentBuf);
  sqlite3_free(zOnUpBuf);
  sqlite3_free(zOnDelBuf);
  return zResult;
}

/* Look up (zTable, rowid) in a previously-loaded ancestor catalog
** and return SQLITE_OK with the row's raw pVal bytes if the row
** existed in the ancestor at the same rowid, SQLITE_NOTFOUND
** otherwise. `aAnc`/`nAnc` are the result of
** doltliteLoadCatalog(&ancCatHash). The caller owns *ppAncVal on
** success. Used by the post-merge walkers to decide whether a
** candidate violation is merge-introduced (row changed or new)
** or pre-existing (row unchanged since ancestor). */
static int fetchAncestorRowByName(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  i64 rowid,
  u8 **ppAncVal, int *pnAncVal
){
  ChunkStore *cs;
  ProllyCache *pCache;
  struct TableEntry *pTE;
  u8 *pAncKey = 0; int nAncKey = 0;
  int rc;

  *ppAncVal = 0;
  *pnAncVal = 0;

  if( !aAnc || nAnc==0 ) return SQLITE_NOTFOUND;

  cs = doltliteGetChunkStore(db);
  pCache = doltliteGetCache(db);
  if( !cs || !pCache ) return SQLITE_ERROR;

  pTE = doltliteFindTableByName(aAnc, nAnc, zTable);
  if( !pTE ) return SQLITE_NOTFOUND;

  rc = fetchRowByRowid(cs, pCache, &pTE->root, pTE->flags, rowid,
                       &pAncKey, &nAncKey, ppAncVal, pnAncVal);
  sqlite3_free(pAncKey);
  return rc;
}

/* Decide whether a candidate violation is pre-existing: the row
** exists in the ancestor catalog at the same rowid with byte-for-
** byte identical value payload. Returns 1 if pre-existing (the
** caller should skip flagging), 0 if merge-introduced or the
** ancestor state can't be consulted. Empty ancCatHash short-
** circuits to 0 (no ancestor — treat every violation as fresh).
**
** Known hole: this rule is the "child row unchanged" filter
** only. It does not catch the case where a parent row is
** deleted on one side while the child row is unchanged — in
** that scenario the merge DID introduce the orphan, but this
** filter will still flag it because the child is unchanged.
** That is a strict improvement over the prior state where every
** pre-existing orphan blocked every unrelated merge, and can be
** tightened later (see GitHub issue for the semantics-drift
** tracking item). */
static int isRowPreExisting(
  const u8 *pMergedVal, int nMergedVal,
  const u8 *pAncVal, int nAncVal
){
  if( !pMergedVal || !pAncVal ) return 0;
  if( nMergedVal != nAncVal ) return 0;
  if( nMergedVal == 0 ) return 1;
  return memcmp(pMergedVal, pAncVal, nMergedVal)==0 ? 1 : 0;
}

/* Resolve a (zTable, rowid) pair to the raw prolly row by looking
** the table up in the current btree's catalog via the public
** doltliteGetSessionTableRoot accessor and walking its prolly
** root to find the matching key. */
static int fetchOrphanRow(
  sqlite3 *db,
  const char *zTable,
  i64 rowid,
  u8 **ppKey, int *pnKey,
  u8 **ppVal, int *pnVal
){
  ChunkStore *cs;
  ProllyCache *pCache;
  ProllyHash root;
  u8 flags = 0;
  Pgno iTable;
  int rc;

  *ppKey = 0; *pnKey = 0;
  *ppVal = 0; *pnVal = 0;

  cs = doltliteGetChunkStore(db);
  pCache = doltliteGetCache(db);
  if( !cs || !pCache ) return SQLITE_ERROR;

  rc = doltliteResolveTableName(db, zTable, &iTable);
  if( rc != SQLITE_OK ) return rc;

  rc = doltliteGetSessionTableRoot(db, iTable, &root, &flags);
  if( rc != SQLITE_OK ) return rc;

  return fetchRowByRowid(cs, pCache, &root, flags, rowid,
                         ppKey, pnKey, ppVal, pnVal);
}

/* Walk every row in zTable and look for duplicate values on the
** given UNIQUE index columns. When a duplicate group is found,
** the row with the lowest rowid is kept (treated as the "main
** side" row) and every other row in the group is evicted from
** the base table into dolt_constraint_violations_<table> with
** violation_type = 'unique index'.
**
** We can't just `GROUP BY` the index columns here — SQLite's
** query planner sees the UNIQUE constraint and optimizes on the
** assumption that each value appears at most once, collapsing
** duplicate rows that our prolly-level merge introduced. Force
** a full table scan with the `NOT INDEXED` clause and do the
** grouping ourselves in the driver. */
static int detectUniqueViolationsForIndex(
  sqlite3 *db,
  struct TableEntry *aAnc, int nAnc,
  const char *zTable,
  const char *zIndexName,
  const char *zCols,
  int *pnFound
){
  sqlite3_stmt *pScan = 0;
  char *zQuery;
  char *zWinnerKey = 0;
  int rc;

  zQuery = sqlite3_mprintf(
    "SELECT rowid, %s FROM \"%w\" NOT INDEXED ORDER BY %s, rowid",
    zCols, zTable, zCols);
  if( !zQuery ) return SQLITE_NOMEM;
  rc = sqlite3_prepare_v2(db, zQuery, -1, &pScan, 0);
  sqlite3_free(zQuery);
  if( rc != SQLITE_OK ) return rc;

  while( (rc = sqlite3_step(pScan)) == SQLITE_ROW ){
    sqlite3_int64 rowid = sqlite3_column_int64(pScan, 0);
    int nc = sqlite3_column_count(pScan);
    int i;
    sqlite3_str *pS = sqlite3_str_new(0);
    char *zRowKey;
    int isDup;

    for(i=1; i<nc; i++){
      const char *zV = (const char*)sqlite3_column_text(pScan, i);
      int type = sqlite3_column_type(pScan, i);
      if( type == SQLITE_NULL ){
        sqlite3_str_appendf(pS, "%sNULL", i>1?"|":"");
      }else{
        sqlite3_str_appendf(pS, "%s%Q", i>1?"|":"", zV ? zV : "");
      }
    }
    zRowKey = sqlite3_str_finish(pS);
    if( !zRowKey ){ rc = SQLITE_NOMEM; break; }

    isDup = zWinnerKey && strcmp(zWinnerKey, zRowKey)==0;
    if( !isDup ){
      /* New group — this row wins, remember its value set. */
      sqlite3_free(zWinnerKey);
      zWinnerKey = zRowKey;
      continue;
    }
    sqlite3_free(zRowKey);

    /* Evict the loser row. */
    {
      u8 *pKey = 0; int nKey = 0;
      u8 *pVal = 0; int nVal = 0;
      char *zInfo;
      int appendRc;

      rc = fetchOrphanRow(db, zTable, rowid, &pKey, &nKey, &pVal, &nVal);
      if( rc == SQLITE_NOTFOUND ){ rc = SQLITE_OK; continue; }
      if( rc != SQLITE_OK ) break;

      /* Skip the eviction if the loser row already existed in
      ** ancestor with identical bytes — the duplicate predates
      ** this merge and isn't ours to flag (or destroy). */
      if( aAnc ){
        u8 *pAncVal = 0; int nAncVal = 0;
        int ancRc = fetchAncestorRowByName(db, aAnc, nAnc, zTable,
                                            rowid, &pAncVal, &nAncVal);
        int preExisting = (ancRc==SQLITE_OK)
            && isRowPreExisting(pVal, nVal, pAncVal, nAncVal);
        sqlite3_free(pAncVal);
        if( preExisting ){
          sqlite3_free(pKey);
          sqlite3_free(pVal);
          continue;
        }
      }

      zInfo = sqlite3_mprintf(
          "{\"Columns\": [%s], \"Name\": \"%w\"}",
          zCols, zIndexName);
      appendRc = doltliteAppendConstraintViolation(
          db, zTable, DOLTLITE_CV_UNIQUE_INDEX,
          rowid, pKey, nKey, pVal, nVal, zInfo);
      sqlite3_free(zInfo);
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      if( appendRc != SQLITE_OK ){ rc = appendRc; break; }

      /* Evict via doltliteApplyRawRowMutation — goes straight
      ** through the prolly layer, unlike SQL DELETE which hits
      ** the mid-merge btree state and returns SQLITE_CORRUPT. */
      rc = doltliteApplyRawRowMutation(db, zTable, pKey, nKey, rowid, 0, 0);
      if( rc != SQLITE_OK ) break;

      if( pnFound ) (*pnFound)++;
    }
  }
  sqlite3_free(zWinnerKey);
  if( rc == SQLITE_DONE ) rc = SQLITE_OK;
  sqlite3_finalize(pScan);
  return rc;
}

/* For every user table in the session's catalog, walk its
** UNIQUE indexes via PRAGMA index_list / PRAGMA index_xinfo
** and feed each into detectUniqueViolationsForIndex. The
** ancestor catalog is loaded once and shared across all
** per-index scans so repeated lookups are cheap. */
int doltliteDetectMergeUniqueViolations(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  int *pnFound
){
  sqlite3_stmt *pTbls = 0;
  struct TableEntry *aAnc = 0;
  int nAnc = 0;
  Pgno iNextAnc = 0;
  int haveAnc = 0;
  int rc;

  if( pnFound ) *pnFound = 0;

  if( pAncCatHash && !prollyHashIsEmpty(pAncCatHash) ){
    if( doltliteLoadCatalog(db, pAncCatHash, &aAnc, &nAnc, &iNextAnc)==SQLITE_OK ){
      haveAnc = 1;
    }
  }

  rc = sqlite3_prepare_v2(db,
      "SELECT name FROM sqlite_master WHERE type='table' "
      "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%'",
      -1, &pTbls, 0);
  if( rc != SQLITE_OK ){
    if( haveAnc ) doltliteFreeCatalog(aAnc, nAnc);
    return rc;
  }

  while( (rc = sqlite3_step(pTbls)) == SQLITE_ROW ){
    const char *zTableRaw = (const char*)sqlite3_column_text(pTbls, 0);
    char *zTable;
    sqlite3_stmt *pIdxList = 0;
    char *zIdxQ;

    if( !zTableRaw ) continue;
    zTable = sqlite3_mprintf("%s", zTableRaw);
    if( !zTable ){ rc = SQLITE_NOMEM; break; }

    zIdxQ = sqlite3_mprintf("PRAGMA index_list(%Q)", zTable);
    if( !zIdxQ ){ sqlite3_free(zTable); rc = SQLITE_NOMEM; break; }
    rc = sqlite3_prepare_v2(db, zIdxQ, -1, &pIdxList, 0);
    sqlite3_free(zIdxQ);
    if( rc != SQLITE_OK ){ sqlite3_free(zTable); break; }

    while( sqlite3_step(pIdxList) == SQLITE_ROW ){
      int unique = sqlite3_column_int(pIdxList, 2);
      const char *zIdxRaw;
      const char *zOrigin;
      char *zIdx;
      char *zColsQ;
      sqlite3_stmt *pCols = 0;
      sqlite3_str *pColList;
      char *zColList;
      int idxRc;

      if( !unique ) continue;
      zIdxRaw = (const char*)sqlite3_column_text(pIdxList, 1);
      if( !zIdxRaw ) continue;

      /* Skip the primary-key auto-index — walking it would
      ** double-count every row as its own duplicate. Use the
      ** origin column (pk / u / c) instead of pattern-matching
      ** the name so WITHOUT ROWID autoindexes get handled
      ** correctly regardless of numbering. */
      zOrigin = (const char*)sqlite3_column_text(pIdxList, 3);
      if( zOrigin && strcmp(zOrigin, "pk")==0 ) continue;

      zIdx = sqlite3_mprintf("%s", zIdxRaw);
      if( !zIdx ) break;
      zColsQ = sqlite3_mprintf("PRAGMA index_xinfo(%Q)", zIdx);
      if( !zColsQ ){ sqlite3_free(zIdx); break; }
      idxRc = sqlite3_prepare_v2(db, zColsQ, -1, &pCols, 0);
      sqlite3_free(zColsQ);
      if( idxRc != SQLITE_OK ){ sqlite3_free(zIdx); continue; }

      pColList = sqlite3_str_new(0);
      while( sqlite3_step(pCols) == SQLITE_ROW ){
        int cno = sqlite3_column_int(pCols, 1);
        const char *zCol = (const char*)sqlite3_column_text(pCols, 2);
        if( cno < 0 || !zCol ) continue;
        if( sqlite3_str_length(pColList) > 0 ){
          sqlite3_str_appendall(pColList, ", ");
        }
        sqlite3_str_appendf(pColList, "\"%w\"", zCol);
      }
      sqlite3_finalize(pCols);
      zColList = sqlite3_str_finish(pColList);
      if( zColList && *zColList ){
        rc = detectUniqueViolationsForIndex(db, aAnc, nAnc, zTable,
                                             zIdx, zColList, pnFound);
      }
      sqlite3_free(zColList);
      sqlite3_free(zIdx);
      if( rc != SQLITE_OK ) break;
    }

    sqlite3_finalize(pIdxList);
    sqlite3_free(zTable);
    if( rc != SQLITE_OK ) break;
  }
  if( rc == SQLITE_DONE ) rc = SQLITE_OK;
  sqlite3_finalize(pTbls);
  return rc;
}

/* Extract the next CHECK (expr) clause starting at or after
** *pOffset in zSql. On success returns 1 and fills *pzExpr with
** a freshly-allocated inner expression and *pzName with the
** constraint name (if preceded by `CONSTRAINT <name>`) or NULL.
** Advances *pOffset past the closing paren. Returns 0 at end of
** string. Returns -1 on malformed input.
**
** Simple state machine: skip forward to `CHECK` (case-insensitive),
** note any CONSTRAINT clause preceding it, then walk balanced
** parens to find the end of the expression. Handles quoted
** identifiers and string literals so CHECK (name = 'silly)name')
** doesn't get chopped at the first `)`. */
static int nextCheckClause(
  const char *zSql, int *pOffset, char **pzExpr, char **pzName
){
  const char *p = zSql + *pOffset;
  const char *pEnd;
  char lastConstraintName[128] = {0};
  int depth;
  const char *pExprStart;

  *pzExpr = 0;
  *pzName = 0;

  while( *p ){
    /* Track a `CONSTRAINT <name>` clause so we can attach it to
    ** the next CHECK that shows up. */
    if( (p[0]=='C' || p[0]=='c')
     && strncasecmp(p, "CONSTRAINT", 10)==0
     && (p[10]==' ' || p[10]=='\t' || p[10]=='\n') ){
      int i = 0;
      p += 10;
      while( *p==' ' || *p=='\t' || *p=='\n' ) p++;
      while( *p && *p!=' ' && *p!='\t' && *p!='\n' && *p!='(' && i<127 ){
        lastConstraintName[i++] = *p++;
      }
      lastConstraintName[i] = 0;
      continue;
    }
    if( (p[0]=='C' || p[0]=='c')
     && strncasecmp(p, "CHECK", 5)==0
     && (p[5]==' ' || p[5]=='\t' || p[5]=='(' || p[5]=='\n') ){
      p += 5;
      while( *p==' ' || *p=='\t' || *p=='\n' ) p++;
      if( *p!='(' ){ p++; lastConstraintName[0] = 0; continue; }
      p++;
      pExprStart = p;
      depth = 1;
      while( *p && depth>0 ){
        char c = *p;
        if( c=='\'' ){
          p++;
          while( *p && !(*p=='\'' && p[1]!='\'') ){
            if( *p=='\'' && p[1]=='\'' ) p++;
            p++;
          }
          if( *p=='\'' ) p++;
          continue;
        }
        if( c=='"' ){
          p++;
          while( *p && *p!='"' ) p++;
          if( *p=='"' ) p++;
          continue;
        }
        if( c=='(' ) depth++;
        else if( c==')' ) depth--;
        if( depth>0 ) p++;
      }
      if( depth!=0 ) return -1;
      pEnd = p;
      p++;
      *pzExpr = sqlite3_malloc((int)(pEnd - pExprStart) + 1);
      if( !*pzExpr ) return -1;
      memcpy(*pzExpr, pExprStart, (size_t)(pEnd - pExprStart));
      (*pzExpr)[pEnd - pExprStart] = 0;
      if( lastConstraintName[0] ){
        *pzName = sqlite3_mprintf("%s", lastConstraintName);
      }
      *pOffset = (int)(p - zSql);
      return 1;
    }
    if( *p=='\'' ){
      p++;
      while( *p && !(*p=='\'' && p[1]!='\'') ){
        if( *p=='\'' && p[1]=='\'' ) p++;
        p++;
      }
      if( *p=='\'' ) p++;
      continue;
    }
    if( *p=='"' ){
      p++;
      while( *p && *p!='"' ) p++;
      if( *p=='"' ) p++;
      continue;
    }
    /* Reset a pending CONSTRAINT name if we hit a comma without
    ** seeing CHECK — that clause was something else (UNIQUE,
    ** FOREIGN KEY, etc). */
    if( *p==',' ) lastConstraintName[0] = 0;
    p++;
  }
  *pOffset = (int)(p - zSql);
  return 0;
}

/* For each user table, parse its CREATE TABLE DDL for CHECK
** constraints and run SELECT rowid WHERE NOT (expr) to find any
** row in the merged result that doesn't satisfy one. Emit each
** as a 'check constraint' violation. Unlike unique detection we
** keep the row in the base table — Dolt's CHECK semantics leave
** violating rows present and block commit until resolved.
** Ancestor filter is the same shape as the FK walker: any
** violating row that already existed unchanged in ancestor was
** already broken before the merge started and is skipped. */
int doltliteDetectMergeCheckViolations(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  int *pnFound
){
  sqlite3_stmt *pTbls = 0;
  struct TableEntry *aAnc = 0;
  int nAnc = 0;
  Pgno iNextAnc = 0;
  int haveAnc = 0;
  int rc;
  int stepRc;

  if( pnFound ) *pnFound = 0;

  if( pAncCatHash && !prollyHashIsEmpty(pAncCatHash) ){
    if( doltliteLoadCatalog(db, pAncCatHash, &aAnc, &nAnc, &iNextAnc)==SQLITE_OK ){
      haveAnc = 1;
    }
  }

  rc = sqlite3_prepare_v2(db,
      "SELECT name, sql FROM sqlite_master WHERE type='table' "
      "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'dolt_%'",
      -1, &pTbls, 0);
  if( rc != SQLITE_OK ){
    if( haveAnc ) doltliteFreeCatalog(aAnc, nAnc);
    return rc;
  }

  while( (stepRc = sqlite3_step(pTbls)) == SQLITE_ROW ){
    const char *zTableRaw = (const char*)sqlite3_column_text(pTbls, 0);
    const char *zSqlRaw   = (const char*)sqlite3_column_text(pTbls, 1);
    char *zTable;
    char *zSql;
    int offset = 0;

    if( !zTableRaw || !zSqlRaw ) continue;
    zTable = sqlite3_mprintf("%s", zTableRaw);
    zSql   = sqlite3_mprintf("%s", zSqlRaw);
    if( !zTable || !zSql ){
      sqlite3_free(zTable);
      sqlite3_free(zSql);
      rc = SQLITE_NOMEM;
      break;
    }

    for(;;){
      char *zExpr = 0;
      char *zCkName = 0;
      int clauseRc = nextCheckClause(zSql, &offset, &zExpr, &zCkName);
      char *zQuery;
      sqlite3_stmt *pQ = 0;
      int prepareRc;

      if( clauseRc <= 0 ){
        sqlite3_free(zExpr);
        sqlite3_free(zCkName);
        break;
      }

      zQuery = sqlite3_mprintf(
          "SELECT rowid FROM \"%w\" NOT INDEXED WHERE NOT (%s)",
          zTable, zExpr);
      if( !zQuery ){
        sqlite3_free(zExpr);
        sqlite3_free(zCkName);
        rc = SQLITE_NOMEM;
        break;
      }
      prepareRc = sqlite3_prepare_v2(db, zQuery, -1, &pQ, 0);
      sqlite3_free(zQuery);
      if( prepareRc != SQLITE_OK ){
        sqlite3_free(zExpr);
        sqlite3_free(zCkName);
        continue;
      }

      while( sqlite3_step(pQ) == SQLITE_ROW ){
        sqlite3_int64 rowid = sqlite3_column_int64(pQ, 0);
        u8 *pKey = 0; int nKey = 0;
        u8 *pVal = 0; int nVal = 0;
        char *zInfo;
        int appendRc;

        rc = fetchOrphanRow(db, zTable, rowid, &pKey, &nKey, &pVal, &nVal);
        if( rc == SQLITE_NOTFOUND ){ rc = SQLITE_OK; continue; }
        if( rc != SQLITE_OK ){
          sqlite3_free(pKey);
          sqlite3_free(pVal);
          break;
        }

        if( haveAnc ){
          u8 *pAncVal = 0; int nAncVal = 0;
          int ancRc = fetchAncestorRowByName(db, aAnc, nAnc, zTable,
                                              rowid, &pAncVal, &nAncVal);
          int preExisting = (ancRc==SQLITE_OK)
              && isRowPreExisting(pVal, nVal, pAncVal, nAncVal);
          sqlite3_free(pAncVal);
          if( preExisting ){
            sqlite3_free(pKey);
            sqlite3_free(pVal);
            continue;
          }
        }

        zInfo = sqlite3_mprintf(
            "{\"Name\": \"%w\", \"Expression\": \"%w\"}",
            zCkName ? zCkName : "", zExpr);
        appendRc = doltliteAppendConstraintViolation(
            db, zTable, DOLTLITE_CV_CHECK_CONSTRAINT,
            rowid, pKey, nKey, pVal, nVal, zInfo);
        sqlite3_free(zInfo);
        sqlite3_free(pKey);
        sqlite3_free(pVal);
        if( appendRc != SQLITE_OK ){ rc = appendRc; break; }
        if( pnFound ) (*pnFound)++;
      }
      sqlite3_finalize(pQ);
      sqlite3_free(zExpr);
      sqlite3_free(zCkName);
      if( rc != SQLITE_OK ) break;
    }

    sqlite3_free(zTable);
    sqlite3_free(zSql);
    if( rc != SQLITE_OK ) break;
  }
  if( rc == SQLITE_OK && stepRc != SQLITE_DONE && stepRc != SQLITE_ROW ){
    rc = stepRc;
  }
  sqlite3_finalize(pTbls);
  if( haveAnc ) doltliteFreeCatalog(aAnc, nAnc);
  return rc;
}

/* Entry point. Runs PRAGMA foreign_key_check to find every
** orphan row across all tables, fetches each row's raw payload
** from the prolly store, and appends one violation per orphan
** into the session-scoped dolt_constraint_violations blob.
**
** `pAncCatHash` points at the three-way-merge ancestor catalog
** hash. For each candidate orphan the walker asks whether the
** same row existed unchanged in ancestor — if so, the row was
** already an orphan before either side started and the merge
** is not introducing anything new, so it is skipped.
**
** Passing an empty/zero hash disables the filter entirely, which
** is fine for call sites that don't have a three-way ancestor
** (e.g. cherry-pick onto empty). Returns the number of violations
** appended via *pnFound. Returns SQLITE_OK on success. */
int doltliteDetectMergeFkViolations(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  int *pnFound
){
  sqlite3_stmt *pStmt = 0;
  struct TableEntry *aAnc = 0;
  int nAnc = 0;
  Pgno iNextAnc = 0;
  int haveAnc = 0;
  int rc;
  int nFound = 0;

  if( pnFound ) *pnFound = 0;

  if( pAncCatHash && !prollyHashIsEmpty(pAncCatHash) ){
    if( doltliteLoadCatalog(db, pAncCatHash, &aAnc, &nAnc, &iNextAnc)==SQLITE_OK ){
      haveAnc = 1;
    }
  }

  rc = sqlite3_prepare_v2(db, "PRAGMA foreign_key_check", -1, &pStmt, 0);
  if( rc != SQLITE_OK ){
    if( haveAnc ) doltliteFreeCatalog(aAnc, nAnc);
    return rc;
  }

  while( (rc = sqlite3_step(pStmt)) == SQLITE_ROW ){
    const char *zTable = (const char*)sqlite3_column_text(pStmt, 0);
    i64 rowid;
    int fkid;
    u8 *pKey = 0; int nKey = 0;
    u8 *pVal = 0; int nVal = 0;
    char *zInfo;
    int appendRc;

    if( !zTable ) continue;
    if( sqlite3_column_type(pStmt, 1) == SQLITE_NULL ) continue;
    rowid = sqlite3_column_int64(pStmt, 1);
    fkid  = sqlite3_column_int(pStmt, 3);

    /* zTable pointer is owned by the statement cursor and goes
    ** stale on the next step — dup it so we can close the
    ** pragma statement before running our own prepared queries. */
    {
      char *zTableCopy = sqlite3_mprintf("%s", zTable);
      if( !zTableCopy ){ rc = SQLITE_NOMEM; break; }

      rc = fetchOrphanRow(db, zTableCopy, rowid, &pKey, &nKey, &pVal, &nVal);
      if( rc == SQLITE_NOTFOUND ){
        /* Row may have been deleted between PRAGMA and fetch —
        ** treat as no longer a violation and skip. */
        sqlite3_free(zTableCopy);
        rc = SQLITE_OK;
        continue;
      }
      if( rc != SQLITE_OK ){
        sqlite3_free(zTableCopy);
        break;
      }

      if( haveAnc ){
        u8 *pAncVal = 0; int nAncVal = 0;
        int ancRc = fetchAncestorRowByName(db, aAnc, nAnc, zTableCopy,
                                            rowid, &pAncVal, &nAncVal);
        int preExisting = (ancRc==SQLITE_OK)
            && isRowPreExisting(pVal, nVal, pAncVal, nAncVal);
        sqlite3_free(pAncVal);
        if( preExisting ){
          sqlite3_free(pKey);
          sqlite3_free(pVal);
          sqlite3_free(zTableCopy);
          continue;
        }
      }

      zInfo = buildFkViolationInfo(db, zTableCopy, fkid);
      appendRc = doltliteAppendConstraintViolation(
          db, zTableCopy, DOLTLITE_CV_FOREIGN_KEY,
          rowid, pKey, nKey, pVal, nVal, zInfo);
      sqlite3_free(zInfo);
      sqlite3_free(pKey);
      sqlite3_free(pVal);
      sqlite3_free(zTableCopy);
      if( appendRc != SQLITE_OK ){ rc = appendRc; break; }
      nFound++;
    }
  }
  if( rc == SQLITE_DONE ) rc = SQLITE_OK;

  sqlite3_finalize(pStmt);
  if( haveAnc ) doltliteFreeCatalog(aAnc, nAnc);
  if( pnFound ) *pnFound = nFound;
  return rc;
}

#endif
