
#ifndef DOLTLITE_INTERNAL_H
#define DOLTLITE_INTERNAL_H

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "prolly_hashset.h"
#include "doltlite_commit.h"
#include "doltlite_name_index.h"
#include "chunk_store.h"
#include "doltlite_catalog_types.h"
#include <time.h>
#include <ctype.h>
#include <limits.h>

typedef struct BtShared BtShared;
typedef struct ProllyCache ProllyCache;
typedef struct ProllyMutMap ProllyMutMap;
typedef struct DoltliteTxnState DoltliteTxnState;
typedef struct DoltlitePkRange DoltlitePkRange;
typedef struct DoltliteCommitQueue DoltliteCommitQueue;

#define DOLTLITE_RANGE_NONE      0
#define DOLTLITE_RANGE_TWO_DOT   2
#define DOLTLITE_RANGE_THREE_DOT 3

/* Commands must not run from persistent schema objects. */
#define DOLTLITE_COMMAND_FUNC_FLAGS (SQLITE_UTF8 | SQLITE_DIRECTONLY)

static SQLITE_INLINE int doltliteSplitRevisionRange(
  const char *zSpec,
  char **pzLeft,
  char **pzRight,
  int *pRangeType
){
  const char *zSep;
  int nLeft;
  int nSep;

  *pzLeft = 0;
  *pzRight = 0;
  *pRangeType = DOLTLITE_RANGE_NONE;
  if( !zSpec ) return SQLITE_MISMATCH;

  zSep = strstr(zSpec, "...");
  nSep = 3;
  if( !zSep ){
    zSep = strstr(zSpec, "..");
    nSep = 2;
  }
  if( !zSep ) return SQLITE_NOTFOUND;
  if( strstr(zSep + nSep, "..") ) return SQLITE_ERROR;

  nLeft = (int)(zSep - zSpec);
  if( nLeft==0 || zSep[nSep]==0 ) return SQLITE_ERROR;
  *pzLeft = sqlite3_mprintf("%.*s", nLeft, zSpec);
  *pzRight = sqlite3_mprintf("%s", zSep + nSep);
  if( !*pzLeft || !*pzRight ){
    sqlite3_free(*pzLeft);
    sqlite3_free(*pzRight);
    *pzLeft = 0;
    *pzRight = 0;
    return SQLITE_NOMEM;
  }
  *pRangeType = nSep;
  return SQLITE_OK;
}

struct DoltlitePkRange {
  i64 pkLo;
  i64 pkHi;
  int hasPkLo;
  int hasPkHi;
  int pkLoStrict;
  int pkHiStrict;
  int isEmpty;            /* NULL bound: scan matches nothing. */
};

struct DoltliteCommitQueue {
  ProllyHash *aQueue;
  int qHead, qTail, qAlloc;
  ProllyHashSet visited, queued;
  int visitedInit, queuedInit;
};

static SQLITE_INLINE void doltliteCommitQueueClear(DoltliteCommitQueue *q){
  sqlite3_free(q->aQueue);
  q->aQueue = 0;
  q->qHead = q->qTail = q->qAlloc = 0;
  if( q->visitedInit ){
    prollyHashSetFree(&q->visited);
    q->visitedInit = 0;
  }
  if( q->queuedInit ){
    prollyHashSetFree(&q->queued);
    q->queuedInit = 0;
  }
}

static SQLITE_INLINE int doltliteCommitQueueEnqueue(
  DoltliteCommitQueue *q,
  const ProllyHash *pHash
){
  int rc;
  if( !pHash || prollyHashIsEmpty(pHash) ) return SQLITE_OK;
  if( prollyHashSetContains(&q->visited, pHash) ) return SQLITE_OK;
  if( prollyHashSetContains(&q->queued, pHash) ) return SQLITE_OK;
  if( q->qTail >= q->qAlloc ){
    i64 nNew = q->qAlloc ? (i64)q->qAlloc * 2 : (i64)16;
    ProllyHash *tmp;
    if( nNew > (i64)0x7fffffff/(i64)sizeof(ProllyHash) ) return SQLITE_NOMEM;
    tmp = sqlite3_realloc(q->aQueue, (int)(nNew * (i64)sizeof(ProllyHash)));
    if( !tmp ) return SQLITE_NOMEM;
    q->aQueue = tmp;
    q->qAlloc = (int)nNew;
  }
  q->aQueue[q->qTail++] = *pHash;
  rc = prollyHashSetAdd(&q->queued, pHash);
  return rc;
}

static SQLITE_INLINE int doltliteCommitQueueInit(
  DoltliteCommitQueue *q,
  const ProllyHash *pHead
){
  int rc;
  doltliteCommitQueueClear(q);
  rc = prollyHashSetInit(&q->visited, 64);
  if( rc!=SQLITE_OK ) return rc;
  q->visitedInit = 1;
  rc = prollyHashSetInit(&q->queued, 64);
  if( rc!=SQLITE_OK ){
    doltliteCommitQueueClear(q);
    return rc;
  }
  q->queuedInit = 1;
  rc = doltliteCommitQueueEnqueue(q, pHead);
  if( rc!=SQLITE_OK ) doltliteCommitQueueClear(q);
  return rc;
}

static SQLITE_INLINE int doltliteCommitQueueNext(
  DoltliteCommitQueue *q,
  ProllyHash *pHash,
  int *pHas
){
  int rc;
  *pHas = 0;
  while( q->qHead < q->qTail ){
    ProllyHash cur = q->aQueue[q->qHead++];
    if( prollyHashSetContains(&q->visited, &cur) ) continue;
    rc = prollyHashSetAdd(&q->visited, &cur);
    if( rc!=SQLITE_OK ) return rc;
    *pHash = cur;
    *pHas = 1;
    return SQLITE_OK;
  }
  return SQLITE_OK;
}

static SQLITE_INLINE int doltliteCommitQueueEnqueueParents(
  DoltliteCommitQueue *q,
  const DoltliteCommit *pCommit
){
  int i, rc;
  for(i=0; i<doltliteCommitParentCount(pCommit); i++){
    const ProllyHash *pParent = doltliteCommitParentHash(pCommit, i);
    rc = doltliteCommitQueueEnqueue(q, pParent);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

typedef enum DoltliteVcTxnMode DoltliteVcTxnMode;
enum DoltliteVcTxnMode {
  DOLTLITE_VC_TXN_PLAIN = 0,
  DOLTLITE_VC_TXN_AUTOCOMMIT_LIKE = 1,
  DOLTLITE_VC_TXN_NESTED_SAVEPOINT = 2
};

#define DOLTLITE_FNV1A_OFFSET 1469598103934665603ULL
#define DOLTLITE_FNV1A_PRIME  1099511628211ULL

static SQLITE_INLINE u64 doltliteFnv1aBytes(u64 h, const u8 *p, int n){
  int i;
  if( p ){
    for(i=0; i<n; i++){ h ^= (u64)p[i]; h *= DOLTLITE_FNV1A_PRIME; }
  }
  return h;
}

static SQLITE_INLINE u64 doltliteFnv1aI64(u64 h, i64 v){
  u64 k = (u64)v;
  int i;
  for(i=0; i<8; i++){ h ^= (k >> (i*8)) & 0xff; h *= DOLTLITE_FNV1A_PRIME; }
  return h;
}

static SQLITE_INLINE u64 doltliteFnv1aStr(u64 h, const char *z){
  if( z ){
    int i;
    for(i=0; z[i]; i++){ h ^= (u64)(u8)z[i]; h *= DOLTLITE_FNV1A_PRIME; }
  }
  return h;
}

static SQLITE_INLINE u64 doltliteFnv1aSep(u64 h){
  return h * DOLTLITE_FNV1A_PRIME;
}

struct DoltliteTxnState {
  ProllyHash refsHash;
  ProllyHash committedRefsHash;
  char *zSessionBranch;
  ProllyHash sessionHead;
  ProllyHash sessionStaged;
  ProllyHash sessionMergeCommit;
  ProllyHash sessionConflictsCatalog;
  ProllyHash sessionConstraintViolationsCatalog;
  ProllyHash sessionCatalogHash;
  u8 sessionIsMerging;
};

static SQLITE_INLINE struct TableEntry *doltliteFindTableByNumber(
  struct TableEntry *a, int n, Pgno iTable
){
  int i;
  for(i=0; i<n; i++){
    if( a[i].iTable==iTable ) return &a[i];
  }
  return 0;
}

static SQLITE_INLINE struct TableEntry *doltliteFindTableByName(
  struct TableEntry *a, int n, const char *zName
){
  int i;
  if( !zName ) return 0;
  for(i=0; i<n; i++){
    if( a[i].zName && strcmp(a[i].zName, zName)==0 ) return &a[i];
  }
  return 0;
}

static SQLITE_INLINE int doltliteBestIndexRefs(
  sqlite3_index_info *pInfo,
  int iFromCol,
  int iToCol,
  int iTableCol
){
  int i;
  int iFrom = -1;
  int iTo = -1;
  int iTbl = -1;
  int argvIdx = 1;

  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pInfo->aConstraint[i].iColumn==iFromCol ){
      iFrom = i;
    }else if( pInfo->aConstraint[i].iColumn==iToCol ){
      iTo = i;
    }else if( pInfo->aConstraint[i].iColumn==iTableCol ){
      iTbl = i;
    }
  }

  if( iFrom>=0 ){
    pInfo->aConstraintUsage[iFrom].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iFrom].omit = 1;
  }
  if( iTo>=0 ){
    pInfo->aConstraintUsage[iTo].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTo].omit = 1;
  }
  if( iTbl>=0 ){
    pInfo->aConstraintUsage[iTbl].argvIndex = argvIdx++;
    pInfo->aConstraintUsage[iTbl].omit = 1;
  }

  pInfo->idxNum = (iFrom>=0 ? 1 : 0) | (iTo>=0 ? 2 : 0) | (iTbl>=0 ? 4 : 0);
  pInfo->estimatedCost = 1000.0;
  return SQLITE_OK;
}

static SQLITE_INLINE int doltliteBestIndexEq(
  sqlite3_index_info *pInfo,
  int iColumn
){
  int i;
  for(i=0; i<pInfo->nConstraint; i++){
    if( !pInfo->aConstraint[i].usable ) continue;
    if( pInfo->aConstraint[i].op!=SQLITE_INDEX_CONSTRAINT_EQ ) continue;
    if( pInfo->aConstraint[i].iColumn!=iColumn ) continue;
    pInfo->aConstraintUsage[i].argvIndex = 1;
    pInfo->aConstraintUsage[i].omit = 1;
    pInfo->idxNum = 1;
    pInfo->estimatedCost = 1.0;
    pInfo->estimatedRows = 1;
    return SQLITE_OK;
  }
  pInfo->idxNum = 0;
  return SQLITE_OK;
}

static SQLITE_INLINE int doltliteBestIndexIntPkRange(
  sqlite3_index_info *pInfo,
  int iPkCol,
  int idxEq,
  int idxGe,
  int idxLe,
  int idxGt,
  int idxLt,
  double fullCost,
  sqlite3_int64 fullRows,
  double eqCost,
  sqlite3_int64 eqRows,
  double rangeCost,
  sqlite3_int64 rangeRows
){
  int iEq = -1, iGe = -1, iLe = -1, iGt = -1, iLt = -1;
  int i, nArg = 0, idxNum = 0;

  pInfo->estimatedCost = fullCost;
  pInfo->estimatedRows = fullRows;

  if( iPkCol < 0 ){
    pInfo->idxNum = 0;
    return SQLITE_OK;
  }

  for(i=0; i<pInfo->nConstraint; i++){
    const struct sqlite3_index_constraint *pC = &pInfo->aConstraint[i];
    if( !pC->usable ) continue;
    if( pC->iColumn != iPkCol ) continue;
    switch( pC->op ){
      case SQLITE_INDEX_CONSTRAINT_EQ: if( iEq<0 ) iEq = i; break;
      case SQLITE_INDEX_CONSTRAINT_GE: if( iGe<0 ) iGe = i; break;
      case SQLITE_INDEX_CONSTRAINT_LE: if( iLe<0 ) iLe = i; break;
      case SQLITE_INDEX_CONSTRAINT_GT: if( iGt<0 ) iGt = i; break;
      case SQLITE_INDEX_CONSTRAINT_LT: if( iLt<0 ) iLt = i; break;
      default: break;
    }
  }

  if( iEq >= 0 ){
    pInfo->aConstraintUsage[iEq].argvIndex = ++nArg;
    /* Apply to rendered values: visited key shape may not match the
    ** declared rowid alias. Seek stays a pushdown fast path. */
    pInfo->aConstraintUsage[iEq].omit = 0;
    idxNum |= idxEq;
    pInfo->estimatedCost = eqCost;
    pInfo->estimatedRows = eqRows;
  }else{
    if( iGe >= 0 ){
      pInfo->aConstraintUsage[iGe].argvIndex = ++nArg;
      pInfo->aConstraintUsage[iGe].omit = 0;
      idxNum |= idxGe;
    }
    if( iGt >= 0 ){
      pInfo->aConstraintUsage[iGt].argvIndex = ++nArg;
      pInfo->aConstraintUsage[iGt].omit = 0;
      idxNum |= idxGt;
    }
    if( iLe >= 0 ){
      pInfo->aConstraintUsage[iLe].argvIndex = ++nArg;
      pInfo->aConstraintUsage[iLe].omit = 0;
      idxNum |= idxLe;
    }
    if( iLt >= 0 ){
      pInfo->aConstraintUsage[iLt].argvIndex = ++nArg;
      pInfo->aConstraintUsage[iLt].omit = 0;
      idxNum |= idxLt;
    }
    if( idxNum != 0 ){
      pInfo->estimatedCost = rangeCost;
      pInfo->estimatedRows = rangeRows;
    }
  }

  pInfo->idxNum = idxNum;
  return SQLITE_OK;
}

static SQLITE_INLINE int doltlitePkRangeIntArg(
  sqlite3_value *pArg,
  i64 *pValue
){
  if( sqlite3_value_type(pArg)==SQLITE_NULL ) return -1;
  if( sqlite3_value_numeric_type(pArg)!=SQLITE_INTEGER ) return 0;
  *pValue = sqlite3_value_int64(pArg);
  return 1;
}

static SQLITE_INLINE void doltlitePkRangeFromArgs(
  int idxNum,
  int idxEq,
  int idxGe,
  int idxLe,
  int idxGt,
  int idxLt,
  int argc,
  sqlite3_value **argv,
  DoltlitePkRange *pRange
){
  int iArg = 0;
  int eArg;
  i64 v;
  memset(pRange, 0, sizeof(*pRange));

  if( idxNum & idxEq ){
    if( iArg < argc ){
      eArg = doltlitePkRangeIntArg(argv[iArg++], &v);
      if( eArg<0 ){
        pRange->isEmpty = 1;
        return;
      }
      if( eArg>0 ){
        pRange->pkLo = v;
        pRange->hasPkLo = 1;
      }
    }
  }else{
    if( idxNum & idxGe ){
      if( iArg < argc ){
        eArg = doltlitePkRangeIntArg(argv[iArg++], &v);
        if( eArg<0 ){
          pRange->isEmpty = 1;
          return;
        }
        if( eArg>0 ){
          pRange->pkLo = v;
          pRange->hasPkLo = 1;
          pRange->pkLoStrict = 0;
        }
      }
    }
    if( idxNum & idxGt ){
      if( iArg < argc ){
        eArg = doltlitePkRangeIntArg(argv[iArg++], &v);
        if( eArg<0 ){
          pRange->isEmpty = 1;
          return;
        }
        if( eArg>0 ){
          if( !pRange->hasPkLo || v >= pRange->pkLo ){
            pRange->pkLo = v;
            pRange->hasPkLo = 1;
            pRange->pkLoStrict = 1;
          }
        }
      }
    }
    if( idxNum & idxLe ){
      if( iArg < argc ){
        eArg = doltlitePkRangeIntArg(argv[iArg++], &v);
        if( eArg<0 ){
          pRange->isEmpty = 1;
          return;
        }
        if( eArg>0 ){
          pRange->pkHi = v;
          pRange->hasPkHi = 1;
          pRange->pkHiStrict = 0;
        }
      }
    }
    if( idxNum & idxLt ){
      if( iArg < argc ){
        eArg = doltlitePkRangeIntArg(argv[iArg++], &v);
        if( eArg<0 ){
          pRange->isEmpty = 1;
          return;
        }
        if( eArg>0 ){
          if( !pRange->hasPkHi || v <= pRange->pkHi ){
            pRange->pkHi = v;
            pRange->hasPkHi = 1;
            pRange->pkHiStrict = 1;
          }
        }
      }
    }
  }
}

static SQLITE_INLINE const char *doltliteDiffTypeNameFromPresence(
  int baseHas,
  int sideHas
){
  if( !sideHas ) return "removed";
  if( !baseHas ) return "added";
  return "modified";
}

static SQLITE_INLINE void doltliteResultTimestamp(sqlite3_context *ctx, i64 timestamp){
  time_t t = (time_t)timestamp;
  struct tm *tm = gmtime(&t);
  if( tm ){
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", tm);
    sqlite3_result_text(ctx, buf, -1, SQLITE_TRANSIENT);
  }else{
    sqlite3_result_null(ctx);
  }
}

static SQLITE_INLINE void doltliteRefResultError(
  sqlite3_context *ctx,
  int rc,
  const char *zNotFound,
  const char *zExists
){
  if( rc==SQLITE_NOTFOUND ){
    sqlite3_result_error(ctx, zNotFound, -1);
  }else if( rc==SQLITE_ERROR && zExists ){
    sqlite3_result_error(ctx, zExists, -1);
  }else{
    sqlite3_result_error(ctx, sqlite3_errstr(rc), -1);
  }
}

static SQLITE_INLINE int doltliteVtabDisconnect(sqlite3_vtab *pVtab){
  sqlite3_free(pVtab);
  return SQLITE_OK;
}

static SQLITE_INLINE int doltliteVtabClose(sqlite3_vtab_cursor *pCur){
  sqlite3_free(pCur);
  return SQLITE_OK;
}

static SQLITE_INLINE int doltliteVtabOpenCursor(
  sqlite3_vtab_cursor **ppCursor,
  int nByte
){
  sqlite3_vtab_cursor *pCur;
  pCur = sqlite3_malloc(nByte);
  if( !pCur ) return SQLITE_NOMEM;
  memset(pCur, 0, nByte);
  *ppCursor = pCur;
  return SQLITE_OK;
}

static SQLITE_INLINE int doltliteVtabConnectSimple(
  sqlite3 *db,
  const char *zSchema,
  int nByte,
  sqlite3_vtab **ppVtab
){
  sqlite3_vtab *pVtab;
  int rc = sqlite3_declare_vtab(db, zSchema);
  if( rc!=SQLITE_OK ) return rc;
  pVtab = sqlite3_malloc(nByte);
  if( !pVtab ) return SQLITE_NOMEM;
  memset(pVtab, 0, nByte);
  *ppVtab = pVtab;
  return SQLITE_OK;
}

/* Branch catalog: working-set catalog wins if recorded against this commit. */
static SQLITE_INLINE void doltliteResolveBranchEffectiveCatalog(
  ChunkStore *cs,
  const char *zBranch,
  const ProllyHash *pBranchCommit,
  const ProllyHash *pCommittedCatHash,
  ProllyHash *pCatHash
){
  ProllyHash wsCatHash, wsCommitHash;
  memset(&wsCatHash, 0, sizeof(wsCatHash));
  memset(&wsCommitHash, 0, sizeof(wsCommitHash));
  /* Unborn branch: working set matches the all-zero commit hash. */
  if( chunkStoreReadBranchWorkingCatalog(cs, zBranch, &wsCatHash, &wsCommitHash)==SQLITE_OK
   && (!prollyHashIsEmpty(&wsCommitHash) || prollyHashIsEmpty(pBranchCommit))
   && memcmp(wsCommitHash.data, pBranchCommit->data, PROLLY_HASH_SIZE)==0
   && memcmp(wsCatHash.data, pCommittedCatHash->data, PROLLY_HASH_SIZE)!=0 ){
    memcpy(pCatHash, &wsCatHash, sizeof(ProllyHash));
  }else{
    memcpy(pCatHash, pCommittedCatHash, sizeof(ProllyHash));
  }
}

/* True if the trimmed CREATE TABLE segment is a table-level constraint. */
static SQLITE_INLINE int doltliteSegmentIsTableConstraint(const char *s, int len){
  if( len>=11 && sqlite3_strnicmp(s, "PRIMARY KEY", 11)==0
      && (len==11 || !isalnum((unsigned char)s[11])) ) return 1;
  if( len>=6 && sqlite3_strnicmp(s, "UNIQUE", 6)==0
      && (len==6 || s[6]=='(' || isspace((unsigned char)s[6])) ) return 1;
  if( len>=5 && sqlite3_strnicmp(s, "CHECK", 5)==0
      && (len==5 || s[5]=='(' || isspace((unsigned char)s[5])) ) return 1;
  if( len>=11 && sqlite3_strnicmp(s, "FOREIGN KEY", 11)==0
      && (len==11 || !isalnum((unsigned char)s[11])) ) return 1;
  if( len>=10 && sqlite3_strnicmp(s, "CONSTRAINT", 10)==0
      && (len==10 || isspace((unsigned char)s[10])) ) return 1;
  return 0;
}

static SQLITE_INLINE int doltliteAppendQuotedColumnList(
  sqlite3_str *pStr,
  char *const *azName,
  int nName,
  const char *zPrefix,
  const char *zSep
){
  int i;
  if( !zPrefix ) zPrefix = "";
  if( !zSep ) zSep = ", ";
  for(i=0; i<nName; i++){
    if( i>0 ) sqlite3_str_appendall(pStr, zSep);
    sqlite3_str_appendf(pStr, "\"%s%w\"", zPrefix, azName[i]);
  }
  return sqlite3_str_errcode(pStr);
}

static SQLITE_INLINE void doltliteFreeStringArray(char **az, int n){
  int i;
  if( !az ) return;
  for(i=0; i<n; i++) sqlite3_free(az[i]);
  sqlite3_free(az);
}

static SQLITE_INLINE int doltliteDupBytes(const u8 *pIn, int nIn, u8 **ppOut){
  u8 *pCopy;
  *ppOut = 0;
  if( !pIn || nIn<=0 ) return SQLITE_OK;
  pCopy = sqlite3_malloc(nIn);
  if( !pCopy ) return SQLITE_NOMEM;
  memcpy(pCopy, pIn, nIn);
  *ppOut = pCopy;
  return SQLITE_OK;
}

int doltliteLoadCatalog(sqlite3 *db, const ProllyHash *catHash,
                        struct TableEntry **ppTables, int *pnTables,
                        Pgno *piNextTable);
void doltliteFreeCatalog(struct TableEntry *a, int n);

static SQLITE_INLINE int doltliteFindTableRootByName(
  struct TableEntry *a, int n, const char *zName,
  ProllyHash *pRoot, u8 *pFlags, ProllyHash *pSchemaHash
){
  struct TableEntry *e = doltliteFindTableByName(a, n, zName);
  if( e ){
    memcpy(pRoot, &e->root, sizeof(ProllyHash));
    if( pFlags ) *pFlags = e->flags;
    if( pSchemaHash ) memcpy(pSchemaHash, &e->schemaHash, sizeof(ProllyHash));
    return SQLITE_OK;
  }
  memset(pRoot, 0, sizeof(ProllyHash));
  if( pFlags ) *pFlags = 0;
  if( pSchemaHash ) memset(pSchemaHash, 0, sizeof(ProllyHash));
  return SQLITE_NOTFOUND;
}

int doltliteLoadTableRootByName(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  const char *zTableName,
  ProllyHash *pRoot,
  u8 *pFlags,
  ProllyHash *pSchemaHash
);

int doltliteLoadTableRootById(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  Pgno iTable,
  ProllyHash *pRoot,
  u8 *pFlags,
  ProllyHash *pSchemaHash
);

static SQLITE_INLINE int doltliteLoadTableRootByNameOrEmpty(
  sqlite3 *db,
  const ProllyHash *pCatHash,
  const char *zTableName,
  ProllyHash *pRoot,
  u8 *pFlags,
  ProllyHash *pSchemaHash
){
  int rc = doltliteLoadTableRootByName(db, pCatHash, zTableName,
                                       pRoot, pFlags, pSchemaHash);
  return rc==SQLITE_NOTFOUND ? SQLITE_OK : rc;
}

typedef struct DlByteReader { const u8 *p; const u8 *end; int err; } DlByteReader;

static SQLITE_INLINE void dlReaderInit(DlByteReader *r, const u8 *data, int n){
  r->p = data; r->end = data + n; r->err = 0;
}
static SQLITE_INLINE u8 dlReadU8(DlByteReader *r){
  if( r->err || r->p+1 > r->end ){ r->err = 1; return 0; }
  return *r->p++;
}
static SQLITE_INLINE int dlReadU16(DlByteReader *r){
  if( r->err || r->p+2 > r->end ){ r->err = 1; return 0; }
  { int v = r->p[0] | (r->p[1]<<8); r->p += 2; return v; }
}
static SQLITE_INLINE int dlReadU32(DlByteReader *r){
  if( r->err || r->p+4 > r->end ){ r->err = 1; return 0; }
  { int v = r->p[0] | (r->p[1]<<8) | (r->p[2]<<16) | (r->p[3]<<24);
    r->p += 4; return v; }
}
static SQLITE_INLINE i64 dlReadI64(DlByteReader *r){
  if( r->err || r->p+8 > r->end ){ r->err = 1; return 0; }
  { i64 v = (i64)((u64)r->p[0] | ((u64)r->p[1]<<8) | ((u64)r->p[2]<<16)
                | ((u64)r->p[3]<<24) | ((u64)r->p[4]<<32) | ((u64)r->p[5]<<40)
                | ((u64)r->p[6]<<48) | ((u64)r->p[7]<<56));
    r->p += 8; return v; }
}
static SQLITE_INLINE int dlReadU32Blob(DlByteReader *r, u8 **pp, int *pn){
  int n = dlReadU32(r);
  *pp = 0; *pn = 0;
  if( r->err ) return SQLITE_CORRUPT;
  if( n<0 || (size_t)n > (size_t)(r->end - r->p) ){ r->err = 1; return SQLITE_CORRUPT; }
  if( n>0 ){
    *pp = sqlite3_malloc(n);
    if( !*pp ) return SQLITE_NOMEM;
    memcpy(*pp, r->p, n);
    *pn = n;
  }
  r->p += n;
  return SQLITE_OK;
}
static SQLITE_INLINE int dlReadU16Name(DlByteReader *r, char **pz){
  int n = dlReadU16(r);
  *pz = 0;
  if( r->err ) return SQLITE_CORRUPT;
  if( n<0 || (size_t)n > (size_t)(r->end - r->p) ){ r->err = 1; return SQLITE_CORRUPT; }
  *pz = sqlite3_malloc(n+1);
  if( !*pz ) return SQLITE_NOMEM;
  memcpy(*pz, r->p, n);
  (*pz)[n] = 0;
  r->p += n;
  return SQLITE_OK;
}
static SQLITE_INLINE int dlReadU32Str(DlByteReader *r, char **pz){
  int n = dlReadU32(r);
  *pz = 0;
  if( r->err ) return SQLITE_CORRUPT;
  if( n<0 || (size_t)n > (size_t)(r->end - r->p) ){ r->err = 1; return SQLITE_CORRUPT; }
  if( n>0 ){
    *pz = sqlite3_malloc(n+1);
    if( !*pz ) return SQLITE_NOMEM;
    memcpy(*pz, r->p, n);
    (*pz)[n] = 0;
    r->p += n;
  }
  return SQLITE_OK;
}

static SQLITE_INLINE int dlAddSize(sqlite3_int64 *pnTotal, sqlite3_int64 nAdd){
  if( nAdd<0 || nAdd>SQLITE_MAX_LENGTH
   || *pnTotal<0 || *pnTotal>SQLITE_MAX_LENGTH-nAdd
   || *pnTotal>INT_MAX-nAdd
  ){
    return SQLITE_TOOBIG;
  }
  *pnTotal += nAdd;
  return SQLITE_OK;
}

typedef struct DlByteWriter {
  u8 *p;
  u8 *end;
  int err;
} DlByteWriter;
static SQLITE_INLINE void dlWriterInit(DlByteWriter *w, u8 *p, int n){
  w->p = p;
  w->end = p + n;
  w->err = 0;
}
static SQLITE_INLINE int dlWriterReserve(DlByteWriter *w, int n){
  if( w->err || n<0 || (size_t)n>(size_t)(w->end-w->p) ){
    w->err = 1;
    return 0;
  }
  return 1;
}
static SQLITE_INLINE void dlWriteU8(DlByteWriter *w, u8 v){
  if( dlWriterReserve(w, 1) ) *w->p++ = v;
}
static SQLITE_INLINE void dlWriteU16(DlByteWriter *w, int v){
  if( dlWriterReserve(w, 2) ){
    w->p[0]=(u8)v; w->p[1]=(u8)(v>>8); w->p+=2;
  }
}
static SQLITE_INLINE void dlWriteU32(DlByteWriter *w, int v){
  if( dlWriterReserve(w, 4) ){
    w->p[0]=(u8)v; w->p[1]=(u8)(v>>8); w->p[2]=(u8)(v>>16); w->p[3]=(u8)(v>>24);
    w->p+=4;
  }
}
static SQLITE_INLINE void dlWriteI64(DlByteWriter *w, i64 v){
  u64 k = (u64)v; int i;
  if( dlWriterReserve(w, 8) ){
    for(i=0; i<8; i++) w->p[i] = (u8)(k >> (i*8));
    w->p += 8;
  }
}
static SQLITE_INLINE void dlWriteU32Blob(DlByteWriter *w, const u8 *p, int n){
  dlWriteU32(w, n);
  if( dlWriterReserve(w, n) ){
    if( n>0 ) memcpy(w->p, p, n);
    w->p += n;
  }
}
static SQLITE_INLINE void dlWriteU16Name(DlByteWriter *w, const char *z, int n){
  dlWriteU16(w, n);
  if( dlWriterReserve(w, n) ){
    if( n>0 ) memcpy(w->p, z, n);
    w->p += n;
  }
}
static SQLITE_INLINE void dlWriteBytes(DlByteWriter *w, const u8 *p, int n){
  if( dlWriterReserve(w, n) ){
    if( n>0 ) memcpy(w->p, p, n);
    w->p += n;
  }
}

static SQLITE_INLINE void dlWriteFramedHeader(DlByteWriter *w, u8 m0, u8 m1,
                                              u8 m2, u8 ver, int nTables){
  dlWriteU8(w, m0);
  dlWriteU8(w, m1);
  dlWriteU8(w, m2);
  dlWriteU8(w, ver);
  dlWriteU16(w, nTables);
}

static SQLITE_INLINE int dlReadFramedHeader(DlByteReader *r, u8 m0, u8 m1,
                                            u8 m2, u8 ver, int *pnTables){
  int a = dlReadU8(r), b = dlReadU8(r), c = dlReadU8(r), v = dlReadU8(r);
  int n = dlReadU16(r);
  if( r->err || a!=m0 || b!=m1 || c!=m2 || v!=ver ) return SQLITE_CORRUPT;
  *pnTables = n;
  return SQLITE_OK;
}

ChunkStore *doltliteGetChunkStore(sqlite3 *db);
ChunkStore *doltliteBtreeChunkStore(Btree *p);
int doltliteGcCompactStoreWithPhase(sqlite3 *db, ChunkStore *cs, const char **pzPhase);
int doltliteGcCompactStore(sqlite3 *db, ChunkStore *cs);
int doltliteGcCompactDbWithPhase(sqlite3 *db, int iDb, const char **pzPhase);
BtShared *doltliteGetBtShared(sqlite3 *db);
int doltliteIsStockSqliteDb(sqlite3 *db);
ProllyCache *doltliteGetCache(sqlite3 *db);
int doltliteSerializeCatalogEntries(sqlite3 *db, struct TableEntry *aTables,
                                    int nTables, u8 **ppOut, int *pnOut);
int doltliteSerializeCatalogEntriesWithFallbackSchema(
    sqlite3 *db, struct TableEntry *aTables, int nTables,
    SchemaEntry *aFallbackSchema, int nFallbackSchema,
    u8 **ppOut, int *pnOut);
int doltliteSerializeCatalogEntriesForeignDomain(
    sqlite3 *db, struct TableEntry *aTables, int nTables,
    SchemaEntry *aFallbackSchema, int nFallbackSchema,
    u8 **ppOut, int *pnOut);
int doltliteGetHeadCatalogHash(sqlite3 *db, ProllyHash *pCatHash);
int doltliteFlushAndSerializeCatalog(sqlite3 *db, u8 **ppOut, int *pnOut);
int doltliteDeserializeCatalogForTest(sqlite3 *db, const u8 *data, int nData);
int doltliteDeserializeConflictsForTest(const u8 *data, int nData);
int doltliteDeserializeConstraintViolationsForTest(
  const u8 *data, int nData
);
int doltliteFlushCatalogToHash(sqlite3 *db, ProllyHash *pHash);
int doltlitePrepareCatalogForPersistence(sqlite3 *db);
int doltliteCreateAndStoreCommit(
  sqlite3 *db,
  const ProllyHash *pParent,
  const ProllyHash *pCatalog,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  const ProllyHash *aExtraParents,
  int nExtraParents,
  ProllyHash *pCommitHash
);
int doltliteCreateAndStoreCommitWithTime(
  sqlite3 *db,
  const ProllyHash *pParent,
  const ProllyHash *pCatalog,
  const char *zMessage,
  const char *zAuthorName,
  const char *zAuthorEmail,
  const ProllyHash *aExtraParents,
  int nExtraParents,
  i64 explicitTimestamp,
  ProllyHash *pCommitHash
);
int doltliteAdvanceBranch(
  sqlite3 *db,
  const ProllyHash *pNewHead,
  const ProllyHash *pCatalogHash,
  const ProllyHash *pWorkingCatHash
);
int doltliteCompareAndAdvanceBranch(
  sqlite3 *db,
  const ProllyHash *pExpectedHead,
  const ProllyHash *pNewHead,
  const ProllyHash *pCatalogHash,
  const ProllyHash *pWorkingCatHash
);
int doltlitePersistOrSaveWorkingSet(sqlite3 *db);
#define DOLTLITE_CMD_OPTION_FLAG 0
#define DOLTLITE_CMD_OPTION_VALUE 1
#define DOLTLITE_CMD_PARSE_SHORT_GROUPS 0x01

typedef struct DoltliteCmdOption DoltliteCmdOption;
struct DoltliteCmdOption {
  const char *zLong;
  char shortName;
  u8 eType;
  int *pSeen;
  const char **pzValue;
};

typedef struct DoltliteCmdArgs DoltliteCmdArgs;
struct DoltliteCmdArgs {
  sqlite3_value **apPositional;
  const char **azPositional;
  int nPositional;
};

int doltliteCmdParseArgs(
  sqlite3_context *ctx, int argc, sqlite3_value **argv,
  DoltliteCmdOption *aOption, int nOption, int flags,
  DoltliteCmdArgs *pArgs
);
void doltliteCmdArgsClear(DoltliteCmdArgs *pArgs);
int doltliteCmdRejectDetached(sqlite3_context *ctx);
void doltliteCmdResultUnknownOption(sqlite3_context *ctx, const char *zOpt);
void doltliteCmdResultMissingOptionValue(
  sqlite3_context *ctx, const char *zOptName
);
int doltliteCmdParseAuthor(
  sqlite3_context *ctx, const char *zAuthor,
  char **pzName, char **pzEmail
);
void doltliteCmdResultPeerBranchBusy(sqlite3_context *ctx, const char *zOp);
int doltliteCmdFinishWithConflicts(
  sqlite3 *db, sqlite3_context *ctx, DoltliteTxnState *pSaved,
  int nConflicts, const char *zOp, int bSealOnPlain
);
int doltliteCmdFinishWithConstraintViolations(
  sqlite3 *db, sqlite3_context *ctx, DoltliteTxnState *pSaved,
  const char *zOp, int bSealOnPlain, const char *zNestedMsg
);
int doltliteCmdFinishWithConflictsAndConstraintViolations(
  sqlite3 *db, sqlite3_context *ctx, DoltliteTxnState *pSaved,
  int nConflicts, const char *zOp, int bSealOnPlain,
  const char *zNestedMsg
);

int doltliteReportConflicts(
  sqlite3 *db, sqlite3_context *ctx, int nConflicts, const char *zOp
);
int doltliteReportConstraintViolations(
  sqlite3 *db, sqlite3_context *ctx, const char *zOp
);
int doltliteReportConflictsAndConstraintViolations(
  sqlite3 *db, sqlite3_context *ctx, int nConflicts, const char *zOp
);
int doltliteDetectPostMergeConstraintViolations(
  sqlite3 *db, const ProllyHash *pAncCatHash, int *pnViolations
);
int doltliteDetectConstraintViolationsFiltered(
  sqlite3 *db,
  const ProllyHash *pAncCatHash,
  const char **azTables,
  int nTables,
  int bPersist,
  int *pnViolations
);
int doltliteVerifyConstraintsRegister(sqlite3 *db);
int doltliteRefreshAndConfirmHead(
  sqlite3 *db, ChunkStore *cs, const ProllyHash *pExpectedHead
);
int doltliteRestoreTxnStateOnFailure(
  sqlite3 *db, DoltliteTxnState *pSaved, int opRc
);
int doltlitePrimeSchemaCache(sqlite3 *db);
void doltliteReportAutocommitConflictRollback(sqlite3_context *ctx);
int doltliteRollbackAutocommitConflict(
  sqlite3 *db, sqlite3_context *ctx, DoltliteTxnState *pSaved
);
int doltliteSavepointIsTopLevelTxn(sqlite3 *db);
int doltliteVcSealTopLevelSavepointTxn(sqlite3 *db);

typedef DoltliteNameIndex AddNameIndex;
int addNameIndexInit(
  AddNameIndex *pIdx, struct TableEntry *aEntry, int nEntry
);
void addNameIndexFree(AddNameIndex *pIdx);
struct TableEntry *addNameIndexFind(
  const AddNameIndex *pIdx, const char *zName
);
int amTableStagedByName(
  struct TableEntry *aStaged, int nStaged, const char *zTbl
);
void addRemoveIndexEntriesOfTable(
  struct TableEntry *aStaged, int *pnStaged,
  SchemaEntry *aStagedSchema, int nStagedSchema, const char *zTable
);
int addAppendIndexEntriesOfTable(
  sqlite3_context *context,
  struct TableEntry **paStaged, int *pnStaged,
  struct TableEntry *aWorking, int nWorking,
  SchemaEntry *aWorkSchema, int nWorkSchema,
  const char *zTable
);
int doltliteStageNamedTables(
  sqlite3 *db, sqlite3_context *context, ChunkStore *cs,
  const ProllyHash *pWorkingHash, int argc, sqlite3_value **argv
);
int doltliteCatalogRenameMate(
  sqlite3 *db,
  struct TableEntry *aFrom, int nFrom,
  struct TableEntry *aTo, int nTo,
  const struct TableEntry *pKnown,
  int bKnownIsFrom,
  struct TableEntry **ppMate
);

int mergeAbortInPlace(sqlite3 *db);
int mergeFastForward(
  sqlite3 *db, sqlite3_context *context, ChunkStore *cs,
  const ProllyHash *pOurHead, const ProllyHash *pTheirHead
);
int doltliteMergeRef(
  sqlite3 *db,
  sqlite3_context *context,
  const char *zBranch,
  const char *zMessage,
  int noFastForward
);

int doltliteAddRegister(sqlite3 *db);
int doltliteCommitCmdRegister(sqlite3 *db);
int doltliteResetRegister(sqlite3 *db);
int doltliteMergeCmdRegister(sqlite3 *db);
int doltliteCherryPickRegister(sqlite3 *db);
int doltliteRevertRegister(sqlite3 *db);
int doltliteRebaseRegister(sqlite3 *db);
int doltliteConfigRegister(sqlite3 *db);
int doltliteMaybeSeedRepo(sqlite3 *db);
int doltliteSeedStoreIfNeeded(sqlite3*, ChunkStore*, const char*,
                              ProllyHash*, int*);

int doltliteRegisterConflictTables(sqlite3 *db);
int doltliteRegisterWorkspaceTables(sqlite3 *db);
int doltliteRegisterBlameTables(sqlite3 *db);
const sqlite3_module *doltliteDiffTableModule(void);
const sqlite3_module *doltliteHistoryTableModule(void);
int doltliteRegisterHistoricalTables(sqlite3 *db);
int doltliteRegisterHistoricalTablesForCatalog(sqlite3 *db,
                                                const ProllyHash *pCatHash);
int doltliteRefreshConstraintViolationTables(sqlite3 *db);
int doltliteSetTableSchemaHash(sqlite3 *db, Pgno iTable, const ProllyHash *pH);
int doltliteUpdateSchemaHashes(sqlite3 *db);

/* nTables<=0 scans every user table. */
int doltliteDetectMergeFkViolations(sqlite3 *db, const ProllyHash *pAncCatHash,
                                    char **pzErrMsg, int *pnFound,
                                    const char **azTables, int nTables);
int doltliteDetectMergeUniqueViolations(sqlite3 *db, const ProllyHash *pAncCatHash,
                                        char **pzErrMsg, int *pnFound,
                                        const char **azTables, int nTables);
int doltliteDetectMergeCheckViolations(sqlite3 *db, const ProllyHash *pAncCatHash,
                                       char **pzErrMsg, int *pnFound,
                                       const char **azTables, int nTables);
int doltliteDetectMergeNotNullViolations(sqlite3 *db, const ProllyHash *pAncCatHash,
                                       char **pzErrMsg, int *pnFound,
                                       const char **azTables, int nTables);
int doltliteDetectMergeStrictViolations(sqlite3 *db, const ProllyHash *pAncCatHash,
                                       char **pzErrMsg, int *pnFound,
                                       const char **azTables, int nTables);

int doltliteConstraintViolationBatchBegin(sqlite3 *db);
int doltliteConstraintViolationBatchEnd(sqlite3 *db, int commit);

int doltliteGetWorkingTableState(sqlite3 *db, const char *zTable,
                                 ProllyHash *pRoot, u8 *pFlags,
                                 ProllyHash *pSchemaHash);
int doltliteHasUncommittedChanges(sqlite3 *db, int *pDirty);
void doltliteTxnStateClear(DoltliteTxnState *p);
int doltliteSaveTxnState(sqlite3 *db, DoltliteTxnState *p);
int doltliteRestoreTxnState(sqlite3 *db, DoltliteTxnState *p);

int doltliteResolveRef(sqlite3 *db, const char *zRef, ProllyHash *pCommit);
int doltliteUserRefNameIsValid(const char *zName);
int doltliteFindAncestor(sqlite3 *db, const ProllyHash *pCommit1,
                         const ProllyHash *pCommit2,
                         ProllyHash *pAncestor);

typedef struct DoltliteCommit DoltliteCommit;
int doltliteLoadCommit(sqlite3 *db, const ProllyHash *pHash,
                       DoltliteCommit *pCommit);
int doltliteLoadFirstParentCommit(
  sqlite3 *db, const DoltliteCommit *pCommit, DoltliteCommit *pParentCommit
);
int doltliteLoadHeadAndParentedCommit(
  sqlite3 *db,
  const ProllyHash *pTargetHash,
  ProllyHash *pOurHead,
  DoltliteCommit *pTargetCommit,
  DoltliteCommit *pParentCommit,
  DoltliteCommit *pOurCommit
);
int applyMergedCatalogAndCommit(
  sqlite3 *db,
  sqlite3_context *context,
  const ProllyHash *ancCatHash,
  const ProllyHash *ourCatHash,
  const ProllyHash *theirCatHash,
  const ProllyHash *ourHead,
  const ProllyHash *pCommitOurCatHash,
  const char *zMessage,
  int bPreferOurMaster,
  int bRejectUnchanged,
  int *pnConflicts,
  int *pnViolations,
  char **pzApplyErr,
  char *hexBuf
);

int doltliteCommitCatalogHash(sqlite3 *db, const ProllyHash *pCommit,
                              ProllyHash *pCatHash);
int doltliteRefToCatalogHash(sqlite3 *db, const char *zRef,
                             ProllyHash *pCatHash);

int doltliteRefIsWorking(const char *zRef);
int doltliteRefIsStaged(const char *zRef);

/* NULL ref is HEAD. WORKING is uncommitted edits; STAGED falls back to HEAD. */
int doltliteResolveCatalogHashForRef(sqlite3 *db, const char *zRef,
                                     ProllyHash *pCatHash);

int doltliteForEachUserTable(sqlite3 *db, const char *zPrefix,
                             const sqlite3_module *pModule);

int doltliteResolveTableName(sqlite3 *db, const char *zTable, Pgno *piTable);
char *doltliteResolveTableNumber(sqlite3 *db, Pgno iTable);
int doltliteApplyRawRowMutation(sqlite3 *db, const char *zTable,
                                const u8 *pKey, int nKey, i64 intKey,
                                const u8 *pVal, int nVal);

typedef struct DoltliteConflictRow DoltliteConflictRow;
struct DoltliteConflictRow {
  i64 intKey;
  u8 *pKey; int nKey;
  u8 *pBaseVal; int nBaseVal;
  u8 *pOurVal; int nOurVal;
  u8 *pTheirVal; int nTheirVal;
};

typedef struct DoltliteConflictTable DoltliteConflictTable;
/* nConflicts==0 marks a schema-conflict table (azSchemaObjects), not an
** empty data-conflict table. Data conflicts always have nConflicts>0. */
struct DoltliteConflictTable {
  char *zName;
  int nConflicts;
  DoltliteConflictRow *aRows;
  char **azSchemaObjects;
  int nSchemaObjects;
};

void doltliteConflictRowFree(DoltliteConflictRow *pRow);
int doltliteSerializeConflicts(ChunkStore *cs,
                               DoltliteConflictTable *aTables,
                               int nTables, ProllyHash *pHash);

/* Index keys must match VDBE (NOCASE/RTRIM/DESC). */
void doltliteIpkSerialType(i64 v, u32 *pType, u32 *pLen);
void doltliteIpkWriteBE(u8 *p, i64 v, int n);
KeyInfo *doltliteKeyInfoOfIndex(sqlite3 *db, Index *pIdx);
int doltliteIndexMutMapRowDelta(
  sqlite3 *db,
  Index *pIdx,
  ProllyMutMap *pMap,
  const i16 *aiColumn, int nIdxCol,
  KeyInfo *pKeyInfo,
  int iPKey, i64 intKey,
  const u8 *pTreeKey, int nTreeKey,
  const u8 *pOldVal, int nOldVal,
  const u8 *pNewVal, int nNewVal
);
int doltliteIndexApplyRowDelta(
  sqlite3 *db,
  ChunkStore *cs,
  ProllyCache *cache,
  ProllyHash *pIdxRoot,
  u8 idxFlags,
  Index *pIdx,
  int iPKey, i64 intKey,
  const u8 *pTreeKey, int nTreeKey,
  const u8 *pOldVal, int nOldVal,
  const u8 *pNewVal, int nNewVal
);
int doltliteEnsureWriteTxnAndSavepoints(sqlite3 *db);
int doltliteSwitchCatalog(sqlite3 *db, const ProllyHash *catHash);
int doltliteHardReset(sqlite3 *db, const ProllyHash *catHash);
int doltliteUpdateBranchWorkingState(sqlite3 *db, const char *zBranch,
                                     const ProllyHash *pCatHash,
                                     const ProllyHash *pCommitHash);
int doltliteWriteBranchCleanWorkingState(sqlite3 *db, const char *zBranch,
                                         const ProllyHash *pCatHash,
                                         const ProllyHash *pCommitHash);
int doltliteCheckRepoGraphIntegrity(Btree *p, int mxErr, int *pnErr);

const char *doltliteGetSessionBranch(sqlite3 *db);
int doltliteIsDetached(sqlite3 *db);
void doltliteSetSessionDetached(sqlite3 *db, int isDetached);
int doltlitePrepareSessionBranch(sqlite3 *db, const char *zBranch,
                                 char **pzPrepared);
void doltliteInstallPreparedSessionBranch(sqlite3 *db, char *zPrepared);
int doltliteSetSessionBranch(sqlite3 *db, const char *zBranch);
void doltliteGetSessionHead(sqlite3 *db, ProllyHash *pHead);
void doltliteSetSessionHead(sqlite3 *db, const ProllyHash *pHead);
void doltliteGetSessionStaged(sqlite3 *db, ProllyHash *pStaged);
int doltliteSetSessionStaged(sqlite3 *db, const ProllyHash *pStaged);
void doltliteGetSessionMergeState(sqlite3 *db, u8 *pIsMerging,
                                  ProllyHash *pMergeCommit,
                                  ProllyHash *pConflictsCatalog);
int doltliteSetSessionMergeState(sqlite3 *db, u8 isMerging,
                                 const ProllyHash *pMergeCommit,
                                 const ProllyHash *pConflictsCatalog);
int doltliteClearSessionMergeState(sqlite3 *db);
int doltliteSetSessionPendingReplayCommit(sqlite3 *db, u8 pending);
int doltliteSessionHasPendingReplayCommit(sqlite3 *db);
int doltliteSetSessionMergeSourceSpec(sqlite3 *db, const char *zSpec,
                                      const ProllyHash *pMergeCommit);
const char *doltliteGetSessionMergeSourceSpec(sqlite3 *db,
                                              const ProllyHash *pMergeCommit);
void doltliteGetSessionRebaseState(sqlite3 *db, u8 *pIsRebasing,
                                   ProllyHash *pPreRebaseCat,
                                   ProllyHash *pRebaseOnto,
                                   const char **pzOrigBranch,
                                   const char **pzReturnBranch);
u8 doltliteGetSessionRebaseFlags(sqlite3 *db);
int doltliteSetSessionRebaseState(sqlite3 *db, u8 isRebasing,
                                  const ProllyHash *pPreRebaseCat,
                                  const ProllyHash *pRebaseOnto,
                                  const char *zOrigBranch,
                                  const char *zReturnBranch);
int doltliteClearSessionRebaseState(sqlite3 *db);
int doltliteBranchWorkingSetRebaseFlags(sqlite3 *db, const char *zBranch, u8 *pFlags);
int doltliteClearBranchRebaseMetadata(sqlite3 *db, const char *zBranch);
void doltliteGetSessionConflictsCatalog(sqlite3 *db, ProllyHash *pHash);
int doltliteSessionHasUnresolvedConflicts(sqlite3 *db);
int doltliteSetSessionConflictsCatalog(sqlite3 *db, const ProllyHash *pHash);
void doltliteGetSessionConstraintViolationsCatalog(sqlite3 *db, ProllyHash *pHash);
int doltliteSetSessionConstraintViolationsCatalog(sqlite3 *db, const ProllyHash *pHash);
int doltliteSessionHasConstraintViolations(sqlite3 *db);
int doltliteSeedSessionHashes(sqlite3 *db, ChunkStore *cs,
                              int (*xPush)(void*, const ProllyHash*),
                              void *pCtx);
int doltliteGetSessionTableRoot(sqlite3 *db, Pgno iTable,
                                 ProllyHash *pRoot, u8 *pFlags);
int doltliteSaveWorkingSet(sqlite3 *db);
int doltlitePersistWorkingSet(sqlite3 *db);
int doltliteSaveWorkingSetWithHash(sqlite3 *db, const ProllyHash *pWorkingCatHash);
int doltlitePersistWorkingSetWithHash(sqlite3 *db, const ProllyHash *pWorkingCatHash);
void doltliteAdoptRollbackBaseline(sqlite3 *db, const ProllyHash *pCatalogHash);
int doltliteLoadWorkingSet(sqlite3 *db, const char *zBranch);
int doltliteBranchWorkingSetIsRebasing(sqlite3 *db, const char *zBranch,
                                       int *pActive);
int doltliteGetPersistedWorkingCatalogHash(sqlite3 *db, ProllyHash *pCatHash);
int doltliteCheckoutBranchForRebase(sqlite3 *db, const char *zBranch);
int doltliteCheckoutPersistedRebase(sqlite3 *db, const char *zBranch);
int doltliteCheckoutBranchForRebaseWithOldCatalog(
  sqlite3 *db,
  const char *zBranch,
  const ProllyHash *pOldCatHash
);
DoltliteVcTxnMode doltliteVcTxnMode(sqlite3 *db);
int doltliteVcSealActiveSavepoints(sqlite3 *db);
int doltliteVcSealEnclosingTxn(sqlite3 *db);
int doltliteVcSealSavepointError(sqlite3 *db);
void doltliteVcResultError(sqlite3_context *ctx, sqlite3 *db, const char *zMsg);
int doltliteVcSealBranchStyleTxn(sqlite3 *db);

typedef int (*DoltliteRefsMutation)(sqlite3 *db, ChunkStore *cs, void *pArg);
typedef struct DoltliteBranchExpectation DoltliteBranchExpectation;
struct DoltliteBranchExpectation {
  const char *zBranch;
  const ProllyHash *pTip;
};
int doltliteMutateRefsExpected(
  sqlite3 *db,
  const DoltliteBranchExpectation *aExpected,
  int nExpected,
  DoltliteRefsMutation xMutate,
  void *pArg
);
int doltliteMutateRefs(sqlite3 *db, DoltliteRefsMutation xMutate, void *pArg);
void doltliteTestCrashFinalize(const char *zOperation);
void doltliteTestFailNextVcSeal(void);
void doltliteTestSetBeforeRefInstallHook(void (*xHook)(void*), void *pArg);
void doltliteTestRunBeforeRefInstallHook(void);
void doltliteTestFailNextHeadConfirm(void);
void doltliteTestSetRebaseBeforeAdvanceHook(void (*xHook)(void));

const char *doltliteGetAuthorName(sqlite3 *db);
int doltliteSetAuthorName(sqlite3 *db, const char *zName);
const char *doltliteGetAuthorEmail(sqlite3 *db);
int doltliteSetAuthorEmail(sqlite3 *db, const char *zEmail);

int loadSchemaFromCatalog(sqlite3 *db, ChunkStore *cs, ProllyCache *pCache,
                          const ProllyHash *pCatHash,
                          SchemaEntry **ppEntries, int *pnEntries);
int loadSchemaEntryFromCatalog(sqlite3 *db, ChunkStore *cs, ProllyCache *pCache,
                               const ProllyHash *pCatHash, const char *zName,
                               SchemaEntry *pEntry, int *pFound);
SchemaEntry *findSchemaEntry(SchemaEntry *a, int n, const char *zName);
void clearSchemaEntry(SchemaEntry *pEntry);
void freeSchemaEntries(SchemaEntry *a, int n);
char *doltliteCanonicalizeSchemaSql(const char *zSql, const char *zName);
int doltliteLoadLiveSchemaSql(sqlite3 *db, const char *zType, const char *zDb,
                              const char *zName, const char *zTblName,
                              char **pzSql);

typedef struct SchemaMergeAction SchemaMergeAction;
struct SchemaMergeAction {
  char *zTableName;
  char **azAddColumns;
  int nAddColumns;
  char **azDropColumns;
  int nDropColumns;
  /* Flat old,new rename pairs the merged layout still needs. */
  char **azRenameColumns;
  int nRenameColumns;
  /* Rename the whole table to this name, before any column action runs. */
  char *zRenameTable;
};

void freeSchemaMergeActions(SchemaMergeAction *a, int n);

int doltliteMergeCatalogs(sqlite3 *db,
    const ProllyHash *ancestor, const ProllyHash *ours,
    const ProllyHash *theirs, ProllyHash *pMergedHash,
    int *pnConflicts, char **pzErrMsg,
    SchemaMergeAction **ppActions, int *pnActions,
    int bPreferOurMaster,
    int bBranchMerge,
    char ***pazReindex, int *pnReindex,
    char ***pazRebuildVtabs, int *pnRebuildVtabs);
void doltliteFreeNameList(char **az, int n);
int doltliteIndexSchemaRowsDifferForTable(SchemaEntry *aA, int nA,
    SchemaEntry *aB, int nB, const char *zTable);
int doltliteBuildNamedStageMasterRoot(sqlite3 *db,
    const ProllyHash *pWorkingMaster, u8 workingFlags,
    const ProllyHash *pOldMaster, u8 oldFlags,
    const char **azTouched, int nTouched,
    struct TableEntry *aFinal, int nFinal,
    int bEntrylessFromWorking,
    ProllyHash *pNewRoot);
void doltliteRenumberStaleStagedEntries(
    struct TableEntry *aStaged, int nStaged,
    struct TableEntry *aWorking, int nWorking);
void doltliteAlignStagedEntriesToWorking(
    struct TableEntry *aWorking, int nWorking,
    struct TableEntry *aStaged, int nStaged);
int doltliteReindexNamedIndexes(sqlite3 *db, char **az, int n);
int doltliteTableSchemaConflictDetail(const char *zAncestorSql,
    const char *zOurSql, const char *zTheirSql, char **pzDetail);
int doltliteSessionHasSchemaConflicts(sqlite3 *db);
int doltliteForEachSchemaConflict(sqlite3 *db,
    int (*xConflict)(void*, const char*), void *pCtx);

static SQLITE_INLINE int doltliteAppendQuotedIdent(sqlite3_str *pStr,
                                                   const char *zName){
  sqlite3_str_appendf(pStr, "\"%w\"", zName ? zName : "");
  return sqlite3_str_errcode(pStr);
}

static SQLITE_INLINE const char *doltliteVcUnavailableMessage(sqlite3 *db){
  if( doltliteIsStockSqliteDb(db) ){
    return "dolt version-control features are not available on stock SQLite databases";
  }
  return "no database open";
}

static SQLITE_INLINE int doltliteGrowArrayImpl(
  void **ppArr, int *pnAlloc, int nNeeded, int elemSize, int seed
){
  i64 nNew;
  void *pNew;
  if( nNeeded <= *pnAlloc ) return SQLITE_OK;
  if( elemSize <= 0 || nNeeded < 0 || seed <= 0 ) return SQLITE_NOMEM;
  nNew = *pnAlloc>0 ? (i64)*pnAlloc * 2 : (i64)seed;
  while( nNew < (i64)nNeeded ){
    if( nNew > (i64)0x7fffffff/2 ){ nNew = (i64)0x7fffffff; break; }
    nNew *= 2;
  }
  if( nNew < (i64)nNeeded ) return SQLITE_NOMEM;
  if( nNew > (i64)0x7fffffff/(i64)elemSize ) return SQLITE_NOMEM;
  pNew = sqlite3_realloc(*ppArr, (int)(nNew * (i64)elemSize));
  if( !pNew ) return SQLITE_NOMEM;
  *ppArr = pNew;
  *pnAlloc = (int)nNew;
  return SQLITE_OK;
}

#define DOLTLITE_GROW_ARRAY(ppArr, pnAlloc, nNeeded, seed) \
  doltliteGrowArrayImpl((void**)(ppArr), (pnAlloc), (nNeeded), \
                        (int)sizeof(**(ppArr)), (seed))

#endif
