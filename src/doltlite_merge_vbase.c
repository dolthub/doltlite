#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

struct VbRow {
  i64 intKey;
  u8 *pKey;
  int nKey;
  u8 bWholeRow;
  int nField;
  int *aiField;
};

struct VbTable {
  char *zName;
  VbRow *aRow;
  int nRow;
  int nRowAlloc;
  u8 bSorted;
};

struct VirtualBaseCells {
  VbTable *aTable;
  int nTable;
};

static int vbKeyCmp(i64 iA, const u8 *pA, int nA, i64 iB, const u8 *pB, int nB){
  int c;
  if( !pA && !pB ) return iA<iB ? -1 : iA>iB;
  if( !pA || !pB ) return pA ? 1 : -1;
  c = memcmp(pA, pB, nA<nB ? nA : nB);
  return c ? c : nA - nB;
}

static int vbRowCmp(const void *a, const void *b){
  const VbRow *x = (const VbRow*)a, *y = (const VbRow*)b;
  return vbKeyCmp(x->intKey, x->pKey, x->nKey, y->intKey, y->pKey, y->nKey);
}

static void vbRowClear(VbRow *pRow){
  sqlite3_free(pRow->pKey);
  sqlite3_free(pRow->aiField);
}

VirtualBaseCells *doltliteVirtualBaseCellsNew(void){
  VirtualBaseCells *p = sqlite3_malloc(sizeof(*p));
  if( p ) memset(p, 0, sizeof(*p));
  return p;
}

void doltliteVirtualBaseCellsFree(VirtualBaseCells *p){
  int i, j;
  if( !p ) return;
  for(i=0; i<p->nTable; i++){
    for(j=0; j<p->aTable[i].nRow; j++) vbRowClear(&p->aTable[i].aRow[j]);
    sqlite3_free(p->aTable[i].aRow);
    sqlite3_free(p->aTable[i].zName);
  }
  sqlite3_free(p->aTable);
  sqlite3_free(p);
}

static VbTable *mergeVbFindTable(VirtualBaseCells *p, const char *zName){
  int i;
  if( !p || !zName ) return 0;
  for(i=0; i<p->nTable; i++){
    if( p->aTable[i].nRow>0
     && sqlite3_stricmp(p->aTable[i].zName, zName)==0 ){
      return &p->aTable[i];
    }
  }
  return 0;
}

static int vbTable(VirtualBaseCells *p, const char *zName, VbTable **ppTab){
  VbTable *aNew;
  int i;
  for(i=0; i<p->nTable; i++){
    if( sqlite3_stricmp(p->aTable[i].zName, zName)==0 ){
      *ppTab = &p->aTable[i];
      return SQLITE_OK;
    }
  }
  aNew = sqlite3_realloc64(p->aTable, (p->nTable+1)*sizeof(VbTable));
  if( !aNew ) return SQLITE_NOMEM;
  p->aTable = aNew;
  memset(&aNew[p->nTable], 0, sizeof(VbTable));
  aNew[p->nTable].zName = sqlite3_mprintf("%s", zName);
  if( !aNew[p->nTable].zName ) return SQLITE_NOMEM;
  *ppTab = &aNew[p->nTable++];
  return SQLITE_OK;
}

static int vbAddRow(
  VbTable *pTab, i64 intKey, const u8 *pKey, int nKey,
  int bWholeRow, const int *aiField, int nField
){
  VbRow *pRow;
  int rc = DOLTLITE_GROW_ARRAY(&pTab->aRow, &pTab->nRowAlloc, pTab->nRow+1, 16);
  if( rc!=SQLITE_OK ) return rc;
  pRow = &pTab->aRow[pTab->nRow];
  memset(pRow, 0, sizeof(*pRow));
  pRow->intKey = intKey;
  pRow->bWholeRow = (u8)bWholeRow;
  if( pKey ){
    rc = doltliteDupBytes(pKey, nKey, &pRow->pKey);
    if( rc!=SQLITE_OK ) return rc;
    pRow->nKey = nKey;
  }
  if( nField>0 ){
    pRow->aiField = sqlite3_malloc64(nField*sizeof(int));
    if( !pRow->aiField ){
      sqlite3_free(pRow->pKey);
      return SQLITE_NOMEM;
    }
    memcpy(pRow->aiField, aiField, nField*sizeof(int));
    pRow->nField = nField;
  }
  pTab->nRow++;
  pTab->bSorted = 0;
  return SQLITE_OK;
}

static int mergeVbTableSort(VbTable *pTab){
  int i, n = 0;
  if( pTab->bSorted ) return SQLITE_OK;
  qsort(pTab->aRow, pTab->nRow, sizeof(VbRow), vbRowCmp);
  for(i=0; i<pTab->nRow; i++){
    VbRow *pDst;
    if( n==0 || vbRowCmp(&pTab->aRow[n-1], &pTab->aRow[i])!=0 ){
      pTab->aRow[n++] = pTab->aRow[i];
      continue;
    }
    pDst = &pTab->aRow[n-1];
    pDst->bWholeRow |= pTab->aRow[i].bWholeRow;
    if( pTab->aRow[i].nField>0 ){
      int *aNew = sqlite3_realloc64(pDst->aiField,
          (pDst->nField + pTab->aRow[i].nField)*sizeof(int));
      if( !aNew ) return SQLITE_NOMEM;
      memcpy(&aNew[pDst->nField], pTab->aRow[i].aiField,
             pTab->aRow[i].nField*sizeof(int));
      pDst->aiField = aNew;
      pDst->nField += pTab->aRow[i].nField;
    }
    vbRowClear(&pTab->aRow[i]);
  }
  pTab->nRow = n;
  pTab->bSorted = 1;
  return SQLITE_OK;
}

const VbRow *mergeVbFindRow(VbTable *pTab, const ThreeWayChange *pChange){
  int lo = 0, hi;
  if( !pTab ) return 0;
  hi = pTab->nRow - 1;
  while( lo<=hi ){
    int mid = (lo+hi)/2;
    VbRow *pRow = &pTab->aRow[mid];
    int c = vbKeyCmp(pRow->intKey, pRow->pKey, pRow->nKey,
                     pChange->intKey, pChange->nKey>0 ? pChange->pKey : 0,
                     pChange->nKey);
    if( c==0 ) return pRow;
    if( c<0 ) lo = mid+1; else hi = mid-1;
  }
  return 0;
}

int doltliteVirtualBaseCellsAdd(VirtualBaseCells **ppDst, VirtualBaseCells *pSrc){
  int i, j, rc = SQLITE_OK;
  if( !pSrc ) return SQLITE_OK;
  if( !*ppDst ){
    *ppDst = pSrc;
    return SQLITE_OK;
  }
  for(i=0; i<pSrc->nTable && rc==SQLITE_OK; i++){
    VbTable *pDst = 0;
    rc = vbTable(*ppDst, pSrc->aTable[i].zName, &pDst);
    for(j=0; j<pSrc->aTable[i].nRow && rc==SQLITE_OK; j++){
      VbRow *pRow = &pSrc->aTable[i].aRow[j];
      rc = vbAddRow(pDst, pRow->intKey, pRow->pKey, pRow->nKey,
                    pRow->bWholeRow, pRow->aiField, pRow->nField);
    }
  }
  doltliteVirtualBaseCellsFree(pSrc);
  return rc;
}

int doltliteVirtualBaseMergeTouches(sqlite3 *db, const char *zTable){
  VirtualBaseMerge *pVb = (VirtualBaseMerge*)doltliteVirtualBaseMerge(db);
  if( !pVb ) return 0;
  return pVb->pCollect!=0 || mergeVbFindTable(pVb->pCheck, zTable)!=0;
}

int mergeVbRowForces(const VbRow *pRow, int iField){
  int i;
  if( !pRow ) return 0;
  if( pRow->bWholeRow ) return 1;
  for(i=0; i<pRow->nField; i++){
    if( pRow->aiField[i]==iField ) return 1;
  }
  return 0;
}

int mergeVbRowWhole(const VbRow *pRow){
  return pRow && pRow->bWholeRow;
}

int mergeVbCollect(
  VirtualBaseCells *p, const char *zTable, const ThreeWayChange *pChange,
  int bWholeRow, const int *aiField, int nField
){
  VbTable *pTab = 0;
  int rc = vbTable(p, zTable, &pTab);
  if( rc!=SQLITE_OK ) return rc;
  return vbAddRow(pTab, pChange->intKey,
                  pChange->nKey>0 ? pChange->pKey : 0, pChange->nKey,
                  bWholeRow, aiField, nField);
}

/* The two sides a one-sided change compares: the unchanged side holds the
** base row. Returns 0 for a change that is not one-sided. */
int mergeVbOneSided(const ThreeWayChange *p, MergeVbSides *pSides){
  memset(pSides, 0, sizeof(*pSides));
  switch( p->type ){
    case THREE_WAY_LEFT_ADD:
      pSides->bL = 1; pSides->pL = p->pOurVal; pSides->nL = p->nOurVal;
      break;
    case THREE_WAY_LEFT_MODIFY:
      pSides->bBase = 1;
      pSides->bL = 1; pSides->pL = p->pOurVal; pSides->nL = p->nOurVal;
      pSides->bR = 1; pSides->pR = p->pBaseVal; pSides->nR = p->nBaseVal;
      break;
    case THREE_WAY_LEFT_DELETE:
      pSides->bBase = 1;
      pSides->bR = 1; pSides->pR = p->pBaseVal; pSides->nR = p->nBaseVal;
      break;
    case THREE_WAY_RIGHT_ADD:
      pSides->bR = 1; pSides->pR = p->pTheirVal; pSides->nR = p->nTheirVal;
      break;
    case THREE_WAY_RIGHT_MODIFY:
      pSides->bBase = 1;
      pSides->bL = 1; pSides->pL = p->pBaseVal; pSides->nL = p->nBaseVal;
      pSides->bR = 1; pSides->pR = p->pTheirVal; pSides->nR = p->nTheirVal;
      break;
    case THREE_WAY_RIGHT_DELETE:
      pSides->bBase = 1;
      pSides->bL = 1; pSides->pL = p->pBaseVal; pSides->nL = p->nBaseVal;
      break;
    default:
      return 0;
  }
  return 1;
}

/* Absent rows carry a null pointer, so a present row with an empty record
** points at aEmpty. */
void mergeVbConflictChange(
  const ThreeWayChange *p, const MergeVbSides *pSides, ThreeWayChange *pOut
){
  static const u8 aEmpty[1] = { 0 };
  memset(pOut, 0, sizeof(*pOut));
  pOut->type = pSides->bL && pSides->bR
      ? THREE_WAY_CONFLICT_MM : THREE_WAY_CONFLICT_DM;
  pOut->pKey = p->pKey;
  pOut->nKey = p->nKey;
  pOut->intKey = p->intKey;
  if( pSides->bBase ){
    pOut->pBaseVal = p->pBaseVal ? p->pBaseVal : aEmpty;
    pOut->nBaseVal = p->nBaseVal;
  }
  if( pSides->bL ){
    pOut->pOurVal = pSides->pL ? pSides->pL : aEmpty;
    pOut->nOurVal = pSides->nL;
  }
  if( pSides->bR ){
    pOut->pTheirVal = pSides->pR ? pSides->pR : aEmpty;
    pOut->nTheirVal = pSides->nR;
  }
}


/* Conflicted cells are numbered in the base's layout, so a table whose
** schema the merge changes cannot be checked against them. */
int mergeVbBind(
  sqlite3 *db, const char *zTable, int bSchemaChanged,
  VbTable **ppCheck, VirtualBaseCells **ppCollect
){
  VirtualBaseMerge *pVb = (VirtualBaseMerge*)doltliteVirtualBaseMerge(db);
  *ppCheck = 0;
  *ppCollect = 0;
  if( !pVb ) return SQLITE_OK;
  *ppCollect = pVb->pCollect;
  *ppCheck = mergeVbFindTable(pVb->pCheck, zTable);
  if( !*ppCheck ) return SQLITE_OK;
  if( bSchemaChanged ){
    pVb->bRefuse = 1;
    return SQLITE_ERROR;
  }
  return mergeVbTableSort(*ppCheck);
}

#endif
