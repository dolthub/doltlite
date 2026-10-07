#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"
#include "vdbeInt.h"

#define SCHEMA_IR_OTHER 0
#define SCHEMA_IR_FK    1
#define SCHEMA_IR_CHECK 2

typedef struct SchemaIr SchemaIr;
struct SchemaIr {
  ParsedColumn *aCols;
  int nCols;
  char *zFkSig;
  char *zCheckSig;
  int hasFk;
  int hasCheck;
};

static int schemaIrBuild(const char *zSql, SchemaIr *pIr);

int parseColumns(
  const char *zSql,
  ParsedColumn **ppCols, int *pnCols
){
  SchemaIr ir;
  int rc;

  *ppCols = 0;
  *pnCols = 0;
  rc = schemaIrBuild(zSql, &ir);
  if( rc!=SQLITE_OK ) return rc;
  *ppCols = ir.aCols;
  *pnCols = ir.nCols;
  sqlite3_free(ir.zFkSig);
  sqlite3_free(ir.zCheckSig);
  return SQLITE_OK;
}

void freeColumns(ParsedColumn *aCols, int nCols){
  int i;
  for(i=0; i<nCols; i++){
    sqlite3_free(aCols[i].zName);
    sqlite3_free(aCols[i].zDef);
  }
  sqlite3_free(aCols);
}

int parsedColumnIndexByName(
  ParsedColumn *aCols,
  int nCols,
  const char *zName
){
  int i;
  for(i=0; i<nCols; i++){
    if( aCols[i].zName && zName
     && sqlite3_stricmp(aCols[i].zName, zName)==0 ){
      return i;
    }
  }
  return -1;
}

/* Omitted trailing fields are NULL. VIRTUAL slots are not compared. */
int mergeStoredFieldsEqual(
  const u8 *pA, int nA, const DoltliteRecordInfo *pAi, int iA,
  const u8 *pB, int nB, const DoltliteRecordInfo *pBi, int iB
){
  int aNull, bNull;
  if( iA<0 || iB<0 ) return 1;
  aNull = iA>=pAi->nField || pAi->aType[iA]==0;
  bNull = iB>=pBi->nField || pBi->aType[iB]==0;
  if( aNull || bNull ) return aNull && bNull;
  return doltliteFieldValuesEqual(
      pAi->aType[iA], pA, nA, pAi->aOffset[iA],
      pBi->aType[iB], pB, nB, pBi->aOffset[iB]);
}

int mergeLoadReaderColumns(
  const char *zSql, const char *zTable, DoltliteColInfo *ci
){
  sqlite3 *tmp = 0;
  int rc;
  memset(ci, 0, sizeof(*ci));
  if( !zSql || !zTable ) return SQLITE_OK;
  rc = sqlite3_open(":memory:", &tmp);
  if( rc==SQLITE_OK ) rc = sqlite3_exec(tmp, zSql, 0, 0, 0);
  if( rc==SQLITE_OK ) rc = doltliteGetReaderColumnNames(tmp, zTable, ci);
  if( tmp ) sqlite3_close(tmp);
  return rc;
}

int mergeReaderRecordSlot(const DoltliteColInfo *ci, int i){
  if( !ci || i<0 || i>=ci->nCol ) return -1;
  return ci->aColToRec ? ci->aColToRec[i] : i;
}

int mergeMapUnmatchedColumns(
  sqlite3 *db,
  const ProllyHash *pAncRoot,
  const ProllyHash *pSideRoot,
  u8 ancFlags, u8 sideFlags,
  const char *zAncSql, const char *zSideSql, const char *zTable,
  int *aSideAnc, int nSide, char **pzErrMsg
){
  ParsedColumn *aAnc = 0, *aSide = 0;
  int nAnc = 0, nParsed = 0;
  DoltliteColInfo ancCi, sideCi;
  DoltliteRecordInfo ancInfo, sideInfo;
  ProllyCursor ancCur, sideCur;
  u8 *aCandidate = 0;
  int i, k, j, res, rc, bPending = 0, curInit = 0;
  const char *zAmbiguous = 0;

  for(i=0; i<nSide; i++) if( aSideAnc[i]<0 ) bPending = 1;
  if( !bPending ) return SQLITE_OK;
  memset(&ancCi, 0, sizeof(ancCi));
  memset(&sideCi, 0, sizeof(sideCi));
  doltliteRecordInfoInit(&ancInfo);
  doltliteRecordInfoInit(&sideInfo);
  rc = parseColumns(zAncSql, &aAnc, &nAnc);
  if( rc==SQLITE_OK ) rc = parseColumns(zSideSql, &aSide, &nParsed);
  if( rc!=SQLITE_OK || nParsed!=nSide || nAnc==0 ) goto done;
  aCandidate = sqlite3_malloc64((u64)nSide * nAnc);
  if( !aCandidate ){ rc = SQLITE_NOMEM; goto done; }
  memset(aCandidate, 0, (size_t)nSide * nAnc);
  bPending = 0;
  for(i=0; i<nSide; i++){
    int nHit = 0, hit = -1, first = 0, end = nAnc;
    if( aSideAnc[i]>=0 ) continue;
    for(j=i-1; j>=0; j--){
      if( aSideAnc[j]>=0 ){ first = aSideAnc[j]+1; break; }
    }
    for(j=i+1; j<nSide; j++){
      if( aSideAnc[j]>=0 ){ end = aSideAnc[j]; break; }
    }
    for(k=first; k<end; k++){
      for(j=0; j<nSide && aSideAnc[j]!=k; j++){}
      if( j<nSide || !parsedColumnDefinitionsMatch(&aSide[i], &aAnc[k]) ){
        continue;
      }
      aCandidate[i*nAnc+k] = 1;
      nHit++;
      hit = k;
    }
    if( nHit==1 ){
      aSideAnc[i] = hit;
      aCandidate[i*nAnc+hit] = 0;
    }else if( nHit>1 ){
      bPending = 1;
    }
  }
  if( !bPending ) goto done;
  rc = mergeLoadReaderColumns(zAncSql, zTable, &ancCi);
  if( rc==SQLITE_OK ) rc = mergeLoadReaderColumns(zSideSql, zTable, &sideCi);
  if( rc!=SQLITE_OK ) goto done;
  if( ancCi.nCol!=nAnc || sideCi.nCol!=nSide ){
    rc = SQLITE_ERROR;
    goto done;
  }
  if( !prollyHashIsEmpty(pAncRoot) && !prollyHashIsEmpty(pSideRoot)
   && ((ancFlags ^ sideFlags) & PROLLY_NODE_INTKEY)==0 ){
    ChunkStore *cs = doltliteGetChunkStore(db);
    ProllyCache *cache = doltliteGetCache(db);
    prollyCursorInit(&ancCur, cs, cache, pAncRoot, ancFlags);
    prollyCursorInit(&sideCur, cs, cache, pSideRoot, sideFlags);
    curInit = 1;
    rc = prollyCursorFirst(&ancCur, &res);
    while( rc==SQLITE_OK && !res && prollyCursorIsValid(&ancCur) ){
      const u8 *pAncVal, *pSideVal, *pKey;
      int nAncVal, nSideVal, nKey, sideRes;
      if( ancFlags & PROLLY_NODE_INTKEY ){
        rc = prollyCursorSeekInt(&sideCur, prollyCursorIntKey(&ancCur), &sideRes);
      }else{
        prollyCursorKey(&ancCur, &pKey, &nKey);
        rc = prollyCursorSeekBlob(&sideCur, pKey, nKey, &sideRes);
      }
      if( rc!=SQLITE_OK ) break;
      if( sideRes==0 && prollyCursorIsValid(&sideCur) ){
        prollyCursorValue(&ancCur, &pAncVal, &nAncVal);
        prollyCursorValue(&sideCur, &pSideVal, &nSideVal);
        rc = doltliteParseRecordStrict(pAncVal, nAncVal, &ancInfo);
        if( rc==SQLITE_OK ){
          rc = doltliteParseRecordStrict(pSideVal, nSideVal, &sideInfo);
        }
        if( rc!=SQLITE_OK ) break;
        for(i=0; i<nSide; i++){
          for(k=0; k<nAnc; k++){
            if( aCandidate[i*nAnc+k]==1
             && !mergeStoredFieldsEqual(
                  pSideVal, nSideVal, &sideInfo, mergeReaderRecordSlot(&sideCi, i),
                  pAncVal, nAncVal, &ancInfo, mergeReaderRecordSlot(&ancCi, k)) ){
              aCandidate[i*nAnc+k] = 2;
            }
          }
        }
      }
      rc = prollyCursorNext(&ancCur);
    }
  }
  if( rc!=SQLITE_OK ) goto done;
  for(i=0; i<nSide; i++){
    int nHit = 0, hit = -1, bPossible = 0;
    for(k=0; k<nAnc; k++){
      if( aCandidate[i*nAnc+k] ) bPossible = 1;
      if( aCandidate[i*nAnc+k]==1 ){ nHit++; hit = k; }
    }
    if( !bPossible ) continue;
    if( nHit!=1 ){
      zAmbiguous = aSide[i].zName;
      rc = SQLITE_ERROR;
      goto done;
    }
    for(j=0; j<nSide; j++){
      if( aSideAnc[j]==hit ){
        zAmbiguous = aSide[i].zName;
        rc = SQLITE_ERROR;
        goto done;
      }
    }
    aSideAnc[i] = hit;
  }
done:
  if( zAmbiguous && pzErrMsg ){
    sqlite3_free(*pzErrMsg);
    *pzErrMsg = sqlite3_mprintf(
        "cannot merge: column '%s' in table '%s' has ambiguous ancestry",
        zAmbiguous, zTable);
    if( !*pzErrMsg ) rc = SQLITE_NOMEM;
  }
  if( curInit ){
    prollyCursorClose(&ancCur);
    prollyCursorClose(&sideCur);
  }
  doltliteRecordInfoClear(&ancInfo);
  doltliteRecordInfoClear(&sideInfo);
  doltliteFreeColInfo(&ancCi);
  doltliteFreeColInfo(&sideCi);
  freeColumns(aAnc, nAnc);
  freeColumns(aSide, nParsed);
  sqlite3_free(aCandidate);
  return rc;
}

void mergeMapColumnsToAncestor(
  ParsedColumn *aAnc, int nAnc,
  ParsedColumn *aSide, int nSide,
  int *aSideAnc
){
  int i, start = 0, last = -1;
  for(i=0; i<nSide; i++){
    aSideAnc[i] = parsedColumnIndexByName(aAnc, nAnc, aSide[i].zName);
  }
  while( start<nSide ){
    int end = start, next, nMissing = 0, k;
    if( aSideAnc[start]>=0 ){
      last = aSideAnc[start++];
      continue;
    }
    while( end<nSide && aSideAnc[end]<0 ) end++;
    next = end<nSide ? aSideAnc[end] : nAnc;
    for(k=last+1; k<next; k++){
      if( parsedColumnIndexByName(aSide, nSide, aAnc[k].zName)<0 ){
        nMissing++;
      }
    }
    if( nMissing<=end-start ){
      i = start;
      for(k=last+1; k<next; k++){
        if( parsedColumnIndexByName(aSide, nSide, aAnc[k].zName)<0 ){
          if( parsedColumnDefinitionsMatch(&aSide[i], &aAnc[k]) ){
            aSideAnc[i] = k;
          }
          i++;
        }
      }
    }
    start = end;
  }
}

static ParsedColumn *findColumn(ParsedColumn *aCols, int nCols, const char *zName){
  int i = parsedColumnIndexByName(aCols, nCols, zName);
  return i>=0 ? &aCols[i] : 0;
}

static int schemaGetToken(
  const char *z,
  const char *zEnd,
  int *pType,
  int *pnToken
){
  i64 n;
  if( z>=zEnd ) return SQLITE_CORRUPT;
  n = sqlite3GetToken((const u8*)z, pType);
  if( n<=0 || n>zEnd-z || *pType==TK_ILLEGAL ) return SQLITE_CORRUPT;
  *pnToken = (int)n;
  return SQLITE_OK;
}

static int schemaNextSignificantToken(
  const char *z,
  const char *zEnd,
  const char **pzToken,
  int *pType,
  int *pnToken
){
  int rc;
  while( z<zEnd ){
    rc = schemaGetToken(z, zEnd, pType, pnToken);
    if( rc!=SQLITE_OK ) return rc;
    if( *pType!=TK_SPACE && *pType!=TK_COMMENT ){
      *pzToken = z;
      return SQLITE_OK;
    }
    z += *pnToken;
  }
  return SQLITE_CORRUPT;
}

static const char *schemaSkipTrivia(const char *z, const char *zEnd){
  int type, n;
  while( z<zEnd && schemaGetToken(z, zEnd, &type, &n)==SQLITE_OK
      && (type==TK_SPACE || type==TK_COMMENT) ){
    z += n;
  }
  return z;
}

static int schemaTokensEquivalent(
  const char *zLeft,
  const char *zLeftEnd,
  const char *zRight,
  const char *zRightEnd
){
  while( 1 ){
    int leftType, leftLen;
    int rightType, rightLen;
    zLeft = schemaSkipTrivia(zLeft, zLeftEnd);
    zRight = schemaSkipTrivia(zRight, zRightEnd);
    if( zLeft==zLeftEnd || zRight==zRightEnd ){
      return zLeft==zLeftEnd && zRight==zRightEnd;
    }
    if( schemaGetToken(zLeft, zLeftEnd, &leftType, &leftLen)!=SQLITE_OK
     || schemaGetToken(zRight, zRightEnd, &rightType, &rightLen)!=SQLITE_OK
     || leftType!=rightType || leftLen!=rightLen ){
      return 0;
    }
    if( leftType==TK_STRING || leftType==TK_BLOB
     || leftType==TK_INTEGER || leftType==TK_FLOAT ){
      if( memcmp(zLeft, zRight, leftLen)!=0 ) return 0;
    }else if( sqlite3_strnicmp(zLeft, zRight, leftLen)!=0 ){
      return 0;
    }
    zLeft += leftLen;
    zRight += rightLen;
  }
}

int schemaDefinitionsEquivalent(const char *zLeft, const char *zRight){
  return schemaTokensEquivalent(
      zLeft, zLeft + strlen(zLeft), zRight, zRight + strlen(zRight));
}

static int schemaSkipParenthesized(
  const char *z,
  const char *zEnd,
  const char **pzAfter
){
  int depth = 0;
  while( z<zEnd ){
    int type, n;
    int rc = schemaGetToken(z, zEnd, &type, &n);
    if( rc!=SQLITE_OK ) return rc;
    if( type==TK_LP ){
      depth++;
    }else if( type==TK_RP ){
      if( --depth==0 ){
        *pzAfter = z + n;
        return SQLITE_OK;
      }
      if( depth<0 ) return SQLITE_CORRUPT;
    }
    z += n;
  }
  return SQLITE_CORRUPT;
}

static int schemaSegmentIsTableConstraint(
  const char *s,
  const char *e,
  int *pIsConstraint
){
  const char *zToken;
  int type, n;
  int rc = schemaNextSignificantToken(s, e, &zToken, &type, &n);
  if( rc!=SQLITE_OK ) return rc;
  *pIsConstraint = type==TK_PRIMARY || type==TK_UNIQUE || type==TK_CHECK
                || type==TK_FOREIGN || type==TK_CONSTRAINT;
  return SQLITE_OK;
}

static const char *schemaColumnDefinitionTail(const char *zDef){
  const char *zEnd = zDef + strlen(zDef);
  const char *zToken = zDef;
  int type, n;
  if( schemaNextSignificantToken(zDef, zEnd, &zToken, &type, &n)!=SQLITE_OK ){
    return zEnd;
  }
  return schemaSkipTrivia(zToken + n, zEnd);
}

int parsedColumnDefinitionsMatch(
  const ParsedColumn *pA,
  const ParsedColumn *pB
){
  return schemaDefinitionsEquivalent(schemaColumnDefinitionTail(pA->zDef),
                                     schemaColumnDefinitionTail(pB->zDef));
}

static const char *schemaFindToken(
  const char *z,
  const char *zEnd,
  const char *zKw,
  int nKw
);

static int schemaTokenTextIs(const char *z, int n, const char *zText){
  int nText = (int)strlen(zText);
  return n==nText && sqlite3_strnicmp(z, zText, n)==0;
}

static const char *schemaGeneratedTailStart(const char *zDef, int *pbVirtual){
  const char *zEnd = zDef + strlen(zDef);
  const char *zGenerated;
  const char *zAs;
  const char *p;
  int type, n;
  int bVirtual = 1;

  zGenerated = schemaFindToken(zDef, zEnd, "GENERATED", 9);
  if( zGenerated ){
    if( schemaGetToken(zGenerated, zEnd, &type, &n)!=SQLITE_OK ) return 0;
    p = schemaSkipTrivia(zGenerated + n, zEnd);
    if( p<zEnd && schemaGetToken(p, zEnd, &type, &n)==SQLITE_OK
     && type==TK_ALWAYS ){
      p = schemaSkipTrivia(p + n, zEnd);
    }
    if( p>=zEnd || schemaGetToken(p, zEnd, &type, &n)!=SQLITE_OK
     || type!=TK_AS ) return 0;
    zAs = p;
  }else{
    zAs = schemaFindToken(zDef, zEnd, "AS", 2);
    if( !zAs ) return 0;
  }

  if( schemaGetToken(zAs, zEnd, &type, &n)!=SQLITE_OK ) return 0;
  p = schemaSkipTrivia(zAs + n, zEnd);
  if( p>=zEnd || schemaGetToken(p, zEnd, &type, &n)!=SQLITE_OK
   || type!=TK_LP ) return 0;
  if( schemaSkipParenthesized(p, zEnd, &p)!=SQLITE_OK ) return 0;
  p = schemaSkipTrivia(p, zEnd);
  if( p<zEnd ){
    if( schemaGetToken(p, zEnd, &type, &n)!=SQLITE_OK ) return 0;
    if( type==TK_VIRTUAL || schemaTokenTextIs(p, n, "STORED") ){
      if( schemaTokenTextIs(p, n, "STORED") ) bVirtual = 0;
      p += n;
    }
  }
  p = schemaSkipTrivia(p, zEnd);
  if( p!=zEnd ) return 0;
  if( pbVirtual ) *pbVirtual = bVirtual;
  return zGenerated ? zGenerated : zAs;
}

int parsedColumnIsVirtual(const ParsedColumn *pCol){
  int bVirtual = 0;
  if( !pCol || !pCol->zDef ) return 0;
  return schemaGeneratedTailStart(pCol->zDef, &bVirtual) && bVirtual;
}

int parsedColumnAddsAsNull(const ParsedColumn *pCol){
  const char *zEnd;
  if( !pCol || !pCol->zDef ) return 1;
  zEnd = pCol->zDef + strlen(pCol->zDef);
  return schemaFindToken(pCol->zDef, zEnd, "DEFAULT", 7)==0
      && schemaGeneratedTailStart(pCol->zDef, 0)==0;
}

static int schemaColumnsMergeEquivalent(
  const char *zOurs,
  const char *zTheirs
){
  const char *zOurTail;
  const char *zTheirTail;
  int nOurs;
  int nTheirs;

  if( schemaDefinitionsEquivalent(zOurs, zTheirs) ) return 1;
  zOurTail = schemaGeneratedTailStart(zOurs, 0);
  zTheirTail = schemaGeneratedTailStart(zTheirs, 0);
  if( !zOurTail && !zTheirTail ) return 0;
  nOurs = zOurTail ? (int)(zOurTail-zOurs) : (int)strlen(zOurs);
  nTheirs = zTheirTail ? (int)(zTheirTail-zTheirs) : (int)strlen(zTheirs);
  return schemaTokensEquivalent(
      zOurs, zOurs + nOurs, zTheirs, zTheirs + nTheirs);
}

static const char *schemaFindToken(
  const char *z,
  const char *zEnd,
  const char *zKw,
  int nKw
){
  const char *p = z;
  if( !z || !zEnd || zEnd<=z || nKw<=0 ) return 0;
  while( p<zEnd ){
    int type, n;
    if( schemaGetToken(p, zEnd, &type, &n)!=SQLITE_OK ) return 0;
    if( type!=TK_STRING && type!=TK_BLOB && type!=TK_COMMENT
     && type!=TK_SPACE && n==nKw && sqlite3_strnicmp(p, zKw, nKw)==0 ){
      return p;
    }
    p += n;
  }
  return 0;
}

static int schemaAppendSig(char **pzSig, const char *zText, int nText){
  char *zNew;
  int nOld = *pzSig ? (int)strlen(*pzSig) : 0;
  if( nText<0 ) nText = (int)strlen(zText);
  if( nText<=0 ) return SQLITE_OK;
  zNew = sqlite3_realloc(*pzSig, nOld + nText + 2);
  if( !zNew ) return SQLITE_NOMEM;
  if( nOld ) zNew[nOld++] = '\n';
  memcpy(zNew + nOld, zText, nText);
  zNew[nOld + nText] = 0;
  *pzSig = zNew;
  return SQLITE_OK;
}

static int schemaConstraintKind(const char *s, int len){
  const char *e = s + len;
  const char *p;
  int type, n;
  if( schemaNextSignificantToken(s, e, &p, &type, &n)!=SQLITE_OK ){
    return SCHEMA_IR_OTHER;
  }
  if( type==TK_CONSTRAINT ){
    if( schemaNextSignificantToken(p+n, e, &p, &type, &n)!=SQLITE_OK ){
      return SCHEMA_IR_OTHER;
    }
    if( schemaNextSignificantToken(p+n, e, &p, &type, &n)!=SQLITE_OK ){
      return SCHEMA_IR_OTHER;
    }
  }
  if( type==TK_FOREIGN ) return SCHEMA_IR_FK;
  if( type==TK_CHECK ) return SCHEMA_IR_CHECK;
  if( schemaFindToken(p, e, "REFERENCES", 10) ) return SCHEMA_IR_FK;
  if( schemaFindToken(p, e, "CHECK", 5) ) return SCHEMA_IR_CHECK;
  return SCHEMA_IR_OTHER;
}

static char *schemaColumnWithoutChecks(const char *zDef){
  int n = (int)strlen(zDef);
  char *zOut = sqlite3_malloc(n + 1);
  const char *z = zDef;
  const char *zEnd = zDef + n;
  char *zWrite = zOut;
  if( !zOut ) return 0;
  while( z<zEnd ){
    const char *zKw = schemaFindToken(z, zEnd, "CHECK", 5);
    if( !zKw ){
      memcpy(zWrite, z, (size_t)(zEnd - z));
      zWrite += (zEnd - z);
      break;
    }
    if( zKw>z ){
      memcpy(zWrite, z, (size_t)(zKw - z));
      zWrite += (zKw - z);
    }
    z = zKw + 5;
    z = schemaSkipTrivia(z, zEnd);
    if( z<zEnd ){
      int type, nToken;
      const char *zAfter;
      if( schemaGetToken(z, zEnd, &type, &nToken)==SQLITE_OK
       && type==TK_LP
       && schemaSkipParenthesized(z, zEnd, &zAfter)==SQLITE_OK ){
        z = zAfter;
      }
    }
  }
  while( zWrite>zOut && isspace((unsigned char)zWrite[-1]) ) zWrite--;
  *zWrite = 0;
  return zOut;
}

static void schemaIrClear(SchemaIr *pIr){
  assert( pIr!=0 );
  freeColumns(pIr->aCols, pIr->nCols);
  sqlite3_free(pIr->zFkSig);
  sqlite3_free(pIr->zCheckSig);
  memset(pIr, 0, sizeof(*pIr));
}

static int schemaIrNoteColumnConstraints(SchemaIr *pIr, const char *zDef){
  const char *zEnd;
  const char *zFk;
  const char *zCk;
  int rc;
  assert( pIr!=0 && zDef!=0 );
  zEnd = zDef + strlen(zDef);
  zFk = schemaFindToken(zDef, zEnd, "REFERENCES", 10);
  zCk = zDef;
  if( zFk ){
    pIr->hasFk = 1;
    rc = schemaAppendSig(&pIr->zFkSig, zFk, (int)(zEnd - zFk));
    if( rc!=SQLITE_OK ) return rc;
  }
  while( (zCk = schemaFindToken(zCk, zEnd, "CHECK", 5))!=0 ){
    const char *zStart = zCk;
    const char *z = zCk + 5;
    pIr->hasCheck = 1;
    z = schemaSkipTrivia(z, zEnd);
    if( z<zEnd ){
      int type, nToken;
      const char *zAfter;
      rc = schemaGetToken(z, zEnd, &type, &nToken);
      if( rc!=SQLITE_OK ) return rc;
      if( type==TK_LP ){
        rc = schemaSkipParenthesized(z, zEnd, &zAfter);
        if( rc!=SQLITE_OK ) return rc;
        z = zAfter;
      }
    }
    rc = schemaAppendSig(&pIr->zCheckSig, zStart, (int)(z - zStart));
    if( rc!=SQLITE_OK ) return rc;
    zCk = z;
  }
  return SQLITE_OK;
}

static int schemaIrAddSegment(
  SchemaIr *pIr,
  int *pnAlloc,
  const char *s,
  const char *e
){
  const char *zNameToken;
  char *zDef;
  char *zName;
  int isConstraint;
  int nameType;
  int nName;
  int len;
  int rc;
  int i;

  while( s<e && isspace((unsigned char)*s) ) s++;
  while( e>s && isspace((unsigned char)e[-1]) ) e--;
  if( s==e ) return SQLITE_OK;
  rc = schemaSegmentIsTableConstraint(s, e, &isConstraint);
  if( rc!=SQLITE_OK ) return rc;
  len = (int)(e - s);
  if( isConstraint ){
    int kind = schemaConstraintKind(s, len);
    if( kind==SCHEMA_IR_FK ){
      pIr->hasFk = 1;
      return schemaAppendSig(&pIr->zFkSig, s, len);
    }
    if( kind==SCHEMA_IR_CHECK ){
      pIr->hasCheck = 1;
      return schemaAppendSig(&pIr->zCheckSig, s, len);
    }
    return SQLITE_OK;
  }

  rc = schemaNextSignificantToken(
      s, e, &zNameToken, &nameType, &nName);
  if( rc!=SQLITE_OK ) return rc;
  zDef = sqlite3_malloc(len + 1);
  zName = sqlite3_malloc(nName + 1);
  if( !zDef || !zName ){
    sqlite3_free(zDef);
    sqlite3_free(zName);
    return SQLITE_NOMEM;
  }
  memcpy(zDef, s, len);
  zDef[len] = 0;
  memcpy(zName, zNameToken, nName);
  zName[nName] = 0;
  sqlite3Dequote(zName);
  for(i=0; zName[i]; i++){
    zName[i] = (char)tolower((unsigned char)zName[i]);
  }
  rc = DOLTLITE_GROW_ARRAY(&pIr->aCols, pnAlloc, pIr->nCols+1, 8);
  if( rc!=SQLITE_OK ){
    sqlite3_free(zDef);
    sqlite3_free(zName);
    return rc;
  }
  pIr->aCols[pIr->nCols].zName = zName;
  pIr->aCols[pIr->nCols].zDef = zDef;
  pIr->nCols++;
  return schemaIrNoteColumnConstraints(pIr, zDef);
}

static int schemaIrBuild(const char *zSql, SchemaIr *pIr){
  const char *p;
  const char *zEnd;
  const char *segStart;
  int depth = 0;
  int nAlloc = 0;
  int rc;
  assert( pIr!=0 );

  memset(pIr, 0, sizeof(*pIr));
  if( !zSql ) return SQLITE_OK;
  p = zSql;
  zEnd = zSql + strlen(zSql);
  while( p<zEnd ){
    int type, n;
    rc = schemaGetToken(p, zEnd, &type, &n);
    if( rc!=SQLITE_OK ) goto schema_ir_build_error;
    p += n;
    if( type==TK_LP ){
      depth = 1;
      break;
    }
  }
  if( depth==0 ){
    rc = SQLITE_CORRUPT;
    goto schema_ir_build_error;
  }

  segStart = p;
  while( p<zEnd ){
    int type, n;
    rc = schemaGetToken(p, zEnd, &type, &n);
    if( rc!=SQLITE_OK ) goto schema_ir_build_error;
    if( type==TK_LP ){
      depth++;
    }else if( type==TK_RP ){
      depth--;
      if( depth==0 ){
        rc = schemaIrAddSegment(pIr, &nAlloc, segStart, p);
        if( rc!=SQLITE_OK ) goto schema_ir_build_error;
        return SQLITE_OK;
      }
      if( depth<0 ){
        rc = SQLITE_CORRUPT;
        goto schema_ir_build_error;
      }
    }else if( type==TK_COMMA && depth==1 ){
      rc = schemaIrAddSegment(pIr, &nAlloc, segStart, p);
      if( rc!=SQLITE_OK ) goto schema_ir_build_error;
      segStart = p + n;
    }
    p += n;
  }
  rc = SQLITE_CORRUPT;

schema_ir_build_error:
  schemaIrClear(pIr);
  return rc;
}

static int schemaIrAncestorColumnsSame(
  const SchemaIr *pAncestor,
  const SchemaIr *pSide,
  int ignoreChecks,
  int *pSame
){
  int i;
  *pSame = 1;
  for(i=0; i<pAncestor->nCols; i++){
    ParsedColumn *pSideCol = findColumn(
        pSide->aCols, pSide->nCols, pAncestor->aCols[i].zName);
    if( !pSideCol ){
      *pSame = 0;
      break;
    }
    if( ignoreChecks ){
      char *zAncestor = schemaColumnWithoutChecks(pAncestor->aCols[i].zDef);
      char *zSide = schemaColumnWithoutChecks(pSideCol->zDef);
      if( !zAncestor || !zSide ){
        sqlite3_free(zAncestor);
        sqlite3_free(zSide);
        return SQLITE_NOMEM;
      }
      if( !schemaDefinitionsEquivalent(zAncestor, zSide) ) *pSame = 0;
      sqlite3_free(zAncestor);
      sqlite3_free(zSide);
    }else if( !schemaDefinitionsEquivalent(
                 pAncestor->aCols[i].zDef, pSideCol->zDef) ){
      *pSame = 0;
    }
    if( !*pSame ) break;
  }
  return SQLITE_OK;
}

static int schemaIrSignaturesSame(const char *zLeft, const char *zRight){
  const char *zL = zLeft ? zLeft : "";
  const char *zR = zRight ? zRight : "";
  return schemaDefinitionsEquivalent(zL, zR);
}

static int schemaConstraintModifyDeleteChoice(
  const char *zAncSql,
  const char *zOursSql,
  const char *zTheirsSql,
  int *pChoice
){
  SchemaIr anc, ours, theirs;
  int sameAO = 0, sameAT = 0;
  int rc;
  *pChoice = SCHEMA_MERGE_DEFAULT;

  rc = schemaIrBuild(zAncSql, &anc);
  if( rc!=SQLITE_OK ) return rc;
  rc = schemaIrBuild(zOursSql, &ours);
  if( rc!=SQLITE_OK ){ schemaIrClear(&anc); return rc; }
  rc = schemaIrBuild(zTheirsSql, &theirs);
  if( rc!=SQLITE_OK ){
    schemaIrClear(&anc);
    schemaIrClear(&ours);
    return rc;
  }

  if( anc.hasFk && ours.hasFk!=theirs.hasFk
   && schemaIrSignaturesSame(anc.zCheckSig, ours.zCheckSig)
   && schemaIrSignaturesSame(anc.zCheckSig, theirs.zCheckSig) ){
    rc = schemaIrAncestorColumnsSame(&anc, &ours, 0, &sameAO);
    if( rc==SQLITE_OK ){
      rc = schemaIrAncestorColumnsSame(&anc, &theirs, 0, &sameAT);
    }
    if( rc==SQLITE_OK && sameAO && sameAT ){
      *pChoice = ours.hasFk ? SCHEMA_MERGE_THEIRS : SCHEMA_MERGE_OURS;
      schemaIrClear(&anc);
      schemaIrClear(&ours);
      schemaIrClear(&theirs);
      return SQLITE_OK;
    }
  }

  if( rc==SQLITE_OK
   && anc.hasCheck && ours.hasCheck!=theirs.hasCheck
   && schemaIrSignaturesSame(anc.zFkSig, ours.zFkSig)
   && schemaIrSignaturesSame(anc.zFkSig, theirs.zFkSig) ){
    const SchemaIr *pSurvivor = ours.hasCheck ? &ours : &theirs;
    rc = schemaIrAncestorColumnsSame(&anc, &ours, 1, &sameAO);
    if( rc==SQLITE_OK ){
      rc = schemaIrAncestorColumnsSame(&anc, &theirs, 1, &sameAT);
    }
    if( rc==SQLITE_OK && sameAO && sameAT ){
      if( schemaIrSignaturesSame(anc.zCheckSig, pSurvivor->zCheckSig) ){
        *pChoice = ours.hasCheck ? SCHEMA_MERGE_THEIRS : SCHEMA_MERGE_OURS;
      }else{
        *pChoice = ours.hasCheck ? SCHEMA_MERGE_OURS : SCHEMA_MERGE_THEIRS;
      }
    }
  }

  schemaIrClear(&anc);
  schemaIrClear(&ours);
  schemaIrClear(&theirs);
  return rc;
}

static int schemaUniqueIndexesSame(Table *pA, Table *pB){
  Index *pLeft, *pRight;
  int nA = 0, nB = 0;
  for(pRight=pB->pIndex; pRight; pRight=pRight->pNext){
    if( pRight->idxType==SQLITE_IDXTYPE_UNIQUE ) nB++;
  }
  for(pLeft=pA->pIndex; pLeft; pLeft=pLeft->pNext){
    int i;
    if( pLeft->idxType!=SQLITE_IDXTYPE_UNIQUE ) continue;
    nA++;
    for(pRight=pB->pIndex; pRight; pRight=pRight->pNext){
      if( pRight->idxType!=SQLITE_IDXTYPE_UNIQUE
       || pLeft->nKeyCol!=pRight->nKeyCol
       || pLeft->onError!=pRight->onError ) continue;
      for(i=0; i<pLeft->nKeyCol; i++){
        if( sqlite3_stricmp(pA->aCol[pLeft->aiColumn[i]].zCnName,
                           pB->aCol[pRight->aiColumn[i]].zCnName)!=0
         || sqlite3_stricmp(pLeft->azColl[i],pRight->azColl[i])!=0
         || pLeft->aSortOrder[i]!=pRight->aSortOrder[i] ) break;
      }
      if( i==pLeft->nKeyCol ) break;
    }
    if( !pRight ) return 0;
  }
  return nA==nB;
}

int schemaUniqueSideChoice(
  const char *zAnc, const char *zOurs, const char *zTheirs, int *pChoice
){
  const char *azSql[3] = {zAnc, zOurs, zTheirs};
  sqlite3 *aDb[3] = {0, 0, 0};
  Table *aTab[3] = {0, 0, 0};
  int i, rc = SQLITE_OK, sameOurs, sameTheirs;
  *pChoice = SCHEMA_MERGE_DEFAULT;
  for(i=0; i<3; i++){
    if( schemaFindToken(azSql[i],azSql[i]+strlen(azSql[i]),"UNIQUE",6) ) break;
  }
  if( i==3 ) return SQLITE_OK;
  for(i=0; i<3 && rc==SQLITE_OK; i++){
    sqlite3_stmt *pStmt = 0;
    rc = sqlite3_open(":memory:", &aDb[i]);
    if( rc==SQLITE_OK ) rc = sqlite3_exec(aDb[i], azSql[i], 0, 0, 0);
    if( rc==SQLITE_OK ){
      rc = sqlite3_prepare_v2(aDb[i],
          "SELECT name FROM sqlite_schema WHERE type='table'"
          " AND name<>'sqlite_sequence' LIMIT 1", -1, &pStmt, 0);
    }
    if( rc==SQLITE_OK ){
      rc = sqlite3_step(pStmt);
      if( rc==SQLITE_ROW ){
        aTab[i] = sqlite3FindTable(aDb[i],
            (const char*)sqlite3_column_text(pStmt, 0), "main");
        rc = aTab[i] ? SQLITE_OK : SQLITE_CORRUPT;
      }else if( rc==SQLITE_DONE ){
        rc = SQLITE_CORRUPT;
      }
    }
    sqlite3_finalize(pStmt);
  }
  if( rc==SQLITE_OK ){
    sameOurs = schemaUniqueIndexesSame(aTab[0], aTab[1]);
    sameTheirs = schemaUniqueIndexesSame(aTab[0], aTab[2]);
    if( sameOurs!=sameTheirs ){
      *pChoice = sameOurs ? SCHEMA_MERGE_THEIRS : SCHEMA_MERGE_OURS;
    }
  }
  for(i=0; i<3; i++) sqlite3_close(aDb[i]);
  return rc==SQLITE_NOMEM ? rc : SQLITE_OK;
}

/* True when zName sits on this side as a plain addition: absent from the
** ancestor, and not standing in an ancestor column's slot with its type,
** which is what a rename leaves behind. */
static int columnPlainlyAdded(
  ParsedColumn *aSide, int nSide,
  ParsedColumn *aAnc, int nAnc,
  const char *zName
){
  int i = parsedColumnIndexByName(aSide, nSide, zName);
  if( i<0 || findColumn(aAnc, nAnc, zName) ) return 0;
  if( i<nAnc
   && !findColumn(aSide, nSide, aAnc[i].zName)
   && parsedColumnDefinitionsMatch(&aSide[i], &aAnc[i]) ){
    return 0;
  }
  return 1;
}

/* True when the ancestor column at iAnc left this side by being renamed
** rather than dropped. The other side decides the ambiguous case: if the
** replacement's name is a plain addition over there, both sides added it
** and this side really did drop a column. */
int columnRenamedAt(
  ParsedColumn *aSide, int nSide,
  ParsedColumn *aAnc, int nAnc,
  int iAnc,
  ParsedColumn *aOther, int nOther
){
  if( iAnc>=nSide ) return 0;
  if( findColumn(aAnc, nAnc, aSide[iAnc].zName) ) return 0;
  if( !parsedColumnDefinitionsMatch(&aSide[iAnc], &aAnc[iAnc]) ) return 0;
  if( columnPlainlyAdded(aOther, nOther, aAnc, nAnc, aSide[iAnc].zName) ) return 0;
  return 1;
}

int trySchemaColumnMerge(
  const char *zAncSql,
  const char *zOursSql,
  const char *zTheirsSql,
  char ***ppAddCols, int *pnAddCols,
  char ***ppDropCols, int *pnDropCols,
  char ***ppRenameCols, int *pnRenameCols,
  int *pSchemaChoice,
  int *pResolvedDivergence,
  char **pzErrDetail
){
  ParsedColumn *aAnc=0, *aOurs=0, *aTheirs=0;
  int nAnc=0, nOurs=0, nTheirs=0;
  int rc;
  char **azAdd = 0;
  int nAdd = 0, nAddAlloc = 0;
  char **azDrop = 0;
  int nDrop = 0, nDropAlloc = 0;
  char **azRename = 0;
  int nRename = 0, nRenameAlloc = 0;
  int i;

  *ppAddCols = 0;
  *pnAddCols = 0;
  if( ppDropCols ) *ppDropCols = 0;
  if( pnDropCols ) *pnDropCols = 0;
  if( ppRenameCols ) *ppRenameCols = 0;
  if( pnRenameCols ) *pnRenameCols = 0;
  *pSchemaChoice = SCHEMA_MERGE_DEFAULT;
  *pResolvedDivergence = 0;

  rc = schemaConstraintModifyDeleteChoice(
      zAncSql, zOursSql, zTheirsSql, pSchemaChoice);
  if( rc!=SQLITE_OK ) return rc;

  rc = parseColumns(zAncSql, &aAnc, &nAnc);
  if( rc!=SQLITE_OK ) return rc;
  rc = parseColumns(zOursSql, &aOurs, &nOurs);
  if( rc!=SQLITE_OK ){ freeColumns(aAnc, nAnc); return rc; }
  rc = parseColumns(zTheirsSql, &aTheirs, &nTheirs);
  if( rc!=SQLITE_OK ){ freeColumns(aAnc, nAnc); freeColumns(aOurs, nOurs); return rc; }

  if( *pSchemaChoice!=SCHEMA_MERGE_DEFAULT ){
    ParsedColumn *aSelected = *pSchemaChoice==SCHEMA_MERGE_OURS
                            ? aOurs : aTheirs;
    ParsedColumn *aOther = *pSchemaChoice==SCHEMA_MERGE_OURS
                         ? aTheirs : aOurs;
    int nSelected = *pSchemaChoice==SCHEMA_MERGE_OURS ? nOurs : nTheirs;
    int nOther = *pSchemaChoice==SCHEMA_MERGE_OURS ? nTheirs : nOurs;
    for(i=0; i<nOther; i++){
      ParsedColumn *pSelected;
      if( findColumn(aAnc, nAnc, aOther[i].zName) ) continue;
      pSelected = findColumn(aSelected, nSelected, aOther[i].zName);
      if( pSelected ){
        if( !schemaColumnsMergeEquivalent(
                pSelected->zDef, aOther[i].zDef) ){
          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "both branches add column '%s' with different definitions",
              aOther[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }
        if( strcmp(pSelected->zDef, aOther[i].zDef)!=0 ){
          *pResolvedDivergence = 1;
        }
        continue;
      }
      rc = DOLTLITE_GROW_ARRAY(&azAdd, &nAddAlloc, nAdd+1, 4);
      if( rc!=SQLITE_OK ) goto schema_merge_cleanup;
      azAdd[nAdd] = sqlite3_mprintf("%s", aOther[i].zDef);
      if( !azAdd[nAdd] ){
        rc = SQLITE_NOMEM;
        goto schema_merge_cleanup;
      }
      nAdd++;
    }
    goto schema_merge_done;
  }

  for(i=0; i<nTheirs; i++){
    ParsedColumn *ancCol = findColumn(aAnc, nAnc, aTheirs[i].zName);
    if( !ancCol ){

      ParsedColumn *ourCol = findColumn(aOurs, nOurs, aTheirs[i].zName);
      if( ourCol ){

        if( !schemaColumnsMergeEquivalent(ourCol->zDef, aTheirs[i].zDef) ){

          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "both branches add column '%s' with different definitions",
              aTheirs[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }
        *pResolvedDivergence = 1;

      }else if( i<nAnc
             && sqlite3_stricmp(aTheirs[i].zName, aAnc[i].zName)!=0
             && !findColumn(aTheirs, nTheirs, aAnc[i].zName)
             && parsedColumnDefinitionsMatch(&aTheirs[i], &aAnc[i]) ){
        ParsedColumn *ourAncestor = findColumn(
            aOurs, nOurs, aAnc[i].zName);
        if( ourAncestor && strcmp(ourAncestor->zDef, aAnc[i].zDef)==0 ){
          *pSchemaChoice = SCHEMA_MERGE_THEIRS;
        }else{
          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "column '%s' renamed on one branch and modified on another",
              aAnc[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }

      }else{

        rc = DOLTLITE_GROW_ARRAY(&azAdd, &nAddAlloc, nAdd+1, 4);
        if( rc!=SQLITE_OK ) goto schema_merge_cleanup;
        azAdd[nAdd] = sqlite3_mprintf("%s", aTheirs[i].zDef);
        nAdd++;
      }
    }else{

      ParsedColumn *ourCol = findColumn(aOurs, nOurs, aTheirs[i].zName);
      if( ourCol ){
        int ancToTheirs = strcmp(ancCol->zDef, aTheirs[i].zDef)!=0;
        int ancToOurs = strcmp(ancCol->zDef, ourCol->zDef)!=0;
        if( ancToTheirs && ancToOurs ){

          if( strcmp(ourCol->zDef, aTheirs[i].zDef)!=0 ){

            if( pzErrDetail ){
              *pzErrDetail = sqlite3_mprintf(
                "both branches modified column '%s' differently",
                aTheirs[i].zName);
            }
            rc = SQLITE_ERROR;
            goto schema_merge_cleanup;
          }

        }
      }else{

        int theirsModified = strcmp(ancCol->zDef, aTheirs[i].zDef)!=0;
        if( theirsModified ){

          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "column '%s' modified on one branch and dropped on another",
              aTheirs[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }

      }
    }
  }

  /* The scan above missed our rename. Ours at a vanished ancestor
  ** slot with the same definition is that rename. */
  if( *pSchemaChoice==SCHEMA_MERGE_DEFAULT ){
    for(i=0; i<nOurs; i++){
      ParsedColumn *theirAncestor;
      if( findColumn(aAnc, nAnc, aOurs[i].zName)
       || findColumn(aTheirs, nTheirs, aOurs[i].zName)
       || i>=nAnc
       || sqlite3_stricmp(aOurs[i].zName, aAnc[i].zName)==0
       || findColumn(aOurs, nOurs, aAnc[i].zName)
       || !parsedColumnDefinitionsMatch(&aOurs[i], &aAnc[i]) ){
        continue;
      }
      theirAncestor = findColumn(aTheirs, nTheirs, aAnc[i].zName);
      if( theirAncestor && strcmp(theirAncestor->zDef, aAnc[i].zDef)==0 ){
        *pSchemaChoice = SCHEMA_MERGE_OURS;
      }else{
        if( pzErrDetail ){
          *pzErrDetail = sqlite3_mprintf(
            "column '%s' renamed on one branch and modified on another",
            aAnc[i].zName);
        }
        rc = SQLITE_ERROR;
        goto schema_merge_cleanup;
      }
    }
  }

  for(i=0; i<nOurs; i++){
    ParsedColumn *ancCol = findColumn(aAnc, nAnc, aOurs[i].zName);
    if( ancCol ){

      ParsedColumn *theirCol = findColumn(aTheirs, nTheirs, aOurs[i].zName);
      if( !theirCol ){

        int oursModified = strcmp(ancCol->zDef, aOurs[i].zDef)!=0;
        if( oursModified ){

          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "column '%s' modified on one branch and dropped on another",
              aOurs[i].zName);
          }
          rc = SQLITE_ERROR;
          goto schema_merge_cleanup;
        }

      }
    }

  }

  {
    int uniqueChoice = SCHEMA_MERGE_DEFAULT;
    rc = schemaUniqueSideChoice(zAncSql, zOursSql, zTheirsSql, &uniqueChoice);
    if( rc!=SQLITE_OK ) goto schema_merge_cleanup;
    if( uniqueChoice!=SCHEMA_MERGE_DEFAULT ) *pSchemaChoice = uniqueChoice;
  }

  if( *pSchemaChoice==SCHEMA_MERGE_THEIRS ){
    int j;
    for(j=0; j<nAdd; j++) sqlite3_free(azAdd[j]);
    sqlite3_free(azAdd);
    azAdd = 0;
    nAdd = 0;
    nAddAlloc = 0;
    for(i=0; i<nOurs; i++){
      /* Our rename: adopted schema still uses the old name, so do not
      ** replay the new name as an ADD. The rename pass carries it. */
      if( i<nAnc
       && !findColumn(aAnc, nAnc, aOurs[i].zName)
       && !findColumn(aOurs, nOurs, aAnc[i].zName)
       && findColumn(aTheirs, nTheirs, aAnc[i].zName)
       && parsedColumnDefinitionsMatch(&aOurs[i], &aAnc[i]) ){
        continue;
      }
      if( !findColumn(aAnc, nAnc, aOurs[i].zName)
       && !findColumn(aTheirs, nTheirs, aOurs[i].zName) ){
        rc = DOLTLITE_GROW_ARRAY(&azAdd, &nAddAlloc, nAdd+1, 4);
        if( rc!=SQLITE_OK ) goto schema_merge_cleanup;
        azAdd[nAdd] = sqlite3_mprintf("%s", aOurs[i].zDef);
        if( !azAdd[nAdd] ){
          rc = SQLITE_NOMEM;
          goto schema_merge_cleanup;
        }
        nAdd++;
      }
    }
  }

schema_merge_done:
  /* Both branches independently adding a column of the same name is only
  ** mergeable when they dropped the same number of ancestor columns; Dolt
  ** reports a schema conflict otherwise, whichever columns went and wherever
  ** the added one sits. Renames must not be counted as drops, and a rename is
  ** what a same-position replacement of matching type looks like -- so the
  ** shared name has to turn up somewhere it cannot be a rename before this
  ** reads the pair as two independent adds. */
  {
    int nDropOurs = 0, nDropTheirs = 0, iAsym = -1;
    for(i=0; i<nAnc; i++){
      int inOurs = findColumn(aOurs, nOurs, aAnc[i].zName)!=0;
      int inTheirs = findColumn(aTheirs, nTheirs, aAnc[i].zName)!=0;
      if( !inOurs && !columnRenamedAt(aOurs, nOurs, aAnc, nAnc, i, aTheirs, nTheirs) ){
        nDropOurs++;
      }
      if( !inTheirs && !columnRenamedAt(aTheirs, nTheirs, aAnc, nAnc, i, aOurs, nOurs) ){
        nDropTheirs++;
      }
      if( inOurs!=inTheirs && iAsym<0 ) iAsym = i;
    }
    if( nDropOurs!=nDropTheirs && iAsym>=0 ){
      for(i=0; i<nOurs; i++){
        if( findColumn(aAnc, nAnc, aOurs[i].zName) ) continue;
        if( !findColumn(aTheirs, nTheirs, aOurs[i].zName) ) continue;
        if( !columnPlainlyAdded(aOurs, nOurs, aAnc, nAnc, aOurs[i].zName)
         && !columnPlainlyAdded(aTheirs, nTheirs, aAnc, nAnc, aOurs[i].zName) ){
          continue;
        }
        if( pzErrDetail ){
          *pzErrDetail = sqlite3_mprintf(
            "column '%s' dropped on one branch while both branches add column '%s'",
            aAnc[iAsym].zName, aOurs[i].zName);
        }
        rc = SQLITE_ERROR;
        goto schema_merge_cleanup;
      }
    }
  }

  /* Carry the other side's deletions. A rename is absent under the
  ** old name too; same-position replacement distinguishes it. */
  if( ppDropCols && pnDropCols ){
    ParsedColumn *aSel = *pSchemaChoice==SCHEMA_MERGE_THEIRS ? aTheirs : aOurs;
    ParsedColumn *aOth = *pSchemaChoice==SCHEMA_MERGE_THEIRS ? aOurs : aTheirs;
    int nSel = *pSchemaChoice==SCHEMA_MERGE_THEIRS ? nTheirs : nOurs;
    int nOth = *pSchemaChoice==SCHEMA_MERGE_THEIRS ? nOurs : nTheirs;
    for(i=0; i<nAnc; i++){
      if( !findColumn(aSel, nSel, aAnc[i].zName) ) continue;
      if( findColumn(aOth, nOth, aAnc[i].zName) ) continue;
      if( columnRenamedAt(aOth, nOth, aAnc, nAnc, i, aSel, nSel) ){
        /* Renamed, not deleted. Carry the new name or it is lost. */
        if( ppRenameCols && pnRenameCols ){
          rc = DOLTLITE_GROW_ARRAY(&azRename, &nRenameAlloc, nRename+2, 4);
          if( rc!=SQLITE_OK ) goto schema_merge_cleanup;
          azRename[nRename] = sqlite3_mprintf("%s", aAnc[i].zName);
          azRename[nRename+1] = sqlite3_mprintf("%s", aOth[i].zName);
          if( !azRename[nRename] || !azRename[nRename+1] ){
            sqlite3_free(azRename[nRename]);
            sqlite3_free(azRename[nRename+1]);
            rc = SQLITE_NOMEM;
            goto schema_merge_cleanup;
          }
          nRename += 2;
        }
        continue;
      }
      rc = DOLTLITE_GROW_ARRAY(&azDrop, &nDropAlloc, nDrop+1, 4);
      if( rc!=SQLITE_OK ) goto schema_merge_cleanup;
      azDrop[nDrop] = sqlite3_mprintf("%s", aAnc[i].zName);
      if( !azDrop[nDrop] ){
        rc = SQLITE_NOMEM;
        goto schema_merge_cleanup;
      }
      nDrop++;
    }
  }
  if( rc==SQLITE_OK ){
    rc = schemaRetainedClauseConflict(
        zAncSql, zOursSql, zTheirsSql, *pSchemaChoice, pzErrDetail);
    if( rc!=SQLITE_OK ) goto schema_merge_cleanup;
  }
  *ppAddCols = azAdd;
  *pnAddCols = nAdd;
  azAdd = 0; nAdd = 0;
  if( ppDropCols && pnDropCols ){
    *ppDropCols = azDrop;
    *pnDropCols = nDrop;
    azDrop = 0; nDrop = 0;
  }
  if( ppRenameCols && pnRenameCols ){
    *ppRenameCols = azRename;
    *pnRenameCols = nRename;
    azRename = 0; nRename = 0;
  }

schema_merge_cleanup:
  freeColumns(aAnc, nAnc);
  freeColumns(aOurs, nOurs);
  freeColumns(aTheirs, nTheirs);
  { int j; for(j=0;j<nDrop;j++) sqlite3_free(azDrop[j]); }
  sqlite3_free(azDrop);
  { int j; for(j=0;j<nRename;j++) sqlite3_free(azRename[j]); }
  sqlite3_free(azRename);
  if( rc!=SQLITE_OK ){
    { int j; for(j=0;j<nAdd;j++) sqlite3_free(azAdd[j]); }
    sqlite3_free(azAdd);
  }
  return rc;
}

/* 1 when p starts [CONSTRAINT name] CHECK(...). *pzAfter is past the clause. */
static int schemaSkipCheckClause(
  const char *p,
  const char *e,
  const char **pzAfter
){
  const char *tok;
  int type, n;
  if( schemaNextSignificantToken(p, e, &tok, &type, &n)!=SQLITE_OK ) return 0;
  if( type==TK_CONSTRAINT ){
    if( schemaNextSignificantToken(tok+n, e, &tok, &type, &n)!=SQLITE_OK ) return 0;
    if( schemaNextSignificantToken(tok+n, e, &tok, &type, &n)!=SQLITE_OK ) return 0;
    if( type!=TK_CHECK ) return 0;
  }else if( type!=TK_CHECK ){
    return 0;
  }
  tok = schemaSkipTrivia(tok+n, e);
  if( tok>=e || schemaGetToken(tok, e, &type, &n)!=SQLITE_OK || type!=TK_LP ){
    return 0;
  }
  return schemaSkipParenthesized(tok, e, pzAfter)==SQLITE_OK;
}

static int schemaAppendSansChecks(sqlite3_str *pOut, const char *s, const char *e){
  const char *p = s;
  while( p<e ){
    int type, n;
    const char *after = 0;
    if( schemaGetToken(p, e, &type, &n)!=SQLITE_OK ) return SQLITE_CORRUPT;
    if( (type==TK_CHECK || type==TK_CONSTRAINT)
     && schemaSkipCheckClause(p, e, &after) ){
      p = after;
      continue;
    }
    sqlite3_str_append(pOut, p, n);
    p += n;
  }
  return SQLITE_OK;
}

static int schemaEmitKeptSegment(
  sqlite3_str *pOut,
  const char *s,
  const char *e,
  int *pnKept
){
  const char *ts = s, *te = e;
  int isConstraint = 0, rc;
  while( ts<te && isspace((unsigned char)*ts) ) ts++;
  while( te>ts && isspace((unsigned char)te[-1]) ) te--;
  if( ts==te ) return SQLITE_OK;
  rc = schemaSegmentIsTableConstraint(ts, te, &isConstraint);
  if( rc!=SQLITE_OK ) return rc;
  if( isConstraint && schemaConstraintKind(ts, (int)(te-ts))==SCHEMA_IR_CHECK ){
    const char *after = 0;
    if( !schemaSkipCheckClause(ts, te, &after) ) return SQLITE_CORRUPT;
    return SQLITE_OK;
  }
  if( *pnKept ) sqlite3_str_appendall(pOut, ",");
  (*pnKept)++;
  if( !isConstraint ) return schemaAppendSansChecks(pOut, ts, te);
  sqlite3_str_append(pOut, ts, (int)(te-ts));
  return SQLITE_OK;
}

static int schemaSqlWithoutChecks(const char *zSql, char **pzOut){
  sqlite3_str *pOut;
  const char *p, *zEnd, *seg, *zLp = 0;
  int depth = 0, nKept = 0, rc = SQLITE_OK;
  *pzOut = 0;
  if( !zSql ) return SQLITE_OK;
  p = zSql;
  zEnd = zSql + strlen(zSql);
  while( p<zEnd ){
    int type, n;
    rc = schemaGetToken(p, zEnd, &type, &n);
    if( rc!=SQLITE_OK ) return SQLITE_OK;
    if( type==TK_LP ){ zLp = p; p += n; depth = 1; break; }
    p += n;
  }
  if( !zLp ) return SQLITE_OK;
  pOut = sqlite3_str_new(0);
  if( !pOut ) return SQLITE_NOMEM;
  sqlite3_str_append(pOut, zSql, (int)((zLp+1)-zSql));
  seg = p;
  while( p<zEnd && depth>0 ){
    int type, n;
    rc = schemaGetToken(p, zEnd, &type, &n);
    if( rc!=SQLITE_OK ) break;
    if( type==TK_LP ){
      depth++;
    }else if( type==TK_RP ){
      depth--;
      if( depth==0 ){
        rc = schemaEmitKeptSegment(pOut, seg, p, &nKept);
        if( rc==SQLITE_OK ) sqlite3_str_append(pOut, p, (int)(zEnd-p));
        break;
      }
    }else if( type==TK_COMMA && depth==1 ){
      rc = schemaEmitKeptSegment(pOut, seg, p, &nKept);
      if( rc!=SQLITE_OK ) break;
      seg = p + n;
    }
    p += n;
  }
  if( rc==SQLITE_OK && depth!=0 ) rc = SQLITE_CORRUPT;
  {
    int strRc = sqlite3_str_errcode(pOut);
    if( rc!=SQLITE_OK || strRc ){
      sqlite3_str_finish(pOut);
      return (rc==SQLITE_NOMEM || strRc) ? SQLITE_NOMEM : SQLITE_OK;
    }
  }
  *pzOut = sqlite3_str_finish(pOut);
  if( !*pzOut ) *pzOut = sqlite3_mprintf("");
  return *pzOut ? SQLITE_OK : SQLITE_NOMEM;
}

int schemaNonCheckTextMatches(const char *zA, const char *zB, int *pbMatch){
  char *zLeft = 0, *zRight = 0;
  int rc;
  *pbMatch = 0;
  rc = schemaSqlWithoutChecks(zA, &zLeft);
  if( rc==SQLITE_OK ) rc = schemaSqlWithoutChecks(zB, &zRight);
  if( rc==SQLITE_OK && zLeft && zRight ){
    *pbMatch = schemaDefinitionsEquivalent(zLeft, zRight);
  }
  sqlite3_free(zLeft);
  sqlite3_free(zRight);
  return rc;
}

#endif
