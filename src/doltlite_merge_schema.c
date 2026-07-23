#ifdef DOLTLITE_PROLLY

#include "doltlite_merge_int.h"

/* CREATE TABLE SQL parsing, schema IR, and column-level schema merge. */

static int parseQuotedIdentifier(
  const char *z,
  const char *zEnd,
  const char **ppEnd,
  char **pzName
){
  char cOpen, cClose;
  const char *p;
  int nOut = 0;
  char *zName;

  *ppEnd = z;
  *pzName = 0;
  if( z>=zEnd ) return SQLITE_CORRUPT;

  cOpen = *z;
  cClose = cOpen=='[' ? ']' : cOpen;
  p = z + 1;
  while( p<zEnd ){
    if( *p==cClose ){
      if( p+1<zEnd && p[1]==cClose ){
        nOut++;
        p += 2;
        continue;
      }
      break;
    }
    nOut++;
    p++;
  }
  if( p>=zEnd || *p!=cClose ) return SQLITE_CORRUPT;

  zName = sqlite3_malloc(nOut + 1);
  if( !zName ) return SQLITE_NOMEM;

  p = z + 1;
  nOut = 0;
  while( p<zEnd && *p!=cClose ){
    if( p+1<zEnd && p[0]==cClose && p[1]==cClose ){
      zName[nOut++] = cClose;
      p += 2;
    }else{
      zName[nOut++] = *p++;
    }
  }
  zName[nOut] = 0;
  *ppEnd = p + 1;
  *pzName = zName;
  return SQLITE_OK;
}

int parseColumns(
  const char *zSql,
  ParsedColumn **ppCols, int *pnCols
){
  const char *p, *pEnd;
  int depth;
  const char *segStart;
  ParsedColumn *aCols = 0;
  int nCols = 0, nAlloc = 0;

  *ppCols = 0;
  *pnCols = 0;

  if( !zSql ) return SQLITE_OK;

  p = zSql;
  while( *p && *p!='(' ) p++;
  if( *p!='(' ) return SQLITE_CORRUPT;
  p++;

  pEnd = p;
  depth = 1;
  while( *pEnd && depth>0 ){
    if( *pEnd=='(' ) depth++;
    else if( *pEnd==')' ) depth--;
    pEnd++;
  }
  if( depth!=0 ) return SQLITE_CORRUPT;
  pEnd--;

  segStart = p;
  depth = 0;
  while( p <= pEnd ){
    if( p==pEnd || (*p==',' && depth==0) ){

      const char *s = segStart;
      const char *e = (p==pEnd) ? p : p;
      char *zTrimmed;
      int len;

      while( s<e && isspace((unsigned char)*s) ) s++;
      while( e>s && isspace((unsigned char)*(e-1)) ) e--;

      len = (int)(e - s);
      if( len > 0 ){

        int isConstraint = doltliteSegmentIsTableConstraint(s, len);

        if( !isConstraint ){

          zTrimmed = sqlite3_malloc(len + 1);
          if( !zTrimmed ){

            { int ci; for(ci=0;ci<nCols;ci++){
              sqlite3_free(aCols[ci].zName);
              sqlite3_free(aCols[ci].zDef);
            }}
            sqlite3_free(aCols);
            return SQLITE_NOMEM;
          }
          memcpy(zTrimmed, s, len);
          zTrimmed[len] = 0;

          {
            char *zName;
            const char *nameStart = s;
            const char *nameEnd = nameStart;
            int nameLen;
            int rc;

            if( *nameStart=='"' || *nameStart=='`' || *nameStart=='[' ){
              rc = parseQuotedIdentifier(nameStart, e, &nameEnd, &zName);
              if( rc!=SQLITE_OK ){
                sqlite3_free(zTrimmed);
                { int ci; for(ci=0;ci<nCols;ci++){
                  sqlite3_free(aCols[ci].zName);
                  sqlite3_free(aCols[ci].zDef);
                }}
                sqlite3_free(aCols);
                return rc;
              }
            }else{
              while( nameEnd<e && !isspace((unsigned char)*nameEnd)
                  && *nameEnd!='(' && *nameEnd!=',' ) nameEnd++;
              nameLen = (int)(nameEnd - nameStart);
              zName = sqlite3_malloc(nameLen + 1);
              if( !zName ){
                sqlite3_free(zTrimmed);
                { int ci; for(ci=0;ci<nCols;ci++){
                  sqlite3_free(aCols[ci].zName);
                  sqlite3_free(aCols[ci].zDef);
                }}
                sqlite3_free(aCols);
                return SQLITE_NOMEM;
              }
              memcpy(zName, nameStart, nameLen);
              zName[nameLen] = 0;
            }

            { int ci; for(ci=0;zName[ci];ci++) zName[ci]=(char)tolower((unsigned char)zName[ci]); }

            if( nameEnd<=nameStart ){
              sqlite3_free(zName);
              sqlite3_free(zTrimmed);
              { int ci; for(ci=0;ci<nCols;ci++){
                sqlite3_free(aCols[ci].zName);
                sqlite3_free(aCols[ci].zDef);
              }}
              sqlite3_free(aCols);
              return SQLITE_CORRUPT;
            }

            if( DOLTLITE_GROW_ARRAY(&aCols, &nAlloc, nCols+1, 8)!=SQLITE_OK ){
              sqlite3_free(zName);
              sqlite3_free(zTrimmed);
              { int ci; for(ci=0;ci<nCols;ci++){
                sqlite3_free(aCols[ci].zName);
                sqlite3_free(aCols[ci].zDef);
              }}
              sqlite3_free(aCols);
              return SQLITE_NOMEM;
            }
            aCols[nCols].zName = zName;
            aCols[nCols].zDef = zTrimmed;
            nCols++;
          }
        }
      }

      segStart = p + 1;
    }else if( *p=='(' ){
      depth++;
    }else if( *p==')' ){
      depth--;
    }
    p++;
  }

  *ppCols = aCols;
  *pnCols = nCols;
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

static ParsedColumn *findColumn(ParsedColumn *aCols, int nCols, const char *zName){
  int i;
  for(i=0; i<nCols; i++){
    if( aCols[i].zName && sqlite3_stricmp(aCols[i].zName, zName)==0 ){
      return &aCols[i];
    }
  }
  return 0;
}


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

static const char *schemaFindToken(
  const char *z,
  const char *zEnd,
  const char *zKw,
  int nKw
){
  const char *p = z;
  if( !z || !zEnd || zEnd<=z || nKw<=0 ) return 0;
  while( p<zEnd ){
    if( *p=='\'' || *p=='"' ){
      char q = *p++;
      while( p<zEnd ){
        if( *p==q ){
          p++;
          if( p<zEnd && *p==q ){ p++; continue; }
          break;
        }
        p++;
      }
      continue;
    }
    if( (p==z || (!sqlite3Isalnum((u8)p[-1]) && p[-1]!='_'))
     && (zEnd - p)>=nKw
     && sqlite3_strnicmp(p, zKw, nKw)==0
     && (p+nKw>=zEnd
         || (!sqlite3Isalnum((u8)p[nKw]) && p[nKw]!='_')) ){
      return p;
    }
    p++;
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
  const char *p = s;
  const char *e = s + len;
  while( p<e && isspace((unsigned char)*p) ) p++;
  if( (e-p)>=10 && sqlite3_strnicmp(p, "CONSTRAINT", 10)==0
   && (e-p==10 || isspace((unsigned char)p[10])) ){
    p += 10;
    while( p<e && isspace((unsigned char)*p) ) p++;
    if( p<e && (*p=='"' || *p=='`' || *p=='[') ){
      char cOpen = *p;
      char cClose = cOpen=='[' ? ']' : cOpen;
      p++;
      while( p<e ){
        if( *p==cClose ){
          p++;
          if( p<e && *p==cClose ){ p++; continue; }
          break;
        }
        p++;
      }
    }else{
      while( p<e && !isspace((unsigned char)*p) && *p!='(' ) p++;
    }
    while( p<e && isspace((unsigned char)*p) ) p++;
  }
  if( (e-p)>=11 && sqlite3_strnicmp(p, "FOREIGN KEY", 11)==0
   && (e-p==11 || !sqlite3Isalnum((u8)p[11])) ){
    return SCHEMA_IR_FK;
  }
  if( (e-p)>=5 && sqlite3_strnicmp(p, "CHECK", 5)==0
   && (e-p==5 || p[5]=='(' || isspace((unsigned char)p[5])) ){
    return SCHEMA_IR_CHECK;
  }
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
    while( z<zEnd && isspace((unsigned char)*z) ) z++;
    if( z<zEnd && *z=='(' ){
      int depth = 0;
      do{
        if( *z=='(' ) depth++;
        else if( *z==')' ) depth--;
        z++;
      }while( z<zEnd && depth>0 );
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
    while( z<zEnd && isspace((unsigned char)*z) ) z++;
    if( z<zEnd && *z=='(' ){
      int depth = 0;
      do{
        if( *z=='(' ) depth++;
        else if( *z==')' ) depth--;
        z++;
      }while( z<zEnd && depth>0 );
    }
    rc = schemaAppendSig(&pIr->zCheckSig, zStart, (int)(z - zStart));
    if( rc!=SQLITE_OK ) return rc;
    zCk = z;
  }
  return SQLITE_OK;
}

static int schemaIrBuild(const char *zSql, SchemaIr *pIr){
  const char *p, *pEnd;
  int depth;
  const char *segStart;
  int rc = SQLITE_OK;
  int nAlloc = 0;
  assert( pIr!=0 );

  memset(pIr, 0, sizeof(*pIr));
  if( !zSql ) return SQLITE_OK;

  p = zSql;
  while( *p && *p!='(' ) p++;
  if( *p!='(' ) return SQLITE_CORRUPT;
  p++;

  pEnd = p;
  depth = 1;
  while( *pEnd && depth>0 ){
    if( *pEnd=='(' ) depth++;
    else if( *pEnd==')' ) depth--;
    pEnd++;
  }
  if( depth!=0 ) return SQLITE_CORRUPT;
  pEnd--;

  segStart = p;
  depth = 0;
  while( p<=pEnd && rc==SQLITE_OK ){
    if( p==pEnd || (*p==',' && depth==0) ){
      const char *s = segStart;
      const char *e = p;
      int len;

      while( s<e && isspace((unsigned char)*s) ) s++;
      while( e>s && isspace((unsigned char)*(e-1)) ) e--;
      len = (int)(e - s);
      if( len>0 ){
        if( doltliteSegmentIsTableConstraint(s, len) ){
          int kind = schemaConstraintKind(s, len);
          if( kind==SCHEMA_IR_FK ){
            pIr->hasFk = 1;
            rc = schemaAppendSig(&pIr->zFkSig, s, len);
          }else if( kind==SCHEMA_IR_CHECK ){
            pIr->hasCheck = 1;
            rc = schemaAppendSig(&pIr->zCheckSig, s, len);
          }
        }else{
          char *zTrimmed = sqlite3_malloc(len + 1);
          char *zName = 0;
          const char *nameStart = s;
          const char *nameEnd = nameStart;
          if( !zTrimmed ){ rc = SQLITE_NOMEM; break; }
          memcpy(zTrimmed, s, len);
          zTrimmed[len] = 0;

          if( *nameStart=='"' || *nameStart=='`' || *nameStart=='[' ){
            rc = parseQuotedIdentifier(nameStart, e, &nameEnd, &zName);
          }else{
            int nameLen;
            while( nameEnd<e && !isspace((unsigned char)*nameEnd)
                && *nameEnd!='(' && *nameEnd!=',' ) nameEnd++;
            nameLen = (int)(nameEnd - nameStart);
            zName = sqlite3_malloc(nameLen + 1);
            if( !zName ) rc = SQLITE_NOMEM;
            else{
              memcpy(zName, nameStart, nameLen);
              zName[nameLen] = 0;
            }
          }
          if( rc!=SQLITE_OK ){
            sqlite3_free(zTrimmed);
            sqlite3_free(zName);
            break;
          }
          { int ci; for(ci=0; zName[ci]; ci++){
              zName[ci] = (char)tolower((unsigned char)zName[ci]);
            }
          }
          if( nameEnd<=nameStart ){
            sqlite3_free(zName);
            sqlite3_free(zTrimmed);
            rc = SQLITE_CORRUPT;
            break;
          }
          rc = DOLTLITE_GROW_ARRAY(&pIr->aCols, &nAlloc, pIr->nCols+1, 8);
          if( rc!=SQLITE_OK ){
            sqlite3_free(zName);
            sqlite3_free(zTrimmed);
            break;
          }
          pIr->aCols[pIr->nCols].zName = zName;
          pIr->aCols[pIr->nCols].zDef = zTrimmed;
          pIr->nCols++;
          rc = schemaIrNoteColumnConstraints(pIr, zTrimmed);
        }
      }
      segStart = p + 1;
    }else if( *p=='(' ){
      depth++;
    }else if( *p==')' ){
      depth--;
    }
    p++;
  }

  if( rc!=SQLITE_OK ) schemaIrClear(pIr);
  return rc;
}

static int schemaIrColumnsSame(
  const SchemaIr *pLeft,
  const SchemaIr *pRight,
  int ignoreChecks,
  int *pSame
){
  int i;
  *pSame = 0;
  if( pLeft->nCols!=pRight->nCols ) return SQLITE_OK;
  *pSame = 1;
  for(i=0; i<pLeft->nCols; i++){
    ParsedColumn *pRightCol = findColumn(
      pRight->aCols, pRight->nCols, pLeft->aCols[i].zName
    );
    if( !pRightCol ){ *pSame = 0; break; }
    if( ignoreChecks ){
      char *zLeft = schemaColumnWithoutChecks(pLeft->aCols[i].zDef);
      char *zRight = schemaColumnWithoutChecks(pRightCol->zDef);
      if( !zLeft || !zRight ){
        sqlite3_free(zLeft);
        sqlite3_free(zRight);
        return SQLITE_NOMEM;
      }
      if( strcmp(zLeft, zRight)!=0 ) *pSame = 0;
      sqlite3_free(zLeft);
      sqlite3_free(zRight);
    }else if( strcmp(pLeft->aCols[i].zDef, pRightCol->zDef)!=0 ){
      *pSame = 0;
    }
    if( !*pSame ) break;
  }
  return SQLITE_OK;
}

static int schemaConstraintModifyDeleteChoice(
  const char *zAncSql,
  const char *zOursSql,
  const char *zTheirsSql,
  int *pChoice
){
  SchemaIr anc, ours, theirs;
  int sameAO = 0, sameAT = 0, sameOT = 0;
  int survivorSame = 0;
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
   && anc.hasCheck==ours.hasCheck && anc.hasCheck==theirs.hasCheck ){
    rc = schemaIrColumnsSame(&anc, &ours, 0, &sameAO);
    if( rc==SQLITE_OK ) rc = schemaIrColumnsSame(&anc, &theirs, 0, &sameAT);
    if( rc==SQLITE_OK ) rc = schemaIrColumnsSame(&ours, &theirs, 0, &sameOT);
    if( rc==SQLITE_OK && sameAO && sameAT && sameOT ){
      const SchemaIr *pSurv = ours.hasFk ? &ours : &theirs;
      const char *zAncFk = anc.zFkSig ? anc.zFkSig : "";
      const char *zSurvFk = pSurv->zFkSig ? pSurv->zFkSig : "";
      if( strcmp(zAncFk, zSurvFk)!=0 ){
        *pChoice = ours.hasFk ? SCHEMA_MERGE_THEIRS : SCHEMA_MERGE_OURS;
      }
      schemaIrClear(&anc);
      schemaIrClear(&ours);
      schemaIrClear(&theirs);
      return SQLITE_OK;
    }
  }

  if( rc==SQLITE_OK
   && anc.hasCheck && ours.hasCheck!=theirs.hasCheck
   && anc.hasFk==ours.hasFk && anc.hasFk==theirs.hasFk ){
    rc = schemaIrColumnsSame(&anc, &ours, 1, &sameAO);
    if( rc==SQLITE_OK ) rc = schemaIrColumnsSame(&anc, &theirs, 1, &sameAT);
    if( rc==SQLITE_OK ) rc = schemaIrColumnsSame(&ours, &theirs, 1, &sameOT);
    if( rc==SQLITE_OK ){
      const SchemaIr *pSurv = ours.hasCheck ? &ours : &theirs;
      rc = schemaIrColumnsSame(&anc, pSurv, 0, &survivorSame);
    }
    if( rc==SQLITE_OK && sameAO && sameAT && sameOT && !survivorSame ){
      *pChoice = ours.hasCheck ? SCHEMA_MERGE_OURS : SCHEMA_MERGE_THEIRS;
    }
  }

  schemaIrClear(&anc);
  schemaIrClear(&ours);
  schemaIrClear(&theirs);
  return rc;
}

int trySchemaColumnMerge(
  const char *zAncSql,
  const char *zOursSql,
  const char *zTheirsSql,
  char ***ppAddCols, int *pnAddCols,
  int *pSchemaChoice,
  char **pzErrDetail
){
  ParsedColumn *aAnc=0, *aOurs=0, *aTheirs=0;
  int nAnc=0, nOurs=0, nTheirs=0;
  int rc;
  char **azAdd = 0;
  int nAdd = 0, nAddAlloc = 0;
  int i;

  *ppAddCols = 0;
  *pnAddCols = 0;
  *pSchemaChoice = SCHEMA_MERGE_DEFAULT;

  rc = schemaConstraintModifyDeleteChoice(
      zAncSql, zOursSql, zTheirsSql, pSchemaChoice);
  if( rc!=SQLITE_OK || *pSchemaChoice!=SCHEMA_MERGE_DEFAULT ) return rc;

  rc = parseColumns(zAncSql, &aAnc, &nAnc);
  if( rc!=SQLITE_OK ) return rc;
  rc = parseColumns(zOursSql, &aOurs, &nOurs);
  if( rc!=SQLITE_OK ){ freeColumns(aAnc, nAnc); return rc; }
  rc = parseColumns(zTheirsSql, &aTheirs, &nTheirs);
  if( rc!=SQLITE_OK ){ freeColumns(aAnc, nAnc); freeColumns(aOurs, nOurs); return rc; }

  for(i=0; i<nTheirs; i++){
    ParsedColumn *ancCol = findColumn(aAnc, nAnc, aTheirs[i].zName);
    if( !ancCol ){

      ParsedColumn *ourCol = findColumn(aOurs, nOurs, aTheirs[i].zName);
      if( ourCol ){

        if( strcmp(ourCol->zDef, aTheirs[i].zDef)!=0 ){

          if( pzErrDetail ){
            *pzErrDetail = sqlite3_mprintf(
              "both branches add column '%s' with different definitions",
              aTheirs[i].zName);
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

  if( nAdd > 0 ){
    *pSchemaChoice = SCHEMA_MERGE_DEFAULT;
  }
  *ppAddCols = azAdd;
  *pnAddCols = nAdd;
  azAdd = 0; nAdd = 0;

schema_merge_cleanup:
  freeColumns(aAnc, nAnc);
  freeColumns(aOurs, nOurs);
  freeColumns(aTheirs, nTheirs);
  if( rc!=SQLITE_OK ){
    { int j; for(j=0;j<nAdd;j++) sqlite3_free(azAdd[j]); }
    sqlite3_free(azAdd);
  }
  return rc;
}

int doltliteTableSchemaConflictDetail(
  const char *zAncestorSql,
  const char *zOurSql,
  const char *zTheirSql,
  char **pzDetail
){
  char **azAdd = 0;
  int nAdd = 0;
  int schemaChoice = SCHEMA_MERGE_DEFAULT;
  int i, rc;

  *pzDetail = 0;
  if( !zAncestorSql || !zOurSql || !zTheirSql ) return SQLITE_OK;
  rc = trySchemaColumnMerge(zAncestorSql, zOurSql, zTheirSql,
                            &azAdd, &nAdd, &schemaChoice, pzDetail);
  for(i=0; i<nAdd; i++) sqlite3_free(azAdd[i]);
  sqlite3_free(azAdd);
  if( rc==SQLITE_ERROR ) return SQLITE_OK;
  return rc;
}


#endif
