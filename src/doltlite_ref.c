
#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "prolly_hash.h"
#include "chunk_store.h"
#include "doltlite_commit.h"
#include "doltlite_internal.h"
#include "doltlite_parse.h"

static int doltliteRefComponentEndsLock(const char *zStart, const char *zEnd){
  return zEnd-zStart>=5 && memcmp(zEnd-5, ".lock", 5)==0;
}

static int doltliteLoadCommitFromStore(
  ChunkStore *cs,
  const ProllyHash *pHash,
  DoltliteCommit *pCommit
){
  u8 *data = 0;
  int nData = 0;
  int rc = chunkStoreGet(cs, pHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteCommitDeserialize(data, nData, pCommit);
  sqlite3_free(data);
  return rc;
}

static int doltliteOpenRevisionBase(
  ChunkStore *cs,
  const char *zRef,
  ProllyHash *pCommit,
  u8 *pIsBranch
){
  DoltliteCommit commit;
  int rc;

  *pIsBranch = 0;
  rc = chunkStoreFindBranch(cs, zRef, pCommit);
  if( rc==SQLITE_OK && !prollyHashIsEmpty(pCommit) ){
    *pIsBranch = 1;
  }else{
    rc = chunkStoreFindTag(cs, zRef, pCommit);
    if( rc!=SQLITE_OK && strlen(zRef)==PROLLY_HASH_SIZE*2 ){
      rc = doltliteHexToHash(zRef, pCommit);
    }
  }
  if( rc!=SQLITE_OK || prollyHashIsEmpty(pCommit) ) return SQLITE_NOTFOUND;
  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommitFromStore(cs, pCommit, &commit);
  if( rc==SQLITE_OK ) doltliteCommitClear(&commit);
  return rc;
}

static int doltliteOpenRevisionParent(
  ChunkStore *cs,
  ProllyHash *pCommit,
  int iParent
){
  DoltliteCommit commit;
  const ProllyHash *pParent;
  int rc;

  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommitFromStore(cs, pCommit, &commit);
  if( rc!=SQLITE_OK ) return rc;
  pParent = doltliteCommitParentHash(&commit, iParent);
  if( !pParent ){
    doltliteCommitClear(&commit);
    return SQLITE_NOTFOUND;
  }
  memcpy(pCommit, pParent, sizeof(*pCommit));
  doltliteCommitClear(&commit);
  return SQLITE_OK;
}

int doltliteResolveOpenRevision(
  ChunkStore *cs,
  const char *zRef,
  ProllyHash *pCommit,
  ProllyHash *pCatalog,
  u8 *pIsBranch
){
  DoltliteCommit commit;
  char *zBase = 0;
  int nRef, nBase, i, rc;

  if( !cs || !zRef || !pCommit || !pCatalog || !pIsBranch ){
    return SQLITE_MISUSE;
  }
  nRef = (int)strlen(zRef);
  nBase = nRef;
  for(;;){
    int q = nBase;
    while( q>0 && zRef[q-1]>='0' && zRef[q-1]<='9' ) q--;
    if( q>0 && (zRef[q-1]=='~' || zRef[q-1]=='^') ){
      nBase = q - 1;
    }else{
      break;
    }
  }
  if( nBase==0 ) return SQLITE_NOTFOUND;
  if( nBase==nRef ){
    rc = doltliteOpenRevisionBase(cs, zRef, pCommit, pIsBranch);
  }else{
    zBase = sqlite3_mprintf("%.*s", nBase, zRef);
    if( !zBase ) return SQLITE_NOMEM;
    rc = doltliteOpenRevisionBase(cs, zBase, pCommit, pIsBranch);
    sqlite3_free(zBase);
    *pIsBranch = 0;
  }
  if( rc!=SQLITE_OK ) return rc;

  for(i=nBase; i<nRef; ){
    char op = zRef[i++];
    int d = i;
    int n, j;
    while( i<nRef && zRef[i]>='0' && zRef[i]<='9' ) i++;
    if( i==d ){
      n = 1;
    }else{
      uint64_t value;
      if( doltliteParseDecimal(zRef+d, zRef+i, 0x7fffffff, &value)
          !=DOLTLITE_DECIMAL_OK ) return SQLITE_NOTFOUND;
      n = (int)value;
      if( n==0 && op=='^' ) return SQLITE_NOTFOUND;
    }
    if( op=='~' ){
      for(j=0; j<n && rc==SQLITE_OK; j++){
        rc = doltliteOpenRevisionParent(cs, pCommit, 0);
      }
    }else{
      rc = doltliteOpenRevisionParent(cs, pCommit, n-1);
    }
    if( rc!=SQLITE_OK ) return SQLITE_NOTFOUND;
  }

  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommitFromStore(cs, pCommit, &commit);
  if( rc==SQLITE_OK ) memcpy(pCatalog, &commit.catalogHash, sizeof(*pCatalog));
  doltliteCommitClear(&commit);
  return rc;
}

int doltliteUserRefNameIsValid(const char *zName){
  const unsigned char *z;
  const char *zComponent;
  int n, i;
  int isHash = 1;

  if( !zName || !zName[0] ) return 0;
  if( strcmp(zName, "HEAD")==0 || strcmp(zName, "head")==0
   || sqlite3_stricmp(zName, "WORKING")==0
   || sqlite3_stricmp(zName, "STAGED")==0
   || strcmp(zName, "-")==0 || strcmp(zName, "@")==0 ){
    return 0;
  }
  n = (int)strlen(zName);
  if( zName[0]=='/' || zName[n-1]=='/' || zName[n-1]=='.' ) return 0;
  if( n==PROLLY_HASH_SIZE*2 ){
    for(i=0; i<n; i++){
      unsigned char c = (unsigned char)zName[i];
      if( !((c>='0' && c<='9') || (c>='a' && c<='f')
         || (c>='A' && c<='F')) ){
        isHash = 0;
        break;
      }
    }
    if( isHash ) return 0;
  }

  zComponent = zName;
  for(z=(const unsigned char*)zName; *z; z++){
    unsigned char c = *z;
    if( c<=0x20 || c>=0x7f || c==':' || c=='?' || c=='[' || c=='\\'
     || c=='^' || c=='~' || c=='*' ){
      return 0;
    }
    if( z==(const unsigned char*)zComponent && c=='.' ) return 0;
    if( c=='.' && z[1]=='.' ) return 0;
    if( c=='@' && z[1]=='{' ) return 0;
    if( c=='/' ){
      if( z==(const unsigned char*)zComponent
       || doltliteRefComponentEndsLock(zComponent, (const char*)z) ){
        return 0;
      }
      zComponent = (const char*)z + 1;
    }
  }
  return !doltliteRefComponentEndsLock(zComponent, (const char*)z);
}

static int doltliteValidateCommitHash(
  sqlite3 *db,
  const ProllyHash *pHash
){
  DoltliteCommit commit;
  int rc;

  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommit(db, pHash, &commit);
  if( rc==SQLITE_OK ){
    doltliteCommitClear(&commit);
  }
  return rc;
}

static int doltliteResolveBaseRef(
  sqlite3 *db,
  const char *zRef,
  ProllyHash *pCommit
){
  ChunkStore *cs = doltliteGetChunkStore(db);
  static const char zTrackingPrefix[] = "refs/remotes/";
  int rc;

  if( strcmp(zRef, "HEAD")==0 ){
    doltliteGetSessionHead(db, pCommit);
    if( prollyHashIsEmpty(pCommit) ) return SQLITE_NOTFOUND;
    return SQLITE_OK;
  }

  if( strlen(zRef)==PROLLY_HASH_SIZE*2 ){
    rc = doltliteHexToHash(zRef, pCommit);
    if( rc==SQLITE_OK ){
      rc = doltliteValidateCommitHash(db, pCommit);
      if( rc==SQLITE_OK ) return SQLITE_OK;
      if( rc!=SQLITE_NOTFOUND ) return rc;
    }
  }

  rc = chunkStoreFindBranch(cs, zRef, pCommit);
  if( rc==SQLITE_OK && !prollyHashIsEmpty(pCommit) ){
    rc = doltliteValidateCommitHash(db, pCommit);
    if( rc==SQLITE_OK ) return SQLITE_OK;
    return rc;
  }

  {
    const char *zTrackingRef = zRef;
    const char *zSlash;
    if( strncmp(zRef, zTrackingPrefix, sizeof(zTrackingPrefix)-1)==0 ){
      zTrackingRef = zRef + sizeof(zTrackingPrefix)-1;
    }else if( strncmp(zRef, "remotes/", 8)==0 ){
      zTrackingRef = zRef + 8;
    }
    zSlash = strchr(zTrackingRef, '/');
    if( zSlash && zSlash!=zRef && zSlash[1] ){
      char *zRemote = sqlite3_mprintf("%.*s",
                                      (int)(zSlash - zTrackingRef),
                                      zTrackingRef);
      const char *zBranch = zSlash + 1;
      if( !zRemote ) return SQLITE_NOMEM;
      rc = chunkStoreFindTracking(cs, zRemote, zBranch, pCommit);
      sqlite3_free(zRemote);
      if( rc==SQLITE_OK && !prollyHashIsEmpty(pCommit) ){
        rc = doltliteValidateCommitHash(db, pCommit);
        if( rc==SQLITE_OK ) return SQLITE_OK;
        return rc;
      }
    }
  }

  rc = chunkStoreFindTag(cs, zRef, pCommit);
  if( rc==SQLITE_OK && !prollyHashIsEmpty(pCommit) ){
    rc = doltliteValidateCommitHash(db, pCommit);
    if( rc==SQLITE_OK ) return SQLITE_OK;
    return rc;
  }

  return SQLITE_NOTFOUND;
}

static int doltliteSelectParent(
  sqlite3 *db,
  ProllyHash *pCommit,
  int iParent
){
  DoltliteCommit commit;
  const ProllyHash *pParent;
  int rc;

  memset(&commit, 0, sizeof(commit));
  rc = doltliteLoadCommit(db, pCommit, &commit);
  if( rc!=SQLITE_OK ) return rc;
  pParent = doltliteCommitParentHash(&commit, iParent);
  if( !pParent ){
    doltliteCommitClear(&commit);
    return SQLITE_NOTFOUND;
  }
  memcpy(pCommit, pParent, sizeof(ProllyHash));
  doltliteCommitClear(&commit);
  return SQLITE_OK;
}

static int doltliteWalkFirstParent(
  sqlite3 *db,
  ProllyHash *pCommit,
  int n
){
  int i;
  for(i=0; i<n; i++){
    int rc = doltliteSelectParent(db, pCommit, 0);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

int doltliteResolveRef(sqlite3 *db, const char *zRef, ProllyHash *pCommit){
  ChunkStore *cs = doltliteGetChunkStore(db);
  int len, base_len, i, rc;
  char *base_buf = 0;

  if( !zRef || !cs ) return SQLITE_ERROR;

  /* A ref may carry a suffix of ~N / ^N parent-walk operators. Peel that
  ** suffix off right-to-left to find the base ref, then apply the operators
  ** left-to-right. This is iterative on purpose: recursing once per operator
  ** let a long "^^^..." argument exhaust the stack. */
  len = (int)strlen(zRef);
  base_len = len;
  for(;;){
    int q = base_len;
    while( q>0 && zRef[q-1]>='0' && zRef[q-1]<='9' ) q--;
    if( q>0 && (zRef[q-1]=='~' || zRef[q-1]=='^') ){
      base_len = q-1;
    }else{
      break;
    }
  }

  if( base_len==len ){
    /* No operator suffix: resolve the ref as-is (including "", which is not
    ** a shorthand for HEAD and must fail like any unknown ref). */
    rc = doltliteResolveBaseRef(db, zRef, pCommit);
  }else if( base_len==0 ){
    /* Parent-walk operators require an explicit base ref. */
    rc = SQLITE_ERROR;
  }else{
    base_buf = sqlite3_malloc(base_len + 1);
    if( !base_buf ) return SQLITE_NOMEM;
    memcpy(base_buf, zRef, base_len);
    base_buf[base_len] = '\0';
    rc = doltliteResolveBaseRef(db, base_buf, pCommit);
    sqlite3_free(base_buf);
  }
  if( rc!=SQLITE_OK ) return rc;

  for(i=base_len; i<len; ){
    char op = zRef[i++];
    int d = i;
    int n;
    while( i<len && zRef[i]>='0' && zRef[i]<='9' ) i++;
    if( i==d ){
      n = 1;
    }else{
      uint64_t value;
      if( doltliteParseDecimal(zRef+d, zRef+i, 0x7fffffff, &value)
          !=DOLTLITE_DECIMAL_OK ){
        return SQLITE_ERROR;
      }
      n = (int)value;
      if( n==0 && op=='^' ) return SQLITE_ERROR;
    }
    if( op=='~' ){
      rc = doltliteWalkFirstParent(db, pCommit, n);
    }else{
      rc = doltliteSelectParent(db, pCommit, n-1);
    }
    if( rc==SQLITE_NOTFOUND ) rc = SQLITE_ERROR;
    if( rc!=SQLITE_OK ) return rc;
  }
  return rc;
}

int doltliteLoadCommit(sqlite3 *db, const ProllyHash *pHash,
                       DoltliteCommit *pCommit){
  ChunkStore *cs = doltliteGetChunkStore(db);
  u8 *data = 0;
  int nData = 0;
  int rc;
  if( !cs ) return SQLITE_ERROR;
  rc = chunkStoreGet(cs, pHash, &data, &nData);
  if( rc!=SQLITE_OK ) return rc;
  rc = doltliteCommitDeserialize(data, nData, pCommit);
  sqlite3_free(data);
  return rc;
}

int doltliteForEachUserTable(
  sqlite3 *db,
  const char *zPrefix,
  const sqlite3_module *pModule
){
  ProllyHash headCommit;
  ProllyHash headCat;
  struct TableEntry *aTables = 0;
  int nTables = 0, i, rc;

  doltliteGetSessionHead(db, &headCommit);
  if( prollyHashIsEmpty(&headCommit) ) return SQLITE_OK;

  rc = doltliteCommitCatalogHash(db, &headCommit, &headCat);
  if( rc!=SQLITE_OK ) return rc;

  rc = doltliteLoadCatalog(db, &headCat, &aTables, &nTables, 0);
  if( rc!=SQLITE_OK ) return rc;

  for(i=0; i<nTables; i++){
    if( aTables[i].zName && aTables[i].iTable > 1 ){
      char *zMod = sqlite3_mprintf("%s%s", zPrefix, aTables[i].zName);
      if( !zMod ){
        doltliteFreeCatalog(aTables, nTables);
        return SQLITE_NOMEM;
      }
      rc = sqlite3_create_module(db, zMod, pModule, 0);
      sqlite3_free(zMod);
      if( rc!=SQLITE_OK ){
        doltliteFreeCatalog(aTables, nTables);
        return rc;
      }
    }
  }
  doltliteFreeCatalog(aTables, nTables);
  return SQLITE_OK;
}

#endif
