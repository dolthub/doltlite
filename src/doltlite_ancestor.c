
#ifdef DOLTLITE_PROLLY

#include "doltlite_ancestor.h"
#include "doltlite_commit.h"
#include "chunk_store.h"
#include "doltlite_internal.h"
#include <string.h>

static int loadCommitByHash(sqlite3 *db, const ProllyHash *hash,
                            DoltliteCommit *pCommit){
  if( prollyHashIsEmpty(hash) ) return SQLITE_NOTFOUND;
  (void)sqlite3FaultSim(959);
  return doltliteLoadCommit(db, hash, pCommit);
}

typedef struct AncestorNode AncestorNode;
typedef struct AncestorGraph AncestorGraph;

struct AncestorNode {
  ProllyHash hash;
  int aParent[DOLTLITE_MAX_PARENTS];
  int nParent;
  int aDistance[2];
  int aOrder[2];
  u8 redundant;
};

struct AncestorGraph {
  AncestorNode *aNode;
  int nNode;
  int nAlloc;
  int *aSlot;
  int nSlot;
  int nUsed;
};

static u32 ancestorHashSlot(const ProllyHash *pHash, int nSlot){
  u32 v;
  v = (u32)pHash->data[0] | ((u32)pHash->data[1] << 8) |
      ((u32)pHash->data[2] << 16) | ((u32)pHash->data[3] << 24);
  return v & (nSlot - 1);
}

static int ancestorGraphInit(AncestorGraph *pGraph){
  memset(pGraph, 0, sizeof(*pGraph));
  pGraph->nSlot = 256;
  pGraph->aSlot = sqlite3_malloc64(
      (sqlite3_uint64)pGraph->nSlot * sizeof(int));
  if( !pGraph->aSlot ) return SQLITE_NOMEM;
  memset(pGraph->aSlot, 0, (size_t)pGraph->nSlot * sizeof(int));
  return SQLITE_OK;
}

static void ancestorGraphClear(AncestorGraph *pGraph){
  sqlite3_free(pGraph->aNode);
  sqlite3_free(pGraph->aSlot);
  memset(pGraph, 0, sizeof(*pGraph));
}

static int ancestorGraphFind(
  const AncestorGraph *pGraph,
  const ProllyHash *pHash
){
  u32 iSlot = ancestorHashSlot(pHash, pGraph->nSlot);
  int i;
  for(i=0; i<pGraph->nSlot; i++){
    int iNode = pGraph->aSlot[(iSlot + (u32)i) & (pGraph->nSlot - 1)];
    if( iNode==0 ) return -1;
    iNode--;
    if( prollyHashCompare(&pGraph->aNode[iNode].hash, pHash)==0 ){
      return iNode;
    }
  }
  return -1;
}

static void ancestorGraphInsertSlot(
  AncestorGraph *pGraph,
  int *aSlot,
  int nSlot,
  int iNode
){
  u32 iSlot = ancestorHashSlot(&pGraph->aNode[iNode].hash, nSlot);
  while( aSlot[iSlot]!=0 ) iSlot = (iSlot + 1) & (nSlot - 1);
  aSlot[iSlot] = iNode + 1;
}

static int ancestorGraphGrowSlots(AncestorGraph *pGraph){
  int *aSlot;
  int nSlot;
  int i;
  if( pGraph->nSlot > 0x3fffffff ) return SQLITE_NOMEM;
  nSlot = pGraph->nSlot * 2;
  aSlot = sqlite3_malloc64((sqlite3_uint64)nSlot * sizeof(int));
  if( !aSlot ) return SQLITE_NOMEM;
  memset(aSlot, 0, (size_t)nSlot * sizeof(int));
  for(i=0; i<pGraph->nNode; i++){
    ancestorGraphInsertSlot(pGraph, aSlot, nSlot, i);
  }
  sqlite3_free(pGraph->aSlot);
  pGraph->aSlot = aSlot;
  pGraph->nSlot = nSlot;
  return SQLITE_OK;
}

static int ancestorGraphAdd(
  AncestorGraph *pGraph,
  const ProllyHash *pHash,
  int *pIndex
){
  AncestorNode *aNode;
  int iNode;
  int nAlloc;
  int rc;

  iNode = ancestorGraphFind(pGraph, pHash);
  if( iNode>=0 ){
    *pIndex = iNode;
    return SQLITE_OK;
  }
  if( pGraph->nUsed >= pGraph->nSlot/2 ){
    rc = ancestorGraphGrowSlots(pGraph);
    if( rc!=SQLITE_OK ) return rc;
  }
  if( pGraph->nNode>=pGraph->nAlloc ){
    nAlloc = pGraph->nAlloc ? pGraph->nAlloc * 2 : 64;
    if( nAlloc<0 || (sqlite3_uint64)nAlloc >
        (sqlite3_uint64)0x7fffffff/sizeof(AncestorNode) ){
      return SQLITE_NOMEM;
    }
    aNode = sqlite3_realloc64(pGraph->aNode,
        (sqlite3_uint64)nAlloc * sizeof(AncestorNode));
    if( !aNode ) return SQLITE_NOMEM;
    pGraph->aNode = aNode;
    pGraph->nAlloc = nAlloc;
  }
  iNode = pGraph->nNode++;
  memset(&pGraph->aNode[iNode], 0, sizeof(AncestorNode));
  pGraph->aNode[iNode].hash = *pHash;
  pGraph->aNode[iNode].aDistance[0] = -1;
  pGraph->aNode[iNode].aDistance[1] = -1;
  pGraph->aNode[iNode].aOrder[0] = -1;
  pGraph->aNode[iNode].aOrder[1] = -1;
  ancestorGraphInsertSlot(pGraph, pGraph->aSlot, pGraph->nSlot, iNode);
  pGraph->nUsed++;
  *pIndex = iNode;
  return SQLITE_OK;
}

static int ancestorGraphBuild(
  sqlite3 *db,
  AncestorGraph *pGraph,
  const ProllyHash *pLeft,
  const ProllyHash *pRight,
  int *pLeftIndex,
  int *pRightIndex
){
  DoltliteCommit commit;
  int rc;
  int i;

  rc = ancestorGraphAdd(pGraph, pLeft, pLeftIndex);
  if( rc!=SQLITE_OK ) return rc;
  rc = ancestorGraphAdd(pGraph, pRight, pRightIndex);
  if( rc!=SQLITE_OK ) return rc;
  for(i=0; i<pGraph->nNode; i++){
    int j;
    memset(&commit, 0, sizeof(commit));
    rc = loadCommitByHash(db, &pGraph->aNode[i].hash, &commit);
    if( rc!=SQLITE_OK ) return rc;
    for(j=0; j<doltliteCommitParentCount(&commit); j++){
      const ProllyHash *pParent = doltliteCommitParentHash(&commit, j);
      int iParent;
      if( !pParent || prollyHashIsEmpty(pParent) ) continue;
      rc = ancestorGraphAdd(pGraph, pParent, &iParent);
      if( rc!=SQLITE_OK ) break;
      pGraph->aNode[i].aParent[pGraph->aNode[i].nParent++] = iParent;
    }
    doltliteCommitClear(&commit);
    if( rc!=SQLITE_OK ) return rc;
  }
  return SQLITE_OK;
}

static int ancestorGraphBfs(
  AncestorGraph *pGraph,
  int iStart,
  int iSide
){
  int *aQueue;
  int iHead = 0;
  int iTail = 0;
  int iOrder = 0;

  aQueue = sqlite3_malloc64((sqlite3_uint64)pGraph->nNode * sizeof(int));
  if( !aQueue ) return SQLITE_NOMEM;
  pGraph->aNode[iStart].aDistance[iSide] = 0;
  aQueue[iTail++] = iStart;
  while( iHead<iTail ){
    AncestorNode *pNode = &pGraph->aNode[aQueue[iHead++]];
    int i;
    pNode->aOrder[iSide] = iOrder++;
    for(i=0; i<pNode->nParent; i++){
      AncestorNode *pParent = &pGraph->aNode[pNode->aParent[i]];
      if( pParent->aDistance[iSide]<0 ){
        pParent->aDistance[iSide] = pNode->aDistance[iSide] + 1;
        aQueue[iTail++] = pNode->aParent[i];
      }
    }
  }
  sqlite3_free(aQueue);
  return SQLITE_OK;
}

static int ancestorGraphMarkRedundant(AncestorGraph *pGraph){
  int *aQueue;
  u8 *aSeen;
  int iHead = 0;
  int iTail = 0;
  int i;

  aQueue = sqlite3_malloc64((sqlite3_uint64)pGraph->nNode * sizeof(int));
  aSeen = sqlite3_malloc64((sqlite3_uint64)pGraph->nNode);
  if( !aQueue || !aSeen ){
    sqlite3_free(aSeen);
    sqlite3_free(aQueue);
    return SQLITE_NOMEM;
  }
  memset(aSeen, 0, (size_t)pGraph->nNode);
  for(i=0; i<pGraph->nNode; i++){
    AncestorNode *pNode = &pGraph->aNode[i];
    int j;
    if( pNode->aDistance[0]<0 || pNode->aDistance[1]<0 ) continue;
    for(j=0; j<pNode->nParent; j++){
      int iParent = pNode->aParent[j];
      if( !aSeen[iParent] ){
        aSeen[iParent] = 1;
        aQueue[iTail++] = iParent;
      }
    }
  }
  while( iHead<iTail ){
    AncestorNode *pNode = &pGraph->aNode[aQueue[iHead++]];
    int j;
    if( pNode->aDistance[0]>=0 && pNode->aDistance[1]>=0 ){
      pNode->redundant = 1;
    }
    for(j=0; j<pNode->nParent; j++){
      int iParent = pNode->aParent[j];
      if( !aSeen[iParent] ){
        aSeen[iParent] = 1;
        aQueue[iTail++] = iParent;
      }
    }
  }
  sqlite3_free(aSeen);
  sqlite3_free(aQueue);
  return SQLITE_OK;
}

static int ancestorGraphCompareCandidate(
  const AncestorNode *pLeft,
  const AncestorNode *pRight
){
  i64 leftSum = (i64)pLeft->aDistance[0] + pLeft->aDistance[1];
  i64 rightSum = (i64)pRight->aDistance[0] + pRight->aDistance[1];
  if( leftSum!=rightSum ) return leftSum<rightSum ? -1 : 1;
  if( pLeft->aDistance[0]!=pRight->aDistance[0] ){
    return pLeft->aDistance[0]<pRight->aDistance[0] ? -1 : 1;
  }
  if( pLeft->aDistance[1]!=pRight->aDistance[1] ){
    return pLeft->aDistance[1]<pRight->aDistance[1] ? -1 : 1;
  }
  if( pLeft->aOrder[0]!=pRight->aOrder[0] ){
    return pLeft->aOrder[0]<pRight->aOrder[0] ? -1 : 1;
  }
  if( pLeft->aOrder[1]!=pRight->aOrder[1] ){
    return pLeft->aOrder[1]<pRight->aOrder[1] ? -1 : 1;
  }
  return prollyHashCompare(&pLeft->hash, &pRight->hash);
}

int doltliteFindAncestor(
  sqlite3 *db,
  const ProllyHash *commitHash1,
  const ProllyHash *commitHash2,
  ProllyHash *pAncestor
){
  AncestorGraph graph;
  int iLeft;
  int iRight;
  int iBest = -1;
  int i;
  int rc;

  memset(pAncestor, 0, sizeof(*pAncestor));

  if( prollyHashIsEmpty(commitHash1) || prollyHashIsEmpty(commitHash2) ){
    return SQLITE_NOTFOUND;
  }

  if( prollyHashCompare(commitHash1, commitHash2)==0 ){
    *pAncestor = *commitHash1;
    return SQLITE_OK;
  }

  rc = ancestorGraphInit(&graph);
  if( rc!=SQLITE_OK ) return rc;
  rc = ancestorGraphBuild(db, &graph, commitHash1, commitHash2,
                          &iLeft, &iRight);
  if( rc!=SQLITE_OK ) goto done;
  rc = ancestorGraphBfs(&graph, iLeft, 0);
  if( rc!=SQLITE_OK ) goto done;
  rc = ancestorGraphBfs(&graph, iRight, 1);
  if( rc!=SQLITE_OK ) goto done;
  rc = ancestorGraphMarkRedundant(&graph);
  if( rc!=SQLITE_OK ) goto done;
  for(i=0; i<graph.nNode; i++){
    AncestorNode *pNode = &graph.aNode[i];
    if( pNode->aDistance[0]<0 || pNode->aDistance[1]<0 || pNode->redundant ){
      continue;
    }
    if( iBest<0 || ancestorGraphCompareCandidate(pNode,
                                                 &graph.aNode[iBest])<0 ){
      iBest = i;
    }
  }
  if( iBest<0 ){
    rc = SQLITE_NOTFOUND;
  }else{
    *pAncestor = graph.aNode[iBest].hash;
    rc = SQLITE_OK;
  }

done:
  ancestorGraphClear(&graph);
  return rc;
}

static void doltMergeBaseFunc(
  sqlite3_context *ctx,
  int argc,
  sqlite3_value **argv
){
  sqlite3 *db = sqlite3_context_db_handle(ctx);
  ProllyHash hash1, hash2, ancestor;
  const char *zRef1, *zRef2;
  int rc;

  if( argc!=2 ){
    sqlite3_result_error(ctx, "dolt_merge_base requires 2 arguments", -1);
    return;
  }

  zRef1 = (const char*)sqlite3_value_text(argv[0]);
  zRef2 = (const char*)sqlite3_value_text(argv[1]);
  if( !zRef1 || !zRef2 ){
    sqlite3_result_error(ctx, "invalid arguments", -1);
    return;
  }

  rc = doltliteResolveRef(db,zRef1, &hash1);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "could not resolve first argument to a commit", -1);
    return;
  }
  rc = doltliteResolveRef(db,zRef2, &hash2);
  if( rc!=SQLITE_OK ){
    sqlite3_result_error(ctx, "could not resolve second argument to a commit", -1);
    return;
  }

  rc = doltliteFindAncestor(db, &hash1, &hash2, &ancestor);
  if( rc==SQLITE_OK ){
    char hexBuf[PROLLY_HASH_SIZE*2+1];
    doltliteHashToHex(&ancestor, hexBuf);
    sqlite3_result_text(ctx, hexBuf, -1, SQLITE_TRANSIENT);
  }else if( rc==SQLITE_NOTFOUND ){
    ChunkStore *cs = doltliteGetChunkStore(db);
    if( !cs || !doltliteCmdSourceResultError(ctx, cs, &rc) ){
      sqlite3_result_null(ctx);
    }
  }else{
    sqlite3_result_error(ctx, "error finding common ancestor", -1);
  }
}

int doltliteAncestorRegister(sqlite3 *db){
  return sqlite3_create_function(db, "dolt_merge_base", 2, SQLITE_UTF8, 0,
                                 doltMergeBaseFunc, 0, 0);
}

#endif
