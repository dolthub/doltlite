
#ifndef SQLITE_CHUNK_STORE_H
#define SQLITE_CHUNK_STORE_H

#include "sqliteInt.h"
#include "prolly_hash.h"

#define CHUNK_STORE_MAGIC 0x444C5443  
#define CHUNK_STORE_VERSION 6
#define CHUNK_MANIFEST_SIZE 168
#define CHUNK_INDEX_ENTRY_SIZE 32     

#define WS_VERSION_SIZE     1
#define WS_STAGED_OFF       WS_VERSION_SIZE
#define WS_MERGING_OFF      (WS_STAGED_OFF + PROLLY_HASH_SIZE)
#define WS_MERGE_COMMIT_OFF (WS_MERGING_OFF + 1)
#define WS_CONFLICTS_OFF    (WS_MERGE_COMMIT_OFF + PROLLY_HASH_SIZE)
#define WS_TOTAL_SIZE       (WS_CONFLICTS_OFF + PROLLY_HASH_SIZE)

typedef struct ChunkStore ChunkStore;
typedef struct ChunkIndexEntry ChunkIndexEntry;
typedef struct ConflictEntry ConflictEntry;

struct ConflictEntry {
  u8 *pKey;           
  int nKey;           
  u8 *pBaseVal;       
  int nBaseVal;       
  u8 *pTheirVal;      
  int nTheirVal;      
};

struct ChunkIndexEntry {
  ProllyHash hash;
  i64 offset;     
  int size;       
};

struct ChunkStore {
  char *zFilename;           
  sqlite3_file *pFile;       
  sqlite3_vfs *pVfs;         
  ProllyHash root;           
  ProllyHash catalog;        
  ProllyHash headCommit;     
  ProllyHash refsHash;       

  
  ProllyHash stagedCatalog;
  u8 isMerging;
  ProllyHash mergeCommitHash;
  ProllyHash conflictsCatalogHash;

  
  struct BranchRef {
    char *zName;
    ProllyHash commitHash;
    ProllyHash workingSetHash;
  } *aBranches;
  int nBranches;
  char *zDefaultBranch;      

  
  struct TagRef {
    char *zName;
    ProllyHash commitHash;
  } *aTags;
  int nTags;

  
  struct RemoteRef {
    char *zName;    
    char *zUrl;     
  } *aRemotes;
  int nRemotes;

  
  struct TrackingBranch {
    char *zRemote;  
    char *zBranch;  
    ProllyHash commitHash;
  } *aTracking;
  int nTracking;

  
  int nChunks;               
  i64 iIndexOffset;          
  int nIndexSize;            
  i64 iWalOffset;            
  i64 iFileSize;             

  
  ChunkIndexEntry *aIndex;   
  int nIndex;
  int nIndexAlloc;

  
  ChunkIndexEntry *aPending;
  int nPending;
  int nPendingAlloc;
  int *aPendingHT;           
  int *aPendingHTNext;       
  int nPendingHTBuilt;       
  int nPendingHTNextAlloc;   
  int nPendingHTSize;        
  u8 *pWriteBuf;
  i64 nWriteBuf;
  i64 nWriteBufAlloc;

  u8 readOnly;
  u8 isMemory;
  u8 snapshotPinned;         
  i64 nCommittedWriteBuf;

  
  u8 *pWalData;              
  i64 nWalData;              
};

int chunkStoreOpen(ChunkStore *cs, sqlite3_vfs *pVfs,
                   const char *zFilename, int flags);

int chunkStoreClose(ChunkStore *cs);

void chunkStoreGetRoot(ChunkStore *cs, ProllyHash *pRoot);

void chunkStoreSetRoot(ChunkStore *cs, const ProllyHash *pRoot);

void chunkStoreGetCatalog(ChunkStore *cs, ProllyHash *pCat);
void chunkStoreSetCatalog(ChunkStore *cs, const ProllyHash *pCat);

void chunkStoreGetHeadCommit(ChunkStore *cs, ProllyHash *pHead);
void chunkStoreSetHeadCommit(ChunkStore *cs, const ProllyHash *pHead);

void chunkStoreGetStagedCatalog(ChunkStore *cs, ProllyHash *pStaged);
void chunkStoreSetStagedCatalog(ChunkStore *cs, const ProllyHash *pStaged);

int chunkStoreReloadRefs(ChunkStore *cs);

const char *chunkStoreGetDefaultBranch(ChunkStore *cs);
int chunkStoreSetDefaultBranch(ChunkStore *cs, const char *zName);
int chunkStoreAddBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit);
int chunkStoreDeleteBranch(ChunkStore *cs, const char *zName);
int chunkStoreFindBranch(ChunkStore *cs, const char *zName, ProllyHash *pCommit);
int chunkStoreUpdateBranch(ChunkStore *cs, const char *zName, const ProllyHash *pCommit);
int chunkStoreSerializeRefs(ChunkStore *cs);

int chunkStoreGetBranchWorkingSet(ChunkStore *cs, const char *zBranch, ProllyHash *pHash);
int chunkStoreSetBranchWorkingSet(ChunkStore *cs, const char *zBranch, const ProllyHash *pHash);

int chunkStoreAddTag(ChunkStore *cs, const char *zName, const ProllyHash *pCommit);
int chunkStoreDeleteTag(ChunkStore *cs, const char *zName);
int chunkStoreFindTag(ChunkStore *cs, const char *zName, ProllyHash *pCommit);

int chunkStoreAddRemote(ChunkStore *cs, const char *zName, const char *zUrl);
int chunkStoreDeleteRemote(ChunkStore *cs, const char *zName);
int chunkStoreFindRemote(ChunkStore *cs, const char *zName, const char **pzUrl);

int chunkStoreUpdateTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch, const ProllyHash *pCommit);
int chunkStoreFindTracking(ChunkStore *cs, const char *zRemote,
                           const char *zBranch, ProllyHash *pCommit);
int chunkStoreDeleteTracking(ChunkStore *cs, const char *zRemote,
                             const char *zBranch);

int chunkStoreLoadRefsFromBlob(ChunkStore *cs, const u8 *data, int nData);

int chunkStoreSerializeRefsToBlob(ChunkStore *cs, u8 **ppOut, int *pnOut);

int chunkStoreHasMany(ChunkStore *cs, const ProllyHash *aHash, int nHash, u8 *aResult);

int chunkStoreHas(ChunkStore *cs, const ProllyHash *hash);

int chunkStoreGet(ChunkStore *cs, const ProllyHash *hash,
                  u8 **ppData, int *pnData);

int chunkStorePut(ChunkStore *cs, const u8 *pData, int nData,
                  ProllyHash *pHash);

int chunkStoreCommit(ChunkStore *cs);

void chunkStoreRollback(ChunkStore *cs);

int chunkStoreIsEmpty(ChunkStore *cs);

const char *chunkStoreFilename(ChunkStore *cs);

int chunkStoreRefreshIfChanged(ChunkStore *cs, int *pChanged);

int chunkStoreGetMergeState(ChunkStore *cs, u8 *pIsMerging,
                            ProllyHash *pMergeCommit,
                            ProllyHash *pConflictsCatalog);
void chunkStoreSetMergeState(ChunkStore *cs, u8 isMerging,
                             const ProllyHash *pMergeCommit,
                             const ProllyHash *pConflictsCatalog);
void chunkStoreClearMergeState(ChunkStore *cs);

void chunkStoreGetConflictsCatalog(ChunkStore *cs, ProllyHash *pHash);
void chunkStoreSetConflictsCatalog(ChunkStore *cs, const ProllyHash *pHash);

#endif 
