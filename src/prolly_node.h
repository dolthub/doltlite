
#ifndef SQLITE_PROLLY_NODE_H
#define SQLITE_PROLLY_NODE_H

#include "sqliteInt.h"
#include "prolly_hash.h"

#define PROLLY_NODE_MAGIC 0x504E4F44  

#define PROLLY_NODE_INTKEY  0x01  
#define PROLLY_NODE_BLOBKEY 0x02  

#define PROLLY_NODE_MAX_ITEMS 4096

typedef struct ProllyNode ProllyNode;
struct ProllyNode {
  const u8 *pData;    
  int nData;           
  u8 level;            
  u16 nItems;          
  u8 flags;            
  const u32 *aKeyOff;  
  const u32 *aValOff;  
  const u8 *pKeyData;  
  const u8 *pValData;  
  ProllyHash addr;     
};

int prollyNodeParse(ProllyNode *pNode, const u8 *pData, int nData);

void prollyNodeKey(const ProllyNode *pNode, int i, const u8 **ppKey, int *pnKey);

void prollyNodeValue(const ProllyNode *pNode, int i, const u8 **ppVal, int *pnVal);

i64 prollyNodeIntKey(const ProllyNode *pNode, int i);

void prollyNodeChildHash(const ProllyNode *pNode, int i, ProllyHash *pHash);

int prollyNodeSearchBlob(const ProllyNode *pNode,
                         const u8 *pKey, int nKey, int *pRes);

int prollyNodeSearchInt(const ProllyNode *pNode, i64 intKey, int *pRes);

typedef struct ProllyNodeBuilder ProllyNodeBuilder;
struct ProllyNodeBuilder {
  u8 level;
  u8 flags;
  int nItems;
  int nKeyBytes;
  int nValBytes;
  int nAlloc;          
  u32 *aKeyOff;        
  u32 *aValOff;        
  u8 *pKeyBuf;         
  int nKeyBufAlloc;
  u8 *pValBuf;         
  int nValBufAlloc;
};

void prollyNodeBuilderInit(ProllyNodeBuilder *b, u8 level, u8 flags);

int prollyNodeBuilderAdd(ProllyNodeBuilder *b,
                         const u8 *pKey, int nKey,
                         const u8 *pVal, int nVal);

int prollyNodeBuilderFinish(ProllyNodeBuilder *b, u8 **ppOut, int *pnOut);

void prollyNodeBuilderReset(ProllyNodeBuilder *b);

void prollyNodeBuilderFree(ProllyNodeBuilder *b);

void prollyNodeComputeHash(const u8 *pData, int nData, ProllyHash *pOut);

int prollyCompareKeys(
  u8 flags,
  const u8 *pKey1, int nKey1, i64 iKey1,
  const u8 *pKey2, int nKey2, i64 iKey2
);

#endif 
