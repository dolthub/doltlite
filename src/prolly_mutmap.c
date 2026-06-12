
#ifdef DOLTLITE_PROLLY

#include "prolly_mutmap.h"
#include "prolly_node.h"
#include <string.h>
#include <stdlib.h>

#define MUTMAP_INIT_CAP 16
#define MUTMAP_MIN_HASH 32
#define MUTMAP_POS_UPDATE_LIMIT 64
#define MUTMAP_RADIX_SORT_MIN 128

i64 prollyMutMapEntryIntKey(const ProllyMutMapEntry *e){
  if( e->nKey != 8 || e->pKey == 0 ) return 0;
  return prollyDecodeIntKey(e->pKey);
}

static void prepKey(ProllyMutMap *mm,
                    const u8 **ppKey, int *pnKey,
                    i64 intKey, u8 buf[8]){
  if( !mm->isIntKey ) return;
  if( *ppKey != 0 && *pnKey > 0 ){
    assert( intKey == 0 );
    return;
  }
  prollyEncodeIntKey(intKey, buf);
  *ppKey = buf;
  *pnKey = 8;
}


static u64 keyPrefix64(const u8 *pKey, int nKey){
  u64 r = 0;
  switch( nKey>=8 ? 8 : nKey ){
    case 8: r |= (u64)pKey[7];
    case 7: r |= (u64)pKey[6] << 8;
    case 6: r |= (u64)pKey[5] << 16;
    case 5: r |= (u64)pKey[4] << 24;
    case 4: r |= (u64)pKey[3] << 32;
    case 3: r |= (u64)pKey[2] << 40;
    case 2: r |= (u64)pKey[1] << 48;
    case 1: r |= (u64)pKey[0] << 56;
    case 0: break;
  }
  return r;
}

static ProllyMutMapEntry *entryAtOrder(ProllyMutMap *mm, int idx){
  return &mm->aEntries[mm->aOrder[idx]];
}

static int compareEntryToKey(
  ProllyMutMap *mm,
  const ProllyMutMapEntry *e,
  const u8 *pKey, int nKey,
  u64 keyPrefix
){
  if( e->keyPrefix != keyPrefix ){
    return e->keyPrefix < keyPrefix ? -1 : 1;
  }
  if( mm->isIntKey ) return 0;
  return prollyKeyCmp(e->pKey, e->nKey, pKey, nKey);
}

static u32 hashKey(const u8 *pKey, int nKey){
  u32 h = 2166136261u;
  int i;
  for(i=0; i<nKey; i++){
    h ^= pKey[i];
    h *= 16777619u;
  }
  h ^= (u32)nKey;
  h *= 16777619u;
  return h;
}

typedef struct OrderPair {
  u64 prefix;
  int phys;
  int pad;
} OrderPair;

static int orderPairCmp(ProllyMutMap *mm, const OrderPair *a, const OrderPair *b){
  ProllyMutMapEntry *ea, *eb;
  if( a->prefix != b->prefix ) return a->prefix < b->prefix ? -1 : 1;
  if( mm->isIntKey ) return 0;
  ea = &mm->aEntries[a->phys];
  eb = &mm->aEntries[b->phys];
  return prollyKeyCmp(ea->pKey, ea->nKey, eb->pKey, eb->nKey);
}

static void mutmapInsertionSort(ProllyMutMap *mm, OrderPair *a, int lo, int hi){
  int i, j;
  OrderPair x;
  for(i = lo + 1; i <= hi; i++){
    x = a[i];
    for(j = i; j > lo && orderPairCmp(mm, &a[j-1], &x) > 0; j--){
      a[j] = a[j-1];
    }
    a[j] = x;
  }
}

static void mutmapQuickSort(ProllyMutMap *mm, OrderPair *a, int lo, int hi){
  while( hi - lo > 16 ){
    int mid = lo + ((hi - lo) >> 1);
    int i, j;
    OrderPair pivot, t;
    if( orderPairCmp(mm, &a[lo], &a[mid]) > 0 ){ t=a[lo]; a[lo]=a[mid]; a[mid]=t; }
    if( orderPairCmp(mm, &a[lo], &a[hi]) > 0 ){ t=a[lo]; a[lo]=a[hi]; a[hi]=t; }
    if( orderPairCmp(mm, &a[mid], &a[hi]) > 0 ){ t=a[mid]; a[mid]=a[hi]; a[hi]=t; }
    t = a[mid]; a[mid] = a[hi-1]; a[hi-1] = t;
    pivot = a[hi-1];
    i = lo;
    j = hi - 1;
    for(;;){
      while( orderPairCmp(mm, &a[++i], &pivot) < 0 );
      while( orderPairCmp(mm, &a[--j], &pivot) > 0 );
      if( i >= j ) break;
      t = a[i]; a[i] = a[j]; a[j] = t;
    }
    t = a[i]; a[i] = a[hi-1]; a[hi-1] = t;
    if( i - lo < hi - i ){
      mutmapQuickSort(mm, a, lo, i - 1);
      lo = i + 1;
    } else {
      mutmapQuickSort(mm, a, i + 1, hi);
      hi = i - 1;
    }
  }
  mutmapInsertionSort(mm, a, lo, hi);
}

static int mutmapSortOrder(ProllyMutMap *mm){
  int n = mm->nEntries;
  OrderPair *scratch;
  OrderPair *aux = 0;
  int needBytes;
  int i;
  if( n <= 1 ){
    /* A single live entry is always packed at physical slot 0; reset aOrder so
    ** a stale index left by a prior compaction can't point at a removed slot. */
    if( n == 1 ) mm->aOrder[0] = 0;
    return SQLITE_OK;
  }
  needBytes = (int)(mm->nAlloc * sizeof(OrderPair));
  if( n >= MUTMAP_RADIX_SORT_MIN ){
    if( mm->nAlloc > 0x7fffffff / (int)(2 * sizeof(OrderPair)) ){
      return SQLITE_TOOBIG;
    }
    needBytes = (int)(mm->nAlloc * 2 * sizeof(OrderPair));
  }
  if( mm->nSortScratchBytes < needBytes ){
    void *p = sqlite3_realloc(mm->aSortScratch, needBytes);
    if( !p ) return SQLITE_NOMEM;
    mm->aSortScratch = p;
    mm->nSortScratchBytes = needBytes;
  }
  scratch = (OrderPair*)mm->aSortScratch;
  for(i=0; i<n; i++){
    ProllyMutMapEntry *e = &mm->aEntries[i];
    scratch[i].prefix = e->keyPrefix;
    scratch[i].phys = i;
    scratch[i].pad = 0;
  }
  if( n >= MUTMAP_RADIX_SORT_MIN ){
    int pass;
    aux = scratch + mm->nAlloc;
    for(pass=0; pass<8; pass++){
      int count[256];
      int pos[256];
      int shift = pass * 8;
      memset(count, 0, sizeof(count));
      for(i=0; i<n; i++){
        count[(scratch[i].prefix >> shift) & 0xff]++;
      }
      pos[0] = 0;
      for(i=1; i<256; i++){
        pos[i] = pos[i-1] + count[i-1];
      }
      for(i=0; i<n; i++){
        int b = (scratch[i].prefix >> shift) & 0xff;
        aux[pos[b]++] = scratch[i];
      }
      {
        OrderPair *tmp = scratch;
        scratch = aux;
        aux = tmp;
      }
    }
    for(i=0; i<n; ){
      int j = i + 1;
      while( j<n && scratch[j].prefix==scratch[i].prefix ) j++;
      if( j-i > 1 ){
        mutmapQuickSort(mm, scratch, i, j - 1);
      }
      i = j;
    }
  }else{
    mutmapQuickSort(mm, scratch, 0, n - 1);
  }
  for(i=0; i<n; i++){
    mm->aOrder[i] = scratch[i].phys;
  }
  return SQLITE_OK;
}

static int encodeLevel(ProllyMutMap *mm, int level){
  if( level<=0 ) return 0;
  return level + mm->levelBase;
}

static int decodeLevel(ProllyMutMap *mm, int stored){
  if( stored<=0 ) return 0;
  if( stored<=mm->levelBase ) return 0;
  return stored - mm->levelBase;
}

static int entryHasInlineKey(ProllyMutMapEntry *e){
  return e->pKey==e->aKeyInline;
}

static void fixInlineKeyPtr(ProllyMutMapEntry *e){
  if( e->nKey > 0 && e->nKey <= (int)sizeof(e->aKeyInline) ){
    e->pKey = e->aKeyInline;
  }
}

static void freeEntryData(ProllyMutMapEntry *e){
  if( !entryHasInlineKey(e) ){
    sqlite3_free(e->pKey);
  }
  sqlite3_free(e->pVal);
  e->pKey = 0;
  e->pVal = 0;
  e->nKey = 0;
  e->keyPrefix = 0;
  e->keyHash = 0;
  e->nVal = 0;
  e->nValAlloc = 0;
}

static int copyEntryData(ProllyMutMap *mm, ProllyMutMapEntry *e,
                         const u8 *pKey, int nKey,
                         const u8 *pVal, int nVal){
  e->pKey = 0;
  e->nKey = 0;
  e->keyPrefix = 0;
  e->keyHash = 0;
  e->pVal = 0;
  e->nVal = 0;
  if( pKey && nKey>0 ){
    if( nKey <= (int)sizeof(e->aKeyInline) ){
      e->pKey = e->aKeyInline;
    }else{
      e->pKey = (u8*)sqlite3_malloc(nKey);
      if( !e->pKey ) return SQLITE_NOMEM;
    }
    memcpy(e->pKey, pKey, nKey);
    e->nKey = nKey;
    e->keyPrefix = keyPrefix64(pKey, nKey);
    e->keyHash = hashKey(pKey, nKey);
  }
  if( pVal && nVal>0 ){
    e->pVal = (u8*)sqlite3_malloc(nVal);
    if( !e->pVal ){
      if( !entryHasInlineKey(e) ){
        sqlite3_free(e->pKey);
      }
      e->pKey = 0;
      return SQLITE_NOMEM;
    }
    memcpy(e->pVal, pVal, nVal);
    e->nVal = nVal;
    e->nValAlloc = nVal;
  }
  return SQLITE_OK;
}

static int replaceEntryValue(ProllyMutMapEntry *e, const u8 *pVal, int nVal){
  if( !pVal || nVal<=0 ){
    sqlite3_free(e->pVal);
    e->pVal = 0;
    e->nVal = 0;
    e->nValAlloc = 0;
    return SQLITE_OK;
  }
  if( e->nValAlloc < nVal ){
    u8 *pNew = (u8*)sqlite3_realloc(e->pVal, nVal);
    if( !pNew ) return SQLITE_NOMEM;
    e->pVal = pNew;
    e->nValAlloc = nVal;
  }
  memcpy(e->pVal, pVal, nVal);
  e->nVal = nVal;
  return SQLITE_OK;
}

static int bsearch_key(ProllyMutMap *mm,
                       const u8 *pKey, int nKey,
                       int *pFound){
  int lo = 0, hi = mm->nEntries;
  u64 prefix = keyPrefix64(pKey, nKey);
  *pFound = 0;
  while( lo < hi ){
    int mid = lo + (hi - lo) / 2;
    ProllyMutMapEntry *e = entryAtOrder(mm, mid);
    int c = compareEntryToKey(mm, e, pKey, nKey, prefix);
    if( c < 0 ){
      lo = mid + 1;
    }else if( c > 0 ){
      hi = mid;
    }else{
      *pFound = 1;
      return mid;
    }
  }
  return lo;
}

static int hashEntryMatches(
  ProllyMutMap *mm, int phys,
  const u8 *pKey, int nKey,
  u32 h
){
  ProllyMutMapEntry *e = &mm->aEntries[phys];
  if( e->keyHash!=h ) return 0;
  if( mm->isIntKey ){
    return e->keyPrefix==keyPrefix64(pKey, nKey);
  }
  return e->nKey==nKey && memcmp(e->pKey, pKey, nKey)==0;
}

static int rebuildHash(ProllyMutMap *mm){
  int i;
  int nHash = MUTMAP_MIN_HASH;
  while( nHash < (mm->nEntries > 0 ? mm->nEntries * 2 : 1) ) nHash *= 2;
  if( nHash > mm->nHashAlloc ){
    int *aNew = sqlite3_realloc(mm->aHash, nHash * sizeof(int));
    if( !aNew ) return SQLITE_NOMEM;
    mm->aHash = aNew;
    mm->nHashAlloc = nHash;
  }else if( mm->nHashAlloc==0 ){
    return SQLITE_OK;
  }
  memset(mm->aHash, 0, mm->nHashAlloc * sizeof(int));
  for(i=0; i<mm->nEntries; i++){
    u32 h = mm->aEntries[i].keyHash;
    int mask = mm->nHashAlloc - 1;
    int slot = (int)(h & (u32)mask);
    while( mm->aHash[slot] != 0 ){
      slot = (slot + 1) & mask;
    }
    mm->aHash[slot] = i + 1;
  }
  return SQLITE_OK;
}

static int ensureHashForInsert(ProllyMutMap *mm){
  if( mm->keepSorted ) return SQLITE_OK;
  if( mm->nHashAlloc==0 || (mm->nEntries + 1) * 2 > mm->nHashAlloc ){
    return rebuildHash(mm);
  }
  return SQLITE_OK;
}

static void hashInsertPhys(ProllyMutMap *mm, int phys){
  u32 h;
  int mask;
  int slot;
  if( mm->keepSorted || mm->nHashAlloc==0 ) return;
  h = mm->aEntries[phys].keyHash;
  mask = mm->nHashAlloc - 1;
  slot = (int)(h & (u32)mask);
  while( mm->aHash[slot] != 0 ){
    slot = (slot + 1) & mask;
  }
  mm->aHash[slot] = phys + 1;
}

static int findPhysLazy(ProllyMutMap *mm,
                        const u8 *pKey, int nKey,
                        int *pPhys){
  *pPhys = -1;
  if( mm->nEntries==0 ) return SQLITE_OK;
  if( mm->nHashAlloc==0 ){
    int rc = rebuildHash(mm);
    if( rc!=SQLITE_OK ) return rc;
  }
  {
    u32 h = hashKey(pKey, nKey);
    int mask = mm->nHashAlloc - 1;
    int slot = (int)(h & (u32)mask);
    while( mm->aHash[slot] != 0 ){
      int phys = mm->aHash[slot] - 1;
      if( hashEntryMatches(mm, phys, pKey, nKey, h) ){
        *pPhys = phys;
        return SQLITE_OK;
      }
      slot = (slot + 1) & mask;
    }
  }
  return SQLITE_OK;
}

static int ensureOrder(ProllyMutMap *mm){
  int i;
  int rc;
  mm->preferSorted = 1;
  if( mm->keepSorted ) return SQLITE_OK;
  if( !mm->orderDirty ){
    if( mm->posDirty ){
      for(i=0; i<mm->nEntries; i++){
        mm->aPos[mm->aOrder[i]] = i;
      }
      mm->posDirty = 0;
    }
    return SQLITE_OK;
  }
  if( mm->appendSorted ){
    for(i=0; i<mm->nEntries; i++){
      mm->aOrder[i] = i;
      mm->aPos[i] = i;
    }
    mm->orderDirty = 0;
    mm->posDirty = 0;
    return SQLITE_OK;
  }
  rc = mutmapSortOrder(mm);
  if( rc!=SQLITE_OK ) return rc;
  for(i=0; i<mm->nEntries; i++){
    mm->aPos[mm->aOrder[i]] = i;
  }
  mm->orderDirty = 0;
  mm->posDirty = 0;
  return SQLITE_OK;
}

/* Materialize the sorted iteration order now, propagating an allocation
** failure. The void iterator-init helpers call ensureOrder() internally and
** discard its return code, so a caller that must not iterate a stale order
** under OOM (e.g. a tree build) calls this first and bails on error. */
int prollyMutMapEnsureOrder(ProllyMutMap *mm){
  return ensureOrder(mm);
}

static int rankEntryWithoutOrder(ProllyMutMap *mm, int phys){
  ProllyMutMapEntry *target = &mm->aEntries[phys];
  int rank = 0;
  int i;
  for(i=0; i<mm->nEntries; i++){
    if( i==phys ) continue;
    if( prollyKeyCmp(mm->aEntries[i].pKey, mm->aEntries[i].nKey,
                       target->pKey, target->nKey) < 0 ){
      rank++;
    }
  }
  return rank;
}

static int ensureCapacity(ProllyMutMap *mm){
  if( mm->nEntries >= mm->nAlloc ){
    int nNew = mm->nAlloc ? mm->nAlloc * 2 : MUTMAP_INIT_CAP;
    ProllyMutMapEntry *aNew;
    int *aOrderNew;
    int *aPosNew;
    aNew = sqlite3_realloc(mm->aEntries, nNew * sizeof(ProllyMutMapEntry));
    if( !aNew ){
      return SQLITE_NOMEM;
    }
    if( aNew != mm->aEntries && mm->nEntries > 0 ){
      int i;
      for(i=0; i<mm->nEntries; i++){
        fixInlineKeyPtr(&aNew[i]);
      }
    }
    mm->aEntries = aNew;
    aOrderNew = sqlite3_realloc(mm->aOrder, nNew * sizeof(int));
    if( !aOrderNew ){
      return SQLITE_NOMEM;
    }
    mm->aOrder = aOrderNew;
    aPosNew = sqlite3_realloc(mm->aPos, nNew * sizeof(int));
    if( !aPosNew ){
      return SQLITE_NOMEM;
    }
    mm->aPos = aPosNew;
    mm->nAlloc = nNew;
  }
  return SQLITE_OK;
}

static void insertOrderEntry(ProllyMutMap *mm, int idx, int phys){
  int shifted = mm->nEntries - idx;
  if( idx < mm->nEntries ){
    memmove(&mm->aOrder[idx+1], &mm->aOrder[idx],
            (mm->nEntries - idx) * sizeof(int));
  }
  mm->aOrder[idx] = phys;
  if( !mm->isIntKey || shifted <= MUTMAP_POS_UPDATE_LIMIT ){
    int i;
    for(i=idx; i<=mm->nEntries; i++){
      mm->aPos[mm->aOrder[i]] = i;
    }
  }else{
    mm->posDirty = 1;
  }
}

int prollyMutMapInit(ProllyMutMap *mm, u8 isIntKey){
  return prollyMutMapInitMode(mm, isIntKey, 1);
}

int prollyMutMapInitMode(ProllyMutMap *mm, u8 isIntKey, u8 keepSorted){
  memset(mm, 0, sizeof(*mm));
  mm->isIntKey = isIntKey;
  mm->keepSorted = keepSorted;
  mm->appendSorted = 1;
  mm->orderDirty = keepSorted ? 0 : 1;
  return SQLITE_OK;
}

static void updateAppendSorted(ProllyMutMap *mm, int phys){
  ProllyMutMapEntry *pPrev;
  ProllyMutMapEntry *pNew;
  if( mm->keepSorted || !mm->appendSorted || phys<=0 ) return;
  pPrev = &mm->aEntries[phys - 1];
  pNew = &mm->aEntries[phys];
  if( prollyKeyCmp(pPrev->pKey, pPrev->nKey,
                     pNew->pKey, pNew->nKey) > 0 ){
    mm->appendSorted = 0;
  }
}

static int appendUndoRec(ProllyMutMap *mm, int idx){
  ProllyMutMapEntry *e;
  ProllyMutMapUndoRec *rec;
  if( mm->nUndo >= mm->nUndoAlloc ){
    int nNew = mm->nUndoAlloc ? mm->nUndoAlloc*2 : 8;
    ProllyMutMapUndoRec *aNew = sqlite3_realloc(mm->aUndo,
        nNew * sizeof(ProllyMutMapUndoRec));
    if( !aNew ) return SQLITE_NOMEM;
    mm->aUndo = aNew;
    mm->nUndoAlloc = nNew;
  }
  e = &mm->aEntries[idx];
  rec = &mm->aUndo[mm->nUndo];
  rec->level = mm->currentSavepointLevel;
  rec->entryIdx = idx;
  rec->prevBornAt = decodeLevel(mm, e->bornAt);
  rec->prevOp = e->op;
  rec->nPrevVal = e->nVal;
  if( e->nVal > 0 && e->pVal ){
    rec->prevVal = (u8*)sqlite3_malloc(e->nVal);
    if( !rec->prevVal ) return SQLITE_NOMEM;
    memcpy(rec->prevVal, e->pVal, e->nVal);
  }else{
    rec->prevVal = 0;
  }
  mm->nUndo++;
  return SQLITE_OK;
}

int prollyMutMapInsert(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  const u8 *pVal, int nVal
){
  int found = 0, idx = 0, rc, phys = -1;
  u8 keyBuf[8];
  prepKey(mm, &pKey, &nKey, intKey, keyBuf);

  if( mm->keepSorted || !mm->orderDirty ){
    idx = bsearch_key(mm, pKey, nKey, &found);
    if( found ){
      phys = mm->aOrder[idx];
    }
  }else{
    rc = findPhysLazy(mm, pKey, nKey, &phys);
    if( rc!=SQLITE_OK ) return rc;
    found = (phys >= 0);
  }

  if( found ){
    ProllyMutMapEntry *e = &mm->aEntries[phys];

    if( mm->currentSavepointLevel > 0
     && decodeLevel(mm, e->bornAt) < mm->currentSavepointLevel ){
      rc = appendUndoRec(mm, phys);
      if( rc!=SQLITE_OK ) return rc;
    }
    e->op = PROLLY_EDIT_INSERT;
    rc = replaceEntryValue(e, pVal, nVal);
    if( rc!=SQLITE_OK ) return rc;
    e->bornAt = encodeLevel(mm, mm->currentSavepointLevel);
    return SQLITE_OK;
  }

  rc = ensureCapacity(mm);
  if( rc!=SQLITE_OK ) return rc;
  rc = ensureHashForInsert(mm);
  if( rc!=SQLITE_OK ) return rc;

  {
    ProllyMutMapEntry *e;
    phys = mm->nEntries;
    e = &mm->aEntries[phys];
    memset(e, 0, sizeof(*e));
    e->op = PROLLY_EDIT_INSERT;
    e->bornAt = encodeLevel(mm, mm->currentSavepointLevel);
    rc = copyEntryData(mm, e, pKey, nKey, pVal, nVal);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    updateAppendSorted(mm, phys);
    if( mm->keepSorted || (!mm->orderDirty && mm->preferSorted) ){
      insertOrderEntry(mm, idx, phys);
    }else{
      mm->aOrder[phys] = phys;
      mm->aPos[phys] = phys;
      mm->orderDirty = 1;
    }
  }

  mm->nEntries++;
  if( !mm->keepSorted ){
    hashInsertPhys(mm, phys);
  }
  mm->generation++;
  return SQLITE_OK;
}

/* Like prollyMutMapInsert, but adopts pVal instead of copying it.
** Ownership transfers to the entry only on SQLITE_OK; on error the
** caller still owns pVal. Lets large values (an expanded zeroblob)
** enter the map without a second full-size buffer. */
int prollyMutMapInsertOwnedVal(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  u8 *pVal, int nVal
){
  int found = 0, idx = 0, rc, phys = -1;
  u8 keyBuf[8];
  prepKey(mm, &pKey, &nKey, intKey, keyBuf);

  if( mm->keepSorted || !mm->orderDirty ){
    idx = bsearch_key(mm, pKey, nKey, &found);
    if( found ){
      phys = mm->aOrder[idx];
    }
  }else{
    rc = findPhysLazy(mm, pKey, nKey, &phys);
    if( rc!=SQLITE_OK ) return rc;
    found = (phys >= 0);
  }

  if( found ){
    ProllyMutMapEntry *e = &mm->aEntries[phys];

    if( mm->currentSavepointLevel > 0
     && decodeLevel(mm, e->bornAt) < mm->currentSavepointLevel ){
      rc = appendUndoRec(mm, phys);
      if( rc!=SQLITE_OK ) return rc;
    }
    e->op = PROLLY_EDIT_INSERT;
    sqlite3_free(e->pVal);
    e->pVal = pVal;
    e->nVal = nVal;
    e->nValAlloc = nVal;
    e->bornAt = encodeLevel(mm, mm->currentSavepointLevel);
    return SQLITE_OK;
  }

  rc = ensureCapacity(mm);
  if( rc!=SQLITE_OK ) return rc;
  rc = ensureHashForInsert(mm);
  if( rc!=SQLITE_OK ) return rc;

  {
    ProllyMutMapEntry *e;
    phys = mm->nEntries;
    e = &mm->aEntries[phys];
    memset(e, 0, sizeof(*e));
    e->op = PROLLY_EDIT_INSERT;
    e->bornAt = encodeLevel(mm, mm->currentSavepointLevel);
    rc = copyEntryData(mm, e, pKey, nKey, 0, 0);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    e->pVal = pVal;
    e->nVal = nVal;
    e->nValAlloc = nVal;
    updateAppendSorted(mm, phys);
    if( mm->keepSorted || (!mm->orderDirty && mm->preferSorted) ){
      insertOrderEntry(mm, idx, phys);
    }else{
      mm->aOrder[phys] = phys;
      mm->aPos[phys] = phys;
      mm->orderDirty = 1;
    }
  }

  mm->nEntries++;
  if( !mm->keepSorted ){
    hashInsertPhys(mm, phys);
  }
  mm->generation++;
  return SQLITE_OK;
}

int prollyMutMapReplaceEntry(
  ProllyMutMap *mm,
  ProllyMutMapEntry *e,
  const u8 *pVal,
  int nVal
){
  int rc;
  int phys;
  if( !mm || !e ) return SQLITE_MISUSE;
  phys = (int)(e - mm->aEntries);
  if( phys<0 || phys>=mm->nEntries ) return SQLITE_CORRUPT_BKPT;
  if( mm->currentSavepointLevel > 0
   && decodeLevel(mm, e->bornAt) < mm->currentSavepointLevel ){
    rc = appendUndoRec(mm, phys);
    if( rc!=SQLITE_OK ) return rc;
  }
  e->op = PROLLY_EDIT_INSERT;
  rc = replaceEntryValue(e, pVal, nVal);
  if( rc!=SQLITE_OK ) return rc;
  e->bornAt = encodeLevel(mm, mm->currentSavepointLevel);
  return SQLITE_OK;
}

int prollyMutMapDelete(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey
){
  int found = 0, idx = 0, rc, phys = -1;
  u8 keyBuf[8];
  prepKey(mm, &pKey, &nKey, intKey, keyBuf);

  if( mm->keepSorted || !mm->orderDirty ){
    idx = bsearch_key(mm, pKey, nKey, &found);
    if( found ){
      phys = mm->aOrder[idx];
    }
  }else{
    rc = findPhysLazy(mm, pKey, nKey, &phys);
    if( rc!=SQLITE_OK ) return rc;
    found = (phys >= 0);
  }

  if( found ){
    ProllyMutMapEntry *e = &mm->aEntries[phys];
    if( e->op == PROLLY_EDIT_INSERT ){
      if( mm->currentSavepointLevel > 0
       && decodeLevel(mm, e->bornAt) < mm->currentSavepointLevel ){
        rc = appendUndoRec(mm, phys);
        if( rc!=SQLITE_OK ) return rc;
      }
      e->op = PROLLY_EDIT_DELETE;
      e->nVal = 0;
      e->bornAt = encodeLevel(mm, mm->currentSavepointLevel);
      return SQLITE_OK;
    }

    return SQLITE_OK;
  }

  rc = ensureCapacity(mm);
  if( rc!=SQLITE_OK ) return rc;
  rc = ensureHashForInsert(mm);
  if( rc!=SQLITE_OK ) return rc;

  {
    ProllyMutMapEntry *e;
    phys = mm->nEntries;
    e = &mm->aEntries[phys];
    memset(e, 0, sizeof(*e));
    e->op = PROLLY_EDIT_DELETE;
    e->bornAt = encodeLevel(mm, mm->currentSavepointLevel);
    rc = copyEntryData(mm, e, pKey, nKey, 0, 0);
    if( rc!=SQLITE_OK ){
      return rc;
    }
    updateAppendSorted(mm, phys);
    if( mm->keepSorted || (!mm->orderDirty && mm->preferSorted) ){
      insertOrderEntry(mm, idx, phys);
    }else{
      mm->aOrder[phys] = phys;
      mm->aPos[phys] = phys;
      mm->orderDirty = 1;
    }
  }

  mm->nEntries++;
  if( !mm->keepSorted ){
    hashInsertPhys(mm, phys);
  }
  mm->generation++;
  return SQLITE_OK;
}

int prollyMutMapDeleteEntry(
  ProllyMutMap *mm,
  ProllyMutMapEntry *e
){
  int rc;
  int phys;
  if( !mm || !e ) return SQLITE_MISUSE;
  phys = (int)(e - mm->aEntries);
  if( phys<0 || phys>=mm->nEntries ) return SQLITE_CORRUPT_BKPT;
  if( e->op!=PROLLY_EDIT_INSERT ){
    return SQLITE_OK;
  }
  if( mm->currentSavepointLevel > 0
   && decodeLevel(mm, e->bornAt) < mm->currentSavepointLevel ){
    rc = appendUndoRec(mm, phys);
    if( rc!=SQLITE_OK ) return rc;
  }
  e->op = PROLLY_EDIT_DELETE;
  e->nVal = 0;
  e->bornAt = encodeLevel(mm, mm->currentSavepointLevel);
  return SQLITE_OK;
}

void prollyMutMapPushSavepoint(ProllyMutMap *mm, int level){
  if( !mm ) return;
  mm->currentSavepointLevel = level;
}

int prollyMutMapRollbackToSavepoint(ProllyMutMap *mm, int level){
  int i;
  int rc;
  if( !mm ) return SQLITE_OK;

  if( mm->nEntries==0 && mm->nUndo==0 ){
    mm->currentSavepointLevel = level - 1;
    return SQLITE_OK;
  }

  while( mm->nUndo > 0
      && mm->aUndo[mm->nUndo - 1].level >= level ){
    ProllyMutMapUndoRec *rec = &mm->aUndo[mm->nUndo - 1];
    int idx = rec->entryIdx;
    if( idx >= 0 && idx < mm->nEntries ){
      ProllyMutMapEntry *e = &mm->aEntries[idx];
      e->op = rec->prevOp;
      e->bornAt = encodeLevel(mm, rec->prevBornAt);
      rc = replaceEntryValue(e, rec->prevVal, rec->nPrevVal);
      if( rc!=SQLITE_OK ) return rc;
    }
    sqlite3_free(rec->prevVal);
    rec->prevVal = 0;
    mm->nUndo--;
  }

  {
    int oldN = mm->nEntries;
    int newN = 0;
    for(i=0; i<oldN; i++){
      if( decodeLevel(mm, mm->aEntries[i].bornAt) >= level ){
        freeEntryData(&mm->aEntries[i]);
        mm->aPos[i] = -1;
      }else{
        if( newN != i ){
          mm->aEntries[newN] = mm->aEntries[i];
          fixInlineKeyPtr(&mm->aEntries[newN]);
        }
        mm->aPos[i] = newN++;
      }
    }
    {
      int j;
      int out = 0;
      for(j=0; j<mm->nUndo; j++){
        int idx = mm->aUndo[j].entryIdx;
        mm->aUndo[j].entryIdx = idx>=0 && idx<oldN ? mm->aPos[idx] : -1;
      }
      if( mm->keepSorted ){
        for(j=0; j<oldN; j++){
          int mapped = mm->aPos[mm->aOrder[j]];
          if( mapped >= 0 ){
            mm->aOrder[out++] = mapped;
          }
        }
        mm->nEntries = out;
        for(j=0; j<mm->nEntries; j++){
          mm->aPos[mm->aOrder[j]] = j;
        }
        mm->posDirty = 0;
      }else{
        mm->nEntries = newN;
        mm->orderDirty = 1;
        mm->posDirty = 1;
        mm->appendSorted = 0;
      }
    }
  }

  if( !mm->keepSorted ){
    int rc = rebuildHash(mm);
    if( rc!=SQLITE_OK ) return rc;
  }

  mm->generation++;

  mm->currentSavepointLevel = level - 1;
  return SQLITE_OK;
}

void prollyMutMapReleaseSavepoint(ProllyMutMap *mm, int level){
  int i;
  int target;
  if( !mm ) return;
  target = level - 1;

  /* The levelBase shortcut is only safe when no nested edits exist. */
  if( level==1 && target==0 && mm->currentSavepointLevel<=1 ){
    for(i=0; i<mm->nUndo; i++){
      sqlite3_free(mm->aUndo[i].prevVal);
      mm->aUndo[i].prevVal = 0;
    }
    mm->nUndo = 0;
    mm->levelBase++;
    mm->currentSavepointLevel = 0;
    return;
  }

  if( target == 0 ){
    for(i=0; i<mm->nUndo; i++){
      sqlite3_free(mm->aUndo[i].prevVal);
      mm->aUndo[i].prevVal = 0;
    }
    mm->nUndo = 0;
  }else{
    for(i=0; i<mm->nUndo; i++){
      if( mm->aUndo[i].level >= level ){
        mm->aUndo[i].level = target;
      }
    }
  }

  for(i=0; i<mm->nEntries; i++){
    if( decodeLevel(mm, mm->aEntries[i].bornAt) >= level ){
      mm->aEntries[i].bornAt = encodeLevel(mm, target);
    }
  }

  mm->currentSavepointLevel = target;
}

int prollyMutMapFindRc(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  ProllyMutMapEntry **ppEntry
){
  int found, idx, phys, rc;
  u8 keyBuf[8];
  *ppEntry = 0;
  if( mm->nEntries==0 ) return SQLITE_OK;
  prepKey(mm, &pKey, &nKey, intKey, keyBuf);
  if( mm->keepSorted ){
    idx = bsearch_key(mm, pKey, nKey, &found);
    *ppEntry = found ? entryAtOrder(mm, idx) : 0;
    return SQLITE_OK;
  }
  rc = findPhysLazy(mm, pKey, nKey, &phys);
  if( rc!=SQLITE_OK ) return rc;
  *ppEntry = phys >= 0 ? &mm->aEntries[phys] : 0;
  return SQLITE_OK;
}

ProllyMutMapEntry *prollyMutMapEntryAt(ProllyMutMap *mm, int idx){
  ensureOrder(mm);
  return entryAtOrder(mm, idx);
}

int prollyMutMapResolveSortedPos(
  ProllyMutMap *mm,
  const u8 *pKey, int nKey, i64 intKey,
  int *pIdx, int *pFound
){
  u8 keyBuf[8];
  int rc;
  *pIdx = 0;
  *pFound = 0;
  if( mm->nEntries==0 ) return SQLITE_OK;
  prepKey(mm, &pKey, &nKey, intKey, keyBuf);
  rc = ensureOrder(mm);
  if( rc!=SQLITE_OK ) return rc;
  *pIdx = bsearch_key(mm, pKey, nKey, pFound);
  return SQLITE_OK;
}

int prollyMutMapOrderIndexFromEntry(ProllyMutMap *mm, ProllyMutMapEntry *pEntry){
  int phys = (int)(pEntry - mm->aEntries);
  if( mm->keepSorted ){
    int found = 0;
    int idx = bsearch_key(mm, pEntry->pKey, pEntry->nKey, &found);
    return found ? idx : mm->nEntries;
  }
  if( !mm->keepSorted && mm->orderDirty ){
    return rankEntryWithoutOrder(mm, phys);
  }
  ensureOrder(mm);
  return mm->aPos[phys];
}

int prollyMutMapCount(ProllyMutMap *mm){
  return mm->nEntries;
}

int prollyMutMapIsEmpty(ProllyMutMap *mm){
  return mm->nEntries == 0;
}

void prollyMutMapIterFirst(ProllyMutMapIter *it, ProllyMutMap *mm){
  ensureOrder(mm);
  it->pMap = mm;
  it->idx = 0;
}

void prollyMutMapIterNext(ProllyMutMapIter *it){
  if( it->idx < it->pMap->nEntries ) it->idx++;
}

int prollyMutMapIterValid(ProllyMutMapIter *it){
  return it->idx >= 0 && it->idx < it->pMap->nEntries;
}

ProllyMutMapEntry *prollyMutMapIterEntry(ProllyMutMapIter *it){
  return entryAtOrder(it->pMap, it->idx);
}

void prollyMutMapIterSeek(ProllyMutMapIter *it, ProllyMutMap *mm,
                          const u8 *pKey, int nKey, i64 intKey){
  int found = 0;
  u8 keyBuf[8];
  ensureOrder(mm);
  prepKey(mm, &pKey, &nKey, intKey, keyBuf);
  it->pMap = mm;
  it->idx = bsearch_key(mm, pKey, nKey, &found);
}

void prollyMutMapIterLast(ProllyMutMapIter *it, ProllyMutMap *mm){
  ensureOrder(mm);
  it->pMap = mm;
  it->idx = mm->nEntries>0 ? mm->nEntries - 1 : mm->nEntries;
}

void prollyMutMapClear(ProllyMutMap *mm){
  int i;
  for(i=0; i<mm->nEntries; i++){
    freeEntryData(&mm->aEntries[i]);
  }
  mm->nEntries = 0;
  mm->orderDirty = 0;
  mm->preferSorted = 0;
  mm->posDirty = 0;
  mm->appendSorted = 1;
  mm->generation++;
  if( mm->aHash && mm->nHashAlloc>0 ){
    memset(mm->aHash, 0, mm->nHashAlloc * sizeof(int));
  }
  for(i=0; i<mm->nUndo; i++){
    sqlite3_free(mm->aUndo[i].prevVal);
  }
  mm->nUndo = 0;
  mm->levelBase = 0;
}

void prollyMutMapFree(ProllyMutMap *mm){
  prollyMutMapClear(mm);
  sqlite3_free(mm->aEntries);
  mm->aEntries = 0;
  sqlite3_free(mm->aOrder);
  mm->aOrder = 0;
  sqlite3_free(mm->aPos);
  mm->aPos = 0;
  sqlite3_free(mm->aHash);
  mm->aHash = 0;
  mm->nHashAlloc = 0;
  sqlite3_free(mm->aSortScratch);
  mm->aSortScratch = 0;
  mm->nSortScratchBytes = 0;
  mm->nAlloc = 0;
  sqlite3_free(mm->aUndo);
  mm->aUndo = 0;
  mm->nUndoAlloc = 0;
  mm->currentSavepointLevel = 0;
}

int prollyMutMapClone(ProllyMutMap **out, const ProllyMutMap *src){
  ProllyMutMap *dst;
  int i;
  *out = 0;
  dst = (ProllyMutMap*)sqlite3_malloc(sizeof(ProllyMutMap));
  if( !dst ) return SQLITE_NOMEM;
  memset(dst, 0, sizeof(*dst));
  dst->isIntKey = src->isIntKey;
  dst->keepSorted = src->keepSorted;
  dst->orderDirty = src->orderDirty;
  dst->preferSorted = src->preferSorted;
  dst->posDirty = src->posDirty;
  dst->appendSorted = src->appendSorted;
  dst->levelBase = src->levelBase;
  dst->currentSavepointLevel = src->currentSavepointLevel;
  dst->generation = src->generation;

  if( src->nEntries > 0 ){
    dst->aEntries = (ProllyMutMapEntry*)sqlite3_malloc(
        src->nEntries * (int)sizeof(ProllyMutMapEntry));
    dst->aOrder = (int*)sqlite3_malloc(src->nEntries * (int)sizeof(int));
    dst->aPos = (int*)sqlite3_malloc(src->nEntries * (int)sizeof(int));
    if( !dst->aEntries || !dst->aOrder || !dst->aPos ){
      prollyMutMapFree(dst);
      sqlite3_free(dst);
      return SQLITE_NOMEM;
    }
    memset(dst->aEntries, 0,
           src->nEntries * sizeof(ProllyMutMapEntry));
    dst->nAlloc = src->nEntries;
    for(i=0; i<src->nEntries; i++){
      ProllyMutMapEntry *se = &src->aEntries[i];
      ProllyMutMapEntry *de = &dst->aEntries[i];
      de->op = se->op;
      de->bornAt = se->bornAt;
      de->nKey = 0;
      de->keyPrefix = 0;
      de->keyHash = 0;
      de->nVal = 0;
      de->nValAlloc = 0;
      de->pKey = 0;
      de->pVal = 0;
      if( se->pKey && se->nKey > 0 ){
        if( se->nKey <= (int)sizeof(de->aKeyInline) ){
          de->pKey = de->aKeyInline;
        }else{
          de->pKey = (u8*)sqlite3_malloc(se->nKey);
          if( !de->pKey ){
            dst->nEntries = i;
            prollyMutMapFree(dst);
            sqlite3_free(dst);
            return SQLITE_NOMEM;
          }
        }
        memcpy(de->pKey, se->pKey, se->nKey);
        de->nKey = se->nKey;
        de->keyPrefix = se->keyPrefix;
        de->keyHash = se->keyHash;
      }
      if( se->pVal && se->nVal > 0 ){
        de->pVal = (u8*)sqlite3_malloc(se->nVal);
        if( !de->pVal ){
          if( !entryHasInlineKey(de) ){
            sqlite3_free(de->pKey);
          }
          de->pKey = 0;
          dst->nEntries = i;
          prollyMutMapFree(dst);
          sqlite3_free(dst);
          return SQLITE_NOMEM;
        }
        memcpy(de->pVal, se->pVal, se->nVal);
        de->nVal = se->nVal;
        de->nValAlloc = se->nVal;
      }
      dst->nEntries++;
    }
    if( src->keepSorted || !src->orderDirty ){
      memcpy(dst->aOrder, src->aOrder, src->nEntries * sizeof(int));
      memcpy(dst->aPos, src->aPos, src->nEntries * sizeof(int));
    }else{
      memset(dst->aOrder, 0, src->nEntries * sizeof(int));
      memset(dst->aPos, 0, src->nEntries * sizeof(int));
    }
  }

  if( src->nUndo > 0 ){
    dst->aUndo = (ProllyMutMapUndoRec*)sqlite3_malloc(
        src->nUndo * (int)sizeof(ProllyMutMapUndoRec));
    if( !dst->aUndo ){
      prollyMutMapFree(dst);
      sqlite3_free(dst);
      return SQLITE_NOMEM;
    }
    memset(dst->aUndo, 0,
           src->nUndo * sizeof(ProllyMutMapUndoRec));
    dst->nUndoAlloc = src->nUndo;
    for(i=0; i<src->nUndo; i++){
      ProllyMutMapUndoRec *sr = &src->aUndo[i];
      ProllyMutMapUndoRec *dr = &dst->aUndo[i];
      dr->level = sr->level;
      dr->entryIdx = sr->entryIdx;
      dr->prevBornAt = sr->prevBornAt;
      dr->prevOp = sr->prevOp;
      dr->nPrevVal = 0;
      dr->prevVal = 0;
      if( sr->prevVal && sr->nPrevVal > 0 ){
        dr->prevVal = (u8*)sqlite3_malloc(sr->nPrevVal);
        if( !dr->prevVal ){
          dst->nUndo = i;
          prollyMutMapFree(dst);
          sqlite3_free(dst);
          return SQLITE_NOMEM;
        }
        memcpy(dr->prevVal, sr->prevVal, sr->nPrevVal);
        dr->nPrevVal = sr->nPrevVal;
      }
      dst->nUndo++;
    }
  }

  if( !dst->keepSorted && dst->nEntries > 0 ){
    int rc = rebuildHash(dst);
    if( rc!=SQLITE_OK ){
      prollyMutMapFree(dst);
      sqlite3_free(dst);
      return rc;
    }
  }

  *out = dst;
  return SQLITE_OK;
}

#endif
