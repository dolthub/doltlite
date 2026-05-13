
#ifdef DOLTLITE_PROLLY

#include "chunk_wal.h"
#include <string.h>

void walStateInit(WalState *w){
  memset(w, 0, sizeof(*w));
}

void walStateReset(WalState *w){
  w->iWalOffset = 0;
  w->nWalData = 0;
}

i64 walStateGetOffset(const WalState *w){
  return w->iWalOffset;
}

i64 walStateGetDataSize(const WalState *w){
  return w->nWalData;
}

void walStateSetOffset(WalState *w, i64 iOffset){
  w->iWalOffset = iOffset;
}

void walStateSetDataSize(WalState *w, i64 nData){
  w->nWalData = nData;
}

#endif
