
#ifndef DOLTLITE_CHUNK_WAL_H
#define DOLTLITE_CHUNK_WAL_H

#include "sqliteInt.h"

typedef struct WalState WalState;

struct WalState {
  i64 iWalOffset;
  i64 nWalData;
};

void walStateInit(WalState *w);
void walStateReset(WalState *w);

i64 walStateGetOffset(const WalState *w);
i64 walStateGetDataSize(const WalState *w);

void walStateSetOffset(WalState *w, i64 iOffset);
void walStateSetDataSize(WalState *w, i64 nData);

#endif
