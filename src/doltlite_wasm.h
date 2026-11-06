#ifndef DOLTLITE_WASM_H
#define DOLTLITE_WASM_H

#ifdef DOLTLITE_PROLLY
void sqlite3WasmDisposalNotify(int, const void *);
#define SQLITE_DISPOSAL_NOTIFIER(T,P) sqlite3WasmDisposalNotify(T,P)
#endif

#endif
