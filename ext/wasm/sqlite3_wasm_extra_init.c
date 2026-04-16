#include "sqlite3.h"

int doltliteInstallAutoExt(void);

int sqlite3_wasm_extra_init(const char *z){
  (void)z;
  return doltliteInstallAutoExt();
}
