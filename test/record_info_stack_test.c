#include <stdio.h>
#include "sqlite3.h"
#include "prolly_record.h"

static int nPass = 0;
static int nFail = 0;

static void check(const char *name, int condition){
  if( condition ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s (sizeof=%zu)\n", name, sizeof(DoltliteRecordInfo));
  }
}

int main(void){
  DoltliteRecordInfo info = {0};
  u8 rec[512];
  int i, n, rc;

  sqlite3_initialize();

  check("record_info_fits_small_stack", sizeof(DoltliteRecordInfo) < 1024);
  check("record_info_not_max_column_arrays",
        sizeof(DoltliteRecordInfo) < (size_t)SQLITE_MAX_COLUMN);

  n = 1;
  rec[0] = 1;
  for(i=0; i<40; i++){
    rec[n++] = 8;
  }
  rec[0] = (u8)n;
  rc = doltliteParseRecordStrict(rec, n, &info);
  check("record_info_grows_past_inline",
        rc==SQLITE_OK && info.nField==40
        && info.aType!=info.aTypeSpace);
  doltliteRecordInfoClear(&info);
  check("record_info_clear_returns_to_inline",
        info.aType==info.aTypeSpace);

  printf("record_info_stack_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
