#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sqlite3.h"
#include "doltlite_merge_int.h"

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

static void test_build_record_overflow(void){
  DoltliteSerialValue aWrap[8];
  DoltliteSerialValue v;
  DoltliteSerialValue aValid[5];
  DoltliteRecordInfo info = {0};
  u8 *pRec;
  int nOut;
  int i;

  memset(aWrap, 0, sizeof(aWrap));
  for(i=0; i<4; i++){
    aWrap[i].eType = SQLITE_BLOB;
    aWrap[i].p = 0;
    aWrap[i].n = 600000000;
  }
  nOut = 12345;
  pRec = doltliteBuildRecord(aWrap, 4, &nOut);
  check("build_record_rejects_sum_overflow_4x600m", pRec==0 && nOut==0);
  sqlite3_free(pRec);

  for(i=0; i<8; i++){
    aWrap[i].eType = SQLITE_BLOB;
    aWrap[i].p = 0;
    aWrap[i].n = 0x20000000;
  }
  nOut = 12345;
  pRec = doltliteBuildRecord(aWrap, 8, &nOut);
  check("build_record_rejects_sum_wrap_to_small_pos", pRec==0 && nOut==0);
  sqlite3_free(pRec);

  memset(&v, 0, sizeof(v));
  v.eType = SQLITE_TEXT;
  v.p = 0;
  v.n = 0x40000000;
  nOut = 12345;
  pRec = doltliteBuildRecord(&v, 1, &nOut);
  check("build_record_rejects_text_serial_type_overflow", pRec==0 && nOut==0);
  sqlite3_free(pRec);

  v.eType = SQLITE_BLOB;
  v.n = 0x40000000;
  nOut = 12345;
  pRec = doltliteBuildRecord(&v, 1, &nOut);
  check("build_record_rejects_blob_serial_type_overflow", pRec==0 && nOut==0);
  sqlite3_free(pRec);

  v.eType = SQLITE_TEXT;
  v.n = 1073741818;
  nOut = 12345;
  pRec = doltliteBuildRecord(&v, 1, &nOut);
  check("build_record_rejects_text_serial_type_boundary", pRec==0 && nOut==0);
  sqlite3_free(pRec);

  v.eType = SQLITE_BLOB;
  v.n = 1073741818;
  nOut = 12345;
  pRec = doltliteBuildRecord(&v, 1, &nOut);
  check("build_record_rejects_blob_serial_type_boundary", pRec==0 && nOut==0);
  sqlite3_free(pRec);

  v.eType = SQLITE_TEXT;
  v.n = -1;
  nOut = 12345;
  pRec = doltliteBuildRecord(&v, 1, &nOut);
  check("build_record_rejects_negative_len", pRec==0 && nOut==0);
  sqlite3_free(pRec);

  memset(aValid, 0, sizeof(aValid));
  aValid[0].eType = SQLITE_NULL;
  aValid[1].eType = SQLITE_INTEGER;
  aValid[1].i = 42;
  aValid[2].eType = SQLITE_FLOAT;
  aValid[2].r = 3.5;
  aValid[3].eType = SQLITE_TEXT;
  aValid[3].p = "hello";
  aValid[3].n = 5;
  aValid[4].eType = SQLITE_BLOB;
  aValid[4].p = "\x01\x02\x03";
  aValid[4].n = 3;
  nOut = 0;
  pRec = doltliteBuildRecord(aValid, 5, &nOut);
  check("build_record_valid_roundtrip",
        pRec!=0 && nOut>0
        && doltliteParseRecordStrict(pRec, nOut, &info)==SQLITE_OK
        && info.nField==5);
  doltliteRecordInfoClear(&info);
  sqlite3_free(pRec);
}

static void test_build_index_entry_wide_header(void){
  static const struct {
    const char *zName;
    int nIdxCol;
    u64 expectedHdrSize;
    int expectedHdrVarintLen;
  } aCases[] = {
    { "index_entry_hdr_1byte_max_127",     125,   127, 1 },
    { "index_entry_hdr_2byte_min_129",     126,   129, 2 },
    { "index_entry_hdr_3byte_over_16383", 16384, 16388, 3 },
  };
  sqlite3 *db = 0;
  KeyInfo *pKeyInfo = 0;
  u8 rowRec[2] = { 0x02, 0x00 }; /* 1-field record: NULL (IPK placeholder) */
  int c;

  check("index_entry_open_memdb", sqlite3_open(":memory:", &db)==SQLITE_OK);
  if( !db ) return;
  /* nKeyField < nAllField forces doltliteBuildIndexEntry to store the raw
  ** index record payload in the mutmap so we can inspect its header varint. */
  pKeyInfo = sqlite3KeyInfoAlloc(db, 1, 1);
  check("index_entry_alloc_keyinfo", pKeyInfo!=0);
  if( !pKeyInfo ){
    sqlite3_close(db);
    return;
  }

  for(c=0; c<(int)(sizeof(aCases)/sizeof(aCases[0])); c++){
    int nIdxCol = aCases[c].nIdxCol;
    int nOut = nIdxCol + 1;
    i16 *aiColumn = (i16*)calloc((size_t)nIdxCol, sizeof(i16));
    ProllyMutMap mm;
    int rc;
    check(aCases[c].zName, aiColumn!=0);
    if( !aiColumn ) continue;

    prollyMutMapInit(&mm, 0);
    rc = doltliteIndexMutMapRowDelta(
        db, 0, &mm, aiColumn, nIdxCol, pKeyInfo,
        0, 42, 0, 0, 0, 0, rowRec, (int)sizeof(rowRec), 0);
    check(aCases[c].zName, rc==SQLITE_OK && mm.nEntries==1);
    if( rc==SQLITE_OK && mm.nEntries==1 ){
      const u8 *pVal = mm.aEntries[0].pVal;
      int nVal = mm.aEntries[0].nVal;
      u64 hdrSize = 0;
      int nVar = 0;
      int ok = (pVal!=0 && nVal==(int)(aCases[c].expectedHdrSize + (u64)nOut));
      if( ok ){
        nVar = dlReadVarint(pVal, pVal + nVal, &hdrSize);
        ok = (nVar==aCases[c].expectedHdrVarintLen
              && hdrSize==aCases[c].expectedHdrSize);
      }
      if( ok ){
        int j;
        for(j=0; j<nOut; j++){
          if( pVal[nVar + j]!=1 || pVal[(int)hdrSize + j]!=42 ){
            ok = 0;
            break;
          }
        }
      }
      check(aCases[c].zName, ok);
    }
    prollyMutMapFree(&mm);
    free(aiColumn);
  }

  sqlite3KeyInfoUnref(pKeyInfo);
  sqlite3_close(db);
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

  test_build_record_overflow();
  test_build_index_entry_wide_header();

  printf("record_info_stack_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
