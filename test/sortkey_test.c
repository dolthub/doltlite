#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "sortkey.h"

static int nPass = 0;
static int nFail = 0;

static void check(const char *zName, int ok){
  if( ok ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n", zName);
  }
}

static KeyInfo *allocKeyInfo(int nField, const u8 *aFlags){
  KeyInfo *p = (KeyInfo*)sqlite3_malloc64(SZ_KEYINFO(nField) + (size_t)nField);
  if( !p ) return 0;
  memset(p, 0, SZ_KEYINFO(nField) + (size_t)nField);
  p->nRef = 1;
  p->enc = SQLITE_UTF8;
  p->nKeyField = (u16)nField;
  p->nAllField = (u16)nField;
  p->aSortFlags = (u8*)p + SZ_KEYINFO(nField);
  if( aFlags ) memcpy(p->aSortFlags, aFlags, (size_t)nField);
  return p;
}

static void checkCorruptAsc(const char *zName, const u8 *pKey, int nKey){
  u8 *pOut = 0;
  int nAlloc = 0;
  int nOut = -1;
  int rc = recordFromSortKeyBuffer(pKey, nKey, &pOut, &nAlloc, &nOut);
  check(zName, rc==SQLITE_CORRUPT);
  sqlite3_free(pOut);
}

static void checkCorruptColl(
  const char *zName,
  const u8 *pKey,
  int nKey,
  const KeyInfo *pKeyInfo
){
  u8 *pOut = 0;
  int nAlloc = 0;
  int nOut = -1;
  int rc = recordFromSortKeyBufferColl(pKey, nKey, pKeyInfo, &pOut, &nAlloc, &nOut);
  check(zName, rc==SQLITE_CORRUPT);
  sqlite3_free(pOut);
}

static void checkRoundTrip(
  const char *zName,
  const u8 *pRec,
  int nRec,
  const KeyInfo *pKeyInfo
){
  u8 *pKey = 0;
  int nKey = 0;
  u8 *pOut = 0;
  int nAlloc = 0;
  int nOut = 0;
  int rc;

  rc = sortKeyFromRecordPrefixColl(pRec, nRec, 0, pKeyInfo, &pKey, &nKey);
  if( rc!=SQLITE_OK ){
    check(zName, 0);
    sqlite3_free(pKey);
    return;
  }
  if( pKeyInfo ){
    rc = recordFromSortKeyBufferColl(pKey, nKey, pKeyInfo, &pOut, &nAlloc, &nOut);
  }else{
    rc = recordFromSortKeyBuffer(pKey, nKey, &pOut, &nAlloc, &nOut);
  }
  check(zName, rc==SQLITE_OK && nOut==nRec && memcmp(pOut, pRec, (size_t)nRec)==0);
  sqlite3_free(pKey);
  sqlite3_free(pOut);
}

static void testUnterminatedAsc(void){
  static const u8 aTextTagOnly[] = { SORTKEY_TEXT };
  static const u8 aBlobTagOnly[] = { SORTKEY_BLOB };
  static const u8 aTextMid[] = { SORTKEY_TEXT, 'a', 'b' };
  static const u8 aBlobMid[] = { SORTKEY_BLOB, 0x11, 0x22 };
  static const u8 aTextSingleZero[] = { SORTKEY_TEXT, 'a', 0x00 };
  static const u8 aBlobSingleZero[] = { SORTKEY_BLOB, 0x11, 0x00 };
  static const u8 aTextAfterEsc[] = { SORTKEY_TEXT, 'a', 0x00, 0x01 };
  static const u8 aBlobAfterEsc[] = { SORTKEY_BLOB, 0x11, 0x00, 0x01 };
  static const u8 aTextBadEsc[] = { SORTKEY_TEXT, 'a', 0x00, 0x02, 0x00, 0x00 };
  static const u8 aBlobBadEsc[] = { SORTKEY_BLOB, 0x11, 0x00, 0x02, 0x00, 0x00 };

  checkCorruptAsc("asc_text_tag_only", aTextTagOnly, sizeof(aTextTagOnly));
  checkCorruptAsc("asc_blob_tag_only", aBlobTagOnly, sizeof(aBlobTagOnly));
  checkCorruptAsc("asc_text_mid_payload", aTextMid, sizeof(aTextMid));
  checkCorruptAsc("asc_blob_mid_payload", aBlobMid, sizeof(aBlobMid));
  checkCorruptAsc("asc_text_single_zero", aTextSingleZero, sizeof(aTextSingleZero));
  checkCorruptAsc("asc_blob_single_zero", aBlobSingleZero, sizeof(aBlobSingleZero));
  checkCorruptAsc("asc_text_after_escape", aTextAfterEsc, sizeof(aTextAfterEsc));
  checkCorruptAsc("asc_blob_after_escape", aBlobAfterEsc, sizeof(aBlobAfterEsc));
  checkCorruptAsc("asc_text_bad_escape", aTextBadEsc, sizeof(aTextBadEsc));
  checkCorruptAsc("asc_blob_bad_escape", aBlobBadEsc, sizeof(aBlobBadEsc));
}

static void testUnterminatedNumericFastPath(void){
  u8 aNum[18];
  int nNum = 0;
  u8 aBuf[32];

  check("encode_int64_42", sortKeyFromInt64(42, aNum, &nNum)==SQLITE_OK && nNum==9);

  /* NUM + BLOB + 2 bytes (12 bytes total, hits recordFromNumericSmallBlobSortKeyBuffer) */
  memcpy(aBuf, aNum, 9);
  aBuf[9] = SORTKEY_BLOB;
  aBuf[10] = 'a';
  aBuf[11] = 'b';
  checkCorruptAsc("num_small_blob_mid_payload", aBuf, 12);

  /* NUM + BLOB + payload + single 0x00 */
  aBuf[10] = 'a';
  aBuf[11] = 0x00;
  checkCorruptAsc("num_small_blob_single_zero", aBuf, 12);

  /* NUM + BLOB + payload + escaped NUL (0x00 0x01) at EOF */
  aBuf[10] = 'a';
  aBuf[11] = 0x00;
  aBuf[12] = 0x01;
  checkCorruptAsc("num_small_blob_after_escape", aBuf, 13);

  /* NUM + BLOB + bad escape */
  aBuf[10] = 'a';
  aBuf[11] = 0x00;
  aBuf[12] = 0x02;
  aBuf[13] = 0x00;
  aBuf[14] = 0x00;
  checkCorruptAsc("num_small_blob_bad_escape", aBuf, 15);

  /* NUM + TEXT + 2 bytes (unterminated) */
  aBuf[9] = SORTKEY_TEXT;
  aBuf[10] = 'a';
  aBuf[11] = 'b';
  checkCorruptAsc("num_text_mid_payload", aBuf, 12);

  /* NUM + TEXT + escaped NUL at EOF */
  aBuf[10] = 'a';
  aBuf[11] = 0x00;
  aBuf[12] = 0x01;
  checkCorruptAsc("num_text_after_escape", aBuf, 13);
}

static void testUnterminatedDesc(void){
  u8 aFlagDesc[1] = { KEYINFO_ORDER_DESC };
  u8 aFlagDescAsc[2] = { KEYINFO_ORDER_DESC, 0 };
  u8 aFlagAscDesc[2] = { 0, KEYINFO_ORDER_DESC };
  KeyInfo *pDesc = allocKeyInfo(1, aFlagDesc);
  KeyInfo *pDescAsc = allocKeyInfo(2, aFlagDescAsc);
  KeyInfo *pAscDesc = allocKeyInfo(2, aFlagAscDesc);

  static const u8 aDescTextTagOnly[] = { (u8)~SORTKEY_TEXT };
  static const u8 aDescBlobTagOnly[] = { (u8)~SORTKEY_BLOB };
  static const u8 aDescTextMid[] = { (u8)~SORTKEY_TEXT, (u8)~'a', (u8)~'b' };
  static const u8 aDescBlobMid[] = { (u8)~SORTKEY_BLOB, (u8)~0x11, (u8)~0x22 };
  static const u8 aDescTextSingleFF[] = { (u8)~SORTKEY_TEXT, (u8)~'a', 0xFF };
  static const u8 aDescBlobSingleFF[] = { (u8)~SORTKEY_BLOB, (u8)~0x11, 0xFF };
  static const u8 aDescTextAfterEsc[] = { (u8)~SORTKEY_TEXT, (u8)~'a', 0xFF, 0xFE };
  static const u8 aDescBlobAfterEsc[] = { (u8)~SORTKEY_BLOB, (u8)~0x11, 0xFF, 0xFE };
  static const u8 aDescTextBadEsc[] = { (u8)~SORTKEY_TEXT, (u8)~'a', 0xFF, 0xFD, 0xFF, 0xFF };
  static const u8 aDescBlobBadEsc[] = { (u8)~SORTKEY_BLOB, (u8)~0x11, 0xFF, 0xFD, 0xFF, 0xFF };

  static const u8 aMixedDescNullAscTextTag[] = { (u8)~SORTKEY_NULL, SORTKEY_TEXT };
  static const u8 aMixedDescNullAscTextMid[] = { (u8)~SORTKEY_NULL, SORTKEY_TEXT, 'x', 'y' };
  static const u8 aMixedDescNullAscTextZero[] = { (u8)~SORTKEY_NULL, SORTKEY_TEXT, 'x', 0x00 };
  static const u8 aMixedDescNullAscTextEsc[] = { (u8)~SORTKEY_NULL, SORTKEY_TEXT, 'x', 0x00, 0x01 };

  static const u8 aMixedAscTextDescBlobTag[] = { SORTKEY_TEXT, 'a', 0x00, 0x00, (u8)~SORTKEY_BLOB };
  static const u8 aMixedAscTextDescBlobMid[] = { SORTKEY_TEXT, 'a', 0x00, 0x00, (u8)~SORTKEY_BLOB, (u8)~0x11 };
  static const u8 aMixedAscTextDescBlobFF[] = { SORTKEY_TEXT, 'a', 0x00, 0x00, (u8)~SORTKEY_BLOB, 0xFF };
  static const u8 aMixedAscTextDescBlobEsc[] = { SORTKEY_TEXT, 'a', 0x00, 0x00, (u8)~SORTKEY_BLOB, 0xFF, 0xFE };

  check("alloc_keyinfo", pDesc!=0 && pDescAsc!=0 && pAscDesc!=0);
  if( !pDesc || !pDescAsc || !pAscDesc ){
    sqlite3_free(pDesc);
    sqlite3_free(pDescAsc);
    sqlite3_free(pAscDesc);
    return;
  }

  checkCorruptColl("desc_text_tag_only", aDescTextTagOnly, sizeof(aDescTextTagOnly), pDesc);
  checkCorruptColl("desc_blob_tag_only", aDescBlobTagOnly, sizeof(aDescBlobTagOnly), pDesc);
  checkCorruptColl("desc_text_mid_payload", aDescTextMid, sizeof(aDescTextMid), pDesc);
  checkCorruptColl("desc_blob_mid_payload", aDescBlobMid, sizeof(aDescBlobMid), pDesc);
  checkCorruptColl("desc_text_single_ff", aDescTextSingleFF, sizeof(aDescTextSingleFF), pDesc);
  checkCorruptColl("desc_blob_single_ff", aDescBlobSingleFF, sizeof(aDescBlobSingleFF), pDesc);
  checkCorruptColl("desc_text_after_escape", aDescTextAfterEsc, sizeof(aDescTextAfterEsc), pDesc);
  checkCorruptColl("desc_blob_after_escape", aDescBlobAfterEsc, sizeof(aDescBlobAfterEsc), pDesc);
  checkCorruptColl("desc_text_bad_escape", aDescTextBadEsc, sizeof(aDescTextBadEsc), pDesc);
  checkCorruptColl("desc_blob_bad_escape", aDescBlobBadEsc, sizeof(aDescBlobBadEsc), pDesc);

  checkCorruptColl("mixed_desc_null_asc_text_tag", aMixedDescNullAscTextTag, sizeof(aMixedDescNullAscTextTag), pDescAsc);
  checkCorruptColl("mixed_desc_null_asc_text_mid", aMixedDescNullAscTextMid, sizeof(aMixedDescNullAscTextMid), pDescAsc);
  checkCorruptColl("mixed_desc_null_asc_text_zero", aMixedDescNullAscTextZero, sizeof(aMixedDescNullAscTextZero), pDescAsc);
  checkCorruptColl("mixed_desc_null_asc_text_esc", aMixedDescNullAscTextEsc, sizeof(aMixedDescNullAscTextEsc), pDescAsc);

  checkCorruptColl("mixed_asc_text_desc_blob_tag", aMixedAscTextDescBlobTag, sizeof(aMixedAscTextDescBlobTag), pAscDesc);
  checkCorruptColl("mixed_asc_text_desc_blob_mid", aMixedAscTextDescBlobMid, sizeof(aMixedAscTextDescBlobMid), pAscDesc);
  checkCorruptColl("mixed_asc_text_desc_blob_ff", aMixedAscTextDescBlobFF, sizeof(aMixedAscTextDescBlobFF), pAscDesc);
  checkCorruptColl("mixed_asc_text_desc_blob_esc", aMixedAscTextDescBlobEsc, sizeof(aMixedAscTextDescBlobEsc), pAscDesc);

  sqlite3_free(pDesc);
  sqlite3_free(pDescAsc);
  sqlite3_free(pAscDesc);
}

static void testValidRoundTrips(void){
  u8 aFlagDesc[1] = { KEYINFO_ORDER_DESC };
  u8 aFlagDescAsc[2] = { KEYINFO_ORDER_DESC, 0 };
  u8 aFlagAscDesc[2] = { 0, KEYINFO_ORDER_DESC };
  KeyInfo *pDesc = allocKeyInfo(1, aFlagDesc);
  KeyInfo *pDescAsc = allocKeyInfo(2, aFlagDescAsc);
  KeyInfo *pAscDesc = allocKeyInfo(2, aFlagAscDesc);

  /* Single-field records: [hdrSize=2, serialType, payload...] */
  static const u8 rEmptyText[] = { 2, 13 };
  static const u8 rEmptyBlob[] = { 2, 12 };
  static const u8 rTextHello[] = { 2, 23, 'h', 'e', 'l', 'l', 'o' };
  static const u8 rBlobPlain[] = { 2, 18, 0x01, 0x02, 0x03 };
  static const u8 rTextNul[] = { 2, 19, 'a', 0x00, 'b' };
  static const u8 rTextOnlyNuls[] = { 2, 17, 0x00, 0x00 };
  static const u8 rBlobNul[] = { 2, 18, 0x00, 0x42, 0x00 };

  /* 2-field records: (INT 42, BLOB/TEXT) exercising small-blob and varlen fast paths */
  static const u8 rNumEmptyBlob[] = { 3, 1, 12, 42 };
  static const u8 rNumSmallBlob[] = { 3, 1, 16, 42, 'h', 'i' };
  static const u8 rNumSmallBlobNul[] = { 3, 1, 18, 42, 'a', 0x00, 'b' };
  static const u8 rNumEmptyText[] = { 3, 1, 13, 42 };
  static const u8 rNumText[] = { 3, 1, 23, 42, 'h', 'e', 'l', 'l', 'o' };
  static const u8 rNumTextNul[] = { 3, 1, 19, 42, 'a', 0x00, 'b' };

  /* 2-field mixed: (TEXT "a\0b", BLOB "\0x\0") */
  static const u8 rTextBlobMixed[] = { 3, 19, 18, 'a', 0x00, 'b', 0x00, 'x', 0x00 };

  check("alloc_keyinfo_rt", pDesc!=0 && pDescAsc!=0 && pAscDesc!=0);
  if( !pDesc || !pDescAsc || !pAscDesc ){
    sqlite3_free(pDesc);
    sqlite3_free(pDescAsc);
    sqlite3_free(pAscDesc);
    return;
  }

  checkRoundTrip("rt_asc_empty_text", rEmptyText, sizeof(rEmptyText), 0);
  checkRoundTrip("rt_asc_empty_blob", rEmptyBlob, sizeof(rEmptyBlob), 0);
  checkRoundTrip("rt_asc_text_hello", rTextHello, sizeof(rTextHello), 0);
  checkRoundTrip("rt_asc_blob_plain", rBlobPlain, sizeof(rBlobPlain), 0);
  checkRoundTrip("rt_asc_text_nul", rTextNul, sizeof(rTextNul), 0);
  checkRoundTrip("rt_asc_text_only_nuls", rTextOnlyNuls, sizeof(rTextOnlyNuls), 0);
  checkRoundTrip("rt_asc_blob_nul", rBlobNul, sizeof(rBlobNul), 0);

  checkRoundTrip("rt_num_empty_blob", rNumEmptyBlob, sizeof(rNumEmptyBlob), 0);
  checkRoundTrip("rt_num_small_blob", rNumSmallBlob, sizeof(rNumSmallBlob), 0);
  checkRoundTrip("rt_num_small_blob_nul", rNumSmallBlobNul, sizeof(rNumSmallBlobNul), 0);
  checkRoundTrip("rt_num_empty_text", rNumEmptyText, sizeof(rNumEmptyText), 0);
  checkRoundTrip("rt_num_text", rNumText, sizeof(rNumText), 0);
  checkRoundTrip("rt_num_text_nul", rNumTextNul, sizeof(rNumTextNul), 0);

  checkRoundTrip("rt_desc_empty_text", rEmptyText, sizeof(rEmptyText), pDesc);
  checkRoundTrip("rt_desc_empty_blob", rEmptyBlob, sizeof(rEmptyBlob), pDesc);
  checkRoundTrip("rt_desc_text_hello", rTextHello, sizeof(rTextHello), pDesc);
  checkRoundTrip("rt_desc_blob_plain", rBlobPlain, sizeof(rBlobPlain), pDesc);
  checkRoundTrip("rt_desc_text_nul", rTextNul, sizeof(rTextNul), pDesc);
  checkRoundTrip("rt_desc_text_only_nuls", rTextOnlyNuls, sizeof(rTextOnlyNuls), pDesc);
  checkRoundTrip("rt_desc_blob_nul", rBlobNul, sizeof(rBlobNul), pDesc);

  checkRoundTrip("rt_mixed_desc_num_asc_text", rNumTextNul, sizeof(rNumTextNul), pDescAsc);
  checkRoundTrip("rt_mixed_asc_text_desc_blob", rTextBlobMixed, sizeof(rTextBlobMixed), pAscDesc);

  sqlite3_free(pDesc);
  sqlite3_free(pDescAsc);
  sqlite3_free(pAscDesc);
}

int main(void){
  testUnterminatedAsc();
  testUnterminatedNumericFastPath();
  testUnterminatedDesc();
  testValidRoundTrips();
  printf("sortkey_test: %d passed, %d failed\n", nPass, nFail);
  return nFail ? 1 : 0;
}
