#ifndef DOLTLITE_PARSE_H
#define DOLTLITE_PARSE_H

#include <stdint.h>

#define DOLTLITE_DECIMAL_OK       0
#define DOLTLITE_DECIMAL_INVALID  1
#define DOLTLITE_DECIMAL_RANGE    2

/*
** Parse the non-empty decimal span [zBegin,zEnd) without accepting signs,
** whitespace, or trailing characters. Return DOLTLITE_DECIMAL_RANGE when
** the value exceeds mxValue, before performing overflowing arithmetic.
*/
static int doltliteParseDecimal(
  const char *zBegin,
  const char *zEnd,
  uint64_t mxValue,
  uint64_t *pValue
){
  const char *z = zBegin;
  uint64_t value = 0;

  if( !z || !zEnd || z>=zEnd || !pValue ){
    return DOLTLITE_DECIMAL_INVALID;
  }
  while( z<zEnd ){
    unsigned int digit;
    if( *z<'0' || *z>'9' ) return DOLTLITE_DECIMAL_INVALID;
    digit = (unsigned int)(*z - '0');
    if( digit>mxValue || value>(mxValue-digit)/10 ){
      return DOLTLITE_DECIMAL_RANGE;
    }
    value = value*10 + digit;
    z++;
  }
  *pValue = value;
  return DOLTLITE_DECIMAL_OK;
}

#endif
