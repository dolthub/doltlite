/******************************************************************************
** The following is an amalgamation of these source code files:
**
**     ext/qrf/qrf.h
**     ext/qrf/qrf.c
**     src/tclsqlite.c
**
******************************************************************************/
/************** Begin file qrf.h ********************************************/
/*
** 2025-10-20
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Header file for the Result-Format or "resfmt" utility library for SQLite.
** See the resfmt.md documentation for additional information.
*/
#ifndef SQLITE_QRF_H
#define SQLITE_QRF_H
#ifdef __cplusplus
extern "C" {
#endif
#include <stdlib.h>
#include "sqlite3.h"

/*
** Specification used by clients to define the output format they want
*/
typedef struct sqlite3_qrf_spec sqlite3_qrf_spec;
struct sqlite3_qrf_spec {
  unsigned char iVersion;     /* Version number of this structure */
  unsigned char eStyle;       /* Formatting style.  "box", "csv", etc... */
  unsigned char eEsc;         /* How to escape control characters in text */
  unsigned char eText;        /* Quoting style for text */
  unsigned char eTitle;       /* Quating style for the text of column names */
  unsigned char eBlob;        /* Quoting style for BLOBs */
  unsigned char bTitles;      /* True to show column names */
  unsigned char bWordWrap;    /* Try to wrap on word boundaries */
  unsigned char bTextJsonb;   /* Render JSONB blobs as JSON text */
  unsigned char eDfltAlign;   /* Default alignment, no covered by aAlignment */
  unsigned char eTitleAlign;  /* Alignment for column headers */
  unsigned char bSplitColumn; /* Wrap single-column output into many columns */
  unsigned char bBorder;      /* Show outer border in Box and Table styles */
  short int nWrap;            /* Wrap columns wider than this */
  short int nScreenWidth;     /* Maximum overall table width */
  short int nLineLimit;       /* Maximum number of lines for any row */
  short int nTitleLimit;      /* Maximum number of characters in a title */
  int nCharLimit;             /* Maximum number of characters in a cell */
  int nWidth;                 /* Number of entries in aWidth[] */
  int nAlign;                 /* Number of entries in aAlignment[] */
  short int *aWidth;          /* Column widths */
  unsigned char *aAlign;      /* Column alignments */
  char *zColumnSep;           /* Alternative column separator */
  char *zRowSep;              /* Alternative row separator */
  char *zTableName;           /* Output table name */
  char *zNull;                /* Rendering of NULL */
  char *(*xRender)(void*,sqlite3_value*);           /* Render a value */
  int (*xWrite)(void*,const char*,sqlite3_int64);   /* Write output */
  void *pRenderArg;           /* First argument to the xRender callback */
  void *pWriteArg;            /* First argument to the xWrite callback */
  char **pzOutput;            /* Storage location for output string */
  /* Fields below are only available if iVersion>=2 */
  unsigned int nMultiInsert;  /* Add rows to one INSERT until size exceeds */
  /* Additional fields may be added in the future */
};

/*
** Interfaces
*/
int sqlite3_format_query_result(
  sqlite3_stmt *pStmt,             /* SQL statement to run */
  const sqlite3_qrf_spec *pSpec,   /* Result format specification */
  char **pzErr                     /* OUT: Write error message here */
);

/*
** Range of values for sqlite3_qrf_spec.aWidth[] entries and for
** sqlite3_qrf_spec.mxColWidth and .nScreenWidth
*/
#define QRF_MAX_WIDTH    10000
#define QRF_MIN_WIDTH    0

/*
** Output styles:
*/
#define QRF_STYLE_Auto      0 /* Choose a style automatically */
#define QRF_STYLE_Box       1 /* Unicode box-drawing characters */
#define QRF_STYLE_Column    2 /* One record per line in neat columns */
#define QRF_STYLE_Count     3 /* Output only a count of the rows of output */
#define QRF_STYLE_Csv       4 /* Comma-separated-value */
#define QRF_STYLE_Eqp       5 /* Format EXPLAIN QUERY PLAN output */
#define QRF_STYLE_Explain   6 /* EXPLAIN output */
#define QRF_STYLE_Html      7 /* Generate an XHTML table */
#define QRF_STYLE_Insert    8 /* Generate SQL "insert" statements */
#define QRF_STYLE_Json      9 /* Output is a list of JSON objects */
#define QRF_STYLE_JObject  10 /* Independent JSON objects for each row */
#define QRF_STYLE_Line     11 /* One column per line. */
#define QRF_STYLE_List     12 /* One record per line with a separator */
#define QRF_STYLE_Markdown 13 /* Markdown formatting */
#define QRF_STYLE_Off      14 /* No query output shown */
#define QRF_STYLE_Quote    15 /* SQL-quoted, comma-separated */
#define QRF_STYLE_Stats    16 /* EQP-like output but with performance stats */
#define QRF_STYLE_StatsEst 17 /* EQP-like output with planner estimates */
#define QRF_STYLE_StatsVm  18 /* EXPLAIN-like output with performance stats */
#define QRF_STYLE_Table    19 /* MySQL-style table formatting */

/*
** Quoting styles for text.
** Allowed values for sqlite3_qrf_spec.eText
*/
#define QRF_TEXT_Auto    0 /* Choose text encoding automatically */
#define QRF_TEXT_Plain   1 /* Literal text */
#define QRF_TEXT_Sql     2 /* Quote as an SQL literal */
#define QRF_TEXT_Csv     3 /* CSV-style quoting */
#define QRF_TEXT_Html    4 /* HTML-style quoting */
#define QRF_TEXT_Tcl     5 /* C/Tcl quoting */
#define QRF_TEXT_Json    6 /* JSON quoting */
#define QRF_TEXT_Relaxed 7 /* Relaxed SQL quoting */

/*
** Quoting styles for BLOBs
** Allowed values for sqlite3_qrf_spec.eBlob
*/
#define QRF_BLOB_Auto    0 /* Determine BLOB quoting using eText */
#define QRF_BLOB_Text    1 /* Display content exactly as it is */
#define QRF_BLOB_Sql     2 /* Quote as an SQL literal */
#define QRF_BLOB_Hex     3 /* Hexadecimal representation */
#define QRF_BLOB_Tcl     4 /* "\000" notation */
#define QRF_BLOB_Json    5 /* A JSON string */
#define QRF_BLOB_Size    6 /* Display the blob size only */

/*
** Control-character escape modes.
** Allowed values for sqlite3_qrf_spec.eEsc
*/
#define QRF_ESC_Auto    0 /* Choose the ctrl-char escape automatically */
#define QRF_ESC_Off     1 /* Do not escape control characters */
#define QRF_ESC_Ascii   2 /* Unix-style escapes.  Ex: U+0007 shows ^G */
#define QRF_ESC_Symbol  3 /* Unicode escapes. Ex: U+0007 shows U+2407 */

/*
** Allowed values for "boolean" fields, such as "bColumnNames", "bWordWrap",
** and "bTextJsonb".  There is an extra "auto" variants so these are actually
** tri-state settings, not booleans.
*/
#define QRF_SW_Auto     0 /* Let QRF choose the best value */
#define QRF_SW_Off      1 /* This setting is forced off */
#define QRF_SW_On       2 /* This setting is forced on */
#define QRF_Auto        0 /* Alternate spelling for QRF_*_Auto */
#define QRF_No          1 /* Alternate spelling for QRF_SW_Off */
#define QRF_Yes         2 /* Alternate spelling for QRF_SW_On */

/*
** Possible alignment values alignment settings
**
**                             Horizontal   Vertial
**                             ----------   --------  */
#define QRF_ALIGN_Auto    0 /*   auto        auto     */
#define QRF_ALIGN_Left    1 /*   left        auto     */
#define QRF_ALIGN_Center  2 /*   center      auto     */
#define QRF_ALIGN_Right   3 /*   right       auto     */
#define QRF_ALIGN_Top     4 /*   auto        top      */
#define QRF_ALIGN_NW      5 /*   left        top      */
#define QRF_ALIGN_N       6 /*   center      top      */
#define QRF_ALIGN_NE      7 /*   right       top      */
#define QRF_ALIGN_Middle  8 /*   auto        middle   */
#define QRF_ALIGN_W       9 /*   left        middle   */
#define QRF_ALIGN_C      10 /*   center      middle   */
#define QRF_ALIGN_E      11 /*   right       middle   */
#define QRF_ALIGN_Bottom 12 /*   auto        bottom   */
#define QRF_ALIGN_SW     13 /*   left        bottom   */
#define QRF_ALIGN_S      14 /*   center      bottom   */
#define QRF_ALIGN_SE     15 /*   right       bottom   */
#define QRF_ALIGN_HMASK   3 /* Horizontal alignment mask */
#define QRF_ALIGN_VMASK  12 /* Vertical alignment mask */

/*
** Auxiliary routines contined within this module that might be useful
** in other contexts, and which are therefore exported.
*/
/*
** Return an estimate of the width, in columns, for the single Unicode
** character c.  For normal characters, the answer is always 1.  But the
** estimate might be 0 or 2 for zero-width and double-width characters.
**
** Different devices display unicode using different widths.  So
** it is impossible to know that true display width with 100% accuracy.
** Inaccuracies in the width estimates might cause columns to be misaligned.
** Unfortunately, there is nothing we can do about that.
*/
int sqlite3_qrf_wcwidth(int c);

/*
** Return an estimate of the number of display columns used by the
** string in the argument.  The width of individual characters is
** determined as for sqlite3_qrf_wcwidth().  VT100 escape code sequences
** are assigned a width of zero.
*/
size_t sqlite3_qrf_wcswidth(const char*);


#ifdef __cplusplus
}
#endif
#endif /* !defined(SQLITE_QRF_H) */

/************** End of qrf.h ************************************************/
/************** Begin file qrf.c ********************************************/
/*
** 2025-10-20
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Implementation of the Result-Format or "qrf" utility library for SQLite.
** See the qrf.md documentation for additional information.
*/
#ifndef SQLITE_QRF_H
#include "qrf.h"
#endif
#include <string.h>
#include <assert.h>
#include <stdint.h>

typedef sqlite3_int64 i64;

/* A single line in the EQP output */
typedef struct qrfEQPGraphRow qrfEQPGraphRow;
struct qrfEQPGraphRow {
  int iEqpId;            /* ID for this row */
  int iParentId;         /* ID of the parent row */
  qrfEQPGraphRow *pNext; /* Next row in sequence */
  char zText[1];         /* Text to display for this row */
};

/* All EQP output is collected into an instance of the following */
typedef struct qrfEQPGraph qrfEQPGraph;
struct qrfEQPGraph {
  qrfEQPGraphRow *pRow;  /* Linked list of all rows of the EQP output */
  qrfEQPGraphRow *pLast; /* Last element of the pRow list */
  int nWidth;            /* Width of the graph */
  char zPrefix[400];     /* Graph prefix */
};

/*
** Private state information.  Subject to change from one release to the
** next.
*/
typedef struct Qrf Qrf;
struct Qrf {
  sqlite3_stmt *pStmt;        /* The statement whose output is to be rendered */
  sqlite3 *db;                /* The corresponding database connection */
  sqlite3_stmt *pJTrans;      /* JSONB to JSON translator statement */
  char **pzErr;               /* Write error message here, if not NULL */
  sqlite3_str *pOut;          /* Accumulated output */
  int iErr;                   /* Error code */
  int nCol;                   /* Number of output columns */
  int expMode;                /* Original sqlite3_stmt_isexplain() plus 1 */
  int mxWidth;                /* Screen width */
  int mxHeight;               /* nLineLimit */
  union {
    struct {                  /* Content for QRF_STYLE_Line */
      int mxColWth;             /* Maximum display width of any column */
      char **azCol;             /* Names of output columns (MODE_Line) */
    } sLine;
    qrfEQPGraph *pGraph;      /* EQP graph (Eqp, Stats, and StatsEst) */
    struct {                  /* Content for QRF_STYLE_Explain */
      int nIndent;              /* Slots allocated for aiIndent */
      int iIndent;              /* Current slot */
      int *aiIndent;            /* Indentation for each opcode */
    } sExpln;
    unsigned int nIns;        /* Bytes used for current INSERT stmt */
  } u;
  sqlite3_int64 nRow;         /* Number of rows handled so far */
  int *actualWidth;           /* Actual width of each column */
  sqlite3_qrf_spec spec;      /* Copy of the original spec */
};

/*
** Data for substitute ctype.h functions.  Used for x-platform
** consistency and so that '_' is counted as an alphabetic
** character.
**
**    0x01 -  space
**    0x02 -  digit
**    0x04 -  alphabetic, including '_'
*/
static const char qrfCType[] = {
  0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0,
  0, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
  4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0, 0, 0, 4,
  0, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
  4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};
#define qrfSpace(x) ((qrfCType[(unsigned char)x]&1)!=0)
#define qrfDigit(x) ((qrfCType[(unsigned char)x]&2)!=0)
#define qrfAlpha(x) ((qrfCType[(unsigned char)x]&4)!=0)
#define qrfAlnum(x) ((qrfCType[(unsigned char)x]&6)!=0)

#ifndef deliberate_fall_through
/* Quiet some compilers about some of our intentional code. */
# if defined(GCC_VERSION) && GCC_VERSION>=7000000
#  define deliberate_fall_through __attribute__((fallthrough));
# else
#  define deliberate_fall_through
# endif
#endif

/*
** Set an error code and error message.
*/
static void qrfError(
  Qrf *p,                /* Query result state */
  int iCode,             /* Error code */
  const char *zFormat,   /* Message format (or NULL) */
  ...
){
  p->iErr = iCode;
  if( p->pzErr!=0 ){
    sqlite3_free(*p->pzErr);
    *p->pzErr = 0;
    if( zFormat ){
      va_list ap;
      va_start(ap, zFormat);
      *p->pzErr = sqlite3_vmprintf(zFormat, ap);
      va_end(ap);
    }
  }
}

/*
** Out-of-memory error.
*/
static void qrfOom(Qrf *p){
  qrfError(p, SQLITE_NOMEM, "out of memory");
}

/*
** Transfer any error in pStr over into p.
*/
static void qrfStrErr(Qrf *p, sqlite3_str *pStr){
  int rc = pStr ? sqlite3_str_errcode(pStr) : 0;
  if( rc ){
    qrfError(p, rc, sqlite3_errstr(rc));
  }
}


/*
** Add a new entry to the EXPLAIN QUERY PLAN data
*/
static void qrfEqpAppend(Qrf *p, int iEqpId, int p2, const char *zText){
  qrfEQPGraphRow *pNew;
  sqlite3_int64 nText;
  if( zText==0 ) return;
  if( p->u.pGraph==0 ){
    p->u.pGraph = sqlite3_malloc64( sizeof(qrfEQPGraph) );
    if( p->u.pGraph==0 ){
      qrfOom(p);
      return;
    }
    memset(p->u.pGraph, 0, sizeof(qrfEQPGraph) );
  }
  nText = strlen(zText);
  pNew = sqlite3_malloc64( sizeof(*pNew) + nText );
  if( pNew==0 ){
    qrfOom(p);
    return;
  }
  pNew->iEqpId = iEqpId;
  pNew->iParentId = p2;
  memcpy(pNew->zText, zText, nText+1);
  pNew->pNext = 0;
  if( p->u.pGraph->pLast ){
    p->u.pGraph->pLast->pNext = pNew;
  }else{
    p->u.pGraph->pRow = pNew;
  }
  p->u.pGraph->pLast = pNew;
}

/*
** Free and reset the EXPLAIN QUERY PLAN data that has been collected
** in p->u.pGraph.
*/
static void qrfEqpReset(Qrf *p){
  qrfEQPGraphRow *pRow, *pNext;
  if( p->u.pGraph ){
    for(pRow = p->u.pGraph->pRow; pRow; pRow = pNext){
      pNext = pRow->pNext;
      sqlite3_free(pRow);
    }
    sqlite3_free(p->u.pGraph);
    p->u.pGraph = 0;
  }
}

/* Return the next EXPLAIN QUERY PLAN line with iEqpId that occurs after
** pOld, or return the first such line if pOld is NULL
*/
static qrfEQPGraphRow *qrfEqpNextRow(Qrf *p, int iEqpId, qrfEQPGraphRow *pOld){
  qrfEQPGraphRow *pRow = pOld ? pOld->pNext : p->u.pGraph->pRow;
  while( pRow && pRow->iParentId!=iEqpId ) pRow = pRow->pNext;
  return pRow;
}

/* Render a single level of the graph that has iEqpId as its parent.  Called
** recursively to render sublevels.
*/
static void qrfEqpRenderLevel(Qrf *p, int iEqpId){
  qrfEQPGraphRow *pRow, *pNext;
  i64 n = strlen(p->u.pGraph->zPrefix);
  char *z;
  for(pRow = qrfEqpNextRow(p, iEqpId, 0); pRow; pRow = pNext){
    pNext = qrfEqpNextRow(p, iEqpId, pRow);
    z = pRow->zText;
    sqlite3_str_appendf(p->pOut, "%s%s%s\n", p->u.pGraph->zPrefix,
                            pNext ? "|--" : "`--", z);
    if( n<(i64)sizeof(p->u.pGraph->zPrefix)-7 ){
      memcpy(&p->u.pGraph->zPrefix[n], pNext ? "|  " : "   ", 4);
      qrfEqpRenderLevel(p, pRow->iEqpId);
      p->u.pGraph->zPrefix[n] = 0;
    }
  }
}

/*
** Render the 64-bit value N in a more human-readable format into
** pOut.
**
**   +  Only show the first three significant digits.
**   +  Append suffixes K, M, G, T, P, and E for 1e3, 1e6, ... 1e18
*/
static void qrfApproxInt64(sqlite3_str *pOut, i64 N){
  static const char aSuffix[] = { 'K', 'M', 'G', 'T', 'P', 'E' };
  int i;
  if( N<0 ){
    N = N==INT64_MIN ? INT64_MAX : -N;
    sqlite3_str_append(pOut, "-", 1);
  }
  if( N<10000 ){
    sqlite3_str_appendf(pOut, "%4lld ", N);
    return;
  }
  for(i=1; i<=18; i++){
    N = (N+5)/10;
    if( N<10000 ){
      int n = (int)N;
      switch( i%3 ){
        case 0:
          sqlite3_str_appendf(pOut, "%d.%02d", n/1000, (n%1000)/10);
          break;
        case 1:
          sqlite3_str_appendf(pOut, "%2d.%d", n/100, (n%100)/10);
          break;
        case 2:
          sqlite3_str_appendf(pOut, "%4d", n/10);
          break;
      }
      sqlite3_str_append(pOut, &aSuffix[i/3], 1);
      break;
    }
  }
}

/*
** Display and reset the EXPLAIN QUERY PLAN data
*/
static void qrfEqpRender(Qrf *p, i64 nCycle){
  qrfEQPGraphRow *pRow;
  if( p->u.pGraph!=0 && (pRow = p->u.pGraph->pRow)!=0 ){
    if( pRow->zText[0]=='-' ){
      if( pRow->pNext==0 ){
        qrfEqpReset(p);
        return;
      }
      sqlite3_str_appendf(p->pOut, "%s\n", pRow->zText+3);
      p->u.pGraph->pRow = pRow->pNext;
      sqlite3_free(pRow);
    }else if( nCycle>0 ){
      int nSp = p->u.pGraph->nWidth - 2;
      if( p->spec.eStyle==QRF_STYLE_StatsEst ){
        sqlite3_str_appendchar(p->pOut, nSp, ' ');
        sqlite3_str_appendall(p->pOut,
                                "Cycles      Loops  (est)  Rows   (est)\n");
        sqlite3_str_appendchar(p->pOut, nSp, ' ');
        sqlite3_str_appendall(p->pOut,
                                "----------  ------------  ------------\n");
      }else{
        sqlite3_str_appendchar(p->pOut, nSp, ' ');
        sqlite3_str_appendall(p->pOut,
                                "Cycles      Loops  Rows \n");
        sqlite3_str_appendchar(p->pOut, nSp, ' ');
        sqlite3_str_appendall(p->pOut,
                                "----------  -----  -----\n");
      }
      sqlite3_str_appendall(p->pOut, "QUERY PLAN");
      sqlite3_str_appendchar(p->pOut, nSp - 10, ' ');
      qrfApproxInt64(p->pOut, nCycle);
      sqlite3_str_appendall(p->pOut, " 100%\n");
    }else{
      sqlite3_str_appendall(p->pOut, "QUERY PLAN\n");
    }
    p->u.pGraph->zPrefix[0] = 0;
    qrfEqpRenderLevel(p, 0);
    qrfEqpReset(p);
  }
}

#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
/*
** Helper function for qrfExpStats().
**
*/
static int qrfStatsHeight(sqlite3_stmt *p, int iEntry){
  int iPid = 0;
  int ret = 1;
  sqlite3_stmt_scanstatus_v2(p, iEntry,
      SQLITE_SCANSTAT_SELECTID, SQLITE_SCANSTAT_COMPLEX, (void*)&iPid
  );
  while( iPid!=0 ){
    int ii;
    for(ii=0; 1; ii++){
      int iId;
      int res;
      res = sqlite3_stmt_scanstatus_v2(p, ii,
          SQLITE_SCANSTAT_SELECTID, SQLITE_SCANSTAT_COMPLEX, (void*)&iId
      );
      if( res ) break;
      if( iId==iPid ){
        sqlite3_stmt_scanstatus_v2(p, ii,
            SQLITE_SCANSTAT_PARENTID, SQLITE_SCANSTAT_COMPLEX, (void*)&iPid
        );
      }
    }
    ret++;
  }
  return ret;
}
#endif /* SQLITE_ENABLE_STMT_SCANSTATUS */


/*
** Generate ".scanstatus est" style of EQP output.
*/
static void qrfEqpStats(Qrf *p){
#ifndef SQLITE_ENABLE_STMT_SCANSTATUS
  qrfError(p, SQLITE_ERROR, "not available in this build");
#else
  static const int f = SQLITE_SCANSTAT_COMPLEX;
  sqlite3_stmt *pS = p->pStmt;
  int i = 0;
  i64 nTotal = 0;
  int nWidth = 0;
  int prevPid = -1;             /* Previous iPid */
  double rEstCum = 1.0;         /* Cumulative row estimate */
  sqlite3_str *pLine = sqlite3_str_new(p->db);
  sqlite3_str *pStats = sqlite3_str_new(p->db);
  qrfEqpReset(p);

  for(i=0; 1; i++){
    const char *z = 0;
    int n = 0;
    if( sqlite3_stmt_scanstatus_v2(pS,i,SQLITE_SCANSTAT_EXPLAIN,f,(void*)&z) ){
      break;
    }
    n = (int)strlen(z) + qrfStatsHeight(pS,i)*3;
    if( n>nWidth ) nWidth = n;
  }
  nWidth += 2;

  sqlite3_stmt_scanstatus_v2(pS,-1, SQLITE_SCANSTAT_NCYCLE, f, (void*)&nTotal);
  for(i=0; 1; i++){
    i64 nLoop = 0;
    i64 nRow = 0;
    i64 nCycle = 0;
    int iId = 0;
    int iPid = 0;
    const char *zo = 0;
    const char *zName = 0;
    double rEst = 0.0;

    if( sqlite3_stmt_scanstatus_v2(pS,i,SQLITE_SCANSTAT_EXPLAIN,f,(void*)&zo) ){
      break;
    }
    sqlite3_stmt_scanstatus_v2(pS,i, SQLITE_SCANSTAT_PARENTID,f,(void*)&iPid);
    if( iPid!=prevPid ){
      prevPid = iPid;
      rEstCum = 1.0;
    }
    sqlite3_stmt_scanstatus_v2(pS,i, SQLITE_SCANSTAT_EST,f,(void*)&rEst);
    rEstCum *= rEst;
    sqlite3_stmt_scanstatus_v2(pS,i, SQLITE_SCANSTAT_NLOOP,f,(void*)&nLoop);
    sqlite3_stmt_scanstatus_v2(pS,i, SQLITE_SCANSTAT_NVISIT,f,(void*)&nRow);
    sqlite3_stmt_scanstatus_v2(pS,i, SQLITE_SCANSTAT_NCYCLE,f,(void*)&nCycle);
    sqlite3_stmt_scanstatus_v2(pS,i, SQLITE_SCANSTAT_SELECTID,f,(void*)&iId);
    sqlite3_stmt_scanstatus_v2(pS,i, SQLITE_SCANSTAT_NAME,f,(void*)&zName);

    if( nCycle>=0 || nLoop>=0 || nRow>=0 ){
      int nSp = 0;
      sqlite3_str_reset(pStats);
      if( nCycle>=0 && nTotal>0 ){
        qrfApproxInt64(pStats, nCycle);
        sqlite3_str_appendf(pStats, " %3d%%",
            ((nCycle*100)+nTotal/2) / nTotal
        );
        nSp = 2;
      }
      if( nLoop>=0 ){
        if( nSp ) sqlite3_str_appendchar(pStats, nSp, ' ');
        qrfApproxInt64(pStats, nLoop);
        nSp = 2;
        if( p->spec.eStyle==QRF_STYLE_StatsEst ){
          sqlite3_str_appendf(pStats, "  ");
          qrfApproxInt64(pStats, (i64)(rEstCum/rEst));
        }
      }
      if( nRow>=0 ){
        if( nSp ) sqlite3_str_appendchar(pStats, nSp, ' ');
        qrfApproxInt64(pStats, nRow);
        nSp = 2;
        if( p->spec.eStyle==QRF_STYLE_StatsEst ){
          sqlite3_str_appendf(pStats, "  ");
          qrfApproxInt64(pStats, (i64)rEstCum);
        }
      }
      sqlite3_str_appendf(pLine,
          "% *s %s", -1*(nWidth-qrfStatsHeight(pS,i)*3), zo,
          sqlite3_str_value(pStats)
      );
      sqlite3_str_reset(pStats);
      qrfEqpAppend(p, iId, iPid, sqlite3_str_value(pLine));
      sqlite3_str_reset(pLine);
    }else{
      qrfEqpAppend(p, iId, iPid, zo);
    }
  }
  if( p->u.pGraph ) p->u.pGraph->nWidth = nWidth;
  qrfStrErr(p, pLine);
  sqlite3_free(sqlite3_str_finish(pLine));
  qrfStrErr(p, pStats);
  sqlite3_free(sqlite3_str_finish(pStats));
#endif
}


/*
** Reset the prepared statement.
*/
static void qrfResetStmt(Qrf *p){
  int rc = sqlite3_reset(p->pStmt);
  if( rc!=SQLITE_OK && p->iErr==SQLITE_OK ){
    qrfError(p, rc, "%s", sqlite3_errmsg(p->db));
  }
}

/*
** If xWrite is defined, send all content of pOut to xWrite and
** reset pOut.
*/
static void qrfWrite(Qrf *p){
  int n;
  if( p->spec.xWrite && (n = sqlite3_str_length(p->pOut))>0 ){
    int rc = p->spec.xWrite(p->spec.pWriteArg,
                 sqlite3_str_value(p->pOut),
                 (sqlite3_int64)n);
    sqlite3_str_reset(p->pOut);
    if( rc ){
      qrfError(p, rc, "Failed to write %d bytes of output", n);
    }
  }
}

/* Lookup table to estimate the number of columns consumed by a Unicode
** character.
*/
static const struct {
  unsigned char w;    /* Width of the character in columns */
  int iFirst;         /* First character in a span having this width */
} aQrfUWidth[] = {
   /* {1, 0x00000}, */
  {0, 0x00300},  {1, 0x00370},  {0, 0x00483},  {1, 0x00487},  {0, 0x00488},
  {1, 0x0048a},  {0, 0x00591},  {1, 0x005be},  {0, 0x005bf},  {1, 0x005c0},
  {0, 0x005c1},  {1, 0x005c3},  {0, 0x005c4},  {1, 0x005c6},  {0, 0x005c7},
  {1, 0x005c8},  {0, 0x00600},  {1, 0x00604},  {0, 0x00610},  {1, 0x00616},
  {0, 0x0064b},  {1, 0x0065f},  {0, 0x00670},  {1, 0x00671},  {0, 0x006d6},
  {1, 0x006e5},  {0, 0x006e7},  {1, 0x006e9},  {0, 0x006ea},  {1, 0x006ee},
  {0, 0x0070f},  {1, 0x00710},  {0, 0x00711},  {1, 0x00712},  {0, 0x00730},
  {1, 0x0074b},  {0, 0x007a6},  {1, 0x007b1},  {0, 0x007eb},  {1, 0x007f4},
  {0, 0x00901},  {1, 0x00903},  {0, 0x0093c},  {1, 0x0093d},  {0, 0x00941},
  {1, 0x00949},  {0, 0x0094d},  {1, 0x0094e},  {0, 0x00951},  {1, 0x00955},
  {0, 0x00962},  {1, 0x00964},  {0, 0x00981},  {1, 0x00982},  {0, 0x009bc},
  {1, 0x009bd},  {0, 0x009c1},  {1, 0x009c5},  {0, 0x009cd},  {1, 0x009ce},
  {0, 0x009e2},  {1, 0x009e4},  {0, 0x00a01},  {1, 0x00a03},  {0, 0x00a3c},
  {1, 0x00a3d},  {0, 0x00a41},  {1, 0x00a43},  {0, 0x00a47},  {1, 0x00a49},
  {0, 0x00a4b},  {1, 0x00a4e},  {0, 0x00a70},  {1, 0x00a72},  {0, 0x00a81},
  {1, 0x00a83},  {0, 0x00abc},  {1, 0x00abd},  {0, 0x00ac1},  {1, 0x00ac6},
  {0, 0x00ac7},  {1, 0x00ac9},  {0, 0x00acd},  {1, 0x00ace},  {0, 0x00ae2},
  {1, 0x00ae4},  {0, 0x00b01},  {1, 0x00b02},  {0, 0x00b3c},  {1, 0x00b3d},
  {0, 0x00b3f},  {1, 0x00b40},  {0, 0x00b41},  {1, 0x00b44},  {0, 0x00b4d},
  {1, 0x00b4e},  {0, 0x00b56},  {1, 0x00b57},  {0, 0x00b82},  {1, 0x00b83},
  {0, 0x00bc0},  {1, 0x00bc1},  {0, 0x00bcd},  {1, 0x00bce},  {0, 0x00c3e},
  {1, 0x00c41},  {0, 0x00c46},  {1, 0x00c49},  {0, 0x00c4a},  {1, 0x00c4e},
  {0, 0x00c55},  {1, 0x00c57},  {0, 0x00cbc},  {1, 0x00cbd},  {0, 0x00cbf},
  {1, 0x00cc0},  {0, 0x00cc6},  {1, 0x00cc7},  {0, 0x00ccc},  {1, 0x00cce},
  {0, 0x00ce2},  {1, 0x00ce4},  {0, 0x00d41},  {1, 0x00d44},  {0, 0x00d4d},
  {1, 0x00d4e},  {0, 0x00dca},  {1, 0x00dcb},  {0, 0x00dd2},  {1, 0x00dd5},
  {0, 0x00dd6},  {1, 0x00dd7},  {0, 0x00e31},  {1, 0x00e32},  {0, 0x00e34},
  {1, 0x00e3b},  {0, 0x00e47},  {1, 0x00e4f},  {0, 0x00eb1},  {1, 0x00eb2},
  {0, 0x00eb4},  {1, 0x00eba},  {0, 0x00ebb},  {1, 0x00ebd},  {0, 0x00ec8},
  {1, 0x00ece},  {0, 0x00f18},  {1, 0x00f1a},  {0, 0x00f35},  {1, 0x00f36},
  {0, 0x00f37},  {1, 0x00f38},  {0, 0x00f39},  {1, 0x00f3a},  {0, 0x00f71},
  {1, 0x00f7f},  {0, 0x00f80},  {1, 0x00f85},  {0, 0x00f86},  {1, 0x00f88},
  {0, 0x00f90},  {1, 0x00f98},  {0, 0x00f99},  {1, 0x00fbd},  {0, 0x00fc6},
  {1, 0x00fc7},  {0, 0x0102d},  {1, 0x01031},  {0, 0x01032},  {1, 0x01033},
  {0, 0x01036},  {1, 0x0103b},  {0, 0x01058},
  {1, 0x0105a},  {2, 0x01100},  {0, 0x01160},  {1, 0x01200},  {0, 0x0135f},
  {1, 0x01360},  {0, 0x01712},  {1, 0x01715},  {0, 0x01732},  {1, 0x01735},
  {0, 0x01752},  {1, 0x01754},  {0, 0x01772},  {1, 0x01774},  {0, 0x017b4},
  {1, 0x017b6},  {0, 0x017b7},  {1, 0x017be},  {0, 0x017c6},  {1, 0x017c7},
  {0, 0x017c9},  {1, 0x017d4},  {0, 0x017dd},  {1, 0x017de},  {0, 0x0180b},
  {1, 0x0180e},  {0, 0x018a9},  {1, 0x018aa},  {0, 0x01920},  {1, 0x01923},
  {0, 0x01927},  {1, 0x01929},  {0, 0x01932},  {1, 0x01933},  {0, 0x01939},
  {1, 0x0193c},  {0, 0x01a17},  {1, 0x01a19},  {0, 0x01b00},  {1, 0x01b04},
  {0, 0x01b34},  {1, 0x01b35},  {0, 0x01b36},  {1, 0x01b3b},  {0, 0x01b3c},
  {1, 0x01b3d},  {0, 0x01b42},  {1, 0x01b43},  {0, 0x01b6b},  {1, 0x01b74},
  {0, 0x01dc0},  {1, 0x01dcb},  {0, 0x01dfe},  {1, 0x01e00},  {0, 0x0200b},
  {1, 0x02010},  {0, 0x0202a},  {1, 0x0202f},  {0, 0x02060},  {1, 0x02064},
  {0, 0x0206a},  {1, 0x02070},  {0, 0x020d0},  {1, 0x020f0},  {2, 0x02329},
  {1, 0x0232b},  {2, 0x02e80},  {0, 0x0302a},  {2, 0x03030},  {1, 0x0303f},
  {2, 0x03040},  {0, 0x03099},  {2, 0x0309b},  {1, 0x0a4d0},  {0, 0x0a806},
  {1, 0x0a807},  {0, 0x0a80b},  {1, 0x0a80c},  {0, 0x0a825},  {1, 0x0a827},
  {2, 0x0ac00},  {1, 0x0d7a4},  {2, 0x0f900},  {1, 0x0fb00},  {0, 0x0fb1e},
  {1, 0x0fb1f},  {0, 0x0fe00},  {2, 0x0fe10},  {1, 0x0fe1a},  {0, 0x0fe20},
  {1, 0x0fe24},  {2, 0x0fe30},  {1, 0x0fe70},  {0, 0x0feff},  {2, 0x0ff00},
  {1, 0x0ff61},  {2, 0x0ffe0},  {1, 0x0ffe7},  {0, 0x0fff9},  {1, 0x0fffc},
  {0, 0x10a01},  {1, 0x10a04},  {0, 0x10a05},  {1, 0x10a07},  {0, 0x10a0c},
  {1, 0x10a10},  {0, 0x10a38},  {1, 0x10a3b},  {0, 0x10a3f},  {1, 0x10a40},
  {0, 0x1d167},  {1, 0x1d16a},  {0, 0x1d173},  {1, 0x1d183},  {0, 0x1d185},
  {1, 0x1d18c},  {0, 0x1d1aa},  {1, 0x1d1ae},  {0, 0x1d242},  {1, 0x1d245},
  {2, 0x20000},  {1, 0x2fffe},  {2, 0x30000},  {1, 0x3fffe},  {0, 0xe0001},
  {1, 0xe0002},  {0, 0xe0020},  {1, 0xe0080},  {0, 0xe0100},  {1, 0xe01f0}
};

/*
** Return an estimate of the width, in columns, for the single Unicode
** character c.  For normal characters, the answer is always 1.  But the
** estimate might be 0 or 2 for zero-width and double-width characters.
**
** Different display devices display unicode using different widths.  So
** it is impossible to know that true display width with 100% accuracy.
** Inaccuracies in the width estimates might cause columns to be misaligned.
** Unfortunately, there is nothing we can do about that.
*/
int sqlite3_qrf_wcwidth(int c){
  int iFirst, iLast;

  /* Fast path for common characters */
  if( c<0x300 ) return 1;

  /* The general case */
  iFirst = 0;
  iLast = sizeof(aQrfUWidth)/sizeof(aQrfUWidth[0]) - 1;
  while( iFirst<iLast-1 ){
    int iMid = (iFirst+iLast)/2;
    int cMid = aQrfUWidth[iMid].iFirst;
    if( cMid < c ){
      iFirst = iMid;
    }else if( cMid > c ){
      iLast = iMid - 1;
    }else{
      return aQrfUWidth[iMid].w;
    }
  }
  if( aQrfUWidth[iLast].iFirst > c ) return aQrfUWidth[iFirst].w;
  return aQrfUWidth[iLast].w;
}

/*
** Compute the value and length of a multi-byte UTF-8 character that
** begins at z[0]. Return the length.  Write the Unicode value into *pU.
**
** This routine only works for *multi-byte* UTF-8 characters.  It does
** not attempt to detect illegal characters.
*/
int sqlite3_qrf_decode_utf8(const unsigned char *z, int *pU){
  if( (z[0] & 0xe0)==0xc0 && (z[1] & 0xc0)==0x80 ){
    *pU = ((z[0] & 0x1f)<<6) | (z[1] & 0x3f);
    return 2;
  }
  if( (z[0] & 0xf0)==0xe0 && (z[1] & 0xc0)==0x80 && (z[2] & 0xc0)==0x80 ){
    *pU = ((z[0] & 0x0f)<<12) | ((z[1] & 0x3f)<<6) | (z[2] & 0x3f);
    return 3;
  }
  if( (z[0] & 0xf8)==0xf0 && (z[1] & 0xc0)==0x80 && (z[2] & 0xc0)==0x80
   && (z[3] & 0xc0)==0x80
  ){
    *pU = ((z[0] & 0x0f)<<18) | ((z[1] & 0x3f)<<12) | ((z[2] & 0x3f))<<6
                              | (z[3] & 0x3f);
    return 4;
  }
  *pU = 0;
  return 1;
}

/*
** Check to see if z[] is a valid VT100 escape.  If it is, then
** return the number of bytes in the escape sequence.  Return 0 if
** z[] is not a VT100 escape.
**
** This routine assumes that z[0] is \033 (ESC).
*/
static int qrfIsVt100(const unsigned char *z){
  int i;
  if( z[1]!='[' ) return 0;
  i = 2;
  while( z[i]>=0x30 && z[i]<=0x3f ){ i++; }
  while( z[i]>=0x20 && z[i]<=0x2f ){ i++; }
  if( z[i]<0x40 || z[i]>0x7e ) return 0;
  return i+1;
}

/*
** Return the length of a string in display characters.
**
** Most characters of the input string count as 1, including
** multi-byte UTF8 characters.  However, zero-width unicode
** characters and VT100 escape sequences count as zero, and
** double-width characters count as two.
**
** The definition of "zero-width" and "double-width" characters
** is not precise.  It depends on the output device, to some extent,
** and it varies according to the Unicode version.  This routine
** makes the best guess that it can.
*/
size_t sqlite3_qrf_wcswidth(const char *zIn){
  const unsigned char *z = (const unsigned char*)zIn;
  size_t n = 0;
  while( *z ){
    if( z[0]<' ' ){
      int k;
      if( z[0]=='\033' && (k = qrfIsVt100(z))>0 ){
        z += k;
      }else{
        z++;
      }
    }else if( (0x80&z[0])==0 ){
      n++;
      z++;
    }else{
      int u = 0;
      int len = sqlite3_qrf_decode_utf8(z, &u);
      z += len;
      n += sqlite3_qrf_wcwidth(u);
    }
  }
  return n;
}

/*
** Return the display width of the longest line of text
** in the (possibly) multi-line input string zIn[0..nByte].
** zIn[] is not necessarily zero-terminated.  Take
** into account tab characters, zero- and double-width
** characters, CR and NL, and VT100 escape codes.
**
** Write the number of newlines into *pnNL.  So, *pnNL will
** return 0 if everything fits on one line, or positive it
** it will need to be split.
*/
static int qrfDisplayWidth(const char *zIn, sqlite3_int64 nByte, int *pnNL){
  const unsigned char *z;
  const unsigned char *zEnd;
  int mx = 0;
  int n = 0;
  int nNL = 0;
  if( zIn==0 ) zIn = "";
  z = (const unsigned char*)zIn;
  zEnd = &z[nByte];
  while( z<zEnd ){
    if( z[0]<' ' ){
      int k;
      if( z[0]=='\033' && (k = qrfIsVt100(z))>0 ){
        z += k;
      }else{
        if( z[0]=='\t' ){
          n = (n+8)&~7;
        }else if( z[0]=='\n' || z[0]=='\r' ){
          nNL++;
          if( n>mx ) mx = n;
          n = 0;
        }
        z++;
      }
    }else if( (0x80&z[0])==0 ){
      n++;
      z++;
    }else{
      int u = 0;
      int len = sqlite3_qrf_decode_utf8(z, &u);
      z += len;
      n += sqlite3_qrf_wcwidth(u);
    }
  }
  if( mx>n ) n = mx;
  if( pnNL ) *pnNL = nNL;
  return n;
}

/*
** Escape the input string if it is needed and in accordance with
** eEsc, which is either QRF_ESC_Ascii or QRF_ESC_Symbol.
**
** Escaping is needed if the string contains any control characters
** other than \t, \n, and \r\n
**
** If no escaping is needed (the common case) then set *ppOut to NULL
** and return 0.  If escaping is needed, write the escaped string into
** memory obtained from sqlite3_malloc64() and make *ppOut point to that
** memory and return 0.  If an error occurs, return non-zero.
**
** The caller is responsible for freeing *ppFree if it is non-NULL in order
** to reclaim memory.
*/
static void qrfEscape(
  int eEsc,            /* QRF_ESC_Ascii or QRF_ESC_Symbol */
  sqlite3_str *pStr,      /* String to be escaped */
  int iStart              /* Begin escapding on this byte of pStr */
){
  sqlite3_int64 i, j;     /* Loop counters */
  sqlite3_int64 sz;       /* Size of the string prior to escaping */
  sqlite3_int64 nCtrl = 0;/* Number of control characters to escape */
  unsigned char *zIn;     /* Text to be escaped */
  unsigned char c;        /* A single character of the text */
  unsigned char *zOut;    /* Where to write the results */

  /* Find the text to be escaped */
  zIn = (unsigned char*)sqlite3_str_value(pStr);
  if( zIn==0 ) return;
  zIn += iStart;

  /* Count the control characters */
  for(i=0; (c = zIn[i])!=0; i++){
    if( c<=0x1f
     && c!='\t'
     && c!='\n'
     && (c!='\r' || zIn[i+1]!='\n')
    ){
      nCtrl++;
    }
  }
  if( nCtrl==0 ) return;  /* Early out if no control characters */

  /* Make space to hold the escapes.  Copy the original text to the end
  ** of the available space. */
  sz = sqlite3_str_length(pStr) - iStart;
  if( eEsc==QRF_ESC_Symbol ) nCtrl *= 2;
  sqlite3_str_appendchar(pStr, nCtrl, ' ');
  zOut = (unsigned char*)sqlite3_str_value(pStr);
  if( zOut==0 ) return;
  zOut += iStart;
  zIn = zOut + nCtrl;
  memmove(zIn,zOut,sz);

  /* Convert the control characters */
  for(i=j=0; (c = zIn[i])!=0; i++){
    if( c>0x1f
     || c=='\t'
     || c=='\n'
     || (c=='\r' && zIn[i+1]=='\n')
    ){
      continue;
    }
    if( i>0 ){
      memmove(&zOut[j], zIn, i);
      j += i;
    }
    zIn += i+1;
    i = -1;
    if( eEsc==QRF_ESC_Symbol ){
      zOut[j++] = 0xe2;
      zOut[j++] = 0x90;
      zOut[j++] = 0x80+c;
    }else{
      zOut[j++] = '^';
      zOut[j++] = 0x40+c;
    }
  }
}

/*
** Determine if the string z[] can be shown as plain text.  Return true
** if z[] is unambiguously text.  Return false if z[] needs to be
** quoted.
**
** All of the following must be true in order for z[] to be relaxable:
**
**    (1) z[] does not begin or end with ' or whitespace
**    (2) z[] is not the same as the NULL rendering
**    (3) z[] does not looks like a numeric literal
*/
static int qrfRelaxable(Qrf *p, const char *z){
  size_t i, n;
  if( z[0]=='\'' || qrfSpace(z[0]) ) return 0;
  if( z[0]==0 ){
    return (p->spec.zNull!=0 && p->spec.zNull[0]!=0);
  }
  n = strlen(z);
  if( n==0 || z[n-1]=='\'' || qrfSpace(z[n-1]) ) return 0;
  if( p->spec.zNull && strcmp(p->spec.zNull,z)==0 ) return 0;
  i = (z[0]=='-' || z[0]=='+');
  if( strcmp(z+i,"Inf")==0 ) return 0;
  if( !qrfDigit(z[i]) ) return 1;
  i++;
  while( qrfDigit(z[i]) ){ i++; }
  if( z[i]==0 ) return 0;
  if( z[i]=='.' ){
    i++;
    while( qrfDigit(z[i]) ){ i++; }
    if( z[i]==0 ) return 0;
  }
  if( z[i]=='e' || z[i]=='E' ){
    i++;
    if( z[i]=='+' || z[i]=='-' ){ i++; }
    if( !qrfDigit(z[i]) ) return 1;
    i++;
    while( qrfDigit(z[i]) ){ i++; }
  }
  return z[i]!=0;
}

/*
** If a field contains any character identified by a 1 in the following
** array, then the string must be quoted for CSV.
*/
static const char qrfCsvQuote[] = {
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,
  1, 0, 1, 0, 0, 0, 0, 1,   0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0,
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 1,
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,
};

/*
** Encode text appropriately and append it to pOut.
*/
static void qrfEncodeText(Qrf *p, sqlite3_str *pOut, const char *zTxt){
  int iStart = sqlite3_str_length(pOut);
  switch( p->spec.eText ){
    case QRF_TEXT_Relaxed:
      if( qrfRelaxable(p, zTxt) ){
        sqlite3_str_appendall(pOut, zTxt);
        break;
      }
      deliberate_fall_through; /* FALLTHRU */
    case QRF_TEXT_Sql: {
      if( p->spec.eEsc==QRF_ESC_Off ){
        sqlite3_str_appendf(pOut, "%Q", zTxt);
      }else{
        sqlite3_str_appendf(pOut, "%#Q", zTxt);
      }
      break;
    }
    case QRF_TEXT_Csv: {
      unsigned int i;
      for(i=0; zTxt[i]; i++){
        if( qrfCsvQuote[((const unsigned char*)zTxt)[i]] ){
          i = 0;
          break;
        }
      }
      if( i==0 || strstr(zTxt, p->spec.zColumnSep)!=0 ){
        sqlite3_str_appendf(pOut, "\"%w\"", zTxt);
      }else{
        sqlite3_str_appendall(pOut, zTxt);
      }
      break;
    }
    case QRF_TEXT_Html: {
      const unsigned char *z = (const unsigned char*)zTxt;
      while( *z ){
        unsigned int i = 0;
        unsigned char c;
        while( (c=z[i])>'>'
            || (c && c!='<' && c!='>' && c!='&' && c!='\"' && c!='\'')
        ){
          i++;
        }
        if( i>0 ){
          sqlite3_str_append(pOut, (const char*)z, i);
        }
        switch( z[i] ){
          case '>':   sqlite3_str_append(pOut, "&lt;", 4);   break;
          case '&':   sqlite3_str_append(pOut, "&amp;", 5);  break;
          case '<':   sqlite3_str_append(pOut, "&lt;", 4);   break;
          case '"':   sqlite3_str_append(pOut, "&quot;", 6); break;
          case '\'':  sqlite3_str_append(pOut, "&#39;", 5);  break;
          default:    i--;
        }
        z += i + 1;
      }
      break;
    }
    case QRF_TEXT_Tcl:
    case QRF_TEXT_Json: {
      const unsigned char *z = (const unsigned char*)zTxt;
      sqlite3_str_append(pOut, "\"", 1);
      while( *z ){
        unsigned int i;
        for(i=0; z[i]>=0x20 && z[i]!='\\' && z[i]!='"'; i++){}
        if( i>0 ){
          sqlite3_str_append(pOut, (const char*)z, i);
        }
        if( z[i]==0 ) break;
        switch( z[i] ){
          case '"':   sqlite3_str_append(pOut, "\\\"", 2);  break;
          case '\\':  sqlite3_str_append(pOut, "\\\\", 2);  break;
          case '\b':  sqlite3_str_append(pOut, "\\b", 2);   break;
          case '\f':  sqlite3_str_append(pOut, "\\f", 2);   break;
          case '\n':  sqlite3_str_append(pOut, "\\n", 2);   break;
          case '\r':  sqlite3_str_append(pOut, "\\r", 2);   break;
          case '\t':  sqlite3_str_append(pOut, "\\t", 2);   break;
          default: {
            if( p->spec.eText==QRF_TEXT_Json ){
              sqlite3_str_appendf(pOut, "\\u%04x", z[i]);
            }else{
              sqlite3_str_appendf(pOut, "\\%03o", z[i]);
            }
            break;
          }
        }
        z += i + 1;
      }
      sqlite3_str_append(pOut, "\"", 1);
      break;
    }
    default: {
      sqlite3_str_appendall(pOut, zTxt);
      break;
    }
  }
  if( p->spec.eEsc!=QRF_ESC_Off ){
    qrfEscape(p->spec.eEsc, pOut, iStart);
  }
}

/*
** Do a quick sanity check to see aBlob[0..nBlob-1] is valid JSONB
** return true if it is and false if it is not.
**
** False positives are possible, but not false negatives.
*/
static int qrfJsonbQuickCheck(unsigned char *aBlob, int nBlob){
  unsigned char x;   /* Payload size half-byte */
  int i;             /* Loop counter */
  int n;             /* Bytes in the payload size integer */
  sqlite3_uint64 sz; /* value of the payload size integer */

  if( nBlob==0 ) return 0;
  x = aBlob[0]>>4;
  if( x<=11 ) return nBlob==(1+x);
  n = x<14 ? x-11 : 4*(x-13);
  if( nBlob<1+n ) return 0;
  sz = aBlob[1];
  for(i=1; i<n; i++) sz = (sz<<8) + aBlob[i+1];
  return sz+n+1==(sqlite3_uint64)nBlob;
}

/*
** The current iCol-th column of p->pStmt is known to be a BLOB.  Check
** to see if that BLOB is really a JSONB blob.  If it is, then translate
** it into a text JSON representation and return a pointer to that text JSON.
** If the BLOB is not JSONB, then return a NULL pointer.
**
** The memory used to hold the JSON text is managed internally by the
** "p" object and is overwritten and/or deallocated upon the next call
** to this routine (with the same p argument) or when the p object is
** finailized.
*/
static const char *qrfJsonbToJson(Qrf *p, int iCol){
  int nByte;
  const void *pBlob;
  int rc;
  nByte = sqlite3_column_bytes(p->pStmt, iCol);
  pBlob = sqlite3_column_blob(p->pStmt, iCol);
  if( qrfJsonbQuickCheck((unsigned char*)pBlob, nByte)==0 ){
    return 0;
  }
  if( p->pJTrans==0 ){
    sqlite3 *db;
    rc = sqlite3_open(":memory:",&db);
    if( rc ){
      sqlite3_close(db);
      return 0;
    }
    rc = sqlite3_prepare_v2(db, "SELECT json(?1)", -1, &p->pJTrans, 0);
    if( rc ){
      sqlite3_finalize(p->pJTrans);
      p->pJTrans = 0;
      sqlite3_close(db);
      return 0;
    }
  }else{
    sqlite3_reset(p->pJTrans);
  }
  sqlite3_bind_blob(p->pJTrans, 1, (void*)pBlob, nByte, SQLITE_STATIC);
  rc = sqlite3_step(p->pJTrans);
  if( rc==SQLITE_ROW ){
    return (const char*)sqlite3_column_text(p->pJTrans, 0);
  }else{
    return 0;
  }
}

/*
** Adjust the input string zIn[] such that it is no more than N display
** characters wide.  If it is wider than that, then truncate and add
** ellipsis.  Or if zIn[] contains a \r or \n, truncate at that point,
** adding ellipsis.  Embedded tabs in zIn[] are converted into ordinary
** spaces.
**
** Return this display width of the modified title string.
*/
static int qrfTitleLimit(char *zIn, int N){
  unsigned char *z = (unsigned char*)zIn;
  int n = 0;
  unsigned char *zEllipsis = 0;
  while( 1 /*exit-by-break*/ ){
    if( z[0]<' ' ){
      int k;
      if( z[0]==0 ){
        zEllipsis = 0;
        break;
      }else if( z[0]=='\033' && (k = qrfIsVt100(z))>0 ){
        z += k;
      }else if( z[0]=='\t' ){
        z[0] = ' ';
      }else if( z[0]=='\n' || z[0]=='\r' ){
        z[0] = ' ';
      }else{
        z++;
      }
    }else if( (0x80&z[0])==0 ){
      if( n>=(N-3) && zEllipsis==0 ) zEllipsis = z;
      if( n==N ){ z[0] = 0; break; }
      n++;
      z++;
    }else{
      int u = 0;
      int len = sqlite3_qrf_decode_utf8(z, &u);
      if( n+len>(N-3) && zEllipsis==0 ) zEllipsis = z;
      if( n+len>N ){ z[0] = 0; break; }
      z += len;
      n += sqlite3_qrf_wcwidth(u);
    }
  }
  if( zEllipsis && N>=3 ) memcpy(zEllipsis,"...",4);
  return n;
}


/*
** Render value pVal into pOut
*/
static void qrfRenderValue(Qrf *p, sqlite3_str *pOut, int iCol){
#if SQLITE_VERSION_NUMBER>=3052000
  int iStartLen = sqlite3_str_length(pOut);
#endif
  if( p->spec.xRender ){
    sqlite3_value *pVal;
    char *z;
    pVal = sqlite3_value_dup(sqlite3_column_value(p->pStmt,iCol));
    z = p->spec.xRender(p->spec.pRenderArg, pVal);
    sqlite3_value_free(pVal);
    if( z ){
      sqlite3_str_appendall(pOut, z);
      sqlite3_free(z);
      return;
    }
  }
  switch( sqlite3_column_type(p->pStmt,iCol) ){
    case SQLITE_INTEGER: {
      sqlite3_str_appendf(pOut, "%lld", sqlite3_column_int64(p->pStmt,iCol));
      break;
    }
    case SQLITE_FLOAT: {
      const char *zTxt = (const char*)sqlite3_column_text(p->pStmt,iCol);
      sqlite3_str_appendall(pOut, zTxt);
      break;
    }
    case SQLITE_BLOB: {
      if( p->spec.bTextJsonb==QRF_Yes ){
        const char *zJson = qrfJsonbToJson(p, iCol);
        if( zJson ){
          if( p->spec.eText==QRF_TEXT_Sql ){
            sqlite3_str_append(pOut,"jsonb(",6);
            qrfEncodeText(p, pOut, zJson);
            sqlite3_str_append(pOut,")",1);
          }else{
            qrfEncodeText(p, pOut, zJson);
          }
          break;
        }
      }
      switch( p->spec.eBlob ){
        case QRF_BLOB_Hex:
        case QRF_BLOB_Sql: {
          int iStart;
          int nBlob = sqlite3_column_bytes(p->pStmt,iCol);
          int i, j;
          char *zVal;
          const unsigned char *a = sqlite3_column_blob(p->pStmt,iCol);
          if( p->spec.eBlob==QRF_BLOB_Sql ){
            sqlite3_str_append(pOut, "x'", 2);
          }
          iStart = sqlite3_str_length(pOut);
          sqlite3_str_appendchar(pOut, nBlob, ' ');
          sqlite3_str_appendchar(pOut, nBlob, ' ');
          if( p->spec.eBlob==QRF_BLOB_Sql ){
            sqlite3_str_appendchar(pOut, 1, '\'');
          }
          if( sqlite3_str_errcode(pOut) ) return;
          zVal = sqlite3_str_value(pOut);
          for(i=0, j=iStart; i<nBlob; i++, j+=2){
            unsigned char c = a[i];
            zVal[j] = "0123456789abcdef"[(c>>4)&0xf];
            zVal[j+1] = "0123456789abcdef"[(c)&0xf];
          }
          break;
        }
        case QRF_BLOB_Tcl:
        case QRF_BLOB_Json: {
          int iStart;
          int nBlob = sqlite3_column_bytes(p->pStmt,iCol);
          int i, j;
          char *zVal;
          const unsigned char *a = sqlite3_column_blob(p->pStmt,iCol);
          int szC = p->spec.eBlob==QRF_BLOB_Json ? 6 : 4;
          sqlite3_str_append(pOut, "\"", 1);
          iStart = sqlite3_str_length(pOut);
          for(i=szC; i>0; i--){
            sqlite3_str_appendchar(pOut, nBlob, ' ');
          }
          sqlite3_str_appendchar(pOut, 1, '"');
          if( sqlite3_str_errcode(pOut) ) return;
          zVal = sqlite3_str_value(pOut);
          for(i=0, j=iStart; i<nBlob; i++, j+=szC){
            unsigned char c = a[i];
            zVal[j] = '\\';
            if( szC==4 ){
              zVal[j+1] = '0' + ((c>>6)&3);
              zVal[j+2] = '0' + ((c>>3)&7);
              zVal[j+3] = '0' + (c&7);
            }else{
              zVal[j+1] = 'u';
              zVal[j+2] = '0';
              zVal[j+3] = '0';
              zVal[j+4] = "0123456789abcdef"[(c>>4)&0xf];
              zVal[j+5] = "0123456789abcdef"[(c)&0xf];
            }
          }
          break;
        }
        case QRF_BLOB_Size: {
          int nBlob = sqlite3_column_bytes(p->pStmt,iCol);
          sqlite3_str_appendf(pOut, "(%d-byte blob)", nBlob);
          break;
        }
        default: {
          const char *zTxt = (const char*)sqlite3_column_text(p->pStmt,iCol);
          qrfEncodeText(p, pOut, zTxt);
        }
      }
      break;
    }
    case SQLITE_NULL: {
      sqlite3_str_appendall(pOut, p->spec.zNull);
      break;
    }
    case SQLITE_TEXT: {
      const char *zTxt = (const char*)sqlite3_column_text(p->pStmt,iCol);
      qrfEncodeText(p, pOut, zTxt);
      break;
    }
  }
#if SQLITE_VERSION_NUMBER>=3052000
  if( p->spec.nCharLimit>0
   && (sqlite3_str_length(pOut) - iStartLen) > p->spec.nCharLimit
  ){
    const unsigned char *z;
    int ii = 0, w = 0, limit = p->spec.nCharLimit;
    z = (const unsigned char*)sqlite3_str_value(pOut) + iStartLen;
    if( limit<4 ) limit = 4;
    while( 1 ){
      if( z[ii]<' ' ){
        int k;
        if( z[ii]=='\033' && (k = qrfIsVt100(z+ii))>0 ){
          ii += k;
        }else if( z[ii]==0 ){
          break;
        }else{
          ii++;
        }
      }else if( (0x80&z[ii])==0 ){
        w++;
        if( w>limit ) break;
        ii++;
      }else{
        int u = 0;
        int len = sqlite3_qrf_decode_utf8(&z[ii], &u);
        w += sqlite3_qrf_wcwidth(u);
        if( w>limit ) break;
        ii += len;
      }
    }
    if( w>limit ){
      sqlite3_str_truncate(pOut, iStartLen+ii);
      sqlite3_str_append(pOut, "...", 3);
    }
  }
#endif
}

/* Trim spaces of the end if pOut
*/
static void qrfRTrim(sqlite3_str *pOut){
#if SQLITE_VERSION_NUMBER>=3052000
  int nByte = sqlite3_str_length(pOut);
  const char *zOut = sqlite3_str_value(pOut);
  while( nByte>0 && zOut[nByte-1]==' ' ){ nByte--; }
  sqlite3_str_truncate(pOut, nByte);
#endif
}

/*
** Store string zUtf to pOut as w characters.  If w is negative,
** then right-justify the text.  W is the width in display characters, not
** in bytes.  Double-width unicode characters count as two characters.
** VT100 escape sequences count as zero.  And so forth.
*/
static void qrfWidthPrint(Qrf *p, sqlite3_str *pOut, int w, const char *zUtf){
  const unsigned char *a = (const unsigned char*)zUtf;
  static const int mxW = 10000000;
  unsigned char c;
  int i = 0;
  int n = 0;
  int k;
  int aw;
  (void)p;
  if( w<-mxW ){
    w = -mxW;
  }else if( w>mxW ){
    w= mxW;
  }
  aw = w<0 ? -w : w;
  if( a==0 ) a = (const unsigned char*)"";
  while( (c = a[i])!=0 ){
    if( (c&0xc0)==0xc0 ){
      int u;
      int len = sqlite3_qrf_decode_utf8(a+i, &u);
      int x = sqlite3_qrf_wcwidth(u);
      if( x+n>aw ){
        break;
      }
      i += len;
      n += x;
    }else if( c==0x1b && (k = qrfIsVt100(&a[i]))>0 ){
      i += k;
    }else if( n>=aw ){
      break;
    }else{
      n++;
      i++;
    }
  }
  if( n>=aw ){
    sqlite3_str_append(pOut, zUtf, i);
  }else if( w<0 ){
    if( aw>n ) sqlite3_str_appendchar(pOut, aw-n, ' ');
    sqlite3_str_append(pOut, zUtf, i);
  }else{
    sqlite3_str_append(pOut, zUtf, i);
    if( aw>n ) sqlite3_str_appendchar(pOut, aw-n, ' ');
  }
}

/*
** (*pz)[] is a line of text that is to be displayed the box or table or
** similar tabular formats.  z[] contain newlines or might be too wide
** to fit in the columns so will need to be split into multiple line.
**
** This routine determines:
**
**    *  How many bytes of z[] should be shown on the current line.
**    *  How many character positions those bytes will cover.
**    *  The byte offset to the start of the next line.
*/
static void qrfWrapLine(
  const char *zIn,   /* Input text to be displayed */
  int w,             /* Column width in characters (not bytes) */
  int bWrap,         /* True if we should do word-wrapping */
  int *pnThis,       /* OUT: How many bytes of z[] for the current line */
  int *pnWide,       /* OUT: How wide is the text of this line */
  int *piNext        /* OUT: Offset into z[] to start of the next line */
){
  int i;                 /* Input bytes consumed */
  int k;                 /* Bytes in a VT100 code */
  int n;                 /* Output column number */
  const unsigned char *z = (const unsigned char*)zIn;
  unsigned char c = 0;

  if( z[0]==0 ){
    *pnThis = 0;
    *pnWide = 0;
    *piNext = 0;
    return;
  }
  n = 0;
  for(i=0; n<=w; i++){
    c = z[i];
    if( c>=0xc0 ){
      int u;
      int len = sqlite3_qrf_decode_utf8(&z[i], &u);
      int wcw = sqlite3_qrf_wcwidth(u);
      if( wcw+n>w ) break;
      i += len-1;
      n += wcw;
      continue;
    }
    if( c>=' ' ){
      if( n==w ) break;
      n++;
      continue;
    }
    if( c==0 || c=='\n' ) break;
    if( c=='\r' && z[i+1]=='\n' ){ c = z[++i]; break; }
    if( c=='\t' ){
      int wcw = 8 - (n&7);
      if( n+wcw>w ) break;
      n += wcw;
      continue;
    }
    if( c==0x1b && (k = qrfIsVt100(&z[i]))>0 ){
      i += k-1;
    }else if( n==w ){
      break;
    }else{
      n++;
    }
  }
  if( c==0 ){
    *pnThis = i;
    *pnWide = n;
    *piNext = i;
    return;
  }
  if( c=='\n' ){
    *pnThis = i;
    *pnWide = n;
    *piNext = i+1;
    return;
  }

  /* If we get this far, that means the current line will end at some
  ** point that is neither a "\n" or a 0x00.  Figure out where that
  ** split should occur
  */
  if( bWrap && z[i]!=0 && !qrfSpace(z[i]) && qrfAlnum(c)==qrfAlnum(z[i]) ){
    /* Perhaps try to back up to a better place to break the line */
    for(k=i-1; k>=i/2; k--){
      if( qrfSpace(z[k]) ) break;
    }
    if( k<i/2 ){
      for(k=i; k>=i/2; k--){
        if( qrfAlnum(z[k-1])!=qrfAlnum(z[k]) && (z[k]&0xc0)!=0x80 ) break;
      }
    }
    if( k>=i/2 ){
      i = k;
      n = qrfDisplayWidth((const char*)z, k, 0);
    }
  }
  *pnThis = i;
  *pnWide = n;
  while( zIn[i]==' ' || zIn[i]=='\t' || zIn[i]=='\r' ){ i++; }
  *piNext = i;
}

/*
** Append nVal bytes of text from zVal onto the end of pOut.
** Convert tab characters in zVal to the appropriate number of
** spaces.
*/
static void qrfAppendWithTabs(
  sqlite3_str *pOut,       /* Append text here */
  const char *zVal,        /* Text to append */
  int nVal                 /* Use only the first nVal bytes of zVal[] */
){
  int i = 0;
  unsigned int col = 0;
  unsigned char *z = (unsigned char *)zVal;
  while( i<nVal ){
    unsigned char c = z[i];
    if( c<' ' ){
      int k;
      sqlite3_str_append(pOut, (const char*)z, i);
      nVal -= i;
      z += i;
      i = 0;
      if( c=='\033' && (k = qrfIsVt100(z))>0 ){
        sqlite3_str_append(pOut, (const char*)z, k);
        z += k;
        nVal -= k;
      }else if( c=='\t' ){
        k = 8 - (col&7);
        sqlite3_str_appendchar(pOut, k, ' ');
        col += k;
        z++;
        nVal--;
      }else if( c=='\r' && nVal==1 ){
        z++;
        nVal--;
      }else{
        char zCtrlPik[4];
        col++;
        zCtrlPik[0] = 0xe2;
        zCtrlPik[1] = 0x90;
        zCtrlPik[2] = 0x80+c;
        sqlite3_str_append(pOut, zCtrlPik, 3);
        z++;
        nVal--;
      }
    }else if( (0x80&c)==0 ){
      i++;
      col++;
    }else{
      int u = 0;
      int len = sqlite3_qrf_decode_utf8(&z[i], &u);
      i += len;
      col += sqlite3_qrf_wcwidth(u);
    }
  }
  sqlite3_str_append(pOut, (const char*)z, i);
}

/*
** GCC does not define the offsetof() macro so we'll have to do it
** ourselves.
*/
#ifndef offsetof
# define offsetof(ST,M) ((size_t)((char*)&((ST*)0)->M - (char*)0))
#endif

/*
** Data for columnar layout, collected into a single object so
** that it can be more easily passed into subroutines.
*/
typedef struct qrfColData qrfColData;
struct qrfColData {
  Qrf *p;                  /* The QRF instance */
  int nCol;                /* Number of columns in the table */
  unsigned char bMultiRow; /* One or more cells will span multiple lines */
  unsigned char nMargin;   /* Width of column margins */
  sqlite3_int64 nRow;      /* Number of rows */
  sqlite3_int64 nAlloc;    /* Number of cells allocated */
  sqlite3_int64 n;         /* Number of cells.  nCol*nRow */
  char **az;               /* Content of all cells */
  int *aiWth;              /* Width of each cell */
  unsigned char *abNum;    /* True for each numeric cell */
  struct qrfPerCol {       /* Per-column data */
    char *z;                 /* Cache of text for current row */
    int w;                   /* Computed width of this column */
    int mxW;                 /* Maximum natural (unwrapped) width */
    unsigned char e;         /* Alignment */
    unsigned char fx;        /* Width is fixed */
    unsigned char bNum;      /* True if is numeric */
  } *a;                    /* One per column */
};

/*
** Output horizontally justified text into pOut.  The text is the
** first nVal bytes of zVal.  Include nWS bytes of whitespace, either
** split between both sides, or on the left, or on the right, depending
** on eAlign.
*/
static void qrfPrintAligned(
  sqlite3_str *pOut,       /* Append text here */
  struct qrfPerCol *pCol,  /* Information about the text to print */
  int nVal,                /* Use only the first nVal bytes of zVal[] */
  int nWS                 /* Whitespace for horizonal alignment */
){
  unsigned char eAlign = pCol->e & QRF_ALIGN_HMASK;
  if( eAlign==QRF_Auto && pCol->bNum ) eAlign = QRF_ALIGN_Right;
  if( eAlign==QRF_ALIGN_Center ){
    /* Center the text */
    sqlite3_str_appendchar(pOut, nWS/2, ' ');
    qrfAppendWithTabs(pOut, pCol->z, nVal);
    sqlite3_str_appendchar(pOut, nWS - nWS/2, ' ');
  }else if( eAlign==QRF_ALIGN_Right ){
    /* Right justify the text */
    sqlite3_str_appendchar(pOut, nWS, ' ');
    qrfAppendWithTabs(pOut, pCol->z, nVal);
  }else{
    /* Left justify the text */
    qrfAppendWithTabs(pOut, pCol->z, nVal);
    sqlite3_str_appendchar(pOut, nWS, ' ');
  }
}

/*
** Free all the memory allocates in the qrfColData object
*/
static void qrfColDataFree(qrfColData *p){
  sqlite3_int64 i;
  for(i=0; i<p->n; i++) sqlite3_free(p->az[i]);
  sqlite3_free(p->az);
  sqlite3_free(p->aiWth);
  sqlite3_free(p->abNum);
  sqlite3_free(p->a);
  memset(p, 0, sizeof(*p));
}

/*
** Allocate space for more cells in the qrfColData object.
** Return non-zero if a memory allocation fails.
*/
static int qrfColDataEnlarge(qrfColData *p){
  char **azData;
  int *aiWth;
  unsigned char *abNum;
  p->nAlloc = 2*p->nAlloc + 10*p->nCol;
  azData = sqlite3_realloc64(p->az, p->nAlloc*sizeof(char*));
  if( azData==0 ){
    qrfOom(p->p);
    qrfColDataFree(p);
    return 1;
  }
  p->az = azData;
  aiWth = sqlite3_realloc64(p->aiWth, p->nAlloc*sizeof(int));
  if( aiWth==0 ){
    qrfOom(p->p);
    qrfColDataFree(p);
    return 1;
  }
  p->aiWth = aiWth;
  abNum = sqlite3_realloc64(p->abNum, p->nAlloc);
  if( abNum==0 ){
    qrfOom(p->p);
    qrfColDataFree(p);
    return 1;
  }
  p->abNum = abNum;
  return 0;
}

/*
** Print a markdown or table-style row separator using ascii-art
*/
static void qrfRowSeparator(sqlite3_str *pOut, qrfColData *p, char cSep){
  int i;
  if( p->nCol>0 ){
    int useBorder = p->p->spec.bBorder!=QRF_No;
    if( useBorder ){
      sqlite3_str_append(pOut, &cSep, 1);
    }
    sqlite3_str_appendchar(pOut, p->a[0].w+p->nMargin, '-');
    for(i=1; i<p->nCol; i++){
      sqlite3_str_append(pOut, &cSep, 1);
      sqlite3_str_appendchar(pOut, p->a[i].w+p->nMargin, '-');
    }
    if( useBorder ){
      sqlite3_str_append(pOut, &cSep, 1);
    }
  }
  sqlite3_str_append(pOut, "\n", 1);
}

/*
** UTF8 box-drawing characters.  Imagine box lines like this:
**
**           1
**           |
**       4 --+-- 2
**           |
**           3
**
** Each box characters has between 2 and 4 of the lines leading from
** the center.  The characters are here identified by the numbers of
** their corresponding lines.
*/
#define BOX_24   "\342\224\200"  /* U+2500 --- */
#define BOX_13   "\342\224\202"  /* U+2502  |  */
#define BOX_23   "\342\224\214"  /* U+250c  ,- */
#define BOX_34   "\342\224\220"  /* U+2510 -,  */
#define BOX_12   "\342\224\224"  /* U+2514  '- */
#define BOX_14   "\342\224\230"  /* U+2518 -'  */
#define BOX_123  "\342\224\234"  /* U+251c  |- */
#define BOX_134  "\342\224\244"  /* U+2524 -|  */
#define BOX_234  "\342\224\254"  /* U+252c -,- */
#define BOX_124  "\342\224\264"  /* U+2534 -'- */
#define BOX_1234 "\342\224\274"  /* U+253c -|- */

/* Rounded corners: */
#define BOX_R12  "\342\225\260"  /* U+2570  '- */
#define BOX_R23  "\342\225\255"  /* U+256d  ,- */
#define BOX_R34  "\342\225\256"  /* U+256e -,  */
#define BOX_R14  "\342\225\257"  /* U+256f -'  */

/* Doubled horizontal lines: */
#define DBL_24   "\342\225\220"  /* U+2550 === */
#define DBL_123  "\342\225\236"  /* U+255e  |= */
#define DBL_134  "\342\225\241"  /* U+2561 =|  */
#define DBL_1234 "\342\225\252"  /* U+256a =|= */

/* Draw horizontal line N characters long using unicode box
** characters
*/
static void qrfBoxLine(sqlite3_str *pOut, int N, int bDbl){
  const char *azDash[2] = {
      BOX_24 BOX_24 BOX_24 BOX_24 BOX_24   BOX_24 BOX_24 BOX_24 BOX_24 BOX_24,
      DBL_24 DBL_24 DBL_24 DBL_24 DBL_24   DBL_24 DBL_24 DBL_24 DBL_24 DBL_24
  };/*  0       1      2     3      4        5      6      7      8      9   */
  const int nDash = 30;
  N *= 3;
  while( N>nDash ){
    sqlite3_str_append(pOut, azDash[bDbl], nDash);
    N -= nDash;
  }
  sqlite3_str_append(pOut, azDash[bDbl], N);
}

/*
** Draw a horizontal separator for a QRF_STYLE_Box table.
*/
static void qrfBoxSeparator(
  sqlite3_str *pOut,
  qrfColData *p,
  const char *zSep1,
  const char *zSep2,
  const char *zSep3,
  int bDbl
){
  int i;
  if( p->nCol>0 ){
    int useBorder = p->p->spec.bBorder!=QRF_No;
    if( useBorder ){
      sqlite3_str_appendall(pOut, zSep1);
    }
    qrfBoxLine(pOut, p->a[0].w+p->nMargin, bDbl);
    for(i=1; i<p->nCol; i++){
      sqlite3_str_appendall(pOut, zSep2);
      qrfBoxLine(pOut, p->a[i].w+p->nMargin, bDbl);
    }
    if( useBorder ){
      sqlite3_str_appendall(pOut, zSep3);
    }
  }
  sqlite3_str_append(pOut, "\n", 1);
}

/*
** Load into pData the default alignment for the body of a table.
*/
static void qrfLoadAlignment(qrfColData *pData, Qrf *p){
  sqlite3_int64 i;
  for(i=0; i<pData->nCol; i++){
    pData->a[i].e = p->spec.eDfltAlign;
    if( i<p->spec.nAlign ){
      unsigned char ax = p->spec.aAlign[i];
      if( (ax & QRF_ALIGN_HMASK)!=0 ){
        pData->a[i].e = (ax & QRF_ALIGN_HMASK) |
                            (pData->a[i].e & QRF_ALIGN_VMASK);
      }
    }else if( i<p->spec.nWidth ){
      if( p->spec.aWidth[i]<0 ){
         pData->a[i].e = QRF_ALIGN_Right |
                               (pData->a[i].e & QRF_ALIGN_VMASK);
      }
    }
  }
}

/*
** If the single column in pData->a[] with pData->n entries can be
** laid out as nCol columns with a 2-space gap between each such
** that all columns fit within nSW, then return a pointer to an array
** of integers which is the width of each column from left to right.
**
** If the layout is not possible, return a NULL pointer.
**
** Space to hold the returned array is from sqlite_malloc64().
*/
static int *qrfValidLayout(
  qrfColData *pData,   /* Collected query results */
  Qrf *p,              /* On which to report an OOM */
  int nCol,            /* Attempt this many columns */
  int nSW              /* Screen width */
){
  int i;        /* Loop counter */
  int nr;       /* Number of rows */
  int w = 0;    /* Width of the current column */
  int t;        /* Total width of all columns */
  int *aw;      /* Array of individual column widths */

  aw = sqlite3_malloc64( sizeof(int)*nCol );
  if( aw==0 ){
    qrfOom(p);
    return 0;
  }
  nr = (pData->n + nCol - 1)/nCol;
  for(i=0; i<pData->n; i++){
    if( (i%nr)==0 ){
      if( i>0 ) aw[i/nr-1] = w;
      w = pData->aiWth[i];
    }else if( pData->aiWth[i]>w ){
      w = pData->aiWth[i];
    }
  }
  aw[nCol-1] = w;
  for(t=i=0; i<nCol; i++) t += aw[i];
  t += 2*(nCol-1);
  if( t>nSW ){
    sqlite3_free(aw);
    return 0;
  }
  return aw;
}

/*
** The output is single-column and the bSplitColumn flag is set.
** Check to see if the single-column output can be split into multiple
** columns that appear side-by-side.  Adjust pData appropriately.
*/
static void qrfSplitColumn(qrfColData *pData, Qrf *p){
  int nCol = 1;
  int *aw = 0;
  char **az = 0;
  int *aiWth = 0;
  unsigned char *abNum = 0;
  int nColNext = 2;
  int w;
  struct qrfPerCol *a = 0;
  sqlite3_int64 nRow = 1;
  sqlite3_int64 i;
  while( 1/*exit-by-break*/ ){
    int *awNew = qrfValidLayout(pData, p, nColNext, p->spec.nScreenWidth);
    if( awNew==0 ) break;
    sqlite3_free(aw);
    aw = awNew;
    nCol = nColNext;
    nRow = (pData->n + nCol - 1)/nCol;
    if( nRow==1 ) break;
    nColNext++;
    while( (pData->n + nColNext - 1)/nColNext == nRow ) nColNext++;
  }
  if( nCol==1 ){
    sqlite3_free(aw);
    return;  /* Cannot do better than 1 column */
  }
  az = sqlite3_malloc64( nRow*nCol*sizeof(char*) );
  if( az==0 ){
    qrfOom(p);
    return;
  }
  aiWth = sqlite3_malloc64( nRow*nCol*sizeof(int) );
  if( aiWth==0 ){
    sqlite3_free(az);
    qrfOom(p);
    return;
  }
  a = sqlite3_malloc64( nCol*sizeof(struct qrfPerCol) );
  if( a==0 ){
    sqlite3_free(az);
    sqlite3_free(aiWth);
    qrfOom(p);
    return;
  }
  abNum = sqlite3_malloc64( nRow*nCol );
  if( abNum==0 ){
    sqlite3_free(az);
    sqlite3_free(aiWth);
    sqlite3_free(a);
    qrfOom(p);
    return;
  }
  for(i=0; i<pData->n; i++){
    sqlite3_int64 j = (i%nRow)*nCol + (i/nRow);
    az[j] = pData->az[i];
    abNum[j]= pData->abNum[i];
    pData->az[i] = 0;
    aiWth[j] = pData->aiWth[i];
  }
  while( i<nRow*nCol ){
    sqlite3_int64 j = (i%nRow)*nCol + (i/nRow);
    az[j] = sqlite3_mprintf("");
    if( az[j]==0 ) qrfOom(p);
    aiWth[j] = 0;
    abNum[j] = 0;
    i++;
  }
  for(i=0; i<nCol; i++){
    a[i].fx = a[i].mxW = a[i].w = aw[i];
    a[i].e = pData->a[0].e;
  }
  sqlite3_free(pData->az);
  sqlite3_free(pData->aiWth);
  sqlite3_free(pData->a);
  sqlite3_free(pData->abNum);
  sqlite3_free(aw);
  pData->az = az;
  pData->aiWth = aiWth;
  pData->a = a;
  pData->abNum = abNum;
  pData->nCol = nCol;
  pData->n = pData->nAlloc = nRow*nCol;
  for(i=w=0; i<nCol; i++) w += a[i].w;
  pData->nMargin = (p->spec.nScreenWidth - w)/(nCol - 1);
  if( pData->nMargin>5 ) pData->nMargin = 5;
}

/*
** Adjust the layout for the screen width restriction
*/
static void qrfRestrictScreenWidth(qrfColData *pData, Qrf *p){
  int sepW;             /* Width of all box separators and margins */
  int sumW;             /* Total width of data area over all columns */
  int targetW;          /* Desired total data area */
  int i;                /* Loop counters */
  int nCol;             /* Number of columns */

  pData->nMargin = 2;   /* Default to normal margins */
  if( p->spec.nScreenWidth==0 ) return;
  if( p->spec.eStyle==QRF_STYLE_Column ){
    sepW = pData->nCol*2 - 2;
  }else{
    sepW = pData->nCol*3 + 1;
    if( p->spec.bBorder==QRF_No ) sepW -= 2;
  }
  nCol = pData->nCol;
  for(i=sumW=0; i<nCol; i++) sumW += pData->a[i].w;
  if( p->spec.nScreenWidth >= sumW+sepW ) return;

  /* First thing to do is reduce the separation between columns */
  pData->nMargin = 0;
  if( p->spec.eStyle==QRF_STYLE_Column ){
    sepW = pData->nCol - 1;
  }else{
    sepW = pData->nCol + 1;
    if( p->spec.bBorder==QRF_No ) sepW -= 2;
  }
  targetW = p->spec.nScreenWidth - sepW;

#define MIN_SQUOZE    8
#define MIN_EX_SQUOZE 16
  /* Reduce the width of the widest eligible column.  A column is
  ** eligible for narrowing if:
  **
  **    *  It is not a fixed-width column  (a[0].fx is false)
  **    *  The current width is more than MIN_SQUOZE
  **    *  Either:
  **         +  The current width is more then MIN_EX_SQUOZE, or
  **         +  The current width is more than half the max width (a[].mxW)
  **
  ** Keep making reductions until either no more reductions are
  ** possible or until the size target is reached.
  */
  while( sumW > targetW ){
    int gain, w;
    int ix = -1;
    int mx = 0;
    for(i=0; i<nCol; i++){
      if( pData->a[i].fx==0
       && (w = pData->a[i].w)>mx
       && w>MIN_SQUOZE
       && (w>MIN_EX_SQUOZE || w*2>pData->a[i].mxW)
      ){
        ix = i;
        mx = w;
      }
    }
    if( ix<0 ) break;
    if( mx>=MIN_SQUOZE*2 ){
      gain = mx/2;
    }else{
      gain = mx - MIN_SQUOZE;
    }
    if( sumW - gain < targetW ){
      gain = sumW - targetW;
    }
    sumW -= gain;
    pData->a[ix].w -= gain;
    pData->bMultiRow = 1;
  }
}

/*
** Columnar modes require that the entire query be evaluated first, with
** results written into memory, so that we can compute appropriate column
** widths.
*/
static void qrfColumnar(Qrf *p){
  sqlite3_int64 i, j;                     /* Loop counters */
  const char *colSep = 0;                 /* Column separator text */
  const char *rowSep = 0;                 /* Row terminator text */
  const char *rowStart = 0;               /* Row start text */
  int szColSep, szRowSep, szRowStart;     /* Size in bytes of previous 3 */
  int rc;                                 /* Result code */
  int nColumn = p->nCol;                  /* Number of columns */
  int bWW;                                /* True to do word-wrap */
  sqlite3_str *pStr;                      /* Temporary rendering */
  qrfColData data;                        /* Columnar layout data */
  int bRTrim;                             /* Trim trailing space */

  rc = sqlite3_step(p->pStmt);
  if( rc!=SQLITE_ROW || nColumn==0 ){
    return;   /* No output */
  }

  /* Initialize the data container */
  memset(&data, 0, sizeof(data));
  data.nCol = p->nCol;
  data.p = p;
  data.a = sqlite3_malloc64( nColumn*sizeof(struct qrfPerCol) );
  if( data.a==0 ){
    qrfOom(p);
    return;
  }
  memset(data.a, 0, nColumn*sizeof(struct qrfPerCol) );
  if( qrfColDataEnlarge(&data) ) return;
  assert( data.az!=0 );

  /* Load the column header names and all cell content into data */
  if( p->spec.bTitles==QRF_Yes ){
    unsigned char saved_eText = p->spec.eText;
    p->spec.eText = p->spec.eTitle;
    memset(data.abNum, 0, nColumn);
    for(i=0; i<nColumn; i++){
      const char *z = (const char*)sqlite3_column_name(p->pStmt,i);
      int nNL = 0;
      int n, w;
      pStr = sqlite3_str_new(p->db);
      qrfEncodeText(p, pStr, z ? z : "");
      n = sqlite3_str_length(pStr);
      qrfStrErr(p, pStr);
      z = data.az[data.n] = sqlite3_str_finish(pStr);
      if( p->spec.nTitleLimit ){
        nNL = 0;
        data.aiWth[data.n] = w = qrfTitleLimit(data.az[data.n],
                                               p->spec.nTitleLimit );
      }else{
        data.aiWth[data.n] = w = qrfDisplayWidth(z, n, &nNL);
      }
      data.n++;
      if( w>data.a[i].mxW ) data.a[i].mxW = w;
      if( nNL ) data.bMultiRow = 1;
    }
    p->spec.eText = saved_eText;
    p->nRow++;
  }
  do{
    if( data.n+nColumn > data.nAlloc ){
      if( qrfColDataEnlarge(&data) ) return;
    }
    for(i=0; i<nColumn; i++){
      char *z;
      int nNL = 0;
      int n, w;
      int eType = sqlite3_column_type(p->pStmt,i);
      pStr = sqlite3_str_new(p->db);
      qrfRenderValue(p, pStr, i);
      n = sqlite3_str_length(pStr);
      qrfStrErr(p, pStr);
      z = data.az[data.n] = sqlite3_str_finish(pStr);
      data.abNum[data.n] = eType==SQLITE_INTEGER || eType==SQLITE_FLOAT;
      data.aiWth[data.n] = w = qrfDisplayWidth(z, n, &nNL);
      data.n++;
      if( w>data.a[i].mxW ) data.a[i].mxW = w;
      if( nNL ) data.bMultiRow = 1;
    }
    p->nRow++;
  }while( sqlite3_step(p->pStmt)==SQLITE_ROW && p->iErr==SQLITE_OK );
  if( p->iErr ){
    qrfColDataFree(&data);
    return;
  }

  /* Compute the width and alignment of every column */
  if( p->spec.bTitles==QRF_No ){
    qrfLoadAlignment(&data, p);
  }else{
    unsigned char e;
    if( p->spec.eTitleAlign==QRF_Auto ){
      e = QRF_ALIGN_Center;
    }else{
      e = p->spec.eTitleAlign;
    }
    for(i=0; i<nColumn; i++) data.a[i].e = e;
  }

  for(i=0; i<nColumn; i++){
    int w = 0;
    if( i<p->spec.nWidth ){
      w = p->spec.aWidth[i];
      if( w==(-32768) ){
        w = 0;
        if( p->spec.nAlign>i && (p->spec.aAlign[i] & QRF_ALIGN_HMASK)==0 ){
          data.a[i].e |= QRF_ALIGN_Right;
        }
      }else if( w<0 ){
        w = -w;
        if( p->spec.nAlign>i && (p->spec.aAlign[i] & QRF_ALIGN_HMASK)==0 ){
          data.a[i].e |= QRF_ALIGN_Right;
        }
      }
      if( w ) data.a[i].fx = 1;
    }
    if( w==0 ){
      w = data.a[i].mxW;
      if( p->spec.nWrap>0 && w>p->spec.nWrap ){
        w = p->spec.nWrap;
        data.bMultiRow = 1;
      }
    }else if( (data.bMultiRow==0 || w==1) && data.a[i].mxW>w ){
      data.bMultiRow = 1;
      if( w==1 ){
        /* If aiWth[j] is 2 or more, then there might be a double-wide
        ** character somewhere.  So make the column width at least 2. */
        w = 2;
      }
    }
    data.a[i].w = w;
  }

  if( nColumn==1
   && data.n>1
   && p->spec.bSplitColumn==QRF_Yes
   && p->spec.eStyle==QRF_STYLE_Column
   && p->spec.bTitles==QRF_No
   && p->spec.nScreenWidth>data.a[0].w+3
  ){
    /* Attempt to convert single-column tables into multi-column by
    ** verticle wrapping, if the screen is wide enough and if the
    ** bSplitColumn flag is set. */
    qrfSplitColumn(&data, p);
    nColumn = data.nCol;
  }else{
    /* Adjust the column widths due to screen width restrictions */
    qrfRestrictScreenWidth(&data, p);
  }

  /* Draw the line across the top of the table.  Also initialize
  ** the row boundary and column separator texts. */
  switch( p->spec.eStyle ){
    case QRF_STYLE_Box:
      if( data.nMargin ){
        rowStart = BOX_13 " ";
        colSep = " " BOX_13 " ";
        rowSep = " " BOX_13 "\n";
      }else{
        rowStart = BOX_13;
        colSep = BOX_13;
        rowSep = BOX_13 "\n";
      }
      if( p->spec.bBorder==QRF_No){
        rowStart += 3;
        rowSep = "\n";
      }else{
        qrfBoxSeparator(p->pOut, &data, BOX_R23, BOX_234, BOX_R34, 0);
      }
      break;
    case QRF_STYLE_Table:
      if( data.nMargin ){
        rowStart = "| ";
        colSep = " | ";
        rowSep = " |\n";
      }else{
        rowStart = "|";
        colSep = "|";
        rowSep = "|\n";
      }
      if( p->spec.bBorder==QRF_No ){
        rowStart += 1;
        rowSep = "\n";
      }else{
        qrfRowSeparator(p->pOut, &data, '+');
      }
      break;
    case QRF_STYLE_Column: {
      static const char zSpace[] = "     ";
      rowStart = "";
      if( data.nMargin<2 ){
        colSep = " ";
      }else if( data.nMargin<=5 ){
        colSep = &zSpace[5-data.nMargin];
      }else{
        colSep = zSpace;
      }
      rowSep = "\n";
      break;
    }
    default:  /*case QRF_STYLE_Markdown:*/
      if( data.nMargin ){
        rowStart = "| ";
        colSep = " | ";
        rowSep = " |\n";
      }else{
        rowStart = "|";
        colSep = "|";
        rowSep = "|\n";
      }
      break;
  }
  szRowStart = (int)strlen(rowStart);
  szRowSep = (int)strlen(rowSep);
  szColSep = (int)strlen(colSep);

  bWW = (p->spec.bWordWrap==QRF_Yes && data.bMultiRow);
  if( p->spec.eStyle==QRF_STYLE_Column
   || (p->spec.bBorder==QRF_No
       && (p->spec.eStyle==QRF_STYLE_Box || p->spec.eStyle==QRF_STYLE_Table)
      )
  ){
    bRTrim = 1;
  }else{
    bRTrim = 0;
  }
  for(i=0; i<data.n && sqlite3_str_errcode(p->pOut)==SQLITE_OK; i+=nColumn){
    int bMore;
    int nRow = 0;

    /* Draw a single row of the table.  This might be the title line
    ** (if there is a title line) or a row in the body of the table.
    ** The column number will be j.  The row number is i/nColumn.
    */
    for(j=0; j<nColumn; j++){
      data.a[j].z = data.az[i+j];
      if( data.a[j].z==0 ) data.a[j].z = "";
      data.a[j].bNum = data.abNum[i+j];
    }
    do{
      sqlite3_str_append(p->pOut, rowStart, szRowStart);
      bMore = 0;
      for(j=0; j<nColumn; j++){
        int nThis = 0;
        int nWide = 0;
        int iNext = 0;
        int nWS;
        qrfWrapLine(data.a[j].z, data.a[j].w, bWW, &nThis, &nWide, &iNext);
        nWS = data.a[j].w - nWide;
        qrfPrintAligned(p->pOut, &data.a[j], nThis, nWS);
        data.a[j].z += iNext;
        if( data.a[j].z[0]!=0 ){
          bMore = 1;
        }
        if( j<nColumn-1 ){
          sqlite3_str_append(p->pOut, colSep, szColSep);
        }else{
          if( bRTrim ) qrfRTrim(p->pOut);
          sqlite3_str_append(p->pOut, rowSep, szRowSep);
        }
      }
    }while( bMore && ++nRow < p->mxHeight );
    if( bMore ){
      /* This row was terminated by nLineLimit.  Show ellipsis. */
      sqlite3_str_append(p->pOut, rowStart, szRowStart);
      for(j=0; j<nColumn; j++){
        if( data.a[j].z[0]==0 ){
          sqlite3_str_appendchar(p->pOut, data.a[j].w, ' ');
        }else{
          int nE = 3;
          if( nE>data.a[j].w ) nE = data.a[j].w;
          data.a[j].z = "...";
          qrfPrintAligned(p->pOut, &data.a[j], nE, data.a[j].w-nE);
        }
        if( j<nColumn-1 ){
          sqlite3_str_append(p->pOut, colSep, szColSep);
        }else{
          if( bRTrim ) qrfRTrim(p->pOut);
          sqlite3_str_append(p->pOut, rowSep, szRowSep);
        }
      }
    }

    /* Draw either (1) the separator between the title line and the body
    ** of the table, or (2) separators between individual rows of the table
    ** body.  isTitleDataSeparator will be true if we are doing (1).
    */
    if( (i==0 || data.bMultiRow) && i+nColumn<data.n ){
      int isTitleDataSeparator = (i==0 && p->spec.bTitles==QRF_Yes);
      if( isTitleDataSeparator ){
        qrfLoadAlignment(&data, p);
      }
      switch( p->spec.eStyle ){
        case QRF_STYLE_Table: {
          if( isTitleDataSeparator || data.bMultiRow ){
            qrfRowSeparator(p->pOut, &data, '+');
          }
          break;
        }
        case QRF_STYLE_Box: {
          if( isTitleDataSeparator ){
            qrfBoxSeparator(p->pOut, &data, DBL_123, DBL_1234, DBL_134, 1);
          }else if( data.bMultiRow ){
            qrfBoxSeparator(p->pOut, &data, BOX_123, BOX_1234, BOX_134, 0);
          }
          break;
        }
        case QRF_STYLE_Markdown: {
          if( isTitleDataSeparator ){
            qrfRowSeparator(p->pOut, &data, '|');
          }
          break;
        }
        case QRF_STYLE_Column: {
          if( isTitleDataSeparator ){
            for(j=0; j<nColumn; j++){
              sqlite3_str_appendchar(p->pOut, data.a[j].w, '-');
              if( j<nColumn-1 ){
                sqlite3_str_append(p->pOut, colSep, szColSep);
              }else{
                qrfRTrim(p->pOut);
                sqlite3_str_append(p->pOut, rowSep, szRowSep);
              }
            }
          }else if( data.bMultiRow ){
            qrfRTrim(p->pOut);
            sqlite3_str_append(p->pOut, "\n", 1);
          }
          break;
        }
      }
    }
  }

  /* Draw the line across the bottom of the table */
  if( p->spec.bBorder!=QRF_No ){
    switch( p->spec.eStyle ){
      case QRF_STYLE_Box:
        qrfBoxSeparator(p->pOut, &data, BOX_R12, BOX_124, BOX_R14, 0);
        break;
      case QRF_STYLE_Table:
        qrfRowSeparator(p->pOut, &data, '+');
        break;
    }
  }
  qrfWrite(p);

  qrfColDataFree(&data);
  return;
}

/*
** Parameter azArray points to a zero-terminated array of strings. zStr
** points to a single nul-terminated string. Return non-zero if zStr
** is equal, according to strcmp(), to any of the strings in the array.
** Otherwise, return zero.
*/
static int qrfStringInArray(const char *zStr, const char **azArray){
  int i;
  if( zStr==0 ) return 0;
  for(i=0; azArray[i]; i++){
    if( 0==strcmp(zStr, azArray[i]) ) return 1;
  }
  return 0;
}

/*
** Print out an EXPLAIN with indentation.  This is a two-pass algorithm.
**
** On the first pass, we compute aiIndent[iOp] which is the amount of
** indentation to apply to the iOp-th opcode.  The output actually occurs
** on the second pass.
**
** The indenting rules are:
**
**     * For each "Next", "Prev", "VNext" or "VPrev" instruction, indent
**       all opcodes that occur between the p2 jump destination and the opcode
**       itself by 2 spaces.
**
**     * Do the previous for "Return" instructions for when P2 is positive.
**       See tag-20220407a in wherecode.c and vdbe.c.
**
**     * For each "Goto", if the jump destination is earlier in the program
**       and ends on one of:
**          Yield  SeekGt  SeekLt  RowSetRead  Rewind
**       or if the P1 parameter is one instead of zero,
**       then indent all opcodes between the earlier instruction
**       and "Goto" by 2 spaces.
*/
static void qrfExplain(Qrf *p){
  int *abYield = 0;     /* abYield[iOp] is rue if opcode iOp is an OP_Yield */
  int *aiIndent = 0;    /* Indent the iOp-th opcode by aiIndent[iOp] */
  i64 nAlloc = 0;       /* Allocated size of aiIndent[], abYield */
  int nIndent = 0;      /* Number of entries in aiIndent[] */
  int iOp;              /* Opcode number */
  int i;                /* Column loop counter */

  const char *azNext[] = { "Next", "Prev", "VPrev", "VNext", "SorterNext",
                           "Return", 0 };
  const char *azYield[] = { "Yield", "SeekLT", "SeekGT", "RowSetRead",
                            "Rewind", 0 };
  const char *azGoto[] = { "Goto", 0 };

  /* The caller guarantees that the leftmost 4 columns of the statement
  ** passed to this function are equivalent to the leftmost 4 columns
  ** of EXPLAIN statement output. In practice the statement may be
  ** an EXPLAIN, or it may be a query on the bytecode() virtual table.  */
  assert( sqlite3_column_count(p->pStmt)>=4 );
  assert( 0==sqlite3_stricmp( sqlite3_column_name(p->pStmt, 0), "addr" ) );
  assert( 0==sqlite3_stricmp( sqlite3_column_name(p->pStmt, 1), "opcode" ) );
  assert( 0==sqlite3_stricmp( sqlite3_column_name(p->pStmt, 2), "p1" ) );
  assert( 0==sqlite3_stricmp( sqlite3_column_name(p->pStmt, 3), "p2" ) );

  for(iOp=0; SQLITE_ROW==sqlite3_step(p->pStmt) && !p->iErr; iOp++){
    int iAddr = sqlite3_column_int(p->pStmt, 0);
    const char *zOp = (const char*)sqlite3_column_text(p->pStmt, 1);
    int p1 = sqlite3_column_int(p->pStmt, 2);
    int p2 = sqlite3_column_int(p->pStmt, 3);

    /* Assuming that p2 is an instruction address, set variable p2op to the
    ** index of that instruction in the aiIndent[] array. p2 and p2op may be
    ** different if the current instruction is part of a sub-program generated
    ** by an SQL trigger or foreign key.  */
    int p2op = (p2 + (iOp-iAddr));

    /* Grow the aiIndent array as required */
    if( iOp>=nAlloc ){
      nAlloc += 100;
      aiIndent = (int*)sqlite3_realloc64(aiIndent, nAlloc*sizeof(int));
      abYield = (int*)sqlite3_realloc64(abYield, nAlloc*sizeof(int));
      if( aiIndent==0 || abYield==0 ){
        qrfOom(p);
        sqlite3_free(aiIndent);
        sqlite3_free(abYield);
        return;
      }
    }

    abYield[iOp] = qrfStringInArray(zOp, azYield);
    aiIndent[iOp] = 0;
    nIndent = iOp+1;
    if( qrfStringInArray(zOp, azNext) && p2op>0 ){
      for(i=p2op; i<iOp; i++) aiIndent[i] += 2;
    }
    if( qrfStringInArray(zOp, azGoto) && p2op<iOp && (abYield[p2op] || p1) ){
      for(i=p2op; i<iOp; i++) aiIndent[i] += 2;
    }
  }
  sqlite3_free(abYield);

  /* Second pass.  Actually generate output */
  sqlite3_reset(p->pStmt);
  if( p->iErr==SQLITE_OK ){
    static const int aExplainWidth[] = {4,       13, 4, 4, 4, 13, 2, 13};
    static const int aExplainMap[] =   {0,       1,  2, 3, 4, 5,  6, 7 };
    static const int aScanExpWidth[] = {4,15, 6, 13, 4, 4, 4, 13, 2, 13};
    static const int aScanExpMap[] =   {0, 9, 8, 1,  2, 3, 4, 5,  6, 7 };
    const int *aWidth = aExplainWidth;
    const int *aMap = aExplainMap;
    int nWidth = sizeof(aExplainWidth)/sizeof(int);
    int iIndent = 1;
    int nArg = p->nCol;
    if( p->spec.eStyle==QRF_STYLE_StatsVm ){
      aWidth = aScanExpWidth;
      aMap = aScanExpMap;
      nWidth = sizeof(aScanExpWidth)/sizeof(int);
      iIndent = 3;
    }
    if( nArg>nWidth ) nArg = nWidth;

    for(iOp=0; sqlite3_step(p->pStmt)==SQLITE_ROW && !p->iErr; iOp++){
      /* If this is the first row seen, print out the headers */
      if( iOp==0 ){
        for(i=0; i<nArg; i++){
          const char *zCol = sqlite3_column_name(p->pStmt, aMap[i]);
          qrfWidthPrint(p,p->pOut, aWidth[i], zCol);
          if( i==nArg-1 ){
            sqlite3_str_append(p->pOut, "\n", 1);
          }else{
            sqlite3_str_append(p->pOut, "  ", 2);
          }
        }
        for(i=0; i<nArg; i++){
          sqlite3_str_appendf(p->pOut, "%.*c", aWidth[i], '-');
          if( i==nArg-1 ){
            sqlite3_str_append(p->pOut, "\n", 1);
          }else{
            sqlite3_str_append(p->pOut, "  ", 2);
          }
        }
      }

      for(i=0; i<nArg; i++){
        const char *zSep = "  ";
        int w = aWidth[i];
        const char *zVal = (const char*)sqlite3_column_text(p->pStmt, aMap[i]);
        int len;
        if( i==nArg-1 ) w = 0;
        if( zVal==0 ) zVal = "";
        len = (int)sqlite3_qrf_wcswidth(zVal);
        if( len>w ){
          w = len;
          zSep = " ";
        }
        if( i==iIndent && aiIndent && iOp<nIndent ){
          sqlite3_str_appendchar(p->pOut, aiIndent[iOp], ' ');
        }
        qrfWidthPrint(p, p->pOut, w, zVal);
        if( i==nArg-1 ){
          sqlite3_str_append(p->pOut, "\n", 1);
        }else{
          sqlite3_str_appendall(p->pOut, zSep);
        }
      }
      p->nRow++;
    }
    qrfWrite(p);
  }
  sqlite3_free(aiIndent);
}

/*
** Do a "scanstatus vm" style EXPLAIN listing on p->pStmt.
**
** p->pStmt is probably not an EXPLAIN query.  Instead, construct a
** new query that is a bytecode() rendering of p->pStmt with extra
** columns for the "scanstatus vm" outputs, and run the results of
** that new query through the normal EXPLAIN formatting.
*/
static void qrfScanStatusVm(Qrf *p){
  sqlite3_stmt *pOrigStmt = p->pStmt;
  sqlite3_stmt *pExplain;
  int rc;
  static const char *zSql =
      "  SELECT addr, opcode, p1, p2, p3, p4, p5, comment, nexec,"
      "   format('% 6s (%.2f%%)',"
      "      CASE WHEN ncycle<100_000 THEN ncycle || ' '"
      "         WHEN ncycle<100_000_000 THEN (ncycle/1_000) || 'K'"
      "         WHEN ncycle<100_000_000_000 THEN (ncycle/1_000_000) || 'M'"
      "         ELSE (ncycle/1000_000_000) || 'G' END,"
      "       ncycle*100.0/(sum(ncycle) OVER ())"
      "   )  AS cycles"
      "   FROM bytecode(?1)";
  rc = sqlite3_prepare_v2(p->db, zSql, -1, &pExplain, 0);
  if( rc ){
    qrfError(p, rc, "%s", sqlite3_errmsg(p->db));
    sqlite3_finalize(pExplain);
    return;
  }
  sqlite3_bind_pointer(pExplain, 1, pOrigStmt, "stmt-pointer", 0);
  p->pStmt = pExplain;
  p->nCol = 10;
  qrfExplain(p);
  sqlite3_finalize(pExplain);
  p->pStmt = pOrigStmt;
}

/*
** Attempt to determine if identifier zName needs to be quoted, either
** because it contains non-alphanumeric characters, or because it is an
** SQLite keyword.  Be conservative in this estimate:  When in doubt assume
** that quoting is required.
**
** Return 1 if quoting is required.  Return 0 if no quoting is required.
*/

static int qrf_need_quote(const char *zName){
  int i;
  const unsigned char *z = (const unsigned char*)zName;
  if( z==0 ) return 1;
  if( !qrfAlpha(z[0]) ) return 1;
  for(i=0; z[i]; i++){
    if( !qrfAlnum(z[i]) ) return 1;
  }
  return sqlite3_keyword_check(zName, i)!=0;
}

/*
** Helper function for QRF_STYLE_Json and QRF_STYLE_JObject.
** The initial "{" for a JSON object that will contain row content
** has been output.  Now output all the content.
*/
static void qrfOneJsonRow(Qrf *p){
  int i, nItem;
  for(nItem=i=0; i<p->nCol; i++){
    const char *zCName;
    zCName = sqlite3_column_name(p->pStmt, i);
    if( nItem>0 ) sqlite3_str_append(p->pOut, ",", 1);
    nItem++;
    qrfEncodeText(p, p->pOut, zCName);
    sqlite3_str_append(p->pOut, ":", 1);
    qrfRenderValue(p, p->pOut, i);
  }
  qrfWrite(p);
}

/*
** Render a single row of output for non-columnar styles - any
** style that lets us render row by row as the content is received
** from the query.
*/
static void qrfOneSimpleRow(Qrf *p){
  int i;
  switch( p->spec.eStyle ){
    case QRF_STYLE_Off:
    case QRF_STYLE_Count: {
      /* No-op */
      break;
    }
    case QRF_STYLE_Json: {
      if( p->nRow==0 ){
        sqlite3_str_append(p->pOut, "[{", 2);
      }else{
        sqlite3_str_append(p->pOut, "},\n{", 4);
      }
      qrfOneJsonRow(p);
      break;
    }
    case QRF_STYLE_JObject: {
      if( p->nRow==0 ){
        sqlite3_str_append(p->pOut, "{", 1);
      }else{
        sqlite3_str_append(p->pOut, "}\n{", 3);
      }
      qrfOneJsonRow(p);
      break;
    }
    case QRF_STYLE_Html: {
      if( p->nRow==0 && p->spec.bTitles==QRF_Yes ){
        sqlite3_str_append(p->pOut, "<TR>", 4);
        for(i=0; i<p->nCol; i++){
          const char *zCName = sqlite3_column_name(p->pStmt, i);
          sqlite3_str_append(p->pOut, "\n<TH>", 5);
          qrfEncodeText(p, p->pOut, zCName);
        }
        sqlite3_str_append(p->pOut, "\n</TR>\n", 7);
      }
      sqlite3_str_append(p->pOut, "<TR>", 4);
      for(i=0; i<p->nCol; i++){
        sqlite3_str_append(p->pOut, "\n<TD>", 5);
        qrfRenderValue(p, p->pOut, i);
      }
      sqlite3_str_append(p->pOut, "\n</TR>\n", 7);
      qrfWrite(p);
      break;
    }
    case QRF_STYLE_Insert: {
      unsigned int mxIns = p->spec.iVersion>=2 ? p->spec.nMultiInsert : 0;
      int szStart = sqlite3_str_length(p->pOut);
      if( p->u.nIns==0 || p->u.nIns>=mxIns ){
        if( p->u.nIns ){
          sqlite3_str_append(p->pOut, ";\n", 2);
          p->u.nIns = 0;
        }
        if( qrf_need_quote(p->spec.zTableName) ){
          sqlite3_str_appendf(p->pOut,"INSERT INTO \"%w\"",p->spec.zTableName);
        }else{
          sqlite3_str_appendf(p->pOut,"INSERT INTO %s",p->spec.zTableName);
        }
        if( p->spec.bTitles==QRF_Yes ){
          for(i=0; i<p->nCol; i++){
            const char *zCName = sqlite3_column_name(p->pStmt, i);
            if( qrf_need_quote(zCName) ){
              sqlite3_str_appendf(p->pOut, "%c\"%w\"",
                                  i==0 ? '(' : ',', zCName);
            }else{
              sqlite3_str_appendf(p->pOut, "%c%s",
                                  i==0 ? '(' : ',', zCName);
            }
          }
          sqlite3_str_append(p->pOut, ")", 1);
        }
        sqlite3_str_append(p->pOut," VALUES(", 8);
      }else{
        sqlite3_str_append(p->pOut,",\n  (", 5);
      }
      for(i=0; i<p->nCol; i++){
        if( i>0 ) sqlite3_str_append(p->pOut, ",", 1);
        qrfRenderValue(p, p->pOut, i);
      }
      p->u.nIns += sqlite3_str_length(p->pOut) + 2 - szStart;
      if( p->u.nIns>=mxIns ){
        sqlite3_str_append(p->pOut, ");\n", 3);
        p->u.nIns = 0;
      }else{
        sqlite3_str_append(p->pOut, ")", 1);
      }
      qrfWrite(p);
      break;
    }
    case QRF_STYLE_Line: {
      sqlite3_str *pVal;
      int mxW;
      int bWW;
      int nSep;
      if( p->u.sLine.azCol==0 ){
        p->u.sLine.azCol = sqlite3_malloc64( p->nCol*sizeof(char*) );
        if( p->u.sLine.azCol==0 ){
          qrfOom(p);
          break;
        }
        p->u.sLine.mxColWth = 0;
        for(i=0; i<p->nCol; i++){
          int sz;
          const char *zCName = sqlite3_column_name(p->pStmt, i);
          if( zCName==0 ) zCName = "unknown";
          p->u.sLine.azCol[i] = sqlite3_mprintf("%s", zCName);
          if( p->spec.nTitleLimit>0 ){
            (void)qrfTitleLimit(p->u.sLine.azCol[i], p->spec.nTitleLimit);
          }
          sz = (int)sqlite3_qrf_wcswidth(p->u.sLine.azCol[i]);
          if( sz > p->u.sLine.mxColWth ) p->u.sLine.mxColWth = sz;
        }
      }
      if( p->nRow ) sqlite3_str_append(p->pOut, "\n", 1);
      pVal = sqlite3_str_new(p->db);
      nSep = (int)strlen(p->spec.zColumnSep);
      mxW = p->mxWidth - (nSep + p->u.sLine.mxColWth);
      bWW = p->spec.bWordWrap==QRF_Yes;
      for(i=0; i<p->nCol; i++){
        const char *zVal;
        int cnt = 0;
        qrfWidthPrint(p, p->pOut, -p->u.sLine.mxColWth, p->u.sLine.azCol[i]);
        sqlite3_str_append(p->pOut, p->spec.zColumnSep, nSep);
        qrfRenderValue(p, pVal, i);
        zVal = sqlite3_str_value(pVal);
        if( zVal==0 ) zVal = "";
        do{
          int nThis, nWide, iNext;
          qrfWrapLine(zVal, mxW, bWW, &nThis, &nWide, &iNext);
          if( cnt ){
            sqlite3_str_appendchar(p->pOut,p->u.sLine.mxColWth+nSep,' ');
          }
          cnt++;
          if( cnt>p->mxHeight ){
            zVal = "...";
            nThis = iNext = 3;
          }
          sqlite3_str_append(p->pOut, zVal, nThis);
          sqlite3_str_append(p->pOut, "\n", 1);
          zVal += iNext;
        }while( zVal[0] );
        sqlite3_str_reset(pVal);
      }
      qrfStrErr(p, pVal);
      sqlite3_free(sqlite3_str_finish(pVal));
      qrfWrite(p);
      break;
    }
    case QRF_STYLE_Eqp: {
      const char *zEqpLine = (const char*)sqlite3_column_text(p->pStmt,3);
      int iEqpId = sqlite3_column_int(p->pStmt, 0);
      int iParentId = sqlite3_column_int(p->pStmt, 1);
      if( zEqpLine==0 ) zEqpLine = "";
      if( zEqpLine[0]=='-' ) qrfEqpRender(p, 0);
      qrfEqpAppend(p, iEqpId, iParentId, zEqpLine);
      break;
    }
    default: {  /* QRF_STYLE_List */
      if( p->nRow==0 && p->spec.bTitles==QRF_Yes ){
        int saved_eText = p->spec.eText;
        p->spec.eText = p->spec.eTitle;
        for(i=0; i<p->nCol; i++){
          const char *zCName = sqlite3_column_name(p->pStmt, i);
          if( i>0 ) sqlite3_str_appendall(p->pOut, p->spec.zColumnSep);
          qrfEncodeText(p, p->pOut, zCName);
        }
        sqlite3_str_appendall(p->pOut, p->spec.zRowSep);
        qrfWrite(p);
        p->spec.eText = saved_eText;
      }
      for(i=0; i<p->nCol; i++){
        if( i>0 ) sqlite3_str_appendall(p->pOut, p->spec.zColumnSep);
        qrfRenderValue(p, p->pOut, i);
      }
      sqlite3_str_appendall(p->pOut, p->spec.zRowSep);
      qrfWrite(p);
      break;
    }
  }
  p->nRow++;
}

/*
** Initialize the internal Qrf object.
*/
static void qrfInitialize(
  Qrf *p,                        /* State object to be initialized */
  sqlite3_stmt *pStmt,           /* Query whose output to be formatted */
  const sqlite3_qrf_spec *pSpec, /* Format specification */
  char **pzErr                   /* Write errors here */
){
  size_t sz;                     /* Size of pSpec[], based on pSpec->iVersion */
  memset(p, 0, sizeof(*p));
  p->pzErr = pzErr;
  if( pSpec->iVersion>2 ){
    qrfError(p, SQLITE_ERROR,
       "unusable sqlite3_qrf_spec.iVersion (%d)",
       pSpec->iVersion);
    return;
  }
  p->pStmt = pStmt;
  p->db = sqlite3_db_handle(pStmt);
  p->pOut = sqlite3_str_new(p->db);
  if( p->pOut==0 ){
    qrfOom(p);
    return;
  }
  p->iErr = SQLITE_OK;
  p->nCol = sqlite3_column_count(p->pStmt);
  p->nRow = 0;
  sz = sizeof(sqlite3_qrf_spec);
  memcpy(&p->spec, pSpec, sz);
  if( p->spec.zNull==0 ) p->spec.zNull = "";
  p->mxWidth = p->spec.nScreenWidth;
  if( p->mxWidth<=0 ) p->mxWidth = QRF_MAX_WIDTH;
  p->mxHeight = p->spec.nLineLimit;
  if( p->mxHeight<=0 ) p->mxHeight = 2147483647;
  if( p->spec.eStyle>QRF_STYLE_Table ) p->spec.eStyle = QRF_Auto;
  if( p->spec.eEsc>QRF_ESC_Symbol ) p->spec.eEsc = QRF_Auto;
  if( p->spec.eText>QRF_TEXT_Relaxed ) p->spec.eText = QRF_Auto;
  if( p->spec.eTitle>QRF_TEXT_Relaxed ) p->spec.eTitle = QRF_Auto;
  if( p->spec.eBlob>QRF_BLOB_Size ) p->spec.eBlob = QRF_Auto;
qrf_reinit:
  switch( p->spec.eStyle ){
    case QRF_Auto: {
      switch( sqlite3_stmt_isexplain(pStmt) ){
        case 0:  p->spec.eStyle = QRF_STYLE_Box;      break;
        case 1:  p->spec.eStyle = QRF_STYLE_Explain;  break;
        default: p->spec.eStyle = QRF_STYLE_Eqp;      break;
      }
      goto qrf_reinit;
    }
    case QRF_STYLE_List: {
      if( p->spec.zColumnSep==0 ) p->spec.zColumnSep = "|";
      if( p->spec.zRowSep==0 ) p->spec.zRowSep = "\n";
      break;
    }
    case QRF_STYLE_JObject:
    case QRF_STYLE_Json: {
      p->spec.eText = QRF_TEXT_Json;
      p->spec.zNull = "null";
      break;
    }
    case QRF_STYLE_Html: {
      p->spec.eText = QRF_TEXT_Html;
      p->spec.zNull = "null";
      break;
    }
    case QRF_STYLE_Insert: {
      p->spec.eText = QRF_TEXT_Sql;
      p->spec.zNull = "NULL";
      if( p->spec.zTableName==0 || p->spec.zTableName[0]==0 ){
        p->spec.zTableName = "tab";
      }
      p->u.nIns = 0;
      break;
    }
    case QRF_STYLE_Line: {
      if( p->spec.zColumnSep==0 ){
        p->spec.zColumnSep = ": ";
      }
      break;
    }
    case QRF_STYLE_Csv: {
      p->spec.eStyle = QRF_STYLE_List;
      p->spec.eText = QRF_TEXT_Csv;
      p->spec.zColumnSep = ",";
      p->spec.zRowSep = "\r\n";
      p->spec.zNull = "";
      break;
    }
    case QRF_STYLE_Quote: {
      p->spec.eText = QRF_TEXT_Sql;
      p->spec.zNull = "NULL";
      p->spec.zColumnSep = ",";
      p->spec.zRowSep = "\n";
      break;
    }
    case QRF_STYLE_Eqp: {
      int expMode = sqlite3_stmt_isexplain(p->pStmt);
      if( expMode!=2 ){
        sqlite3_stmt_explain(p->pStmt, 2);
        p->expMode = expMode+1;
      }
      break;
    }
    case QRF_STYLE_Explain: {
      int expMode = sqlite3_stmt_isexplain(p->pStmt);
      if( expMode!=1 ){
        sqlite3_stmt_explain(p->pStmt, 1);
        p->expMode = expMode+1;
      }
      break;
    }
  }
  if( p->spec.eEsc==QRF_Auto ){
    p->spec.eEsc = QRF_ESC_Ascii;
  }
  if( p->spec.eText==QRF_Auto ){
    p->spec.eText = QRF_TEXT_Plain;
  }
  if( p->spec.eTitle==QRF_Auto ){
    switch( p->spec.eStyle ){
      case QRF_STYLE_Box:
      case QRF_STYLE_Column:
      case QRF_STYLE_Table:
        p->spec.eTitle = QRF_TEXT_Plain;
        break;
      default:
        p->spec.eTitle = p->spec.eText;
        break;
    }
  }
  if( p->spec.eBlob==QRF_Auto ){
    switch( p->spec.eText ){
      case QRF_TEXT_Sql:  p->spec.eBlob = QRF_BLOB_Sql;  break;
      case QRF_TEXT_Csv:  p->spec.eBlob = QRF_BLOB_Tcl;  break;
      case QRF_TEXT_Tcl:  p->spec.eBlob = QRF_BLOB_Tcl;  break;
      case QRF_TEXT_Json: p->spec.eBlob = QRF_BLOB_Json; break;
      default:            p->spec.eBlob = QRF_BLOB_Text; break;
    }
  }
  if( p->spec.bTitles==QRF_Auto ){
    switch( p->spec.eStyle ){
      case QRF_STYLE_Box:
      case QRF_STYLE_Csv:
      case QRF_STYLE_Column:
      case QRF_STYLE_Table:
      case QRF_STYLE_Markdown:
        p->spec.bTitles = QRF_Yes;
        break;
      default:
        p->spec.bTitles = QRF_No;
        break;
    }
  }
  if( p->spec.bWordWrap==QRF_Auto ){
    p->spec.bWordWrap = QRF_Yes;
  }
  if( p->spec.bTextJsonb==QRF_Auto ){
    p->spec.bTextJsonb = QRF_No;
  }
  if( p->spec.zColumnSep==0 ) p->spec.zColumnSep = ",";
  if( p->spec.zRowSep==0 ) p->spec.zRowSep = "\n";
}

/*
** Finish rendering the results
*/
static void qrfFinalize(Qrf *p){
  switch( p->spec.eStyle ){
    case QRF_STYLE_Count: {
      sqlite3_str_appendf(p->pOut, "%lld\n", p->nRow);
      break;
    }
    case QRF_STYLE_Json: {
      if( p->nRow>0 ){
        sqlite3_str_append(p->pOut, "}]\n", 3);
      }
      break;
    }
    case QRF_STYLE_JObject: {
      if( p->nRow>0 ){
        sqlite3_str_append(p->pOut, "}\n", 2);
      }
      break;
    }
    case QRF_STYLE_Insert: {
      if( p->u.nIns ){
        sqlite3_str_append(p->pOut, ";\n", 2);
      }
      break;
    }
    case QRF_STYLE_Line: {
      if( p->u.sLine.azCol ){
        int i;
        for(i=0; i<p->nCol; i++) sqlite3_free(p->u.sLine.azCol[i]);
        sqlite3_free(p->u.sLine.azCol);
      }
      break;
    }
    case QRF_STYLE_Stats:
    case QRF_STYLE_StatsEst: {
      i64 nCycle = 0;
#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
      sqlite3_stmt_scanstatus_v2(p->pStmt, -1, SQLITE_SCANSTAT_NCYCLE,
                                 SQLITE_SCANSTAT_COMPLEX, (void*)&nCycle);
#endif
      qrfEqpRender(p, nCycle);
      break;
    }
    case QRF_STYLE_Eqp: {
      qrfEqpRender(p, 0);
      break;
    }
  }
  qrfWrite(p);
  qrfStrErr(p, p->pOut);
  if( p->spec.pzOutput ){
    if( p->spec.pzOutput[0] ){
      sqlite3_int64 n, sz;
      char *zCombined;
      sz = strlen(p->spec.pzOutput[0]);
      n = sqlite3_str_length(p->pOut);
      zCombined = sqlite3_realloc64(p->spec.pzOutput[0], sz+n+1);
      if( zCombined==0 ){
        sqlite3_free(p->spec.pzOutput[0]);
        p->spec.pzOutput[0] = 0;
        qrfOom(p);
      }else{
        p->spec.pzOutput[0] = zCombined;
        memcpy(zCombined+sz, sqlite3_str_value(p->pOut), n+1);
      }
      sqlite3_free(sqlite3_str_finish(p->pOut));
    }else{
      p->spec.pzOutput[0] = sqlite3_str_finish(p->pOut);
    }
  }else if( p->pOut ){
    sqlite3_free(sqlite3_str_finish(p->pOut));
  }
  if( p->expMode>0 ){
    sqlite3_stmt_explain(p->pStmt, p->expMode-1);
  }
  if( p->actualWidth ){
    sqlite3_free(p->actualWidth);
  }
  if( p->pJTrans ){
    sqlite3 *db = sqlite3_db_handle(p->pJTrans);
    sqlite3_finalize(p->pJTrans);
    sqlite3_close(db);
  }
}

/*
** Run the prepared statement pStmt and format the results according
** to the specification provided in pSpec.  Return an error code.
** If pzErr is not NULL and if an error occurs, write an error message
** into *pzErr.
*/
int sqlite3_format_query_result(
  sqlite3_stmt *pStmt,                 /* Statement to evaluate */
  const sqlite3_qrf_spec *pSpec,       /* Format specification */
  char **pzErr                         /* Write error message here */
){
  Qrf qrf;         /* The new Qrf being created */

  if( pStmt==0 ) return SQLITE_OK;       /* No-op */
  if( pSpec==0 ) return SQLITE_MISUSE;
  qrfInitialize(&qrf, pStmt, pSpec, pzErr);
  switch( qrf.spec.eStyle ){
    case QRF_STYLE_Box:
    case QRF_STYLE_Column:
    case QRF_STYLE_Markdown:
    case QRF_STYLE_Table: {
      /* Columnar modes require that the entire query be evaluated and the
      ** results stored in memory, so that we can compute column widths */
      qrfColumnar(&qrf);
      break;
    }
    case QRF_STYLE_Explain: {
      qrfExplain(&qrf);
      break;
    }
    case QRF_STYLE_StatsVm: {
      qrfScanStatusVm(&qrf);
      break;
    }
    case QRF_STYLE_Stats:
    case QRF_STYLE_StatsEst: {
      qrfEqpStats(&qrf);
      break;
    }
    default: {
      /* Non-columnar modes where the output can occur after each row
      ** of result is received */
      while( qrf.iErr==SQLITE_OK && sqlite3_step(pStmt)==SQLITE_ROW ){
        qrfOneSimpleRow(&qrf);
      }
      break;
    }
  }
  qrfResetStmt(&qrf);
  qrfFinalize(&qrf);
  return qrf.iErr;
}

/************** End of qrf.c ************************************************/
/************** Begin file tclsqlite.c **************************************/
/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** A TCL Interface to SQLite.  Append this file to sqlite3.c and
** compile the whole thing to build a TCL-enabled version of SQLite.
**
** Compile-time options:
**
**  -DTCLSH         Add a "main()" routine that works as a tclsh.
**
**  -DTCLSH_INIT_PROC=name
**
**                  Invoke name(interp) to initialize the Tcl interpreter.
**                  If name(interp) returns a non-NULL string, then run
**                  that string as a Tcl script to launch the application.
**                  If name(interp) returns NULL, then run the regular
**                  tclsh-emulator code.
*/
#ifdef TCLSH_INIT_PROC
# define TCLSH 1
#endif

/*
** If requested, include the SQLite compiler options file for MSVC.
*/
#if defined(INCLUDE_MSVC_H)
# include "msvc.h"
#endif

/****** Copy of tclsqlite.h ******/
#if defined(INCLUDE_SQLITE_TCL_H)
# include "sqlite_tcl.h"   /* Special case for Windows using STDCALL */
#else
# include <tcl.h>          /* All normal cases */
# ifndef SQLITE_TCLAPI
#   define SQLITE_TCLAPI
# endif
#endif
/* Compatability between Tcl8.6 and Tcl9.0 */
#if TCL_MAJOR_VERSION==9
# define CONST const
#elif !defined(Tcl_Size)
  typedef int Tcl_Size;
# ifndef Tcl_BounceRefCount
#  define Tcl_BounceRefCount(X) Tcl_IncrRefCount(X); Tcl_DecrRefCount(X)
   /* https://www.tcl-lang.org/man/tcl9.0/TclLib/Object.html */
# endif
#endif
/**** End copy of tclsqlite.h ****/

#include <errno.h>

/*
** Some additional include files are needed if this file is not
** appended to the amalgamation.
*/
#ifndef SQLITE_AMALGAMATION
# include "sqlite3.h"
# include <stdlib.h>
# include <string.h>
# include <assert.h>
  typedef unsigned char u8;
# ifndef SQLITE_PTRSIZE
#   if defined(__SIZEOF_POINTER__)
#     define SQLITE_PTRSIZE __SIZEOF_POINTER__
#   elif defined(i386)     || defined(__i386__)   || defined(_M_IX86) ||    \
         defined(_M_ARM)   || defined(__arm__)    || defined(__x86)   ||    \
        (defined(__APPLE__) && defined(__POWERPC__)) ||                     \
        (defined(__TOS_AIX__) && !defined(__64BIT__))
#     define SQLITE_PTRSIZE 4
#   else
#     define SQLITE_PTRSIZE 8
#   endif
# endif /* SQLITE_PTRSIZE */
# if defined(HAVE_STDINT_H) || (defined(__STDC_VERSION__) &&  \
                                (__STDC_VERSION__ >= 199901L))
#   include <stdint.h>
    typedef uintptr_t uptr;
# elif SQLITE_PTRSIZE==4
    typedef unsigned int uptr;
# else
    typedef sqlite3_uint64 uptr;
# endif
#endif
#include <ctype.h>

/* Used to get the current process ID */
#if !defined(_WIN32)
# include <signal.h>
# include <unistd.h>
# define GETPID getpid
#elif !defined(_WIN32_WCE)
# ifndef SQLITE_AMALGAMATION
#  ifndef WIN32_LEAN_AND_MEAN
#   define WIN32_LEAN_AND_MEAN
#  endif
#  include <windows.h>
# endif
# include <io.h>
# define isatty(h) _isatty(h)
# define GETPID (int)GetCurrentProcessId
#endif

/*
 * Windows needs to know which symbols to export.  Unix does not.
 * BUILD_sqlite should be undefined for Unix.
 */
#ifdef BUILD_sqlite
#undef TCL_STORAGE_CLASS
#define TCL_STORAGE_CLASS DLLEXPORT
#endif /* BUILD_sqlite */

#define NUM_PREPARED_STMTS 10
#define MAX_PREPARED_STMTS 100

/* Forward declaration */
typedef struct SqliteDb SqliteDb;

/* Add -DSQLITE_ENABLE_QRF_IN_TCL to add the Query Result Formatter (QRF)
** into the build of the TCL extension, when building using separate
** source files.  The QRF is included automatically when building from
** the tclsqlite3.c amalgamation.
*/
#if defined(SQLITE_ENABLE_QRF_IN_TCL)
#include "qrf.h"
#endif

/*
** New SQL functions can be created as TCL scripts.  Each such function
** is described by an instance of the following structure.
**
** Variable eType may be set to SQLITE_INTEGER, SQLITE_FLOAT, SQLITE_TEXT,
** SQLITE_BLOB or SQLITE_NULL. If it is SQLITE_NULL, then the implementation
** attempts to determine the type of the result based on the Tcl object.
** If it is SQLITE_TEXT or SQLITE_BLOB, then a text (sqlite3_result_text())
** or blob (sqlite3_result_blob()) is returned. If it is SQLITE_INTEGER
** or SQLITE_FLOAT, then an attempt is made to return an integer or float
** value, falling back to float and then text if this is not possible.
*/
typedef struct SqlFunc SqlFunc;
struct SqlFunc {
  Tcl_Interp *interp;   /* The TCL interpret to execute the function */
  Tcl_Obj *pScript;     /* The Tcl_Obj representation of the script */
  SqliteDb *pDb;        /* Database connection that owns this function */
  int useEvalObjv;      /* True if it is safe to use Tcl_EvalObjv */
  int eType;            /* Type of value to return */
  char *zName;          /* Name of this function */
  SqlFunc *pNext;       /* Next function on the list of them all */
};

/*
** New collation sequences function can be created as TCL scripts.  Each such
** function is described by an instance of the following structure.
*/
typedef struct SqlCollate SqlCollate;
struct SqlCollate {
  Tcl_Interp *interp;   /* The TCL interpret to execute the function */
  char *zScript;        /* The script to be run */
  SqlCollate *pNext;    /* Next function on the list of them all */
};

/*
** Prepared statements are cached for faster execution.  Each prepared
** statement is described by an instance of the following structure.
*/
typedef struct SqlPreparedStmt SqlPreparedStmt;
struct SqlPreparedStmt {
  SqlPreparedStmt *pNext;  /* Next in linked list */
  SqlPreparedStmt *pPrev;  /* Previous on the list */
  sqlite3_stmt *pStmt;     /* The prepared statement */
  int nSql;                /* chars in zSql[] */
  const char *zSql;        /* Text of the SQL statement */
  int nParm;               /* Size of apParm array */
  Tcl_Obj **apParm;        /* Array of referenced object pointers */
};

typedef struct IncrblobChannel IncrblobChannel;

/*
** There is one instance of this structure for each SQLite database
** that has been opened by the SQLite TCL interface.
**
** If this module is built with SQLITE_TEST defined (to create the SQLite
** testfixture executable), then it may be configured to use either
** sqlite3_prepare_v2() or sqlite3_prepare() to prepare SQL statements.
** If SqliteDb.bLegacyPrepare is true, sqlite3_prepare() is used.
*/
struct SqliteDb {
  sqlite3 *db;               /* The "real" database structure. MUST BE FIRST */
  Tcl_Interp *interp;        /* The interpreter used for this database */
  char *zBusy;               /* The busy callback routine */
  char *zCommit;             /* The commit hook callback routine */
  char *zTrace;              /* The trace callback routine */
  char *zTraceV2;            /* The trace_v2 callback routine */
  char *zProfile;            /* The profile callback routine */
  char *zProgress;           /* The progress callback routine */
  char *zBindFallback;       /* Callback to invoke on a binding miss */
  char *zAuth;               /* The authorization callback routine */
  int disableAuth;           /* Disable the authorizer if it exists */
  char *zNull;               /* Text to substitute for an SQL NULL value */
  SqlFunc *pFunc;            /* List of SQL functions */
  Tcl_Obj *pUpdateHook;      /* Update hook script (if any) */
  Tcl_Obj *pPreUpdateHook;   /* Pre-update hook script (if any) */
  Tcl_Obj *pRollbackHook;    /* Rollback hook script (if any) */
  Tcl_Obj *pWalHook;         /* WAL hook script (if any) */
  Tcl_Obj *pUnlockNotify;    /* Unlock notify script (if any) */
  SqlCollate *pCollate;      /* List of SQL collation functions */
  int rc;                    /* Return code of most recent sqlite3_exec() */
  Tcl_Obj *pCollateNeeded;   /* Collation needed script */
  SqlPreparedStmt *stmtList; /* List of prepared statements*/
  SqlPreparedStmt *stmtLast; /* Last statement in the list */
  int maxStmt;               /* The next maximum number of stmtList */
  int nStmt;                 /* Number of statements in stmtList */
  IncrblobChannel *pIncrblob;/* Linked list of open incrblob channels */
  int nStep, nSort, nIndex;  /* Statistics for most recent operation */
  int nVMStep;               /* Another statistic for most recent operation */
  int nTransaction;          /* Number of nested [transaction] methods */
  int openFlags;             /* Flags used to open.  (SQLITE_OPEN_URI) */
  int nRef;                  /* Delete object when this reaches 0 */
#ifdef SQLITE_TEST
  int bLegacyPrepare;        /* True to use sqlite3_prepare() */
#endif
};

struct IncrblobChannel {
  sqlite3_blob *pBlob;      /* sqlite3 blob handle */
  SqliteDb *pDb;            /* Associated database connection */
  sqlite3_int64 iSeek;      /* Current seek offset */
  unsigned int isClosed;    /* TCL_CLOSE_READ or TCL_CLOSE_WRITE */
  Tcl_Channel channel;      /* Channel identifier */
  IncrblobChannel *pNext;   /* Linked list of all open incrblob channels */
  IncrblobChannel *pPrev;   /* Linked list of all open incrblob channels */
};

/*
** Compute a string length that is limited to what can be stored in
** lower 30 bits of a 32-bit signed integer.
*/
static int strlen30(const char *z){
  const char *z2 = z;
  while( *z2 ){ z2++; }
  return 0x3fffffff & (int)(z2 - z);
}


#ifndef SQLITE_OMIT_INCRBLOB
/*
** Close all incrblob channels opened using database connection pDb.
** This is called when shutting down the database connection.
*/
static void closeIncrblobChannels(SqliteDb *pDb){
  IncrblobChannel *p;
  IncrblobChannel *pNext;

  for(p=pDb->pIncrblob; p; p=pNext){
    pNext = p->pNext;

    /* Note: Calling unregister here call Tcl_Close on the incrblob channel,
    ** which deletes the IncrblobChannel structure at *p. So do not
    ** call Tcl_Free() here.
    */
    Tcl_UnregisterChannel(pDb->interp, p->channel);
  }
}

/*
** Close an incremental blob channel.
*/
static int SQLITE_TCLAPI incrblobClose2(
  ClientData instanceData,
  Tcl_Interp *interp,
  int flags
){
  IncrblobChannel *p = (IncrblobChannel *)instanceData;
  int  rc;
  sqlite3 *db = p->pDb->db;

  if( flags ){
    p->isClosed |= flags;
    return TCL_OK;
  }

  /* If we reach this point, then we really do need to close the channel */
  rc = sqlite3_blob_close(p->pBlob);

  /* Remove the channel from the SqliteDb.pIncrblob list. */
  if( p->pNext ){
    p->pNext->pPrev = p->pPrev;
  }
  if( p->pPrev ){
    p->pPrev->pNext = p->pNext;
  }
  if( p->pDb->pIncrblob==p ){
    p->pDb->pIncrblob = p->pNext;
  }

  /* Free the IncrblobChannel structure */
  Tcl_Free((char *)p);

  if( rc!=SQLITE_OK ){
    Tcl_SetResult(interp, (char *)sqlite3_errmsg(db), TCL_VOLATILE);
    return TCL_ERROR;
  }
  return TCL_OK;
}
static int SQLITE_TCLAPI incrblobClose(
  ClientData instanceData,
  Tcl_Interp *interp
){
  return incrblobClose2(instanceData, interp, 0);
}


/*
** Read data from an incremental blob channel.
*/
static int SQLITE_TCLAPI incrblobInput(
  ClientData instanceData,
  char *buf,
  int bufSize,
  int *errorCodePtr
){
  IncrblobChannel *p = (IncrblobChannel *)instanceData;
  sqlite3_int64 nRead = bufSize;   /* Number of bytes to read */
  sqlite3_int64 nBlob;             /* Total size of the blob */
  int rc;                          /* sqlite error code */

  nBlob = sqlite3_blob_bytes(p->pBlob);
  if( (p->iSeek+nRead)>nBlob ){
    nRead = nBlob-p->iSeek;
  }
  if( nRead<=0 ){
    return 0;
  }

  rc = sqlite3_blob_read(p->pBlob, (void *)buf, (int)nRead, (int)p->iSeek);
  if( rc!=SQLITE_OK ){
    *errorCodePtr = rc;
    return -1;
  }

  p->iSeek += nRead;
  return nRead;
}

/*
** Write data to an incremental blob channel.
*/
static int SQLITE_TCLAPI incrblobOutput(
  ClientData instanceData,
  const char *buf,
  int toWrite,
  int *errorCodePtr
){
  IncrblobChannel *p = (IncrblobChannel *)instanceData;
  sqlite3_int64 nWrite = toWrite;   /* Number of bytes to write */
  sqlite3_int64 nBlob;              /* Total size of the blob */
  int rc;                           /* sqlite error code */

  nBlob = sqlite3_blob_bytes(p->pBlob);
  if( (p->iSeek+nWrite)>nBlob ){
    *errorCodePtr = EINVAL;
    return -1;
  }
  if( nWrite<=0 ){
    return 0;
  }

  rc = sqlite3_blob_write(p->pBlob, (void*)buf,(int)nWrite, (int)p->iSeek);
  if( rc!=SQLITE_OK ){
    *errorCodePtr = EIO;
    return -1;
  }

  p->iSeek += nWrite;
  return nWrite;
}

/* The datatype of Tcl_DriverWideSeekProc changes between tcl8.6 and tcl9.0 */
#if TCL_MAJOR_VERSION==9
# define WideSeekProcType long long
#else
# define WideSeekProcType Tcl_WideInt
#endif

/*
** Seek an incremental blob channel.
*/
static WideSeekProcType SQLITE_TCLAPI incrblobWideSeek(
  ClientData instanceData,
  WideSeekProcType offset,
  int seekMode,
  int *errorCodePtr
){
  IncrblobChannel *p = (IncrblobChannel *)instanceData;

  switch( seekMode ){
    case SEEK_SET:
      p->iSeek = offset;
      break;
    case SEEK_CUR:
      p->iSeek += offset;
      break;
    case SEEK_END:
      p->iSeek = sqlite3_blob_bytes(p->pBlob) + offset;
      break;

    default: assert(!"Bad seekMode");
  }

  return p->iSeek;
}
static int SQLITE_TCLAPI incrblobSeek(
  ClientData instanceData,
  long offset,
  int seekMode,
  int *errorCodePtr
){
  return incrblobWideSeek(instanceData,offset,seekMode,errorCodePtr);
}


static void SQLITE_TCLAPI incrblobWatch(
  ClientData instanceData,
  int mode
){
  /* NO-OP */
}
static int SQLITE_TCLAPI incrblobHandle(
  ClientData instanceData,
  int dir,
  ClientData *hPtr
){
  return TCL_ERROR;
}

static Tcl_ChannelType IncrblobChannelType = {
  "incrblob",                        /* typeName                             */
  TCL_CHANNEL_VERSION_5,             /* version                              */
  incrblobClose,                     /* closeProc                            */
  incrblobInput,                     /* inputProc                            */
  incrblobOutput,                    /* outputProc                           */
  incrblobSeek,                      /* seekProc                             */
  0,                                 /* setOptionProc                        */
  0,                                 /* getOptionProc                        */
  incrblobWatch,                     /* watchProc (this is a no-op)          */
  incrblobHandle,                    /* getHandleProc (always returns error) */
  incrblobClose2,                    /* close2Proc                           */
  0,                                 /* blockModeProc                        */
  0,                                 /* flushProc                            */
  0,                                 /* handlerProc                          */
  incrblobWideSeek,                  /* wideSeekProc                         */
};

/*
** Create a new incrblob channel.
*/
static int createIncrblobChannel(
  Tcl_Interp *interp,
  SqliteDb *pDb,
  const char *zDb,
  const char *zTable,
  const char *zColumn,
  sqlite_int64 iRow,
  int isReadonly
){
  IncrblobChannel *p;
  sqlite3 *db = pDb->db;
  sqlite3_blob *pBlob;
  int rc;
  int flags = TCL_READABLE|(isReadonly ? 0 : TCL_WRITABLE);

  /* This variable is used to name the channels: "incrblob_[incr count]" */
  static int count = 0;
  char zChannel[64];

  rc = sqlite3_blob_open(db, zDb, zTable, zColumn, iRow, !isReadonly, &pBlob);
  if( rc!=SQLITE_OK ){
    Tcl_SetResult(interp, (char *)sqlite3_errmsg(pDb->db), TCL_VOLATILE);
    return TCL_ERROR;
  }

  p = (IncrblobChannel *)Tcl_Alloc(sizeof(IncrblobChannel));
  memset(p, 0, sizeof(*p));
  p->pBlob = pBlob;
  if( (flags & TCL_WRITABLE)==0 ) p->isClosed |= TCL_CLOSE_WRITE;

  sqlite3_snprintf(sizeof(zChannel), zChannel, "incrblob_%d", ++count);
  p->channel = Tcl_CreateChannel(&IncrblobChannelType, zChannel, p, flags);
  Tcl_RegisterChannel(interp, p->channel);

  /* Link the new channel into the SqliteDb.pIncrblob list. */
  p->pNext = pDb->pIncrblob;
  p->pPrev = 0;
  if( p->pNext ){
    p->pNext->pPrev = p;
  }
  pDb->pIncrblob = p;
  p->pDb = pDb;

  Tcl_SetResult(interp, (char *)Tcl_GetChannelName(p->channel), TCL_VOLATILE);
  return TCL_OK;
}
#else  /* else clause for "#ifndef SQLITE_OMIT_INCRBLOB" */
  #define closeIncrblobChannels(pDb)
#endif

/*
** Look at the script prefix in pCmd.  We will be executing this script
** after first appending one or more arguments.  This routine analyzes
** the script to see if it is safe to use Tcl_EvalObjv() on the script
** rather than the more general Tcl_EvalEx().  Tcl_EvalObjv() is much
** faster.
**
** Scripts that are safe to use with Tcl_EvalObjv() consists of a
** command name followed by zero or more arguments with no [...] or $
** or {...} or ; to be seen anywhere.  Most callback scripts consist
** of just a single procedure name and they meet this requirement.
*/
static int safeToUseEvalObjv(Tcl_Obj *pCmd){
  /* We could try to do something with Tcl_Parse().  But we will instead
  ** just do a search for forbidden characters.  If any of the forbidden
  ** characters appear in pCmd, we will report the string as unsafe.
  */
  const char *z;
  Tcl_Size n;
  z = Tcl_GetStringFromObj(pCmd, &n);
  while( n-- > 0 ){
    int c = *(z++);
    if( c=='$' || c=='[' || c==';' ) return 0;
  }
  return 1;
}

/*
** Find an SqlFunc structure with the given name.  Or create a new
** one if an existing one cannot be found.  Return a pointer to the
** structure.
*/
static SqlFunc *findSqlFunc(SqliteDb *pDb, const char *zName){
  SqlFunc *p, *pNew;
  int nName = strlen30(zName);
  pNew = (SqlFunc*)Tcl_Alloc( sizeof(*pNew) + nName + 1 );
  pNew->zName = (char*)&pNew[1];
  memcpy(pNew->zName, zName, nName+1);
  for(p=pDb->pFunc; p; p=p->pNext){
    if( sqlite3_stricmp(p->zName, pNew->zName)==0 ){
      Tcl_Free((char*)pNew);
      return p;
    }
  }
  pNew->interp = pDb->interp;
  pNew->pDb = pDb;
  pNew->pScript = 0;
  pNew->pNext = pDb->pFunc;
  pDb->pFunc = pNew;
  return pNew;
}

/*
** Free a single SqlPreparedStmt object.
*/
static void dbFreeStmt(SqlPreparedStmt *pStmt){
#ifdef SQLITE_TEST
  if( sqlite3_sql(pStmt->pStmt)==0 ){
    Tcl_Free((char *)pStmt->zSql);
  }
#endif
  sqlite3_finalize(pStmt->pStmt);
  Tcl_Free((char *)pStmt);
}

/*
** Finalize and free a list of prepared statements
*/
static void flushStmtCache(SqliteDb *pDb){
  SqlPreparedStmt *pPreStmt;
  SqlPreparedStmt *pNext;

  for(pPreStmt = pDb->stmtList; pPreStmt; pPreStmt=pNext){
    pNext = pPreStmt->pNext;
    dbFreeStmt(pPreStmt);
  }
  pDb->nStmt = 0;
  pDb->stmtLast = 0;
  pDb->stmtList = 0;
}

/*
** Increment the reference counter on the SqliteDb object. The reference
** should be released by calling delDatabaseRef().
*/
static void addDatabaseRef(SqliteDb *pDb){
  pDb->nRef++;
}

/*
** Decrement the reference counter associated with the SqliteDb object.
** If it reaches zero, delete the object.
*/
static void delDatabaseRef(SqliteDb *pDb){
  assert( pDb->nRef>0 );
  pDb->nRef--;
  if( pDb->nRef==0 ){
    flushStmtCache(pDb);
    closeIncrblobChannels(pDb);
    sqlite3_close(pDb->db);
    while( pDb->pFunc ){
      SqlFunc *pFunc = pDb->pFunc;
      pDb->pFunc = pFunc->pNext;
      assert( pFunc->pDb==pDb );
      Tcl_DecrRefCount(pFunc->pScript);
      Tcl_Free((char*)pFunc);
    }
    while( pDb->pCollate ){
      SqlCollate *pCollate = pDb->pCollate;
      pDb->pCollate = pCollate->pNext;
      Tcl_Free((char*)pCollate);
    }
    if( pDb->zBusy ){
      Tcl_Free(pDb->zBusy);
    }
    if( pDb->zTrace ){
      Tcl_Free(pDb->zTrace);
    }
    if( pDb->zTraceV2 ){
      Tcl_Free(pDb->zTraceV2);
    }
    if( pDb->zProfile ){
      Tcl_Free(pDb->zProfile);
    }
    if( pDb->zBindFallback ){
      Tcl_Free(pDb->zBindFallback);
    }
    if( pDb->zAuth ){
      Tcl_Free(pDb->zAuth);
    }
    if( pDb->zNull ){
      Tcl_Free(pDb->zNull);
    }
    if( pDb->pUpdateHook ){
      Tcl_DecrRefCount(pDb->pUpdateHook);
    }
    if( pDb->pPreUpdateHook ){
      Tcl_DecrRefCount(pDb->pPreUpdateHook);
    }
    if( pDb->pRollbackHook ){
      Tcl_DecrRefCount(pDb->pRollbackHook);
    }
    if( pDb->pWalHook ){
      Tcl_DecrRefCount(pDb->pWalHook);
    }
    if( pDb->pCollateNeeded ){
      Tcl_DecrRefCount(pDb->pCollateNeeded);
    }
    Tcl_Free((char*)pDb);
  }
}

/*
** TCL calls this procedure when an sqlite3 database command is
** deleted.
*/
static void SQLITE_TCLAPI DbDeleteCmd(void *db){
  SqliteDb *pDb = (SqliteDb*)db;
  delDatabaseRef(pDb);
}

/*
** This routine is called when a database file is locked while trying
** to execute SQL.
*/
static int DbBusyHandler(void *cd, int nTries){
  SqliteDb *pDb = (SqliteDb*)cd;
  int rc;
  char zVal[30];

  sqlite3_snprintf(sizeof(zVal), zVal, "%d", nTries);
  rc = Tcl_VarEval(pDb->interp, pDb->zBusy, " ", zVal, (char*)0);
  if( rc!=TCL_OK || atoi(Tcl_GetStringResult(pDb->interp)) ){
    return 0;
  }
  return 1;
}

#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
/*
** This routine is invoked as the 'progress callback' for the database.
*/
static int DbProgressHandler(void *cd){
  SqliteDb *pDb = (SqliteDb*)cd;
  int rc;

  assert( pDb->zProgress );
  rc = Tcl_Eval(pDb->interp, pDb->zProgress);
  if( rc!=TCL_OK || atoi(Tcl_GetStringResult(pDb->interp)) ){
    return 1;
  }
  return 0;
}
#endif

#if !defined(SQLITE_OMIT_TRACE) && !defined(SQLITE_OMIT_FLOATING_POINT) && \
    !defined(SQLITE_OMIT_DEPRECATED)
/*
** This routine is called by the SQLite trace handler whenever a new
** block of SQL is executed.  The TCL script in pDb->zTrace is executed.
*/
static void DbTraceHandler(void *cd, const char *zSql){
  SqliteDb *pDb = (SqliteDb*)cd;
  Tcl_DString str;

  Tcl_DStringInit(&str);
  Tcl_DStringAppend(&str, pDb->zTrace, -1);
  Tcl_DStringAppendElement(&str, zSql);
  Tcl_Eval(pDb->interp, Tcl_DStringValue(&str));
  Tcl_DStringFree(&str);
  Tcl_ResetResult(pDb->interp);
}
#endif

#ifndef SQLITE_OMIT_TRACE
/*
** This routine is called by the SQLite trace_v2 handler whenever a new
** supported event is generated.  Unsupported event types are ignored.
** The TCL script in pDb->zTraceV2 is executed, with the arguments for
** the event appended to it (as list elements).
*/
static int DbTraceV2Handler(
  unsigned type, /* One of the SQLITE_TRACE_* event types. */
  void *cd,      /* The original context data pointer. */
  void *pd,      /* Primary event data, depends on event type. */
  void *xd       /* Extra event data, depends on event type. */
){
  SqliteDb *pDb = (SqliteDb*)cd;
  Tcl_Obj *pCmd;

  switch( type ){
    case SQLITE_TRACE_STMT: {
      sqlite3_stmt *pStmt = (sqlite3_stmt *)pd;
      char *zSql = (char *)xd;

      pCmd = Tcl_NewStringObj(pDb->zTraceV2, -1);
      Tcl_IncrRefCount(pCmd);
      Tcl_ListObjAppendElement(pDb->interp, pCmd,
                               Tcl_NewWideIntObj((Tcl_WideInt)(uptr)pStmt));
      Tcl_ListObjAppendElement(pDb->interp, pCmd,
                               Tcl_NewStringObj(zSql, -1));
      Tcl_EvalObjEx(pDb->interp, pCmd, TCL_EVAL_DIRECT);
      Tcl_DecrRefCount(pCmd);
      Tcl_ResetResult(pDb->interp);
      break;
    }
    case SQLITE_TRACE_PROFILE: {
      sqlite3_stmt *pStmt = (sqlite3_stmt *)pd;
      sqlite3_int64 ns = *(sqlite3_int64*)xd;

      pCmd = Tcl_NewStringObj(pDb->zTraceV2, -1);
      Tcl_IncrRefCount(pCmd);
      Tcl_ListObjAppendElement(pDb->interp, pCmd,
                               Tcl_NewWideIntObj((Tcl_WideInt)(uptr)pStmt));
      Tcl_ListObjAppendElement(pDb->interp, pCmd,
                               Tcl_NewWideIntObj((Tcl_WideInt)ns));
      Tcl_EvalObjEx(pDb->interp, pCmd, TCL_EVAL_DIRECT);
      Tcl_DecrRefCount(pCmd);
      Tcl_ResetResult(pDb->interp);
      break;
    }
    case SQLITE_TRACE_ROW: {
      sqlite3_stmt *pStmt = (sqlite3_stmt *)pd;

      pCmd = Tcl_NewStringObj(pDb->zTraceV2, -1);
      Tcl_IncrRefCount(pCmd);
      Tcl_ListObjAppendElement(pDb->interp, pCmd,
                               Tcl_NewWideIntObj((Tcl_WideInt)(uptr)pStmt));
      Tcl_EvalObjEx(pDb->interp, pCmd, TCL_EVAL_DIRECT);
      Tcl_DecrRefCount(pCmd);
      Tcl_ResetResult(pDb->interp);
      break;
    }
    case SQLITE_TRACE_CLOSE: {
      sqlite3 *db = (sqlite3 *)pd;

      pCmd = Tcl_NewStringObj(pDb->zTraceV2, -1);
      Tcl_IncrRefCount(pCmd);
      Tcl_ListObjAppendElement(pDb->interp, pCmd,
                               Tcl_NewWideIntObj((Tcl_WideInt)(uptr)db));
      Tcl_EvalObjEx(pDb->interp, pCmd, TCL_EVAL_DIRECT);
      Tcl_DecrRefCount(pCmd);
      Tcl_ResetResult(pDb->interp);
      break;
    }
  }
  return SQLITE_OK;
}
#endif

#if !defined(SQLITE_OMIT_TRACE) && !defined(SQLITE_OMIT_FLOATING_POINT) && \
    !defined(SQLITE_OMIT_DEPRECATED)
/*
** This routine is called by the SQLite profile handler after a statement
** SQL has executed.  The TCL script in pDb->zProfile is evaluated.
*/
static void DbProfileHandler(void *cd, const char *zSql, sqlite_uint64 tm){
  SqliteDb *pDb = (SqliteDb*)cd;
  Tcl_DString str;
  char zTm[100];

  sqlite3_snprintf(sizeof(zTm)-1, zTm, "%lld", tm);
  Tcl_DStringInit(&str);
  Tcl_DStringAppend(&str, pDb->zProfile, -1);
  Tcl_DStringAppendElement(&str, zSql);
  Tcl_DStringAppendElement(&str, zTm);
  Tcl_Eval(pDb->interp, Tcl_DStringValue(&str));
  Tcl_DStringFree(&str);
  Tcl_ResetResult(pDb->interp);
}
#endif

/*
** This routine is called when a transaction is committed.  The
** TCL script in pDb->zCommit is executed.  If it returns non-zero or
** if it throws an exception, the transaction is rolled back instead
** of being committed.
*/
static int DbCommitHandler(void *cd){
  SqliteDb *pDb = (SqliteDb*)cd;
  int rc;

  rc = Tcl_Eval(pDb->interp, pDb->zCommit);
  if( rc!=TCL_OK || atoi(Tcl_GetStringResult(pDb->interp)) ){
    return 1;
  }
  return 0;
}

static void DbRollbackHandler(void *clientData){
  SqliteDb *pDb = (SqliteDb*)clientData;
  assert(pDb->pRollbackHook);
  if( TCL_OK!=Tcl_EvalObjEx(pDb->interp, pDb->pRollbackHook, 0) ){
    Tcl_BackgroundError(pDb->interp);
  }
}

/*
** This procedure handles wal_hook callbacks.
*/
static int DbWalHandler(
  void *clientData,
  sqlite3 *db,
  const char *zDb,
  int nEntry
){
  int ret = SQLITE_OK;
  Tcl_Obj *p;
  SqliteDb *pDb = (SqliteDb*)clientData;
  Tcl_Interp *interp = pDb->interp;
  assert(pDb->pWalHook);

  assert( db==pDb->db );
  p = Tcl_DuplicateObj(pDb->pWalHook);
  Tcl_IncrRefCount(p);
  Tcl_ListObjAppendElement(interp, p, Tcl_NewStringObj(zDb, -1));
  Tcl_ListObjAppendElement(interp, p, Tcl_NewIntObj(nEntry));
  if( TCL_OK!=Tcl_EvalObjEx(interp, p, 0)
   || TCL_OK!=Tcl_GetIntFromObj(interp, Tcl_GetObjResult(interp), &ret)
  ){
    Tcl_BackgroundError(interp);
  }
  Tcl_DecrRefCount(p);

  return ret;
}

#if defined(SQLITE_TEST) && defined(SQLITE_ENABLE_UNLOCK_NOTIFY)
static void setTestUnlockNotifyVars(Tcl_Interp *interp, int iArg, int nArg){
  char zBuf[64];
  sqlite3_snprintf(sizeof(zBuf), zBuf, "%d", iArg);
  Tcl_SetVar(interp, "sqlite_unlock_notify_arg", zBuf, TCL_GLOBAL_ONLY);
  sqlite3_snprintf(sizeof(zBuf), zBuf, "%d", nArg);
  Tcl_SetVar(interp, "sqlite_unlock_notify_argcount", zBuf, TCL_GLOBAL_ONLY);
}
#else
# define setTestUnlockNotifyVars(x,y,z)
#endif

#ifdef SQLITE_ENABLE_UNLOCK_NOTIFY
static void DbUnlockNotify(void **apArg, int nArg){
  int i;
  for(i=0; i<nArg; i++){
    const int flags = (TCL_EVAL_GLOBAL|TCL_EVAL_DIRECT);
    SqliteDb *pDb = (SqliteDb *)apArg[i];
    setTestUnlockNotifyVars(pDb->interp, i, nArg);
    assert( pDb->pUnlockNotify);
    Tcl_EvalObjEx(pDb->interp, pDb->pUnlockNotify, flags);
    Tcl_DecrRefCount(pDb->pUnlockNotify);
    pDb->pUnlockNotify = 0;
  }
}
#endif

#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
/*
** Pre-update hook callback.
*/
static void DbPreUpdateHandler(
  void *p,
  sqlite3 *db,
  int op,
  const char *zDb,
  const char *zTbl,
  sqlite_int64 iKey1,
  sqlite_int64 iKey2
){
  SqliteDb *pDb = (SqliteDb *)p;
  Tcl_Obj *pCmd;
  static const char *azStr[] = {"DELETE", "INSERT", "UPDATE"};

  assert( (SQLITE_DELETE-1)/9 == 0 );
  assert( (SQLITE_INSERT-1)/9 == 1 );
  assert( (SQLITE_UPDATE-1)/9 == 2 );
  assert( pDb->pPreUpdateHook );
  assert( db==pDb->db );
  assert( op==SQLITE_INSERT || op==SQLITE_UPDATE || op==SQLITE_DELETE );

  pCmd = Tcl_DuplicateObj(pDb->pPreUpdateHook);
  Tcl_IncrRefCount(pCmd);
  Tcl_ListObjAppendElement(0, pCmd, Tcl_NewStringObj(azStr[(op-1)/9], -1));
  Tcl_ListObjAppendElement(0, pCmd, Tcl_NewStringObj(zDb, -1));
  Tcl_ListObjAppendElement(0, pCmd, Tcl_NewStringObj(zTbl, -1));
  Tcl_ListObjAppendElement(0, pCmd, Tcl_NewWideIntObj(iKey1));
  Tcl_ListObjAppendElement(0, pCmd, Tcl_NewWideIntObj(iKey2));
  Tcl_EvalObjEx(pDb->interp, pCmd, TCL_EVAL_DIRECT);
  Tcl_DecrRefCount(pCmd);
}
#endif /* SQLITE_ENABLE_PREUPDATE_HOOK */

static void DbUpdateHandler(
  void *p,
  int op,
  const char *zDb,
  const char *zTbl,
  sqlite_int64 rowid
){
  SqliteDb *pDb = (SqliteDb *)p;
  Tcl_Obj *pCmd;
  static const char *azStr[] = {"DELETE", "INSERT", "UPDATE"};

  assert( (SQLITE_DELETE-1)/9 == 0 );
  assert( (SQLITE_INSERT-1)/9 == 1 );
  assert( (SQLITE_UPDATE-1)/9 == 2 );

  assert( pDb->pUpdateHook );
  assert( op==SQLITE_INSERT || op==SQLITE_UPDATE || op==SQLITE_DELETE );

  pCmd = Tcl_DuplicateObj(pDb->pUpdateHook);
  Tcl_IncrRefCount(pCmd);
  Tcl_ListObjAppendElement(0, pCmd, Tcl_NewStringObj(azStr[(op-1)/9], -1));
  Tcl_ListObjAppendElement(0, pCmd, Tcl_NewStringObj(zDb, -1));
  Tcl_ListObjAppendElement(0, pCmd, Tcl_NewStringObj(zTbl, -1));
  Tcl_ListObjAppendElement(0, pCmd, Tcl_NewWideIntObj(rowid));
  Tcl_EvalObjEx(pDb->interp, pCmd, TCL_EVAL_DIRECT);
  Tcl_DecrRefCount(pCmd);
}

static void tclCollateNeeded(
  void *pCtx,
  sqlite3 *db,
  int enc,
  const char *zName
){
  SqliteDb *pDb = (SqliteDb *)pCtx;
  Tcl_Obj *pScript = Tcl_DuplicateObj(pDb->pCollateNeeded);
  Tcl_IncrRefCount(pScript);
  Tcl_ListObjAppendElement(0, pScript, Tcl_NewStringObj(zName, -1));
  Tcl_EvalObjEx(pDb->interp, pScript, 0);
  Tcl_DecrRefCount(pScript);
}

/*
** This routine is called to evaluate an SQL collation function implemented
** using TCL script.
*/
static int tclSqlCollate(
  void *pCtx,
  int nA,
  const void *zA,
  int nB,
  const void *zB
){
  SqlCollate *p = (SqlCollate *)pCtx;
  Tcl_Obj *pCmd;

  pCmd = Tcl_NewStringObj(p->zScript, -1);
  Tcl_IncrRefCount(pCmd);
  Tcl_ListObjAppendElement(p->interp, pCmd, Tcl_NewStringObj(zA, nA));
  Tcl_ListObjAppendElement(p->interp, pCmd, Tcl_NewStringObj(zB, nB));
  Tcl_EvalObjEx(p->interp, pCmd, TCL_EVAL_DIRECT);
  Tcl_DecrRefCount(pCmd);
  return (atoi(Tcl_GetStringResult(p->interp)));
}

/*
** This routine is called to evaluate an SQL function implemented
** using TCL script.
*/
static void tclSqlFunc(sqlite3_context *context, int argc, sqlite3_value**argv){
  SqlFunc *p = sqlite3_user_data(context);
  Tcl_Obj *pCmd;
  int i;
  int rc;

  if( argc==0 ){
    /* If there are no arguments to the function, call Tcl_EvalObjEx on the
    ** script object directly.  This allows the TCL compiler to generate
    ** bytecode for the command on the first invocation and thus make
    ** subsequent invocations much faster. */
    pCmd = p->pScript;
    Tcl_IncrRefCount(pCmd);
    rc = Tcl_EvalObjEx(p->interp, pCmd, 0);
    Tcl_DecrRefCount(pCmd);
  }else{
    /* If there are arguments to the function, make a shallow copy of the
    ** script object, lappend the arguments, then evaluate the copy.
    **
    ** By "shallow" copy, we mean only the outer list Tcl_Obj is duplicated.
    ** The new Tcl_Obj contains pointers to the original list elements.
    ** That way, when Tcl_EvalObjv() is run and shimmers the first element
    ** of the list to tclCmdNameType, that alternate representation will
    ** be preserved and reused on the next invocation.
    */
    Tcl_Obj **aArg;
    Tcl_Size nArg;
    if( Tcl_ListObjGetElements(p->interp, p->pScript, &nArg, &aArg) ){
      sqlite3_result_error(context, Tcl_GetStringResult(p->interp), -1);
      return;
    }
    pCmd = Tcl_NewListObj(nArg, aArg);
    Tcl_IncrRefCount(pCmd);
    for(i=0; i<argc; i++){
      sqlite3_value *pIn = argv[i];
      Tcl_Obj *pVal;

      /* Set pVal to contain the i'th column of this row. */
      switch( sqlite3_value_type(pIn) ){
        case SQLITE_BLOB: {
          int bytes = sqlite3_value_bytes(pIn);
          pVal = Tcl_NewByteArrayObj(sqlite3_value_blob(pIn), bytes);
          break;
        }
        case SQLITE_INTEGER: {
          sqlite_int64 v = sqlite3_value_int64(pIn);
          if( v>=-2147483647 && v<=2147483647 ){
            pVal = Tcl_NewIntObj((int)v);
          }else{
            pVal = Tcl_NewWideIntObj(v);
          }
          break;
        }
        case SQLITE_FLOAT: {
          double r = sqlite3_value_double(pIn);
          pVal = Tcl_NewDoubleObj(r);
          break;
        }
        case SQLITE_NULL: {
          pVal = Tcl_NewStringObj(p->pDb->zNull, -1);
          break;
        }
        default: {
          int bytes = sqlite3_value_bytes(pIn);
          pVal = Tcl_NewStringObj((char *)sqlite3_value_text(pIn), bytes);
          break;
        }
      }
      rc = Tcl_ListObjAppendElement(p->interp, pCmd, pVal);
      if( rc ){
        Tcl_DecrRefCount(pCmd);
        sqlite3_result_error(context, Tcl_GetStringResult(p->interp), -1);
        return;
      }
    }
    if( !p->useEvalObjv ){
      /* Tcl_EvalObjEx() will automatically call Tcl_EvalObjv() if pCmd
      ** is a list without a string representation.  To prevent this from
      ** happening, make sure pCmd has a valid string representation */
      Tcl_GetString(pCmd);
    }
    rc = Tcl_EvalObjEx(p->interp, pCmd, TCL_EVAL_DIRECT);
    Tcl_DecrRefCount(pCmd);
  }

  if( TCL_BREAK==rc ){
    sqlite3_result_null(context);
  }else if( rc && rc!=TCL_RETURN ){
    sqlite3_result_error(context, Tcl_GetStringResult(p->interp), -1);
  }else{
    Tcl_Obj *pVar = Tcl_GetObjResult(p->interp);
    Tcl_Size n;
    u8 *data;
    const char *zType = (pVar->typePtr ? pVar->typePtr->name : "");
    char c = zType[0];
    int eType = p->eType;

    if( eType==SQLITE_NULL ){
      if( c=='b' && strcmp(zType,"bytearray")==0 && pVar->bytes==0 ){
        /* Only return a BLOB type if the Tcl variable is a bytearray and
        ** has no string representation. */
        eType = SQLITE_BLOB;
      }else if( (c=='b' && pVar->bytes==0 && strcmp(zType,"boolean")==0 )
             || (c=='b' && pVar->bytes==0 && strcmp(zType,"booleanString")==0 )
             || (c=='w' && strcmp(zType,"wideInt")==0)
             || (c=='i' && strcmp(zType,"int")==0)
      ){
        eType = SQLITE_INTEGER;
      }else if( c=='d' && strcmp(zType,"double")==0 ){
        eType = SQLITE_FLOAT;
      }else{
        eType = SQLITE_TEXT;
      }
    }

    switch( eType ){
      case SQLITE_BLOB: {
        data = Tcl_GetByteArrayFromObj(pVar, &n);
        sqlite3_result_blob(context, data, n, SQLITE_TRANSIENT);
        break;
      }
      case SQLITE_INTEGER: {
        Tcl_WideInt v;
        if( TCL_OK==Tcl_GetWideIntFromObj(0, pVar, &v) ){
          sqlite3_result_int64(context, v);
          break;
        }
        /* fall-through */
      }
      case SQLITE_FLOAT: {
        double r;
        if( TCL_OK==Tcl_GetDoubleFromObj(0, pVar, &r) ){
          sqlite3_result_double(context, r);
          break;
        }
        /* fall-through */
      }
      default: {
        data = (unsigned char *)Tcl_GetStringFromObj(pVar, &n);
        sqlite3_result_text64(context, (char *)data, n, SQLITE_TRANSIENT,
                              SQLITE_UTF8);
        break;
      }
    }

  }
}

#ifndef SQLITE_OMIT_AUTHORIZATION
/*
** This is the authentication function.  It appends the authentication
** type code and the two arguments to zCmd[] then invokes the result
** on the interpreter.  The reply is examined to determine if the
** authentication fails or succeeds.
*/
static int auth_callback(
  void *pArg,
  int code,
  const char *zArg1,
  const char *zArg2,
  const char *zArg3,
  const char *zArg4
){
  const char *zCode;
  Tcl_DString str;
  int rc;
  const char *zReply;
  /* EVIDENCE-OF: R-38590-62769 The first parameter to the authorizer
  ** callback is a copy of the third parameter to the
  ** sqlite3_set_authorizer() interface.
  */
  SqliteDb *pDb = (SqliteDb*)pArg;
  if( pDb->disableAuth ) return SQLITE_OK;

  /* EVIDENCE-OF: R-56518-44310 The second parameter to the callback is an
  ** integer action code that specifies the particular action to be
  ** authorized. */
  switch( code ){
    case SQLITE_COPY              : zCode="SQLITE_COPY"; break;
    case SQLITE_CREATE_INDEX      : zCode="SQLITE_CREATE_INDEX"; break;
    case SQLITE_CREATE_TABLE      : zCode="SQLITE_CREATE_TABLE"; break;
    case SQLITE_CREATE_TEMP_INDEX : zCode="SQLITE_CREATE_TEMP_INDEX"; break;
    case SQLITE_CREATE_TEMP_TABLE : zCode="SQLITE_CREATE_TEMP_TABLE"; break;
    case SQLITE_CREATE_TEMP_TRIGGER: zCode="SQLITE_CREATE_TEMP_TRIGGER"; break;
    case SQLITE_CREATE_TEMP_VIEW  : zCode="SQLITE_CREATE_TEMP_VIEW"; break;
    case SQLITE_CREATE_TRIGGER    : zCode="SQLITE_CREATE_TRIGGER"; break;
    case SQLITE_CREATE_VIEW       : zCode="SQLITE_CREATE_VIEW"; break;
    case SQLITE_DELETE            : zCode="SQLITE_DELETE"; break;
    case SQLITE_DROP_INDEX        : zCode="SQLITE_DROP_INDEX"; break;
    case SQLITE_DROP_TABLE        : zCode="SQLITE_DROP_TABLE"; break;
    case SQLITE_DROP_TEMP_INDEX   : zCode="SQLITE_DROP_TEMP_INDEX"; break;
    case SQLITE_DROP_TEMP_TABLE   : zCode="SQLITE_DROP_TEMP_TABLE"; break;
    case SQLITE_DROP_TEMP_TRIGGER : zCode="SQLITE_DROP_TEMP_TRIGGER"; break;
    case SQLITE_DROP_TEMP_VIEW    : zCode="SQLITE_DROP_TEMP_VIEW"; break;
    case SQLITE_DROP_TRIGGER      : zCode="SQLITE_DROP_TRIGGER"; break;
    case SQLITE_DROP_VIEW         : zCode="SQLITE_DROP_VIEW"; break;
    case SQLITE_INSERT            : zCode="SQLITE_INSERT"; break;
    case SQLITE_PRAGMA            : zCode="SQLITE_PRAGMA"; break;
    case SQLITE_READ              : zCode="SQLITE_READ"; break;
    case SQLITE_SELECT            : zCode="SQLITE_SELECT"; break;
    case SQLITE_TRANSACTION       : zCode="SQLITE_TRANSACTION"; break;
    case SQLITE_UPDATE            : zCode="SQLITE_UPDATE"; break;
    case SQLITE_ATTACH            : zCode="SQLITE_ATTACH"; break;
    case SQLITE_DETACH            : zCode="SQLITE_DETACH"; break;
    case SQLITE_ALTER_TABLE       : zCode="SQLITE_ALTER_TABLE"; break;
    case SQLITE_REINDEX           : zCode="SQLITE_REINDEX"; break;
    case SQLITE_ANALYZE           : zCode="SQLITE_ANALYZE"; break;
    case SQLITE_CREATE_VTABLE     : zCode="SQLITE_CREATE_VTABLE"; break;
    case SQLITE_DROP_VTABLE       : zCode="SQLITE_DROP_VTABLE"; break;
    case SQLITE_FUNCTION          : zCode="SQLITE_FUNCTION"; break;
    case SQLITE_SAVEPOINT         : zCode="SQLITE_SAVEPOINT"; break;
    case SQLITE_RECURSIVE         : zCode="SQLITE_RECURSIVE"; break;
    default                       : zCode="????"; break;
  }
  Tcl_DStringInit(&str);
  Tcl_DStringAppend(&str, pDb->zAuth, -1);
  Tcl_DStringAppendElement(&str, zCode);
  Tcl_DStringAppendElement(&str, zArg1 ? zArg1 : "");
  Tcl_DStringAppendElement(&str, zArg2 ? zArg2 : "");
  Tcl_DStringAppendElement(&str, zArg3 ? zArg3 : "");
  Tcl_DStringAppendElement(&str, zArg4 ? zArg4 : "");
  rc = Tcl_GlobalEval(pDb->interp, Tcl_DStringValue(&str));
  Tcl_DStringFree(&str);
  zReply = rc==TCL_OK ? Tcl_GetStringResult(pDb->interp) : "SQLITE_DENY";
  if( strcmp(zReply,"SQLITE_OK")==0 ){
    rc = SQLITE_OK;
  }else if( strcmp(zReply,"SQLITE_DENY")==0 ){
    rc = SQLITE_DENY;
  }else if( strcmp(zReply,"SQLITE_IGNORE")==0 ){
    rc = SQLITE_IGNORE;
  }else{
    rc = 999;
  }
  return rc;
}
#endif /* SQLITE_OMIT_AUTHORIZATION */

#if 0
/*
** This routine reads a line of text from FILE in, stores
** the text in memory obtained from malloc() and returns a pointer
** to the text.  NULL is returned at end of file, or if malloc()
** fails.
**
** The interface is like "readline" but no command-line editing
** is done.
**
** copied from shell.c from '.import' command
*/
static char *local_getline(char *zPrompt, FILE *in){
  char *zLine;
  int nLine;
  int n;

  nLine = 100;
  zLine = malloc( nLine );
  if( zLine==0 ) return 0;
  n = 0;
  while( 1 ){
    if( n+100>nLine ){
      nLine = nLine*2 + 100;
      zLine = realloc(zLine, nLine);
      if( zLine==0 ) return 0;
    }
    if( fgets(&zLine[n], nLine - n, in)==0 ){
      if( n==0 ){
        free(zLine);
        return 0;
      }
      zLine[n] = 0;
      break;
    }
    while( zLine[n] ){ n++; }
    if( n>0 && zLine[n-1]=='\n' ){
      n--;
      zLine[n] = 0;
      break;
    }
  }
  zLine = realloc( zLine, n+1 );
  return zLine;
}
#endif


/*
** This function is part of the implementation of the command:
**
**   $db transaction [-deferred|-immediate|-exclusive] SCRIPT
**
** It is invoked after evaluating the script SCRIPT to commit or rollback
** the transaction or savepoint opened by the [transaction] command.
*/
static int SQLITE_TCLAPI DbTransPostCmd(
  ClientData data[],                   /* data[0] is the Sqlite3Db* for $db */
  Tcl_Interp *interp,                  /* Tcl interpreter */
  int result                           /* Result of evaluating SCRIPT */
){
  static const char *const azEnd[] = {
    "RELEASE _tcl_transaction",        /* rc==TCL_ERROR, nTransaction!=0 */
    "COMMIT",                          /* rc!=TCL_ERROR, nTransaction==0 */
    "ROLLBACK TO _tcl_transaction ; RELEASE _tcl_transaction",
    "ROLLBACK"                         /* rc==TCL_ERROR, nTransaction==0 */
  };
  SqliteDb *pDb = (SqliteDb*)data[0];
  int rc = result;
  const char *zEnd;

  pDb->nTransaction--;
  zEnd = azEnd[(rc==TCL_ERROR)*2 + (pDb->nTransaction==0)];

  pDb->disableAuth++;
  if( sqlite3_exec(pDb->db, zEnd, 0, 0, 0) ){
      /* This is a tricky scenario to handle. The most likely cause of an
      ** error is that the exec() above was an attempt to commit the
      ** top-level transaction that returned SQLITE_BUSY. Or, less likely,
      ** that an IO-error has occurred. In either case, throw a Tcl exception
      ** and try to rollback the transaction.
      **
      ** But it could also be that the user executed one or more BEGIN,
      ** COMMIT, SAVEPOINT, RELEASE or ROLLBACK commands that are confusing
      ** this method's logic. Not clear how this would be best handled.
      */
    if( rc!=TCL_ERROR ){
      Tcl_AppendResult(interp, sqlite3_errmsg(pDb->db), (char*)0);
      rc = TCL_ERROR;
    }
    sqlite3_exec(pDb->db, "ROLLBACK", 0, 0, 0);
  }
  pDb->disableAuth--;

  delDatabaseRef(pDb);
  return rc;
}

/*
** Unless SQLITE_TEST is defined, this function is a simple wrapper around
** sqlite3_prepare_v2(). If SQLITE_TEST is defined, then it uses either
** sqlite3_prepare_v2() or legacy interface sqlite3_prepare(), depending
** on whether or not the [db_use_legacy_prepare] command has been used to
** configure the connection.
*/
static int dbPrepare(
  SqliteDb *pDb,                  /* Database object */
  const char *zSql,               /* SQL to compile */
  sqlite3_stmt **ppStmt,          /* OUT: Prepared statement */
  const char **pzOut              /* OUT: Pointer to next SQL statement */
){
  unsigned int prepFlags = 0;
#ifdef SQLITE_TEST
  if( pDb->bLegacyPrepare ){
    return sqlite3_prepare(pDb->db, zSql, -1, ppStmt, pzOut);
  }
#endif
  /* If the statement cache is large, use the SQLITE_PREPARE_PERSISTENT
  ** flags, which uses less lookaside memory.  But if the cache is small,
  ** omit that flag to make full use of lookaside */
  if( pDb->maxStmt>5 ) prepFlags = SQLITE_PREPARE_PERSISTENT;

  return sqlite3_prepare_v3(pDb->db, zSql, -1, prepFlags, ppStmt, pzOut);
}

/*
** Search the cache for a prepared-statement object that implements the
** first SQL statement in the buffer pointed to by parameter zIn. If
** no such prepared-statement can be found, allocate and prepare a new
** one. In either case, bind the current values of the relevant Tcl
** variables to any $var, :var or @var variables in the statement. Before
** returning, set *ppPreStmt to point to the prepared-statement object.
**
** Output parameter *pzOut is set to point to the next SQL statement in
** buffer zIn, or to the '\0' byte at the end of zIn if there is no
** next statement.
**
** If successful, TCL_OK is returned. Otherwise, TCL_ERROR is returned
** and an error message loaded into interpreter pDb->interp.
*/
static int dbPrepareAndBind(
  SqliteDb *pDb,                  /* Database object */
  char const *zIn,                /* SQL to compile */
  char const **pzOut,             /* OUT: Pointer to next SQL statement */
  SqlPreparedStmt **ppPreStmt     /* OUT: Object used to cache statement */
){
  const char *zSql = zIn;         /* Pointer to first SQL statement in zIn */
  sqlite3_stmt *pStmt = 0;        /* Prepared statement object */
  SqlPreparedStmt *pPreStmt;      /* Pointer to cached statement */
  int nSql;                       /* Length of zSql in bytes */
  int nVar = 0;                   /* Number of variables in statement */
  int iParm = 0;                  /* Next free entry in apParm */
  char c;
  int i;
  int needResultReset = 0;        /* Need to invoke Tcl_ResetResult() */
  int rc = SQLITE_OK;             /* Value to return */
  Tcl_Interp *interp = pDb->interp;

  *ppPreStmt = 0;

  /* Trim spaces from the start of zSql and calculate the remaining length. */
  while( (c = zSql[0])==' ' || c=='\t' || c=='\r' || c=='\n' ){ zSql++; }
  nSql = strlen30(zSql);

  for(pPreStmt = pDb->stmtList; pPreStmt; pPreStmt=pPreStmt->pNext){
    int n = pPreStmt->nSql;
    if( nSql>=n
        && memcmp(pPreStmt->zSql, zSql, n)==0
        && (zSql[n]==0 || zSql[n-1]==';')
    ){
      pStmt = pPreStmt->pStmt;
      *pzOut = &zSql[pPreStmt->nSql];

      /* When a prepared statement is found, unlink it from the
      ** cache list.  It will later be added back to the beginning
      ** of the cache list in order to implement LRU replacement.
      */
      if( pPreStmt->pPrev ){
        pPreStmt->pPrev->pNext = pPreStmt->pNext;
      }else{
        pDb->stmtList = pPreStmt->pNext;
      }
      if( pPreStmt->pNext ){
        pPreStmt->pNext->pPrev = pPreStmt->pPrev;
      }else{
        pDb->stmtLast = pPreStmt->pPrev;
      }
      pDb->nStmt--;
      nVar = sqlite3_bind_parameter_count(pStmt);
      break;
    }
  }

  /* If no prepared statement was found. Compile the SQL text. Also allocate
  ** a new SqlPreparedStmt structure.  */
  if( pPreStmt==0 ){
    int nByte;

    if( SQLITE_OK!=dbPrepare(pDb, zSql, &pStmt, pzOut) ){
      Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3_errmsg(pDb->db), -1));
      return TCL_ERROR;
    }
    if( pStmt==0 ){
      if( SQLITE_OK!=sqlite3_errcode(pDb->db) ){
        /* A compile-time error in the statement. */
        Tcl_SetObjResult(interp, Tcl_NewStringObj(sqlite3_errmsg(pDb->db), -1));
        return TCL_ERROR;
      }else{
        /* The statement was a no-op.  Continue to the next statement
        ** in the SQL string.
        */
        return TCL_OK;
      }
    }

    assert( pPreStmt==0 );
    nVar = sqlite3_bind_parameter_count(pStmt);
    nByte = sizeof(SqlPreparedStmt) + nVar*sizeof(Tcl_Obj *);
    pPreStmt = (SqlPreparedStmt*)Tcl_Alloc(nByte);
    memset(pPreStmt, 0, nByte);

    pPreStmt->pStmt = pStmt;
    pPreStmt->nSql = (int)(*pzOut - zSql);
    pPreStmt->zSql = sqlite3_sql(pStmt);
    pPreStmt->apParm = (Tcl_Obj **)&pPreStmt[1];
#ifdef SQLITE_TEST
    if( pPreStmt->zSql==0 ){
      char *zCopy = Tcl_Alloc(pPreStmt->nSql + 1);
      memcpy(zCopy, zSql, pPreStmt->nSql);
      zCopy[pPreStmt->nSql] = '\0';
      pPreStmt->zSql = zCopy;
    }
#endif
  }
  assert( pPreStmt );
  assert( strlen30(pPreStmt->zSql)==pPreStmt->nSql );
  assert( 0==memcmp(pPreStmt->zSql, zSql, pPreStmt->nSql) );

  /* Bind values to parameters that begin with $ or : */
  for(i=1; i<=nVar; i++){
    const char *zVar = sqlite3_bind_parameter_name(pStmt, i);
    if( zVar!=0 && (zVar[0]=='$' || zVar[0]==':' || zVar[0]=='@') ){
      Tcl_Obj *pVar = Tcl_GetVar2Ex(interp, &zVar[1], 0, 0);
      if( pVar==0 && pDb->zBindFallback!=0 ){
        Tcl_Obj *pCmd;
        int rx;
        pCmd = Tcl_NewStringObj(pDb->zBindFallback, -1);
        Tcl_IncrRefCount(pCmd);
        Tcl_ListObjAppendElement(interp, pCmd, Tcl_NewStringObj(zVar,-1));
        if( needResultReset ) Tcl_ResetResult(interp);
        needResultReset = 1;
        rx = Tcl_EvalObjEx(interp, pCmd, TCL_EVAL_DIRECT);
        Tcl_DecrRefCount(pCmd);
        if( rx==TCL_OK ){
          pVar = Tcl_GetObjResult(interp);
        }else if( rx==TCL_ERROR ){
          rc = TCL_ERROR;
          break;
        }else{
          pVar = 0;
        }
      }
      if( pVar ){
        Tcl_Size n;
        u8 *data;
        const char *zType = (pVar->typePtr ? pVar->typePtr->name : "");
        c = zType[0];
        if( zVar[0]=='@' ||
           (c=='b' && strcmp(zType,"bytearray")==0 && pVar->bytes==0) ){
          /* Load a BLOB type if the Tcl variable is a bytearray and
          ** it has no string representation or the host
          ** parameter name begins with "@". */
          data = Tcl_GetByteArrayFromObj(pVar, &n);
          sqlite3_bind_blob(pStmt, i, data, n, SQLITE_STATIC);
          Tcl_IncrRefCount(pVar);
          pPreStmt->apParm[iParm++] = pVar;
        }else if( c=='b' && pVar->bytes==0
               && (strcmp(zType,"booleanString")==0
                   || strcmp(zType,"boolean")==0)
        ){
          int nn;
          Tcl_GetBooleanFromObj(interp, pVar, &nn);
          sqlite3_bind_int(pStmt, i, nn);
        }else if( c=='d' && strcmp(zType,"double")==0 ){
          double r;
          Tcl_GetDoubleFromObj(interp, pVar, &r);
          sqlite3_bind_double(pStmt, i, r);
        }else if( (c=='w' && strcmp(zType,"wideInt")==0) ||
              (c=='i' && strcmp(zType,"int")==0) ){
          Tcl_WideInt v;
          Tcl_GetWideIntFromObj(interp, pVar, &v);
          sqlite3_bind_int64(pStmt, i, v);
        }else{
          data = (unsigned char *)Tcl_GetStringFromObj(pVar, &n);
          sqlite3_bind_text64(pStmt, i, (char *)data, n, SQLITE_STATIC,
                              SQLITE_UTF8);
          Tcl_IncrRefCount(pVar);
          pPreStmt->apParm[iParm++] = pVar;
        }
      }else{
        sqlite3_bind_null(pStmt, i);
      }
      if( needResultReset ) Tcl_ResetResult(pDb->interp);
    }
  }
  pPreStmt->nParm = iParm;
  *ppPreStmt = pPreStmt;
  if( needResultReset && rc==TCL_OK ) Tcl_ResetResult(pDb->interp);

  return rc;
}

/*
** Release a statement reference obtained by calling dbPrepareAndBind().
** There should be exactly one call to this function for each call to
** dbPrepareAndBind().
**
** If the discard parameter is non-zero, then the statement is deleted
** immediately. Otherwise it is added to the LRU list and may be returned
** by a subsequent call to dbPrepareAndBind().
*/
static void dbReleaseStmt(
  SqliteDb *pDb,                  /* Database handle */
  SqlPreparedStmt *pPreStmt,      /* Prepared statement handle to release */
  int discard                     /* True to delete (not cache) the pPreStmt */
){
  int i;

  /* Free the bound string and blob parameters */
  for(i=0; i<pPreStmt->nParm; i++){
    Tcl_DecrRefCount(pPreStmt->apParm[i]);
  }
  pPreStmt->nParm = 0;

  if( pDb->maxStmt<=0 || discard ){
    /* If the cache is turned off, deallocated the statement */
    dbFreeStmt(pPreStmt);
  }else{
    /* Add the prepared statement to the beginning of the cache list. */
    pPreStmt->pNext = pDb->stmtList;
    pPreStmt->pPrev = 0;
    if( pDb->stmtList ){
     pDb->stmtList->pPrev = pPreStmt;
    }
    pDb->stmtList = pPreStmt;
    if( pDb->stmtLast==0 ){
      assert( pDb->nStmt==0 );
      pDb->stmtLast = pPreStmt;
    }else{
      assert( pDb->nStmt>0 );
    }
    pDb->nStmt++;

    /* If we have too many statement in cache, remove the surplus from
    ** the end of the cache list.  */
    while( pDb->nStmt>pDb->maxStmt ){
      SqlPreparedStmt *pLast = pDb->stmtLast;
      pDb->stmtLast = pLast->pPrev;
      pDb->stmtLast->pNext = 0;
      pDb->nStmt--;
      dbFreeStmt(pLast);
    }
  }
}

/*
** Structure used with dbEvalXXX() functions:
**
**   dbEvalInit()
**   dbEvalStep()
**   dbEvalFinalize()
**   dbEvalRowInfo()
**   dbEvalColumnValue()
*/
typedef struct DbEvalContext DbEvalContext;
struct DbEvalContext {
  SqliteDb *pDb;                  /* Database handle */
  Tcl_Obj *pSql;                  /* Object holding string zSql */
  const char *zSql;               /* Remaining SQL to execute */
  SqlPreparedStmt *pPreStmt;      /* Current statement */
  int nCol;                       /* Number of columns returned by pStmt */
  int evalFlags;                  /* Flags used */
  Tcl_Obj *pVarName;              /* Name of target array/dict variable */
  Tcl_Obj **apColName;            /* Array of column names */
};

#define SQLITE_EVAL_WITHOUTNULLS  0x00001  /* Unset array(*) for NULL */
#define SQLITE_EVAL_ASDICT        0x00002  /* Use dict instead of array */

/*
** Release any cache of column names currently held as part of
** the DbEvalContext structure passed as the first argument.
*/
static void dbReleaseColumnNames(DbEvalContext *p){
  if( p->apColName ){
    int i;
    for(i=0; i<p->nCol; i++){
      Tcl_DecrRefCount(p->apColName[i]);
    }
    Tcl_Free((char *)p->apColName);
    p->apColName = 0;
  }
  p->nCol = 0;
}

/*
** Initialize a DbEvalContext structure.
**
** If pVarName is not NULL, then it contains the name of a Tcl array
** variable. The "*" member of this array is set to a list containing
** the names of the columns returned by the statement as part of each
** call to dbEvalStep(), in order from left to right. e.g. if the names
** of the returned columns are a, b and c, it does the equivalent of the
** tcl command:
**
**     set ${pVarName}(*) {a b c}
*/
static void dbEvalInit(
  DbEvalContext *p,               /* Pointer to structure to initialize */
  SqliteDb *pDb,                  /* Database handle */
  Tcl_Obj *pSql,                  /* Object containing SQL script */
  Tcl_Obj *pVarName,              /* Name of Tcl array to set (*) element of */
  int evalFlags                   /* Flags controlling evaluation */
){
  memset(p, 0, sizeof(DbEvalContext));
  p->pDb = pDb;
  p->zSql = Tcl_GetString(pSql);
  p->pSql = pSql;
  Tcl_IncrRefCount(pSql);
  if( pVarName ){
    p->pVarName = pVarName;
    Tcl_IncrRefCount(pVarName);
  }
  p->evalFlags = evalFlags;
  addDatabaseRef(p->pDb);
}

/*
** Obtain information about the row that the DbEvalContext passed as the
** first argument currently points to.
*/
static void dbEvalRowInfo(
  DbEvalContext *p,               /* Evaluation context */
  int *pnCol,                     /* OUT: Number of column names */
  Tcl_Obj ***papColName           /* OUT: Array of column names */
){
  /* Compute column names */
  if( 0==p->apColName ){
    sqlite3_stmt *pStmt = p->pPreStmt->pStmt;
    int i;                        /* Iterator variable */
    int nCol;                     /* Number of columns returned by pStmt */
    Tcl_Obj **apColName = 0;      /* Array of column names */

    p->nCol = nCol = sqlite3_column_count(pStmt);
    if( nCol>0 && (papColName || p->pVarName) ){
      apColName = (Tcl_Obj**)Tcl_Alloc( sizeof(Tcl_Obj*)*nCol );
      for(i=0; i<nCol; i++){
        apColName[i] = Tcl_NewStringObj(sqlite3_column_name(pStmt,i), -1);
        Tcl_IncrRefCount(apColName[i]);
      }
      p->apColName = apColName;
    }

    /* If results are being stored in a variable then create the
    ** array(*) or dict(*) entry for that variable.
    */
    if( p->pVarName ){
      Tcl_Interp *interp = p->pDb->interp;
      Tcl_Obj *pColList = Tcl_NewObj();
      Tcl_Obj *pStar = Tcl_NewStringObj("*", -1);

      Tcl_IncrRefCount(pColList);
      Tcl_IncrRefCount(pStar);
      for(i=0; i<nCol; i++){
        Tcl_ListObjAppendElement(interp, pColList, apColName[i]);
      }
      if( 0==(SQLITE_EVAL_ASDICT & p->evalFlags) ){
        Tcl_ObjSetVar2(interp, p->pVarName, pStar, pColList, 0);
      }else{
        Tcl_Obj * pDict = Tcl_ObjGetVar2(interp, p->pVarName, NULL, 0);
        if( !pDict ){
          pDict = Tcl_NewDictObj();
        }else if( Tcl_IsShared(pDict) ){
          pDict = Tcl_DuplicateObj(pDict);
        }
        if( Tcl_DictObjPut(interp, pDict, pStar, pColList)==TCL_OK ){
          Tcl_ObjSetVar2(interp, p->pVarName, NULL, pDict, 0);
        }
        Tcl_BounceRefCount(pDict);
      }
      Tcl_DecrRefCount(pStar);
      Tcl_DecrRefCount(pColList);
    }
  }

  if( papColName ){
    *papColName = p->apColName;
  }
  if( pnCol ){
    *pnCol = p->nCol;
  }
}

/*
** Return one of TCL_OK, TCL_BREAK or TCL_ERROR. If TCL_ERROR is
** returned, then an error message is stored in the interpreter before
** returning.
**
** A return value of TCL_OK means there is a row of data available. The
** data may be accessed using dbEvalRowInfo() and dbEvalColumnValue(). This
** is analogous to a return of SQLITE_ROW from sqlite3_step(). If TCL_BREAK
** is returned, then the SQL script has finished executing and there are
** no further rows available. This is similar to SQLITE_DONE.
*/
static int dbEvalStep(DbEvalContext *p){
  const char *zPrevSql = 0;       /* Previous value of p->zSql */

  while( p->zSql[0] || p->pPreStmt ){
    int rc;
    if( p->pPreStmt==0 ){
      zPrevSql = (p->zSql==zPrevSql ? 0 : p->zSql);
      rc = dbPrepareAndBind(p->pDb, p->zSql, &p->zSql, &p->pPreStmt);
      if( rc!=TCL_OK ) return rc;
    }else{
      int rcs;
      SqliteDb *pDb = p->pDb;
      SqlPreparedStmt *pPreStmt = p->pPreStmt;
      sqlite3_stmt *pStmt = pPreStmt->pStmt;

      rcs = sqlite3_step(pStmt);
      if( rcs==SQLITE_ROW ){
        return TCL_OK;
      }
      if( p->pVarName ){
        dbEvalRowInfo(p, 0, 0);
      }
      rcs = sqlite3_reset(pStmt);

      pDb->nStep = sqlite3_stmt_status(pStmt,SQLITE_STMTSTATUS_FULLSCAN_STEP,1);
      pDb->nSort = sqlite3_stmt_status(pStmt,SQLITE_STMTSTATUS_SORT,1);
      pDb->nIndex = sqlite3_stmt_status(pStmt,SQLITE_STMTSTATUS_AUTOINDEX,1);
      pDb->nVMStep = sqlite3_stmt_status(pStmt,SQLITE_STMTSTATUS_VM_STEP,1);
      dbReleaseColumnNames(p);
      p->pPreStmt = 0;

      if( rcs!=SQLITE_OK ){
        /* If a run-time error occurs, report the error and stop reading
        ** the SQL.  */
        dbReleaseStmt(pDb, pPreStmt, 1);
#if SQLITE_TEST
        if( p->pDb->bLegacyPrepare && rcs==SQLITE_SCHEMA && zPrevSql ){
          /* If the runtime error was an SQLITE_SCHEMA, and the database
          ** handle is configured to use the legacy sqlite3_prepare()
          ** interface, retry prepare()/step() on the same SQL statement.
          ** This only happens once. If there is a second SQLITE_SCHEMA
          ** error, the error will be returned to the caller. */
          p->zSql = zPrevSql;
          continue;
        }
#endif
        Tcl_SetObjResult(pDb->interp,
                         Tcl_NewStringObj(sqlite3_errmsg(pDb->db), -1));
        return TCL_ERROR;
      }else{
        dbReleaseStmt(pDb, pPreStmt, 0);
      }
    }
  }

  /* Finished */
  return TCL_BREAK;
}

/*
** Free all resources currently held by the DbEvalContext structure passed
** as the first argument. There should be exactly one call to this function
** for each call to dbEvalInit().
*/
static void dbEvalFinalize(DbEvalContext *p){
  if( p->pPreStmt ){
    sqlite3_reset(p->pPreStmt->pStmt);
    dbReleaseStmt(p->pDb, p->pPreStmt, 0);
    p->pPreStmt = 0;
  }
  if( p->pVarName ){
    Tcl_DecrRefCount(p->pVarName);
    p->pVarName = 0;
  }
  Tcl_DecrRefCount(p->pSql);
  dbReleaseColumnNames(p);
  delDatabaseRef(p->pDb);
}

/*
** Return a pointer to a Tcl_Obj structure with ref-count 0 that contains
** the value for the iCol'th column of the row currently pointed to by
** the DbEvalContext structure passed as the first argument.
*/
static Tcl_Obj *dbEvalColumnValue(DbEvalContext *p, int iCol){
  sqlite3_stmt *pStmt = p->pPreStmt->pStmt;
  switch( sqlite3_column_type(pStmt, iCol) ){
    case SQLITE_BLOB: {
      int bytes = sqlite3_column_bytes(pStmt, iCol);
      const char *zBlob = sqlite3_column_blob(pStmt, iCol);
      if( !zBlob ) bytes = 0;
      return Tcl_NewByteArrayObj((u8*)zBlob, bytes);
    }
    case SQLITE_INTEGER: {
      sqlite_int64 v = sqlite3_column_int64(pStmt, iCol);
      if( v>=-2147483647 && v<=2147483647 ){
        return Tcl_NewIntObj((int)v);
      }else{
        return Tcl_NewWideIntObj(v);
      }
    }
    case SQLITE_FLOAT: {
      return Tcl_NewDoubleObj(sqlite3_column_double(pStmt, iCol));
    }
    case SQLITE_NULL: {
      return Tcl_NewStringObj(p->pDb->zNull, -1);
    }
  }

  return Tcl_NewStringObj((char*)sqlite3_column_text(pStmt, iCol), -1);
}

/*
** If using Tcl version 8.6 or greater, use the NR functions to avoid
** recursive evaluation of scripts by the [db eval] and [db trans]
** commands. Even if the headers used while compiling the extension
** are 8.6 or newer, the code still tests the Tcl version at runtime.
** This allows stubs-enabled builds to be used with older Tcl libraries.
*/
#if TCL_MAJOR_VERSION>8 || !defined(TCL_MINOR_VERSION) \
                        || TCL_MINOR_VERSION>=6
# define SQLITE_TCL_NRE 1
static int DbUseNre(void){
  int major, minor;
  Tcl_GetVersion(&major, &minor, 0, 0);
  return( (major==8 && minor>=6) || major>8 );
}
#else
/*
** Compiling using headers earlier than 8.6. In this case NR cannot be
** used, so DbUseNre() to always return zero. Add #defines for the other
** Tcl_NRxxx() functions to prevent them from causing compilation errors,
** even though the only invocations of them are within conditional blocks
** of the form:
**
**   if( DbUseNre() ) { ... }
*/
# define SQLITE_TCL_NRE 0
# define DbUseNre() 0
# define Tcl_NRAddCallback(a,b,c,d,e,f) (void)0
# define Tcl_NREvalObj(a,b,c) 0
# define Tcl_NRCreateCommand(a,b,c,d,e,f) (void)0
#endif

/*
** This function is part of the implementation of the command:
**
**   $db eval SQL ?TGT-NAME? SCRIPT
*/
static int SQLITE_TCLAPI DbEvalNextCmd(
  ClientData data[],                   /* data[0] is the (DbEvalContext*) */
  Tcl_Interp *interp,                  /* Tcl interpreter */
  int result                           /* Result so far */
){
  int rc = result;                     /* Return code */

  /* The first element of the data[] array is a pointer to a DbEvalContext
  ** structure allocated using Tcl_Alloc(). The second element of data[]
  ** is a pointer to a Tcl_Obj containing the script to run for each row
  ** returned by the queries encapsulated in data[0]. */
  DbEvalContext *p = (DbEvalContext *)data[0];
  Tcl_Obj * const pScript = (Tcl_Obj *)data[1];
  Tcl_Obj * const pVarName = p->pVarName;

  while( (rc==TCL_OK || rc==TCL_CONTINUE) && TCL_OK==(rc = dbEvalStep(p)) ){
    int i;
    int nCol;
    Tcl_Obj **apColName;
    dbEvalRowInfo(p, &nCol, &apColName);
    for(i=0; i<nCol; i++){
      if( pVarName==0 ){
        Tcl_ObjSetVar2(interp, apColName[i], 0, dbEvalColumnValue(p,i), 0);
      }else if( (p->evalFlags & SQLITE_EVAL_WITHOUTNULLS)!=0
             && sqlite3_column_type(p->pPreStmt->pStmt, i)==SQLITE_NULL
      ){
        /* Remove NULL-containing column from the target container... */
        if( 0==(SQLITE_EVAL_ASDICT & p->evalFlags) ){
          /* Target is an array */
          Tcl_UnsetVar2(interp, Tcl_GetString(pVarName),
                        Tcl_GetString(apColName[i]), 0);
        }else{
          /* Target is a dict */
          Tcl_Obj *pDict = Tcl_ObjGetVar2(interp, pVarName, NULL, 0);
          if( pDict ){
            if( Tcl_IsShared(pDict) ){
              pDict = Tcl_DuplicateObj(pDict);
            }
            if( Tcl_DictObjRemove(interp, pDict, apColName[i])==TCL_OK ){
              Tcl_ObjSetVar2(interp, pVarName, NULL, pDict, 0);
            }
            Tcl_BounceRefCount(pDict);
          }
        }
      }else if( 0==(SQLITE_EVAL_ASDICT & p->evalFlags) ){
        /* Target is an array: set target(colName) = colValue */
        Tcl_ObjSetVar2(interp, pVarName, apColName[i],
                       dbEvalColumnValue(p,i), 0);
      }else{
        /* Target is a dict: set target(colName) = colValue */
        Tcl_Obj *pDict = Tcl_ObjGetVar2(interp, pVarName, NULL, 0);
        if( !pDict ){
          pDict = Tcl_NewDictObj();
        }else if( Tcl_IsShared(pDict) ){
          pDict = Tcl_DuplicateObj(pDict);
        }
        if( Tcl_DictObjPut(interp, pDict, apColName[i],
                           dbEvalColumnValue(p,i))==TCL_OK ){
          Tcl_ObjSetVar2(interp, pVarName, NULL, pDict, 0);
        }
        Tcl_BounceRefCount(pDict);
      }
    }

    /* The required interpreter variables are now populated with the data
    ** from the current row. If using NRE, schedule callbacks to evaluate
    ** script pScript, then to invoke this function again to fetch the next
    ** row (or clean up if there is no next row or the script throws an
    ** exception). After scheduling the callbacks, return control to the
    ** caller.
    **
    ** If not using NRE, evaluate pScript directly and continue with the
    ** next iteration of this while(...) loop.  */
    if( DbUseNre() ){
      Tcl_NRAddCallback(interp, DbEvalNextCmd, (void*)p, (void*)pScript, 0, 0);
      return Tcl_NREvalObj(interp, pScript, 0);
    }else{
      rc = Tcl_EvalObjEx(interp, pScript, 0);
    }
  }

  Tcl_DecrRefCount(pScript);
  dbEvalFinalize(p);
  Tcl_Free((char *)p);

  if( rc==TCL_OK || rc==TCL_BREAK ){
    Tcl_ResetResult(interp);
    rc = TCL_OK;
  }
  return rc;
}

/*
** This function is used by the implementations of the following database
** handle sub-commands:
**
**   $db update_hook ?SCRIPT?
**   $db wal_hook ?SCRIPT?
**   $db commit_hook ?SCRIPT?
**   $db preupdate hook ?SCRIPT?
*/
static void DbHookCmd(
  Tcl_Interp *interp,             /* Tcl interpreter */
  SqliteDb *pDb,                  /* Database handle */
  Tcl_Obj *pArg,                  /* SCRIPT argument (or NULL) */
  Tcl_Obj **ppHook                /* Pointer to member of SqliteDb */
){
  sqlite3 *db = pDb->db;

  if( *ppHook ){
    Tcl_SetObjResult(interp, *ppHook);
    if( pArg ){
      Tcl_DecrRefCount(*ppHook);
      *ppHook = 0;
    }
  }
  if( pArg ){
    assert( !(*ppHook) );
    if( Tcl_GetString(pArg)[0] ){
      *ppHook = pArg;
      Tcl_IncrRefCount(*ppHook);
    }
  }

#ifdef SQLITE_ENABLE_PREUPDATE_HOOK
  sqlite3_preupdate_hook(db, (pDb->pPreUpdateHook?DbPreUpdateHandler:0), pDb);
#endif
  sqlite3_update_hook(db, (pDb->pUpdateHook?DbUpdateHandler:0), pDb);
  sqlite3_rollback_hook(db, (pDb->pRollbackHook?DbRollbackHandler:0), pDb);
  sqlite3_wal_hook(db, (pDb->pWalHook?DbWalHandler:0), pDb);
}

/*
** Implementation of the "db format" command.
**
** Based on provided options, format the results of the SQL statement(s)
** provided into human-readable form using the Query Result Formatter (QRF)
** and return the resuling text.
**
** Syntax:    db format OPTIONS SQL
**
** OPTIONS may be:
**
**     -style ("auto"|"box"|"column"|...)      Output style
**     -esc ("auto"|"off"|"ascii"|"symbol")    How to deal with ctrl chars
**     -text ("auto"|"off"|"sql"|"csv"|...)    How to escape TEXT values
**     -title ("auto"|"off"|"sql"|...|"off")   How to escape column names
**     -blob ("auto"|"text"|"sql"|...)         How to escape BLOB values
**     -wordwrap ("auto"|"off"|"on")           Try to wrap at word boundry?
**     -textjsonb ("auto"|"off"|"on")          Auto-convert JSONB to text?
**     -splitcolumn ("auto"|"off"|"on")        Enable split-column mode
**     -defaultalign ("auto"|"left"|...)       Default alignment
**     -titalalign ("auto"|"left"|"right"|...) Default column name alignment
**     -border ("auto"|"off"|"on")             Border for box and table styles
**     -wrap NUMBER                            Max width of any single column
**     -screenwidth NUMBER                     Width of the display TTY
**     -linelimit NUMBER                       Max lines for any cell
**     -charlimit NUMBER                       Content truncated to this size
**     -titlelimit NUMBER                      Max width of column titles
**     -multiinsert NUMBER                     Multi-row INSERT byte size
**     -align LIST-OF-ALIGNMENT                Alignment of columns
**     -widths LIST-OF-NUMBERS                 Widths for individual columns
**     -columnsep TEXT                         Column separator text
**     -rowsep TEXT                            Row separator text
**     -tablename TEXT                         Table name for style "insert"
**     -null TEXT                              Text for NULL values
**
** A mapping from TCL "format" command options to sqlite3_qrf_spec fields
** is below.  Use this to reference the QRF documentation:
**
**     TCL Option        spec field
**     ----------        ----------
**     -style            eStyle
**     -esc              eEsc
**     -text             eText
**     -title            eTitle, bTitle
**     -blob             eBlob
**     -wordwrap         bWordWrap
**     -textjsonb        bTextJsonb
**     -splitcolumn      bSplitColumn
**     -defaultalign     eDfltAlign
**     -titlealign       eTitleAlign
**     -border           bBorder
**     -wrap             nWrap
**     -screenwidth      nScreenWidth
**     -linelimit        nLineLimit
**     -charlimit        nCharLimit
**     -titlelimit       nTitleLimit
**     -multiinsert      nMultiInsert
**     -align            nAlign, aAlign
**     -widths           nWidth, aWidth
**     -columnsep        zColumnSep
**     -rowsep           zRowSep
**     -tablename        zTableName
**     -null             zNull
*/
static int dbQrf(SqliteDb *pDb, int objc, Tcl_Obj *const*objv){
#ifndef SQLITE_QRF_H
  Tcl_SetResult(pDb->interp, "QRF not available in this build", TCL_VOLATILE);
  return TCL_ERROR;
#else
  char *zResult = 0;             /* Result to be returned */
  const char *zSql = 0;          /* SQL to run */
  int i;                         /* Loop counter */
  int rc;                        /* Result code */
  sqlite3_qrf_spec qrf;          /* Formatting spec */
  static const char *azAlign[] = {
    "auto",           "bottom",          "c",
    "center",         "e",               "left",
    "middle",         "n",               "ne",
    "nw",             "right",           "s",
    "se",             "sw",              "top",
    "w",              0
  };
  static const unsigned char aAlignMap[] = {
    QRF_ALIGN_Auto,   QRF_ALIGN_Bottom,  QRF_ALIGN_C,
    QRF_ALIGN_Center, QRF_ALIGN_E,       QRF_ALIGN_Left,
    QRF_ALIGN_Middle, QRF_ALIGN_N,       QRF_ALIGN_NE,
    QRF_ALIGN_NW,     QRF_ALIGN_Right,   QRF_ALIGN_S,
    QRF_ALIGN_SE,     QRF_ALIGN_SW,      QRF_ALIGN_Top,
    QRF_ALIGN_W
  };

  memset(&qrf, 0, sizeof(qrf));
  qrf.iVersion = 2;
  qrf.pzOutput = &zResult;
  for(i=2; i<objc; i++){
    const char *zArg = Tcl_GetString(objv[i]);
    const char *azBool[] = { "auto", "yes", "no", "on", "off", 0 };
    const unsigned char aBoolMap[] = { 0, 2, 1, 2, 1 };
    if( zArg[0]!='-' ){
      if( zSql ){
        Tcl_AppendResult(pDb->interp, "unknown argument: ", zArg, (char*)0);
        rc = TCL_ERROR;
        goto format_failed;
      }
      zSql  = zArg;
    }else if( i==objc-1 ){
      Tcl_AppendResult(pDb->interp, "option has no argument: ", zArg, (char*)0);
      rc = TCL_ERROR;
      goto format_failed;
    }else if( strcmp(zArg,"-style")==0 ){
      static const char *azStyles[] = {
        "auto",             "box",              "column",
        "count",            "csv",              "eqp",
        "explain",          "html",             "insert",
        "jobject",          "json",             "line",
        "list",             "markdown",         "quote",
        "stats",            "stats-est",        "stats-vm",
        "table",            0
      };
      static unsigned char aStyleMap[] = {
        QRF_STYLE_Auto,     QRF_STYLE_Box,      QRF_STYLE_Column,
        QRF_STYLE_Count,    QRF_STYLE_Csv,      QRF_STYLE_Eqp,
        QRF_STYLE_Explain,  QRF_STYLE_Html,     QRF_STYLE_Insert,
        QRF_STYLE_JObject,  QRF_STYLE_Json,     QRF_STYLE_Line,
        QRF_STYLE_List,     QRF_STYLE_Markdown, QRF_STYLE_Quote,
        QRF_STYLE_Stats,    QRF_STYLE_StatsEst, QRF_STYLE_StatsVm,
        QRF_STYLE_Table
      };
      int style;
      rc = Tcl_GetIndexFromObj(pDb->interp, objv[i+1], azStyles,
                              "format style (-style)", 0, &style);
      if( rc ) goto format_failed;
      qrf.eStyle = aStyleMap[style];
      i++;
    }else if( strcmp(zArg,"-esc")==0 ){
      static const char *azEsc[] = {
        "ascii",        "auto",         "off",      "symbol",   0
      };
      static unsigned char aEscMap[] = {
        QRF_ESC_Ascii,  QRF_ESC_Auto,   QRF_ESC_Off, QRF_ESC_Symbol
      };
      int esc;
      rc = Tcl_GetIndexFromObj(pDb->interp, objv[i+1], azEsc,
                              "control character escape (-esc)", 0, &esc);
      if( rc ) goto format_failed;
      qrf.eEsc = aEscMap[esc];
      i++;
    }else if( strcmp(zArg,"-text")==0 || strcmp(zArg, "-title")==0 ){
      /* NB: --title can be "off" or "on but --text may not be.  Thus we put
      ** the "off" and "on" choices first and start the search on the
      ** thrid element of the array when processing --text */
      static const char *azText[] = {           "off",   "on",
        "auto",             "csv",              "html",
        "json",             "plain",            "relaxed",
        "sql",              "tcl",              0
      };
      static unsigned char aTextMap[] = {
        QRF_TEXT_Auto,      QRF_TEXT_Csv,       QRF_TEXT_Html,
        QRF_TEXT_Json,      QRF_TEXT_Plain,     QRF_TEXT_Relaxed,
        QRF_TEXT_Sql,       QRF_TEXT_Tcl
      };
      int txt;
      int k = zArg[2]=='e';
      rc = Tcl_GetIndexFromObj(pDb->interp, objv[i+1], &azText[k*2], zArg,
                               0, &txt);
      if( rc ) goto format_failed;
      if( k ){
        qrf.eText = aTextMap[txt];
      }else if( txt<=1 ){
        qrf.bTitles = txt ? QRF_Yes : QRF_No;
        qrf.eTitle = QRF_TEXT_Auto;
      }else{
        qrf.bTitles = QRF_Yes;
        qrf.eTitle = aTextMap[txt-2];
      }
      i++;
    }else if( strcmp(zArg,"-blob")==0 ){
      static const char *azBlob[] = {
        "auto",             "hex",              "json",
        "tcl",              "text",             "sql",
        "size",             0
      };
      static unsigned char aBlobMap[] = {
        QRF_BLOB_Auto,      QRF_BLOB_Hex,       QRF_BLOB_Json,
        QRF_BLOB_Tcl,       QRF_BLOB_Text,      QRF_BLOB_Sql,
        QRF_BLOB_Size
      };
      int blob;
      rc = Tcl_GetIndexFromObj(pDb->interp, objv[i+1], azBlob,
                              "BLOB encoding (-blob)", 0, &blob);
      if( rc ) goto format_failed;
      qrf.eBlob = aBlobMap[blob];
      i++;
    }else if( strcmp(zArg,"-wordwrap")==0 ){
      int v = 0;
      rc = Tcl_GetIndexFromObj(pDb->interp, objv[i+1], azBool,
                              "-wordwrap", 0, &v);
      if( rc ) goto format_failed;
      qrf.bWordWrap = aBoolMap[v];
      i++;
    }else if( strcmp(zArg,"-textjsonb")==0
           || strcmp(zArg,"-splitcolumn")==0
           || strcmp(zArg,"-border")==0
    ){
      int v = 0;
      rc = Tcl_GetIndexFromObj(pDb->interp, objv[i+1], azBool,
                              zArg, 0, &v);
      if( rc ) goto format_failed;
      if( zArg[1]=='t' ){
        qrf.bTextJsonb = aBoolMap[v];
      }else if( zArg[1]=='b' ){
        qrf.bBorder = aBoolMap[v];
      }else{
        qrf.bSplitColumn = aBoolMap[v];
      }
      i++;
    }else if( strcmp(zArg,"-defaultalign")==0 || strcmp(zArg,"-titlealign")==0){
      int ax = 0;
      rc = Tcl_GetIndexFromObj(pDb->interp, objv[i+1], azAlign,
                    zArg[1]=='d' ?  "default alignment (-defaultalign)" :
                                    "title alignment (-titlealign)",
                    0, &ax);
      if( rc ) goto format_failed;
      if( zArg[1]=='d' ){
        qrf.eDfltAlign = aAlignMap[ax];
      }else{
        qrf.eTitleAlign = aAlignMap[ax];
      }
      i++;
    }else if( strcmp(zArg,"-wrap")==0
           || strcmp(zArg,"-screenwidth")==0
           || strcmp(zArg,"-linelimit")==0
           || strcmp(zArg,"-titlelimit")==0
    ){
      int v = 0;
      rc = Tcl_GetIntFromObj(pDb->interp, objv[i+1], &v);
      if( rc ) goto format_failed;
      if( v<QRF_MIN_WIDTH ){
        v = QRF_MIN_WIDTH;
      }else if( v>QRF_MAX_WIDTH ){
        v = QRF_MAX_WIDTH;
      }
      if( zArg[1]=='w' ){
        qrf.nWrap = v;
      }else if( zArg[1]=='s' ){
        qrf.nScreenWidth = v;
      }else if( zArg[1]=='t' ){
        qrf.nTitleLimit = v;
      }else{
        qrf.nLineLimit = v;
      }
      i++;
    }else if( strcmp(zArg,"-charlimit")==0 ){
      int v = 0;
      rc = Tcl_GetIntFromObj(pDb->interp, objv[i+1], &v);
      if( rc ) goto format_failed;
      if( v<0 ) v = 0;
      qrf.nCharLimit = v;
      i++;
    }else if( strcmp(zArg,"-multiinsert")==0 ){
      int v = 0;
      rc = Tcl_GetIntFromObj(pDb->interp, objv[i+1], &v);
      if( rc ) goto format_failed;
      if( v<0 ) v = 0;
      qrf.nMultiInsert = v;
      i++;
    }else if( strcmp(zArg,"-align")==0 ){
      Tcl_Size n = 0;
      int jj;
      rc = Tcl_ListObjLength(pDb->interp, objv[i+1], &n);
      if( rc ) goto format_failed;
      sqlite3_free(qrf.aAlign);
      qrf.aAlign = sqlite3_malloc64( (n+1)*sizeof(qrf.aAlign[0]) );
      if( qrf.aAlign==0 ){
        Tcl_AppendResult(pDb->interp, "out of memory", (char*)0);
        rc = TCL_ERROR;
        goto format_failed;
      }
      memset(qrf.aAlign, 0, (n+1)*sizeof(qrf.aAlign[0]));
      qrf.nAlign = n;
      for(jj=0; jj<n; jj++){
        int x;
        Tcl_Obj *pTerm;
        rc = Tcl_ListObjIndex(pDb->interp, objv[i+1], jj, &pTerm);
        if( rc ) goto format_failed;
        rc = Tcl_GetIndexFromObj(pDb->interp, pTerm, azAlign,
                          "column alignment (-align)", 0, &x);
        if( rc ) goto format_failed;
        qrf.aAlign[jj] = aAlignMap[x];
      }
      i++;
    }else if( strcmp(zArg,"-widths")==0 ){
      Tcl_Size n = 0;
      int jj;
      rc = Tcl_ListObjLength(pDb->interp, objv[i+1], &n);
      if( rc ) goto format_failed;
      sqlite3_free(qrf.aWidth);
      qrf.aWidth = sqlite3_malloc64( (n+1)*sizeof(qrf.aWidth[0]) );
      if( qrf.aWidth==0 ){
        Tcl_AppendResult(pDb->interp, "out of memory", (char*)0);
        rc = TCL_ERROR;
        goto format_failed;
      }
      memset(qrf.aWidth, 0, (n+1)*sizeof(qrf.aWidth[0]));
      qrf.nWidth = n;
      for(jj=0; jj<n; jj++){
        Tcl_Obj *pTerm;
        int v;
        rc = Tcl_ListObjIndex(pDb->interp, objv[i+1], jj, &pTerm);
        if( rc ) goto format_failed;
        rc = Tcl_GetIntFromObj(pDb->interp, pTerm, &v);
        if( v<(-QRF_MAX_WIDTH) ){
          v = -QRF_MAX_WIDTH;
        }else if( v>QRF_MAX_WIDTH ){
          v = QRF_MAX_WIDTH;
        }
        qrf.aWidth[jj] = (short int)v;
      }
      i++;
    }else if( strcmp(zArg,"-columnsep")==0 ){
      qrf.zColumnSep = Tcl_GetString(objv[i+1]);
      i++;
    }else if( strcmp(zArg,"-rowsep")==0 ){
      qrf.zRowSep = Tcl_GetString(objv[i+1]);
      i++;
    }else if( strcmp(zArg,"-tablename")==0 ){
      qrf.zTableName = Tcl_GetString(objv[i+1]);
      i++;
    }else if( strcmp(zArg,"-null")==0 ){
      qrf.zNull = Tcl_GetString(objv[i+1]);
      i++;
    }else if( strcmp(zArg,"-version")==0 ){
      /* Undocumented. Testing use only */
      qrf.iVersion = atoi(Tcl_GetString(objv[i+1]));
      i++;
    }else{
      Tcl_AppendResult(pDb->interp, "unknown option: ", zArg, (char*)0);
      rc = TCL_ERROR;
      goto format_failed;
    }
  }
  while( zSql && zSql[0] ){
    SqlPreparedStmt *pStmt = 0;        /* Next statement to run */
    char *zErr = 0;                    /* Error message from QRF */

    rc = dbPrepareAndBind(pDb, zSql, &zSql, &pStmt);
    if( rc ) goto format_failed;
    if( pStmt==0 ) continue;
    rc = sqlite3_format_query_result(pStmt->pStmt, &qrf, &zErr);
    dbReleaseStmt(pDb, pStmt, 0);
    if( rc ){
      Tcl_SetResult(pDb->interp, zErr, TCL_VOLATILE);
      sqlite3_free(zErr);
      rc = TCL_ERROR;
      goto format_failed;
    }
  }
  Tcl_SetResult(pDb->interp, zResult, TCL_VOLATILE);
  rc = TCL_OK;
  /* Fall through...*/

format_failed:
  sqlite3_free(qrf.aWidth);
  sqlite3_free(qrf.aAlign);
  sqlite3_free(zResult);
  return rc;

#endif
}

/*
** The "sqlite" command below creates a new Tcl command for each
** connection it opens to an SQLite database.  This routine is invoked
** whenever one of those connection-specific commands is executed
** in Tcl.  For example, if you run Tcl code like this:
**
**       sqlite3 db1  "my_database"
**       db1 close
**
** The first command opens a connection to the "my_database" database
** and calls that connection "db1".  The second command causes this
** subroutine to be invoked.
*/
static int SQLITE_TCLAPI DbObjCmd(
  void *cd,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const*objv
){
  SqliteDb *pDb = (SqliteDb*)cd;
  int choice;
  int rc = TCL_OK;
  static const char *DB_strs[] = {
    "authorizer",             "backup",                "bind_fallback",
    "busy",                   "cache",                 "changes",
    "close",                  "collate",               "collation_needed",
    "commit_hook",            "complete",              "config",
    "copy",                   "deserialize",           "enable_load_extension",
    "errorcode",              "erroroffset",           "eval",
    "exists",                 "format",                "function",
    "incrblob",               "interrupt",             "last_insert_rowid",
    "nullvalue",              "onecolumn",             "preupdate",
    "profile",                "progress",              "rekey",
    "restore",                "rollback_hook",         "serialize",
    "status",                 "timeout",               "total_changes",
    "trace",                  "trace_v2",              "transaction",
    "unlock_notify",          "update_hook",           "version",
    "wal_hook",               0
  };
  enum DB_enum {
    DB_AUTHORIZER,            DB_BACKUP,               DB_BIND_FALLBACK,
    DB_BUSY,                  DB_CACHE,                DB_CHANGES,
    DB_CLOSE,                 DB_COLLATE,              DB_COLLATION_NEEDED,
    DB_COMMIT_HOOK,           DB_COMPLETE,             DB_CONFIG,
    DB_COPY,                  DB_DESERIALIZE,          DB_ENABLE_LOAD_EXTENSION,
    DB_ERRORCODE,             DB_ERROROFFSET,          DB_EVAL,
    DB_EXISTS,                DB_FORMAT,               DB_FUNCTION,
    DB_INCRBLOB,              DB_INTERRUPT,            DB_LAST_INSERT_ROWID,
    DB_NULLVALUE,             DB_ONECOLUMN,            DB_PREUPDATE,
    DB_PROFILE,               DB_PROGRESS,             DB_REKEY,
    DB_RESTORE,               DB_ROLLBACK_HOOK,        DB_SERIALIZE,
    DB_STATUS,                DB_TIMEOUT,              DB_TOTAL_CHANGES,
    DB_TRACE,                 DB_TRACE_V2,             DB_TRANSACTION,
    DB_UNLOCK_NOTIFY,         DB_UPDATE_HOOK,          DB_VERSION,
    DB_WAL_HOOK
  };
  /* don't leave trailing commas on DB_enum, it confuses the AIX xlc compiler */

  if( objc<2 ){
    Tcl_WrongNumArgs(interp, 1, objv, "SUBCOMMAND ...");
    return TCL_ERROR;
  }
  if( Tcl_GetIndexFromObj(interp, objv[1], DB_strs, "option", 0, &choice) ){
    return TCL_ERROR;
  }

  switch( (enum DB_enum)choice ){

  /*    $db authorizer ?CALLBACK?
  **
  ** Invoke the given callback to authorize each SQL operation as it is
  ** compiled.  5 arguments are appended to the callback before it is
  ** invoked:
  **
  **   (1) The authorization type (ex: SQLITE_CREATE_TABLE, SQLITE_INSERT, ...)
  **   (2) First descriptive name (depends on authorization type)
  **   (3) Second descriptive name
  **   (4) Name of the database (ex: "main", "temp")
  **   (5) Name of trigger that is doing the access
  **
  ** The callback should return one of the following strings: SQLITE_OK,
  ** SQLITE_IGNORE, or SQLITE_DENY.  Any other return value is an error.
  **
  ** If this method is invoked with no arguments, the current authorization
  ** callback string is returned.
  */
  case DB_AUTHORIZER: {
#ifdef SQLITE_OMIT_AUTHORIZATION
    Tcl_AppendResult(interp, "authorization not available in this build",
                     (char*)0);
    return TCL_ERROR;
#else
    if( objc>3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "?CALLBACK?");
      return TCL_ERROR;
    }else if( objc==2 ){
      if( pDb->zAuth ){
        Tcl_AppendResult(interp, pDb->zAuth, (char*)0);
      }
    }else{
      char *zAuth;
      Tcl_Size len;
      if( pDb->zAuth ){
        Tcl_Free(pDb->zAuth);
      }
      zAuth = Tcl_GetStringFromObj(objv[2], &len);
      if( zAuth && len>0 ){
        pDb->zAuth = Tcl_Alloc( len + 1 );
        memcpy(pDb->zAuth, zAuth, len+1);
      }else{
        pDb->zAuth = 0;
      }
      if( pDb->zAuth ){
        typedef int (*sqlite3_auth_cb)(
           void*,int,const char*,const char*,
           const char*,const char*);
        pDb->interp = interp;
        sqlite3_set_authorizer(pDb->db,(sqlite3_auth_cb)auth_callback,pDb);
      }else{
        sqlite3_set_authorizer(pDb->db, 0, 0);
      }
    }
#endif
    break;
  }

  /*    $db backup ?DATABASE? FILENAME
  **
  ** Open or create a database file named FILENAME.  Transfer the
  ** content of local database DATABASE (default: "main") into the
  ** FILENAME database.
  */
  case DB_BACKUP: {
    const char *zDestFile;
    const char *zSrcDb;
    sqlite3 *pDest;
    sqlite3_backup *pBackup;

    if( objc==3 ){
      zSrcDb = "main";
      zDestFile = Tcl_GetString(objv[2]);
    }else if( objc==4 ){
      zSrcDb = Tcl_GetString(objv[2]);
      zDestFile = Tcl_GetString(objv[3]);
    }else{
      Tcl_WrongNumArgs(interp, 2, objv, "?DATABASE? FILENAME");
      return TCL_ERROR;
    }
    rc = sqlite3_open_v2(zDestFile, &pDest,
               SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE| pDb->openFlags, 0);
    if( rc!=SQLITE_OK ){
      Tcl_AppendResult(interp, "cannot open target database: ",
           sqlite3_errmsg(pDest), (char*)0);
      sqlite3_close(pDest);
      return TCL_ERROR;
    }
    pBackup = sqlite3_backup_init(pDest, "main", pDb->db, zSrcDb);
    if( pBackup==0 ){
      Tcl_AppendResult(interp, "backup failed: ",
           sqlite3_errmsg(pDest), (char*)0);
      sqlite3_close(pDest);
      return TCL_ERROR;
    }
    while(  (rc = sqlite3_backup_step(pBackup,100))==SQLITE_OK ){}
    sqlite3_backup_finish(pBackup);
    if( rc==SQLITE_DONE ){
      rc = TCL_OK;
    }else{
      Tcl_AppendResult(interp, "backup failed: ",
           sqlite3_errmsg(pDest), (char*)0);
      rc = TCL_ERROR;
    }
    sqlite3_close(pDest);
    break;
  }

  /*    $db bind_fallback ?CALLBACK?
  **
  ** When resolving bind parameters in an SQL statement, if the parameter
  ** cannot be associated with a TCL variable then invoke CALLBACK with a
  ** single argument that is the name of the parameter and use the return
  ** value of the CALLBACK as the binding.  If CALLBACK returns something
  ** other than TCL_OK or TCL_ERROR then bind a NULL.
  **
  ** If CALLBACK is an empty string, then revert to the default behavior
  ** which is to set the binding to NULL.
  **
  ** If CALLBACK returns an error, that causes the statement execution to
  ** abort.  Hence, to configure a connection so that it throws an error
  ** on an attempt to bind an unknown variable, do something like this:
  **
  **     proc bind_error {name} {error "no such variable: $name"}
  **     db bind_fallback bind_error
  */
  case DB_BIND_FALLBACK: {
    if( objc>3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "?CALLBACK?");
      return TCL_ERROR;
    }else if( objc==2 ){
      if( pDb->zBindFallback ){
        Tcl_AppendResult(interp, pDb->zBindFallback, (char*)0);
      }
    }else{
      char *zCallback;
      Tcl_Size len;
      if( pDb->zBindFallback ){
        Tcl_Free(pDb->zBindFallback);
      }
      zCallback = Tcl_GetStringFromObj(objv[2], &len);
      if( zCallback && len>0 ){
        pDb->zBindFallback = Tcl_Alloc( len + 1 );
        memcpy(pDb->zBindFallback, zCallback, len+1);
      }else{
        pDb->zBindFallback = 0;
      }
    }
    break;
  }

  /*    $db busy ?CALLBACK?
  **
  ** Invoke the given callback if an SQL statement attempts to open
  ** a locked database file.
  */
  case DB_BUSY: {
    if( objc>3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "CALLBACK");
      return TCL_ERROR;
    }else if( objc==2 ){
      if( pDb->zBusy ){
        Tcl_AppendResult(interp, pDb->zBusy, (char*)0);
      }
    }else{
      char *zBusy;
      Tcl_Size len;
      if( pDb->zBusy ){
        Tcl_Free(pDb->zBusy);
      }
      zBusy = Tcl_GetStringFromObj(objv[2], &len);
      if( zBusy && len>0 ){
        pDb->zBusy = Tcl_Alloc( len + 1 );
        memcpy(pDb->zBusy, zBusy, len+1);
      }else{
        pDb->zBusy = 0;
      }
      if( pDb->zBusy ){
        pDb->interp = interp;
        sqlite3_busy_handler(pDb->db, DbBusyHandler, pDb);
      }else{
        sqlite3_busy_handler(pDb->db, 0, 0);
      }
    }
    break;
  }

  /*     $db cache flush
  **     $db cache size n
  **
  ** Flush the prepared statement cache, or set the maximum number of
  ** cached statements.
  */
  case DB_CACHE: {
    char *subCmd;
    int n;

    if( objc<=2 ){
      Tcl_WrongNumArgs(interp, 1, objv, "cache option ?arg?");
      return TCL_ERROR;
    }
    subCmd = Tcl_GetStringFromObj( objv[2], 0 );
    if( *subCmd=='f' && strcmp(subCmd,"flush")==0 ){
      if( objc!=3 ){
        Tcl_WrongNumArgs(interp, 2, objv, "flush");
        return TCL_ERROR;
      }else{
        flushStmtCache( pDb );
      }
    }else if( *subCmd=='s' && strcmp(subCmd,"size")==0 ){
      if( objc!=4 ){
        Tcl_WrongNumArgs(interp, 2, objv, "size n");
        return TCL_ERROR;
      }else{
        if( TCL_ERROR==Tcl_GetIntFromObj(interp, objv[3], &n) ){
          Tcl_AppendResult( interp, "cannot convert \"",
               Tcl_GetStringFromObj(objv[3],0), "\" to integer", (char*)0);
          return TCL_ERROR;
        }else{
          if( n<0 ){
            flushStmtCache( pDb );
            n = 0;
          }else if( n>MAX_PREPARED_STMTS ){
            n = MAX_PREPARED_STMTS;
          }
          pDb->maxStmt = n;
        }
      }
    }else{
      Tcl_AppendResult( interp, "bad option \"",
          Tcl_GetStringFromObj(objv[2],0), "\": must be flush or size",
          (char*)0);
      return TCL_ERROR;
    }
    break;
  }

  /*     $db changes
  **
  ** Return the number of rows that were modified, inserted, or deleted by
  ** the most recent INSERT, UPDATE or DELETE statement, not including
  ** any changes made by trigger programs.
  */
  case DB_CHANGES: {
    Tcl_Obj *pResult;
    if( objc!=2 ){
      Tcl_WrongNumArgs(interp, 2, objv, "");
      return TCL_ERROR;
    }
    pResult = Tcl_GetObjResult(interp);
    Tcl_SetWideIntObj(pResult, sqlite3_changes64(pDb->db));
    break;
  }

  /*    $db close
  **
  ** Shutdown the database
  */
  case DB_CLOSE: {
    Tcl_DeleteCommand(interp, Tcl_GetStringFromObj(objv[0], 0));
    break;
  }

  /*
  **     $db collate NAME SCRIPT
  **
  ** Create a new SQL collation function called NAME.  Whenever
  ** that function is called, invoke SCRIPT to evaluate the function.
  */
  case DB_COLLATE: {
    SqlCollate *pCollate;
    char *zName;
    char *zScript;
    Tcl_Size nScript;
    if( objc!=4 ){
      Tcl_WrongNumArgs(interp, 2, objv, "NAME SCRIPT");
      return TCL_ERROR;
    }
    zName = Tcl_GetStringFromObj(objv[2], 0);
    zScript = Tcl_GetStringFromObj(objv[3], &nScript);
    pCollate = (SqlCollate*)Tcl_Alloc( sizeof(*pCollate) + nScript + 1 );
    if( pCollate==0 ) return TCL_ERROR;
    pCollate->interp = interp;
    pCollate->pNext = pDb->pCollate;
    pCollate->zScript = (char*)&pCollate[1];
    pDb->pCollate = pCollate;
    memcpy(pCollate->zScript, zScript, nScript+1);
    if( sqlite3_create_collation(pDb->db, zName, SQLITE_UTF8,
        pCollate, tclSqlCollate) ){
      Tcl_SetResult(interp, (char *)sqlite3_errmsg(pDb->db), TCL_VOLATILE);
      return TCL_ERROR;
    }
    break;
  }

  /*
  **     $db collation_needed SCRIPT
  **
  ** Create a new SQL collation function called NAME.  Whenever
  ** that function is called, invoke SCRIPT to evaluate the function.
  */
  case DB_COLLATION_NEEDED: {
    if( objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "SCRIPT");
      return TCL_ERROR;
    }
    if( pDb->pCollateNeeded ){
      Tcl_DecrRefCount(pDb->pCollateNeeded);
    }
    pDb->pCollateNeeded = Tcl_DuplicateObj(objv[2]);
    Tcl_IncrRefCount(pDb->pCollateNeeded);
    sqlite3_collation_needed(pDb->db, pDb, tclCollateNeeded);
    break;
  }

  /*    $db commit_hook ?CALLBACK?
  **
  ** Invoke the given callback just before committing every SQL transaction.
  ** If the callback throws an exception or returns non-zero, then the
  ** transaction is aborted.  If CALLBACK is an empty string, the callback
  ** is disabled.
  */
  case DB_COMMIT_HOOK: {
    if( objc>3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "?CALLBACK?");
      return TCL_ERROR;
    }else if( objc==2 ){
      if( pDb->zCommit ){
        Tcl_AppendResult(interp, pDb->zCommit, (char*)0);
      }
    }else{
      const char *zCommit;
      Tcl_Size len;
      if( pDb->zCommit ){
        Tcl_Free(pDb->zCommit);
      }
      zCommit = Tcl_GetStringFromObj(objv[2], &len);
      if( zCommit && len>0 ){
        pDb->zCommit = Tcl_Alloc( len + 1 );
        memcpy(pDb->zCommit, zCommit, len+1);
      }else{
        pDb->zCommit = 0;
      }
      if( pDb->zCommit ){
        pDb->interp = interp;
        sqlite3_commit_hook(pDb->db, DbCommitHandler, pDb);
      }else{
        sqlite3_commit_hook(pDb->db, 0, 0);
      }
    }
    break;
  }

  /*    $db complete SQL
  **
  ** Return TRUE if SQL is a complete SQL statement.  Return FALSE if
  ** additional lines of input are needed.  This is similar to the
  ** built-in "info complete" command of Tcl.
  */
  case DB_COMPLETE: {
#ifndef SQLITE_OMIT_COMPLETE
    Tcl_Obj *pResult;
    int isComplete;
    if( objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "SQL");
      return TCL_ERROR;
    }
    isComplete = sqlite3_complete( Tcl_GetStringFromObj(objv[2], 0) );
    pResult = Tcl_GetObjResult(interp);
    Tcl_SetBooleanObj(pResult, isComplete);
#endif
    break;
  }

  /*    $db config ?OPTION? ?BOOLEAN?
  **
  ** Configure the database connection using the sqlite3_db_config()
  ** interface.
  */
  case DB_CONFIG: {
    static const struct DbConfigChoices {
      const char *zName;
      int op;
    } aDbConfig[] = {
        { "defensive",          SQLITE_DBCONFIG_DEFENSIVE             },
        { "dqs_ddl",            SQLITE_DBCONFIG_DQS_DDL               },
        { "dqs_dml",            SQLITE_DBCONFIG_DQS_DML               },
        { "enable_fkey",        SQLITE_DBCONFIG_ENABLE_FKEY           },
        { "enable_qpsg",        SQLITE_DBCONFIG_ENABLE_QPSG           },
        { "enable_trigger",     SQLITE_DBCONFIG_ENABLE_TRIGGER        },
        { "enable_view",        SQLITE_DBCONFIG_ENABLE_VIEW           },
        { "fts3_tokenizer",     SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER },
        { "legacy_alter_table", SQLITE_DBCONFIG_LEGACY_ALTER_TABLE    },
        { "legacy_file_format", SQLITE_DBCONFIG_LEGACY_FILE_FORMAT    },
        { "load_extension",     SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION },
        { "no_ckpt_on_close",   SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE      },
        { "reset_database",     SQLITE_DBCONFIG_RESET_DATABASE        },
        { "trigger_eqp",        SQLITE_DBCONFIG_TRIGGER_EQP           },
        { "trusted_schema",     SQLITE_DBCONFIG_TRUSTED_SCHEMA        },
        { "writable_schema",    SQLITE_DBCONFIG_WRITABLE_SCHEMA       },
    };
    Tcl_Obj *pResult;
    int ii;
    if( objc>4 ){
      Tcl_WrongNumArgs(interp, 2, objv, "?OPTION? ?BOOLEAN?");
      return TCL_ERROR;
    }
    if( objc==2 ){
      /* With no arguments, list all configuration options and with the
      ** current value */
      pResult = Tcl_NewListObj(0,0);
      for(ii=0; ii<sizeof(aDbConfig)/sizeof(aDbConfig[0]); ii++){
        int v = 0;
        sqlite3_db_config(pDb->db, aDbConfig[ii].op, -1, &v);
        Tcl_ListObjAppendElement(interp, pResult,
           Tcl_NewStringObj(aDbConfig[ii].zName,-1));
        Tcl_ListObjAppendElement(interp, pResult,
           Tcl_NewIntObj(v));
      }
    }else{
      const char *zOpt = Tcl_GetString(objv[2]);
      int onoff = -1;
      int v = 0;
      if( zOpt[0]=='-' ) zOpt++;
      for(ii=0; ii<sizeof(aDbConfig)/sizeof(aDbConfig[0]); ii++){
        if( strcmp(aDbConfig[ii].zName, zOpt)==0 ) break;
      }
      if( ii>=sizeof(aDbConfig)/sizeof(aDbConfig[0]) ){
        Tcl_AppendResult(interp, "unknown config option: \"", zOpt,
                                "\"", (void*)0);
        return TCL_ERROR;
      }
      if( objc==4 ){
        if( Tcl_GetBooleanFromObj(interp, objv[3], &onoff) ){
          return TCL_ERROR;
        }
      }
      sqlite3_db_config(pDb->db, aDbConfig[ii].op, onoff, &v);
      pResult = Tcl_NewIntObj(v);
    }
    Tcl_SetObjResult(interp, pResult);
    break;
  }

  /*    $db copy conflict-algorithm table filename ?SEPARATOR? ?NULLINDICATOR?
  **
  ** Copy data into table from filename, optionally using SEPARATOR
  ** as column separators.  If a column contains a null string, or the
  ** value of NULLINDICATOR, a NULL is inserted for the column.
  ** conflict-algorithm is one of the sqlite conflict algorithms:
  **    rollback, abort, fail, ignore, replace
  ** On success, return the number of lines processed, not necessarily same
  ** as 'db changes' due to conflict-algorithm selected.
  **
  ** This code is basically an implementation/enhancement of
  ** the sqlite3 shell.c ".import" command.
  **
  ** This command usage is equivalent to the sqlite2.x COPY statement,
  ** which imports file data into a table using the PostgreSQL COPY file format:
  **   $db copy $conflict_algorithm $table_name $filename \t \\N
  */
  case DB_COPY: {
    char *zTable;               /* Insert data into this table */
    char *zFile;                /* The file from which to extract data */
    char *zConflict;            /* The conflict algorithm to use */
    sqlite3_stmt *pStmt;        /* A statement */
    int nCol;                   /* Number of columns in the table */
    int nByte;                  /* Number of bytes in an SQL string */
    int i, j;                   /* Loop counters */
    int nSep;                   /* Number of bytes in zSep[] */
    int nNull;                  /* Number of bytes in zNull[] */
    char *zSql;                 /* An SQL statement */
    char *zLine;                /* A single line of input from the file */
    char **azCol;               /* zLine[] broken up into columns */
    const char *zCommit;        /* How to commit changes */
    Tcl_Channel in;             /* The input file */
    int lineno = 0;             /* Line number of input file */
    char zLineNum[80];          /* Line number print buffer */
    Tcl_Obj *str;
    Tcl_Obj *pResult;           /* interp result */

    const char *zSep;
    const char *zNull;
    if( objc<5 || objc>7 ){
      Tcl_WrongNumArgs(interp, 2, objv,
         "CONFLICT-ALGORITHM TABLE FILENAME ?SEPARATOR? ?NULLINDICATOR?");
      return TCL_ERROR;
    }
    if( objc>=6 ){
      zSep = Tcl_GetStringFromObj(objv[5], 0);
    }else{
      zSep = "\t";
    }
    if( objc>=7 ){
      zNull = Tcl_GetStringFromObj(objv[6], 0);
    }else{
      zNull = "";
    }
    zConflict = Tcl_GetStringFromObj(objv[2], 0);
    zTable = Tcl_GetStringFromObj(objv[3], 0);
    zFile = Tcl_GetStringFromObj(objv[4], 0);
    nSep = strlen30(zSep);
    nNull = strlen30(zNull);
    if( nSep==0 ){
      Tcl_AppendResult(interp,"Error: non-null separator required for copy",
                       (char*)0);
      return TCL_ERROR;
    }
    if(strcmp(zConflict, "rollback") != 0 &&
       strcmp(zConflict, "abort"   ) != 0 &&
       strcmp(zConflict, "fail"    ) != 0 &&
       strcmp(zConflict, "ignore"  ) != 0 &&
       strcmp(zConflict, "replace" ) != 0 ) {
      Tcl_AppendResult(interp, "Error: \"", zConflict,
            "\", conflict-algorithm must be one of: rollback, "
            "abort, fail, ignore, or replace", (char*)0);
      return TCL_ERROR;
    }
    zSql = sqlite3_mprintf("SELECT * FROM '%q'", zTable);
    if( zSql==0 ){
      Tcl_AppendResult(interp, "Error: no such table: ", zTable, (char*)0);
      return TCL_ERROR;
    }
    nByte = strlen30(zSql);
    rc = sqlite3_prepare(pDb->db, zSql, -1, &pStmt, 0);
    sqlite3_free(zSql);
    if( rc ){
      Tcl_AppendResult(interp, "Error: ", sqlite3_errmsg(pDb->db), (char*)0);
      nCol = 0;
    }else{
      nCol = sqlite3_column_count(pStmt);
    }
    sqlite3_finalize(pStmt);
    if( nCol==0 ) {
      return TCL_ERROR;
    }
    zSql = malloc( nByte + 50 + nCol*2 );
    if( zSql==0 ) {
      Tcl_AppendResult(interp, "Error: can't malloc()", (char*)0);
      return TCL_ERROR;
    }
    sqlite3_snprintf(nByte+50, zSql, "INSERT OR %q INTO '%q' VALUES(?",
         zConflict, zTable);
    j = strlen30(zSql);
    for(i=1; i<nCol; i++){
      zSql[j++] = ',';
      zSql[j++] = '?';
    }
    zSql[j++] = ')';
    zSql[j] = 0;
    rc = sqlite3_prepare(pDb->db, zSql, -1, &pStmt, 0);
    free(zSql);
    if( rc ){
      Tcl_AppendResult(interp, "Error: ", sqlite3_errmsg(pDb->db), (char*)0);
      sqlite3_finalize(pStmt);
      return TCL_ERROR;
    }
    in = Tcl_OpenFileChannel(interp, zFile, "rb", 0666);
    if( in==0 ){
      sqlite3_finalize(pStmt);
      return TCL_ERROR;
    }
    Tcl_SetChannelOption(NULL, in, "-translation", "auto");
    azCol = malloc( sizeof(azCol[0])*(nCol+1) );
    if( azCol==0 ) {
      Tcl_AppendResult(interp, "Error: can't malloc()", (char*)0);
      Tcl_Close(interp, in);
      return TCL_ERROR;
    }
    str = Tcl_NewObj();
    Tcl_IncrRefCount(str);
    (void)sqlite3_exec(pDb->db, "BEGIN", 0, 0, 0);
    zCommit = "COMMIT";
    while( Tcl_GetsObj(in, str)>=0 ) {
      char *z;
      Tcl_Size byteLen;
      lineno++;
      zLine = (char *)Tcl_GetByteArrayFromObj(str, &byteLen);
      azCol[0] = zLine;
      for(i=0, z=zLine; *z; z++){
        if( *z==zSep[0] && strncmp(z, zSep, nSep)==0 ){
          *z = 0;
          i++;
          if( i<nCol ){
            azCol[i] = &z[nSep];
            z += nSep-1;
          }
        }
      }
      if( i+1!=nCol ){
        char *zErr;
        int nErr = strlen30(zFile) + 200;
        zErr = malloc(nErr);
        if( zErr ){
          sqlite3_snprintf(nErr, zErr,
             "Error: %s line %d: expected %d columns of data but found %d",
             zFile, lineno, nCol, i+1);
          Tcl_AppendResult(interp, zErr, (char*)0);
          free(zErr);
        }
        zCommit = "ROLLBACK";
        break;
      }
      for(i=0; i<nCol; i++){
        /* check for null data, if so, bind as null */
        if( (nNull>0 && strcmp(azCol[i], zNull)==0)
          || strlen30(azCol[i])==0
        ){
          sqlite3_bind_null(pStmt, i+1);
        }else{
          sqlite3_bind_text(pStmt, i+1, azCol[i], -1, SQLITE_STATIC);
        }
      }
      sqlite3_step(pStmt);
      rc = sqlite3_reset(pStmt);
      Tcl_SetObjLength(str, 0);
      if( rc!=SQLITE_OK ){
        Tcl_AppendResult(interp,"Error: ", sqlite3_errmsg(pDb->db), (char*)0);
        zCommit = "ROLLBACK";
        break;
      }
    }
    Tcl_DecrRefCount(str);
    free(azCol);
    Tcl_Close(interp, in);
    sqlite3_finalize(pStmt);
    (void)sqlite3_exec(pDb->db, zCommit, 0, 0, 0);

    if( zCommit[0] == 'C' ){
      /* success, set result as number of lines processed */
      pResult = Tcl_GetObjResult(interp);
      Tcl_SetIntObj(pResult, lineno);
      rc = TCL_OK;
    }else{
      /* failure, append lineno where failed */
      sqlite3_snprintf(sizeof(zLineNum), zLineNum,"%d",lineno);
      Tcl_AppendResult(interp,", failed while processing line: ",zLineNum,
                       (char*)0);
      rc = TCL_ERROR;
    }
    break;
  }

  /*
  **     $db deserialize ?-maxsize N? ?-readonly BOOL? ?DATABASE? VALUE
  **
  ** Reopen DATABASE (default "main") using the content in $VALUE
  */
  case DB_DESERIALIZE: {
#ifdef SQLITE_OMIT_DESERIALIZE
    Tcl_AppendResult(interp, "MEMDB not available in this build",
                     (char*)0);
    rc = TCL_ERROR;
#else
    const char *zSchema = 0;
    Tcl_Obj *pValue = 0;
    unsigned char *pBA;
    unsigned char *pData;
    Tcl_Size len;
    int xrc;
    sqlite3_int64 mxSize = 0;
    int i;
    int isReadonly = 0;


    if( objc<3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "?DATABASE? VALUE");
      rc = TCL_ERROR;
      break;
    }
    for(i=2; i<objc-1; i++){
      const char *z = Tcl_GetString(objv[i]);
      if( strcmp(z,"-maxsize")==0 && i<objc-2 ){
        Tcl_WideInt x;
        rc = Tcl_GetWideIntFromObj(interp, objv[++i], &x);
        if( rc ) goto deserialize_error;
        mxSize = x;
        continue;
      }
      if( strcmp(z,"-readonly")==0 && i<objc-2 ){
        rc = Tcl_GetBooleanFromObj(interp, objv[++i], &isReadonly);
        if( rc ) goto deserialize_error;
        continue;
      }
      if( zSchema==0 && i==objc-2 && z[0]!='-' ){
        zSchema = z;
        continue;
      }
      Tcl_AppendResult(interp, "unknown option: ", z, (char*)0);
      rc = TCL_ERROR;
      goto deserialize_error;
    }
    pValue = objv[objc-1];
    pBA = Tcl_GetByteArrayFromObj(pValue, &len);
    pData = sqlite3_malloc64( len );
    if( pData==0 && len>0 ){
      Tcl_AppendResult(interp, "out of memory", (char*)0);
      rc = TCL_ERROR;
    }else{
      int flags;
      if( len>0 ) memcpy(pData, pBA, len);
      if( isReadonly ){
        flags = SQLITE_DESERIALIZE_FREEONCLOSE | SQLITE_DESERIALIZE_READONLY;
      }else{
        flags = SQLITE_DESERIALIZE_FREEONCLOSE | SQLITE_DESERIALIZE_RESIZEABLE;
      }
      xrc = sqlite3_deserialize(pDb->db, zSchema, pData, len, len, flags);
      if( xrc ){
        Tcl_AppendResult(interp, "unable to set MEMDB content", (char*)0);
        rc = TCL_ERROR;
      }
      if( mxSize>0 ){
        sqlite3_file_control(pDb->db, zSchema,SQLITE_FCNTL_SIZE_LIMIT,&mxSize);
      }
    }
deserialize_error:
#endif
    break;
  }

  /*
  **    $db enable_load_extension BOOLEAN
  **
  ** Turn the extension loading feature on or off.  It if off by
  ** default.
  */
  case DB_ENABLE_LOAD_EXTENSION: {
#ifndef SQLITE_OMIT_LOAD_EXTENSION
    int onoff;
    if( objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "BOOLEAN");
      return TCL_ERROR;
    }
    if( Tcl_GetBooleanFromObj(interp, objv[2], &onoff) ){
      return TCL_ERROR;
    }
    sqlite3_enable_load_extension(pDb->db, onoff);
    break;
#else
    Tcl_AppendResult(interp, "extension loading is turned off at compile-time",
                     (char*)0);
    return TCL_ERROR;
#endif
  }

  /*
  **    $db errorcode
  **
  ** Return the numeric error code that was returned by the most recent
  ** call to sqlite3_exec().
  */
  case DB_ERRORCODE: {
    Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_errcode(pDb->db)));
    break;
  }

  /*
  **    $db erroroffset
  **
  ** Return the numeric error code that was returned by the most recent
  ** call to sqlite3_exec().
  */
  case DB_ERROROFFSET: {
    Tcl_SetObjResult(interp, Tcl_NewIntObj(sqlite3_error_offset(pDb->db)));
    break;
  }

  /*
  **    $db exists $sql
  **    $db onecolumn $sql
  **
  ** The onecolumn method is the equivalent of:
  **     lindex [$db eval $sql] 0
  */
  case DB_EXISTS:
  case DB_ONECOLUMN: {
    Tcl_Obj *pResult = 0;
    DbEvalContext sEval;
    if( objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "SQL");
      return TCL_ERROR;
    }

    dbEvalInit(&sEval, pDb, objv[2], 0, 0);
    rc = dbEvalStep(&sEval);
    if( choice==DB_ONECOLUMN ){
      if( rc==TCL_OK ){
        pResult = dbEvalColumnValue(&sEval, 0);
      }else if( rc==TCL_BREAK ){
        Tcl_ResetResult(interp);
      }
    }else if( rc==TCL_BREAK || rc==TCL_OK ){
      pResult = Tcl_NewBooleanObj(rc==TCL_OK);
    }
    dbEvalFinalize(&sEval);
    if( pResult ) Tcl_SetObjResult(interp, pResult);

    if( rc==TCL_BREAK ){
      rc = TCL_OK;
    }
    break;
  }

  /*
  **    $db eval ?options? $sql ?varName? ?{  ...code... }?
  **
  ** The SQL statement in $sql is evaluated.  For each row, the values
  ** are placed in elements of the array or dict named $varName and
  ** ...code... is executed.  If $varName and $code are omitted, then
  ** no callback is ever invoked.  If $varName is an empty string,
  ** then the values are placed in variables that have the same name
  ** as the fields extracted by the query, and those variables are
  ** accessible during the eval of $code.
  */
  case DB_EVAL: {
    int evalFlags = 0;
    const char *zOpt;
    while( objc>3 && (zOpt = Tcl_GetString(objv[2]))!=0 && zOpt[0]=='-' ){
      if( strcmp(zOpt, "-withoutnulls")==0 ){
        evalFlags |= SQLITE_EVAL_WITHOUTNULLS;
      }else if( strcmp(zOpt, "-asdict")==0 ){
        evalFlags |= SQLITE_EVAL_ASDICT;
      }else{
        Tcl_AppendResult(interp, "unknown option: \"", zOpt, "\"", (void*)0);
        return TCL_ERROR;
      }
      objc--;
      objv++;
    }
    if( objc<3 || objc>5 ){
      Tcl_WrongNumArgs(interp, 2, objv,
          "?OPTIONS? SQL ?VAR-NAME? ?SCRIPT?");
      return TCL_ERROR;
    }

    if( objc==3 ){
      DbEvalContext sEval;
      Tcl_Obj *pRet = Tcl_NewObj();
      Tcl_IncrRefCount(pRet);
      dbEvalInit(&sEval, pDb, objv[2], 0, 0);
      while( TCL_OK==(rc = dbEvalStep(&sEval)) ){
        int i;
        int nCol;
        dbEvalRowInfo(&sEval, &nCol, 0);
        for(i=0; i<nCol; i++){
          Tcl_ListObjAppendElement(interp, pRet, dbEvalColumnValue(&sEval, i));
        }
      }
      dbEvalFinalize(&sEval);
      if( rc==TCL_BREAK ){
        Tcl_SetObjResult(interp, pRet);
        rc = TCL_OK;
      }
      Tcl_DecrRefCount(pRet);
    }else{
      ClientData cd2[2];
      DbEvalContext *p;
      Tcl_Obj *pVarName = 0;
      Tcl_Obj *pScript;

      if( objc>=5 && *(char *)Tcl_GetString(objv[3]) ){
        pVarName = objv[3];
      }
      pScript = objv[objc-1];
      Tcl_IncrRefCount(pScript);

      p = (DbEvalContext *)Tcl_Alloc(sizeof(DbEvalContext));
      dbEvalInit(p, pDb, objv[2], pVarName, evalFlags);

      cd2[0] = (void *)p;
      cd2[1] = (void *)pScript;
      rc = DbEvalNextCmd(cd2, interp, TCL_OK);
    }
    break;
  }

  /*
  **     $db format [OPTIONS] SQL
  **
  ** Run the SQL statement(s) given as the final argument.  Use the
  ** Query Result Formatter extension of SQLite to format the output as
  ** text and return that text.
  */
  case DB_FORMAT: {
    rc = dbQrf(pDb, objc, objv);
    break;
  }

  /*
  **     $db function NAME [OPTIONS] SCRIPT
  **
  ** Create a new SQL function called NAME.  Whenever that function is
  ** called, invoke SCRIPT to evaluate the function.
  **
  ** Options:
  **         --argcount N           Function has exactly N arguments
  **         --deterministic        The function is pure
  **         --directonly           Prohibit use inside triggers and views
  **         --innocuous            Has no side effects or information leaks
  **         --returntype TYPE      Specify the return type of the function
  */
  case DB_FUNCTION: {
    int flags = SQLITE_UTF8;
    SqlFunc *pFunc;
    Tcl_Obj *pScript;
    char *zName;
    int nArg = -1;
    int i;
    int eType = SQLITE_NULL;
    if( objc<4 ){
      Tcl_WrongNumArgs(interp, 2, objv, "NAME ?SWITCHES? SCRIPT");
      return TCL_ERROR;
    }
    for(i=3; i<(objc-1); i++){
      const char *z = Tcl_GetString(objv[i]);
      int n = strlen30(z);
      if( n>1 && strncmp(z, "-argcount",n)==0 ){
        if( i==(objc-2) ){
          Tcl_AppendResult(interp, "option requires an argument: ", z,(char*)0);
          return TCL_ERROR;
        }
        if( Tcl_GetIntFromObj(interp, objv[i+1], &nArg) ) return TCL_ERROR;
        if( nArg<0 ){
          Tcl_AppendResult(interp, "number of arguments must be non-negative",
                           (char*)0);
          return TCL_ERROR;
        }
        i++;
      }else
      if( n>1 && strncmp(z, "-deterministic",n)==0 ){
        flags |= SQLITE_DETERMINISTIC;
      }else
      if( n>1 && strncmp(z, "-directonly",n)==0 ){
        flags |= SQLITE_DIRECTONLY;
      }else
      if( n>1 && strncmp(z, "-innocuous",n)==0 ){
        flags |= SQLITE_INNOCUOUS;
      }else
      if( n>1 && strncmp(z, "-returntype", n)==0 ){
        const char *azType[] = {"integer", "real", "text", "blob", "any", 0};
        assert( SQLITE_INTEGER==1 && SQLITE_FLOAT==2 && SQLITE_TEXT==3 );
        assert( SQLITE_BLOB==4 && SQLITE_NULL==5 );
        if( i==(objc-2) ){
          Tcl_AppendResult(interp, "option requires an argument: ", z,(char*)0);
          return TCL_ERROR;
        }
        i++;
        if( Tcl_GetIndexFromObj(interp, objv[i], azType, "type", 0, &eType) ){
          return TCL_ERROR;
        }
        eType++;
      }else{
        Tcl_AppendResult(interp, "bad option \"", z,
            "\": must be -argcount, -deterministic, -directonly,"
            " -innocuous, or -returntype", (char*)0
        );
        return TCL_ERROR;
      }
    }

    pScript = objv[objc-1];
    zName = Tcl_GetStringFromObj(objv[2], 0);
    pFunc = findSqlFunc(pDb, zName);
    if( pFunc==0 ) return TCL_ERROR;
    if( pFunc->pScript ){
      Tcl_DecrRefCount(pFunc->pScript);
    }
    pFunc->pScript = pScript;
    Tcl_IncrRefCount(pScript);
    pFunc->useEvalObjv = safeToUseEvalObjv(pScript);
    pFunc->eType = eType;
    rc = sqlite3_create_function(pDb->db, zName, nArg, flags,
        pFunc, tclSqlFunc, 0, 0);
    if( rc!=SQLITE_OK ){
      rc = TCL_ERROR;
      Tcl_SetResult(interp, (char *)sqlite3_errmsg(pDb->db), TCL_VOLATILE);
    }
    break;
  }

  /*
  **     $db incrblob ?-readonly? ?DB? TABLE COLUMN ROWID
  */
  case DB_INCRBLOB: {
#ifdef SQLITE_OMIT_INCRBLOB
    Tcl_AppendResult(interp, "incrblob not available in this build", (char*)0);
    return TCL_ERROR;
#else
    int isReadonly = 0;
    const char *zDb = "main";
    const char *zTable;
    const char *zColumn;
    Tcl_WideInt iRow;

    /* Check for the -readonly option */
    if( objc>3 && strcmp(Tcl_GetString(objv[2]), "-readonly")==0 ){
      isReadonly = 1;
    }

    if( objc!=(5+isReadonly) && objc!=(6+isReadonly) ){
      Tcl_WrongNumArgs(interp, 2, objv, "?-readonly? ?DB? TABLE COLUMN ROWID");
      return TCL_ERROR;
    }

    if( objc==(6+isReadonly) ){
      zDb = Tcl_GetString(objv[2+isReadonly]);
    }
    zTable = Tcl_GetString(objv[objc-3]);
    zColumn = Tcl_GetString(objv[objc-2]);
    rc = Tcl_GetWideIntFromObj(interp, objv[objc-1], &iRow);

    if( rc==TCL_OK ){
      rc = createIncrblobChannel(
          interp, pDb, zDb, zTable, zColumn, (sqlite3_int64)iRow, isReadonly
      );
    }
#endif
    break;
  }

  /*
  **     $db interrupt
  **
  ** Interrupt the execution of the inner-most SQL interpreter.  This
  ** causes the SQL statement to return an error of SQLITE_INTERRUPT.
  */
  case DB_INTERRUPT: {
    sqlite3_interrupt(pDb->db);
    break;
  }

  /*
  **     $db nullvalue ?STRING?
  **
  ** Change text used when a NULL comes back from the database. If ?STRING?
  ** is not present, then the current string used for NULL is returned.
  ** If STRING is present, then STRING is returned.
  **
  */
  case DB_NULLVALUE: {
    if( objc!=2 && objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "NULLVALUE");
      return TCL_ERROR;
    }
    if( objc==3 ){
      Tcl_Size len;
      char *zNull = Tcl_GetStringFromObj(objv[2], &len);
      if( pDb->zNull ){
        Tcl_Free(pDb->zNull);
      }
      if( zNull && len>0 ){
        pDb->zNull = Tcl_Alloc( len + 1 );
        memcpy(pDb->zNull, zNull, len);
        pDb->zNull[len] = '\0';
      }else{
        pDb->zNull = 0;
      }
    }
    Tcl_SetObjResult(interp, Tcl_NewStringObj(pDb->zNull, -1));
    break;
  }

  /*
  **     $db last_insert_rowid
  **
  ** Return an integer which is the ROWID for the most recent insert.
  */
  case DB_LAST_INSERT_ROWID: {
    Tcl_Obj *pResult;
    Tcl_WideInt rowid;
    if( objc!=2 ){
      Tcl_WrongNumArgs(interp, 2, objv, "");
      return TCL_ERROR;
    }
    rowid = sqlite3_last_insert_rowid(pDb->db);
    pResult = Tcl_GetObjResult(interp);
    Tcl_SetWideIntObj(pResult, rowid);
    break;
  }

  /*
  ** The DB_ONECOLUMN method is implemented together with DB_EXISTS.
  */

  /*    $db progress ?N CALLBACK?
  **
  ** Invoke the given callback every N virtual machine opcodes while executing
  ** queries.
  */
  case DB_PROGRESS: {
    if( objc==2 ){
      if( pDb->zProgress ){
        Tcl_AppendResult(interp, pDb->zProgress, (char*)0);
      }
#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
      sqlite3_progress_handler(pDb->db, 0, 0, 0);
#endif
    }else if( objc==4 ){
      char *zProgress;
      Tcl_Size len;
      int N;
      if( TCL_OK!=Tcl_GetIntFromObj(interp, objv[2], &N) ){
        return TCL_ERROR;
      };
      if( pDb->zProgress ){
        Tcl_Free(pDb->zProgress);
      }
      zProgress = Tcl_GetStringFromObj(objv[3], &len);
      if( zProgress && len>0 ){
        pDb->zProgress = Tcl_Alloc( len + 1 );
        memcpy(pDb->zProgress, zProgress, len+1);
      }else{
        pDb->zProgress = 0;
      }
#ifndef SQLITE_OMIT_PROGRESS_CALLBACK
      if( pDb->zProgress ){
        pDb->interp = interp;
        sqlite3_progress_handler(pDb->db, N, DbProgressHandler, pDb);
      }else{
        sqlite3_progress_handler(pDb->db, 0, 0, 0);
      }
#endif
    }else{
      Tcl_WrongNumArgs(interp, 2, objv, "N CALLBACK");
      return TCL_ERROR;
    }
    break;
  }

  /*    $db profile ?CALLBACK?
  **
  ** Make arrangements to invoke the CALLBACK routine after each SQL statement
  ** that has run.  The text of the SQL and the amount of elapse time are
  ** appended to CALLBACK before the script is run.
  */
  case DB_PROFILE: {
    if( objc>3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "?CALLBACK?");
      return TCL_ERROR;
    }else if( objc==2 ){
      if( pDb->zProfile ){
        Tcl_AppendResult(interp, pDb->zProfile, (char*)0);
      }
    }else{
      char *zProfile;
      Tcl_Size len;
      if( pDb->zProfile ){
        Tcl_Free(pDb->zProfile);
      }
      zProfile = Tcl_GetStringFromObj(objv[2], &len);
      if( zProfile && len>0 ){
        pDb->zProfile = Tcl_Alloc( len + 1 );
        memcpy(pDb->zProfile, zProfile, len+1);
      }else{
        pDb->zProfile = 0;
      }
#if !defined(SQLITE_OMIT_TRACE) && !defined(SQLITE_OMIT_FLOATING_POINT) && \
    !defined(SQLITE_OMIT_DEPRECATED)
      if( pDb->zProfile ){
        pDb->interp = interp;
        sqlite3_profile(pDb->db, DbProfileHandler, pDb);
      }else{
        sqlite3_profile(pDb->db, 0, 0);
      }
#endif
    }
    break;
  }

  /*
  **     $db rekey KEY
  **
  ** Change the encryption key on the currently open database.
  */
  case DB_REKEY: {
    if( objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "KEY");
      return TCL_ERROR;
    }
    break;
  }

  /*    $db restore ?DATABASE? FILENAME
  **
  ** Open a database file named FILENAME.  Transfer the content
  ** of FILENAME into the local database DATABASE (default: "main").
  */
  case DB_RESTORE: {
    const char *zSrcFile;
    const char *zDestDb;
    sqlite3 *pSrc;
    sqlite3_backup *pBackup;
    int nTimeout = 0;

    if( objc==3 ){
      zDestDb = "main";
      zSrcFile = Tcl_GetString(objv[2]);
    }else if( objc==4 ){
      zDestDb = Tcl_GetString(objv[2]);
      zSrcFile = Tcl_GetString(objv[3]);
    }else{
      Tcl_WrongNumArgs(interp, 2, objv, "?DATABASE? FILENAME");
      return TCL_ERROR;
    }
    rc = sqlite3_open_v2(zSrcFile, &pSrc,
                         SQLITE_OPEN_READONLY | pDb->openFlags, 0);
    if( rc!=SQLITE_OK ){
      Tcl_AppendResult(interp, "cannot open source database: ",
           sqlite3_errmsg(pSrc), (char*)0);
      sqlite3_close(pSrc);
      return TCL_ERROR;
    }
    pBackup = sqlite3_backup_init(pDb->db, zDestDb, pSrc, "main");
    if( pBackup==0 ){
      Tcl_AppendResult(interp, "restore failed: ",
           sqlite3_errmsg(pDb->db), (char*)0);
      sqlite3_close(pSrc);
      return TCL_ERROR;
    }
    while( (rc = sqlite3_backup_step(pBackup,100))==SQLITE_OK
              || rc==SQLITE_BUSY ){
      if( rc==SQLITE_BUSY ){
        if( nTimeout++ >= 3 ) break;
        sqlite3_sleep(100);
      }
    }
    sqlite3_backup_finish(pBackup);
    if( rc==SQLITE_DONE ){
      rc = TCL_OK;
    }else if( rc==SQLITE_BUSY || rc==SQLITE_LOCKED ){
      Tcl_AppendResult(interp, "restore failed: source database busy",
                       (char*)0);
      rc = TCL_ERROR;
    }else{
      Tcl_AppendResult(interp, "restore failed: ",
           sqlite3_errmsg(pDb->db), (char*)0);
      rc = TCL_ERROR;
    }
    sqlite3_close(pSrc);
    break;
  }

  /*
  **     $db serialize ?DATABASE?
  **
  ** Return a serialization of a database.
  */
  case DB_SERIALIZE: {
#ifdef SQLITE_OMIT_DESERIALIZE
    Tcl_AppendResult(interp, "MEMDB not available in this build",
                     (char*)0);
    rc = TCL_ERROR;
#else
    const char *zSchema = objc>=3 ? Tcl_GetString(objv[2]) : "main";
    sqlite3_int64 sz = 0;
    unsigned char *pData;
    if( objc!=2 && objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "?DATABASE?");
      rc = TCL_ERROR;
    }else{
      int needFree;
      pData = sqlite3_serialize(pDb->db, zSchema, &sz, SQLITE_SERIALIZE_NOCOPY);
      if( pData ){
        needFree = 0;
      }else{
        pData = sqlite3_serialize(pDb->db, zSchema, &sz, 0);
        needFree = 1;
      }
      Tcl_SetObjResult(interp, Tcl_NewByteArrayObj(pData,sz));
      if( needFree ) sqlite3_free(pData);
    }
#endif
    break;
  }

  /*
  **     $db status (step|sort|autoindex|vmstep)
  **
  ** Display SQLITE_STMTSTATUS_FULLSCAN_STEP or
  ** SQLITE_STMTSTATUS_SORT for the most recent eval.
  */
  case DB_STATUS: {
    int v;
    const char *zOp;
    if( objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "(step|sort|autoindex)");
      return TCL_ERROR;
    }
    zOp = Tcl_GetString(objv[2]);
    if( strcmp(zOp, "step")==0 ){
      v = pDb->nStep;
    }else if( strcmp(zOp, "sort")==0 ){
      v = pDb->nSort;
    }else if( strcmp(zOp, "autoindex")==0 ){
      v = pDb->nIndex;
    }else if( strcmp(zOp, "vmstep")==0 ){
      v = pDb->nVMStep;
    }else{
      Tcl_AppendResult(interp,
            "bad argument: should be autoindex, step, sort or vmstep",
            (char*)0);
      return TCL_ERROR;
    }
    Tcl_SetObjResult(interp, Tcl_NewIntObj(v));
    break;
  }

  /*
  **     $db timeout MILLESECONDS
  **
  ** Delay for the number of milliseconds specified when a file is locked.
  */
  case DB_TIMEOUT: {
    int ms;
    if( objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "MILLISECONDS");
      return TCL_ERROR;
    }
    if( Tcl_GetIntFromObj(interp, objv[2], &ms) ) return TCL_ERROR;
    sqlite3_busy_timeout(pDb->db, ms);
    break;
  }

  /*
  **     $db total_changes
  **
  ** Return the number of rows that were modified, inserted, or deleted
  ** since the database handle was created.
  */
  case DB_TOTAL_CHANGES: {
    Tcl_Obj *pResult;
    if( objc!=2 ){
      Tcl_WrongNumArgs(interp, 2, objv, "");
      return TCL_ERROR;
    }
    pResult = Tcl_GetObjResult(interp);
    Tcl_SetWideIntObj(pResult, sqlite3_total_changes64(pDb->db));
    break;
  }

  /*    $db trace ?CALLBACK?
  **
  ** Make arrangements to invoke the CALLBACK routine for each SQL statement
  ** that is executed.  The text of the SQL is appended to CALLBACK before
  ** it is executed.
  */
  case DB_TRACE: {
    if( objc>3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "?CALLBACK?");
      return TCL_ERROR;
    }else if( objc==2 ){
      if( pDb->zTrace ){
        Tcl_AppendResult(interp, pDb->zTrace, (char*)0);
      }
    }else{
      char *zTrace;
      Tcl_Size len;
      if( pDb->zTrace ){
        Tcl_Free(pDb->zTrace);
      }
      zTrace = Tcl_GetStringFromObj(objv[2], &len);
      if( zTrace && len>0 ){
        pDb->zTrace = Tcl_Alloc( len + 1 );
        memcpy(pDb->zTrace, zTrace, len+1);
      }else{
        pDb->zTrace = 0;
      }
#if !defined(SQLITE_OMIT_TRACE) && !defined(SQLITE_OMIT_FLOATING_POINT) && \
    !defined(SQLITE_OMIT_DEPRECATED)
      if( pDb->zTrace ){
        pDb->interp = interp;
        sqlite3_trace(pDb->db, DbTraceHandler, pDb);
      }else{
        sqlite3_trace(pDb->db, 0, 0);
      }
#endif
    }
    break;
  }

  /*    $db trace_v2 ?CALLBACK? ?MASK?
  **
  ** Make arrangements to invoke the CALLBACK routine for each trace event
  ** matching the mask that is generated.  The parameters are appended to
  ** CALLBACK before it is executed.
  */
  case DB_TRACE_V2: {
    if( objc>4 ){
      Tcl_WrongNumArgs(interp, 2, objv, "?CALLBACK? ?MASK?");
      return TCL_ERROR;
    }else if( objc==2 ){
      if( pDb->zTraceV2 ){
        Tcl_AppendResult(interp, pDb->zTraceV2, (char*)0);
      }
    }else{
      char *zTraceV2;
      Tcl_Size len;
      Tcl_WideInt wMask = 0;
      if( objc==4 ){
        static const char *TTYPE_strs[] = {
          "statement", "profile", "row", "close", 0
        };
        enum TTYPE_enum {
          TTYPE_STMT, TTYPE_PROFILE, TTYPE_ROW, TTYPE_CLOSE
        };
        Tcl_Size i;
        if( TCL_OK!=Tcl_ListObjLength(interp, objv[3], &len) ){
          return TCL_ERROR;
        }
        for(i=0; i<len; i++){
          Tcl_Obj *pObj;
          int ttype;
          if( TCL_OK!=Tcl_ListObjIndex(interp, objv[3], i, &pObj) ){
            return TCL_ERROR;
          }
          if( Tcl_GetIndexFromObj(interp, pObj, TTYPE_strs, "trace type",
                                  0, &ttype)!=TCL_OK ){
            Tcl_WideInt wType;
            Tcl_Obj *pError = Tcl_DuplicateObj(Tcl_GetObjResult(interp));
            Tcl_IncrRefCount(pError);
            if( TCL_OK==Tcl_GetWideIntFromObj(interp, pObj, &wType) ){
              Tcl_DecrRefCount(pError);
              wMask |= wType;
            }else{
              Tcl_SetObjResult(interp, pError);
              Tcl_DecrRefCount(pError);
              return TCL_ERROR;
            }
          }else{
            switch( (enum TTYPE_enum)ttype ){
              case TTYPE_STMT:    wMask |= SQLITE_TRACE_STMT;    break;
              case TTYPE_PROFILE: wMask |= SQLITE_TRACE_PROFILE; break;
              case TTYPE_ROW:     wMask |= SQLITE_TRACE_ROW;     break;
              case TTYPE_CLOSE:   wMask |= SQLITE_TRACE_CLOSE;   break;
            }
          }
        }
      }else{
        wMask = SQLITE_TRACE_STMT; /* use the "legacy" default */
      }
      if( pDb->zTraceV2 ){
        Tcl_Free(pDb->zTraceV2);
      }
      zTraceV2 = Tcl_GetStringFromObj(objv[2], &len);
      if( zTraceV2 && len>0 ){
        pDb->zTraceV2 = Tcl_Alloc( len + 1 );
        memcpy(pDb->zTraceV2, zTraceV2, len+1);
      }else{
        pDb->zTraceV2 = 0;
      }
#if !defined(SQLITE_OMIT_TRACE) && !defined(SQLITE_OMIT_FLOATING_POINT)
      if( pDb->zTraceV2 ){
        pDb->interp = interp;
        sqlite3_trace_v2(pDb->db, (unsigned)wMask, DbTraceV2Handler, pDb);
      }else{
        sqlite3_trace_v2(pDb->db, 0, 0, 0);
      }
#endif
    }
    break;
  }

  /*    $db transaction [-deferred|-immediate|-exclusive] SCRIPT
  **
  ** Start a new transaction (if we are not already in the midst of a
  ** transaction) and execute the TCL script SCRIPT.  After SCRIPT
  ** completes, either commit the transaction or roll it back if SCRIPT
  ** throws an exception.  Or if no new transaction was started, do nothing.
  ** pass the exception on up the stack.
  **
  ** This command was inspired by Dave Thomas's talk on Ruby at the
  ** 2005 O'Reilly Open Source Convention (OSCON).
  */
  case DB_TRANSACTION: {
    Tcl_Obj *pScript;
    const char *zBegin = "SAVEPOINT _tcl_transaction";
    if( objc!=3 && objc!=4 ){
      Tcl_WrongNumArgs(interp, 2, objv, "[TYPE] SCRIPT");
      return TCL_ERROR;
    }

    if( pDb->nTransaction==0 && objc==4 ){
      static const char *TTYPE_strs[] = {
        "deferred",   "exclusive",  "immediate", 0
      };
      enum TTYPE_enum {
        TTYPE_DEFERRED, TTYPE_EXCLUSIVE, TTYPE_IMMEDIATE
      };
      int ttype;
      if( Tcl_GetIndexFromObj(interp, objv[2], TTYPE_strs, "transaction type",
                              0, &ttype) ){
        return TCL_ERROR;
      }
      switch( (enum TTYPE_enum)ttype ){
        case TTYPE_DEFERRED:    /* no-op */;                 break;
        case TTYPE_EXCLUSIVE:   zBegin = "BEGIN EXCLUSIVE";  break;
        case TTYPE_IMMEDIATE:   zBegin = "BEGIN IMMEDIATE";  break;
      }
    }
    pScript = objv[objc-1];

    /* Run the SQLite BEGIN command to open a transaction or savepoint. */
    pDb->disableAuth++;
    rc = sqlite3_exec(pDb->db, zBegin, 0, 0, 0);
    pDb->disableAuth--;
    if( rc!=SQLITE_OK ){
      Tcl_AppendResult(interp, sqlite3_errmsg(pDb->db), (char*)0);
      return TCL_ERROR;
    }
    pDb->nTransaction++;

    /* If using NRE, schedule a callback to invoke the script pScript, then
    ** a second callback to commit (or rollback) the transaction or savepoint
    ** opened above. If not using NRE, evaluate the script directly, then
    ** call function DbTransPostCmd() to commit (or rollback) the transaction
    ** or savepoint.  */
    addDatabaseRef(pDb);          /* DbTransPostCmd() calls delDatabaseRef() */
    if( DbUseNre() ){
      Tcl_NRAddCallback(interp, DbTransPostCmd, cd, 0, 0, 0);
      (void)Tcl_NREvalObj(interp, pScript, 0);
    }else{
      rc = DbTransPostCmd(&cd, interp, Tcl_EvalObjEx(interp, pScript, 0));
    }
    break;
  }

  /*
  **    $db unlock_notify ?script?
  */
  case DB_UNLOCK_NOTIFY: {
#ifndef SQLITE_ENABLE_UNLOCK_NOTIFY
    Tcl_AppendResult(interp, "unlock_notify not available in this build",
                     (char*)0);
    rc = TCL_ERROR;
#else
    if( objc!=2 && objc!=3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "?SCRIPT?");
      rc = TCL_ERROR;
    }else{
      void (*xNotify)(void **, int) = 0;
      void *pNotifyArg = 0;

      if( pDb->pUnlockNotify ){
        Tcl_DecrRefCount(pDb->pUnlockNotify);
        pDb->pUnlockNotify = 0;
      }

      if( objc==3 ){
        xNotify = DbUnlockNotify;
        pNotifyArg = (void *)pDb;
        pDb->pUnlockNotify = objv[2];
        Tcl_IncrRefCount(pDb->pUnlockNotify);
      }

      if( sqlite3_unlock_notify(pDb->db, xNotify, pNotifyArg) ){
        Tcl_AppendResult(interp, sqlite3_errmsg(pDb->db), (char*)0);
        rc = TCL_ERROR;
      }
    }
#endif
    break;
  }

  /*
  **    $db preupdate_hook count
  **    $db preupdate_hook hook ?SCRIPT?
  **    $db preupdate_hook new INDEX
  **    $db preupdate_hook old INDEX
  */
  case DB_PREUPDATE: {
#ifndef SQLITE_ENABLE_PREUPDATE_HOOK
    Tcl_AppendResult(interp, "preupdate_hook was omitted at compile-time",
                     (char*)0);
    rc = TCL_ERROR;
#else
    static const char *azSub[] = {"count", "depth", "hook", "new", "old", 0};
    enum DbPreupdateSubCmd {
      PRE_COUNT, PRE_DEPTH, PRE_HOOK, PRE_NEW, PRE_OLD
    };
    int iSub;

    if( objc<3 ){
      Tcl_WrongNumArgs(interp, 2, objv, "SUB-COMMAND ?ARGS?");
    }
    if( Tcl_GetIndexFromObj(interp, objv[2], azSub, "sub-command", 0, &iSub) ){
      return TCL_ERROR;
    }

    switch( (enum DbPreupdateSubCmd)iSub ){
      case PRE_COUNT: {
        int nCol = sqlite3_preupdate_count(pDb->db);
        Tcl_SetObjResult(interp, Tcl_NewIntObj(nCol));
        break;
      }

      case PRE_HOOK: {
        if( objc>4 ){
          Tcl_WrongNumArgs(interp, 2, objv, "hook ?SCRIPT?");
          return TCL_ERROR;
        }
        DbHookCmd(interp, pDb, (objc==4 ? objv[3] : 0), &pDb->pPreUpdateHook);
        break;
      }

      case PRE_DEPTH: {
        Tcl_Obj *pRet;
        if( objc!=3 ){
          Tcl_WrongNumArgs(interp, 3, objv, "");
          return TCL_ERROR;
        }
        pRet = Tcl_NewIntObj(sqlite3_preupdate_depth(pDb->db));
        Tcl_SetObjResult(interp, pRet);
        break;
      }

      case PRE_NEW:
      case PRE_OLD: {
        int iIdx;
        sqlite3_value *pValue;
        if( objc!=4 ){
          Tcl_WrongNumArgs(interp, 3, objv, "INDEX");
          return TCL_ERROR;
        }
        if( Tcl_GetIntFromObj(interp, objv[3], &iIdx) ){
          return TCL_ERROR;
        }

        if( iSub==PRE_OLD ){
          rc = sqlite3_preupdate_old(pDb->db, iIdx, &pValue);
        }else{
          assert( iSub==PRE_NEW );
          rc = sqlite3_preupdate_new(pDb->db, iIdx, &pValue);
        }

        if( rc==SQLITE_OK ){
          Tcl_Obj *pObj;
          pObj = Tcl_NewStringObj((char*)sqlite3_value_text(pValue), -1);
          Tcl_SetObjResult(interp, pObj);
        }else{
          Tcl_AppendResult(interp, sqlite3_errmsg(pDb->db), (char*)0);
          return TCL_ERROR;
        }
      }
    }
#endif /* SQLITE_ENABLE_PREUPDATE_HOOK */
    break;
  }

  /*
  **    $db wal_hook ?script?
  **    $db update_hook ?script?
  **    $db rollback_hook ?script?
  */
  case DB_WAL_HOOK:
  case DB_UPDATE_HOOK:
  case DB_ROLLBACK_HOOK: {
    /* set ppHook to point at pUpdateHook or pRollbackHook, depending on
    ** whether [$db update_hook] or [$db rollback_hook] was invoked.
    */
    Tcl_Obj **ppHook = 0;
    if( choice==DB_WAL_HOOK ) ppHook = &pDb->pWalHook;
    if( choice==DB_UPDATE_HOOK ) ppHook = &pDb->pUpdateHook;
    if( choice==DB_ROLLBACK_HOOK ) ppHook = &pDb->pRollbackHook;
    if( objc>3 ){
       Tcl_WrongNumArgs(interp, 2, objv, "?SCRIPT?");
       return TCL_ERROR;
    }

    DbHookCmd(interp, pDb, (objc==3 ? objv[2] : 0), ppHook);
    break;
  }

  /*    $db version
  **
  ** Return the version string for this database.
  */
  case DB_VERSION: {
    int i;
    for(i=2; i<objc; i++){
      const char *zArg = Tcl_GetString(objv[i]);
      /* Optional arguments to $db version are used for testing purpose */
#ifdef SQLITE_TEST
      /* $db version -use-legacy-prepare BOOLEAN
      **
      ** Turn the use of legacy sqlite3_prepare() on or off.
      */
      if( strcmp(zArg, "-use-legacy-prepare")==0 && i+1<objc ){
        i++;
        if( Tcl_GetBooleanFromObj(interp, objv[i], &pDb->bLegacyPrepare) ){
          return TCL_ERROR;
        }
      }else

      /* $db version -last-stmt-ptr
      **
      ** Return a string which is a hex encoding of the pointer to the
      ** most recent sqlite3_stmt in the statement cache.
      */
      if( strcmp(zArg, "-last-stmt-ptr")==0 ){
        char zBuf[100];
        sqlite3_snprintf(sizeof(zBuf), zBuf, "%p",
                         pDb->stmtList ? pDb->stmtList->pStmt: 0);
        Tcl_SetResult(interp, zBuf, TCL_VOLATILE);
      }else
#endif /* SQLITE_TEST */
      {
        Tcl_AppendResult(interp, "unknown argument: ", zArg, (char*)0);
        return TCL_ERROR;
      }
    }
    if( i==2 ){
      Tcl_SetResult(interp, (char *)sqlite3_libversion(), TCL_STATIC);
    }
    break;
  }


  } /* End of the SWITCH statement */
  return rc;
}

#if SQLITE_TCL_NRE
/*
** Adaptor that provides an objCmd interface to the NRE-enabled
** interface implementation.
*/
static int SQLITE_TCLAPI DbObjCmdAdaptor(
  void *cd,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const*objv
){
  return Tcl_NRCallObjProc(interp, DbObjCmd, cd, objc, objv);
}
#endif /* SQLITE_TCL_NRE */

/*
** Issue the usage message when the "sqlite3" command arguments are
** incorrect.
*/
static int sqliteCmdUsage(
  Tcl_Interp *interp,
  Tcl_Obj *const*objv
){
  Tcl_WrongNumArgs(interp, 1, objv,
    "HANDLE ?FILENAME? ?-vfs VFSNAME? ?-readonly BOOLEAN? ?-create BOOLEAN?"
    " ?-nofollow BOOLEAN?"
    " ?-nomutex BOOLEAN? ?-fullmutex BOOLEAN? ?-uri BOOLEAN?"
  );
  return TCL_ERROR;
}

/*
**   sqlite3 DBNAME FILENAME ?-vfs VFSNAME? ?-key KEY? ?-readonly BOOLEAN?
**                           ?-create BOOLEAN? ?-nomutex BOOLEAN?
**                           ?-nofollow BOOLEAN?
**
** This is the main Tcl command.  When the "sqlite" Tcl command is
** invoked, this routine runs to process that command.
**
** The first argument, DBNAME, is an arbitrary name for a new
** database connection.  This command creates a new command named
** DBNAME that is used to control that connection.  The database
** connection is deleted when the DBNAME command is deleted.
**
** The second argument is the name of the database file.
**
*/
static int SQLITE_TCLAPI DbMain(
  void *cd,
  Tcl_Interp *interp,
  int objc,
  Tcl_Obj *const*objv
){
  SqliteDb *p;
  const char *zArg;
  char *zErrMsg;
  int i;
  const char *zFile = 0;
  const char *zVfs = 0;
  int flags;
  int bTranslateFileName = 1;
  Tcl_DString translatedFilename;
  int rc;

  /* In normal use, each TCL interpreter runs in a single thread.  So
  ** by default, we can turn off mutexing on SQLite database connections.
  ** However, for testing purposes it is useful to have mutexes turned
  ** on.  So, by default, mutexes default off.  But if compiled with
  ** SQLITE_TCL_DEFAULT_FULLMUTEX then mutexes default on.
  */
#ifdef SQLITE_TCL_DEFAULT_FULLMUTEX
  flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX;
#else
  flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX;
#endif

  if( objc==1 ) return sqliteCmdUsage(interp, objv);
  if( objc==2 ){
    zArg = Tcl_GetStringFromObj(objv[1], 0);
    if( strcmp(zArg,"-version")==0 ){
      Tcl_AppendResult(interp,sqlite3_libversion(), (char*)0);
      return TCL_OK;
    }
    if( strcmp(zArg,"-sourceid")==0 ){
      Tcl_AppendResult(interp,sqlite3_sourceid(), (char*)0);
      return TCL_OK;
    }
    if( strcmp(zArg,"-has-codec")==0 ){
      Tcl_AppendResult(interp,"0",(char*)0);
      return TCL_OK;
    }
    if( zArg[0]=='-' ) return sqliteCmdUsage(interp, objv);
  }
  for(i=2; i<objc; i++){
    zArg = Tcl_GetString(objv[i]);
    if( zArg[0]!='-' ){
      if( zFile!=0 ) return sqliteCmdUsage(interp, objv);
      zFile = zArg;
      continue;
    }
    if( i==objc-1 ) return sqliteCmdUsage(interp, objv);
    i++;
    if( strcmp(zArg,"-key")==0 ){
      /* no-op */
    }else if( strcmp(zArg, "-vfs")==0 ){
      zVfs = Tcl_GetString(objv[i]);
    }else if( strcmp(zArg, "-readonly")==0 ){
      int b;
      if( Tcl_GetBooleanFromObj(interp, objv[i], &b) ) return TCL_ERROR;
      if( b ){
        flags &= ~(SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE);
        flags |= SQLITE_OPEN_READONLY;
      }else{
        flags &= ~SQLITE_OPEN_READONLY;
        flags |= SQLITE_OPEN_READWRITE;
      }
    }else if( strcmp(zArg, "-create")==0 ){
      int b;
      if( Tcl_GetBooleanFromObj(interp, objv[i], &b) ) return TCL_ERROR;
      if( b && (flags & SQLITE_OPEN_READONLY)==0 ){
        flags |= SQLITE_OPEN_CREATE;
      }else{
        flags &= ~SQLITE_OPEN_CREATE;
      }
    }else if( strcmp(zArg, "-nofollow")==0 ){
      int b;
      if( Tcl_GetBooleanFromObj(interp, objv[i], &b) ) return TCL_ERROR;
      if( b ){
        flags |= SQLITE_OPEN_NOFOLLOW;
      }else{
        flags &= ~SQLITE_OPEN_NOFOLLOW;
      }
    }else if( strcmp(zArg, "-nomutex")==0 ){
      int b;
      if( Tcl_GetBooleanFromObj(interp, objv[i], &b) ) return TCL_ERROR;
      if( b ){
        flags |= SQLITE_OPEN_NOMUTEX;
        flags &= ~SQLITE_OPEN_FULLMUTEX;
      }else{
        flags &= ~SQLITE_OPEN_NOMUTEX;
      }
    }else if( strcmp(zArg, "-fullmutex")==0 ){
      int b;
      if( Tcl_GetBooleanFromObj(interp, objv[i], &b) ) return TCL_ERROR;
      if( b ){
        flags |= SQLITE_OPEN_FULLMUTEX;
        flags &= ~SQLITE_OPEN_NOMUTEX;
      }else{
        flags &= ~SQLITE_OPEN_FULLMUTEX;
      }
    }else if( strcmp(zArg, "-uri")==0 ){
      int b;
      if( Tcl_GetBooleanFromObj(interp, objv[i], &b) ) return TCL_ERROR;
      if( b ){
        flags |= SQLITE_OPEN_URI;
      }else{
        flags &= ~SQLITE_OPEN_URI;
      }
    }else if( strcmp(zArg, "-translatefilename")==0 ){
      if( Tcl_GetBooleanFromObj(interp, objv[i], &bTranslateFileName) ){
        return TCL_ERROR;
      }
    }else{
      Tcl_AppendResult(interp, "unknown option: ", zArg, (char*)0);
      return TCL_ERROR;
    }
  }
  zErrMsg = 0;
  p = (SqliteDb*)Tcl_Alloc( sizeof(*p) );
  memset(p, 0, sizeof(*p));
  if( zFile==0 ) zFile = "";
  if( bTranslateFileName ){
    zFile = Tcl_TranslateFileName(interp, zFile, &translatedFilename);
  }
  rc = sqlite3_open_v2(zFile, &p->db, flags, zVfs);
  if( bTranslateFileName ){
    Tcl_DStringFree(&translatedFilename);
  }
  if( p->db ){
    if( SQLITE_OK!=sqlite3_errcode(p->db) ){
      zErrMsg = sqlite3_mprintf("%s", sqlite3_errmsg(p->db));
      sqlite3_close(p->db);
      p->db = 0;
    }
  }else{
    zErrMsg = sqlite3_mprintf("%s", sqlite3_errstr(rc));
  }
  if( p->db==0 ){
    Tcl_SetResult(interp, zErrMsg, TCL_VOLATILE);
    Tcl_Free((char*)p);
    sqlite3_free(zErrMsg);
    return TCL_ERROR;
  }
  p->maxStmt = NUM_PREPARED_STMTS;
  p->openFlags = flags & SQLITE_OPEN_URI;
  p->interp = interp;
  zArg = Tcl_GetStringFromObj(objv[1], 0);
  if( DbUseNre() ){
    Tcl_NRCreateCommand(interp, zArg, DbObjCmdAdaptor, DbObjCmd,
                        (char*)p, DbDeleteCmd);
  }else{
    Tcl_CreateObjCommand(interp, zArg, DbObjCmd, (char*)p, DbDeleteCmd);
  }
  p->nRef = 1;
  return TCL_OK;
}

/*
** Provide a dummy Tcl_InitStubs if we are using this as a static
** library.
*/
#ifndef USE_TCL_STUBS
# undef  Tcl_InitStubs
# define Tcl_InitStubs(a,b,c) TCL_VERSION
#endif

/*
** Make sure we have a PACKAGE_VERSION macro defined.  This will be
** defined automatically by the TEA makefile.  But other makefiles
** do not define it.
*/
#ifndef PACKAGE_VERSION
# define PACKAGE_VERSION SQLITE_VERSION
#endif

/*
** Initialize this module.
**
** This Tcl module contains only a single new Tcl command named "sqlite".
** (Hence there is no namespace.  There is no point in using a namespace
** if the extension only supplies one new name!)  The "sqlite" command is
** used to open a new SQLite database.  See the DbMain() routine above
** for additional information.
**
** The EXTERN macros are required by TCL in order to work on windows.
*/
EXTERN int Sqlite3_Init(Tcl_Interp *interp){
  int rc = Tcl_InitStubs(interp, "8.5-", 0) ? TCL_OK : TCL_ERROR;
  if( rc==TCL_OK ){
    Tcl_CreateObjCommand(interp, "sqlite3", (Tcl_ObjCmdProc*)DbMain, 0, 0);
#ifndef SQLITE_3_SUFFIX_ONLY
    /* The "sqlite" alias is undocumented.  It is here only to support
    ** legacy scripts.  All new scripts should use only the "sqlite3"
    ** command. */
    Tcl_CreateObjCommand(interp, "sqlite", (Tcl_ObjCmdProc*)DbMain, 0, 0);
#endif
    rc = Tcl_PkgProvide(interp, "sqlite3", PACKAGE_VERSION);
  }
  return rc;
}
EXTERN int Tclsqlite3_Init(Tcl_Interp *interp){ return Sqlite3_Init(interp); }
EXTERN int Sqlite3_Unload(Tcl_Interp *interp, int flags){ return TCL_OK; }
EXTERN int Tclsqlite3_Unload(Tcl_Interp *interp, int flags){ return TCL_OK; }

/* Because it accesses the file-system and uses persistent state, SQLite
** is not considered appropriate for safe interpreters.  Hence, we cause
** the _SafeInit() interfaces return TCL_ERROR.
*/
EXTERN int Sqlite3_SafeInit(Tcl_Interp *interp){ return TCL_ERROR; }
EXTERN int Sqlite3_SafeUnload(Tcl_Interp *interp, int flags){return TCL_ERROR;}

/*
** Versions of all of the above entry points that omit the "3" at the end
** of the name.  Years ago (circa 2004) the "3" was necessary to distinguish
** SQLite version 3 from Sqlite version 2.  But two decades have elapsed.
** SQLite2 is not longer a conflict.  So it is ok to omit the "3".
**
** Omitting the "3" helps TCL find the entry point.
*/
EXTERN int Sqlite_Init(Tcl_Interp *interp){ return Sqlite3_Init(interp);}
EXTERN int Tclsqlite_Init(Tcl_Interp *interp){ return Sqlite3_Init(interp); }
EXTERN int Sqlite_Unload(Tcl_Interp *interp, int flags){ return TCL_OK; }
EXTERN int Tclsqlite_Unload(Tcl_Interp *interp, int flags){ return TCL_OK; }
EXTERN int Sqlite_SafeInit(Tcl_Interp *interp){ return TCL_ERROR; }
EXTERN int Sqlite_SafeUnload(Tcl_Interp *interp, int flags){return TCL_ERROR;}

/* Also variants with a lowercase "s".  I'm told that these are
** deprecated in Tcl9, but they continue to be included for backwards
** compatibility. */
EXTERN int sqlite3_Init(Tcl_Interp *interp){ return Sqlite3_Init(interp);}
EXTERN int sqlite_Init(Tcl_Interp *interp){ return Sqlite3_Init(interp);}


/*
** If the TCLSH macro is defined, add code to make a stand-alone program.
*/
#if defined(TCLSH)

/* This is the main routine for an ordinary TCL shell.  If there are
** arguments, run the first argument as a script.  Otherwise, read TCL
** commands from standard input
*/
static const char *tclsh_main_loop(void){
  static const char zMainloop[] =
    "if {[llength $argv]>=1} {\n"
#ifdef WIN32
      "set new [list]\n"
      "foreach arg $argv {\n"
        "if {[string match -* $arg] || [file exists $arg]} {\n"
          "lappend new $arg\n"
        "} else {\n"
          "set once 0\n"
          "foreach match [lsort [glob -nocomplain $arg]] {\n"
            "lappend new $match\n"
            "set once 1\n"
          "}\n"
          "if {!$once} {lappend new $arg}\n"
        "}\n"
      "}\n"
      "set argv $new\n"
      "unset new\n"
#endif
      "set argv0 [lindex $argv 0]\n"
      "set argv [lrange $argv 1 end]\n"
      "source $argv0\n"
    "} else {\n"
      "set line {}\n"
      "while {![eof stdin]} {\n"
        "if {$line!=\"\"} {\n"
          "puts -nonewline \"> \"\n"
        "} else {\n"
          "puts -nonewline \"% \"\n"
        "}\n"
        "flush stdout\n"
        "append line [gets stdin]\n"
        "if {[info complete $line]} {\n"
          "if {[catch {uplevel #0 $line} result]} {\n"
            "puts stderr \"Error: $result\"\n"
          "} elseif {$result!=\"\"} {\n"
            "puts $result\n"
          "}\n"
          "set line {}\n"
        "} else {\n"
          "append line \\n\n"
        "}\n"
      "}\n"
    "}\n"
  ;
  return zMainloop;
}

#ifndef TCLSH_MAIN
# define TCLSH_MAIN main
#endif
int SQLITE_CDECL TCLSH_MAIN(int argc, char **argv){
  Tcl_Interp *interp;
  int i;
  const char *zScript = 0;
  char zArgc[32];
#if defined(TCLSH_INIT_PROC)
  extern const char *TCLSH_INIT_PROC(Tcl_Interp*);
#endif

#if !defined(_WIN32_WCE)
  if( getenv("SQLITE_DEBUG_BREAK") ){
    if( isatty(0) && isatty(2) ){
      fprintf(stderr,
          "attach debugger to process %d and press any key to continue.\n",
          GETPID());
      fgetc(stdin);
    }else{
#if defined(_WIN32) || defined(WIN32)
      DebugBreak();
#elif defined(SIGTRAP)
      raise(SIGTRAP);
#endif
    }
  }
#endif

  /* Call sqlite3_shutdown() once before doing anything else. This is to
  ** test that sqlite3_shutdown() can be safely called by a process before
  ** sqlite3_initialize() is. */
  sqlite3_shutdown();

  Tcl_FindExecutable(argv[0]);
  Tcl_SetSystemEncoding(NULL, "utf-8");
  interp = Tcl_CreateInterp();
  Sqlite3_Init(interp);

  sqlite3_snprintf(sizeof(zArgc), zArgc, "%d", argc-1);
  Tcl_SetVar(interp,"argc", zArgc, TCL_GLOBAL_ONLY);
  Tcl_SetVar(interp,"argv0",argv[0],TCL_GLOBAL_ONLY);
  Tcl_SetVar(interp,"argv", "", TCL_GLOBAL_ONLY);
  for(i=1; i<argc; i++){
    Tcl_SetVar(interp, "argv", argv[i],
        TCL_GLOBAL_ONLY | TCL_LIST_ELEMENT | TCL_APPEND_VALUE);
  }
#if defined(TCLSH_INIT_PROC)
  zScript = TCLSH_INIT_PROC(interp);
#endif
  if( zScript==0 ){
    zScript = tclsh_main_loop();
  }
  if( Tcl_GlobalEval(interp, zScript)!=TCL_OK ){
    const char *zInfo = Tcl_GetVar(interp, "errorInfo", TCL_GLOBAL_ONLY);
    if( zInfo==0 ) zInfo = Tcl_GetStringResult(interp);
    fprintf(stderr,"%s: %s\n", *argv, zInfo);
    return 1;
  }
  return 0;
}
#endif /* TCLSH */

/************** End of tclsqlite.c ******************************************/
/*********** End of the amalgamation of qrf.h, qrf.c, tclsqlite.c ***********/
