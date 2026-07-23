#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_internal.h"

extern int doltliteLogRegister(sqlite3 *db);
extern int doltliteCommitAncestorsRegister(sqlite3 *db);
extern int doltliteStatusRegister(sqlite3 *db);
extern int doltliteDiffRegister(sqlite3 *db);
extern int doltliteSchemasRegister(sqlite3 *db);
extern int doltliteDiffStatRegister(sqlite3 *db);
extern int doltliteBranchRegister(sqlite3 *db);
extern int doltliteConflictsRegister(sqlite3 *db);
extern int doltliteTagRegister(sqlite3 *db);
extern int doltliteGcRegister(sqlite3 *db);
extern int doltliteRegisterDiffTables(sqlite3 *db);
extern int doltliteRegisterWorkspaceTables(sqlite3 *db);
extern int doltliteAncestorRegister(sqlite3 *db);
extern int doltliteRegisterAtTables(sqlite3 *db);
extern int doltliteRegisterHistoryTables(sqlite3 *db);
extern int doltliteRegisterBlameTables(sqlite3 *db);
extern int doltliteSchemaDiffRegister(sqlite3 *db);
extern int doltlitePatchRegister(sqlite3 *db);
extern int doltliteRemoteSqlRegister(sqlite3 *db);
extern int doltliteHashofRegister(sqlite3 *db);
extern int doltliteConstraintViolationsRegister(sqlite3 *db);
extern int doltliteVerifyConstraintsRegister(sqlite3 *db);

int doltliteRegister(sqlite3 *db){
  int rc;
  if( (rc = doltliteCommitCmdRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteAddRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteResetRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteMergeCmdRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteCherryPickRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRevertRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRebaseRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteConfigRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteLogRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteCommitAncestorsRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteStatusRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteDiffRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteBranchRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteTagRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteConflictsRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteGcRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRegisterDiffTables(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRegisterWorkspaceTables(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteAncestorRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRegisterAtTables(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRegisterHistoryTables(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRegisterBlameTables(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteSchemaDiffRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltlitePatchRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteSchemasRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteDiffStatRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRemoteSqlRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteHashofRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteConstraintViolationsRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteVerifyConstraintsRegister(db))!=SQLITE_OK ) return rc;
  return doltliteMaybeSeedRepo(db);
}

#endif
