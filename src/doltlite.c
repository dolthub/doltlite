#ifdef DOLTLITE_PROLLY

#include "sqliteInt.h"
#include "doltlite_internal.h"
#include "doltlite_constraint_violations.h"
#include "doltlite_ignore.h"

int doltliteRegister(sqlite3 *db){
  int rc;
  if( (rc = doltliteCommitCmdRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteAddRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteCleanRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteResetRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteMergeCmdRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteCherryPickRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRevertRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRebaseRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteConfigRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteLogRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteCommitAncestorsRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteStatusRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteMergeStatusRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteDiffRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteBranchRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteTagRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteConflictsRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteGcRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRegisterHistoricalTables(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRegisterWorkspaceTables(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteAncestorRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRegisterBlameTables(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteSchemaDiffRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltlitePatchRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteSchemasRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteDiffStatRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteRemoteSqlRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteHashofRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteConstraintViolationsRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteVerifyConstraintsRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteDocsRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteTestsRegister(db))!=SQLITE_OK ) return rc;
  if( (rc = doltliteIgnoreRegister(db))!=SQLITE_OK ) return rc;
  rc = doltliteMaybeSeedRepo(db);
  if( rc==SQLITE_OK ) doltliteBtreeRegistrationDone(db);
  return rc;
}

#endif
