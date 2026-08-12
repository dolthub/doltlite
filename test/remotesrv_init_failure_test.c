#include <stdio.h>
#include <string.h>
#include "sqlite3.h"
#include "doltlite_remotesrv.h"

extern int doltliteRemoteSrvInitForTest(
  const DoltliteServeOpts *o,
  int *pMutexInit,
  int *pCondInit
);

typedef struct FaultAllocator FaultAllocator;
struct FaultAllocator {
  sqlite3_mem_methods real;
  sqlite3_int64 nOutstanding;
  int nCall;
  int failAt;
  int triggered;
};

static FaultAllocator gFault;
static int nPass;
static int nFail;

static void check(const char *zName, int condition){
  if( condition ){
    nPass++;
  }else{
    nFail++;
    fprintf(stderr, "FAIL: %s\n", zName);
  }
}

static int shouldFail(void){
  gFault.nCall++;
  if( gFault.nCall==gFault.failAt ){
    gFault.triggered = 1;
    return 1;
  }
  return 0;
}

static void *faultMalloc(int n){
  void *p;
  if( shouldFail() ) return 0;
  p = gFault.real.xMalloc(n);
  if( p ) gFault.nOutstanding++;
  return p;
}

static void faultFree(void *p){
  if( !p ) return;
  gFault.nOutstanding--;
  gFault.real.xFree(p);
}

static void *faultRealloc(void *p, int n){
  void *pNew;
  if( !p ) return faultMalloc(n);
  if( shouldFail() ) return 0;
  pNew = gFault.real.xRealloc(p, n);
  return pNew;
}

static int faultSize(void *p){
  return gFault.real.xSize(p);
}

static int faultRoundup(int n){
  return gFault.real.xRoundup(n);
}

static int faultInit(void *pArg){
  (void)pArg;
  return gFault.real.xInit
       ? gFault.real.xInit(gFault.real.pAppData) : SQLITE_OK;
}

static void faultShutdown(void *pArg){
  (void)pArg;
  if( gFault.real.xShutdown ){
    gFault.real.xShutdown(gFault.real.pAppData);
  }
}

static int installFaultAllocator(void){
  sqlite3_mem_methods wrapped;
  int rc;

  sqlite3_shutdown();
  memset(&gFault, 0, sizeof(gFault));
  gFault.failAt = -1;
  rc = sqlite3_config(SQLITE_CONFIG_GETMALLOC, &gFault.real);
  if( rc!=SQLITE_OK ) return rc;
  wrapped = gFault.real;
  wrapped.xMalloc = faultMalloc;
  wrapped.xFree = faultFree;
  wrapped.xRealloc = faultRealloc;
  wrapped.xSize = faultSize;
  wrapped.xRoundup = faultRoundup;
  wrapped.xInit = faultInit;
  wrapped.xShutdown = faultShutdown;
  wrapped.pAppData = 0;
  rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &wrapped);
  if( rc!=SQLITE_OK ) return rc;
  return sqlite3_initialize();
}

static void setFailure(int failAt){
  gFault.nCall = 0;
  gFault.failAt = failAt;
  gFault.triggered = 0;
}

int main(void){
  DoltliteServeOpts opts;
  sqlite3_int64 nBaseline;
  int mutexInit;
  int condInit;
  int cleanFailures = 1;
  int rc;
  int i;

  printf("=== RemoteSrv Initialization Failure Tests ===\n\n");
  rc = installFaultAllocator();
  check("install_fault_allocator", rc==SQLITE_OK);
  if( rc!=SQLITE_OK ) return 1;

  memset(&opts, 0, sizeof(opts));
  opts.zDir = ".";
  opts.zBindAddr = "not-a-numeric-address";
  nBaseline = gFault.nOutstanding;

  setFailure(-1);
  for(i=0; i<1000; i++){
    rc = doltliteServeOpts(&opts);
    if( rc!=SQLITE_ERROR ) cleanFailures = 0;
    if( doltliteServeAsyncOpts(&opts)!=0 ) cleanFailures = 0;
  }
  check("repeated_invalid_address_fails", cleanFailures);
  check("repeated_invalid_address_frees_allocations",
        gFault.nOutstanding==nBaseline);

  mutexInit = -1;
  condInit = -1;
  rc = doltliteRemoteSrvInitForTest(&opts, &mutexInit, &condInit);
  check("invalid_address_init_fails", rc==SQLITE_ERROR);
  check("invalid_address_destroys_mutex", mutexInit==0);
  check("invalid_address_destroys_condition", condInit==0);

  opts.zBindAddr = "127.0.0.1";
  opts.authKeysDir = ".";
  opts.audience = 0;
  setFailure(-1);
  mutexInit = -1;
  condInit = -1;
  rc = doltliteRemoteSrvInitForTest(&opts, &mutexInit, &condInit);
  check("auth_keys_without_audience_fails", rc==SQLITE_ERROR);
  check("auth_keys_without_audience_destroys_mutex", mutexInit==0);
  check("auth_keys_without_audience_destroys_condition", condInit==0);
  check("auth_keys_without_audience_frees_allocations",
        gFault.nOutstanding==nBaseline);
  opts.authKeysDir = 0;

  setFailure(1);
  mutexInit = -1;
  condInit = -1;
  rc = doltliteRemoteSrvInitForTest(&opts, &mutexInit, &condInit);
  check("directory_copy_oom_injected", gFault.triggered);
  check("directory_copy_oom_reported", rc==SQLITE_NOMEM);
  check("directory_copy_oom_destroys_mutex", mutexInit==0);
  check("directory_copy_oom_destroys_condition", condInit==0);
  check("directory_copy_oom_frees_allocations",
        gFault.nOutstanding==nBaseline);

  setFailure(2);
  check("async_directory_copy_oom_returns_null",
        doltliteServeAsyncOpts(&opts)==0);
  check("async_directory_copy_oom_injected", gFault.triggered);
  check("async_directory_copy_oom_frees_allocations",
        gFault.nOutstanding==nBaseline);

  setFailure(-1);
  sqlite3_shutdown();
  sqlite3_config(SQLITE_CONFIG_MALLOC, &gFault.real);

  printf("\nResults: %d passed, %d failed out of %d tests\n",
         nPass, nFail, nPass+nFail);
  return nFail ? 1 : 0;
}
