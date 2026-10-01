#include <dirent.h>

typedef struct TestTempDir {
  char zDir[128];
  char zCwd[4096];
} TestTempDir;

static int testTempDirSetup(TestTempDir *p){
  if( !getcwd(p->zCwd, sizeof(p->zCwd)) ) return 0;
  snprintf(p->zDir, sizeof(p->zDir), "/tmp/doltlite-c-test-XXXXXX");
  if( !mkdtemp(p->zDir) ) return 0;
  if( chdir(p->zDir)!=0 ){
    rmdir(p->zDir);
    return 0;
  }
  return 1;
}

static int testTempDirCleanup(TestTempDir *p){
  DIR *dir = opendir(".");
  struct dirent *entry;
  int ok = dir!=0;
  if( dir ){
    while( (entry = readdir(dir))!=0 ){
      if( strcmp(entry->d_name, ".")==0 || strcmp(entry->d_name, "..")==0 ){
        continue;
      }
      if( unlink(entry->d_name)!=0 ) ok = 0;
    }
    if( closedir(dir)!=0 ) ok = 0;
  }
  if( chdir(p->zCwd)!=0 ) return 0;
  if( rmdir(p->zDir)!=0 ) ok = 0;
  return ok;
}
