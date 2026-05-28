
#ifdef DOLTLITE_PROLLY

#include "chunk_file.h"
#include <string.h>

const char *chunkFileGetFilename(const ChunkFile *cf){
  return cf->zFilename;
}

sqlite3_vfs *chunkFileGetVfs(const ChunkFile *cf){
  return cf->pVfs;
}

sqlite3_file *chunkFileGetHandle(const ChunkFile *cf){
  return cf->pFile;
}

void chunkFileSetHandle(ChunkFile *cf, sqlite3_file *pFile){
  cf->pFile = pFile;
}

void chunkFileSetSize(ChunkFile *cf, i64 nSize){
  cf->iFileSize = nSize;
}

void csCloseFile(sqlite3_file *pFile){
  if( pFile ){
    sqlite3OsCloseFree(pFile);
  }
}

#endif
