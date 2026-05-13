
#ifndef DOLTLITE_CHUNK_FILE_H
#define DOLTLITE_CHUNK_FILE_H

#include "sqliteInt.h"

typedef struct ChunkFile ChunkFile;

struct ChunkFile {
  char *zFilename;
  sqlite3_file *pFile;
  sqlite3_vfs *pVfs;
  i64 iFileSize;
};

void chunkFileInit(ChunkFile *cf);

const char *chunkFileGetFilename(const ChunkFile *cf);
sqlite3_vfs *chunkFileGetVfs(const ChunkFile *cf);
sqlite3_file *chunkFileGetHandle(const ChunkFile *cf);
i64 chunkFileGetSize(const ChunkFile *cf);

void chunkFileSetHandle(ChunkFile *cf, sqlite3_file *pFile);
void chunkFileSetSize(ChunkFile *cf, i64 nSize);

void csCloseFile(sqlite3_file *pFile);

#endif
