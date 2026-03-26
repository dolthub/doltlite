/*
** doltlite-remotesrv: standalone HTTP server for doltlite remotes.
**
** Usage:
**   doltlite-remotesrv [-p PORT] DIRECTORY
**
** Serves all .doltlite/.db files in DIRECTORY over HTTP.
** Each database is accessible at http://host:port/FILENAME
**
** Options:
**   -p PORT   Listen port (default: 8080)
**   -h        Show help
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "doltlite_remotesrv.h"

static void usage(const char *prog){
  fprintf(stderr,
    "Usage: %s [-p PORT] DIRECTORY\n"
    "\n"
    "Serve doltlite databases over HTTP.\n"
    "\n"
    "Options:\n"
    "  -p PORT   Listen port (default: 8080)\n"
    "  -h        Show this help\n"
    "\n"
    "Example:\n"
    "  %s -p 9000 /var/lib/doltlite\n"
    "  # Databases at http://localhost:9000/mydb.db\n",
    prog, prog
  );
}

int main(int argc, char **argv){
  int port = 8080;
  const char *zDir = 0;
  int i;

  for(i=1; i<argc; i++){
    if( strcmp(argv[i], "-p")==0 && i+1<argc ){
      port = atoi(argv[++i]);
    }else if( strcmp(argv[i], "-h")==0 || strcmp(argv[i], "--help")==0 ){
      usage(argv[0]);
      return 0;
    }else if( argv[i][0]=='-' ){
      fprintf(stderr, "Unknown option: %s\n", argv[i]);
      usage(argv[0]);
      return 1;
    }else{
      zDir = argv[i];
    }
  }

  if( !zDir ){
    fprintf(stderr, "Error: directory argument required\n\n");
    usage(argv[0]);
    return 1;
  }

  printf("doltlite-remotesrv serving %s on port %d\n", zDir, port);
  printf("Press Ctrl+C to stop.\n\n");

  int rc = doltliteServe(zDir, port);
  return rc==0 ? 0 : 1;
}
