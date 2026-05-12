/*
** doltlite-remotesrv: standalone HTTP server for doltlite remotes.
**
** Usage:
**   doltlite-remotesrv [-p PORT] [--bind ADDR] DIRECTORY
**
** Serves all .doltlite/.db files in DIRECTORY over HTTP.
** Each database is accessible at http://host:port/FILENAME
**
** Options:
**   -p PORT       Listen port (default: 8080)
**   --bind ADDR   IPv4 bind address (default: 127.0.0.1)
**   -h            Show help
*/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "doltlite_remotesrv.h"

static void usage(const char *prog){
  fprintf(stderr,
    "Usage: %s [-p PORT] [--bind ADDR] DIRECTORY\n"
    "\n"
    "Serve doltlite databases over HTTP.\n"
    "\n"
    "Options:\n"
    "  -p PORT       Listen port (default: 8080)\n"
    "  --bind ADDR   IPv4 bind address (default: 127.0.0.1; pass 0.0.0.0\n"
    "                to listen on all interfaces — note that the remote\n"
    "                protocol has no auth/TLS yet, see issue #228)\n"
    "  -h            Show this help\n"
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
  const char *zBind = 0;  /* NULL → library default (127.0.0.1) */
  int i;
  int rc;

  for(i=1; i<argc; i++){
    if( strcmp(argv[i], "-p")==0 && i+1<argc ){
      port = atoi(argv[++i]);
    }else if( strcmp(argv[i], "--bind")==0 && i+1<argc ){
      zBind = argv[++i];
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

  printf("doltlite-remotesrv serving %s on %s:%d\n",
         zDir, zBind ? zBind : "127.0.0.1", port);
  printf("Press Ctrl+C to stop.\n\n");

  rc = doltliteServe(zDir, port, zBind);
  return rc==0 ? 0 : 1;
}
