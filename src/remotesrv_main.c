
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif
#include "doltlite_remotesrv.h"

static void usage(const char *prog){
  fprintf(stderr,
    "Usage: %s [-p PORT] [--bind ADDR] [--cert FILE --key FILE]\n"
    "          [--auth-keys DIR] [--audience AUD] DIRECTORY\n"
    "\n"
    "Serve doltlite databases over HTTP/HTTPS.\n"
    "\n"
    "Options:\n"
    "  -p PORT         Listen port (default: 8080; 0 = pick a free port)\n"
    "  --bind ADDR     IPv4 bind address (default: 127.0.0.1)\n"
    "  --cert FILE     PEM certificate chain (enables TLS; requires --key)\n"
    "  --key FILE      PEM private key (requires --cert)\n"
    "  --auth-keys DIR Require a bearer credential; authorized public keys are\n"
    "                  <kid>.jwk files in DIR\n"
    "  --audience AUD  Expected JWT audience (default: none)\n"
    "  --timeout-ms MS Connection I/O timeout (default: 30000)\n"
    "  -h              Show this help\n"
    "\n"
    "Example:\n"
    "  %s --cert server.crt --key server.key --auth-keys ./keys \\\n"
    "     --audience 127.0.0.1 -p 9000 /var/lib/doltlite\n",
    prog, prog
  );
}

static volatile sig_atomic_t g_stop = 0;
static void onSignal(int sig){ (void)sig; g_stop = 1; }

int main(int argc, char **argv){
  DoltliteServeOpts o;
  DoltliteServer *srv;
  const char *zBindShown;
  int i;

  memset(&o, 0, sizeof(o));
  o.port = 8080;

  for(i=1; i<argc; i++){
    if( strcmp(argv[i], "-p")==0 && i+1<argc ){
      o.port = atoi(argv[++i]);
    }else if( strcmp(argv[i], "--bind")==0 && i+1<argc ){
      o.zBindAddr = argv[++i];
    }else if( strcmp(argv[i], "--cert")==0 && i+1<argc ){
      o.certFile = argv[++i];
    }else if( strcmp(argv[i], "--key")==0 && i+1<argc ){
      o.keyFile = argv[++i];
    }else if( strcmp(argv[i], "--auth-keys")==0 && i+1<argc ){
      o.authKeysDir = argv[++i];
    }else if( strcmp(argv[i], "--audience")==0 && i+1<argc ){
      o.audience = argv[++i];
    }else if( strcmp(argv[i], "--timeout-ms")==0 && i+1<argc ){
      o.timeoutMs = atoi(argv[++i]);
    }else if( strcmp(argv[i], "-h")==0 || strcmp(argv[i], "--help")==0 ){
      usage(argv[0]);
      return 0;
    }else if( argv[i][0]=='-' ){
      fprintf(stderr, "Unknown option: %s\n", argv[i]);
      usage(argv[0]);
      return 1;
    }else{
      o.zDir = argv[i];
    }
  }

  if( !o.zDir ){
    fprintf(stderr, "Error: directory argument required\n\n");
    usage(argv[0]);
    return 1;
  }
  if( (o.certFile!=0) != (o.keyFile!=0) ){
    fprintf(stderr, "Error: --cert and --key must be given together\n");
    return 1;
  }
  if( o.timeoutMs<0 ){
    fprintf(stderr, "Error: --timeout-ms must not be negative\n");
    return 1;
  }

  signal(SIGINT, onSignal);
  signal(SIGTERM, onSignal);

  srv = doltliteServeAsyncOpts(&o);
  if( !srv ){
    fprintf(stderr, "Error: failed to start server "
                    "(check DIRECTORY, --cert/--key, and the bind address)\n");
    return 1;
  }

  zBindShown = o.zBindAddr ? o.zBindAddr : "127.0.0.1";
  printf("doltlite-remotesrv serving %s on %s://%s:%d\n",
         o.zDir, o.certFile ? "https" : "http",
         zBindShown, doltliteServerPort(srv));
  if( o.authKeysDir ){
    printf("credential auth enabled (authorized keys: %s)\n", o.authKeysDir);
  }
  printf("Press Ctrl+C to stop.\n");
  fflush(stdout);

  while( !g_stop ){
#ifdef _WIN32
    Sleep(1000);
#else
    sleep(1);
#endif
  }

  doltliteServerStop(srv);
  return 0;
}
