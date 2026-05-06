set -e
if [ "$#" -ne 1 ] ; then
  echo "Usage: $0 <target>"
  exit -1
fi

SRCDIR="/Users/timsehn/dolthub/codex/doltlite"
TCLDIR="/opt/homebrew/Cellar/tcl-tk/9.0.3/lib"

if [ ! -f Makefile ] ; then
  $SRCDIR/configure --with-tcl=$TCLDIR --enable-all 
fi

OPTS="$OPTS -DHAVE_USLEEP=1"
OPTS="$OPTS -DSQLITE_OS_UNIX=1"

CFLAGS="-g -O0"

make $1 "CFLAGS=$CFLAGS" "OPTS=$OPTS"
