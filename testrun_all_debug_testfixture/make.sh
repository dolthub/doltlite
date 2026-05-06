set -e
if [ "$#" -ne 1 ] ; then
  echo "Usage: $0 <target>"
  exit -1
fi

SRCDIR="/Users/timsehn/dolthub/codex/doltlite"
TCLDIR="/opt/homebrew/Cellar/tcl-tk/9.0.3/lib"

if [ ! -f Makefile ] ; then
  $SRCDIR/configure --with-tcl=$TCLDIR --with-debug --enable-all 
fi

OPTS="$OPTS -DHAVE_USLEEP=1"
OPTS="$OPTS -DSQLITE_ENABLE_NORMALIZE"
OPTS="$OPTS -DSQLITE_ENABLE_ORDERED_SET_AGGREGATES"
OPTS="$OPTS -DSQLITE_OS_UNIX=1"

CFLAGS="-g"

make $1 "CFLAGS=$CFLAGS" "OPTS=$OPTS"
