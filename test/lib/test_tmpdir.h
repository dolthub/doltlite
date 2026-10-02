#ifndef DOLTLITE_TEST_TMPDIR_H
#define DOLTLITE_TEST_TMPDIR_H

#include <sys/stat.h>

/* Scratch root for test databases. The build sets it per build directory
** so concurrent checkouts on one host never share a fixed /tmp path. */
#ifndef DOLTLITE_TEST_TMPDIR
# define DOLTLITE_TEST_TMPDIR "/tmp"
#endif

/* Before main, so forked children inherit an existing directory. */
__attribute__((constructor)) static void doltliteTestTmpdirCreate(void){
  (void)mkdir(DOLTLITE_TEST_TMPDIR, 0700);
}

#endif
