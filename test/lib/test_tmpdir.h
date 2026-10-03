#ifndef DOLTLITE_TEST_TMPDIR_H
#define DOLTLITE_TEST_TMPDIR_H

#ifdef _WIN32
# include <direct.h>
#else
# include <sys/stat.h>
#endif

/* Scratch root for test databases. The build sets it per build directory
** so concurrent checkouts on one host never share a fixed /tmp path. */
#ifndef DOLTLITE_TEST_TMPDIR
# define DOLTLITE_TEST_TMPDIR "/tmp"
#endif

/* Before main, so forked children inherit an existing directory. */
__attribute__((constructor)) static void doltliteTestTmpdirCreate(void){
#ifdef _WIN32
  (void)_mkdir(DOLTLITE_TEST_TMPDIR);
#else
  (void)mkdir(DOLTLITE_TEST_TMPDIR, 0700);
#endif
}

#endif
