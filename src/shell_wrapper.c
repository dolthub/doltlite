/* The shell recurses deeply during SQL parse/codegen, and doltlite's
** prolly-tree cursor stack adds another layer on top. The default
** 8MB main-thread stack overflows on pathological queries, so we
** hand the shell off to a worker thread with a 256MB stack and just
** wait for it to return. */

#include <pthread.h>
#include <stdlib.h>

extern int sqlite3_shell_main(int argc, char **argv);

struct ShellArgs {
  int argc;
  char **argv;
  int rc;
};

static void *shell_thread(void *arg){
  struct ShellArgs *sa = (struct ShellArgs *)arg;
  sa->rc = sqlite3_shell_main(sa->argc, sa->argv);
  return NULL;
}

int main(int argc, char **argv){
  pthread_t th;
  pthread_attr_t attr;
  struct ShellArgs sa;

  sa.argc = argc;
  sa.argv = argv;
  sa.rc = 0;

  pthread_attr_init(&attr);
  pthread_attr_setstacksize(&attr, 256 * 1024 * 1024);
  pthread_create(&th, &attr, shell_thread, &sa);
  pthread_join(th, NULL);
  pthread_attr_destroy(&attr);

  return sa.rc;
}
