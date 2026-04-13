#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>

static char *read_file(const char *path) {
  FILE *fp = fopen(path, "rb");
  long n;
  char *buf;
  if (!fp) return NULL;
  fseek(fp, 0, SEEK_END);
  n = ftell(fp);
  fseek(fp, 0, SEEK_SET);
  buf = (char *)malloc((size_t)n + 1);
  if (!buf) {
    fclose(fp);
    return NULL;
  }
  if (fread(buf, 1, (size_t)n, fp) != (size_t)n) {
    free(buf);
    fclose(fp);
    return NULL;
  }
  fclose(fp);
  buf[n] = '\0';
  return buf;
}

int main(int argc, char **argv) {
  sqlite3 *db = NULL;
  char *sql;
  char *errmsg = NULL;
  int rc;

  const char *db_path = argc > 1 ? argv[1] : "/tmp/fts5-min-repro.db";
  const char *sql_path = argc > 2 ? argv[2] : "doltlite-fts5-corruption-noshell.sql";

  sql = read_file(sql_path);
  if (!sql) {
    fprintf(stderr, "read failed: %s\n", sql_path);
    return 2;
  }

  rc = sqlite3_open(db_path, &db);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "open failed: %s\n", sqlite3_errmsg(db));
    free(sql);
    sqlite3_close(db);
    return 2;
  }

  rc = sqlite3_exec(db, sql, NULL, NULL, &errmsg);
  if (rc != SQLITE_OK) {
    fprintf(stderr, "%s\n", errmsg ? errmsg : sqlite3_errmsg(db));
    sqlite3_free(errmsg);
    free(sql);
    sqlite3_close(db);
    return 1;
  }

  puts("PASS");
  free(sql);
  sqlite3_close(db);
  return 0;
}
