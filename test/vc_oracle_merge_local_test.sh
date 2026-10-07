#!/bin/bash
set -u
set -o pipefail

DOLTLITE="${1:-build/doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap 'rm -rf "$TMPROOT"' EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"
DOLTLITE=$(vc_oracle_resolve_binary "$DOLTLITE") || exit 1
DOLT=$(vc_oracle_resolve_binary "$DOLT") || exit 1

for kind in ff noff threeway nocommit squash abort; do
  for change in insert update delete add_column new_table add_index; do
    for staged in 0 1; do
      name="${kind}_${change}_${staged}"
      dir="$TMPROOT/$name"
      mkdir -p "$dir/dt"
      setup="CREATE TABLE t(id INT PRIMARY KEY,v INT);
CREATE TABLE z(id INT PRIMARY KEY,v INT);
INSERT INTO t VALUES(1,10); INSERT INTO z VALUES(1,10);
SELECT dolt_commit('-Am','base'); SELECT dolt_branch('feat');
SELECT dolt_checkout('feat'); INSERT INTO t VALUES(2,20);
SELECT dolt_commit('-am','feat'); SELECT dolt_checkout('main');"
      options=""
      case "$kind" in
        noff) options="'--no-ff'," ;;
        threeway|nocommit|squash|abort)
          setup="$setup INSERT INTO t VALUES(3,30); SELECT dolt_commit('-am','main');"
          case "$kind" in
            nocommit|abort) options="'--no-commit'," ;;
            squash) options="'--squash'," ;;
          esac ;;
      esac
      owner=z
      case "$change" in
        insert) setup="$setup INSERT INTO z VALUES(9,90);" ;;
        update) setup="$setup UPDATE z SET v=90 WHERE id=1;" ;;
        delete) setup="$setup DELETE FROM z;" ;;
        add_column) setup="$setup ALTER TABLE z ADD COLUMN x INT DEFAULT 7;" ;;
        new_table)
          setup="$setup CREATE TABLE u(id INT PRIMARY KEY,v INT); INSERT INTO u VALUES(9,90);"
          owner=u ;;
        add_index) setup="$setup CREATE INDEX iz ON z(v);" ;;
      esac
      if [ "$staged" -eq 1 ]; then setup="$setup SELECT dolt_add('$owner');"; fi
      setup="$setup SELECT dolt_merge(${options}'feat');"
      if [ "$kind" = abort ]; then setup="$setup SELECT dolt_merge('--abort');"; fi
      query="SELECT concat('rows:',count(*),':',coalesce(sum(v),0)) AS result FROM $owner
UNION ALL SELECT concat('unstaged:',count(*)) FROM dolt_status WHERE staged=0
UNION ALL SELECT concat('staged:',count(*)) FROM dolt_status WHERE staged=1
UNION ALL SELECT concat('t:',count(*)) FROM t"
      dl_query="$query UNION ALL SELECT concat('committed:',count(*),':',sum(v)) FROM dolt_at_z('HEAD');"
      dt_query="$query UNION ALL SELECT concat('committed:',count(*),':',sum(v)) FROM z AS OF 'HEAD';"
      dl_rc=0; dt_rc=0
      vc_oracle_run_doltlite_script "$dir/db" "$dir/setup.out" "$dir/dl.err" "$setup" || dl_rc=$?
      printf '.headers off\n.mode list\n%s\n' "$dl_query" \
        | vc_oracle_run_doltlite "$dir/db" >"$dir/dl.out" 2>>"$dir/dl.err" || dl_rc=$?
      dt_setup=$(vc_oracle_translate_for_dolt "$setup")
      vc_oracle_run_dolt_setup_query "$dir/dt" "$dir/dt.out" "$dir/dt.err" \
        "$dt_setup" "$dt_query" || dt_rc=$?
      if [ "$dl_rc" -ne 0 ] || [ "$dt_rc" -ne 0 ]; then
        fail=$((fail+1)); FAILED_NAMES="$FAILED_NAMES $name"
        echo "  FAIL: $name (doltlite=$dl_rc, dolt=$dt_rc)"
        cat "$dir/dl.err" "$dir/dt.err"
      else
        vc_oracle_assert_match "$name" "$(cat "$dir/dl.out")" \
          "$(vc_oracle_tail_csv_body "$dir/dt.out")"
      fi
    done
  done
done

vc_oracle_finish
