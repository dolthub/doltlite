#!/bin/bash

set -u
set -o pipefail

DOLTLITE="${1:-./doltlite}"
DOLT="${2:-dolt}"
TMPROOT=$(mktemp -d)
trap "rm -rf $TMPROOT" EXIT
pass=0; fail=0
FAILED_NAMES=""
source "$(dirname "$0")/lib/vc_oracle_common.sh"

normalize_model_output() {
  tr -d '\r"' \
    | sed -E 's/:true/:1/g; s/:false/:0/g' \
    | awk '/^Q\|/ { print }'
}

generate_case() {
  local seed="$1" dl_sql="$2" dt_sql="$3"
  python3 - "$seed" "$dl_sql" "$dt_sql" <<'PY'
import random
import sys

seed = int(sys.argv[1])
dl_path = sys.argv[2]
dt_path = sys.argv[3]
rng = random.Random(seed)

dl = [
    ".headers off",
    ".mode list",
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, k INTEGER);",
    "CREATE INDEX idx_t_k ON t(k);",
]
dt = [
    "CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT, k INTEGER);",
    "CREATE INDEX idx_t_k ON t(k);",
]

next_id = 1
branches = {"main": {}}
current = "main"

def esc(s):
    return s.replace("'", "''")

def emit(sql, dolt_sql=None):
    dl.append(sql)
    dt.append(dolt_sql if dolt_sql is not None else sql)

def vc_call(name, *args):
    quoted = ", ".join("'" + esc(a) + "'" for a in args)
    emit(f"SELECT {name}({quoted});", f"CALL {name}({quoted});")

def insert_row(row_id, label):
    v = f"{label}_{seed}_{row_id}"
    k = (row_id * 17 + seed) % 97
    emit(f"INSERT INTO t VALUES({row_id}, '{esc(v)}', {k});")
    branches[current][row_id] = (v, k)

def update_row(row_id, label):
    v = f"{label}_{seed}_{row_id}"
    k = (row_id * 31 + seed) % 101
    emit(f"UPDATE t SET v='{esc(v)}', k={k} WHERE id={row_id};")
    branches[current][row_id] = (v, k)

def delete_row(row_id):
    emit(f"DELETE FROM t WHERE id={row_id};")
    branches[current].pop(row_id, None)

def checkpoint(name):
    tag = f"seed{seed}_{name}"
    dl.append(
        "SELECT 'Q|%s|rows|' || coalesce(group_concat("
        "CAST(id AS TEXT) || ':' || v || ':' || CAST(k AS TEXT), ','), '') "
        "FROM (SELECT id, v, k FROM t ORDER BY id);" % tag
    )
    dt.append(
        "SELECT concat('Q|%s|rows|', coalesce(group_concat("
        "concat(cast(id as char), ':', v, ':', cast(k as char))), '')) "
        "FROM (SELECT id, v, k FROM t ORDER BY id) q;" % tag
    )
    dl.append(
        "SELECT 'Q|%s|branches|' || coalesce(group_concat("
        "name || ':' || dirty, ','), '') "
        "FROM (SELECT name, dirty FROM dolt_branches ORDER BY name);" % tag
    )
    dt.append(
        "SELECT concat('Q|%s|branches|', coalesce(group_concat("
        "concat(name, ':', dirty)), '')) "
        "FROM (SELECT name, dirty FROM dolt_branches ORDER BY name) q;" % tag
    )

def commit(msg):
    vc_call("dolt_add", "-A")
    vc_call("dolt_commit", "-m", msg)

def checkout(branch):
    global current
    vc_call("dolt_checkout", branch)
    current = branch

def branch_from_current(branch):
    vc_call("dolt_branch", branch)
    branches[branch] = dict(branches[current])

def merge_no_ff(branch):
    vc_call("dolt_merge", branch, "--no-ff", "-m", f"merge {branch}")
    merged = dict(branches[current])
    merged.update(branches[branch])
    branches[current] = merged

for _ in range(3):
    insert_row(next_id, "base")
    next_id += 1
commit("base")
checkpoint("base")

for episode in range(6):
    kind = rng.choice(["branch_merge", "reset", "index_update"])

    if kind == "branch_merge":
        checkout("main")
        branch = f"b{seed}_{episode}"
        branch_from_current(branch)
        checkout(branch)
        for _ in range(rng.randint(1, 3)):
            insert_row(next_id, f"{branch}_ins")
            next_id += 1
        commit(f"{branch} commit")
        checkpoint(f"{episode}_branch_committed")
        checkout("main")
        for _ in range(rng.randint(1, 2)):
            insert_row(next_id, "main_ins")
            next_id += 1
        commit(f"main {episode}")
        merge_no_ff(branch)
        checkpoint(f"{episode}_merged")

    elif kind == "reset":
        checkout("main")
        before = dict(branches[current])
        if branches[current]:
            row_id = rng.choice(sorted(branches[current]))
            update_row(row_id, "dirty_update")
        insert_row(next_id, "dirty_insert")
        next_id += 1
        checkpoint(f"{episode}_dirty_before_reset")
        vc_call("dolt_reset", "--hard")
        branches[current] = before
        checkpoint(f"{episode}_after_reset")

    elif kind == "index_update":
        checkout("main")
        if branches[current]:
            row_id = rng.choice(sorted(branches[current]))
            update_row(row_id, f"idx_update_{episode}")
            commit(f"idx update {episode}")
            checkpoint(f"{episode}_index_update")

dl.append("")
dt.append("")

with open(dl_path, "w") as f:
    f.write("\n".join(dl))
with open(dt_path, "w") as f:
    f.write("\n".join(dt))
PY
}

run_model_case() {
  local seed="$1"
  local name="model_seed_${seed}"
  local dir="$TMPROOT/$name"
  mkdir -p "$dir/dl" "$dir/dt"

  generate_case "$seed" "$dir/dl.sql" "$dir/dt.sql"

  local dl_rc dt_rc
  "$DOLTLITE" "$dir/dl/db" < "$dir/dl.sql" > "$dir/dl.raw" 2> "$dir/dl.err"
  dl_rc=$?

  (
    cd "$dir/dt" || exit 1
    vc_oracle_init_repo
    "$DOLT" sql -c -r csv < "$dir/dt.sql"
  ) > "$dir/dt.raw" 2> "$dir/dt.err"
  dt_rc=$?

  if [ "$dl_rc" -ne 0 ] || [ "$dt_rc" -ne 0 ]; then
    fail=$((fail+1))
    FAILED_NAMES="$FAILED_NAMES $name"
    echo "  FAIL: $name (runner error; dl_rc=$dl_rc dt_rc=$dt_rc)"
    echo "    doltlite stderr:"; sed 's/^/      /' "$dir/dl.err"
    echo "    dolt stderr:"; sed 's/^/      /' "$dir/dt.err"
    return
  fi

  local dl_out dt_out
  dl_out=$(normalize_model_output < "$dir/dl.raw")
  dt_out=$(normalize_model_output < "$dir/dt.raw")

  vc_oracle_assert_match "$name" "$dl_out" "$dt_out"
}

echo "=== Version Control Model Oracle Tests ==="
echo ""

for seed in 7 11 19 23 31; do
  run_model_case "$seed"
done

echo ""
echo "Results: $pass passed, $fail failed"
if [ "$fail" -ne 0 ]; then
  echo "Failed:$FAILED_NAMES"
  exit 1
fi
