#!/bin/bash

DOLTLITE="${1:-$(dirname "$0")/../build/doltlite}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CACHE_DIR="${DOLTLITE_COMPAT_CACHE:-$REPO_ROOT/.compat-cache}"
JOBS="${DOLTLITE_COMPAT_JOBS:-4}"

TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT
set -o pipefail

pass=0
fail=0

ok()   { echo "PASS: $1"; pass=$((pass+1)); }
bad()  { echo "FAIL: $1"; fail=$((fail+1)); }
check(){ if [ "$2" = "0" ]; then ok "$1"; else bad "$1${3:+ — $3}"; fi; }

DOLTLITE="$(cd "$(dirname "$DOLTLITE")" && pwd)/$(basename "$DOLTLITE")"
if [ ! -x "$DOLTLITE" ]; then
  echo "ERROR: current doltlite binary not found: $DOLTLITE"
  exit 1
fi
if ! git -C "$REPO_ROOT" rev-parse v0.11.0 >/dev/null 2>&1; then
  echo "ERROR: git tags unavailable (shallow clone?); fetch tags first:"
  echo "  git fetch --tags --unshallow"
  exit 1
fi

format_signature() {
  local ref="$1" f content
  for f in src/chunk_store.h src/prolly_node.h; do
    if [ "$ref" = "WORKTREE" ]; then
      content=$(cat "$REPO_ROOT/$f" 2>/dev/null)
    else
      content=$(git -C "$REPO_ROOT" show "$ref:$f" 2>/dev/null)
    fi
    printf '%s\n' "$content" | grep -E \
      '^#define +(CHUNK_STORE_VERSION|CHUNK_STORE_MAGIC|WS_FORMAT_VERSION|PROLLY_NODE_MAGIC) ' \
      | tr -s ' '
  done
}

chunk_store_version() {
  printf '%s\n' "$1" | sed -n 's/^#define CHUNK_STORE_VERSION \([0-9]*\)$/\1/p'
}

series_of() {  # v0.11.12[-g...] -> 0.11
  printf '%s\n' "$1" | sed -n 's/^v\([0-9]*\.[0-9]*\)\..*/\1/p'
}

compute_default_tags() {
  local cur_series="$1"
  local exact t series out="" priors=""
  local cur_first="" cur_last=""
  exact=$(git -C "$REPO_ROOT" describe --tags --exact-match 2>/dev/null)
  while read -r t; do
    [ "$t" = "$exact" ] && continue
    series=$(series_of "$t")
    [ -z "$series" ] && continue
    if [ "$series" = "$cur_series" ]; then
      [ -z "$cur_last" ] && cur_last="$t"
      cur_first="$t"
    else
      case " $priors " in
        *" $series "*) ;;
        *)
          if [ "$(printf '%s' "$priors" | wc -w)" -lt 2 ]; then
            out="$out $t"
            priors="$priors $series"
          fi
          ;;
      esac
    fi
  done <<EOF
$(git -C "$REPO_ROOT" tag --list 'v*' --sort=-v:refname)
EOF
  [ -n "$cur_last" ] && out="$out $cur_last"
  if [ -n "$cur_first" ] && [ "$cur_first" != "$cur_last" ]; then
    out="$out $cur_first"
  fi
  printf '%s' "$out"
}

sql() {  # sql <binary> <db> <sql...>
  local bin="$1" db="$2"; shift 2
  printf '%s\n' "$*" | "$bin" "$db" 2>&1
}

build_old() {  # build_old <tag> -> echoes binary path, rc!=0 on failure
  local tag="$1"
  local bin="$CACHE_DIR/$tag/doltlite"
  if [ -x "$bin" ]; then echo "$bin"; return 0; fi
  local wt="$CACHE_DIR/src-$tag"
  mkdir -p "$CACHE_DIR/$tag"
  rm -rf "$wt"
  git -C "$REPO_ROOT" worktree add --force --detach "$wt" "$tag" \
      >"$CACHE_DIR/$tag/build.log" 2>&1 || return 1
  ( cd "$wt" && ./configure && make -j"$JOBS" doltlite ) \
      >>"$CACHE_DIR/$tag/build.log" 2>&1
  local rc=$?
  if [ $rc -eq 0 ] && [ -x "$wt/doltlite" ]; then
    cp "$wt/doltlite" "$bin"
  else
    rc=1
  fi
  git -C "$REPO_ROOT" worktree remove --force "$wt" >/dev/null 2>&1
  [ $rc -eq 0 ] && echo "$bin"
  return $rc
}

make_fixture() {  # make_fixture <old-binary> <db>
  local bin="$1" db="$2"
  sql "$bin" "$db" "
    CREATE TABLE people(id INTEGER PRIMARY KEY, name TEXT, data BLOB);
    INSERT INTO people
      WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<40)
      SELECT i, 'name'||i, randomblob(32) FROM n;
    CREATE INDEX idx_people_name ON people(name);
    SELECT dolt_commit('-A','-m','initial data');
    INSERT INTO people
      WITH RECURSIVE n(i) AS (SELECT 41 UNION ALL SELECT i+1 FROM n WHERE i<60)
      SELECT i, 'name'||i, randomblob(32) FROM n;
    SELECT dolt_commit('-A','-m','second batch');
    SELECT dolt_branch('fixture-branch');
    SELECT dolt_tag('fixture-tag');
  " >/dev/null 2>&1
  local n
  n=$(sql "$bin" "$db" "SELECT count(*) FROM people;")
  [ "$n" = "60" ]
}

smoke_current() {  # smoke_current <db> <tag>
  local db="$1" tag="$2" out

  out=$(sql "$DOLTLITE" "$db" "SELECT count(*) FROM people;")
  check "$tag: open + full scan" "$([ "$out" = "60" ]; echo $?)" "got '$out'"

  out=$(sql "$DOLTLITE" "$db" "SELECT id FROM people WHERE name='name37';")
  check "$tag: indexed lookup" "$([ "$out" = "37" ]; echo $?)" "got '$out'"

  out=$(sql "$DOLTLITE" "$db" "SELECT count(*) FROM dolt_log;")
  check "$tag: dolt_log readable" "$([ "${out:-0}" -ge 2 ] 2>/dev/null; echo $?)" "got '$out'"

  out=$(sql "$DOLTLITE" "$db" "SELECT count(*) FROM dolt_tags;")
  check "$tag: dolt_tags readable" "$([ "${out:-0}" -ge 1 ] 2>/dev/null; echo $?)" "got '$out'"

  out=$(sql "$DOLTLITE" "$db" "
    INSERT INTO people VALUES(9001,'new-row',x'00');
    SELECT dolt_commit('-A','-m','commit from current version');")
  check "$tag: new commit on old db" \
    "$(printf '%s' "$out" | grep -qE '^[0-9a-f]{40}$'; echo $?)" "got '$out'"

  out=$(sql "$DOLTLITE" "$db" "SELECT dolt_checkout('fixture-branch');
    INSERT INTO people VALUES(9002,'branch-row',x'00');
    SELECT dolt_commit('-A','-m','branch commit');
    SELECT dolt_checkout('main');
    SELECT dolt_merge('fixture-branch');
    SELECT count(*) FROM people WHERE id=9002;")
  check "$tag: checkout + merge on old db" \
    "$(printf '%s' "$out" | tail -1 | grep -qx '1'; echo $?)" "got '$out'"
}

assert_refused() {  # assert_refused <db> <tag>
  local db="$1" tag="$2" out rc
  out=$(sql "$DOLTLITE" "$db" "SELECT count(*) FROM people;")
  rc=$?
  if [ $rc -ge 126 ]; then
    bad "$tag: refusal must be a clean error, not a crash (rc=$rc)"
    return
  fi
  if [ $rc -eq 0 ]; then
    bad "$tag: incompatible-format db opened without error — output '$out'"
    return
  fi
  if printf '%s' "$out" | grep -qiE 'not a database|notadb'; then
    ok "$tag: incompatible format refused cleanly"
  else
    bad "$tag: refused, but without the not-a-database error — output '$out'"
  fi
}

cur_sig=$(format_signature WORKTREE)
cur_csv=$(chunk_store_version "$cur_sig")
cur_desc=$(git -C "$REPO_ROOT" describe --tags 2>/dev/null)
cur_series=$(series_of "$cur_desc")

if [ -z "$cur_csv" ] || [ -z "$cur_series" ]; then
  echo "ERROR: cannot determine current CHUNK_STORE_VERSION ('$cur_csv')"
  echo "       or version series from git describe ('$cur_desc')"
  exit 1
fi

TAGS="${DOLTLITE_COMPAT_TAGS:-$(compute_default_tags "$cur_series")}"
if [ -z "${TAGS// }" ]; then
  echo "ERROR: no historical tags to test against"
  exit 1
fi
echo "current: $cur_desc (series $cur_series, chunk store v$cur_csv)"
echo "tags: $TAGS"
echo

for tag in $TAGS; do
  old_sig=$(format_signature "$tag")
  old_csv=$(chunk_store_version "$old_sig")
  old_series=$(series_of "$tag")
  if [ -z "$old_csv" ] || [ -z "$old_series" ]; then
    bad "$tag: cannot read format signature from tag"
    continue
  fi

  if [ "$old_series" = "$cur_series" ] && [ "$old_sig" != "$cur_sig" ]; then
    bad "$tag: format signature changed inside minor series $cur_series — bump the minor version"
    continue
  fi
  if [ "$old_csv" = "$cur_csv" ] && [ "$old_sig" != "$cur_sig" ]; then
    bad "$tag: format signature changed without a CHUNK_STORE_VERSION bump — old databases would open and misparse"
    continue
  fi

  echo "-- $tag (series $old_series, chunk store v$old_csv): building fixture"
  bin=$(build_old "$tag")
  if [ $? -ne 0 ] || [ ! -x "$bin" ]; then
    bad "$tag: failed to build old doltlite (see $CACHE_DIR/$tag/build.log)"
    continue
  fi

  db="$TMPDIR/compat-$tag.db"
  if ! make_fixture "$bin" "$db"; then
    bad "$tag: failed to create fixture database with old binary"
    continue
  fi
  ok "$tag: fixture created with old binary"

  if [ "$old_sig" = "$cur_sig" ]; then
    smoke_current "$db" "$tag"
  else
    assert_refused "$db" "$tag"
  fi
  echo
done

echo "Results: $pass passed, $fail failed"
[ $fail -eq 0 ]
