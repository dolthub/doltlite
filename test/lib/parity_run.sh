#!/bin/bash
# Shared by doltlite_parity.sh and engine_floor_test.sh.
# Caller sets DOLTLITE, SQLITE3, PASS, FAIL, ERRORS.

run_parity() {
  local name="$1"
  local sql="$2"
  local out_dl out_sq rc_dl=0 rc_sq=0

  out_dl=$(echo "$sql" | perl -e 'alarm(10);exec @ARGV' "$DOLTLITE" :memory: 2>&1) || rc_dl=$?
  out_sq=$(echo "$sql" | perl -e 'alarm(10);exec @ARGV' "$SQLITE3" :memory: 2>&1) || rc_sq=$?

  if [ "$rc_dl" -eq 0 ] && [ "$rc_sq" -eq 0 ] && [ "$out_dl" = "$out_sq" ]; then
    PASS=$((PASS+1))
  else
    FAIL=$((FAIL+1))
    ERRORS="$ERRORS\nFAIL: $name\n  doltlite rc: $rc_dl\n  --- doltlite ---\n$out_dl\n  sqlite3 rc: $rc_sq\n  --- sqlite3 ---\n$out_sq\n"
  fi
}
