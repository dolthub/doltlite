#!/usr/bin/env bash

# The freshness/flags helpers are sourced by scripts running under
# `set -euo pipefail`, where a grep or stat that legitimately finds nothing would
# abort the caller with no output. That is the failure mode the helpers exist to
# prevent, so it is worth a test of its own: every query must answer "unknown" as
# empty output with status 0.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
BUILD_DIR="${DOLTLITE_BUILD_DIR:-$REPO_ROOT/build}"

PASS=0
FAIL=0
ERRORS=""

ok() { PASS=$((PASS+1)); }
bad() { FAIL=$((FAIL+1)); ERRORS="$ERRORS\nFAIL: $1\n  $2"; }

# Each probe runs in its own strict-mode subshell and must print a trailing
# sentinel. The sentinel is the whole point: a helper that aborts the caller
# produces empty output, which is indistinguishable from a helper that correctly
# returned empty -- unless something after the call has to run for the probe to
# pass. Never assert on the helper's output alone.
probe() {
  local name="$1" body="$2" want="$3" got
  got=$(bash -c "
    set -euo pipefail
    source '$HERE/lib/build_artifacts.sh'
    $body
    printf '|alive'
  " 2>/dev/null)
  if [ "$got" = "$want|alive" ]; then
    ok
  else
    bad "$name" "expected '$want|alive', got '$got'"
  fi
}

echo "=== build artifact helper guards ==="

probe "sanitizers_missing_archive" \
  's=$(dl_archive_sanitizers /nonexistent/libx.a); printf "[%s]" "$s"' '[]'

probe "epoch_unreachable_sources" \
  'e=$(dl_newest_source_epoch /nonexistent); printf "[%s]" "$e"' '[]'

probe "file_epoch_missing_file" \
  'e=$(dl_file_epoch /nonexistent/file); printf "[%s]" "$e"' '[]'

probe "stale_unknown_epoch_is_not_stale" \
  'if dl_artifact_is_stale /nonexistent/file ""; then printf stale; else printf unknown; fi' \
  'unknown'

probe "stale_unknown_artifact_is_not_stale" \
  'if dl_artifact_is_stale /nonexistent/file 1; then printf stale; else printf unknown; fi' \
  'unknown'

probe "flags_ok_when_archive_absent" \
  'dl_check_archive_flags /nonexistent/libx.a "-g" && printf ok' 'ok'

# An archive with no sanitizer symbols is the common case, and the one whose
# empty grep result previously killed the caller.
if [ -f "$BUILD_DIR/libdoltlite.a" ]; then
  probe "sanitizers_clean_archive_survives" \
    "s=\$(dl_archive_sanitizers '$BUILD_DIR/libdoltlite.a'); printf '[%s]' \"\$s\"" '[]'
  probe "flags_ok_for_clean_archive" \
    "dl_check_archive_flags '$BUILD_DIR/libdoltlite.a' '-g' && printf ok" 'ok'
  # Which verdict is correct depends on the environment -- an extracted CI
  # artifact legitimately predates a fresh checkout -- so assert only that a
  # definite verdict comes back, which is the invariant callers rely on.
  probe "stale_check_reaches_a_verdict" \
    "e=\$(dl_newest_source_epoch '$REPO_ROOT')
     if dl_artifact_is_stale '$BUILD_DIR/libdoltlite.a' \"\$e\"; then v=stale; else v=fresh; fi
     case \$v in stale|fresh) printf verdict ;; *) printf none ;; esac" \
    'verdict'
else
  echo "  (skipping clean-archive probes: no $BUILD_DIR/libdoltlite.a)"
fi

# Sanitizer detection has to name -fsanitize= values, since the error message
# tells the reader to paste them back.
SAN_ARCHIVE="${DOLTLITE_SAN_ARCHIVE:-}"
if [ -n "$SAN_ARCHIVE" ] && [ -f "$SAN_ARCHIVE" ]; then
  probe "sanitizers_named_as_fsanitize_values" \
    "s=\$(dl_archive_sanitizers '$SAN_ARCHIVE'); printf '%s' \"\$s\"" 'address,undefined'
  probe "flags_mismatch_rejected" \
    "if dl_check_archive_flags '$SAN_ARCHIVE' '-g' 2>/dev/null; then printf allowed; else printf rejected; fi" \
    'rejected'
  probe "flags_match_accepted" \
    "if dl_check_archive_flags '$SAN_ARCHIVE' '-g -fsanitize=address,undefined' 2>/dev/null; then printf ok; else printf wrongly_rejected; fi" \
    'ok'
else
  echo "  (skipping sanitizer probes: set DOLTLITE_SAN_ARCHIVE to an instrumented libdoltlite.a)"
fi

echo
echo "Results: $PASS passed, $FAIL failed out of $((PASS+FAIL)) tests"
if [ "$FAIL" -gt 0 ]; then
  echo -e "$ERRORS"
  exit 1
fi
