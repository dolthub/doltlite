#!/usr/bin/env bash

# Guards against validating a build that is not the one you think you built.
# CI is immune -- it builds from a clean checkout and hands binaries to the test
# phase -- so these checks exist for local runs, where a stale binary silently
# reports a pass for code that no longer exists.

# Both guards are local-only, and not merely as an optimisation: CI's test phase
# untars a build directory whose mtimes predate its own fresh checkout, so every
# artifact there looks stale and every make target looks out of date. Applying
# either guard would mean a guaranteed false positive and a full rebuild in a
# phase whose whole point is not to build.
dl_is_ci() {
  [ -n "${CI:-}" ]
}

# Every query here reports "unknown" as empty output with status 0, never a
# non-zero status: callers run under `set -euo pipefail`, where a grep that
# legitimately matches nothing would otherwise abort the caller mid-run -- the
# exact silent failure this file exists to prevent.

# GNU and BSD stat spell mtime differently *and* each accepts the other's flag
# with an unrelated meaning instead of failing: GNU `stat -f` reports the
# filesystem, not the file, and happily succeeds printing non-numeric text. So a
# plain `||` chain silently yields garbage that later dies in a numeric compare,
# leaving staleness undetectable on one platform. Validate the value; never trust
# the exit status.
dl_is_epoch() {
  case "${1:-}" in
    '' | *[!0-9]* ) return 1 ;;
    * ) return 0 ;;
  esac
}

dl_file_epoch() {
  local e=""
  e=$(stat -c '%Y' "$1" 2>/dev/null || true)
  dl_is_epoch "$e" || e=$(stat -f '%m' "$1" 2>/dev/null || true)
  dl_is_epoch "$e" || e=""
  printf '%s' "$e"
  return 0
}

# Newest mtime across the engine sources, as a bare epoch second. Empty when the
# sources are not reachable (a prebuilt artifact directory), which is a "cannot
# tell" answer, never a "looks fine" answer.
dl_newest_source_epoch() {
  local repo_root="$1" newest="" f e
  [ -d "$repo_root/src" ] || { printf ''; return 0; }
  # Fast path, keeping only all-digit lines so a find/stat spelling that
  # "succeeds" with prose contributes nothing rather than a bogus maximum.
  newest=$( { find "$repo_root/src" -maxdepth 1 -type f \
                  \( -name '*.c' -o -name '*.h' \) -printf '%T@\n' 2>/dev/null \
              || find "$repo_root/src" -maxdepth 1 -type f \
                  \( -name '*.c' -o -name '*.h' \) -exec stat -f '%m' {} + 2>/dev/null; } \
            | cut -d. -f1 \
            | grep -xE '[0-9]+' \
            | sort -n | tail -1 || true )
  # Whether find's and stat's flavours happen to agree is not something to bet
  # the guard on, so fall back to the portable per-file query.
  if ! dl_is_epoch "$newest"; then
    newest=""
    for f in "$repo_root"/src/*.c "$repo_root"/src/*.h; do
      [ -f "$f" ] || continue
      e=$(dl_file_epoch "$f")
      dl_is_epoch "$e" || continue
      if [ -z "$newest" ] || [ "$e" -gt "$newest" ]; then newest="$e"; fi
    done
  fi
  dl_is_epoch "$newest" || newest=""
  printf '%s' "$newest"
  return 0
}

# 0 when the artifact predates the newest engine source. Unknown ages are not
# stale: a check that cannot run must not invent a verdict.
dl_artifact_is_stale() {
  local artifact="$1" src_epoch="$2"
  [ -n "$src_epoch" ] || return 1
  local art_epoch
  art_epoch=$(dl_file_epoch "$artifact")
  [ -n "$art_epoch" ] || return 1
  [ "$art_epoch" -lt "$src_epoch" ]
}

dl_stale_hint() {
  echo "  Rebuild before trusting these results:"
  echo "    cd ${1:-build} && make doltlite-c-tests-build"
}

# Sanitizer runtimes an archive expects its final link to provide. A test
# compiled without matching -fsanitize flags fails with hundreds of undefined
# __asan_*/__ubsan_* symbols, which reads as a code error rather than the build
# mismatch it is. Reported as -fsanitize= names, not __xsan_ symbol prefixes, so
# the remedy can be pasted as-is.
dl_archive_sanitizers() {
  local archive="$1" out=""
  if [ -f "$archive" ] && command -v nm >/dev/null 2>&1; then
    out=$( nm -u "$archive" 2>/dev/null \
           | grep -oE '_*__(asan|ubsan|tsan|msan)_' \
           | sed -E 's/_*__([a-z]+)_/\1/' \
           | sort -u \
           | sed -e 's/^asan$/address/' -e 's/^ubsan$/undefined/' \
                 -e 's/^tsan$/thread/'  -e 's/^msan$/memory/' \
           | paste -sd, - || true )
  fi
  printf '%s' "$out"
  return 0
}

# Fail early, and say which side to fix, when the archive was built with
# sanitizers the caller's flags do not ask for.
dl_check_archive_flags() {
  local archive="$1" flags="${2:-}"
  local sans
  sans=$(dl_archive_sanitizers "$archive")
  [ -n "$sans" ] || return 0
  case "$flags" in
    *-fsanitize*) return 0 ;;
  esac
  echo "ERROR: stale build, not a code failure." >&2
  echo "       $archive was built with -fsanitize=$sans but the current CFLAGS" >&2
  echo "       enable no sanitizer, so this link fails with hundreds of" >&2
  echo "       undefined sanitizer symbols. Pick one:" >&2
  echo "         make libdoltlite.a                              # clean archive" >&2
  echo "         CFLAGS=\"-g -fsanitize=$sans\" <this script>   # match it" >&2
  return 1
}
