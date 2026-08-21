#!/usr/bin/env bash

# Local-only: CI's test-phase tarball mtimes predate the checkout, so every
# artifact looks stale. A stale local binary silently passes for gone code.
dl_is_ci() {
  [ -n "${CI:-}" ]
}

# Unknown is empty + status 0: callers use set -e; a miss-grep must not abort.
# GNU `stat -f` reports the filesystem (not mtime) and prints non-numeric text;
# validate the value, never trust the exit status.
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

# Newest source mtime as epoch. Empty if sources are unreachable (cannot tell).
dl_newest_source_epoch() {
  local repo_root="$1" newest="" f e
  [ -d "$repo_root/src" ] || { printf ''; return 0; }
  newest=$( { find "$repo_root/src" -maxdepth 1 -type f \
                  \( -name '*.c' -o -name '*.h' \) -printf '%T@\n' 2>/dev/null \
              || find "$repo_root/src" -maxdepth 1 -type f \
                  \( -name '*.c' -o -name '*.h' \) -exec stat -f '%m' {} + 2>/dev/null; } \
            | cut -d. -f1 \
            | grep -xE '[0-9]+' \
            | sort -n | tail -1 || true )
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

# Unknown ages are not stale: a check that cannot run must not invent a verdict.
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

# Report -fsanitize= names (not __asan_ prefixes) so the remedy pastes as-is.
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
