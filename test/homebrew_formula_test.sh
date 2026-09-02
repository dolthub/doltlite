#!/bin/bash
# The in-repo Homebrew formula must not pin a pre-freeze tarball, and
# bump-formula.sh must rewrite url/sha256 the way the release job does.
. "$(dirname "$0")/lib/doltlite_test_common.sh"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FORMULA="$ROOT/packaging/homebrew/doltlite.rb"
BUMP="$ROOT/packaging/homebrew/bump-formula.sh"
RELEASE_YML="$ROOT/.github/workflows/release.yml"

if [ ! -f "$FORMULA" ]; then
  echo "FAIL: missing $FORMULA"
  exit 1
fi
if [ ! -f "$BUMP" ]; then
  echo "FAIL: missing $BUMP"
  exit 1
fi

formula_url() {
  grep -E '^[[:space:]]*url "' "$1" | head -1
}

if grep -q 'v0\.10\.6' "$FORMULA"; then
  dltest_fail "formula_not_pre_freeze_0_10_6" "  formula still pins v0.10.6"
else
  dltest_pass
fi

if echo "$(formula_url "$FORMULA")" | grep -Eq 'doltlite-autoconf-[0-9]+\.[0-9]+\.[0-9]+\.tar\.gz'; then
  dltest_pass
else
  dltest_fail "formula_url_is_autoconf_tarball" "  $(formula_url "$FORMULA")"
fi

url_ver=$(formula_url "$FORMULA" | sed -n 's|.*/v\([^/]*\)/doltlite-autoconf-.*|\1|p')
tarball_ver=$(formula_url "$FORMULA" | sed -n 's|.*/doltlite-autoconf-\([^"]*\)\.tar\.gz.*|\1|p')
if [ -n "$url_ver" ] && [ "$url_ver" = "$tarball_ver" ]; then
  dltest_pass
else
  dltest_fail "formula_url_version_matches_tarball" "  url=$url_ver tarball=$tarball_ver"
fi

if grep -q 'release-homebrew:' "$RELEASE_YML" \
   && grep -q 'packaging/homebrew/bump-formula.sh' "$RELEASE_YML"; then
  dltest_pass
else
  dltest_fail "release_yml_has_homebrew_bump" "  missing release-homebrew job or bump-formula.sh"
fi

TMP="$(mktemp -d "${TMPDIR:-/tmp}/doltlite-homebrew.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT
cp "$FORMULA" "$TMP/doltlite.rb"
if bash "$BUMP" --file "$TMP/doltlite.rb" 9.9.9 \
     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
   && formula_url "$TMP/doltlite.rb" | grep -q 'v9.9.9/doltlite-autoconf-9.9.9.tar.gz' \
   && grep -q 'sha256 "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"' \
        "$TMP/doltlite.rb"; then
  dltest_pass
else
  dltest_fail "bump_formula_rewrites_url_and_sha256" "  $(formula_url "$TMP/doltlite.rb")"
fi

if bash "$BUMP" --file "$TMP/doltlite.rb" 1.0.0 not-a-hash >/dev/null 2>&1; then
  dltest_fail "bump_formula_rejects_bad_sha256" "  accepted a non-hex sha256"
else
  dltest_pass
fi

dltest_finish
