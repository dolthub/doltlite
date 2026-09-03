#!/bin/bash
# The in-repo Homebrew formula must not pin a pre-freeze tarball, and
# bump-formula.sh must rewrite url/sha256 the way the release job does.
. "$(dirname "$0")/lib/doltlite_test_common.sh"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FORMULA="$ROOT/packaging/homebrew/doltlite.rb"
BUMP="$ROOT/packaging/homebrew/bump-formula.sh"
OPEN_PR="$ROOT/packaging/homebrew/open-bump-pr.sh"
RELEASE_YML="$ROOT/.github/workflows/release.yml"

if [ ! -f "$FORMULA" ]; then
  echo "FAIL: missing $FORMULA"
  exit 1
fi
if [ ! -f "$BUMP" ]; then
  echo "FAIL: missing $BUMP"
  exit 1
fi
if [ ! -f "$OPEN_PR" ]; then
  echo "FAIL: missing $OPEN_PR"
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
   && grep -q 'packaging/homebrew/bump-formula.sh' "$RELEASE_YML" \
   && grep -q 'packaging/homebrew/open-bump-pr.sh' "$RELEASE_YML"; then
  dltest_pass
else
  dltest_fail "release_yml_has_homebrew_bump" "  missing release-homebrew job or helper"
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

cp "$FORMULA" "$TMP/path.rb"
cp "$TMP/path.rb" "$TMP/path-before.rb"
if bash "$BUMP" --file "$TMP/path.rb" '9.9/../../evil' \
     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
     >/dev/null 2>&1; then
  dltest_fail "bump_formula_rejects_path_version" "  accepted a path-like version"
elif ! cmp -s "$TMP/path.rb" "$TMP/path-before.rb"; then
  dltest_fail "bump_formula_rejects_path_version" "  mutated the formula on invalid version"
else
  dltest_pass
fi

cp "$FORMULA" "$TMP/dup.rb"
grep -E '^[[:space:]]*url "' "$TMP/dup.rb" | head -1 >> "$TMP/dup.rb"
cp "$TMP/dup.rb" "$TMP/dup-before.rb"
if bash "$BUMP" --file "$TMP/dup.rb" 9.9.9 \
     "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" \
     >/dev/null 2>&1; then
  dltest_fail "bump_formula_rejects_duplicate_url" "  rewrote a formula with two urls"
elif ! cmp -s "$TMP/dup.rb" "$TMP/dup-before.rb"; then
  dltest_fail "bump_formula_rejects_duplicate_url" "  mutated the formula before rejecting"
else
  dltest_pass
fi

if grep -E 'tar czf doltlite-autoconf' -A6 "$RELEASE_YML" | grep -q 'doltlite.mk'; then
  dltest_pass
else
  dltest_fail "autoconf_tarball_includes_doltlite_mk" \
    "  release.yml tarball file list is missing doltlite.mk"
fi

mkdir "$TMP/bin"
cat > "$TMP/bin/gh" <<'EOF'
#!/bin/bash
printf 'gh %s\n' "$*" >> "$CALLS"
if [ "$1 $2" = "pr list" ]; then
  printf '%s\n' "$MOCK_OPEN_PR"
elif [ "$1" = "api" ]; then
  [ "$MOCK_BRANCH_EXISTS" = "1" ]
fi
EOF
cat > "$TMP/bin/git" <<'EOF'
#!/bin/bash
printf 'git %s\n' "$*" >> "$CALLS"
EOF
chmod +x "$TMP/bin/gh" "$TMP/bin/git"

run_open_pr() {
  : > "$TMP/calls"
  PATH="$TMP/bin:$PATH" CALLS="$TMP/calls" \
    MOCK_OPEN_PR="$1" MOCK_BRANCH_EXISTS="$2" \
    bash "$OPEN_PR" 9.9.9 v9.9.9 >/dev/null
}

if run_open_pr "" 1 \
   && grep -q '^gh pr create ' "$TMP/calls" \
   && ! grep -q '^git ' "$TMP/calls"; then
  dltest_pass
else
  dltest_fail "homebrew_existing_branch_opens_missing_pr" "$(sed 's/^/  /' "$TMP/calls")"
fi

if run_open_pr 2596 1 \
   && ! grep -q '^gh pr create ' "$TMP/calls" \
   && ! grep -q '^git ' "$TMP/calls"; then
  dltest_pass
else
  dltest_fail "homebrew_existing_pr_is_idempotent" "$(sed 's/^/  /' "$TMP/calls")"
fi

if run_open_pr "" 0 \
   && grep -q '^git checkout -b automation/homebrew-9.9.9$' "$TMP/calls" \
   && grep -q '^git push origin automation/homebrew-9.9.9$' "$TMP/calls" \
   && grep -q '^gh pr create ' "$TMP/calls"; then
  dltest_pass
else
  dltest_fail "homebrew_missing_branch_pushes_and_opens_pr" "$(sed 's/^/  /' "$TMP/calls")"
fi

dltest_finish
