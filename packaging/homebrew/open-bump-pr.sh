#!/bin/bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 VERSION RELEASE_TAG" >&2
  exit 2
fi

version="$1"
release_tag="$2"
repository="dolthub/doltlite"
branch="automation/homebrew-${version}"

find_open_pr() {
  gh pr list --repo "$repository" --base master --head "$branch" \
    --state open --json number --jq '.[0].number'
}

open_pr="$(find_open_pr)"
if [ -n "$open_pr" ]; then
  echo "Homebrew bump pull request #${open_pr} already exists"
  exit 0
fi

if ! gh api "repos/${repository}/git/ref/heads/${branch}" >/dev/null 2>&1; then
  git checkout -b "$branch"
  git add packaging/homebrew/doltlite.rb
  git commit -m "Bump Homebrew formula to ${version}"
  git push origin "$branch"
fi

if gh pr create --repo "$repository" --base master --head "$branch" \
  --title "Bump Homebrew formula to ${version}" \
  --body "Points the formula at the ${release_tag} release tarball. Opened by the release workflow."; then
  exit 0
fi

open_pr="$(find_open_pr)"
if [ -n "$open_pr" ]; then
  echo "Homebrew bump pull request #${open_pr} already exists"
  exit 0
fi
exit 1
