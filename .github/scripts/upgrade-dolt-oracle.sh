#!/usr/bin/env bash
set -euo pipefail

repo_root=${REPO_ROOT:-$(cd "$(dirname "$0")/../.." && pwd)}
repository=${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is required}
branch_prefix=${DOLT_ORACLE_BRANCH_PREFIX:-automation/dolt-oracle-}
version_file="$repo_root/.dolt-oracle-version"

cd "$repo_root"
pinned=$(tr -d '[:space:]' < "$version_file")
latest=$(gh api repos/dolthub/dolt/releases/latest --jq .tag_name)
version_pattern='^v[0-9]+\.[0-9]+\.[0-9]+$'
if [[ ! "$pinned" =~ $version_pattern ]]; then
  echo "invalid pinned Dolt oracle version: $pinned"
  exit 1
fi
if [[ ! "$latest" =~ $version_pattern ]]; then
  echo "invalid latest Dolt release version: $latest"
  exit 1
fi

version_greater_than() {
  local left=${1#v}
  local right=${2#v}
  local left_major left_minor left_patch
  local right_major right_minor right_patch
  IFS=. read -r left_major left_minor left_patch <<<"$left"
  IFS=. read -r right_major right_minor right_patch <<<"$right"
  (( left_major > right_major )) ||
    (( left_major == right_major && left_minor > right_minor )) ||
    (( left_major == right_major && left_minor == right_minor && left_patch > right_patch ))
}

echo "Pinned Dolt oracle: $pinned"
echo "Latest Dolt release: $latest"

if ! version_greater_than "$latest" "$pinned"; then
  echo "No newer Dolt release"
  exit 0
fi

existing=$(gh pr list \
  --repo "$repository" \
  --state open \
  --json number,headRefName,headRefOid)
existing=$(jq -c \
  --arg prefix "$branch_prefix" \
  'map(select(.headRefName | startswith($prefix))) | sort_by(.number) | last // empty' \
  <<<"$existing")
merged_existing=0

if [ -n "$existing" ]; then
  number=$(jq -r .number <<<"$existing")
  branch=$(jq -r .headRefName <<<"$existing")
  head=$(jq -r .headRefOid <<<"$existing")
  target=${branch#"$branch_prefix"}
  if [ "$target" = "$latest" ]; then
    echo "Upgrade PR #$number already targets $latest"
    exit 0
  fi

  run=$(gh run list \
    --repo "$repository" \
    --branch "$branch" \
    --workflow CI \
    --event pull_request \
    --limit 20 \
    --json status,conclusion,headSha,databaseId)
  run=$(jq -c --arg head "$head" \
    'map(select(.headSha == $head)) | first // empty' <<<"$run")

  if [ -n "$run" ] &&
     [ "$(jq -r .status <<<"$run")" = completed ] &&
     [ "$(jq -r .conclusion <<<"$run")" = success ]; then
    gh pr merge "$number" \
      --repo "$repository" \
      --merge \
      --delete-branch
    merged_existing=1
  else
    gh pr comment "$number" \
      --repo "$repository" \
      --body "Superseded by Dolt $latest. CI was not fully green, so this upgrade will be replaced."
    gh pr close "$number" \
      --repo "$repository" \
      --delete-branch
  fi
fi

git fetch origin master
attempts=1
[ "$merged_existing" -eq 1 ] && attempts=5
for _ in $(seq 1 "$attempts"); do
  master_version=$(git show origin/master:.dolt-oracle-version 2>/dev/null | tr -d '[:space:]' || true)
  if [ "$master_version" = "$latest" ]; then
    echo "Master already pins $latest"
    exit 0
  fi
  [ "$attempts" -eq 1 ] && break
  sleep "${DOLT_ORACLE_MERGE_WAIT_SECONDS:-2}"
  git fetch origin master
done
git checkout -B "${branch_prefix}${latest}" origin/master
pinned=$(tr -d '[:space:]' < "$version_file")

printf '%s\n' "$latest" > "$version_file"
git config user.name "DoltHub Release Bot"
git config user.email "releases@dolthub.com"
git add .dolt-oracle-version
git commit \
  -m "Upgrade Dolt oracle to $latest" \
  -m "Co-Authored-By: OpenAI Codex <noreply@openai.com>"
git push --force-with-lease --set-upstream origin "${branch_prefix}${latest}"

body=$(mktemp)
trap 'rm -f "$body"' EXIT
cat > "$body" <<EOF
Upgrade the pinned Dolt reference implementation from $pinned to $latest.

The oracle suites in this PR's CI will expose any Dolt API or behavior drift that requires a corresponding DoltLite change.

[Dolt $latest release](https://github.com/dolthub/dolt/releases/tag/$latest)

Co-Authored-By: OpenAI Codex <noreply@openai.com>
EOF
gh pr create \
  --repo "$repository" \
  --base master \
  --head "${branch_prefix}${latest}" \
  --title "Upgrade Dolt oracle to $latest" \
  --body-file "$body"
