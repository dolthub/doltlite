#!/usr/bin/env bash
# The AGENT.md row a fresh database serves must equal the fenced block under
# "## The default text" in doc/doltlite/agents.md.
#
# Usage: oracle_doc_agent_guide_test.sh [doltlite] [ignored]

DOLTLITE="${1:-./doltlite}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOC="$SCRIPT_DIR/../doc/doltlite/agents.md"
TMPDIR=$(mktemp -d)
trap 'rm -rf "$TMPDIR"' EXIT

awk '/^## The default text$/{s=1} s && /^````markdown$/{f=1; next} f && /^````$/{exit} f' "$DOC" > "$TMPDIR/doc.md"
"$DOLTLITE" -newline '' "$TMPDIR/fresh.db" "SELECT doc_text FROM dolt_docs WHERE doc_name='AGENT.md';" > "$TMPDIR/engine.md" 2>&1

if [ ! -s "$TMPDIR/doc.md" ]; then
  echo "FAIL: agents.md has no fenced block under '## The default text'"; echo "Results: 0 passed, 1 failed"; exit 1
fi
if diff -u "$TMPDIR/doc.md" "$TMPDIR/engine.md" > "$TMPDIR/diff.txt"; then
  echo "PASS: AGENT.md default text matches agents.md"; echo "Results: 1 passed, 0 failed"; exit 0
fi
echo "FAIL: AGENT.md default text differs from agents.md (- doc, + engine)"; cat "$TMPDIR/diff.txt"
echo "Results: 0 passed, 1 failed"; exit 1
