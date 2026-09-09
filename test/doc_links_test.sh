#!/usr/bin/env bash
# Every relative link and same-file anchor in a tracked Markdown file must
# resolve. External URLs are not fetched. Skipped: upstream doc/*.md and
# autosetup/, and packaging/ READMEs whose links are package-relative.

set -u
cd "$(dirname "${BASH_SOURCE[0]}")/.."

git ls-files -z -- '*.md' ':!doc/*.md' ':!ext/**' ':!autosetup/**' ':!packaging/**' | python3 -c '
import os, re, sys

def slug(h):
    h = re.sub(r"[`*_]", "", h.strip().lower())
    h = re.sub(r"[^\w\s-]", "", h)
    return re.sub(r"\s", "-", h)

bad = 0
for path in filter(None, sys.stdin.read().split("\0")):
    text = open(path, encoding="utf-8").read()
    body = re.sub(r"```.*?```", "", text, flags=re.S)
    anchors = {slug(m.group(1)) for m in re.finditer(r"^#+\s+(.+?)\s*#*$", body, re.M)}
    for m in re.finditer(r"\]\(([^)\s]+)\)", body):
        target = m.group(1)
        if re.match(r"[a-z][a-z0-9+.-]*:", target):
            continue
        file, _, anchor = target.partition("#")
        if file:
            if not os.path.exists(os.path.join(os.path.dirname(path), file)):
                print(f"{path}: missing file {target}"); bad += 1
        elif anchor and anchor not in anchors:
            print(f"{path}: missing anchor {target}"); bad += 1
sys.exit(1 if bad else 0)
'
