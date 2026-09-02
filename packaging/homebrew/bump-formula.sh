#!/usr/bin/env bash
# Rewrite url and sha256 in the Homebrew formula for a doltlite release.
# Usage: bump-formula.sh [--file PATH] VERSION SHA256

set -euo pipefail

FORMULA=""
while [ $# -gt 0 ]; do
  case "$1" in
    --file)
      FORMULA="${2:?}"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "usage: bump-formula.sh [--file PATH] VERSION SHA256" >&2
      exit 2
      ;;
    *)
      break
      ;;
  esac
done

VERSION="${1:-}"
SHA256="${2:-}"
if [ -z "$VERSION" ] || [ -z "$SHA256" ]; then
  echo "usage: bump-formula.sh [--file PATH] VERSION SHA256" >&2
  exit 2
fi
VERSION="${VERSION#v}"

if ! printf '%s' "$VERSION" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$'; then
  echo "version must be X.Y.Z (got ${VERSION})" >&2
  exit 2
fi
if ! printf '%s' "$SHA256" | grep -Eq '^[0-9a-f]{64}$'; then
  echo "sha256 must be 64 lowercase hex characters" >&2
  exit 2
fi

if [ -z "$FORMULA" ]; then
  FORMULA="$(cd "$(dirname "$0")" && pwd)/doltlite.rb"
fi
if [ ! -f "$FORMULA" ]; then
  echo "formula not found: $FORMULA" >&2
  exit 1
fi

URL="https://github.com/dolthub/doltlite/releases/download/v${VERSION}/doltlite-autoconf-${VERSION}.tar.gz"

python3 - "$FORMULA" "$URL" "$SHA256" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
url = sys.argv[2]
sha256 = sys.argv[3]
text = path.read_text()
url_pat = r'url "https://github.com/dolthub/doltlite/releases/download/v[^"]+"'
sha_pat = r'sha256 "[0-9a-fA-F]+"'
n_url = len(re.findall(url_pat, text))
n_sha = len(re.findall(sha_pat, text))
if n_url != 1 or n_sha != 1:
    raise SystemExit(
        f"expected exactly one url and one sha256, got url={n_url} sha256={n_sha}"
    )
text = re.sub(url_pat, f'url "{url}"', text, count=1)
text = re.sub(sha_pat, f'sha256 "{sha256}"', text, count=1)
path.write_text(text)
PY
