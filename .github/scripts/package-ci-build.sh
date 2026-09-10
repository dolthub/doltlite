#!/usr/bin/env bash

set -euo pipefail

archive="${1:?Usage: package-ci-build.sh <archive.tar.gz> <tar arguments...>}"
shift
tar -cf - "$@" | gzip -1 > "$archive"
