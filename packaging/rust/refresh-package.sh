#!/usr/bin/env bash
set -euo pipefail

# Cargo normalizes archive mtimes; refresh them before reusing a target directory.
find "${1:?package directory required}" -type f -exec touch {} +
