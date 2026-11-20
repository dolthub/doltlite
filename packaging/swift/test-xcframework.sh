#!/usr/bin/env bash
set -euo pipefail

archive="${1:?XCFramework archive required}"
checksum="${2:?SwiftPM checksum required}"
version="${3:?engine version required}"
source="${4:?Swift package checkout required}"
test_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

actual_checksum="$(swift package compute-checksum "$archive")"
if [ "$actual_checksum" != "$checksum" ]; then
  echo "SwiftPM checksum mismatch: expected $checksum, got $actual_checksum" >&2
  exit 1
fi

consumer="$(mktemp -d)"
trap 'rm -rf "$consumer"' EXIT
cp -R "$source/Package.swift" "$source/Sources" "$source/Tests" "$consumer/"
cp "$archive" "$consumer/candidate.zip"
cp "$test_dir/XCFrameworkTests.swift" "$consumer/Tests/DoltliteTests/"

cd "$consumer"
python3 - "$version" <<'PY'
import json
from pathlib import Path
import re
import sys

manifest = Path("Package.swift")
text, replacements = re.subn(
    r'url:\s*"[^"]*",\s*checksum:\s*"[0-9a-f]+"',
    'path: "candidate.zip"',
    manifest.read_text(),
)
if replacements != 1:
    sys.exit("Expected exactly one remote binary target in Package.swift")
manifest.write_text(text)
Path("Tests/DoltliteTests/ExpectedVersion.swift").write_text(
    f"let expectedEngineVersion = {json.dumps(sys.argv[1])}\n"
)
PY

swift test
simulator="$(xcrun simctl list devices available --json | python3 -c '
import json
import sys

devices = json.load(sys.stdin)["devices"]
def version(runtime):
    return tuple(map(int, runtime.split(".iOS-")[-1].split("-")))

for runtime in sorted((key for key in devices if ".iOS-" in key), key=version, reverse=True):
    for device in devices[runtime]:
        if device["isAvailable"] and device["name"].startswith("iPhone"):
            print(device["udid"])
            sys.exit(0)
sys.exit("No available iPhone simulator")
')"
xcodebuild test -scheme Doltlite -destination "platform=iOS Simulator,id=$simulator" -parallel-testing-enabled NO
