#!/usr/bin/env bash
set -euo pipefail
build=$(cd "${1:-build}" && pwd)
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
tmp=$(mktemp -d)
udid=
cleanup() {
  if [[ -n "$udid" ]]; then
    xcrun simctl shutdown "$udid" >/dev/null 2>&1 || true
    xcrun simctl delete "$udid" >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp"
}
trap cleanup EXIT
sdk=$(xcrun --sdk iphonesimulator --show-sdk-path)
runtime=$(xcrun simctl list runtimes available -j | python3 -c 'import json,sys; r=[v for v in json.load(sys.stdin)["runtimes"] if v["identifier"].startswith("com.apple.CoreSimulator.SimRuntime.iOS-")]; assert r,"No iOS simulator runtime"; print(max(r,key=lambda v:tuple(map(int,v["version"].split("."))))["identifier"])')
device=$(xcrun simctl list devices available -j | python3 -c 'import json,sys; d=[v for v in json.load(sys.stdin)["devices"].get(sys.argv[1],[]) if v["name"].startswith("iPhone")]; assert d,"No available iPhone for selected runtime"; print(d[0]["deviceTypeIdentifier"])' "$runtime")
arch=$(uname -m)
app="$tmp/ExportProbe.app"
mkdir -p "$app"
xcrun --sdk iphonesimulator clang -O1 -Werror -Wno-comment \
  -target "$arch-apple-ios15.0-simulator" -isysroot "$sdk" \
  -DDOLTLITE_ENABLE_REMOTES=0 -DSQLITE_THREADSAFE=1 -I"$build" \
  -c "$build/sqlite3.c" -o "$tmp/doltlite.o"
xcrun --sdk iphonesimulator clang -O1 -Werror -fobjc-arc \
  -target "$arch-apple-ios15.0-simulator" -isysroot "$sdk" -I"$build" \
  "$root/test/ios_dump_probe.m" "$tmp/doltlite.o" -framework Foundation -framework UIKit -lz \
  -o "$app/ExportProbe"
cat > "$app/Info.plist" <<'PLIST'
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleIdentifier</key><string>com.dolthub.doltlite.ExportProbe</string>
<key>CFBundleName</key><string>ExportProbe</string>
<key>CFBundleInfoDictionaryVersion</key><string>6.0</string>
<key>CFBundleExecutable</key><string>ExportProbe</string>
<key>CFBundlePackageType</key><string>APPL</string>
<key>CFBundleVersion</key><string>1</string>
<key>CFBundleShortVersionString</key><string>1.0</string>
<key>LSRequiresIPhoneOS</key><true/>
<key>CFBundleSupportedPlatforms</key><array><string>iPhoneSimulator</string></array>
<key>UIDeviceFamily</key><array><integer>1</integer></array>
<key>MinimumOSVersion</key><string>15.0</string>
<key>UILaunchScreen</key><dict/>
<key>UIApplicationSceneManifest</key><dict>
<key>UIApplicationSupportsMultipleScenes</key><false/>
<key>UISceneConfigurations</key><dict>
<key>UIWindowSceneSessionRoleApplication</key><array><dict>
<key>UISceneConfigurationName</key><string>ExportProbe</string>
<key>UISceneDelegateClassName</key><string>ExportProbeSceneDelegate</string>
</dict></array></dict></dict>
</dict></plist>
PLIST
codesign --force --sign - "$app"
udid=$(xcrun simctl create "DoltLite export probe" "$device" "$runtime")
xcrun simctl boot "$udid"
xcrun simctl bootstatus "$udid" -b
xcrun simctl install "$udid" "$app"
xcrun simctl launch --console "$udid" com.dolthub.doltlite.ExportProbe
container=$(xcrun simctl get_app_container "$udid" com.dolthub.doltlite.ExportProbe data)
test "$(cat "$container/tmp/doltlite-export-probe/result.txt")" = 0
echo 'iOS in-process logical export: PASS'
