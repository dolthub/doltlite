#!/usr/bin/env bash
set -euo pipefail

pkg_dir=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/pkg/src" "$work/pkg/vendor"
cat > "$work/pkg/Cargo.toml" <<'TOML'
[package]
name = "doltlite-cache-fixture"
version = "0.0.0"
edition = "2021"
TOML
cat > "$work/pkg/build.rs" <<'RS'
fn main() {
    println!("cargo:rerun-if-changed=vendor/value");
    let value = std::fs::read_to_string("vendor/value").unwrap();
    println!("cargo:rustc-env=FIXTURE_VALUE={}", value.trim());
}
RS
cat > "$work/pkg/src/main.rs" <<'RS'
fn main() { println!("{}", env!("FIXTURE_VALUE")); }
RS
export CARGO_TARGET_DIR="$work/target"
for value in first second; do
  printf '%s\n' "$value" > "$work/pkg/vendor/value"
  find "$work/pkg" -type f -exec touch -t 200001010000 {} +
  bash "$pkg_dir/refresh-package.sh" "$work/pkg"
  output=$(cargo run --manifest-path "$work/pkg/Cargo.toml" --offline --release --quiet)
  [ "$output" = "$value" ] || {
    echo "FAIL: reused '$output' after changing packaged source to '$value'" >&2
    exit 1
  }
done
printf '%s\n' 'invalid Rust source' > "$work/pkg/src/main.rs"
find "$work/pkg" -type f -exec touch -t 200001010000 {} +
bash "$pkg_dir/refresh-package.sh" "$work/pkg"
if cargo run --manifest-path "$work/pkg/Cargo.toml" --offline --release --quiet > "$work/output" 2>&1; then
  echo 'FAIL: reused cached binary after introducing a compile error' >&2
  exit 1
fi
echo 'Cargo package cache invalidation tests passed'
