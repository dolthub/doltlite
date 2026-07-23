#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${1:-build-coverage}"
PROFILE_DIR="${2:-coverage-profiles}"
REPORT_DIR="${3:-coverage-report}"

case "$BUILD_DIR" in
  /*) ;;
  *) BUILD_DIR="$REPO_ROOT/$BUILD_DIR" ;;
esac
case "$PROFILE_DIR" in
  /*) ;;
  *) PROFILE_DIR="$REPO_ROOT/$PROFILE_DIR" ;;
esac
case "$REPORT_DIR" in
  /*) ;;
  *) REPORT_DIR="$REPO_ROOT/$REPORT_DIR" ;;
esac

if [ ! -d "$BUILD_DIR" ]; then
  echo "ERROR: coverage build directory not found: $BUILD_DIR"
  exit 1
fi
if [ ! -d "$PROFILE_DIR" ]; then
  echo "ERROR: profile directory not found: $PROFILE_DIR"
  exit 1
fi
for tool in llvm-cov llvm-profdata; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "ERROR: $tool is required"
    exit 1
  fi
done

mkdir -p "$REPORT_DIR"

SOURCE_LIST="$REPORT_DIR/sources.txt"
PROFILE_LIST="$REPORT_DIR/profiles.txt"
PROFILE_DATA="$REPORT_DIR/coverage.profdata"
LCOV_DATA="$REPORT_DIR/coverage.lcov"
REPORT_TSV="$REPORT_DIR/coverage.tsv"
SUMMARY_ENV="$REPORT_DIR/summary.env"
SUMMARY_MD="$REPORT_DIR/summary.md"

find "$REPO_ROOT/src" -maxdepth 1 -type f \
  \( -name 'doltlite.c' \
     -o -name 'doltlite_*.c' \
     -o -name 'prolly_*.c' \
     -o -name 'chunk_*.c' \
     -o -name 'pager_shim.c' \
     -o -name 'sortkey.c' \
     -o -name 'btree_orig_api.c' \) \
  | LC_ALL=C sort > "$SOURCE_LIST"

find "$PROFILE_DIR" -type f -name '*.profraw' | LC_ALL=C sort > "$PROFILE_LIST"
if [ ! -s "$PROFILE_LIST" ]; then
  echo "ERROR: no LLVM raw profiles found under $PROFILE_DIR"
  exit 1
fi

llvm-profdata merge -sparse -f "$PROFILE_LIST" -o "$PROFILE_DATA"

OBJECT_ARGS=()
for object in doltlite testfixture doltlite-remotesrv; do
  if [ -x "$BUILD_DIR/$object" ]; then
    if [ ${#OBJECT_ARGS[@]} -eq 0 ]; then
      OBJECT_ARGS+=("$BUILD_DIR/$object")
    else
      OBJECT_ARGS+=(-object "$BUILD_DIR/$object")
    fi
  fi
done
if [ ${#OBJECT_ARGS[@]} -eq 0 ]; then
  echo "ERROR: no instrumented coverage objects found in $BUILD_DIR"
  exit 1
fi

SOURCE_ARGS=()
while IFS= read -r source; do
  SOURCE_ARGS+=(--sources "$source")
done < "$SOURCE_LIST"

llvm-cov export \
  "${OBJECT_ARGS[@]}" \
  -instr-profile="$PROFILE_DATA" \
  -format=lcov \
  "${SOURCE_ARGS[@]}" > "$LCOV_DATA"

printf 'file\tlines_covered\tlines_total\tline_percent\tbranches_covered\tbranches_total\tbranch_percent\tfunctions_covered\tfunctions_total\tfunction_percent\n' > "$REPORT_TSV"

awk -v root="$REPO_ROOT/" '
  function pct(covered, total) {
    if( total==0 ) return "-"
    return sprintf("%.2f", covered * 100.0 / total)
  }
  function emit() {
    if( file=="" ) return
    sub("^" root, "", file)
    printf "%s\t%d\t%d\t%s\t%d\t%d\t%s\t%d\t%d\t%s\n",
      file, lh, lf, pct(lh, lf), brh, brf, pct(brh, brf),
      fnh, fnf, pct(fnh, fnf)
  }
  /^SF:/ { file=substr($0, 4); next }
  /^LH:/ { lh=substr($0, 4)+0; next }
  /^LF:/ { lf=substr($0, 4)+0; next }
  /^BRH:/ { brh=substr($0, 5)+0; next }
  /^BRF:/ { brf=substr($0, 5)+0; next }
  /^FNH:/ { fnh=substr($0, 5)+0; next }
  /^FNF:/ { fnf=substr($0, 5)+0; next }
  /^end_of_record$/ {
    emit()
    file=""; lh=0; lf=0; brh=0; brf=0; fnh=0; fnf=0
  }
' "$LCOV_DATA" | LC_ALL=C sort >> "$REPORT_TSV"

read -r source_count sum_lc sum_lt sum_bc sum_bt sum_fc sum_ft < <(
  awk -F '\t' 'NR>1 {
    files++
    lc += $2; lt += $3
    bc += $5; bt += $6
    fc += $8; ft += $9
  }
  END { print files+0, lc+0, lt+0, bc+0, bt+0, fc+0, ft+0 }' "$REPORT_TSV"
)

if [ "$source_count" -eq 0 ]; then
  echo "ERROR: LLVM coverage report contained no DoltLite source files"
  exit 1
fi

percent() {
  awk -v covered="$1" -v total="$2" 'BEGIN {
    if( total==0 ){ printf "-" }
    else{ printf "%.2f", covered * 100.0 / total }
  }'
}

line_percent="$(percent "$sum_lc" "$sum_lt")"
branch_percent="$(percent "$sum_bc" "$sum_bt")"
function_percent="$(percent "$sum_fc" "$sum_ft")"
profile_count="$(wc -l < "$PROFILE_LIST" | tr -d ' ')"
{
  echo "RAW_PROFILES=$profile_count"
  echo "SOURCE_FILES=$source_count"
  echo "LINES_COVERED=$sum_lc"
  echo "LINES_TOTAL=$sum_lt"
  echo "LINE_PERCENT=$line_percent"
  echo "BRANCHES_COVERED=$sum_bc"
  echo "BRANCHES_TOTAL=$sum_bt"
  echo "BRANCH_PERCENT=$branch_percent"
  echo "FUNCTIONS_COVERED=$sum_fc"
  echo "FUNCTIONS_TOTAL=$sum_ft"
  echo "FUNCTION_PERCENT=$function_percent"
} > "$SUMMARY_ENV"

{
  echo "## DoltLite source coverage"
  echo ""
  echo "| Metric | Covered | Total | Coverage |"
  echo "|---|---:|---:|---:|"
  echo "| Lines | $sum_lc | $sum_lt | ${line_percent}% |"
  echo "| Branches | $sum_bc | $sum_bt | ${branch_percent}% |"
  echo "| Functions | $sum_fc | $sum_ft | ${function_percent}% |"
  echo ""
  echo "Merged $profile_count pooled raw profiles from the distributed Linux correctness jobs."
  echo ""
  echo "<details>"
  echo "<summary>Per-file coverage ($source_count files)</summary>"
  echo ""
  echo "| File | Lines | Branches | Functions |"
  echo "|---|---:|---:|---:|"
  awk -F '\t' 'NR>1 {
    lp = $4=="-" ? "-" : $4 "%"
    bp = $7=="-" ? "-" : $7 "%"
    fp = $10=="-" ? "-" : $10 "%"
    printf "| `%s` | %s | %s | %s |\n", $1, lp, bp, fp
  }' "$REPORT_TSV"
  echo ""
  echo "</details>"
} > "$SUMMARY_MD"

rm -rf "$REPORT_DIR/html"
llvm-cov show \
  "${OBJECT_ARGS[@]}" \
  -instr-profile="$PROFILE_DATA" \
  -format=html \
  -output-dir="$REPORT_DIR/html" \
  "${SOURCE_ARGS[@]}" >/dev/null

cat "$SUMMARY_MD"

floor_failed=0
check_floor() {
  metric="$1"
  actual="$2"
  minimum="$3"
  if [ -z "$minimum" ]; then
    return
  fi
  if ! awk -v actual="$actual" -v minimum="$minimum" \
      'BEGIN { exit !(actual+0 >= minimum+0) }'; then
    echo "ERROR: $metric coverage ${actual}% is below ${minimum}%"
    floor_failed=1
  fi
}

check_floor "line" "$line_percent" "${DOLTLITE_COVERAGE_MIN_LINES:-}"
check_floor "branch" "$branch_percent" "${DOLTLITE_COVERAGE_MIN_BRANCHES:-}"
check_floor "function" "$function_percent" "${DOLTLITE_COVERAGE_MIN_FUNCTIONS:-}"
exit "$floor_failed"
