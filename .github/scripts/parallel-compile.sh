ci_compile_init() {
  ci_compile_jobs="$1"
  [[ "$ci_compile_jobs" =~ ^[1-9][0-9]*$ ]] || return 1
  ci_compile_tmp=$(mktemp -d)
  ci_compile_pids=()
  ci_compile_index=0
  trap 'ci_compile_cleanup "$?"' EXIT
}

ci_compile_reap() {
  local rc i
  while :; do
    for i in "${!ci_compile_pids[@]}"; do
      if ! kill -0 "${ci_compile_pids[$i]}" 2>/dev/null; then
        rc=0
        wait "${ci_compile_pids[$i]}" || rc=$?
        cat "$ci_compile_tmp/$i.log" || rc=$?
        unset 'ci_compile_pids[i]'
        return "$rc"
      fi
    done
    sleep 0.05
  done
}

ci_compile_wait() {
  local rc=0 status
  while [ "${#ci_compile_pids[@]}" -gt 0 ]; do
    if ci_compile_reap; then :; else
      status=$?
      if [ "$rc" -eq 0 ]; then rc=$status; fi
    fi
  done
  return "$rc"
}

ci_compile_cleanup() {
  local rc="$1" status
  if ci_compile_wait; then :; else
    status=$?
    if [ "$rc" -eq 0 ]; then rc=$status; fi
  fi
  rm -rf "$ci_compile_tmp"
  exit "$rc"
}

ci_compile() {
  if [ "$ci_compile_jobs" -eq 1 ]; then
    "$@"
    return
  fi
  "$@" > "$ci_compile_tmp/$ci_compile_index.log" 2>&1 &
  ci_compile_pids[$ci_compile_index]=$!
  ci_compile_index=$((ci_compile_index+1))
  if [ "${#ci_compile_pids[@]}" -ge "$ci_compile_jobs" ]; then
    ci_compile_reap
  fi
}
