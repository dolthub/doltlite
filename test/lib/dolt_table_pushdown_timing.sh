#!/bin/bash

history_pushdown_time() {
  local engine="$1" db="$2" row="$3"
  local point="SELECT count(*) FROM dolt_history_t WHERE id=$row;"
  local full="SELECT count(*) FROM dolt_history_t;"
  local trial i output times constrained unconstrained
  local constrained_samples="" unconstrained_samples=""
  for trial in 1 2 3; do
    if ! output=$({
      printf '%s\n' "$point" "$full" '.timer on'
      for i in $(seq 1 100); do
        printf '%s\n' "$point" "$full"
      done
    } | "$engine" -bail "$db" 2>&1); then
      printf '%s\n' "$output" >&2
      return 1
    fi
    if ! times=$(printf '%s\n' "$output" | awk '
      /Run Time:/ {
        if ($4 !~ /^[0-9]+\.[0-9]+$/) { bad=1; exit }
        n++
        if (n%2) constrained+=$4
        else unconstrained+=$4
      }
      END {
        if (bad || n!=200 || unconstrained<=0) exit 1
        printf "%.0f %.0f\n", int(constrained*1000+0.999),
                                int(unconstrained*1000+0.999)
      }
    '); then
      echo "history pushdown measurement did not return 200 query timings" >&2
      return 1
    fi
    read -r constrained unconstrained <<<"$times"
    constrained_samples="$constrained_samples$constrained"$'\n'
    unconstrained_samples="$unconstrained_samples$unconstrained"$'\n'
  done
  constrained=$(printf '%s' "$constrained_samples" | sort -n | sed -n '2p')
  unconstrained=$(printf '%s' "$unconstrained_samples" | sort -n | sed -n '2p')
  echo "  Query time: median of 3x100 constrained=${constrained}ms unconstrained=${unconstrained}ms"
  [ "$constrained" -le "$unconstrained" ]
}
