#!/bin/bash
# Runs every workload's smoke test in sequence (about 12 min on a shared sdfiana node, longer when the Python env is
# not in the page cache yet) and prints a PASS/FAIL table. Each smoke test also runs on its own: <dir>/run_smoke.sh.
# usage: ./run_all_smoke.sh      env: ONLY="02_dp2_lightcurve_scan 04_lsdb_read_path" PY=... BENCH_SCRATCH=...
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; cd "$HERE"
ONLY="${ONLY:-01_crossmatch_ladder 02_dp2_lightcurve_scan 03_hats_reencode 04_lsdb_read_path 05_dp2_gaia_crossmatch}"
declare -A status; fail=0
for d in $ONLY; do
  echo "##### $(date '+%F %T') $d"; T0=$(date +%s)
  if (cd "$d" && ./run_smoke.sh); then status[$d]="PASS"; else status[$d]="FAIL"; fail=1; fi
  status[$d]="${status[$d]} $(( $(date +%s) - T0 )) s"
done
echo; printf "%-26s %s\n" "workload" "result"; for d in $ONLY; do printf "%-26s %s\n" "$d" "${status[$d]}"; done
exit $fail
