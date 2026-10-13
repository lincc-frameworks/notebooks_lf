#!/bin/bash
# Smoke test: 4 of the 8840 DP2 object_lc partitions, 4 workers x 6 GB, ~1 min. Full scan: --n-partitions 0.
# usage: ./run_smoke.sh      env: N_PARTITIONS=4 WORKERS=4 MEMORY_LIMIT=6GB CPU_OFFSET=0 PY=... BENCH_SCRATCH=...
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$HERE/../common.sh"
N_PARTITIONS="${N_PARTITIONS:-4}"; WORKERS="${WORKERS:-4}"; MEMORY_LIMIT="${MEMORY_LIMIT:-6GB}"
OUT="$HERE/results/smoke"; rm -rf "$OUT"; mkdir -p "$OUT"
CPUS="$(pick_cpus "$WORKERS" "${CPU_OFFSET:-0}")"
echo "python=$PY cpus=$CPUS partitions=$N_PARTITIONS workers=$WORKERS mem=$MEMORY_LIMIT out=$OUT"
OMP_NUM_THREADS=2 taskset -c "$CPUS" "$PY" "$HERE/lightcurve_scan.py" --n-partitions "$N_PARTITIONS" --workers "$WORKERS" \
  --memory-limit "$MEMORY_LIMIT" --out-dir "$OUT" --dask-local-dir "$BENCH_SCRATCH/dask-lightcurve-scan" "$@" 2>&1 | quiet | tee "$OUT/smoke.log"
rc=${PIPESTATUS[0]}
n=$(ls "$OUT/perobj" 2>/dev/null | wc -l)
if [ "$rc" -eq 0 ] && [ "$n" -eq "$N_PARTITIONS" ] && [ -s "$OUT/flag_incidence.csv" ]; then echo "##### SMOKE OK ($n partition outputs)"; else echo "##### SMOKE FAILED (rc=$rc, $n outputs)"; exit 1; fi
