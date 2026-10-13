#!/bin/bash
# Smoke test: 8 DP2 object partitions x Gaia DR3, 4 workers x 8 GB, count mode, ~1.5 min. Full catalog: --n-partitions 0
# with --workers 48 --memory-limit 4GB (the July 2026 run; see dp2_gaia_crossmatch.py).
# usage: ./run_smoke.sh      env: N_PARTITIONS=8 WORKERS=4 MEMORY_LIMIT=8GB OUTPUT=count CPU_OFFSET=0 PY=... BENCH_SCRATCH=...
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$HERE/../common.sh"
N_PARTITIONS="${N_PARTITIONS:-8}"; WORKERS="${WORKERS:-4}"; MEMORY_LIMIT="${MEMORY_LIMIT:-8GB}"; OUTPUT="${OUTPUT:-count}"
OUT="$HERE/results/smoke"; rm -rf "$OUT"; mkdir -p "$OUT"
CPUS="$(pick_cpus "$WORKERS" "${CPU_OFFSET:-0}")"
echo "python=$PY cpus=$CPUS partitions=$N_PARTITIONS workers=$WORKERS mem=$MEMORY_LIMIT output=$OUTPUT"
OMP_NUM_THREADS=2 taskset -c "$CPUS" "$PY" "$HERE/dp2_gaia_crossmatch.py" --n-partitions "$N_PARTITIONS" --workers "$WORKERS" \
  --memory-limit "$MEMORY_LIMIT" --output "$OUTPUT" --out-dir "$BENCH_SCRATCH/dp2_gaia_crossmatch" \
  --dask-local-dir "$BENCH_SCRATCH/dask-dp2-gaia" "$@" 2>&1 | quiet | tee "$OUT/smoke.log"
rc=${PIPESTATUS[0]}
matched=$(grep '"result": "dp2_gaia_crossmatch"' "$OUT/smoke.log" | tail -1 | "$PY" -c "import json,sys; print(json.loads(sys.stdin.read())['matched'])" 2>/dev/null || echo 0)
if [ "$rc" -eq 0 ] && [ "${matched:-0}" -gt 0 ]; then echo "##### SMOKE OK ($matched matches)"; else echo "##### SMOKE FAILED (rc=$rc matched=$matched)"; exit 1; fi
