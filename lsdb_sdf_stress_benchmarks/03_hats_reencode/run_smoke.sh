#!/bin/bash
# Smoke test: cut 4 Gaia DR3 partitions + margin into a collection (~1.5 GB), re-encode it, verify the copy. ~3 min.
# usage: ./run_smoke.sh      env: N_PARTITIONS=4 WORKERS=4 KEEP=0 CATALOG=<hats path> PY=... BENCH_SCRATCH=... CPU_OFFSET=0
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$HERE/../common.sh"
N_PARTITIONS="${N_PARTITIONS:-4}"; WORKERS="${WORKERS:-4}"; CATALOG="${CATALOG:-/sdf/data/rubin/cosmicdata/lsdb/gaia_dr3}"
SRC="$BENCH_SCRATCH/reencode-smoke/src"; OUT="$BENCH_SCRATCH/reencode-smoke/out"; rm -rf "$SRC" "$OUT"
LOGDIR="$HERE/results/smoke"; rm -rf "$LOGDIR"; mkdir -p "$LOGDIR"
CPUS="$(pick_cpus "$WORKERS" "${CPU_OFFSET:-0}")"
echo "python=$PY cpus=$CPUS catalog=$CATALOG partitions=$N_PARTITIONS workers=$WORKERS scratch=$BENCH_SCRATCH"
{
  OMP_NUM_THREADS=2 taskset -c "$CPUS" "$PY" "$HERE/make_collection_subset.py" --catalog "$CATALOG" --n-partitions "$N_PARTITIONS" \
    --out "$SRC" --workers "$WORKERS" --dask-local-dir "$BENCH_SCRATCH/dask-reencode" 2>&1 | quiet; r1=${PIPESTATUS[0]}
  CPU_OFFSET="${CPU_OFFSET:-0}" "$HERE/run_reencode.sh" "$SRC" "$OUT" "$WORKERS"; r2=$?
  taskset -c "$CPUS" "$PY" "$HERE/verify_copy.py" "$SRC" "$OUT" 2>&1 | quiet; r3=${PIPESTATUS[0]}
  du -sh "$SRC" "$OUT" 2>/dev/null
  echo "subset rc=$r1 reencode rc=$r2 verify rc=$r3"
  [ "$r1" -eq 0 ] && [ "$r2" -eq 0 ] && [ "$r3" -eq 0 ] && echo "##### SMOKE OK" || echo "##### SMOKE FAILED"
} 2>&1 | tee "$LOGDIR/smoke.log"
grep -q "SMOKE OK" "$LOGDIR/smoke.log"; rc=$?
[ "${KEEP:-0}" = "1" ] || rm -rf "$SRC" "$OUT"
exit $rc
