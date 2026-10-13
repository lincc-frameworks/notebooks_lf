#!/bin/bash
# Smoke test: 16 Gaia partitions (~0.5 GB of the 4 columns), 4 threads, 2 reps, ~1 min. Checks that every variant
# returns the same mean. Full catalog: --n-partitions 0 (2016 partitions; lsdb ~320 s / pyarrow ~88 s on sdfiana).
# usage: ./run_smoke.sh      env: N_PARTITIONS=16 THREADS=4 REPS=2 ROOT=<hats catalog> COLUMN=... CPU_OFFSET=0 PY=...
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$HERE/../common.sh"
N_PARTITIONS="${N_PARTITIONS:-16}"; THREADS="${THREADS:-4}"; REPS="${REPS:-2}"
OUT="$HERE/results/smoke"; rm -rf "$OUT"; mkdir -p "$OUT"
CPUS="$(pick_cpus "$THREADS" "${CPU_OFFSET:-0}")"
EXTRA=(); [ -n "${ROOT:-}" ] && EXTRA+=(--root "$ROOT"); [ -n "${COLUMN:-}" ] && EXTRA+=(--column "$COLUMN")
echo "python=$PY cpus=$CPUS partitions=$N_PARTITIONS threads=$THREADS reps=$REPS"
OMP_NUM_THREADS="$THREADS" taskset -c "$CPUS" "$PY" "$HERE/readbench.py" --n-partitions "$N_PARTITIONS" --threads "$THREADS" --reps "$REPS" "${EXTRA[@]}" "$@" 2>&1 | quiet | tee "$OUT/readbench.jsonl"
rc=${PIPESTATUS[0]}
"$PY" - "$OUT/readbench.jsonl" "$REPS" <<'PYEOF'
import json, sys
recs = [json.loads(l) for l in open(sys.argv[1]) if l.startswith("{") and '"rep"' in l]
means = {r["mean"] for r in recs}; variants = {r["variant"] for r in recs}
print(f"{'variant':<16} {'npart':>5} {'median_s':>9} {'MB_read':>8} {'syscr':>7}")
for v in sorted(variants):
    rs = sorted(r["wall_s"] for r in recs if r["variant"] == v); r0 = next(r for r in recs if r["variant"] == v)
    print(f"{v:<16} {r0['npart']:>5} {rs[len(rs)//2]:>9.3f} {r0['rchar_MB']:>8.0f} {r0['syscr']:>7}")
ok = len(recs) == int(sys.argv[2]) * len(variants) and len(means) == 1 and len(variants) >= 2
print(f"{len(recs)} records, {len(variants)} variants, {'one' if len(means)==1 else len(means)} distinct mean(s)")
sys.exit(0 if ok else 1)
PYEOF
rc2=$?
[ "$rc" -eq 0 ] && [ "$rc2" -eq 0 ] && echo "##### SMOKE OK" || { echo "##### SMOKE FAILED (rc=$rc check=$rc2)"; exit 1; }
