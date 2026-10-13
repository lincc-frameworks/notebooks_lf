#!/bin/bash
# The 2026-09-17 cone ladder: one centre on the sky, cone radii calibrated so the LEFT (ZTF) row count steps
# 1, 3, 10, ... 1e8 (results/cone_calibration_cones.json, from `run_benchmark.py --calibrate-cones`); x axis = left rows.
# Every tool writes its result to parquet. Shared node, 32 physical cores, 128 GB cap, LSDB with ONE thread per worker,
# non-LSDB tools capped at TOOL_THREADS (scipy query, pyarrow decode, STILTS runner=parallel<n>) = the largest LSDB worker count,
# run NATIVELY (2026-09-17 12:30: both tables in memory, one call, result written once; the streamed records live in results/cones_streamed).
# Resumable: existing per-repeat JSONs are skipped.
# usage: nohup ./run_all_cones.sh > results/logs/run_all_cones_<ts>.log 2>&1 &
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Python with lsdb installed: $PY, else the lsdb-py313-linux env of the published runs, else `python` on PATH
if [ -z "${PY:-}" ]; then PY=/sdf/group/rubin/user/ncaplar/conda_envs/lsdb-py313-linux/bin/python; [ -x "$PY" ] || PY=python; fi
N_CPUS="${N_CPUS:-32}"; MEM_CAP_GB="${MEM_CAP_GB:-128}"; TOOL_THREADS="${TOOL_THREADS:-16}"
SEED="${SEED:-4,715}"                       # Order 4 / Npix 715 centre: RA 216.82 Dec +57.40, |b| ~ 55
N_RUNS="${N_RUNS:-3}"
CALIB="$HERE/results/cone_calibration_cones.json"
STEPS="${STEPS:-$("$PY" -c "import json,sys; print(json.load(open(sys.argv[1]))['steps'])" "$CALIB")}"
CPUS="$(lscpu -p=CPU,CORE | grep -v '^#' | sort -t, -k2,2n -u | head -n "$N_CPUS" | cut -d, -f1 | paste -sd,)"
export OMP_NUM_THREADS="$TOOL_THREADS" OPENBLAS_NUM_THREADS="$TOOL_THREADS" PYTHONUNBUFFERED=1
[ -d /lscratch ] && mkdir -p "/lscratch/$USER/dask-xmatch-bench"; mkdir -p "$HERE/results/logs"
TOOL_MODE="${TOOL_MODE:-native}"; TIME_CAP_S="${TIME_CAP_S:-1800}"   # native: whole tables in memory, one call per tool; a run over the cap ends that tool's curve
bench() { echo "##### $(date '+%F %T') bench $*"; taskset -c "$CPUS" "$PY" "$HERE/run_benchmark.py" --mem-cap-gb "$MEM_CAP_GB" --threads-per-worker 1 \
  --tool-threads "$TOOL_THREADS" --tool-mode "$TOOL_MODE" --time-cap-s "$TIME_CAP_S" --seed "$SEED" --output write "$@" 2>&1 | grep --line-buffered -vE "FutureWarning|warnings.warn|dashboard link|DeprecationWarning"; }

echo "##### steps: $STEPS"
ALL="${ALGOS:-lsdb}"   # LSDB only by default; ALGOS=lsdb,astropy,smatch,scipy,stilts reproduces the tool comparison (NOTES.md)
# 1. every step; LSDB 1/4/16 workers x 1 thread (plus the ALGOS tools, if any)
bench --config-name cones --columns default --algos "$ALL" --workers 1,4,16 --n-runs "$N_RUNS" --steps "$STEPS"
# 2. only RA and Dec columns, LSDB 16 workers
bench --config-name cones_radec --columns radec --algos lsdb --workers 16 --n-runs "$N_RUNS" --steps "$STEPS"
echo "##### $(date '+%F %T') ALL DONE"; touch "$HERE/results/cones/ALL_DONE"
