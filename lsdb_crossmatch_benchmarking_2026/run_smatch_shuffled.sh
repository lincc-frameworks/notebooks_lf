#!/bin/bash
# smatch on the 2026-09-17 cone ladder with its second (Gaia) argument passed in RANDOM order (`--smatch-shuffle`):
# smatch 0.10.2 builds an unbalanced BST keyed by nside-4096 pixel from that argument, and HATS order degenerates it into a
# chain (see README). Same node, cap, threads and steps as run_all_cones.sh; results in results/cones_smatch_shuffled/,
# plotted with `make_plot_notebook.py --smatch-config cones_smatch_shuffled`. Resumable.
# usage: nohup ./run_smatch_shuffled.sh > results/logs/run_smatch_shuffled_<ts>.log 2>&1 &
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Python with lsdb installed: $PY, else the lsdb-py313-linux env of the published runs, else `python` on PATH
if [ -z "${PY:-}" ]; then PY=/sdf/group/rubin/user/ncaplar/conda_envs/lsdb-py313-linux/bin/python; [ -x "$PY" ] || PY=python; fi
N_CPUS="${N_CPUS:-32}"; MEM_CAP_GB="${MEM_CAP_GB:-128}"; TOOL_THREADS="${TOOL_THREADS:-16}"
SEED="${SEED:-4,715}"; N_RUNS="${N_RUNS:-3}"; CONFIG="${CONFIG:-cones_smatch_shuffled}"
# the 17 calibrated steps up to 1e8 left rows (the calibration file also holds 3e8 and 1e9, which are not part of the figure)
STEPS="${STEPS:-cone:8.437894,cone:15.32955,cone:26.51309,cone:58.038,cone:98.72446,cone:165.5816,cone:254.488,cone:372.8226,cone:587.3358,cone:912.8864,cone:1563.811,cone:2605.741,cone:4429.416,cone:7466.107,cone:13894.98,cone:23931.84,cone:44133.7}"
CPUS="$(lscpu -p=CPU,CORE | grep -v '^#' | sort -t, -k2,2n -u | head -n "$N_CPUS" | cut -d, -f1 | paste -sd,)"
export OMP_NUM_THREADS="$TOOL_THREADS" OPENBLAS_NUM_THREADS="$TOOL_THREADS" PYTHONUNBUFFERED=1
[ -d /lscratch ] && mkdir -p "/lscratch/$USER/dask-xmatch-bench"; mkdir -p "$HERE/results/logs"
TIME_CAP_S="${TIME_CAP_S:-1800}"
echo "##### $(date '+%F %T') smatch shuffled, config $CONFIG, steps: $STEPS"
taskset -c "$CPUS" "$PY" "$HERE/run_benchmark.py" --mem-cap-gb "$MEM_CAP_GB" --threads-per-worker 1 --tool-threads "$TOOL_THREADS" \
  --tool-mode native --time-cap-s "$TIME_CAP_S" --seed "$SEED" --output write --smatch-shuffle \
  --config-name "$CONFIG" --columns default --algos smatch --workers 16 --n-runs "$N_RUNS" --steps "$STEPS" 2>&1 \
  | grep --line-buffered -vE "FutureWarning|warnings.warn|dashboard link|DeprecationWarning"
echo "##### $(date '+%F %T') ALL DONE"; touch "$HERE/results/$CONFIG/ALL_DONE"
