#!/bin/bash
# Launcher for run_benchmark.py on a SHARED interactive node (SLAC SDF sdfiana*).
# Pins the whole process tree (Dask workers, pyarrow, numpy threads, the astropy/smatch child) to
# N_CPUS distinct physical cores with taskset and runs detached with a log.
#
# usage: ./run_benchmark.sh <config-name> [run_benchmark.py args...]
#   N_CPUS=32 (default)   MEM_CAP_GB=128 (default)   PY=<python> (default: lsdb-py313-linux env)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Python with lsdb installed: $PY, else the lsdb-py313-linux env of the published runs, else `python` on PATH
if [ -z "${PY:-}" ]; then PY=/sdf/group/rubin/user/ncaplar/conda_envs/lsdb-py313-linux/bin/python; [ -x "$PY" ] || PY=python; fi
N_CPUS="${N_CPUS:-32}"
MEM_CAP_GB="${MEM_CAP_GB:-128}"
CONFIG="${1:?config name (results subdirectory), e.g. default or radec}"
shift

# One logical CPU per physical core, so 32 "cpus" are 32 real cores, not 16 cores x 2 hyperthreads.
CPUS="$(lscpu -p=CPU,CORE | grep -v '^#' | sort -t, -k2,2n -u | head -n "$N_CPUS" | cut -d, -f1 | paste -sd,)"

mkdir -p "$HERE/results/logs"; [ -d /lscratch ] && mkdir -p "/lscratch/$USER/dask-xmatch-bench"
LOG="$HERE/results/logs/${CONFIG}_$(date +%Y-%m-%d_%H-%M-%S).log"

export OMP_NUM_THREADS="$N_CPUS" OPENBLAS_NUM_THREADS="$N_CPUS" MKL_NUM_THREADS="$N_CPUS"
export PYTHONUNBUFFERED=1

echo "cpus=$CPUS mem_cap=${MEM_CAP_GB}GB log=$LOG"
nohup taskset -c "$CPUS" "$PY" "$HERE/run_benchmark.py" --config-name "$CONFIG" --mem-cap-gb "$MEM_CAP_GB" "$@" > "$LOG" 2>&1 &
echo "pid=$!"
