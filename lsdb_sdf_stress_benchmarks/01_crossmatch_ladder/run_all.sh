#!/bin/bash
# Full 2026 sweep on a shared node (32 physical cores, 128 GB): LSDB with ONE thread per worker so
# "N workers" = N cores; astropy/smatch as far as they fit in memory. Sequential; every step is
# resumable (existing per-repeat JSONs are skipped), so re-running this script only fills gaps.
# usage: nohup ./run_all.sh > results/logs/run_all_<ts>.log 2>&1 &
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Python with lsdb installed: $PY, else the lsdb-py313-linux env of the published runs, else `python` on PATH
if [ -z "${PY:-}" ]; then PY=/sdf/group/rubin/user/ncaplar/conda_envs/lsdb-py313-linux/bin/python; [ -x "$PY" ] || PY=python; fi
N_CPUS="${N_CPUS:-32}"; MEM_CAP_GB="${MEM_CAP_GB:-128}"
CPUS="$(lscpu -p=CPU,CORE | grep -v '^#' | sort -t, -k2,2n -u | head -n "$N_CPUS" | cut -d, -f1 | paste -sd,)"
export OMP_NUM_THREADS="$N_CPUS" OPENBLAS_NUM_THREADS="$N_CPUS" PYTHONUNBUFFERED=1
[ -d /lscratch ] && mkdir -p "/lscratch/$USER/dask-xmatch-bench"
bench() { echo "##### $(date '+%F %T') bench $*"; taskset -c "$CPUS" "$PY" "$HERE/run_benchmark.py" --mem-cap-gb "$MEM_CAP_GB" --threads-per-worker 1 "$@" 2>&1 | grep -vE "FutureWarning|warnings.warn|dashboard link"; }

SMALL="cone:115,cone:375,cone:1150,cone:3700,pixels:1,pixels:2,pixels:4,pixels:8,pixels:16,pixels:32"
# 1. LSDB, default columns, 1/4/16 workers x 1 thread, to 1e8 rows
bench --config-name default --columns default --algos lsdb --workers 1,4,16 --n-runs 3 --steps "$SMALL"
bench --config-name default --columns default --algos lsdb --workers 1,4,16 --n-runs 2 --steps pixels:64,pixels:128
# 2. smatch to 1e8 rows (astropy is out of memory from 5e7 rows on; smatch would be at 2e8)
bench --config-name default --columns default --algos smatch --n-runs 2 --steps pixels:64,pixels:128
# 3. LSDB beyond 1e8: each curve stops where a repeat would take longer than ~30 min
bench --config-name default --columns default --algos lsdb --workers 1 --n-runs 2 --steps pixels:256
bench --config-name default --columns default --algos lsdb --workers 4,16 --n-runs 2 --steps pixels:256,pixels:512
bench --config-name default --columns default --algos lsdb --workers 16 --n-runs 2 --steps pixels:1024,pixels:1791
# 4. only RA and Dec columns, LSDB 16 workers, full ladder
bench --config-name radec --columns radec --algos lsdb --workers 16 --n-runs 3 --steps "$SMALL"
bench --config-name radec --columns radec --algos lsdb --workers 16 --n-runs 2 --steps pixels:64,pixels:128,pixels:256,pixels:512,pixels:1024,pixels:1791
echo "##### $(date '+%F %T') ALL DONE"; touch "$HERE/results/ALL_DONE"
