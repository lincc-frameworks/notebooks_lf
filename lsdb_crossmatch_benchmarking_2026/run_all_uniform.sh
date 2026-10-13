#!/bin/bash
# The 2026-09-16 sweep: uniform ZTF partitions (2.5-3.5M rows) and every tool writing its result to parquet.
# Shared node, 32 physical cores, 128 GB cap, LSDB with ONE thread per worker. Sequential; every step is resumable
# (existing per-repeat JSONs are skipped), so re-running this script only fills gaps.
# usage: nohup ./run_all_uniform.sh > results/logs/run_all_uniform_<ts>.log 2>&1 &
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Python with lsdb installed: $PY, else the lsdb-py313-linux env of the published runs, else `python` on PATH
if [ -z "${PY:-}" ]; then PY=/sdf/group/rubin/user/ncaplar/conda_envs/lsdb-py313-linux/bin/python; [ -x "$PY" ] || PY=python; fi
N_CPUS="${N_CPUS:-32}"; MEM_CAP_GB="${MEM_CAP_GB:-128}"
BAND="${BAND:-2.5e6,3.5e6}"
CPUS="$(lscpu -p=CPU,CORE | grep -v '^#' | sort -t, -k2,2n -u | head -n "$N_CPUS" | cut -d, -f1 | paste -sd,)"
export OMP_NUM_THREADS="$N_CPUS" OPENBLAS_NUM_THREADS="$N_CPUS" PYTHONUNBUFFERED=1
[ -d /lscratch ] && mkdir -p "/lscratch/$USER/dask-xmatch-bench"
bench() { echo "##### $(date '+%F %T') bench $*"; taskset -c "$CPUS" "$PY" "$HERE/run_benchmark.py" --mem-cap-gb "$MEM_CAP_GB" --threads-per-worker 1 \
  --rows-band "$BAND" --output write "$@" 2>&1 | grep --line-buffered -vE "FutureWarning|warnings.warn|dashboard link|DeprecationWarning"; }

SMALL="cone:115,cone:375,cone:1150,cone:3700,pixels:1,pixels:2,pixels:4,pixels:8,pixels:16,pixels:32"
ALL="lsdb,astropy,smatch,scipy,stilts"
# 1. every tool on the small steps, 3 repeats; LSDB 1/4/16 workers x 1 thread
bench --config-name uniform --columns default --algos "$ALL" --workers 1,4,16 --n-runs 3 --steps "$SMALL"
# 2. 64 and 128 partitions, 2 repeats, every tool (STILTS stops here, or earlier when it runs out of Java heap)
bench --config-name uniform --columns default --algos "$ALL" --workers 1,4,16 --n-runs 2 --steps pixels:64,pixels:128
# 3. 256: LSDB 1/4/16 + astropy, smatch, scipy (astropy stops here: ~20 min per repeat)
bench --config-name uniform --columns default --algos lsdb,astropy,smatch,scipy --workers 1,4,16 --n-runs 2 --steps pixels:256
# 4. 512: LSDB 4/16 + scipy. smatch dropped here on 2026-09-17: its per-chunk cost grows with the size of the indexed
#    Gaia side (2.7 s/chunk at 8 partitions, 12.9 s at 256 -> 3258 s per repeat), so 512 would have taken ~3.5 h per repeat.
bench --config-name uniform --columns default --algos lsdb,scipy --workers 4,16 --n-runs 2 --steps pixels:512
# 5. 911 = every partition in the band: LSDB 16 workers
bench --config-name uniform --columns default --algos lsdb --workers 16 --n-runs 2 --steps pixels:911
# 6. only RA and Dec columns, LSDB 16 workers, full ladder
bench --config-name uniform_radec --columns radec --algos lsdb --workers 16 --n-runs 3 --steps "$SMALL"
bench --config-name uniform_radec --columns radec --algos lsdb --workers 16 --n-runs 2 --steps pixels:64,pixels:128,pixels:256,pixels:512,pixels:911
echo "##### $(date '+%F %T') ALL DONE"; touch "$HERE/results/uniform/ALL_DONE"
