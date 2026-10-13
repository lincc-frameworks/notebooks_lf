#!/bin/bash
# The largest default-column LSDB steps on a DEDICATED SLURM node (milano: 2 x AMD EPYC 7713, 128 cores,
# 480 GB, no hyperthreading). On the shared interactive node the 16 x 8 GB workers were restarted for
# exceeding their memory budget at pixels:1024 and pixels:1791 (see README), so those two points are
# re-measured here with the same 32-core pinning and a 256 GB cap (16 GB per worker). pixels:512 is
# repeated as the overlap point with the interactive-node curve.
# Results go to results/default_slurm/ (the contaminated results/default/ JSONs are kept).
#
# submit from this directory:
#   sbatch --output="$PWD/results/logs/slurm_default_%j.log" run_slurm.sh
# another account/partition: sbatch --account=<acct> --partition=<part> ... run_slurm.sh (command-line flags
# override the #SBATCH lines below); STEPS=... MEM_CAP_GB=... N_CPUS=... are read from the environment.
#SBATCH --job-name=xmatch-bench-default
#SBATCH --partition=milano
#SBATCH --account=rubin:default
#SBATCH --qos=preemptable
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --exclusive
#SBATCH --mem=0
#SBATCH --time=04:00:00
set -uo pipefail
# sbatch copies this script to /var/spool/slurmd/, so the script directory is the SUBMIT directory
HERE="${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
[ -f "$HERE/run_benchmark.py" ] || { echo "run_benchmark.py not found in $HERE; submit from the benchmark directory"; exit 2; }
# Python with lsdb installed: $PY, else the lsdb-py313-linux env of the published runs, else `python` on PATH
if [ -z "${PY:-}" ]; then PY=/sdf/group/rubin/user/ncaplar/conda_envs/lsdb-py313-linux/bin/python; [ -x "$PY" ] || PY=python; fi
N_CPUS="${N_CPUS:-32}"; MEM_CAP_GB="${MEM_CAP_GB:-256}"
STEPS="${STEPS:-pixels:512,pixels:1024,pixels:1791}"

echo "##### $(date '+%F %T') job ${SLURM_JOB_ID:-none} on $(hostname)"
lscpu | grep -E 'Model name|^CPU\(s\)|Thread|Socket'; free -g | head -2
# First N_CPUS physical cores FROM THE JOB'S ALLOWED SET. SLURM keeps CoreSpecCount=8 cores for the system on
# milano (CPUSpecList=24-31 on sdfmilan090), so a plain "taskset -c 0-31" is silently cut to 24 CPUs by the
# job cgroup (that is what happened in job 38418325: settings.json says n_cpus=24).
CPUS="$("$PY" - "$N_CPUS" <<'PYEOF'
import os, subprocess, sys
n = int(sys.argv[1]); allowed = sorted(os.sched_getaffinity(0))
core_of = {}
for line in subprocess.run(["lscpu", "-p=CPU,CORE"], capture_output=True, text=True).stdout.splitlines():
    if not line.startswith("#"):
        cpu, core = map(int, line.split(",")[:2]); core_of[cpu] = core
picked, seen = [], set()
for cpu in allowed:
    if core_of.get(cpu) not in seen:
        seen.add(core_of[cpu]); picked.append(cpu)
    if len(picked) == n:
        break
print(",".join(map(str, picked)), file=sys.stdout)
print(f"allowed cpus: {len(allowed)}; using {len(picked)} distinct physical cores", file=sys.stderr)
PYEOF
)"
if [ -d /lscratch ] && mkdir -p "/lscratch/$USER/dask-xmatch-bench" 2>/dev/null; then
  DASK_DIR="/lscratch/$USER/dask-xmatch-bench"
else
  DASK_DIR="${SCRATCH:-/sdf/scratch/users/${USER:0:1}/$USER}/dask-xmatch-bench-${SLURM_JOB_ID:-nojob}"; mkdir -p "$DASK_DIR"
fi
echo "cpus=$CPUS mem_cap=${MEM_CAP_GB}GB dask_dir=$DASK_DIR steps=$STEPS"
export OMP_NUM_THREADS="$N_CPUS" OPENBLAS_NUM_THREADS="$N_CPUS" PYTHONUNBUFFERED=1

echo "##### $(date '+%F %T') bench"
taskset -c "$CPUS" "$PY" "$HERE/run_benchmark.py" --config-name default_slurm --columns default --algos lsdb \
  --workers 16 --n-runs 2 --steps "$STEPS" --mem-cap-gb "$MEM_CAP_GB" --threads-per-worker 1 \
  --dask-local-dir "$DASK_DIR" 2>&1 | grep --line-buffered -vE "FutureWarning|warnings.warn|dashboard link"
rc=${PIPESTATUS[0]}
echo "##### $(date '+%F %T') done rc=$rc"; [ "$rc" -eq 0 ] && touch "$HERE/results/default_slurm/DONE"
rm -rf "$DASK_DIR"
exit "$rc"
