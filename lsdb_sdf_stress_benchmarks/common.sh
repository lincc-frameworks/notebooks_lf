# Sourced by every run_smoke.sh: Python interpreter, scratch location, CPU pinning.
# Python with lsdb installed: $PY, else the lsdb-py313-linux env of the published runs, else `python` on PATH
if [ -z "${PY:-}" ]; then PY=/sdf/group/rubin/user/ncaplar/conda_envs/lsdb-py313-linux/bin/python; [ -x "$PY" ] || PY=python; fi
# Scratch for result catalogs and Dask spill: $BENCH_SCRATCH, else node-local /lscratch (sdfiana*, milano),
# else $SCRATCH, else the SDF per-user scratch area.
if [ -z "${BENCH_SCRATCH:-}" ]; then
  if [ -d /lscratch ]; then BENCH_SCRATCH="/lscratch/$USER/lsdb-bench"
  else BENCH_SCRATCH="${SCRATCH:-/sdf/scratch/users/${USER:0:1}/$USER}/lsdb-bench"; fi
fi
mkdir -p "$BENCH_SCRATCH"
export PYTHONUNBUFFERED=1
# pick_cpus N [OFFSET]: N distinct physical cores among the CPUs this process may use (shared node or SLURM
# cgroup), skipping the first OFFSET physical cores. One logical CPU per core, so N cores are N real cores.
pick_cpus() {
  "$PY" - "$1" "${2:-0}" <<'PYEOF'
import os, subprocess, sys
n, skip = int(sys.argv[1]), int(sys.argv[2]); allowed = sorted(os.sched_getaffinity(0)); core_of = {}
for line in subprocess.run(["lscpu", "-p=CPU,CORE"], capture_output=True, text=True).stdout.splitlines():
    if not line.startswith("#"):
        cpu, core = map(int, line.split(",")[:2]); core_of[cpu] = core
picked, seen = [], set()
for cpu in allowed:
    if core_of.get(cpu) not in seen:
        seen.add(core_of[cpu]); picked.append(cpu)
print(",".join(map(str, picked[skip:skip + n])))
PYEOF
}
# quiet the usual Dask/pandas noise in logs
quiet() { grep --line-buffered -vE "FutureWarning|DeprecationWarning|warnings.warn|dashboard link|UserWarning|^\s*$|it/s\]"; }
