#!/bin/bash
# Smoke test: LSDB on three small cone steps (10, 1e3 and 1e5 ZTF rows around the seed partition), one repeat,
# 8 physical cores, 32 GB cap. Checks the Python environment, access to the catalogs, the output path and the Dask
# scratch dir before a real sweep. Records go to results/smoke/ (overwritten on every run) and a summary is printed.
#
# usage: ./run_smoke.sh [extra run_benchmark.py args]
#   ALGOS=lsdb                 (default; the comparison tools astropy,smatch,scipy,stilts can be added, see NOTES.md)
#   N_CPUS=8  MEM_CAP_GB=32  TOOL_THREADS=4  WORKERS=1,4  STEPS=cone:...  PY=<python with lsdb>
#   XMATCH_OUT_DIR=<dir>      where the result catalogs are written (see run_benchmark.py --out-dir)
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Python with lsdb installed: $PY, else the lsdb-py313-linux env of the published runs, else `python` on PATH
if [ -z "${PY:-}" ]; then PY=/sdf/group/rubin/user/ncaplar/conda_envs/lsdb-py313-linux/bin/python; [ -x "$PY" ] || PY=python; fi
N_CPUS="${N_CPUS:-8}"; MEM_CAP_GB="${MEM_CAP_GB:-32}"; TOOL_THREADS="${TOOL_THREADS:-4}"
ALGOS="${ALGOS:-lsdb}"; WORKERS="${WORKERS:-1,4}"
SEED="${SEED:-4,715}"
# radii from results/cone_calibration_cones.json: 10, 1e3 and 1e5 left rows, all inside the seed partition
STEPS="${STEPS:-cone:26.51309,cone:254.488,cone:1563.811}"
# first N_CPUS distinct physical cores of the CPUs this process may use (works on a shared node and inside a SLURM cgroup)
CPUS="$("$PY" - "$N_CPUS" <<'PYEOF'
import os, subprocess, sys
n = int(sys.argv[1]); allowed = sorted(os.sched_getaffinity(0)); core_of = {}
for line in subprocess.run(["lscpu", "-p=CPU,CORE"], capture_output=True, text=True).stdout.splitlines():
    if not line.startswith("#"):
        cpu, core = map(int, line.split(",")[:2]); core_of[cpu] = core
picked, seen = [], set()
for cpu in allowed:
    if core_of.get(cpu) not in seen:
        seen.add(core_of[cpu]); picked.append(cpu)
    if len(picked) == n:
        break
print(",".join(map(str, picked)))
PYEOF
)"
export OMP_NUM_THREADS="$TOOL_THREADS" OPENBLAS_NUM_THREADS="$TOOL_THREADS" PYTHONUNBUFFERED=1
mkdir -p "$HERE/results/logs"
LOG="$HERE/results/logs/smoke_$(date +%Y-%m-%d_%H-%M-%S).log"
echo "python=$PY cpus=$CPUS mem_cap=${MEM_CAP_GB}GB algos=$ALGOS workers=$WORKERS steps=$STEPS log=$LOG"
rm -rf "$HERE/results/smoke"
T0=$(date +%s)
taskset -c "$CPUS" "$PY" "$HERE/run_benchmark.py" --config-name smoke --columns default --algos "$ALGOS" --workers "$WORKERS" \
  --n-runs 1 --steps "$STEPS" --seed "$SEED" --mem-cap-gb "$MEM_CAP_GB" --threads-per-worker 1 --tool-threads "$TOOL_THREADS" \
  --tool-mode native --output write --time-cap-s 600 --overwrite "$@" 2>&1 \
  | grep --line-buffered -vE "FutureWarning|warnings.warn|dashboard link|DeprecationWarning" | tee "$LOG"
rc=${PIPESTATUS[0]}
echo "##### run_benchmark.py exit $rc after $(( $(date +%s) - T0 )) s"
# one line per record: tool, step, left rows, matched rows, crossmatch time (lsdb: the crossmatch+write call;
# other tools: their match call; the full breakdown is in the JSON)
"$PY" - "$HERE/results/smoke" <<'PYEOF'
import json, sys
from pathlib import Path
root = Path(sys.argv[1]); recs = sorted(root.glob("*/*_run*.json"))
if not recs:
    print("no records written under", root); sys.exit(1)
print(f"{'tool':<10} {'step':<16} {'left_rows':>10} {'matched':>10} {'xmatch_s':>9}  ok")
bad = 0
for f in recs:
    d = json.loads(f.read_text())
    tool = d["tool"] + (f"_w{d['workers']}" if d["tool"] == "lsdb" else "")
    if d["tool"] == "lsdb":
        m = d.get("lsdb_crossmatch") or {}; ok = "time" in m
    else:
        ms = d.get("measurements", {}); ok = bool(d.get("ok"))
        key = next((k for k in ms if k.endswith("xmatch") or k.endswith("crossmatch") or k.endswith("match")), None)
        m = ms.get(key) or {}
    bad += not ok
    print(f"{tool:<10} {d['step']:<16} {d.get('left_rows', 0):>10} {str(m.get('result_length', '')):>10} {m.get('time', float('nan')):>9.2f}  {ok}")
print(f"{len(recs)} records, {bad} failed")
sys.exit(1 if bad else 0)
PYEOF
rc2=$?
[ "$rc" -eq 0 ] && [ "$rc2" -eq 0 ] && echo "##### SMOKE OK" || { echo "##### SMOKE FAILED (see $LOG)"; exit 1; }
