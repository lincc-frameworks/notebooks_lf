#!/bin/bash
# Extend the cone ladder by extra step(s) (default: the 3e8-row cone, 21.04 deg, 113 ZTF partitions) for every series:
# LSDB 1/4/16 workers, astropy, scipy, STILTS (run_all_cones.sh leg 1), LSDB 16 workers RA/Dec only (leg 2), then smatch with
# its Gaia argument in random order (run_smatch_shuffled.sh). The plain smatch call is left out: it has been time-capped since
# 3e7 rows. Waits for a running sweep to finish so timings never overlap. Resumable like the scripts it calls.
# usage: nohup ./run_extra_steps.sh > results/logs/run_extra_steps_<ts>.log 2>&1 &
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STEPS="${STEPS:-cone:75730.94}"
while pgrep -f 'run_all_cone[s]\.sh|run_smatch_shuffle[d]\.sh' >/dev/null; do sleep 30; done
echo "##### $(date '+%F %T') extra steps: $STEPS"
STEPS="$STEPS" ALGOS="${ALGOS:-lsdb,astropy,scipy,stilts}" "$HERE/run_all_cones.sh"
STEPS="$STEPS" "$HERE/run_smatch_shuffled.sh"
echo "##### $(date '+%F %T') EXTRA STEPS DONE"
