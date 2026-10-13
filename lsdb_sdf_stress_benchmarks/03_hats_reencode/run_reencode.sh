#!/bin/bash
# Re-encode a HATS COLLECTION with ../../lsdb/reencode_hats_collection.py (every parquet file rewritten: zstd 15,
# 100 MB row groups, 128 KB pages, statistics; _metadata and properties rebuilt). Bare catalogs are not accepted by
# that script: wrap them first with make_collection_subset.py --n-partitions 0.
# usage: ./run_reencode.sh <collection> <output> [workers=4]     env: PY, CPU_OFFSET
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; source "$HERE/../common.sh"
IN="${1:?source collection}"; OUT="${2:?output directory}"; WORKERS="${3:-4}"
[ -f "$IN/collection.properties" ] || { echo "$IN is not a HATS collection (no collection.properties); wrap it with make_collection_subset.py first"; exit 2; }
CPUS="$(pick_cpus "$WORKERS" "${CPU_OFFSET:-0}")"
echo "reencode $IN -> $OUT workers=$WORKERS cpus=$CPUS"
T0=$(date +%s)
taskset -c "$CPUS" "$PY" "$HERE/../../lsdb/reencode_hats_collection.py" "$IN" "$OUT" -w "$WORKERS" 2>&1 | quiet
rc=${PIPESTATUS[0]}
echo "{\"result\": \"reencode\", \"in\": \"$IN\", \"out\": \"$OUT\", \"workers\": $WORKERS, \"wall_s\": $(( $(date +%s) - T0 )), \"rc\": $rc}"
exit "$rc"
