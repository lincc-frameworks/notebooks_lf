# 03: HATS collection re-encode

`../../lsdb/reencode_hats_collection.py` (Konstantin Malanchev, 2026-09) copies a HATS collection to a new path and
rewrites every Parquet file with fixed storage settings (zstd level 15, 100 MB row groups capped at 100k rows,
128 KB pages, dictionary encoding, statistics), then regenerates `_metadata`, `_common_metadata` and the properties
through hats and validates the result. Rows and columns are unchanged. It is the closest thing to a full
`hats-import` that runs without the DP2 import pipeline: every byte of the catalog is read and written again.

- **Only collections are accepted** (it reads `collection.properties`). `make_collection_subset.py` turns any
  catalog or collection into a collection of its first N partitions with lsdb (`--n-partitions 0` for all of
  it); that write is itself an LSDB write workload (8 Gaia partitions + margin: 165 s on 4 workers).
- **Baseline:** 8-partition Gaia DR3 collection with its 10 arcsec margin, 47 files, 3.0 GB: 116 s on 4 workers,
  2.5 GB written (2026-10-13). Targets at scale: Gaia DR3 (2016 partitions, 713 GB) and DP2 `object_collection`
  (4.04 TB, whose `object_lc` `_metadata` hit the 4 GB thrift limit during the DP2 import).
- **Smoke:** `./run_smoke.sh` (subset of 4 Gaia partitions, re-encode, `verify_copy.py` checks partition list,
  row count, columns, margin and the full content of the first partition; ~3 min; `KEEP=1` keeps the outputs).
- **Full run:** `./run_reencode.sh /sdf/data/rubin/cosmicdata/lsdb/gaia_dr3 $BENCH_SCRATCH/gaia_reencoded 16`
  (needs ~700 GB of free space; the script also has `--resume` and `--skip_primary_metadata`, see its `--help`).
