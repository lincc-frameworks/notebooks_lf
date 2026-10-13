# 03: HATS collection re-encode

**What it does.** `run_reencode.sh` runs `../../lsdb/reencode_hats_collection.py` (Konstantin Malanchev, 2026-09).
That script lives elsewhere in this repository, so you need the whole `notebooks_lf` checkout, not only this folder.
It copies a HATS collection to a new path and rewrites every Parquet file with fixed storage settings: zstd level 15,
100 MB row groups of at most 100k rows, 128 KB pages, dictionary encoding and statistics. It then rebuilds
`_metadata`, `_common_metadata` and the properties files with hats, and checks the result. Rows and columns do not
change. Every byte of the catalog is read and written again, so this is the closest we have to a full `hats-import`
without the DP2 import pipeline.

**Input must be a collection.** The script reads `collection.properties` and rejects a bare catalog.
`make_collection_subset.py` turns any catalog or collection into a collection of its first N partitions, using lsdb
(`--n-partitions 0` keeps all of it). That step is itself an LSDB write workload: 8 Gaia partitions plus margin take
165 s on 4 workers.

**Smoke test.** `./run_smoke.sh`, about 3 minutes:

1. make a collection of 4 Gaia partitions;
2. re-encode it;
3. `verify_copy.py` checks the partition list, the row count, the columns, the margin, and the full content of the
   first partition.

Set `KEEP=1` to keep the outputs.

**Full run.**

```bash
./run_reencode.sh /sdf/data/rubin/cosmicdata/lsdb/gaia_dr3 $BENCH_SCRATCH/gaia_reencoded 16
```

This needs ~700 GB of free space. The underlying script also has `--resume` and `--skip_primary_metadata`; see
its `--help`.

**Recorded results.**

- 8-partition Gaia DR3 collection with its 10 arcsec margin, 47 files, 3.0 GB in: 116 s on 4 workers, 2.5 GB
  written (2026-10-13).
- Full-scale targets: Gaia DR3 (2016 partitions, 713 GB) and DP2 `object_collection` (4.04 TB). During the DP2
  import, the `_metadata` of `object_lc` hit the 4 GB Thrift serialization limit. A full run on DP2 tests that path.
