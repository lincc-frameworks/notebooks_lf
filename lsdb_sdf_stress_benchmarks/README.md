# LSDB / HATS stress workloads for SLAC SDF

Five workloads that exercise the HATS/LSDB stack on real catalogs under `/sdf/data/rubin`, each with a short smoke
test and a documented full-scale run. They come from LSDB work on SDF in 2026 and keep the measured numbers of those
runs, so a rerun on new hardware or storage has a baseline. Everything reads the public HATS mirrors in place;
nothing has to be copied.

| # | workload | what it stresses | full-scale run | recorded baseline |
|---|---|---|---|---|
| 01 | `01_crossmatch_ladder` ZTF DR23 objects x Gaia DR3, LSDB and four other tools, cone ladder 1 to 1e9 rows | parallel Parquet reads of two catalogs, Dask worker memory, result writes | 1e9 ZTF rows: 1.15e9 rows read, 199M matches; full catalog 7.57e9 rows (parked; 300-400 GB of output per repeat) | 1e9 rows, LSDB 16 workers: 209 s on sdfiana033; full catalog RA/Dec only: ~400 s; 1791-partition step on a milano node: 854 s |
| 02 | `02_dp2_lightcurve_scan` per-object reductions over the nested forced-source lightcurves of DP2 | nested Parquet reads (list columns), nested-pandas reductions, task scheduling over 8840 partitions | 8840 partitions, 786M objects, 44.7e9 forced-source epochs, ~3.6 TB | 100 partitions, 540M epochs: 69 s on 16 workers; 4 partitions: 33 s on 4 workers |
| 03 | `03_hats_reencode` read and rewrite every Parquet file of a HATS collection through hats, regenerate `_metadata` | write bandwidth, Parquet encode CPU, metadata write (the 4 GB thrift limit of `_metadata` on DP2 `object_lc`) | DP2 `object_collection` (4.04 TB) or Gaia DR3 (713 GB) | 8 Gaia partitions, 3.0 GB in: 116 s on 4 workers, 2.5 GB out |
| 04 | `04_lsdb_read_path` same 4-column scan through lsdb, nested-pandas and pyarrow, bytes and syscalls from `/proc/self/io` | read bandwidth and per-partition overhead of the lsdb read path, warm vs cold cache | Gaia DR3, 2016 partitions, 713 GB (`--n-partitions 0`) | full Gaia, 1 column: lsdb 320 s vs pyarrow 88 s; 192 partitions, 4 columns: 4.0 s vs 2.7 s |
| 05 | `05_dp2_gaia_crossmatch` DP2 object collection x Gaia DR3 with margins | Dask graph size (9139 output partitions), scheduler, many-worker reads | 8840 DP2 partitions, 48 workers x 4 GB | July 2026: ~250 s total, 88 s of it client-side graph materialization before the first task |

## Quick start

1. **Environment.** Python 3.13 with `pip install -r requirements.txt` (the versions of the published runs; the
   conda env `lsdb-py313-linux` on SDF has them). smatch and STILTS are only used by workload 01 and are optional
   there (see its README).
2. **Data.** The HATS mirrors under `/sdf/data/rubin/cosmicdata/lsdb/` (Gaia DR3, ZTF DR23) are world-readable;
   the DP2 collections under `/sdf/data/rubin/shared/lsdb_commissioning/hats/dp2_rc/` need Rubin data rights
   (`rubin_users` group).
3. **Scratch.** Result catalogs and Dask spill go to `$BENCH_SCRATCH`, by default `/lscratch/$USER/lsdb-bench` on
   nodes with a node-local disk (sdfiana*, milano) and `$SCRATCH/lsdb-bench` otherwise. Workload 01 keeps its own
   `XMATCH_OUT_DIR`; see its README.
4. **Smoke tests.** `./run_all_smoke.sh` runs all five in sequence (about 12 min on a shared sdfiana node, pinned to
   4-8 physical cores each) and prints a PASS/FAIL table. Each `<dir>/run_smoke.sh` also runs alone, and every
   script prints one JSON line with its settings and timings.
5. **Full runs.** Each directory's README gives the full-scale command, its resource needs and what broke in the
   past. Use `taskset` and Dask memory limits on shared interactive nodes (SDF asks for under ~30 cores there) or an
   exclusive SLURM node (`01_crossmatch_ladder/run_slurm.sh` is the template: `--exclusive --mem=0`, cores picked
   from the job's allowed set).

## What to expect to break

- **Worker memory.** LSDB crossmatch tasks on dense sky (Galactic plane) reach ~6 GiB RSS; 16 workers x 8 GB
  restarted 40 times at the 1e9-row step. Give workers 16 GB or use fewer of them.
- **Graph size.** Above ~100k partitions the Dask scheduler and dashboard stall; at 9139 output partitions the
  client already spends ~90 s building the graph before the first task.
- **Metadata size.** Rewriting `_metadata` for DP2 `object_lc` hit the 4 GB thrift serialization limit during the
  DP2 import; the re-encode workload reproduces that path.
- **Cold Python env.** The first process importing lsdb from a conda env on `/sdf/group` after a cache eviction
  takes minutes (3.5 min for the first Dask client start on 2026-10-13); later processes take seconds.
- **Weka cache.** `posix_fadvise(DONTNEED)` does not evict on `/sdf/data`; a cold read needs partitions no process
  on the node has touched (workload 04 `--offset`).
