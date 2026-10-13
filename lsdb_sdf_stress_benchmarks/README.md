# LSDB / HATS stress workloads for SLAC SDF

Four workloads that load the HATS/LSDB software stack with real catalogs on SDF. Each one has a short smoke test and
a full-scale run. Each README records the timings measured in 2026.

All catalogs are read in place from `/sdf/data/rubin`. 

## The four workloads

| # | Directory | What it does | What it stresses |
|---|---|---|---|
| 01 | `01_crossmatch_ladder` | Crossmatches ZTF DR23 objects with Gaia DR3 with LSDB, from 1 to 1e9 ZTF rows. | Parallel Parquet reads of two catalogs, Dask worker memory, result writes |
| 02 | `02_dp2_lightcurve_scan` | Computes per-object statistics over the DP2 forced-source lightcurves. | Reads of nested Parquet (list columns), scheduling of 8840 tasks |
| 03 | `03_hats_reencode` | Reads and rewrites every Parquet file of a HATS collection, then rebuilds `_metadata`. | Write bandwidth, Parquet encoding CPU, metadata writes |
| 04 | `04_lsdb_read_path` | Reads the same 4 columns of Gaia DR3 with lsdb, nested-pandas and pyarrow, and compares them. | Read bandwidth, per-partition overhead of lsdb, warm and cold cache |

## Sizes and recorded results

| # | Full-scale run | Recorded results |
|---|---|---|
| 01 | 1e9 ZTF rows: 1.15e9 rows read, 199M matches. The full catalog is 7.57e9 rows. | 1e9 rows, 16 LSDB workers: 209 s (sdfiana033). Full catalog, RA/Dec columns only: ~400 s. Full catalog, default columns, dedicated milano node: 854 s. |
| 02 | All 8840 partitions: 786M objects, 44.7e9 forced-source measurements, ~3.6 TB. | 100 partitions (540M measurements): 69 s on 16 workers. 4 partitions: 33 s on 4 workers. |
| 03 | DP2 `object_collection` (4.04 TB) or Gaia DR3 (713 GB). | 8 Gaia partitions, 3.0 GB in, 2.5 GB out: 116 s on 4 workers. |
| 04 | All of Gaia DR3: 2016 partitions, 713 GB. | 192 partitions, 4 columns: lsdb 4.0 s, pyarrow 2.7 s, same bytes read within 1%. |

## Quick start

1. **Python.** Create your own Python 3.13 environment and run `pip install -r requirements.txt`. These are the
   package versions of the recorded runs. SDF home directories have a small quota, so put the environment on a
   data or group disk. The scripts use `python` from your `PATH`; to use a different interpreter, set
   `PY=/path/to/env/bin/python`. Clone the whole `notebooks_lf` repository, not only this folder: workload 03 uses
   a script from `../lsdb/`.
2. **Data access.** The Gaia DR3 and ZTF DR23 catalogs under `/sdf/data/rubin/cosmicdata/lsdb/` are readable by
   everyone. The DP2 catalogs under `/sdf/data/rubin/shared/lsdb_commissioning/hats/dp2_rc/` need Rubin data rights.
3. **Scratch space.** Output catalogs and Dask spill files go to `$BENCH_SCRATCH`. The default is
   `/lscratch/$USER/lsdb-bench` on nodes with a local disk (sdfiana*, milano), and `$SCRATCH/lsdb-bench` on other
   nodes. Workload 01 uses its own `XMATCH_OUT_DIR` instead (see its README).
4. **Smoke tests.** `./run_all_smoke.sh` runs all four, one after another, and prints a PASS/FAIL table. It takes
   about 10 minutes on a shared sdfiana node, with each test pinned to 4-8 physical cores. Each `<dir>/run_smoke.sh`
   can also run on its own. Every script prints one JSON line with its settings and timings.
5. **Full runs.** Each directory's README gives the full-scale command, the resources it needs, and what failed in
   the past. On a shared interactive node, pin the run with `taskset` and set Dask memory limits; SDF asks for fewer
   than ~30 cores there. For more, use an exclusive SLURM node. `01_crossmatch_ladder/run_slurm.sh` is a template
   for this: it uses `--exclusive --mem=0` and picks cores from the set the job is allowed to use.

## Expected problems

- **Worker memory (01).** One LSDB crossmatch task in the dense Galactic plane uses up to ~6 GiB of resident memory.
  Give each worker at least 16 GB.
- **Metadata size (03).** During the DP2 import, rewriting `_metadata` for DP2 `object_lc` hit the 4 GB size limit
  of Parquet's metadata serialization (Thrift). Workload 03 runs the same code path; run it on DP2
  `object_collection` to reach that limit.
- **Cold reads (04).** On `/sdf/data` (Weka), `posix_fadvise(DONTNEED)` does not remove files from the cache. A cold
  read needs partitions that no process on the node has read yet (`readbench.py --offset`).
