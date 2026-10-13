# 01: ZTF x Gaia crossmatch ladder (LSDB)

**What it does.** `run_benchmark.py` crossmatches ZTF DR23 objects with Gaia DR3 using LSDB. For each ZTF object it
finds the nearest Gaia source within 10 arcsec. It does this on a "ladder" of sky cones around one point. The cone
radii are chosen so that the cone holds 1, 3, 10, 30, ... up to 1e9 ZTF rows.

For each step of the ladder the benchmark:

1. reads every HATS partition that the cone touches;
2. runs `left.crossmatch(right).write_catalog(path)`, which matches and writes the result catalog to disk;
3. reads the row count back from the written catalog, then deletes it.

The timed part is step 2: read + crossmatch + write. Each (step, worker count, repeat) writes one JSON record to
`results/<config>/`.

A step reads whole partitions, so its cost depends on the partitions the cone touches, not only on the rows inside
the cone. For example, the 1e7 step reads 39M ZTF rows to match 1e7 of them.

## Catalogs

Both are read in place from `/sdf/data/rubin/cosmicdata/lsdb/` (readable by everyone on SDF).

| Side | Catalog | Rows | Partitions | Columns |
|---|---|---:|---:|---|
| left | `ztf_dr23/objects` | 7.57e9 | 1791 (Orders 1-5, 1.3-12.6M rows each) | the 6 default columns: oid ra dec filtercode medianmag medmagerr |
| right | `gaia_dr3` collection, with its 10 arcsec margin | 1.81e9 | 2016 (Orders 2-6) | 27 columns, listed in `run_benchmark.py` |

The "RA/Dec only" configuration reads only `ra` and `dec` on both sides.

Other catalogs can be used with `--ztf-path` / `--gaia-path` (or `XMATCH_ZTF_PATH` / `XMATCH_GAIA_PATH`).

## Quick start

1. **Python.** See the top-level README. Only LSDB and its dependencies are needed.
2. **Output directory.** Result catalogs go to `--out-dir`, or `XMATCH_OUT_DIR` if set. The default is
   `/sdf/data/rubin/user/$USER/xmatch-bench-out` if that area exists, otherwise `$SCRATCH/xmatch-bench-out`. Each
   result is deleted after its repeat. The 1e9 step writes ~30 GB per repeat. Dask spills to `/lscratch/$USER` when
   the node has a local disk.
3. **Smoke test.** `./run_smoke.sh` runs LSDB with 1 and 4 workers on three small steps (10, 1e3 and 1e5 ZTF rows),
   one repeat each, on 8 cores with a 32 GB memory cap. It checks the Python environment, catalog access and the
   output directory. It takes about 3 minutes (155 s on sdfiana035, 2026-10-13). Records go to `results/smoke/`.
4. **Full ladder on a shared node.** Uses 32 physical cores and a 128 GB memory cap:

   ```bash
   nohup ./run_all_cones.sh > results/logs/run_all_cones_$(date +%F_%H-%M-%S).log 2>&1 &
   ```

   This runs LSDB with 1, 4 and 16 workers on all 19 steps (1 to 1e9 rows), then LSDB with 16 workers on RA/Dec
   only. 3 repeats each. From the recorded timings, expect about 4-5 hours; most of it is the 1-worker series at
   the top steps. It can be restarted: finished repeats are skipped. Use `STEPS=cone:<radius>,...` (radii in
   `results/cone_calibration_cones.json`) or `N_RUNS=1` for a shorter run.
5. **Full catalog on a dedicated SLURM node.**

   ```bash
   sbatch --account=<account> --partition=<partition> --output="$PWD/results/logs/slurm_default_%j.log" run_slurm.sh
   ```

   This runs the 512, 1024 and 1791 nearest ZTF partitions (1791 = the whole catalog) with 16 workers x 16 GB and
   the default columns. The script's `#SBATCH` defaults are milano and `rubin:default`; the command-line flags
   override them.

## How it is run

- **One thread per worker.** `Client(n_workers=w, threads_per_worker=1)`, and pyarrow's decode pool in each worker is
  set to 1 thread. So "w workers" means w cores.
- **Pinned cores and a memory cap.** The run is pinned with `taskset` to distinct physical cores. The Dask worker
  memory limit is the cap divided by the number of workers (128 GB / 16 = 8 GB per worker on the shared node).
- **Warm-up, not timed.** Before the timed repeats, a fresh Dask client imports the stack on every worker and runs
  one small crossmatch. Every file of the step is also read once, so the page cache is warm.
- **Statistic.** The tables give the median of 3 repeats.

## Results (sdfiana033, 2026-09-17/18, 32 cores, 128 GB cap)

Median wall time in seconds. Default columns unless noted.

| ZTF rows in cone | Radius | ZTF partitions read | ZTF rows read | Matched rows | 1 worker | 4 workers | 16 workers | 16 workers, RA/Dec only |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 8.4" | 1 | 3.0M | 0 | 1.41 | 1.01 | 0.95 | 0.69 |
| 100 | 98.7" | 1 | 3.0M | 7 | 1.41 | 1.10 | 1.07 | 0.90 |
| 10,000 | 587.3" | 1 | 3.0M | 1,358 | 1.44 | 1.06 | 1.07 | 0.71 |
| 100,000 | 1563.8" | 1 | 3.0M | 14,065 | 1.71 | 1.34 | 1.22 | 0.80 |
| 1,000,000 | 1.23 deg | 1 | 3.0M | 140,508 | 2.80 | 2.49 | 2.43 | 1.60 |
| 3,000,001 | 2.07 deg | 6 | 25.1M | 417,995 | 11.2 | 5.29 | 5.03 | 2.78 |
| 10,000,004 | 3.86 deg | 8 | 38.9M | 1,373,233 | 24.1 | 7.62 | 5.77 | 3.04 |
| 30,000,015 | 6.65 deg | 17 | 65.7M | 4,110,321 | 58.0 | 20.8 | 12.6 | 6.90 |
| 100,000,015 | 12.26 deg | 42 | 154.2M | 14,070,812 | 190 | 55.2 | 29.4 | 15.9 |
| 299,999,972 | 21.04 deg | 113 | 385.5M | 45,528,252 | 573 | 154 | 56.5 | 27.6 |
| 1,000,000,093 | 38.43 deg | 341 | 1149.8M | 198,955,959 | 2362 | 634 | 209 | 94.2 |

The other steps (3, 10, 30, ... rows) fall on the same curves; all steps are in `NOTES.md`.

What the numbers show:

- **Up to 1e6 rows, time is almost constant.** The cone stays inside one 3M-row partition, so LSDB runs one task.
  The ~1-1.4 s is reading that partition plus Dask graph and scheduling overhead.
- **Above 1e6 rows, time follows the number of partitions.** With 16 workers, tasks run in rounds of 16: 6 and 8
  partitions take one round (5.0, 5.8 s), 17 take two (12.6 s), 42 three (29.4 s), 113 eight (56.5 s) and 341 take
  22 rounds (209 s). With 1 worker the tasks run one after another.
- **At 1e9 rows** (341 ZTF and 47 Gaia partitions, 1.15e9 rows read): 16 workers take 209 s (repeats 217, 209,
  202 s), or 94 s with RA/Dec only. Going from 1 to 4 workers is 3.7x faster; from 4 to 16 workers, 3.0x. The
  16-worker run reads 5.5M ZTF rows per second (12M with RA/Dec only).
- **From 3e8 to 1e9 rows, time grows faster than the rows read** (3.7x for 3.0x more rows read). The 38 deg cone
  reaches Galactic latitude 17 deg, where Gaia is denser: matched rows grew 4.4x, and the Gaia partitions read grew
  from 14 to 47.
- **No Dask worker restarts at any step** of this ladder, with 16 workers x 8 GB.

## Full catalog (earlier run, 2026-09-15/16)

An earlier ladder crossmatched all 1791 ZTF partitions (7.57e9 rows, 3.85e9 matched rows, 51% of ZTF):

- **Shared node (sdfiana030), RA/Dec only, 16 workers x 8 GB:** 400 s.
- **Shared node, default columns, 16 workers x 8 GB:** workers ran out of memory and were restarted, so the
  time is not valid.
- **Dedicated milano node, default columns, 16 workers x 16 GB:** 854 s and 817 s (2 repeats), no restarts. Only 24
  of the requested 32 cores were usable: SLURM keeps 8 cores on milano for the system. At a step that ran on both
  nodes (512 partitions), the milano node was 1.34x slower than sdfiana030; the cause was not found.

## Worker memory

In the Galactic plane almost every ZTF row has a Gaia match (~98%, compared with 15-20% at high latitude). Task
memory follows the number of matched rows, not the number of input rows. The largest task in the full crossmatch
(ZTF pixel 4/879, 10.0M rows, 9.8M matched) peaks at 5.7 GiB. After a task, a worker still holds 1.2-1.4 GiB, so an
8 GB worker can reach Dask's 95% restart threshold (7.08 GiB). 12 GB per worker avoids this; 16 GB is safe.
Details and per-task measurements are in `HANDOFF.md`.

## Not yet run

The 3e9-row step and the full catalog on the cone ladder are planned but not run. They write 100-400 GB per repeat
and need 16 GB per worker. `HANDOFF.md` has the plan, time estimates and commands.

## Files

| File | Purpose |
|---|---|
| `run_benchmark.py` | Benchmark driver. One JSON per (step, worker count, repeat). Can be restarted. |
| `run_smoke.sh` | Smoke test (three small steps). |
| `run_all_cones.sh` | The full cone ladder on a shared node. |
| `run_slurm.sh` | The largest steps on a dedicated SLURM node. |
| `run_benchmark.sh` | One ad hoc run on a shared node (`taskset`, `nohup`, log). |
| `results/cone_calibration_cones.json` | Cone radii for 1, 3, 10, ... 1e9 ZTF rows around the centre. |
| `results/cones/`, `results/cones_radec/` | Records of the results above. |
| `results/<config>/settings.json` | Software versions, host, CPU set, memory cap, catalogs and columns of each run. |
| `make_plot_notebook.py`, `plot_benchmark*.ipynb`, `*.png` | Figures. These compare LSDB with astropy, smatch, scipy and STILTS for the LSDB documentation. |
| `NOTES.md` | Full lab notes: the tool comparison, the two earlier ladder designs, all result tables, differences from the 2025 benchmark, and the commands that made each figure. |
| `HANDOFF.md` | Plan for the 3e9 and full-catalog steps, and the worker-memory measurements. |
| `memory_tests/` | One-worker memory scripts and their outputs (summarised in `HANDOFF.md`). |

## Commands

```bash
# smoke test
./run_smoke.sh
# full ladder, restartable; STEPS=... and N_RUNS=... select a subset
nohup ./run_all_cones.sh > results/logs/run_all_cones_$(date +%F_%H-%M-%S).log 2>&1 &
# only the 1e9 step
STEPS=cone:138355.8 nohup ./run_all_cones.sh > results/logs/run_all_cones_1e9_$(date +%F_%H-%M-%S).log 2>&1 &
# largest steps on a dedicated node
sbatch --account=<account> --partition=<partition> --output="$PWD/results/logs/slurm_default_%j.log" run_slurm.sh
# one ad hoc run
./run_benchmark.sh default --columns default --algos lsdb --workers 16 --steps pixels:1791 --n-runs 1
# recalibrate the cone radii (exact row counts around the centre)
python run_benchmark.py --config-name cones --seed 4,715 --calibrate-cones 1,3,10,30,100,300,1e3,3e3,1e4,3e4,1e5,3e5,1e6,3e6,1e7,3e7,1e8
```
