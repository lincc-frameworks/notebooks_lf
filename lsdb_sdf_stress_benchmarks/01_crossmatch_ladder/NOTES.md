# Lab notes: crossmatch benchmark, 2026 rerun (SLAC SDF)

> These are the full working notes, kept for reference. They also cover the comparison of LSDB with astropy,
> smatch, scipy and STILTS made for the LSDB documentation figure. For the LSDB benchmark itself, read
> `README.md` first.

Fresh timings for the "Crossmatch Performance by Tool" figure (LSDB docs, `tutorials/performance.rst`;
LSDB talks). Same design as the Feb 2025 benchmark in `../../lsdb_crossmatch_benchmarking/`
(Sean McGuire, PSC Bridges-2), re-run with current software on a shared SDF interactive node.

Three sweeps live here. The first (2026-09-15, `results/default`, `results/radec`, `results/default_slurm`) kept the
2025 design: k-nearest partitions, results counted in memory, three tools. The second (2026-09-16,
`results/uniform`, `results/uniform_radec`, `run_all_uniform.sh`) used uniform partitions, wrote every result to disk and
added two tools. The current design (2026-09-17, `results/cones`, `results/cones_radec`, `run_all_cones.sh`) replaces the
partition ladder by cones of growing radius around one sky position and puts the LEFT row count on the x axis; see the
next section. Sections further down describe what is common to all of them and note where the earlier sweeps differed.

## Quick start (new user on SDF; e.g. as a storage / node stress test)

What runs: `run_benchmark.py` crossmatches ZTF DR23 objects against Gaia DR3 (10 arcsec nearest neighbour) with LSDB and,
optionally, astropy, smatch, scipy and STILTS, over a ladder of sky cones whose ZTF row count grows from 1 to 1e9.
Per step it reads every HATS partition the cone touches (the "rows read" column of the results table, 1.15e9 rows at
the 1e9 step), runs the match on a pinned CPU set under a memory cap, writes the result catalog to disk, reads the row
count back and deletes it. One JSON per (tool, step, repeat) lands in `results/<config>/`; `make_plot_notebook.py`
turns them into the figure.

1. **Environment.** Python 3.13 with the versions in `requirements.txt` (`pip install -r requirements.txt`; the
   published runs used the conda env `lsdb-py313-linux`). `smatch` must be built from Erin Sheldon's git, not PyPI.
   STILTS needs a JRE (`/usr/bin/java` on the SDF nodes) and the jar: download
   https://www.star.bris.ac.uk/~mbt/stilts/stilts.jar into this directory or set `STILTS_JAR`. smatch and STILTS are
   optional: `ALGOS=lsdb,astropy,scipy ./run_smoke.sh`.
2. **Data.** The HATS mirrors under `/sdf/data/rubin/cosmicdata/lsdb/` are world-readable on SDF; nothing to copy.
   Other catalogs: `--ztf-path` / `--gaia-path` (or `XMATCH_ZTF_PATH` / `XMATCH_GAIA_PATH`).
3. **Scratch.** Result catalogs go to `--out-dir` (default `/sdf/data/rubin/user/$USER/xmatch-bench-out` when that area
   exists, else `$SCRATCH/xmatch-bench-out`; `XMATCH_OUT_DIR` overrides) and are deleted after every repeat; the 1e9
   step writes ~30 GB per repeat, the full catalog 300-400 GB. Dask spills to `/lscratch/$USER` when the node has one.
4. **Smoke test.** `./run_smoke.sh`: every tool, 10 / 1e3 / 1e5 ZTF rows, one repeat, 8 cores, 32 GB cap, results in
   `results/smoke/` (about 5 min on a shared sdfiana node, 9 min when the conda env is not yet in the page cache). Checks the environment, data access and the output path.
5. **Ladders.** Shared node, 32 cores / 128 GB: `nohup ./run_all_cones.sh > results/logs/run_all_cones_$(date +%F_%H-%M-%S).log 2>&1 &`
   (resumable; `STEPS=... ALGOS=... N_RUNS=...` from the environment). Recorded node time: 51 min for the 15 steps to 1e7,
   84 min for 3e7 + 1e8, 2 h for 3e8, 3 h for the 1e9 step (LSDB only; the other tools would need ~180 GiB there).
   Dedicated node: `sbatch --account=<acct> --partition=<part> run_slurm.sh` (16 LSDB workers x 16 GB over the
   512 / 1024 / 1791-partition steps; the full-catalog step took 854 s on a milano node). Details under "Re-running".

## Current design (2026-09-17): cone ladder, x = rows of the left catalog

- **Why.** With crossmatched rows on the x axis and a ladder that switches from cones inside one partition to k whole
  partitions, every LSDB curve showed a kink between 1e5 and 1e6 rows: the cone steps all read the same single partition
  (flat, ~1-2 s), the first pixel step matched the whole partition (~5.5 s for any worker count), and from there the
  4- and 16-worker curves stayed flat until k exceeded the worker count (time = ceil(k / workers) x one task). That is
  the task model, not the tools, and it hides the comparison. The new ladder has one kind of step only.
- **Ladder** = cones around one centre (the centre of Order 4 / Npix 715: RA 216.82, Dec +57.40, |b| = 55), radii chosen
  so the number of ZTF rows inside is 1, 3, 10, 30, ... 3e7, 1e8, 3e8, 1e9 (half decades; the 3e8 step was added 16:56,
  the 1e9 step on 2026-09-18 for the LSDB series only). `run_benchmark.py --calibrate-cones
  1,3,10,...` reads ra/dec of every ZTF file a wide cone touches once (67 files, 1.6e8 rows, 73 s), sorts the separations
  and puts each radius halfway between the N-th and (N+1)-th nearest row, so the counts are exact
  (`results/cone_calibration_cones.json`, also the `--steps` string). 1e6 rows still fit in the seed partition (1.23 deg);
  3e6 = 2.07 deg / 6 partitions, 1e7 = 3.86 deg / 8, 3e7 = 6.65 deg / 17, 1e8 = 12.26 deg / 42, 3e8 = 21.04 deg / 113,
  1e9 = 38.43 deg / 341 (13% of the 7.57e9-row catalog; the cone reaches down to |b| = 17 deg). Partitions of any size
  take part (no `--rows-band`): a cone touching an Order 3 partition reads its 10.7M rows and keeps the few inside.
- **x axis = left rows** (`make_plot_notebook.py --x left`). Every step record carries `left_rows`, counted untimed with
  the same `cone_filter` the sequential tools apply. The crossmatched count is still checked for agreement across tools.
  Around this centre ~14% of ZTF rows have a Gaia neighbour within 10 arcsec, so 1e8 left rows ~ 1.4e7 matched.
- **Thread cap for the non-LSDB tools** (`--tool-threads 16` = the largest LSDB worker count): scipy's k-d tree query
  (`workers=16`), pyarrow decode in the child process, and STILTS `runner=parallel16`. Note that STILTS `runner=parallel`
  on its own means min(6, cores) threads, so the earlier sweeps ran STILTS on 6 threads while scipy used all 32.
- **Native tool mode** (`--tool-mode native`, the default since 2026-09-17 12:30, Neven: "I want native behavior. It is ok if
  it blows up, it is a limitation of the tool"). Each non-LSDB tool loads the whole ZTF selection (every partition file the
  cone touches, then the cone filter) and the whole Gaia side (partitions + margin) into memory and makes ONE call, exactly
  as a user would write it: astropy `SkyCoord` + `match_coordinates_sky` + radius cut (the 27 Gaia columns are gathered for
  every left row before the cut, as in 2025); `smatch.match(ztf_ra, ztf_dec, r, gaia_ra, gaia_dec, maxmatch=1)` in the
  plain call form, no shuffling (on HATS-sorted input this is smatch's slow path: 8.7 s for 10 rows, 223 s at 3e6 against
  8.9 s streamed and shuffled); scipy `cKDTree` on all Gaia unit vectors, one `query` over all ZTF unit vectors; STILTS
  `tmatch2` with tables in the Java heap (no `-disk`). The joined result is written to parquet once. The plotted time is
  everything the user has to do: pyarrow reads, Gaia/margin merge, cone filter, SkyCoord construction, the match, the
  write; for STILTS only tmatch2 (see below). If a tool exceeds the 128 GB memory cap or the `--time-cap-s 1800` per-run
  time cap it is killed, recorded as oom/timeout, and skipped at every larger step: that is where its curve ends. STILTS's
  Parquet-to-FITS conversion, which is not timed, now runs once per step and is reused across repeats (`stilts_fits/`
  cache under the Dask local dir). The streamed design of 2026-09-16 (`--tool-mode stream`) is kept for reference; its
  records for the cone ladder live in `results/cones_streamed/`.
- **smatch input order** (`--smatch-shuffle`; `run_smatch_shuffled.sh` -> `results/cones_smatch_shuffled/`, plotted with
  `make_plot_notebook.py --smatch-config cones_smatch_shuffled`; Neven, 2026-09-17: "shuffle the data to make it a fair
  comparison"). Root cause of smatch's slow path, read from the installed C source (`site-packages/smatch/smatch.c`,
  `tree.c`, v0.10.2): `smatch.match(ra1, dec1, r, ra2, dec2)` does not index set 1. `domatch` builds a binary search tree
  from the SECOND set (`create_hpix_tree`: one `tree_insert` per point, keyed by its nside-4096 nested pixel; `tree_insert`
  is a plain unbalanced recursive BST, equal keys append to the node's index list), then loops over every set-1 object:
  `hpix_disc_intersect` -> 1-4 disc pixels -> `tree_find` per pixel -> dot products over that node's indices. HATS files are
  sorted by `_healpix_29`, so the nside-4096 keys arrive non-decreasing (99.9998% of the steps in the seed Gaia frame) and
  the tree is a single chain: insert i walks i nodes and every lookup walks to the pixel's rank. Measured on the 1e6 cone
  (Gaia Order-2 partition + margins = 805,591 rows, 554,227 distinct pixels): tree build 8.5 s in file order against 0.7 s
  shuffled; 1e5 ZTF rows 14.8 s against 0.74 s, identical pairs. Hence the 9 s floor, ~65 us per ZTF row (653 s at 1e7)
  and the time-cap kill at 3e7 of the plain call. The fix is one line a smatch user has to know: pass the second argument
  in random order (`perm = rng.permutation(n); match(ztf, r, gaia[perm], ...); i2 = perm[i2]`), timed as part of the
  call. k-d trees (scipy, astropy), STILTS's HEALPix binning and LSDB are insensitive to input order, so nothing else
  changes. The plain-call records stay in `results/cones/`; the figure shows the shuffled series, the results table both.
- **Empty chunks** (stream mode only). A partition whose pixel touches the cone can hold no row inside it (Npix 716 at 2.07
  deg). smatch's C extension returns NULL on an empty input array (`SystemError`), so the streaming loop skips empty chunks.
- **Validation** (`results/test_cones/`, one repeat, 10 rows and 3e6 rows): all tools agree on crossmatched rows
  (0 and 417,995); at 3e6 left rows LSDB 16 w 4.9 s, LSDB 1 w 10.9 s, scipy 5.9 s, astropy 9.0 s, STILTS 11.6 s + 43 s of
  JVM/Parquet I/O.
- **Sweep** (`run_all_cones.sh`, `STEPS=...` to run a subset): every tool and LSDB 1/4/16 workers on every step, 3
  repeats; then RA/Dec-only LSDB 16 workers. Run in two launches on 2026-09-17: the 15 steps up to 1e7 rows, then 3e7 and 1e8
  (`STEPS=cone:23931.84,cone:44133.7`). A third launch 12:27-14:52 re-ran the four non-LSDB tools over all 17 steps in native
  mode; existing per-repeat records are skipped, so a relaunch only fills what is missing. A fourth launch 16:40-16:55
  re-ran smatch with its Gaia argument in random order (previous bullet). A fifth launch 16:56-19:01 (`run_extra_steps.sh`,
  which waits for a running sweep, then runs `run_all_cones.sh` and `run_smatch_shuffled.sh` on `STEPS=cone:75730.94`)
  added the 3e8 step for every series except the plain smatch call, time-capped since 3e7.
- Plot: `python make_plot_notebook.py --config cones --radec-config cones_radec --slurm-config none --x left
  --legend-ncol 2 --legend-pos below --smatch-config cones_smatch_shuffled --stilts-series xmatch
  --png crossmatching-performance-2026-cones.png --notebook plot_benchmark_cones.ipynb`; `--x read` for the rows-read
  figure, `--smatch-plain dotted` for the figure with the plain smatch call added as a dotted line.

## Second design (2026-09-16): uniform partitions, results written to disk, six tools

- **Uniform partitions** (`--rows-band 2.5e6,3.5e6`). ZTF DR23 partitions span 1.3-12.6M rows (Orders 1-5). The
  k-nearest ladder of the first sweep picked up a 10.5M-row Order 3 partition at k=8 next to ~3M-row Order 4 ones; with
  16 single-thread workers the wall time of a step with k <= 16 tasks is the slowest task, so the 4- and 16-worker curves
  showed a step at k=8 and a plateau to k=16. Now only partitions with 2.5-3.5M rows take part: 911 qualify (all Order 4
  but one, 1.4x spread), 2.78e9 ZTF rows in total. The seed is the qualifying partition closest to 3.0M rows inside the
  footprint cuts: Order 4 / Npix 715, RA 216.8, Dec +57.4, |b| = 55; the match rate there is ~14%, so the full band gives
  ~4e8 crossmatched rows (the first sweep's region matched 22%, and the whole catalog 51%).
- **Every tool writes its result to disk** (`--output write`, the default; `--output count` restores the first sweep's
  behaviour). The timed quantity is read + match + write of the crossmatched table to parquet under `--out-dir`
  (`/sdf/data/rubin/user/ncaplar/xmatch-bench-out`, deleted after every repeat unless `--keep-output`).
  - LSDB: `left.crossmatch(right).write_catalog(path)`; the row count is read back from the written catalog.
  - astropy, smatch, scipy: the Gaia side (partitions + margin) is loaded once and indexed once; the ZTF side is
    streamed one partition file at a time (read, cone-filter, match, write the in-radius pairs of that chunk, drop
    the chunk). Peak memory is the Gaia frame + index + one chunk, independent of the left size: at 2.4e7 left rows
    astropy went from 22 GiB (first sweep) to 2.2 GiB, smatch from 7.3 to 1.9 GiB.
  - STILTS: `-disk` (tables backed by temp files) and `tmatch2 ... out=xmatch.parquet`, row count from the parquet
    footer. Its JVM still grows ~0.9 KB of resident memory per input row (21 GiB at 2.4e7 rows); the heap limit is set
    to 80% of the cap so it fails on its own OutOfMemoryError, recorded as out of memory, rather than by the RSS kill.
- **astropy** applies the 10 arcsec mask to the neighbour index *before* gathering the 27 Gaia columns. The first
  sweep's `run_astropy` gathered them for every left row first (`match_coordinates_sky` has no radius), which is what
  put it out of memory at 5e7 rows.
- **smatch** is given the streamed points in random order. Its cost depends on input order alone: for equal-size
  ~3M-row partitions against the same Gaia index, HEALPix-sorted input took 2-15 s, Dec-sorted 45-62 s, shuffled
  1.5-1.6 s, with identical pairs. `smatch.Catalog` indexes Gaia once; the permutation is undone on the returned indices.
- **Two tools added** (`--algos scipy,stilts`): scipy `cKDTree` on unit vectors with the query on all 32 threads
  (astropy's algorithm without the single-threaded query), and STILTS `tmatch2 matcher=sky find=best1 runner=parallel`
  (the TOPCAT engine; its Parquet reader and JVM start are counted as I/O, ~10 s even for the smallest step).
- **Validation** (`results/test_uniform_write/`, `test_uniform_write.png`, one repeat, cone:1150 + pixels:1..8): all
  six series agree exactly on row counts at every step; at 3.2e6 crossmatched rows LSDB 16 w 6.4 s, LSDB 4 w 11.3 s,
  scipy 9.3 s, smatch 23.2 s, astropy 36.5 s, STILTS 83.5 s.
- **Sweep** (`run_all_uniform.sh`): cones 115/375/1150/3700 arcsec and k = 1..32 with 3 repeats and every tool; k = 64,
  128 with 2 repeats and every tool (STILTS ends at 128 or at its heap limit); k = 256 without STILTS; k = 512 LSDB
  4/16 workers, smatch, scipy; k = 911 (the whole band) LSDB 16 workers; RA/Dec-only LSDB 16 workers over the full
  ladder. LSDB 1 worker stops at 256 and 4 workers at 512 (the ~30 min per repeat rule).
- Plot: `python make_plot_notebook.py --config uniform --radec-config uniform_radec --slurm-config none` (the
  generator's defaults still point at the first sweep).

## Files

| file | purpose |
|---|---|
| `run_benchmark.py` | benchmark driver; one JSON per (config, ladder step, repeat); resumable |
| `run_benchmark.sh` | launcher for one invocation on a shared node: `taskset` to N physical cores, `nohup`, log |
| `run_all.sh` | the FIRST sweep (2026-09-15) as a sequence of invocations |
| `run_all_uniform.sh` | the SECOND sweep (2026-09-16): uniform partitions, results written, six tools |
| `run_all_cones.sh` | the CURRENT sweep (2026-09-17): calibrated cones around one centre, x = left rows, tools capped at 16 threads |
| `run_extra_steps.sh` | extends the cone ladder by calibrated step(s) (default the 3e8 cone) for every series but the plain smatch call; waits for a running sweep first |
| `run_slurm.sh` | sbatch script: the largest default-column LSDB steps on a dedicated milano node, 256 GB cap (`results/default_slurm/`) |
| `make_plot_notebook.py` | generates `plot_benchmark.ipynb` (kept as a script so the source is diff-able); `--config/--radec-config/--slurm-config/--png/--notebook/--title` select other results, `--x left` puts left-catalog rows on the x axis, `--aspect equal` gives both log axes the same decade length plus a slope-1 guide |
| `plot_benchmark.ipynb` | aggregates `results/`, checks row counts agree across tools, writes the PNG |
| `results/<config>/settings.json` | versions, host, CPU set, memory cap, catalogs, columns, seed partition |
| `results/<config>/<step>/{lsdb_w<w>,astropy,smatch}_run<i>.json` | raw timings and result row counts per component and repeat |
| `results/calibration.json` | k-nearest ladder calibration (one lsdb crossmatch per candidate step) |
| `HANDOFF.md` | the parked 3e9 / full-catalog extension (2026-09-19): what to change in the driver, radii, time and disk estimates, the 2026-09-18 worker-memory findings |
| `memory_tests/` | one-worker memory scripts of 2026-09-18 (`plane_task_memory.py`, `plane_task_order.py`, `worst_pair.py`, `measure_pairs.py`), their logs and RSS timelines; findings summarised in `HANDOFF.md` |
| `results/cone_calibration_cones.json` | cone radii holding 1, 3, 10, ... 1e8, 3e8, 1e9 ZTF rows around the centre (`--calibrate-cones`) |
| `crossmatching-performance-2026-cones.png`, `plot_benchmark_cones.ipynb` | the figure of the current (cone) sweep; `-rowsread` = rows read on the x axis, `-smatch-both` = plain smatch call added as a dotted line (notebooks named alike) |
| `crossmatching-performance-2026-uniform.png`, `plot_benchmark_uniform.ipynb` | the figure of the second (uniform-partition) sweep |
| `crossmatching-performance-2026.png` | the figure of the first sweep |
| `test_uniform_write.png`, `plot_test_uniform_write.ipynb` | validation run of the second design (one repeat, small steps) |
| `results/test_cones/` | validation run of the cone design (one repeat, 10 and 3e6 rows) |
| `results/test_native/` | validation run of native tool mode (one repeat, 10, 3e6 and 1e7 rows; row counts agree for every tool) |
| `run_smatch_shuffled.sh`, `results/cones_smatch_shuffled/` | smatch alone with its second (Gaia) argument in random order (`--smatch-shuffle`), same node, cap, threads and steps as the cone sweep |
| `results/test_smatch_shuffled/` | validation of `--smatch-shuffle` (1 row and 1e6 rows, one repeat: 0 and 140,508 pairs as the plain call, 0.6 and 1.3 s) |
| `results/cones_streamed/` | the streamed-mode (2026-09-16 design) tool records of the cone ladder, 204 files, for reference |

## Setup

- Node: `sdfiana030` (AMD EPYC 7713P, 64 cores / 128 threads, 503 GB), shared. The benchmark is pinned
  with `taskset` to 32 distinct physical cores (CPUs 0-31) and capped at 128 GB:
  Dask `memory_limit = 128 GB / n_workers`; astropy and smatch run in a child process whose RSS is
  polled and killed at 128 GB (recorded as out of memory, the curve stops there).
- Dedicated node for the two largest default-column steps: `run_slurm.sh` runs `pixels:512,1024,1791` on a
  milano batch node (2 x AMD EPYC 7713, 128 cores, 480 GB, exclusive) with a 256 GB cap (16 GB per worker).
  Job 38418325 ended up on 24 cores, not 32: SLURM keeps 8 specialised cores (CPUs 24-31) for the system and the
  job cgroup silently cut `taskset -c 0-31` down to 0-23 (`results/default_slurm/settings.json`, `n_cpus`).
  16 single-thread workers plus scheduler still fit in 24 cores. The script now picks cores from the job's
  allowed set. See Results for why the rerun was made.
- Environment: conda env `lsdb-py313-linux` (Python 3.13). Versions are recorded in every
  `settings.json`; at the time of the run: lsdb 0.10.4, hats 0.10.4, dask/distributed 2026.3.0,
  astropy 7.2.0, numpy 2.4.6, pyarrow 23.0.1, smatch 0.10.2.
- `smatch` is Erin Sheldon's HEALPix matcher, built from source:
  `pip install "smatch @ git+https://github.com/esheldon/smatch.git"`.
  The PyPI / conda-forge package named `smatch` is an unrelated NLP tool.

## Catalogs (SDF mirrors under `/sdf/data/rubin/cosmicdata/lsdb/`)

| side | catalog | rows | partitions | columns used ("default" config) |
|---|---|---:|---:|---|
| left | `ztf_dr23/objects` (ZTF DR23 objects, one row per field/quadrant/filter object) | 7.57e9 | 1791 (Order 1-5, 1.3-12.6M rows each) | the 6 `hats_cols_default`: oid ra dec filtercode medianmag medmagerr |
| right | `gaia_dr3` collection (Gaia DR3, default margin `gaia_10arcs`) | 1.81e9 | 2016 (Order 2-6) | 27 columns reconstructing the 2025 default set (listed in `run_benchmark.py`) |

The "only RA and Dec" config loads `ra, dec` on both sides. Crossmatch: nearest neighbour within
10 arcsec (`n_neighbors=1`, `radius_arcsec=10`), matching the 10 arcsec margin caches.

## Ladder (how the row count is varied)

This section describes the first two sweeps. The current sweep uses only `cone:<arcsec>` steps around one centre with
radii from `--calibrate-cones` and plots left-catalog rows (see the design section above).

In the first two sweeps the x-axis is the number of crossmatched rows. Steps:

- `cone:<arcsec>`: a fine cone search inside one seed partition. The seed is the smallest ZTF
  partition whose centre lies at Dec > -10 and |b| > 30 (Order 4, pixel 484, RA 125.0, Dec +63.4,
  2.15M rows). The globally smallest partitions are at the southern edge of the ZTF footprint and
  their centres are empty, so those are excluded.
- `pixels:<k>`: the k ZTF partitions whose centres are nearest to the seed centre, a contiguous
  patch. Task count in LSDB equals k (Gaia is finer than ZTF here), so the 1/4/16-worker curves
  separate from the first pixel steps upward.

Radii and k were chosen from `results/calibration.json` so that steps land near each decade from
1e2 to 1e8 rows; k = 256, 512, 1024 and 1791 (every ZTF partition, i.e. the full catalogs) extend the
LSDB curves. Each curve stops where a single repeat would take longer than about 30 minutes on the
32-core budget: 1 worker at k=256 (3.2e8 rows), 4 workers at k=512 (7.9e8). With the default columns
the 16-worker curve on the shared node is clean up to k=512; at k=1024 and 1791 the 16 x 8 GB workers
exceeded their memory budget and were restarted, so those two points are excluded from the figure and
re-measured on a dedicated node (`run_slurm.sh`, see Results). With only RA and Dec, 16 workers reach the
full catalogs (k=1791, 3.85e9 rows). Astropy runs out of the 128 GB at 5e7 rows; smatch stops at 1.1e8
(1180 s per repeat). The same ladder is used for every tool.
The result cannot exceed the ZTF row count (7.6e9). The match fraction is ~22% around the high-latitude
seed and much higher in the Galactic plane: the full catalogs give 3.85e9 crossmatched rows (51% of ZTF).

## What each curve measures

- LSDB (w workers): wall time of `left.crossmatch(right).map_partitions(len).compute()`, so
  reading, matching and the Dask overhead. `Client(n_workers=w, threads_per_worker=1)`, and pyarrow's
  decode pool inside each worker is set to 1 thread, so "w workers" means w cores. (The 2025 run left
  the Dask default, 1 worker = 1 process x 128 threads; `--threads-per-worker 0` restores that.)
  With the default threads a first pass showed the 1/4/16 curves on top of each other up to 5e7 rows,
  because the k-d tree work releases the GIL and 1 x 32 threads equals 16 x 2 threads.
- Astropy: pyarrow read of the same ZTF, Gaia and Gaia-margin files (sequential per catalog),
  cone filter in pandas where applicable, `match_coordinates_sky` and the 10 arcsec cut.
  `SkyCoord` construction is timed but excluded from the plotted total, as in 2025.
- smatch: the same pyarrow read plus `smatch.match(gaia, 10", ztf, maxmatch=0)` (Gaia indexed, all pairs)
  followed by a per-ZTF-row closest-pair reduction in numpy. This is NOT the 2025 call form
  (`smatch.match(ztf, 10", gaia, maxmatch=1)`, ZTF indexed): on spatially sorted (HATS) input that form
  hits a slow path in smatch 0.10.2 when most Gaia points fall outside every ZTF disc. Measured on the
  seed partition: 20 s for 555 ZTF rows vs 1.6 s for the same Gaia points shuffled, and 0.6 s with Gaia
  indexed; the pairs are identical. `--smatch-orientation left` restores the 2025 form. (Root cause found 2026-09-17:
  smatch puts its SECOND argument into an unbalanced binary search tree keyed by HEALPix pixel, which HATS order
  degenerates into a chain; see the current-design section.)
- LSDB: before the timed repeats, each fresh Dask client imports the stack on every worker, reads one
  partition per side and runs one single-partition crossmatch, all untimed. The 2025 script absorbed
  this start-up in an untimed lsdb load; without it the first repeat on a fresh client costs ~3-8 s extra.
- Page cache is warm: every step's files are read once, untimed, before the timed repeats.
- Repeats: LSDB 3 per step up to 32 partitions and 2 above; astropy/smatch 5 up to 8 partitions, 3 at
  16 and 32, 2 above (the 2025 run likewise used 2 repeats for its largest steps). The plot shows the
  median with 10th/90th percentile bars; the per-step repeat count `n` is in the notebook's tables.

## Results of the first sweep (2026-09-15, sdfiana030, k-nearest ladder, results counted in memory)

Median wall time in seconds. Repeats per point: LSDB 3 up to 2.8e7 rows and 2 above; astropy and smatch
5 up to 6.2e6, 3 at 1.2e7 and 2.8e7, 2 above. 10th/90th percentiles and per-point `n` are in the notebook.

| crossmatched rows | astropy | smatch | LSDB 1 w | LSDB 4 w | LSDB 16 w | LSDB 16 w, RA/Dec only |
|---:|---:|---:|---:|---:|---:|---:|
| 87 | 0.9 | 1.0 | 1.9 | 1.7 | 1.7 | 0.9 |
| 1,060 | 0.9 | 1.2 | 2.0 | 1.6 | 1.7 | 0.9 |
| 10,195 | 1.0 | 1.3 | 2.0 | 1.8 | 1.7 | 0.9 |
| 99,626 | 1.6 | 2.3 | 2.2 | 2.0 | 2.0 | 1.1 |
| 469,610 | 3.7 | 5.4 | 3.0 | 2.9 | 2.9 | 1.9 |
| 1.10e6 | 7.8 | 5.7 | 5.5 | 3.3 | 3.3 | 2.3 |
| 2.39e6 | 15.9 | 14.5 | 10.3 | 3.8 | 3.7 | 2.5 |
| 6.20e6 | 44.0 | 43.7 | 26.1 | 10.1 | 6.9 | 5.4 |
| 1.23e7 | 82.3 | 95.9 | 48.4 | 15.6 | 7.6 | 5.6 |
| 2.78e7 | 182.3 | 222.4 | 102.9 | 28.1 | 14.2 | 10.3 |
| 5.16e7 | out of memory | 550.2 | 182.3 | 51.0 | 19.2 | 12.8 |
| 1.14e8 | | 1174.2 | 346.8 | 93.8 | 31.3 | 22.6 |
| 3.22e8 | | | 830.9 | 212.3 | 59.6 | 42.7 |
| 7.88e8 | | | | 437.1 | 125.4 | 84.2 |
| 1.82e9 | | | | | (362.7, excluded) | 186.9 |
| 3.85e9 | | | | | (1260, excluded) | 400.2 |

Excluded points. The default-column 16-worker runs at k=1024 and k=1791 (`results/default/pixels_1024`,
`pixels_1791`) ran while the workers hit their 8 GB budget: the run_all log has 40
`Worker ... exceeded 95% memory budget. Restarting` lines between 18:00 and 20:45, all in that block, none
elsewhere. Restarted workers recompute their tasks inside the timed region; the two repeats at k=1791 are
852 s and 1668 s, and the k=1024 pair (356 s, 369 s) is 1.9x the RA/Dec time where the ratio is 1.4-1.5x at
every clean step. The same block also read its files 60x slower than usual in the untimed warm-up (57 min for
k=1024 and 1 h 56 min for k=1791, against 55 s and 158 s in the RA/Dec block an hour later), and the client
warm-up at k=1791 took 246 s instead of ~15 s, so the node was under pressure for the whole block. The JSONs
are kept and listed in the notebook (`EXCLUDE` in `make_plot_notebook.py`); the RA/Dec block at the same
steps had no restarts and repeats within 4%. Both steps are re-measured by `run_slurm.sh` (results in
`results/default_slurm/`, plotted as squares when present).

Dedicated-node rerun (`run_slurm.sh`, job 38418325, sdfmilan090, 2026-09-16, 256 GB cap = 16 GB per worker,
24 usable cores (see Setup), 2 repeats each, no worker restarts, warm-up reads 180 s and 352 s):

| step | rows | dedicated node | shared node, default columns | shared node, RA/Dec only |
|---|---:|---:|---:|---:|
| pixels:512 | 7.88e8 | 172.3, 163.0 s | 125.4 s (clean) | 84.2 s |
| pixels:1024 | 1.82e9 | 372.5, 335.8 s | 355.9, 369.5 s (excluded) | 186.9 s |
| pixels:1791 | 3.85e9 | 853.9, 816.6 s | 852.1, 1668.0 s (excluded) | 400.2 s |

The overlap step (k=512) runs 1.34x slower on the milano node than on sdfiana030 (same CPU model, but a
dual-socket node, 24 instead of 32 cores available to the process tree, and different memory and filesystem
paths; the cause was not isolated), so the dedicated-node points are drawn as a
separate series (squares) and not merged into the shared-node curve. Scaled by the overlap ratio they
put a clean shared-node value near 260 s at 1.82e9 rows and near 620 s at 3.85e9 rows; the excluded shared-node
repeats were 1.4x (k=1024) and up to 2.7x (k=1791) above that.

Row counts agree exactly between LSDB (all worker counts, both column sets) and astropy at every step.
smatch returns 1, 2 and 9 fewer rows than the others at 2.8e7, 5.2e7 and 1.1e8 rows (fractions of 1e-7 to
1e-8; identical below that). Not investigated; presumably pairs at the 10 arcsec boundary in smatch's
HEALPix-disc (nside 4096) search.

## Differences from the 2025 run

| | 2025 (Bridges-2) | 2026 (SDF) |
|---|---|---|
| left catalog | ZTF DR14 objects, 1.2e9 rows | ZTF DR23 objects, 7.6e9 rows (DR14 not mirrored on SDF) |
| default columns | 15 ZTF + 27 Gaia | 6 ZTF + 27 Gaia (Gaia list reconstructed; SDF Gaia has no `hats_cols_default`) |
| hardware | 1 node, 128 cores, 256 GB, dedicated | 32 physical cores of a shared node, 128 GB cap |
| Dask threads | default (1 worker = 128 threads) | 1 thread per worker (w workers = w cores) |
| lsdb | ~0.4.5 | 0.10.4 |
| smatch numbers | reused from Kostya Malanchev's earlier run | run here with the same ladder; Gaia indexed (see above) |
| page cache | unspecified | warm (untimed pre-read) |
| top of ladder | full catalogs (1e9) | full catalogs (3.85e9 rows): RA/Dec only on the shared node, default columns on a dedicated node; default columns to 7.9e8 on the shared node; 1 worker stops at 3e8 |
| radius | 10 arcsec in code, 1 arcsec in the docs text | 10 arcsec, stated on the figure |

## Bugs in the 2025 code fixed here

1. `run_smatch` concatenated left rows, right rows and distances without `axis=1`, stacking them
   vertically: the reported match count was 3x too high and columns were misaligned. It did not
   affect the 2025 figure because that smatch curve came from separate code.
2. For cone steps the astropy SkyCoord was built from Gaia+margin (pyarrow) while the match
   function indexed a different Gaia frame from lsdb (no margin, different order): wrong rows
   joined. Timings were real, the joined output was not. Here one right-hand frame (Gaia + margin,
   deduplicated on `_healpix_29`) feeds both.
3. The plotting notebook paired I/O and match series positionally (`* 3`, `[2:]`, `[:-1]`). Here
   everything is keyed by (config, step).
4. lsdb 0.10 API: `Catalog._ddf` is gone, `MOCSearch` is not a top-level export and ZTF DR23 has no
   point map (its MOC is None), `compute()` needs `progress_bar=False`, smatch needs float64 arrays
   rather than arrow-backed columns.

## Results of the second sweep (2026-09-16 17:21 to 2026-09-17 05:13, sdfiana033)

Median wall time in seconds (read + match + write to parquet); repeats: 3 up to 32 partitions, 2 above.
Figure: `crossmatching-performance-2026-uniform.png` (notebook `plot_benchmark_uniform.ipynb`).

| step | matched rows | LSDB 1 w | LSDB 4 w | LSDB 16 w | LSDB 16 w, RA/Dec | scipy | smatch | astropy | STILTS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cone 115" | 9 | 1.5 | 1.0 | 1.1 | 0.7 | 0.9 | 0.9 | 0.8 | 12.1 |
| cone 375" | 376 | 1.4 | 1.1 | 1.1 | 0.7 | 0.9 | 0.9 | 0.8 | 12.1 |
| cone 1150" | 6,953 | 1.6 | 1.1 | 1.1 | 0.7 | 0.9 | 0.9 | 0.9 | 12.5 |
| cone 3700" | 92,585 | 2.5 | 2.1 | 2.1 | 1.1 | 1.2 | 1.4 | 1.8 | 13.8 |
| pixels 1 | 411,119 | 5.7 | 5.5 | 5.5 | 2.8 | 1.5 | 2.8 | 4.6 | 16.7 |
| pixels 2 | 801,942 | 10.4 | 5.6 | 5.5 | 2.8 | 2.6 | 5.2 | 9.0 | 24.6 |
| pixels 4 | 1.60e6 | 19.7 | 5.7 | 5.9 | 3.0 | 4.7 | 10.4 | 17.6 | 39.2 |
| pixels 8 | 3.20e6 | 39.8 | 12.7 | 6.2 | 3.3 | 9.6 | 24.5 | 36.4 | 83.5 |
| pixels 16 | 6.59e6 | 79.2 | 21.9 | 7.4 | 4.8 | 18.6 | 49.4 | 73.2 | 145.1 |
| pixels 32 | 1.31e7 | 155.7 | 42.4 | 14.1 | 8.6 | 36.6 | 120.6 | 149.6 | 293.3 |
| pixels 64 | 2.77e7 | 329.2 | 88.5 | 27.7 | 14.8 | 74.8 | 313.4 | 316.3 | 699.3 |
| pixels 128 | 6.19e7 | 716.4 | 184.9 | 57.3 | 29.4 | 173.8 | 883.5 | 687.8 | 1390.4 |
| pixels 256 | 1.44e8 | 1655.5 | 435.5 | 141.7 | 61.7 | 384.5 | 3287.2 | 1605.7 | |
| pixels 512 | 4.04e8 | | 1271.2 | 371.4 | 152.8 | 958.6 | | | |
| pixels 911 | 9.38e8 | | | 872.7 | 325.6 | | | | |

- Row counts agree exactly between LSDB (all worker counts, both column sets), astropy, scipy and STILTS at every step.
  smatch returns 2 fewer rows at 128 partitions and 10 fewer at 256 (pairs at the 10 arcsec boundary; the same effect as in
  the first sweep). No failures, no Dask worker restarts, all outputs deleted after each repeat.
- Peak resident memory at each tool's largest step: astropy 16.4 GiB (256), smatch 16.3 GiB (256), scipy 42.8 GiB (512),
  STILTS 60.0 GiB (128). The cap was never approached; every curve ends by the time rule, not by memory.
- The LSDB curves now behave as expected from the task model: with 1 worker the time doubles with k throughout; with 4
  and 16 workers it is flat while the k tasks fit in one round (k <= 4 and k <= 16) and then doubles. No partition-size
  kink. 1 to 16 workers speeds up 12.5x at 128 partitions and 11.7x at 256.
- The match rate rises from 14% at the seed to 34% over the whole band (938M matched of 2.78e9 ZTF rows), because the
  band's outer partitions reach denser Gaia sky; the 512 step therefore matched 2.8x the rows of the 256 step.
- smatch was dropped from the 512 step on 2026-09-17 (script comment): its per-chunk time grows with the size of the
  indexed Gaia side (2.7 s per 3M-row chunk with 3 Gaia partitions indexed, 12.9 s with 34), unlike the k-d tree tools,
  so 512 would have taken ~3.5 h per repeat. Its curve ends at 256 together with astropy's.
- STILTS: about half of its time is JVM start plus its own Parquet reader (`ztf_io` + `gaia_io`: 12 s at the smallest
  step, 640 s of the 1390 s at 128 partitions).
- The sweep was stopped once (02:44, start of the 512 block) and relaunched two minutes later with smatch removed from
  that block; the resumable driver skipped every finished repeat. Both logs are in `results/logs/run_all_uniform_*.log`.

## Results of the current sweep (2026-09-17/18, sdfiana033, cones from 1 to 1e9 left rows)

Three launches of `run_all_cones.sh`: the 15 steps up to 1e7 rows 08:39-09:30 and the 3e7 and 1e8 steps 09:43-11:07 (every
series; tools in streamed mode), then 12:27-14:52 the four non-LSDB tools over all 17 steps in native mode (`--tool-mode
native`, see the design section; the LSDB records were kept, the streamed tool records moved to `results/cones_streamed/`).
A fourth launch 16:40-16:55 (`run_smatch_shuffled.sh`) re-ran smatch with its Gaia argument in random order (`--smatch-shuffle`,
see the design section), and a fifth 16:56-19:01 (`run_extra_steps.sh`) added the 3e8 step for every series but the plain
smatch call. A sixth launch on 2026-09-18, 07:32-10:28 (`STEPS=cone:138355.8 ALGOS=lsdb ./run_all_cones.sh`), added the
1e9 step for the four LSDB series only: scipy, smatch and STILTS peaked at 60-63 GiB at 3e8 with memory proportional to
the rows read, so at 1e9 (3x the rows read) each would exceed the 128 GB cap as astropy already did at 3e8 (Memory, below).
The table and figures are the native-mode results; the figure's smatch curve is the shuffled one, the
table has both. Median wall time in seconds, 3 repeats per point (5 for the RA/Dec point at 3e7). LSDB: read + crossmatch + write to parquet. Tools: pyarrow reads, Gaia/margin merge, cone filter, SkyCoord
construction, the match call, parquet write. STILTS: `tmatch2` alone (`--stilts-series xmatch`; Neven, 2026-09-17: the
Parquet-to-FITS conversion is not charged to STILTS). The conversion runs once per step, is reused by the repeats and is
listed in the last column for reference.
Figures: `crossmatching-performance-2026-cones.png` (x = rows in the cone) and `crossmatching-performance-2026-cones-rowsread.png`
(x = rows of the partitions the cone touches), `crossmatching-performance-2026-cones-smatch-both.png` (plain smatch call added
as a dotted line), `crossmatching-performance-2026-cones-equal-decades.png` (2026-09-18, `--aspect equal`: one decade the same
length on both log axes, so time proportional to rows runs at 45 degrees, with a slope-1 guide through the LSDB 16-worker point
at 1e9; from 1e7 to the top of each curve the slopes are 0.75 RA/Dec-only, 0.78 LSDB 16 workers, 0.83 scipy, 0.92-0.96 smatch,
STILTS, astropy and LSDB 4 workers, 1.0 LSDB 1 worker), notebooks `plot_benchmark_cones*.ipynb`.

| left rows | radius | ZTF partitions read | ZTF rows read | matched | LSDB 1 w | LSDB 4 w | LSDB 16 w | LSDB 16 w, RA/Dec | scipy 16 thr | smatch, plain call | smatch, Gaia side in random order | astropy | STILTS tmatch2, 16 thr | STILTS Parquet-to-FITS, once per step, not plotted |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 8.4" | 1 | 3.0M | 0 | 1.41 | 1.01 | 0.95 | 0.69 | 0.98 | 8.99 | 1.33 | 0.87 | 1.73 | 9.2 |
| 10 | 26.5" | 1 | 3.0M | 0 | 1.50 | 1.10 | 1.01 | 0.71 | 0.96 | 9.49 | 1.31 | 0.86 | 1.69 | 9.5 |
| 100 | 98.7" | 1 | 3.0M | 7 | 1.41 | 1.10 | 1.07 | 0.90 | 0.99 | 8.83 | 1.34 | 0.85 | 1.77 | 9.4 |
| 1,000 | 254.5" | 1 | 3.0M | 116 | 1.74 | 1.22 | 1.08 | 0.70 | 1.01 | 9.19 | 1.34 | 0.85 | 1.85 | 10.1 |
| 10,000 | 587.3" | 1 | 3.0M | 1,358 | 1.44 | 1.06 | 1.07 | 0.71 | 1.01 | 9.88 | 1.33 | 0.89 | 1.94 | 9.4 |
| 100,000 | 1563.8" | 1 | 3.0M | 14,065 | 1.71 | 1.34 | 1.22 | 0.80 | 1.03 | 14.7 | 1.44 | 1.01 | 2.39 | 9.3 |
| 300,000 | 2605.7" | 1 | 3.0M | 41,921 | 2.01 | 1.62 | 1.42 | 0.91 | 1.15 | 29.5 | 1.62 | 1.35 | 2.94 | 9.3 |
| 1,000,000 | 1.23 deg | 1 | 3.0M | 140,508 | 2.80 | 2.49 | 2.43 | 1.60 | 1.32 | 72.2 | 2.24 | 2.47 | 4.08 | 9.4 |
| 3,000,001 | 2.07 deg | 6 | 25.1M | 417,995 | 11.2 | 5.29 | 5.03 | 2.78 | 5.44 | 234 | 8.40 | 9.04 | 11.7 | 36.5 |
| 10,000,004 | 3.86 deg | 8 | 38.9M | 1,373,233 | 24.1 | 7.62 | 5.77 | 3.04 | 9.44 | 653* | 16.2* | 22.3 | 24.9 | 51.9 |
| 30,000,015 | 6.65 deg | 17 | 65.7M | 4,110,321 | 58.0 | 20.8 | 12.6 | 6.90 | 20.1 | killed at 1800 | 40.7* | 58.0 | 61.6 | 89.8 |
| 100,000,015 | 12.26 deg | 42 | 154.2M | 14,070,812 | 190 | 55.2 | 29.4 | 15.9 | 56.4 | skipped | 127* | 203 | 174 | 231 |
| 299,999,972 | 21.04 deg | 113 | 385.5M | 45,528,252 | 573 | 154 | 56.5 | 27.6 | 160 | not run | 372* | killed at 128 GiB | 589 | 639 |
| 1,000,000,093 | 38.43 deg | 341 | 1149.8M | 198,955,959 | 2362 | 634 | 209 | 94.2 | not run (memory) | not run | not run (memory) | not run (memory) | not run (memory) | not run |

(The 3, 30, 300, 3,000 and 30,000-row steps were also run and sit on the same floor; they are in the notebook table.)

- **Row counts.** Every tool agrees on the crossmatched count at every step except smatch, which returns one row fewer at
  1e7 (1,373,232 against 1,373,233), and with shuffled input also at 3e7, 1e8 and 3e8 (4,110,320, 14,070,811 and 45,528,251
  against one more): the same pair at the 10 arcsec boundary as in the streamed run. At 3e6 smatch agrees exactly (417,995).
  At 1e9 the four LSDB series agree on 198,955,959 rows (19.9% of the left rows; 15.2% at 3e8: the wider cone reaches
  denser Gaia sky). No Dask worker restarts at any step, 1e9 included; the only tool to reach the 128 GB cap is astropy at 3e8 (Memory, below).
- **smatch, plain call.** `smatch.match(ztf_ra, ztf_dec, 10", gaia_ra, gaia_dec, maxmatch=1)` on HATS-sorted input costs
  9 s for 1 row, 72 s at 1e6, 234 s at 3e6 and 653 s at 1e7, and the first repeat at 3e7 was killed at the 1800 s per-run
  cap (repeats 1-2 and the 1e8 step skipped, recorded as timeout). Cause: smatch's HEALPix tree is an unbalanced binary
  search tree built from the second argument, and sorted keys turn it into a chain (design section).
- **smatch, Gaia side in random order.** The same call with `gaia[perm]`: 1.3 s floor, 2.2 s at 1e6, 8.4 s at 3e6, 16 s at
  1e7, 41 s at 3e7, 127 s at 1e8 (85 s of it the match call; peak 25 GiB) and 372 s at 3e8 (254 s match, 54 s write, 48 s
  cone filter; peak 60 GiB). That is 40x faster than the plain call at 1e7 and also faster than the streamed design (191 s at
  1e8), which iterated every Gaia row per chunk. Single-threaded, it sits between scipy on 16 threads (56 s) and STILTS
  tmatch2 on 16 threads (174 s) at 1e8, and between scipy (160 s) and LSDB 1 worker (573 s) at 3e8.
- **Native against streamed for the other tools: the same numbers within noise.** At 1e8 scipy 56 against 62 s, astropy
  203 against 188 s, STILTS tmatch2 174 against 185 s; at 1e7 scipy 9.4 against 10.4, astropy 22.3 against 22.1, STILTS
  24.9 against 24.7. The native rerun moved one curve, smatch.
- **Memory.** Peak resident memory at 1e8: astropy 66.2 GiB (the 27 Gaia columns are gathered for every one of the 1e8
  left rows before the radius cut, as in 2025), STILTS 45.5 GiB (Java heap, `-Xmx102g`), scipy 25.6 GiB; at 3e7
  astropy 20.5, STILTS 26.3, scipy 11.3 GiB. At 3e8 (386M rows read) astropy was killed at the 128 GB cap (128.1 GiB after
  780 s in the first repeat; repeats 2-3 skipped, recorded as oom), so its curve ends at 1e8; STILTS peaked at 63, scipy 61
  and smatch 60 GiB, about 2.5x their 1e8 values, i.e. proportional to the rows read. At 1e9 (1.15e9 rows read) those three
  would need ~180 GiB each and would also end at the cap, so the 1e9 step was run for the LSDB series only. LSDB's memory
  is per task (one ZTF partition and the Gaia partitions it overlaps, 16 workers x 8 GB), not per selection. astropy's 203 s at 1e8 is 160 s of `match_coordinates_sky` (225 s in the
  first repeat), 18.8 s of cone filter (numpy haversine over the 154M rows read), 17 s of parquet write and 7 s of reads,
  merge and SkyCoord construction.
- **Below ~1e5 rows every curve is flat**: the cost is reading the one 3M-row seed partition (plus one Gaia partition and
  its margin) whatever the cone radius. LSDB pays ~1.0 s (4 and 16 workers) to ~1.4 s (1 worker) of Dask graph and
  scheduling overhead against 0.85-1.0 s for astropy and scipy in process; with only RA and Dec LSDB 16 workers is the
  fastest series at 0.7 s. STILTS tmatch2 sits at 1.7 s (JVM start), plus 9 s of Parquet-to-FITS conversion that is not
  plotted. smatch sits at 9 s, the slow-path floor.
- **From 1e6 upward the curves separate.** Up to 1e6 rows the cone is still inside one partition, so LSDB runs a single
  task and 1, 4 and 16 workers differ only by overhead (2.8 / 2.5 / 2.4 s). The cone spans 6 partitions at 3e6, 8 at
  1e7, 17 at 3e7, 42 at 1e8, 113 at 3e8 and 341 at 1e9. With 16 workers 6 and 8 tasks run in one round (5.0, 5.8 s), 17 in two (12.6 s),
  42 in three (29.4 s), 113 in eight (56.5 s) and 341 in 22 (209 s); 1 worker runs them in series (11.2, 24.1, 58.0, 190, 573, 2362 s). The cost of a cone step is set by the
  partitions it touches, not by the rows inside the cone: the 1e7 step reads 39M ZTF rows to match 1e7 of them, for
  every tool.
- The kink of the earlier design is gone: there is no step where the ladder changes kind, and every curve is monotonic.
- **At 1e8 left rows** (1.4e7 matched): LSDB 16 workers 29.4 s (15.9 s with RA/Dec only), LSDB 4 workers 55 s, scipy
  56 s on 16 threads, smatch with shuffled input 127 s, STILTS tmatch2 174 s on 16 threads, LSDB 1 worker 190 s,
  single-threaded astropy 203 s; the plain smatch call did not reach this step. Under the same 16-thread cap scipy is 1.9x
  and STILTS 5.9x slower than LSDB 16 workers; astropy matches LSDB 1 worker.
- **At 3e8 left rows** (4.55e7 matched, 113 partitions, 386M rows read): LSDB 16 workers 56.5 s (27.6 s with RA/Dec only),
  LSDB 4 workers 154 s, scipy 160 s on 16 threads (43 s of k-d tree query, 48 s of cone filter over the 386M rows read, 57 s
  of parquet write), smatch with shuffled input 372 s, LSDB 1 worker 573 s, STILTS tmatch2 589 s on 16 threads (plus 639 s
  of Parquet-to-FITS conversion, once); astropy killed at the memory cap. From 1e8 to 3e8 (2.5x the rows read) LSDB 16
  workers grew 1.9x, LSDB 4 workers 2.8x, scipy 2.8x, smatch 2.9x, LSDB 1 worker 3.0x, STILTS 3.4x. Under the same 16-thread
  cap scipy is 2.8x and STILTS 10x slower than LSDB 16 workers.
- **At 1e9 left rows** (1.99e8 matched, 341 ZTF and 47 Gaia partitions, 1.15e9 rows read; LSDB series only, 2026-09-18):
  LSDB 16 workers 209 s (repeats 217 / 209 / 202; 94 s with RA/Dec only), LSDB 4 workers 634 s, LSDB 1 worker 2362 s
  (39 min). From 3e8 to 1e9 (3.0x the rows read and the partitions) LSDB 16 workers grew 3.7x, 4 workers 4.1x, 1 worker
  4.1x and RA/Dec 3.4x, more than the rows read: the 38 deg cone reaches |b| = 17 deg, where Gaia is denser, so the
  matched rows grew 4.4x and the Gaia partitions read from 14 to 47. Worker scaling at this size: 1 -> 4 workers 3.7x,
  4 -> 16 workers 3.0x. Throughput of the 16-worker run: 5.5M ZTF rows read per second (12M with RA/Dec only).
- The RA/Dec-only 16-worker point at 3e7 was re-measured 12:03 with 5 repeats (6.9 / 7.5 / 6.8 / 6.1 / 7.0 s, median 6.9)
  after the first run had 39.0 / 6.3 / 6.0 s; the three original records are kept in
  `results/cones_radec/cone_23931.84/outlier_run_11-05/` (outside the plot's glob). The 39 s repeat was the first timed run
  after the Dask client came up; it did not recur.
- Node time: 51 min for the 15 streamed steps to 1e7 (mostly fixed overhead: three Dask client start-ups and 15 child
  processes per step), 84 min for 3e7 + 1e8; the native rerun 2 h 25 min, of which ~65 min smatch (3 x 11 min at 1e7,
  30 min at the cap at 3e7) and ~40 min STILTS (conversions plus repeats at 3e7 and 1e8); the shuffled smatch rerun 15 min;
  the 3e8 step 2 h 5 min (LSDB 1 worker 29 min, STILTS 40 min incl. the conversion, smatch 19 min, astropy 13 min before the
  kill); the 1e9 step 2 h 56 min (LSDB 1 worker 3 x 40 min, 4 workers 3 x 10.5 min, 16 workers 3 x 3.5 min, RA/Dec 3 x 1.5 min,
  plus 5 min of untimed row counting and warm-up reads).

## Re-running

```bash
# cone calibration: radii holding N ZTF rows around the seed centre -> results/cone_calibration_cones.json + STEPS line
python run_benchmark.py --config-name cones --seed 4,715 --calibrate-cones 1,3,10,30,100,300,1e3,3e3,1e4,3e4,1e5,3e5,1e6,3e6,1e7,3e7,1e8
# the current (cone) sweep, resumable; STEPS=... restricts it to a subset of the calibrated steps
nohup ./run_all_cones.sh > results/logs/run_all_cones_$(date +%F_%H-%M-%S).log 2>&1 &
# extend the ladder by a calibrated step (default cone:75730.94 = 3e8 rows) for every series but the plain smatch call
STEPS=cone:75730.94 nohup ./run_extra_steps.sh > results/logs/run_extra_steps_$(date +%F_%H-%M-%S).log 2>&1 &
# the 1e9 step (cone:138355.8), LSDB series only: the other tools would exceed the 128 GB cap
STEPS=cone:138355.8 ALGOS=lsdb nohup ./run_all_cones.sh > results/logs/run_all_cones_1e9_$(date +%F_%H-%M-%S).log 2>&1 &
python make_plot_notebook.py --config cones --radec-config cones_radec --slurm-config none --x left --legend-ncol 2 --legend-pos below \
  --smatch-config cones_smatch_shuffled --stilts-series xmatch \
  --png crossmatching-performance-2026-cones.png --notebook plot_benchmark_cones.ipynb && jupyter nbconvert --execute --to notebook --inplace plot_benchmark_cones.ipynb
# the same with equal decade length on both axes (add --x read / --smatch-plain dotted for the other two figures)
python make_plot_notebook.py --config cones --radec-config cones_radec --slurm-config none --x left --legend-ncol 3 --legend-pos below \
  --smatch-config cones_smatch_shuffled --stilts-series xmatch --aspect equal \
  --png crossmatching-performance-2026-cones-equal-decades.png --notebook plot_benchmark_cones_equal_decades.ipynb && jupyter nbconvert --execute --to notebook --inplace plot_benchmark_cones_equal_decades.ipynb
# k-nearest calibration (lsdb only, prints result rows per candidate step)
./run_benchmark.sh calib --calibrate --workers 16 --steps cone:80,cone:250,pixels:1,pixels:4
# the second sweep (resumable: existing per-repeat JSONs are skipped)
nohup ./run_all_uniform.sh > results/logs/run_all_uniform_$(date +%F_%H-%M-%S).log 2>&1 &
# the first sweep
nohup ./run_all.sh > results/logs/run_all_$(date +%F_%H-%M-%S).log 2>&1 &
# one ad-hoc invocation
./run_benchmark.sh default --columns default --algos lsdb --workers 16 --steps pixels:1791 --n-runs 1
# the largest default-column steps on a dedicated milano node (log: results/logs/slurm_default_<jobid>.log)
sbatch --output="$PWD/results/logs/slurm_default_%j.log" run_slurm.sh
python make_plot_notebook.py && jupyter nbconvert --execute --to notebook --inplace plot_benchmark.ipynb
```
