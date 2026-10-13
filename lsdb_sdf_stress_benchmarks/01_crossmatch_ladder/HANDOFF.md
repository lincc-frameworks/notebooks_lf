# LSDB crossmatch cone ladder, 3e9 and full-catalog extension — handoff

**Status: PARKED (2026-09-19).** The cone ladder is complete from 1 to 1e9 ZTF rows (1e9 = LSDB series only, run
2026-09-18). Neven decided on 2026-09-19 not to run the next steps (3e9 rows and the full 7.57e9-row catalog) now.
This file holds everything needed to run them later. Committed to `notebooks_lf` on 2026-10-13 (logs, `review_page.html` and the STILTS jar are excluded).

## What to read
- `README.md` — design, results tables, figures, re-running commands for every sweep so far.
- `HANDOFF.md` (this file) — only the 3e9 / full-catalog extension and the memory investigation of 2026-09-18.
- `run_all_cones.sh`, `run_benchmark.py` — the driver (`STEPS=... ALGOS=... ./run_all_cones.sh`, resumable).
- `make_plot_notebook.py` — figure generator (`--aspect equal` added 2026-09-18).
- `memory_tests/` — the four one-worker memory scripts, their logs and the RSS timelines (details below).
- `results/cone_calibration_cones.json` — 19 calibrated cone steps up to 1e9 (`cone:138355.8` = 1e9 rows).

## Done
- 1e9 step, LSDB series only, sdfiana033, 2026-09-18 07:32-10:28 (log `results/logs/run_all_cones_1e9_2026-09-18_07-32-46.log`):
  341 ZTF + 47 Gaia partitions, 1.15e9 rows read, 198,955,959 matched (19.9%). Medians: LSDB 1 w 2362 s, 4 w 634 s,
  16 w 209 s, 16 w RA/Dec 94 s. No worker restarts. Figures and README updated.
- Legend label "smatch" (was "smatch (Gaia side passed in random order)"); equal-decade figure variant.
- Memory investigation (below), 2026-09-18 15:15-17:10 on sdfiana013, one Dask worker, 60 GB limit.

## Facts established on 2026-09-18 (why the 16-worker default-column series restarts in the Galactic plane)
- The first sweep (2026-09-15) timed `map_partitions(len).compute()`: nothing was gathered to the client. Its 80 worker
  restarts (all 16 workers x 7.45 GiB, default columns, steps that reached the plane; none with RA/Dec) came with Dask's
  "Unmanaged memory use is high" warning. Output mode (count vs `write_catalog`) does not change worker memory.
- `write_catalog` in lsdb 0.10.4 is `map_partitions(perform_write)`; in the task stream `perform_crossmatch` and
  `perform_write` are separate tasks run back to back per partition. Nothing of a previous partition is held.
- Task memory follows the MATCHED rows x 33 columns (plus arrow's copy at write time), not the input rows:

  | same-pixel pair | ZTF rows | Gal. lat. | matched | peak RSS |
  |---|---|---|---|---|
  | 2/135 (largest ZTF partition, largest task by input rows) | 12.61M | -42 | 2.48M (20%) | 2.3 GiB |
  | 3/379 | 10.77M | -8 | 7.23M (67%) | 4.4 GiB |
  | 3/635 | 10.61M | +11 | 7.73M (73%) | 4.5 GiB |
  | 3/337 | 10.29M | +9 | 8.09M (79%) | 4.8 GiB |
  | 4/912 | 9.68M | -1 | 9.5M (98%) | 5.2 GiB |
  | **4/879 (worst task in the whole crossmatch)** | 10.01M | -3 | 9.81M (98%) | **5.7 GiB** |

  Within one task: crossmatch step peaks 5.7 GiB (inputs ~1 GiB tracked + 3.9 GiB working memory), write step 4.9 GiB
  (2.3 GiB result + 2 GiB arrow copy); write of a 10M-row result takes ~130 s.
- The split decides the task size: where Gaia is at Order 5 a ZTF Order 4 partition runs as four ~3 GiB tasks
  (4/921, 909, 901, 2007); where Gaia is Order 4 (4/879, 912, 815, 891, 827, 976) the whole 10M-row partition is one task.
  lsdb has no knob to align finer than the finer input catalog (feature request material).
- Of the 2298 tasks in the full crossmatch, ~59 have >6M expected matches (4-6 GiB) and ~193 have >4M (>=3 GiB).
  Worker baseline after a task is 1.2-1.4 GiB (arrow memory not yet returned) and a shared ZTF read for a split
  neighbour adds ~1 GB, so 5.7 + ~1.5 GiB reaches the 95% kill line (7.08 GiB) of an 8 GB worker. 12 GB per worker
  is already comfortable; 16 GB is safe.
- Plane geometry from the cone centre (RA 216.82 Dec +57.40, l = 100, b = +55): the cone reaches b = 0 from ~55 deg;
  a 3e9-row cone is ~65 deg (about 870 ZTF partitions, ~55 of them Order 5, ~3.5e9 rows read, ~340 Gaia partitions,
  Dec down to -7.6, inside the footprint). Whole catalog: 7,568,652,529 rows, 1791 partitions (Orders 1-5), 1.34 TB,
  ~80% of the sky, 3.85e9 matched (51%). Match rate is ~98% within 5 deg of the plane, 15-20% at high latitude.

## In progress / Next (nothing started)
- [ ] Driver: `--worker-mem-gb` (or similar) so the 16-worker series can run with `memory_limit=16GB` per worker while
      the process count and the 32-core `taskset` stay as they are; log the summed worker RSS every few seconds so the
      128 GB shared-node cap is verified rather than nominal. Record the setting in the step records for the plot footer.
- [ ] 3e9 step: pick the radius from partition_info sums (~65 deg = `cone:234000`; expect ~3.0e9 rows in the cone) and
      let the existing untimed per-file `count_left_rows` give the exact `left_rows`. The exact-count calibrator
      (`--calibrate-cones`, sorts all separations in memory) does not fit in 128 GB at this size; a streaming histogram
      would, if an exact 3.0e9 is wanted. Add the radius to `results/cone_calibration_cones.json` by hand or extend it.
- [ ] Full-catalog step: `pixels:1791` already works (k nearest partitions with k = all; `left_rows` = partition_info sum
      = 7.57e9). No cone filter, no calibration.
- [ ] Repeats: 1 repeat for LSDB 1 worker at 3e9 and full (else 8-10 h + 15-20 h for that series alone); 3 for the rest.
- [ ] Disk: results are written to `/sdf/data/rubin/user/ncaplar/xmatch-bench-out` and deleted per repeat. Expect
      ~1.2-1.5e9 matched rows at 3e9 (~100-150 GB per repeat) and 3.85e9 at full (~300-400 GB per repeat; 583 GB were free
      on 2026-09-18). Check `df -h /sdf/data/rubin` first.
- [ ] Plot: `make_plot_notebook.py` picks new steps up by glob; add a footer/legend note if the 16-worker series ran with
      a different per-worker limit at the top steps.
- [ ] README: new rows in the results table, a "3e9 and full" bullet, node time, re-running commands.

## Time estimates (from the 1e9 step; the plane raises the match rate to ~98%, so expect the upper end)
| series | 1e9 measured | 3e9 per repeat | full 7.57e9 per repeat |
|---|---|---|---|
| LSDB 1 worker | 2362 s | 2.5-3.5 h | 5-7 h |
| LSDB 4 workers | 634 s | 45-60 min | 1.7-2 h |
| LSDB 16 workers | 209 s | 15-20 min | 30-40 min |
| LSDB 16 workers RA/Dec | 94 s | 6-8 min | 10-15 min (first sweep: 392/408 s) |
Non-LSDB tools stop at 3e8 (astropy at 1e8): memory proportional to rows read (~180 GiB each at 1e9). A milano node
(480 GB, 24 usable cores, 1.34x slower) could take scipy/smatch/STILTS to 1e9 but not 3e9; astropy nowhere past 1e8.

## Decisions & rationale
- 1e9 was run for LSDB only (Neven, 2026-09-18) — the other tools would exceed the 128 GB cap; kill records add no curve.
- 3e9 / full not run now (Neven, 2026-09-19).
- 16 GB per worker for the plane steps is the recommended fix — the per-task peak is set by the catalogs' partitioning,
  not by scheduling; fewer workers or fewer columns would change the series definition.
- Legend says "smatch"; the footer records the shuffled Gaia argument (Neven, 2026-09-18).

## Gotchas / superseded
- The "worker holds a finished result while the next crossmatch runs" explanation (2026-09-18 afternoon) was WRONG;
  superseded by the task-stream attribution above. Do not resurrect it.
- `memory_tests/plane_task_order.py` case B (hand-fused write) fails: nested-pandas `to_parquet` rejects `index=False`.
  Not needed; case A's task stream answered the question.
- Scripts spawning a `LocalCluster` need `if __name__ == "__main__":` (multiprocessing spawn).
- `pkill -f` with a pattern that also appears literally elsewhere in the same command line kills the shell (exit 144).
- `ssh node "setsid nohup ... & disown"` detaches the job fine but the local ssh client may hang; verify with a second
  `ssh node pgrep`, do not wait on the launch call.
- Cone calibration at 3e9 does not fit in memory (see Next). `pixels:k` steps need no calibration.
- Test outputs of 2026-09-18 (`xmatch-bench-out/plane-test`, 24 GB) were deleted on 2026-09-19; the small evidence files
  are in `memory_tests/`.

## Resume
- env: `conda activate lsdb-py313-linux` (or use the absolute python `~/rubin-user/conda_envs/lsdb-py313-linux/bin/python`)
- node: sdfiana033 (every cone-ladder point so far was measured there), `ssh sdfiana033`, then in this directory:
  ```bash
  # after the driver change for the per-worker limit; 3e9 radius ~234000 arcsec (~65 deg), full catalog = pixels:1791
  STEPS=cone:234000 ALGOS=lsdb N_RUNS=3 nohup ./run_all_cones.sh > results/logs/run_all_cones_3e9_$(date +%F_%H-%M-%S).log 2>&1 &
  STEPS=pixels:1791 ALGOS=lsdb N_RUNS=3 nohup ./run_all_cones.sh > results/logs/run_all_cones_full_$(date +%F_%H-%M-%S).log 2>&1 &
  python make_plot_notebook.py --config cones --radec-config cones_radec --slurm-config none --x left --legend-ncol 2 --legend-pos below \
    --smatch-config cones_smatch_shuffled --stilts-series xmatch --png crossmatching-performance-2026-cones.png \
    --notebook plot_benchmark_cones.ipynb && jupyter nbconvert --execute --to notebook --inplace plot_benchmark_cones.ipynb
  ```
- memory tests: `python memory_tests/measure_pairs.py 4/879:4/879` (one worker, prints peak RSS), `python memory_tests/worst_pair.py`
  (ranks all aligned pieces from `_metadata`, then measures the top ones).
