# Not yet run: the 3e9-row step and the full catalog

**Status (2026-09-19): planned, not started.** The cone ladder is measured from 1 to 1e9 ZTF rows (see `README.md`).
The next two steps are a cone with 3e9 rows and the full catalog (7.57e9 rows). This file has what is needed to
run them.

## Why the 16-worker run needs more memory per worker

These facts come from one-worker memory tests on 2026-09-18 (scripts and outputs in `memory_tests/`).

- **Memory follows the matched rows, not the input rows.** A task holds its matched rows x 33 columns, plus a copy
  made by Arrow when it writes. In the Galactic plane ~98% of ZTF rows have a Gaia match. At high latitude it is
  15-20%.

  | ZTF pixel (same pixel on both sides) | ZTF rows | Galactic latitude | Matched rows | Peak memory |
  |---|---:|---:|---:|---:|
  | 2/135 (largest ZTF partition) | 12.61M | -42 | 2.48M (20%) | 2.3 GiB |
  | 3/379 | 10.77M | -8 | 7.23M (67%) | 4.4 GiB |
  | 3/635 | 10.61M | +11 | 7.73M (73%) | 4.5 GiB |
  | 3/337 | 10.29M | +9 | 8.09M (79%) | 4.8 GiB |
  | 4/912 | 9.68M | -1 | 9.5M (98%) | 5.2 GiB |
  | **4/879 (largest task in the full crossmatch)** | 10.01M | -3 | 9.81M (98%) | **5.7 GiB** |

- **Inside one task.** The crossmatch peaks at 5.7 GiB: ~1 GiB of inputs and 3.9 GiB of working memory. The write
  peaks at 4.9 GiB: 2.3 GiB of result and a 2 GiB Arrow copy. Writing a 10M-row result takes ~130 s.
- **Crossmatch and write are separate tasks.** In lsdb 0.10.4, `write_catalog` is `map_partitions(perform_write)`.
  For each partition the crossmatch task and the write task run one after the other. Nothing from the previous
  partition is kept. Counting the rows instead of writing them uses the same worker memory.
- **Partitioning sets the task size.** Where Gaia is split finer (Order 5), one ZTF Order 4 partition runs as four
  ~3 GiB tasks. Where Gaia is at Order 4, the whole 10M-row ZTF partition is one task. lsdb cannot split a task finer
  than the finer of the two catalogs.
- **Count of large tasks.** Of the 2298 tasks in the full crossmatch, ~59 expect more than 6M matches (4-6 GiB) and
  ~193 expect more than 4M (3 GiB or more).
- **Why 8 GB is not enough.** After a task, a worker still holds 1.2-1.4 GiB (Arrow memory not yet released). Reading
  a ZTF partition shared with a neighbouring task adds ~1 GB. So 5.7 + ~1.5 GiB reaches the point where Dask restarts
  an 8 GB worker (95%, 7.08 GiB). 12 GB per worker is enough; 16 GB is safe.

The first ladder (2026-09-15) restarted workers 80 times with 16 workers x 7.45 GiB, default columns, at the steps
that reached the plane. With RA/Dec only there were no restarts.

## Geometry

The cone centre is RA 216.82, Dec +57.40 (Galactic l = 100, b = +55). The cone reaches the Galactic plane at a
radius of ~55 deg.

- **3e9-row cone:** radius ~65 deg. About 870 ZTF partitions (~55 of them Order 5), ~3.5e9 rows read, ~340 Gaia
  partitions. It reaches Dec -7.6, still inside the ZTF footprint.
- **Full catalog:** 7,568,652,529 rows, 1791 partitions (Orders 1-5), 1.34 TB, ~80% of the sky, 3.85e9 matched rows
  (51%).

## Steps to do

1. **Driver: per-worker memory option.** Add an option such as `--worker-mem-gb` so the 16-worker series can use
   16 GB per worker while keeping 16 processes and the 32 pinned cores. Log the summed worker memory every few
   seconds, so the 128 GB cap on the shared node is checked, not assumed. Store the setting in each record.
2. **3e9 step.** Use radius `cone:234000` (~65 deg; expect ~3.0e9 rows in the cone). The driver already counts the
   exact rows in the cone (untimed). The exact calibration (`--calibrate-cones`) sorts all distances in memory and
   does not fit in 128 GB at this size. Add the radius to `results/cone_calibration_cones.json` by hand.
3. **Full-catalog step.** `pixels:1791` already works: it takes all 1791 partitions, with no cone and no calibration.
4. **Repeats.** 1 repeat for the 1-worker series at 3e9 and at full size; otherwise that series alone takes
   8-10 h + 15-20 h. 3 repeats for the other series.
5. **Disk.** Results are written to the output directory and deleted after each repeat. Expect ~1.2-1.5e9 matched
   rows at 3e9 (~100-150 GB per repeat) and 3.85e9 at full size (~300-400 GB per repeat). Check free space first.
6. **Plot and README.** `make_plot_notebook.py` finds new steps automatically. Add a note to the figure if the
   16-worker series used a different memory limit at the top steps. Add the new rows to the README table.

## Time estimates per repeat

Scaled from the 1e9 step. In the plane the match rate rises to ~98%, so expect the upper end.

| Series | 1e9 (measured) | 3e9 | Full catalog (7.57e9) |
|---|---|---|---|
| 1 worker | 2362 s | 2.5-3.5 h | 5-7 h |
| 4 workers | 634 s | 45-60 min | 1.7-2 h |
| 16 workers | 209 s | 15-20 min | 30-40 min |
| 16 workers, RA/Dec only | 94 s | 6-8 min | 10-15 min (first ladder: 392 s, 408 s) |

## Commands

Run from this directory, after step 1 above:

```bash
STEPS=cone:234000 N_RUNS=3 nohup ./run_all_cones.sh > results/logs/run_all_cones_3e9_$(date +%F_%H-%M-%S).log 2>&1 &
STEPS=pixels:1791 N_RUNS=3 nohup ./run_all_cones.sh > results/logs/run_all_cones_full_$(date +%F_%H-%M-%S).log 2>&1 &
```

Memory tests:

```bash
python memory_tests/measure_pairs.py 4/879:4/879   # one worker, prints the peak memory of one task
python memory_tests/worst_pair.py                  # ranks all tasks from _metadata, then measures the largest
```

## Notes

- An earlier explanation, "a worker holds a finished result while the next crossmatch runs", was wrong. The
  task-by-task measurements above replace it.
- Case B of `memory_tests/plane_task_order.py` (crossmatch and write fused by hand) fails: nested-pandas
  `to_parquet` does not accept `index=False`. It is not needed; case A answered the question.
- Scripts that start a Dask `LocalCluster` need an `if __name__ == "__main__":` guard.
