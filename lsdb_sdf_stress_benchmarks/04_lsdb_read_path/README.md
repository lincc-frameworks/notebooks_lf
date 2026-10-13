# 04: LSDB read path vs pyarrow

`readbench.py` reads the same four columns from the same N partitions of one HATS catalog through five readers
(pyarrow to Arrow, pyarrow to pandas with numpy and with Arrow dtypes, nested-pandas, the full lsdb
`open_catalog` + `map_partitions` + `compute` path) and reduces to one mean, so the readers can be compared on
bytes read, read syscalls (`/proc/self/io`) and wall time. Readers run in-process threads (not Dask worker
processes) so that the I/O counters cover everything.

- **Data:** default Gaia DR3 (`/sdf/data/rubin/cosmicdata/lsdb/gaia_dr3/gaia`, 2016 partitions, 713 GB, 153
  columns); any HATS catalog with `--root` and a numeric `--column`.
- **Baseline (2026-09, sdfiana033):** full Gaia, 1 column: lsdb 320 s / 56 GB read vs pyarrow 88 s / 10 GB (that
  gap was a 4-column-vs-1-column artifact of lsdb always reading ra/dec/_healpix_29); same 4 columns, 192
  partitions: lsdb 4.0 s vs pyarrow 2.7 s, bytes within 1%; 48 partitions on 2026-10-13: 2.0 s vs 1.1 s.
- **Smoke:** `./run_smoke.sh` (16 partitions, 4 threads, 2 reps, ~1 min; checks every reader returns the same mean).
- **Full run:** `python readbench.py --n-partitions 0 --threads 8 --reps 3` (warm cache; one pass over the
  4 columns of all 2016 partitions reads ~56 GB, so 3 reps x 5 readers is ~840 GB of reads). Cold cache:
  `--no-warm --reps 1 --offset <untouched partitions>`; `posix_fadvise` cannot evict weka's cache.
- Origin: `analysis/lsdb/perf/issue-1010-io/readbench.py` (lsdb#1010 verification), with catalog, column,
  partition selection, thread count and variants turned into arguments.
