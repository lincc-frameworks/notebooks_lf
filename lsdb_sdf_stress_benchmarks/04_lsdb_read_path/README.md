# 04: LSDB read path compared with pyarrow

**What it does.** `readbench.py` reads the same four columns from the same N partitions of one HATS catalog with five
readers, and reduces each to one mean value. The readers are:

- pyarrow, to an Arrow table;
- pyarrow, to pandas with numpy types;
- pyarrow, to pandas with Arrow types;
- nested-pandas;
- the full lsdb path: `open_catalog`, `map_partitions`, `compute`.

For each reader it records wall time, bytes read and the number of read system calls (from `/proc/self/io`). The
readers run as threads inside one process, not as Dask worker processes, so the I/O counters include all the reads.

**Data.** By default Gaia DR3 (`/sdf/data/rubin/cosmicdata/lsdb/gaia_dr3/gaia`: 2016 partitions, 713 GB, 153
columns). Any HATS catalog works with `--root` and a numeric `--column`.

**Smoke test.** `./run_smoke.sh`: 16 partitions, 4 threads, 2 repeats, about 1 minute. It passes if every reader
returns the same mean.

**Full run.**

```bash
python readbench.py --n-partitions 0 --threads 8 --reps 3
```

This uses a warm cache. One pass over the 4 columns of all 2016 partitions reads ~56 GB, so 3 repeats x 5 readers
read ~840 GB in total.

For a cold cache, use `--no-warm --reps 1 --offset <N>`, where the offset skips partitions that were already read.
`posix_fadvise` cannot clear the Weka cache on `/sdf/data`, so only partitions no process on the node has read are
cold.

**Recorded results** (sdfiana033 unless noted).

- Same 4 columns, 192 partitions: lsdb 4.0 s, pyarrow 2.7 s, bytes read within 1% (2026-09).
- Same 4 columns, 48 partitions: lsdb 2.0 s, pyarrow 1.1 s (2026-10-13).
- Full Gaia, 1 requested column: lsdb 320 s and 56 GB read, pyarrow 88 s and 10 GB read (2026-09). This gap is
  not a fair comparison. lsdb always also reads `ra`, `dec` and `_healpix_29`, so it read 4 columns and pyarrow
  read 1.

**Origin.** `analysis/lsdb/perf/issue-1010-io/readbench.py`, written to check lsdb#1010. The catalog, column,
partition selection, thread count and readers were turned into arguments.
