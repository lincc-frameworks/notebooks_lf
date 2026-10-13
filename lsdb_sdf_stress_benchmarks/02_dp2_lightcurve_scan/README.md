# 02: DP2 lightcurve scan

**What it does.** `lightcurve_scan.py` reads partitions of the DP2 `object_lc` catalog. In this catalog each row is one
object, and its forced-source measurements are stored in list columns (a nested catalog). For each object the script
applies quality cuts and computes the brightness amplitude in each band, with nested-pandas. It writes one Parquet
file per partition and a summary table, `flag_incidence.csv`. Most DP2 lightcurve analyses in LSDB do this kind of
per-object reduction. With `--n-partitions 0` it runs over the whole catalog.

**Data.** `/sdf/data/rubin/shared/lsdb_commissioning/hats/dp2_rc/object_collection/object_lc` (needs Rubin data
rights). 8840 partitions, 786M objects, 44.7e9 forced-source measurements (57 per object on average), ~3.6 TB. Each
partition is read in full, so the bytes read grow with the number of partitions.

**Smoke test.** `./run_smoke.sh`: 4 partitions, 4 workers, about 1 minute. It passes if the 4 per-partition outputs
and the summary table are written.

**Full run.**

```bash
python lightcurve_scan.py --n-partitions 0 --workers 16 --memory-limit 24GB --out-dir <dir>
```

This runs 8840 tasks. It can be restarted: partitions that already have an output file are skipped. The largest
partitions hold several times the mean number of measurements, so keep the limit at 24 GB per worker.

**Recorded results.**

- 100 partitions (9.4M objects, 540M measurements): 69 s on 16 workers x 24 GB (sdfiana, 2026-08).
- 4 partitions: 33 s on 4 workers x 6 GB (2026-10-13).

**Origin.** `analysis/rubin/dp2/variability-amplitude/amplitude_scan.py` (2026-08). The partition count, worker
count, memory limit, catalog and output paths were turned into arguments.
