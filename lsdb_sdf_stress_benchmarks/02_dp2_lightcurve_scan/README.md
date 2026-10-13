# 02: DP2 nested lightcurve scan

`lightcurve_scan.py` reads N partitions of the DP2 `object_lc` nested catalog (objects with their forced-source
epochs as list columns), applies the quality cuts and computes per-band amplitudes per object with nested-pandas,
writing one parquet per partition plus `flag_incidence.csv`. It is the per-object-reduction pattern that every DP2
lightcurve analysis in LSDB runs, at the scale of the whole catalog when `--n-partitions 0`.

- **Data:** `/sdf/data/rubin/shared/lsdb_commissioning/hats/dp2_rc/object_collection/object_lc`, 8840 partitions,
  786M objects, 44.7e9 forced-source epochs (mean 57 per object), ~3.6 TB. Partitions are read in full (nested
  columns, no row filter), so bytes read scale with partitions.
- **Baseline:** 100 partitions (9.4M objects, 540M epochs) in 69 s on 16 workers x 24 GB (sdfiana, 2026-08);
  4 partitions in 33 s on 4 workers x 6 GB (2026-10-13).
- **Smoke:** `./run_smoke.sh` (4 partitions, 4 workers, ~1 min). Checks that 4 per-partition outputs and the
  flag table were written.
- **Full run:** `python lightcurve_scan.py --n-partitions 0 --workers 16 --memory-limit 24GB --out-dir <dir>`
  (resumable: existing per-partition outputs are skipped). Expect 8840 tasks; the largest partitions hold several
  times the mean number of epochs, so keep the per-worker limit at 24 GB.
- Origin: `analysis/rubin/dp2/variability-amplitude/amplitude_scan.py` (2026-08), with the partition count,
  worker count, memory limit, catalog and output paths turned into arguments.
