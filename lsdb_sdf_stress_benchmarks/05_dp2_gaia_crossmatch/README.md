# 05: DP2 x Gaia crossmatch (graph-size scaling)

`dp2_gaia_crossmatch.py` crossmatches N partitions of the DP2 object collection (with its margin cache) against
Gaia DR3 (with its margin) through `lsdb.crossmatch` and counts, or writes, the result. At `--n-partitions 0` it is
the full-catalog run of July 2026: 8840 DP2 partitions, 9139 output partitions, 48 workers x 4 GB, about 250 s on
sdfiana035, of which ~90 s was the client building the Dask graph before any task ran. That client-side cost grows
with partition count, and the scheduler and dashboard stall above ~100k partitions, which is what this workload is
for: how many partitions a node's scheduler process handles before graph construction dominates.

- **Data:** `/sdf/data/rubin/shared/lsdb_commissioning/hats/dp2_rc/object_collection` (Rubin data rights) and
  `/sdf/data/rubin/cosmicdata/lsdb/gaia_dr3` (4 columns; Gaia on SDF has no default column list).
- **Baseline:** 8 partitions (530k DP2 rows): 62,923 matches within 1 arcsec, 29 s of compute on 4 workers,
  68 s wall (2026-10-13). Full catalog: see above.
- **Smoke:** `./run_smoke.sh` (8 partitions, 4 workers, count mode, ~1.5 min; passes when matches > 0).
- **Full run:** `python dp2_gaia_crossmatch.py --n-partitions 0 --workers 48 --memory-limit 4GB`; add
  `--output write` to also write the 9139-partition result catalog to `$BENCH_SCRATCH`. The JSON line reports
  open, graph and compute times separately.
- Origin: `analysis/rubin/dp2/notebook-run/dp2_xmatch_PR1494/full_dp2_crossmatches*.ipynb` (July 2026), reduced to
  a script with a partition-count argument.
