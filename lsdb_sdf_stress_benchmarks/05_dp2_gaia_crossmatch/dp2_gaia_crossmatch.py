"""Crossmatch N partitions of the DP2 object collection (margin included) against Gaia DR3 with LSDB.

--n-partitions 0 is the full catalog: 8840 DP2 partitions, 9139 output partitions in the July 2026 run
(48 workers x 4 GB on sdfiana035: ~250 s, of which ~90 s was the client-side Dask graph materialization before
any task ran; scheduler and dashboard struggle above ~100k partitions). Smaller N spreads the partitions over the
footprint. Output: one JSON line with rows read, matches and timings.
"""
import argparse
import json
import os
import time


def default_scratch() -> str:
    user = os.environ.get("USER", "user")
    if os.environ.get("BENCH_SCRATCH"):
        return os.environ["BENCH_SCRATCH"]
    if os.path.isdir("/lscratch"):
        return f"/lscratch/{user}/lsdb-bench"
    return (os.environ.get("SCRATCH") or f"/sdf/scratch/users/{user[:1]}/{user}") + "/lsdb-bench"


def main(args):
    import logging
    import warnings

    warnings.filterwarnings("ignore")
    logging.getLogger().setLevel(logging.WARNING)
    import numpy as np
    import lsdb
    from dask.distributed import Client, LocalCluster

    t0 = time.time()
    left = lsdb.open_catalog(args.left)
    right = lsdb.open_catalog(args.right, columns=[c for c in args.right_columns.split(",") if c])
    pixels = left.get_healpix_pixels()
    if 0 < args.n_partitions < len(pixels):
        sel = np.unique(np.linspace(0, len(pixels) - 1, args.n_partitions).round().astype(int))
        left = left.pixel_search([(pixels[i].order, pixels[i].pixel) for i in sel])
    t_open = time.time() - t0
    xm = left.crossmatch(right, radius_arcsec=args.radius_arcsec, n_neighbors=1, suffixes=("_left", "_right"))
    n_out = len(xm.get_healpix_pixels())
    t_graph = time.time() - t0 - t_open
    with Client(LocalCluster(n_workers=args.workers, threads_per_worker=1, memory_limit=args.memory_limit,
                             local_directory=args.dask_local_dir, dashboard_address=None)):
        t1 = time.time()
        if args.output == "write":
            out = os.path.join(args.out_dir, "dp2_gaia_xmatch")
            xm.write_catalog(out, catalog_name="dp2_gaia_xmatch", overwrite=True)
            matched = int(lsdb.open_catalog(out).map_partitions(len).compute().sum())
        else:
            matched = int(xm.map_partitions(len).compute().sum())
        t_compute = time.time() - t1
    print(json.dumps({"result": "dp2_gaia_crossmatch", "left": args.left, "right": args.right,
                      "left_partitions": len(left.get_healpix_pixels()), "left_partitions_total": len(pixels),
                      "output_partitions": n_out, "left_margin": left.margin is not None, "right_margin": right.margin is not None,
                      "radius_arcsec": args.radius_arcsec, "matched": matched, "output": args.output, "workers": args.workers,
                      "memory_limit": args.memory_limit, "open_s": round(t_open, 1), "graph_s": round(t_graph, 1),
                      "compute_s": round(t_compute, 1), "wall_s": round(time.time() - t0, 1)}), flush=True)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--left", default="/sdf/data/rubin/shared/lsdb_commissioning/hats/dp2_rc/object_collection",
                   help="left HATS collection (default: DP2 RC object collection, with margin)")
    p.add_argument("--right", default="/sdf/data/rubin/cosmicdata/lsdb/gaia_dr3", help="right HATS collection (default: Gaia DR3)")
    p.add_argument("--right-columns", default="source_id,ra,dec,phot_g_mean_mag",
                   help="columns of the right catalog (Gaia on SDF has no default column list; all 153 would load)")
    p.add_argument("--n-partitions", type=int, default=8, help="left partitions, spread over the footprint; 0 = all")
    p.add_argument("--radius-arcsec", type=float, default=1.0)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--memory-limit", default="8GB")
    p.add_argument("--output", choices=["count", "write"], default="count",
                   help="count: rows of the crossmatch in memory; write: write_catalog to --out-dir then count")
    p.add_argument("--out-dir", default=f"{default_scratch()}/dp2_gaia_crossmatch")
    p.add_argument("--dask-local-dir", default=f"{default_scratch()}/dask-dp2-gaia")
    return p.parse_args(argv)


if __name__ == "__main__":
    main(parse_args())
