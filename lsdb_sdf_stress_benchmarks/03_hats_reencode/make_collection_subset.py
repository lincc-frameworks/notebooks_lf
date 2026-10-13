"""Cut the first N partitions of a HATS catalog or collection (margin included when present) and write them as a
new HATS collection with lsdb. Two uses: (1) the re-encode script only accepts collections, so this wraps a bare
catalog; (2) it is itself an LSDB write workload (read N partitions, write catalog + margin + metadata).
--n-partitions 0 copies the whole catalog."""
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
    import lsdb
    from dask.distributed import Client, LocalCluster

    t0 = time.time()
    cat = lsdb.open_catalog(args.catalog)
    pixels = cat.get_healpix_pixels()
    if 0 < args.n_partitions < len(pixels):
        cat = cat.pixel_search([(p.order, p.pixel) for p in pixels[: args.n_partitions]])
    n_part = len(cat.get_healpix_pixels())
    with Client(LocalCluster(n_workers=args.workers, threads_per_worker=1, memory_limit=args.memory_limit,
                             local_directory=args.dask_local_dir, dashboard_address=None)):
        cat.write_catalog(args.out, catalog_name=args.name, as_collection=True, overwrite=True)
    print(json.dumps({"result": "make_collection_subset", "source": args.catalog, "out": args.out, "partitions": n_part,
                      "margin": cat.margin is not None, "workers": args.workers, "wall_s": round(time.time() - t0, 1)}), flush=True)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", default="/sdf/data/rubin/cosmicdata/lsdb/gaia_dr3", help="HATS catalog or collection (default: Gaia DR3 collection)")
    p.add_argument("--n-partitions", type=int, default=4, help="partitions to keep (first N of the pixel list); 0 = all")
    p.add_argument("--out", default=f"{default_scratch()}/reencode-src", help="output collection directory")
    p.add_argument("--name", default="subset", help="catalog name inside the collection")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--memory-limit", default="8GB")
    p.add_argument("--dask-local-dir", default=f"{default_scratch()}/dask-reencode")
    return p.parse_args(argv)


if __name__ == "__main__":
    main(parse_args())
