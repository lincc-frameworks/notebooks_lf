"""Read-path comparison on one HATS catalog: lsdb vs nested-pandas vs pyarrow, imports EXCLUDED from the clock.

Every variant reads the SAME four columns (--column, ra, dec, _healpix_29) from the same N partitions and reduces
to the mean of --column, so the only difference is the read machinery. One warm pass (not timed), then --reps
interleaved timed passes. Bytes and read syscalls come from /proc/self/io (rchar, syscr), which is why the
variants run in-process threads rather than Dask worker processes. Output: one JSON line per (rep, variant).

Cold-cache note (weka at /sdf/data): posix_fadvise(DONTNEED) does not evict; "cold" means partitions no process
on the node has touched yet. For a cold number pick fresh partitions (--offset) and set --reps 1 --no-warm.

Derived from analysis for lsdb#1010 (2026-09): on the full Gaia DR3 mirror (2016 partitions, 713 GB) the 1-column
scan took 320 s through lsdb vs 88 s through pyarrow; with the same 4 columns, 192 partitions, the gap was
4.0 s vs 2.7 s (the difference is per-partition Dask overhead of 6-8 ms plus fsspec read calls)."""
import argparse
import json
import sys
import time
import warnings
import logging
from concurrent.futures import ThreadPoolExecutor

logging.disable(logging.WARNING)
warnings.filterwarnings("ignore")

# ---- imports (NOT timed) --------------------------------------------------
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import nested_pandas as npd
import dask
import lsdb

DEFAULT_ROOT = "/sdf/data/rubin/cosmicdata/lsdb/gaia_dr3/gaia"


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--root", default=DEFAULT_ROOT, help="HATS catalog directory (not a collection); default Gaia DR3")
    p.add_argument("--column", default="phot_g_mean_mag", help="numeric column to average")
    p.add_argument("--n-partitions", type=int, default=16, help="partitions to read, taken in pixel order; 0 = all")
    p.add_argument("--offset", type=int, default=0, help="skip the first OFFSET partitions (pick untouched ones)")
    p.add_argument("--threads", type=int, default=4, help="reader threads / Dask threaded-scheduler workers")
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--no-warm", action="store_true", help="skip the untimed warm pass (cold-cache measurement)")
    p.add_argument("--variants", default="pa_arrow,pa_pandas_numpy,pa_pandas_arrow,npd_direct,lsdb")
    return p.parse_args(argv)


ARGS = parse_args()
COLS4 = [ARGS.column, "ra", "dec", "_healpix_29"]
pa.set_cpu_count(ARGS.threads)
_cat = lsdb.open_catalog(ARGS.root)
_pix = _cat.get_healpix_pixels()[ARGS.offset:]
if ARGS.n_partitions > 0:
    _pix = _pix[: ARGS.n_partitions]
PIXELS = [(p.order, p.pixel) for p in _pix]
PATHS = [f"{ARGS.root}/dataset/Norder={o}/Dir={(p // 10000) * 10000}/Npix={p}.parquet" for o, p in PIXELS]
FIELD = ARGS.column


def io():
    d = {}
    for line in open("/proc/self/io"):
        k, v = line.split(":")
        d[k.strip()] = int(v)
    return d


def _mean_of(sums_counts):
    tot, cnt = 0.0, 0
    for s, c in sums_counts:
        tot += s
        cnt += c
    return tot / cnt


def _pool_map(fn):
    with ThreadPoolExecutor(ARGS.threads) as ex:
        return _mean_of(ex.map(fn, PATHS))


# ---- variants -------------------------------------------------------------
def pa_arrow():
    """pyarrow -> arrow Table, reduce in arrow. No pandas at all."""
    def one(p):
        t = pq.read_table(p, columns=COLS4, use_threads=False)
        c = t.column(FIELD)
        return (pc.sum(c).as_py() or 0.0), pc.count(c, mode="only_valid").as_py()
    return _pool_map(one)


def pa_pandas_numpy():
    """pyarrow -> pandas with numpy dtypes (the usual .to_pandas())."""
    def one(p):
        c = pq.read_table(p, columns=COLS4, use_threads=False).to_pandas()[FIELD]
        return float(c.sum()), int(c.count())
    return _pool_map(one)


def pa_pandas_arrow():
    """pyarrow -> pandas with ArrowDtype: the materialization lsdb ends up with."""
    def one(p):
        c = pq.read_table(p, columns=COLS4, use_threads=False).to_pandas(types_mapper=pd.ArrowDtype)[FIELD]
        return float(c.sum()), int(c.count())
    return _pool_map(one)


def npd_direct():
    """nested-pandas read_parquet per file: lsdb's actual reader, without dask."""
    def one(p):
        c = npd.read_parquet(p, columns=COLS4)[FIELD]
        return float(c.sum()), int(c.count())
    return _pool_map(one)


def lsdb_compute():
    """Full lsdb path: open_catalog + PixelSearch + map_partitions + compute (threaded scheduler, in-process)."""
    cat = lsdb.open_catalog(ARGS.root, columns=[FIELD], search_filter=lsdb.PixelSearch(PIXELS))

    def part_mean(df, target_column=""):
        c = df[target_column]
        return pd.DataFrame([{"s": float(c.sum()), "n": int(c.count())}])

    meta = pd.DataFrame({"s": pd.Series(dtype="float64"), "n": pd.Series(dtype="int64")})
    with dask.config.set({"num_workers": ARGS.threads}):
        res = cat.map_partitions(part_mean, target_column=FIELD, meta=meta).compute(progress_bar=False)
    return float(res["s"].sum()) / int(res["n"].sum())


ALL = {"pa_arrow": pa_arrow, "pa_pandas_numpy": pa_pandas_numpy, "pa_pandas_arrow": pa_pandas_arrow,
       "npd_direct": npd_direct, "lsdb": lsdb_compute}
VARIANTS = [(n, ALL[n]) for n in ARGS.variants.split(",") if n]

print(json.dumps({"settings": True, "root": ARGS.root, "column": FIELD, "npart": len(PATHS), "threads": ARGS.threads,
                  "reps": ARGS.reps, "warm": not ARGS.no_warm, "lsdb": lsdb.__version__, "pyarrow": pa.__version__}), flush=True)
if not ARGS.no_warm:
    for _, fn in VARIANTS:
        fn()
for rep in range(1, ARGS.reps + 1):
    for name, fn in VARIANTS:
        a, t = io(), time.perf_counter()
        val = fn()
        dt = time.perf_counter() - t
        b = io()
        print(json.dumps({"rep": rep, "variant": name, "npart": len(PATHS), "wall_s": round(dt, 3),
                          "rchar_MB": round((b["rchar"] - a["rchar"]) / 1e6, 1), "syscr": b["syscr"] - a["syscr"],
                          "mean": round(val, 6)}), flush=True)
