"""DP2: per-band forced-lightcurve amplitudes over N partitions of the nested object_lc catalog.

Benchmark use: every partition read is a full nested read of its forced sources (~5M epochs per partition on
average, 44.7e9 over the 8840 partitions of DP2 RC) followed by per-object reductions in nested-pandas.
--n-partitions 0 scans the whole catalog. Resumable: partitions whose output parquet exists are skipped.

Photometry is `objectForcedSource.psfMag` - forced PSF photometry on the DIRECT image.

Quality cuts: every boolean flag in objectForcedSource must be False, plus S/N > 5 and a finite
magnitude. Each band needs >= N_MIN surviving epochs for its amplitude to be used.

Output: one parquet per partition under OUTDIR/perobj/, plus OUTDIR/flag_incidence.csv.
"""

import json
import os
import time

import numpy as np
import pandas as pd
import pyarrow as pa

import lsdb

DEFAULT_CATALOG = "/sdf/data/rubin/shared/lsdb_commissioning/hats/dp2_rc/object_collection/object_lc"
N_MIN = 5  # minimum surviving epochs in a band for its amplitude to be used
SNR_MIN = 5.0
BANDS = ["u", "g", "r", "i", "z", "y"]

# every boolean field of objectForcedSource - an epoch is dropped if ANY is True
FLAG_COLS = [
    "psfFlux_flag",
    "psfDiffFlux_flag",
    "diff_PixelFlags_nodataCenter",
    "pixelFlags_bad",
    "pixelFlags_cr",
    "pixelFlags_crCenter",
    "pixelFlags_edge",
    "pixelFlags_interpolated",
    "pixelFlags_interpolatedCenter",
    "pixelFlags_nodata",
    "pixelFlags_saturated",
    "pixelFlags_saturatedCenter",
    "pixelFlags_suspect",
    "pixelFlags_suspectCenter",
    "invalidPsfFlag",
    "psfFluxErr_corrected_flag",
    "psfDiffFluxErr_corrected_flag",
]

TOP_COLS = [
    "objectId",
    "coord_ra",
    "coord_dec",
    "refBand",
    "refExtendedness",
    "griz_model_extendedness",
    "ebv",
    "shape_xx",
    "shape_yy",
    "shape_xy",
    "detect_isIsolated",
    "deblend_blendNChild",
    "parentObjectId",
    "i_blendedness",
    "r_inputCount",
    "tract",
    "patch",
    "u_psfMag",
    "g_psfMag",
    "r_psfMag",
    "i_psfMag",
    "z_psfMag",
    "y_psfMag",
    "r_psfMagErr",
]

_BASE_NESTED = [
    "band",
    "psfFlux",
    "psfFluxErr",
    "psfMag",
    "psfMagErr",
    "midpointMjdTai",
]
NESTED_COLS = [f"objectForcedSource.{c}" for c in _BASE_NESTED + FLAG_COLS]

# stat -> (column prefix, dtype, fill) for the full per-band pass
FULL_STATS = {
    "n": ("n", np.int32, 0),
    "amp": ("amp", np.float32, np.nan),
    "amp2": ("ramp", np.float32, np.nan),
    "mmin": ("mmin", np.float32, np.nan),   # brightest epoch used
    "mmax": ("mmax", np.float32, np.nan),   # faintest epoch used
    "sig_rob": ("sig", np.float32, np.nan),
    "med": ("med", np.float32, np.nan),
    "err": ("err", np.float32, np.nan),
    "chi2": ("chi2", np.float32, np.nan),
    "nbright": ("nbr", np.int32, 0),
    "nfaint": ("nft", np.int32, 0),
    "tspan": ("tsp", np.float32, np.nan),
}
# the no-cuts pass only needs a count and a range
NAIVE_STATS = {"n": ("nn", np.int32, 0), "amp": ("namp", np.float32, np.nan)}


# ----------------------------------------------------------------------------
# grouped statistics: groups are contiguous after a lexsort on (mag, key)
# ----------------------------------------------------------------------------
def grouped_stats(key, mag, magerr, mjd):
    """Per-group statistics for integer group labels `key`.

    Every statistic is a segment reduction over one lexsort, which is far cheaper
    than a pandas groupby on the ~4e6 lightcurve points in a partition.
    """
    order = np.lexsort((mag, key))
    key_s = key[order]
    mag_s = mag[order]
    err_s = magerr[order]
    mjd_s = mjd[order]

    uniq, start, cnt = np.unique(key_s, return_index=True, return_counts=True)

    def at(frac):
        return mag_s[start + np.minimum(cnt - 1, np.floor((cnt - 1) * frac + 0.5).astype(np.int64))]

    mmin = mag_s[start]
    mmax = mag_s[start + cnt - 1]
    med = at(0.50)
    # (p84-p16)/2 is a robust sigma that already trims for n>=5, unlike p05/p95
    sig_rob = 0.5 * (at(0.84) - at(0.16))
    # amplitude after discarding the single most extreme epoch at each end:
    # the direct test of "is this driven by one bad point?"
    two = cnt >= 5
    m2min = np.where(two, mag_s[start + np.minimum(1, cnt - 1)], mmin)
    m2max = np.where(two, mag_s[start + np.maximum(cnt - 2, 0)], mmax)

    # weighted-constant-fit chi2, from segment sums
    w = 1.0 / np.maximum(err_s, 1e-4) ** 2
    sw = np.add.reduceat(w, start)
    swm = np.add.reduceat(w * mag_s, start)
    swm2 = np.add.reduceat(w * mag_s * mag_s, start)
    chi2 = swm2 - swm * swm / sw
    chi2red = np.where(cnt > 1, chi2 / np.maximum(cnt - 1, 1), np.nan)

    mean_err = np.add.reduceat(err_s, start) / cnt

    # significant excursions from the per-band median
    med_rep = np.repeat(med, cnt)
    err_rep = np.maximum(np.repeat(mean_err, cnt), 1e-3)
    thr = np.maximum(0.2, 5.0 * err_rep)
    n_bright = np.add.reduceat(((med_rep - mag_s) > thr).astype(np.int32), start)
    n_faint = np.add.reduceat(((mag_s - med_rep) > thr).astype(np.int32), start)

    tmin = np.minimum.reduceat(mjd_s, start)
    tmax = np.maximum.reduceat(mjd_s, start)

    return {
        "uniq": uniq,
        "n": cnt,
        "mmin": mmin,
        "mmax": mmax,
        "amp": mmax - mmin,
        "amp2": m2max - m2min,
        "sig_rob": sig_rob,
        "med": med,
        "err": mean_err,
        "chi2": chi2red,
        "nbright": n_bright,
        "nfaint": n_faint,
        "tspan": tmax - tmin,
    }


def _band_pass(n_obj, key, mag, magerr, mjd, stats, prefix):
    """Scatter grouped statistics into one column per band."""
    cols = {}
    st = grouped_stats(key, mag, magerr, mjd) if key.size else None
    if st is None:
        for _, (name, dt, fill) in stats.items():
            for b in BANDS:
                cols[f"{prefix}{name}_{b}"] = np.full(n_obj, fill, dtype=dt)
        return cols

    obj = st["uniq"] // 8
    bnd = st["uniq"] % 8
    ok = st["n"] >= N_MIN
    for stat, (name, dt, fill) in stats.items():
        arr = st[stat]
        for bi, b in enumerate(BANDS):
            sel = ok & (bnd == bi)
            v = np.full(n_obj, fill, dtype=dt)
            if sel.any():
                v[obj[sel]] = arr[sel].astype(dt)
            cols[f"{prefix}{name}_{b}"] = v
    return cols


def _np(series, dtype):
    return np.asarray(series, dtype=dtype)


def per_partition(df):
    """df: nested_pandas.NestedFrame for one HATS partition."""
    n_obj = len(df)
    out = pd.DataFrame(index=df.index)
    for c in TOP_COLS:
        out[c] = df[c].to_numpy() if hasattr(df[c], "to_numpy") else df[c]

    tally = {"epochs": 0, "flagged": 0}
    for c in FLAG_COLS:
        tally[c] = 0

    if n_obj == 0:
        return _finalize(out), tally

    # The _healpix_29 index is not guaranteed unique, so map lightcurve rows back
    # to objects via the nested list lengths, never via the flat frame's index.
    lengths = df["objectForcedSource"].array.list_lengths
    obj_codes = np.repeat(np.arange(n_obj, dtype=np.int64), lengths)

    flat = df["objectForcedSource"].nest.to_flat()
    if len(flat) == 0:
        return _finalize(out), tally
    assert len(flat) == obj_codes.size

    # dictionary-encode the band strings rather than materialising 3e7 python objects
    barr = pa.array(flat["band"].array)
    if isinstance(barr, pa.ChunkedArray):
        barr = barr.combine_chunks()
    if not pa.types.is_dictionary(barr.type):
        barr = barr.dictionary_encode()
    lut = pd.Index(BANDS).get_indexer(barr.dictionary.to_pylist()).astype(np.int64)
    band_code = lut[barr.indices.to_numpy(zero_copy_only=False)]

    mjd = _np(flat["midpointMjdTai"], np.float64)

    # ---- every boolean flag, OR-ed together ----
    anyflag = np.zeros(len(flat), dtype=bool)
    tally["epochs"] = int(len(flat))
    for c in FLAG_COLS:
        v = _np(flat[c], bool)
        tally[c] = int(v.sum())
        anyflag |= v
    tally["flagged"] = int(anyflag.sum())

    # ---- direct-image forced photometry ----
    mag = _np(flat["psfMag"], np.float64)
    magerr = _np(flat["psfMagErr"], np.float64)
    with np.errstate(invalid="ignore", divide="ignore"):
        snr = _np(flat["psfFlux"], np.float64) / _np(flat["psfFluxErr"], np.float64)

    finite = np.isfinite(mag) & (band_code >= 0)
    clean = finite & ~anyflag & np.isfinite(magerr) & (snr > SNR_MIN)

    key_all = obj_codes * 8 + band_code
    out["n_naive"] = _fill_count(n_obj, obj_codes[finite])
    out["n_clean"] = _fill_count(n_obj, obj_codes[clean])
    for k, v in _band_pass(
        n_obj, key_all[finite], mag[finite], magerr[finite], mjd[finite], NAIVE_STATS, ""
    ).items():
        out[k] = v
    for k, v in _band_pass(
        n_obj, key_all[clean], mag[clean], magerr[clean], mjd[clean], FULL_STATS, ""
    ).items():
        out[k] = v

    out["n_epochs"] = _fill_count(n_obj, obj_codes)
    out["n_flagged"] = _fill_count(n_obj, obj_codes[anyflag])
    return _finalize(out), tally


def _fill_count(n_obj, codes):
    c = np.zeros(n_obj, dtype=np.int32)
    if codes.size:
        u, n = np.unique(codes, return_counts=True)
        c[u] = n
    return c


def _meta_extra():
    m = {"n_naive": np.int32, "n_clean": np.int32, "n_epochs": np.int32, "n_flagged": np.int32}
    for stats in (NAIVE_STATS, FULL_STATS):
        for _, (name, dt, _f) in stats.items():
            for b in BANDS:
                m[f"{name}_{b}"] = dt
    return m


META_EXTRA = _meta_extra()


def _finalize(out):
    """Fill any column the aggregation did not produce (e.g. a partition with no
    lightcurve points) so every output file has an identical schema."""
    n = len(out)
    for c, dt in META_EXTRA.items():
        if c not in out.columns:
            fill = 0 if np.issubdtype(np.dtype(dt), np.integer) else np.nan
            out[c] = np.full(n, fill, dtype=dt)
        else:
            out[c] = out[c].astype(dt)
    return out


def main(args):
    import logging
    import warnings

    warnings.filterwarnings("ignore")
    logging.getLogger().setLevel(logging.WARNING)
    logging.getLogger("distributed").setLevel(logging.ERROR)

    import dask
    from dask.distributed import Client, LocalCluster

    t0 = time.time()
    out_dir = args.out_dir
    os.makedirs(f"{out_dir}/perobj", exist_ok=True)

    pixels = lsdb.open_catalog(args.catalog).get_healpix_pixels()
    print(f"catalog has {len(pixels)} partitions", flush=True)

    # evenly spaced across the ordered pixel list -> spread over the footprint
    if args.n_partitions <= 0 or args.n_partitions >= len(pixels):
        chosen = list(pixels)
    else:
        sel = np.unique(np.linspace(0, len(pixels) - 1, args.n_partitions).round().astype(int))
        chosen = [pixels[i] for i in sel]
    print(f"selected {len(chosen)} partitions", flush=True)
    pd.DataFrame(
        {"order": [p.order for p in chosen], "pixel": [p.pixel for p in chosen]}
    ).to_csv(f"{out_dir}/selected_partitions.csv", index=False)

    cat = lsdb.open_catalog(args.catalog, columns=TOP_COLS + NESTED_COLS)
    cat = cat.pixel_search([(p.order, p.pixel) for p in chosen])
    print(f"after pixel_search: {len(cat.get_healpix_pixels())} partitions", flush=True)

    cluster = LocalCluster(
        n_workers=args.workers,
        threads_per_worker=1,
        memory_limit=args.memory_limit,
        local_directory=args.dask_local_dir,
        dashboard_address=None,
    )
    client = Client(cluster)
    print(client, flush=True)

    @dask.delayed
    def run_one(part, order, pixel):
        res, tally = per_partition(part)
        res = res.reset_index(drop=False)
        res.to_parquet(f"{out_dir}/perobj/Norder{order}_Npix{pixel}.parquet", index=False)
        return {"order": order, "pixel": pixel, "rows": len(res), **tally}

    tasks = [
        run_one(d, p.order, p.pixel)
        for d, p in zip(cat.to_delayed(), cat.get_healpix_pixels())
        if not os.path.exists(f"{out_dir}/perobj/Norder{p.order}_Npix{p.pixel}.parquet")
    ]
    print(f"{len(tasks)} partitions still to do", flush=True)
    results = list(dask.compute(*tasks))
    if results:
        tal = pd.DataFrame(results)
        path = f"{out_dir}/flag_incidence.csv"
        if os.path.exists(path):
            tal = pd.concat([pd.read_csv(path), tal], ignore_index=True)
        tal.to_csv(path, index=False)
        print(f"wrote {len(results)} partitions, {tal['rows'].sum():,} objects", flush=True)
    print(f"elapsed {time.time() - t0:.0f}s", flush=True)
    print(json.dumps({"result": "lightcurve_scan", "catalog": args.catalog, "partitions_total": len(pixels),
                      "partitions_scanned": len(tasks), "objects": int(tal["rows"].sum()) if results else 0,
                      "workers": args.workers, "memory_limit": args.memory_limit, "wall_s": round(time.time() - t0, 1)}), flush=True)
    client.close()
    cluster.close()


def default_scratch() -> str:
    user = os.environ.get("USER", "user")
    if os.environ.get("BENCH_SCRATCH"):
        return os.environ["BENCH_SCRATCH"]
    if os.path.isdir("/lscratch"):
        return f"/lscratch/{user}/lsdb-bench"
    return (os.environ.get("SCRATCH") or f"/sdf/scratch/users/{user[:1]}/{user}") + "/lsdb-bench"


def parse_args(argv=None):
    import argparse

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", default=DEFAULT_CATALOG, help="nested HATS catalog with objectForcedSource (default: DP2 RC object_lc)")
    p.add_argument("--n-partitions", type=int, default=100, help="partitions to scan, spread over the footprint; 0 = all (default 100)")
    p.add_argument("--workers", type=int, default=16, help="Dask workers, 1 thread each (default 16)")
    p.add_argument("--memory-limit", default="24GB", help="per-worker memory limit (default 24GB)")
    p.add_argument("--out-dir", default=f"{default_scratch()}/lightcurve_scan", help="per-partition parquet + flag_incidence.csv")
    p.add_argument("--dask-local-dir", default=f"{default_scratch()}/dask-lightcurve-scan")
    return p.parse_args(argv)


if __name__ == "__main__":
    main(parse_args())
