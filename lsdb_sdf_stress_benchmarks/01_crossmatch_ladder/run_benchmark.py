#!/usr/bin/env python
"""Crossmatch performance benchmark: LSDB vs astropy vs smatch (2026 rerun on SLAC SDF).

Derived from ../../lsdb_crossmatch_benchmarking/run_benchmark.py (Sean McGuire, Feb 2025, PSC Bridges-2).
Same design: nearest-neighbour crossmatch of a ZTF objects table against Gaia DR3 within 10 arcsec,
row count scaled through a ladder of sky selections, each tool timed with time.monotonic(), several
repeats, median with 10th/90th percentiles in the plot.

Changes vs 2025 (see README.md for the full list):
* SDF paths; ZTF DR23 objects instead of ZTF DR14 (not mirrored on SDF).
* Explicit column lists (Gaia on SDF has no hats_cols_default).
* Ladder = cone steps inside the smallest ZTF partition + the k partitions nearest to it.
* Fixes: smatch concat axis, single right-hand frame for astropy, arrow dtypes -> float64,
  lsdb 0.10 API (open_catalog, PixelSearch, progress_bar=False, no _ddf).
* Resource envelope for a shared interactive node: CPU set from taskset/affinity, Dask memory
  limits, astropy/smatch run in a child process with an RSS watchdog.
* One JSON per (config, step, repeat); existing files are skipped so a killed run resumes.
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import math
import os
import shutil
import platform
import socket
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path

import numpy as np
import pandas as pd

# SDF mirrors (world-readable). Override with --ztf-path/--gaia-path or XMATCH_ZTF_PATH/XMATCH_GAIA_PATH.
ZTF_PATH = os.environ.get("XMATCH_ZTF_PATH", "/sdf/data/rubin/cosmicdata/lsdb/ztf_dr23/objects")
GAIA_PATH = os.environ.get("XMATCH_GAIA_PATH", "/sdf/data/rubin/cosmicdata/lsdb/gaia_dr3")


def default_out_dir() -> str:
    """Where --output write puts catalogs: $XMATCH_OUT_DIR, else the Rubin user data disk if the user has one,
    else $SCRATCH, else the SDF per-user scratch area."""
    if os.environ.get("XMATCH_OUT_DIR"):
        return os.environ["XMATCH_OUT_DIR"]
    user = os.environ.get("USER", "user")
    rubin_user = f"/sdf/data/rubin/user/{user}"
    if os.path.isdir(rubin_user):
        return f"{rubin_user}/xmatch-bench-out"
    scratch = os.environ.get("SCRATCH") or f"/sdf/scratch/users/{user[:1]}/{user}"
    return f"{scratch}/xmatch-bench-out"


def default_dask_local_dir() -> str:
    """Dask spill/temp dir: node-local /lscratch when the node has one (sdfiana*, milano), else under the output dir's parent."""
    user = os.environ.get("USER", "user")
    if os.path.isdir("/lscratch"):
        return f"/lscratch/{user}/dask-xmatch-bench"
    return str(Path(default_out_dir()).parent / "dask-xmatch-bench")

# ZTF DR23 objects hats_cols_default (6 columns). ZTF DR14 in 2025 had 15 default columns.
ZTF_DEFAULT_COLS = ["oid", "ra", "dec", "filtercode", "medianmag", "medmagerr"]
# 27 Gaia columns reconstructing the 2025 default set (astrometry + photometry + GSP-Phot).
GAIA_DEFAULT_COLS = [
    "source_id", "ra", "ra_error", "dec", "dec_error",
    "parallax", "parallax_error", "pmra", "pmra_error", "pmdec", "pmdec_error", "ruwe",
    "phot_g_mean_flux", "phot_g_mean_mag", "phot_bp_mean_flux", "phot_bp_mean_mag",
    "phot_rp_mean_flux", "phot_rp_mean_mag", "bp_rp",
    "radial_velocity", "radial_velocity_error", "phot_variable_flag", "l", "b",
    "teff_gspphot", "logg_gspphot", "distance_gspphot",
]
RADEC_COLS = ["ra", "dec"]
SPATIAL_INDEX_COLUMN = "_healpix_29"  # hats.pixel_math.spatial_index.SPATIAL_INDEX_COLUMN; hardcoded so timed code imports nothing
XMATCH_RADIUS_ARCSEC = 10.0
XMATCH_N_NEIGHBORS = 1

log = logging.getLogger("bench")


# ----------------------------------------------------------------------------- timing helpers
@dataclass
class Measurement:
    time: float
    result: object


def measure(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        t = time.monotonic()
        result = func(*args, **kwargs)
        return Measurement(time=time.monotonic() - t, result=result)

    return wrapper


def result_length(result):
    """Row count of a measurement result; a frame with a single 'len' column is a per-partition count."""
    if isinstance(result, (int, np.integer)):
        return int(result)
    try:
        if list(result.columns) == ["len"]:
            return int(result["len"].sum())
        return int(len(result))
    except AttributeError:
        return None


def to_report(m: Measurement) -> dict:
    return {"time": m.time, "result_length": result_length(m.result)}


# ----------------------------------------------------------------------------- resource envelope
def allowed_cpus() -> list[int]:
    return sorted(os.sched_getaffinity(0))


def rss_tree_bytes(pid: int) -> int:
    import psutil

    try:
        p = psutil.Process(pid)
        procs = [p] + p.children(recursive=True)
        return sum(q.memory_info().rss for q in procs if q.is_running())
    except psutil.Error:
        return 0


# ----------------------------------------------------------------------------- ladder geometry
def ztf_partition_table(ztf_cat) -> pd.DataFrame:
    """Partition table of the ZTF main catalog with pixel centres (deg)."""
    import cdshealpix

    base = Path(str(ztf_cat.hc_structure.catalog_path))
    pi = pd.read_csv(base / "partition_info.csv")
    ra = np.empty(len(pi))
    dec = np.empty(len(pi))
    for order, grp in pi.groupby("Norder"):
        lon, lat = cdshealpix.nested.healpix_to_lonlat(grp["Npix"].to_numpy(dtype=np.uint64), int(order))
        ra[grp.index] = lon.deg
        dec[grp.index] = lat.deg
    pi["ra_c"] = ra
    pi["dec_c"] = dec
    return pi


def angular_sep_deg(ra1, dec1, ra2, dec2):
    ra1, dec1, ra2, dec2 = (np.radians(np.asarray(x, dtype=float)) for x in (ra1, dec1, ra2, dec2))
    sdlon = np.sin(ra2 - ra1)
    cdlon = np.cos(ra2 - ra1)
    num1 = np.cos(dec2) * sdlon
    num2 = np.cos(dec1) * np.sin(dec2) - np.sin(dec1) * np.cos(dec2) * cdlon
    den = np.sin(dec1) * np.sin(dec2) + np.cos(dec1) * np.cos(dec2) * cdlon
    return np.degrees(np.arctan2(np.hypot(num1, num2), den))


def ladder_geometry(ztf_cat, min_dec: float = -10.0, min_abs_b: float = 30.0, seed_override: tuple[int, int] | None = None,
                    rows_band: tuple[float, float] | None = None, seed_radec: tuple[float, float] | None = None):
    """Seed = smallest ZTF partition well inside the footprint (centre Dec > min_dec, |b| > min_abs_b);
    all partitions ordered by distance of their centre to the seed centre.

    With `rows_band=(lo, hi)` only partitions with lo <= num_rows <= hi take part (seed and ladder), and the
    seed is the qualifying partition closest to the band's geometric centre. ZTF DR23 partitions span 1.3-12.6M
    rows; without the band the k-nearest ladder picks up 10M-row Order 3 partitions among ~3M-row Order 4 ones
    and the wall time of a parallel run is set by that single largest task (visible as a kink at k=8..16).

    The globally smallest partitions sit at the southern edge of ZTF coverage (Dec ~ -30), where the
    pixel centre has no data, so cones there are empty; hence the footprint cuts.
    """
    from astropy.coordinates import SkyCoord

    pi = ztf_partition_table(ztf_cat)
    pi["b"] = SkyCoord(pi["ra_c"].to_numpy(), pi["dec_c"].to_numpy(), unit="deg").galactic.b.deg
    if rows_band is not None:
        lo, hi = rows_band
        pi = pi[(pi["num_rows"] >= lo) & (pi["num_rows"] <= hi)].reset_index(drop=True)
    if seed_radec is not None:
        # cone centre given directly; the "seed partition" is the one whose centre is nearest (informational)
        seed = pi.iloc[int(np.argmin(angular_sep_deg(pi["ra_c"], pi["dec_c"], seed_radec[0], seed_radec[1])))]
    elif seed_override is not None:
        seed = pi[(pi["Norder"] == seed_override[0]) & (pi["Npix"] == seed_override[1])].iloc[0]
    else:
        ok = pi[(pi["dec_c"] > min_dec) & (pi["b"].abs() > min_abs_b)]
        if rows_band is None:
            seed = ok.sort_values("num_rows").iloc[0]
        else:
            seed = ok.iloc[int((ok["num_rows"] - math.sqrt(lo * hi)).abs().to_numpy().argmin())]
    ra_s, dec_s = seed_radec if seed_radec is not None else (float(seed["ra_c"]), float(seed["dec_c"]))
    pi["sep_deg"] = angular_sep_deg(pi["ra_c"], pi["dec_c"], ra_s, dec_s)
    near = pi.sort_values(["sep_deg", "Norder", "Npix"]).reset_index(drop=True)
    near["cum_rows"] = near["num_rows"].cumsum()
    return {"ra": float(ra_s), "dec": float(dec_s), "order": int(seed["Norder"]),
            "pixel": int(seed["Npix"]), "rows": int(seed["num_rows"])}, near


def parse_step(step: str):
    kind, _, val = step.partition(":")
    if kind == "cone":
        return "cone", float(val)
    if kind == "pixels":
        return "pixels", int(val)
    raise ValueError(f"bad step {step!r}; expected cone:<arcsec> or pixels:<k>")


def select_left(ztf_full, step: str, seed: dict, near: pd.DataFrame):
    from hats.pixel_math import HealpixPixel
    from lsdb import ConeSearch, PixelSearch

    kind, val = parse_step(step)
    if kind == "cone":
        return ztf_full.search(ConeSearch(seed["ra"], seed["dec"], val, fine=True))
    if val > len(near):
        raise ValueError(f"{step}: only {len(near)} partitions qualify (rows band / footprint); cannot take {val}")
    rows = near.iloc[:val]
    pixels = [HealpixPixel(int(o), int(p)) for o, p in zip(rows["Norder"], rows["Npix"])]
    return ztf_full.search(PixelSearch(pixels))


def select_right(gaia_full, ztf_sel):
    from lsdb import PixelSearch

    return gaia_full.search(PixelSearch(ztf_sel.get_healpix_pixels()))


# ----------------------------------------------------------------------------- pyarrow loading
def pixel_files(cat, columns_hint=None) -> list[str]:
    """Parquet files of every partition of `cat`, expanding directory-style pixels (npix_suffix='/')."""
    import hats

    base = cat.hc_structure.catalog_path
    suffix = cat.hc_structure.catalog_info.npix_suffix
    files: list[str] = []
    for pix in cat.get_healpix_pixels():
        p = hats.io.paths.pixel_catalog_file(base, pix, npix_suffix=suffix)
        p = Path(str(p))
        if p.is_dir():
            files.extend(sorted(str(f) for f in p.glob("*.parquet")))
        else:
            files.append(str(p))
    return files


@measure
def load_parquet(files: list[str], columns: list[str]) -> pd.DataFrame:
    cols = list(columns) + [SPATIAL_INDEX_COLUMN]
    return pd.read_parquet(files, engine="pyarrow", columns=cols).set_index(SPATIAL_INDEX_COLUMN)


@measure
def cone_filter(df: pd.DataFrame, ra0: float, dec0: float, radius_arcsec: float) -> pd.DataFrame:
    sep = angular_sep_deg(df["ra"].to_numpy(dtype=float), df["dec"].to_numpy(dtype=float), ra0, dec0)
    return df[sep <= radius_arcsec / 3600.0]


def count_left_rows(ztf_files: list[str], cone: tuple | None, ztf_sel, near: pd.DataFrame) -> int:
    """Rows of the left selection, untimed. Cone steps: read ra/dec of every file the cone touches and apply the
    same cone_filter the sequential tools use. Pixel steps: sum of partition_info rows of the selected pixels."""
    if cone is None:
        pix = {(p.order, p.pixel) for p in ztf_sel.get_healpix_pixels()}
        keep = [(o, p) in pix for o, p in zip(near["Norder"], near["Npix"])]
        return int(near.loc[keep, "num_rows"].sum())
    return int(sum(len(cone_filter(load_parquet([f], RADEC_COLS).result, *cone).result) for f in ztf_files))


def count_rows_read(ztf_sel, pi_rows: pd.Series) -> int:
    """Rows of every left partition the selection touches (what each tool actually decodes), from partition_info."""
    return int(sum(int(pi_rows[(p.order, p.pixel)]) for p in ztf_sel.get_healpix_pixels()))


@measure
def write_parquet(df: pd.DataFrame, path) -> pd.DataFrame:
    """The joined result written once (native mode); returns df so result_length is the row count."""
    df.to_parquet(path, engine="pyarrow", index=False)
    return df


@measure
def merge_right(gaia_df: pd.DataFrame, margin_df: pd.DataFrame) -> pd.DataFrame:
    """One right-hand frame = Gaia partitions + their margins, deduplicated on the spatial index."""
    both = pd.concat([gaia_df, margin_df])
    return both[~both.index.duplicated(keep="first")]


# ----------------------------------------------------------------------------- the matchers
@measure
def run_lsdb(left, right, *, radius_arcsec, n_neighbors):
    xm = left.crossmatch(
        right, radius_arcsec=radius_arcsec, n_neighbors=n_neighbors, suffixes=("_ztf", "_gaia"), suffix_method="all_columns"
    )
    counts = xm.map_partitions(
        lambda df: pd.DataFrame({"len": [len(df)]}), meta=pd.DataFrame({"len": pd.Series(dtype="int64")})
    )
    return counts.compute(progress_bar=False)


@measure
def run_lsdb_write(left, right, out_path, *, radius_arcsec, n_neighbors):
    """Crossmatch and write the result as a HATS catalog; returns the row count read back from the written catalog.
    Nothing but per-partition counts and histograms comes back to the client, so memory is bounded per task."""
    import hats

    xm = left.crossmatch(
        right, radius_arcsec=radius_arcsec, n_neighbors=n_neighbors, suffixes=("_ztf", "_gaia"), suffix_method="all_columns"
    )
    xm.write_catalog(out_path, catalog_name="xmatch", as_collection=False, overwrite=True, progress_bar=False, error_if_empty=False)
    return int(hats.read_hats(out_path).catalog_info.total_rows)


@measure
def lsdb_io(cat):
    counts = cat.map_partitions(
        lambda df: pd.DataFrame({"len": [len(df)]}), meta=pd.DataFrame({"len": pd.Series(dtype="int64")})
    )
    return counts.compute(progress_bar=False)


@measure
def dfs_to_skycoords(left_df, right_df):
    from astropy.coordinates import SkyCoord

    return (
        SkyCoord(ra=left_df["ra"].to_numpy(dtype=float), dec=left_df["dec"].to_numpy(dtype=float), unit="deg"),
        SkyCoord(ra=right_df["ra"].to_numpy(dtype=float), dec=right_df["dec"].to_numpy(dtype=float), unit="deg"),
    )


@measure
def run_astropy(left_df, right_df, left_coord, right_coord, *, radius_arcsec, n_neighbors):
    from astropy.coordinates import Angle, match_coordinates_sky

    assert n_neighbors == 1
    idx_right, d2, _ = match_coordinates_sky(left_coord, right_coord, nthneighbor=1)
    result = pd.concat(
        [
            left_df.reset_index(drop=True),
            right_df.iloc[idx_right].reset_index(drop=True).add_suffix("_gaia"),
            pd.DataFrame({"_dist_arcsec": d2.to_value("arcsec")}),
        ],
        axis=1,
    )
    return result[d2 < Angle(radius_arcsec, "arcsec")]


@measure
def run_smatch(left_df, right_df, *, radius_arcsec, n_neighbors, orientation="right", shuffle=False):
    """Nearest right-hand neighbour of every left row within radius, with smatch.

    shuffle=True passes smatch's SECOND argument (the side it puts into its HEALPix tree) in random order and maps the
        returned indices back. smatch 0.10.2 builds an unbalanced binary search tree keyed by nside-4096 pixel from that
        argument (smatch.c create_hpix_tree / tree.c tree_insert); HATS files arrive sorted by _healpix_29, so the tree
        degenerates into one chain of length = distinct pixels (554K for the seed Gaia frame) and every insert and lookup
        walks it. The permutation is inside the timed region: it is part of what a user must do.

    orientation="left"  (the 2025 call form): smatch.match(left, radius, right, maxmatch=1); the left rows are
        the indexed catalog. On spatially sorted (HATS) inputs this hits a slow path in smatch when most right
        points fall outside every left disc (measured: 20 s vs 1.6 s for the same points shuffled).
    orientation="right" (default): the right catalog is indexed, all pairs within radius are returned
        (maxmatch=0) and the closest right row per left row is kept with a lexsort. Identical pairs, no slow path.
    """
    import smatch

    assert n_neighbors == 1
    lra, ldec = left_df["ra"].to_numpy(dtype=float), left_df["dec"].to_numpy(dtype=float)
    rra, rdec = right_df["ra"].to_numpy(dtype=float), right_df["dec"].to_numpy(dtype=float)
    radius_deg = radius_arcsec / 3600.0
    rng = np.random.default_rng(0)
    if orientation == "left":
        if shuffle:
            perm = rng.permutation(len(rra))
            m = smatch.match(lra, ldec, radius_deg, rra[perm], rdec[perm], maxmatch=1)
            i_left, i_right, cosdist = m["i1"], perm[m["i2"]], m["cosdist"]
        else:
            m = smatch.match(lra, ldec, radius_deg, rra, rdec, maxmatch=1)
            i_left, i_right, cosdist = m["i1"], m["i2"], m["cosdist"]
    else:
        if shuffle:
            perm = rng.permutation(len(lra))
            m = smatch.match(rra, rdec, radius_deg, lra[perm], ldec[perm], maxmatch=0)
            m["i2"] = perm[m["i2"]]
        else:
            m = smatch.match(rra, rdec, radius_deg, lra, ldec, maxmatch=0)
        order = np.lexsort((-m["cosdist"], m["i2"]))
        m = m[order]
        keep = np.empty(len(m), dtype=bool)
        keep[:1] = True
        keep[1:] = m["i2"][1:] != m["i2"][:-1]
        i_left, i_right, cosdist = m["i2"][keep], m["i1"][keep], m["cosdist"][keep]
    dist_arcsec = np.degrees(np.arccos(np.clip(cosdist, -1.0, 1.0))) * 3600.0
    return pd.concat(
        [
            left_df.iloc[i_left].reset_index(drop=True),
            right_df.iloc[i_right].reset_index(drop=True).add_suffix("_gaia"),
            pd.DataFrame({"_dist_arcsec": dist_arcsec}),
        ],
        axis=1,
    )


def _nearest_within(left_df, right_df, i_left, i_right, dist_arcsec):
    """Assemble the (left, right, distance) frame for the given in-radius pairs only."""
    return pd.concat(
        [
            left_df.iloc[i_left].reset_index(drop=True),
            right_df.iloc[i_right].reset_index(drop=True).add_suffix("_gaia"),
            pd.DataFrame({"_dist_arcsec": dist_arcsec}),
        ],
        axis=1,
    )


def astropy_chunk(left_df, right_df, right_coord, *, radius_arcsec):
    """Nearest Gaia neighbour of every left row within radius. The radius mask is applied to the neighbour index
    BEFORE gathering the right-hand columns (run_astropy gathers all 27 Gaia columns for every left row first,
    which is what drove its 128 GiB out-of-memory at 5e7 rows). The k-d tree of right_coord is built by astropy
    on the first call and cached on the SkyCoord (storekdtree), so later chunks reuse it."""
    from astropy.coordinates import Angle, SkyCoord, match_coordinates_sky

    left_coord = SkyCoord(ra=left_df["ra"].to_numpy(dtype=float), dec=left_df["dec"].to_numpy(dtype=float), unit="deg")
    idx_right, d2, _ = match_coordinates_sky(left_coord, right_coord, nthneighbor=1)
    mask = (d2 < Angle(radius_arcsec, "arcsec")).nonzero()[0]
    return _nearest_within(left_df, right_df, mask, idx_right[mask], d2[mask].to_value("arcsec"))


def smatch_chunk(left_df, right_df, right_index, *, radius_arcsec, rng=None):
    """All pairs within radius from the pre-built smatch.Catalog of the right side (Gaia), reduced to the closest
    right row per left row (same reduction as run_smatch, orientation='right').

    The streamed (left) points are handed to smatch in RANDOM order. smatch 0.10.2 has an input-order-dependent
    slow path: for ~3M-row ZTF partitions against the same Gaia index, HEALPix-sorted input took 2-15 s per
    partition and Dec-sorted input 45-62 s, shuffled input 1.5-1.6 s, identical pairs in all cases
    (measured 2026-09-16, sdfiana033). The permutation is undone on the returned indices."""
    lra, ldec = left_df["ra"].to_numpy(dtype=float), left_df["dec"].to_numpy(dtype=float)
    perm = (rng or np.random.default_rng(0)).permutation(len(lra))
    right_index.match(lra[perm], ldec[perm], maxmatch=0)
    m = right_index.matches
    if len(m) == 0:
        return _nearest_within(left_df, right_df, np.array([], dtype=int), np.array([], dtype=int), np.array([], dtype=float))
    i_left_all = perm[m["i2"]]  # back to the chunk's row order
    order = np.lexsort((-m["cosdist"], i_left_all))
    i_left_all, i_right_all, cosdist = i_left_all[order], m["i1"][order], m["cosdist"][order]
    keep = np.empty(len(order), dtype=bool)
    keep[:1] = True
    keep[1:] = i_left_all[1:] != i_left_all[:-1]
    dist_arcsec = np.degrees(np.arccos(np.clip(cosdist[keep], -1.0, 1.0))) * 3600.0
    return _nearest_within(left_df, right_df, i_left_all[keep], i_right_all[keep], dist_arcsec)


def _unit_xyz(df):
    ra = np.radians(df["ra"].to_numpy(dtype=float))
    dec = np.radians(df["dec"].to_numpy(dtype=float))
    cd = np.cos(dec)
    return np.column_stack([cd * np.cos(ra), cd * np.sin(ra), np.sin(dec)])


def scipy_chunk(left_df, right_df, right_tree, *, radius_arcsec, workers):
    """Nearest right row within radius via a scipy cKDTree on unit vectors (built once on the right side), the query
    spread over `workers` threads. This is astropy's algorithm without the single-threaded query and SkyCoord overhead."""
    chord = 2.0 * np.sin(np.radians(radius_arcsec / 3600.0) / 2.0)
    d, idx = right_tree.query(_unit_xyz(left_df), k=1, distance_upper_bound=chord, workers=workers)
    ok = (idx < len(right_df)).nonzero()[0]
    dist_arcsec = np.degrees(2.0 * np.arcsin(np.clip(d[ok], 0.0, 2.0) / 2.0)) * 3600.0
    return _nearest_within(left_df, right_df, ok, idx[ok], dist_arcsec)


def run_chunked_write(tool, ztf_files, ztf_columns, cone, right_df, out_dir, *, radius_arcsec, workers=1):
    """Stream the left catalog one ZTF partition file at a time: read, (cone-filter), match against the resident
    right-hand frame, write the in-radius pairs of that chunk to parquet, drop the chunk. Peak memory is the right
    frame + its index + one chunk + its result, independent of the total left size. Returns (written_rows, left_rows,
    wall seconds). The right-hand index (astropy k-d tree / smatch HEALPix catalog) is built inside the timed region on
    first use, as run_astropy did implicitly."""
    import smatch
    from astropy.coordinates import SkyCoord

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    t = time.monotonic()
    if tool == "astropy":
        right_coord = SkyCoord(ra=right_df["ra"].to_numpy(dtype=float), dec=right_df["dec"].to_numpy(dtype=float), unit="deg")
        match = lambda ldf: astropy_chunk(ldf, right_df, right_coord, radius_arcsec=radius_arcsec)  # noqa: E731
    elif tool == "smatch":
        right_index = smatch.Catalog(right_df["ra"].to_numpy(dtype=float), right_df["dec"].to_numpy(dtype=float), radius_arcsec / 3600.0)
        rng = np.random.default_rng(0)
        match = lambda ldf: smatch_chunk(ldf, right_df, right_index, radius_arcsec=radius_arcsec, rng=rng)  # noqa: E731
    elif tool == "scipy":
        from scipy.spatial import cKDTree

        right_tree = cKDTree(_unit_xyz(right_df))
        match = lambda ldf: scipy_chunk(ldf, right_df, right_tree, radius_arcsec=radius_arcsec, workers=workers)  # noqa: E731
    else:
        raise ValueError(tool)
    written = left_rows = 0
    for i, f in enumerate(ztf_files):
        ldf = load_parquet([f], ztf_columns).result
        if cone is not None:
            ldf = cone_filter(ldf, *cone).result
        left_rows += len(ldf)
        if len(ldf) == 0:
            # a partition whose pixel touches the cone but holds no row inside it; smatch's C extension returns NULL on empty input
            continue
        res = match(ldf)
        res.to_parquet(out_dir / f"part_{i:05d}.parquet", engine="pyarrow", index=False)
        written += len(res)
        del ldf, res
    return written, left_rows, time.monotonic() - t


@measure
def run_scipy(left_df, right_df, *, radius_arcsec, n_neighbors, workers):
    """Hand-rolled baseline: scipy cKDTree on unit vectors, parallel query (what astropy does single-threaded)."""
    from scipy.spatial import cKDTree

    assert n_neighbors == 1

    def xyz(df):
        ra = np.radians(df["ra"].to_numpy(dtype=float))
        dec = np.radians(df["dec"].to_numpy(dtype=float))
        cd = np.cos(dec)
        return np.column_stack([cd * np.cos(ra), cd * np.sin(ra), np.sin(dec)])

    tree = cKDTree(xyz(right_df))
    chord = 2.0 * np.sin(np.radians(radius_arcsec / 3600.0) / 2.0)
    d, idx = tree.query(xyz(left_df), k=1, distance_upper_bound=chord, workers=workers)
    ok = idx < len(right_df)
    dist_arcsec = np.degrees(2.0 * np.arcsin(np.clip(d[ok], 0.0, 2.0) / 2.0)) * 3600.0
    return pd.concat(
        [
            left_df[ok].reset_index(drop=True),
            right_df.iloc[idx[ok]].reset_index(drop=True).add_suffix("_gaia"),
            pd.DataFrame({"_dist_arcsec": dist_arcsec}),
        ],
        axis=1,
    )


def _find_stilts_jar() -> str:
    """$STILTS_JAR, else stilts.jar next to this script, else the original SDF copy. The jar is a public download:
    https://www.star.bris.ac.uk/~mbt/stilts/stilts.jar (needs a JRE; `java` is on the SDF nodes)."""
    for cand in (os.environ.get("STILTS_JAR"), str(Path(__file__).with_name("stilts.jar")),
                 "/sdf/group/rubin/user/ncaplar/tools/stilts/stilts.jar"):
        if cand and os.path.isfile(cand):
            return cand
    return str(Path(__file__).with_name("stilts.jar"))


STILTS_JAR_DEFAULT = _find_stilts_jar()


def _stilts(jar, mem_gb, args):
    """Run one STILTS command; returns (wall seconds, stdout)."""
    import subprocess

    # heap limit below the RSS cap (JVM RSS = heap + metaspace + off-heap), so Java runs out of heap and fails cleanly
    # (OutOfMemoryError -> MemoryError -> recorded as oom, tool skipped for larger steps) before the RSS poll kills it
    cmd = ["java", f"-Xmx{int(mem_gb * 0.8)}g", "-jar", jar] + args
    t = time.monotonic()
    r = subprocess.run(cmd, capture_output=True, text=True)
    dt = time.monotonic() - t
    if r.returncode != 0:
        if "OutOfMemoryError" in r.stderr or "GC overhead limit" in r.stderr:
            raise MemoryError(f"stilts out of Java heap: {' '.join(args)[:200]}")
        raise RuntimeError(f"stilts failed ({r.returncode}): {' '.join(args)[:300]}\n{r.stderr[-1500:]}")
    return dt, r.stdout


def run_stilts_leg(files, columns, cone, radius_arcsec, mem_gb, jar, workdir, output_mode="count", out_dir=None, n_threads=None,
                   use_disk=False, fits_cache=None):
    """STILTS (TOPCAT engine): tcat each side's Parquet files to one FITS table (its Parquet reader,
    counted as I/O), then tmatch2 matcher=sky find=best1 (nearest table-2 row per table-1 row).
    output_mode="write": STILTS runs with -disk (tables backed by temp files instead of the heap) and tmatch2 writes
    the matched table to <out_dir>/xmatch.parquet; the row count is read from the parquet footer (untimed)."""
    wd = Path(workdir) / f"stilts_{os.getpid()}"
    wd.mkdir(parents=True, exist_ok=True)
    flags = ["-disk"] if (output_mode == "write" and use_disk) else []  # default: STILTS holds tables in the Java heap (native)
    out = {}
    try:
        # the Parquet->FITS conversions are not part of the plotted tmatch2 time; with fits_cache they run once per step
        fits_dir = Path(fits_cache) if fits_cache else wd
        fits_dir.mkdir(parents=True, exist_ok=True)
        zfits, gfits = fits_dir / "ztf.fits", fits_dir / "gaia.fits"
        if zfits.exists() and gfits.exists():
            out["ztf_io"] = {"time": 0.0, "result_length": None, "cached": True}
            out["gaia_io"] = {"time": 0.0, "result_length": None, "cached": True}
        else:
            zlist, glist = wd / "ztf.lis", wd / "gaia.lis"
            zlist.write_text("\n".join(files["ztf"]) + "\n")
            glist.write_text("\n".join(files["gaia"] + files["gaia_margin"]) + "\n")
            dt, so = _stilts(jar, mem_gb, flags + ["tcat", f"in=@{zlist}", "ifmt=parquet", f"icmd=keepcols \"{' '.join(columns['ztf'])}\"",
                                                    "ofmt=fits-basic", f"out={zfits}.part"])
            os.replace(f"{zfits}.part", zfits)
            out["ztf_io"] = {"time": dt, "result_length": None}
            dt, so = _stilts(jar, mem_gb, flags + ["tcat", f"in=@{glist}", "ifmt=parquet", f"icmd=keepcols \"{' '.join(columns['gaia'])}\"",
                                                    "ofmt=fits-basic", f"out={gfits}.part"])
            os.replace(f"{gfits}.part", gfits)
            out["gaia_io"] = {"time": dt, "result_length": None}
        args = flags + ["tmatch2", f"in1={zfits}", f"in2={gfits}", "matcher=sky", "values1=ra dec", "values2=ra dec",
                        f"params={radius_arcsec:g}", "join=1and2", "find=best1", f"runner=parallel{n_threads}" if n_threads else "runner=parallel", "fixcols=all", "suffix1=_ztf", "suffix2=_gaia"]
        if output_mode == "write":
            out_path = Path(out_dir)
            out_path.mkdir(parents=True, exist_ok=True)
            args += [f"out={out_path / 'xmatch.parquet'}", "ofmt=parquet"]
        else:
            args += ["omode=count"]
        if cone is not None:
            ra0, dec0, r = cone
            args.insert(len(flags) + 2, f"icmd1=select skyDistanceDegrees(ra,dec,{ra0!r},{dec0!r})<{r!r}/3600.")
        dt, so = _stilts(jar, mem_gb, args)
        n = None
        if output_mode == "write":
            import pyarrow.parquet as pq

            n = int(pq.read_metadata(out_path / "xmatch.parquet").num_rows)
        else:
            for line in so.splitlines():
                if "rows:" in line:
                    n = int(line.split("rows:")[1].strip().split()[0])
        out["stilts_xmatch"] = {"time": dt, "result_length": n}
        out["left_rows"] = None
        out["right_rows"] = None
        return out
    finally:
        shutil.rmtree(wd, ignore_errors=True)


# ----------------------------------------------------------------------------- sequential-tool leg (child process)
def sequential_leg(tool: str, files: dict, columns: dict, cone: tuple | None, n_cpus: int, conn, smatch_orientation: str = "right",
                   stilts_jar: str = STILTS_JAR_DEFAULT, mem_cap_gb: float = 128.0, workdir: str = "/tmp",
                   output_mode: str = "count", out_dir: str | None = None, tool_mode: str = "native",
                   stilts_disk: bool = False, stilts_fits_cache: str | None = None, smatch_shuffle: bool = False):
    """Load with pyarrow, optionally cone-filter, run one matcher. Runs in a child process. `n_cpus` is the thread cap
    of this tool (pyarrow decode, scipy query, STILTS runner=parallel<n>); the driver passes --tool-threads.
    output_mode="count": whole left catalog in memory, one match call, result held in memory (the 2025 design).
    output_mode="write", tool_mode="native": whole left and right catalogs in memory, one native match call exactly as a user
        would write it (astropy match_coordinates_sky + radius cut; smatch.match(left, r, right, maxmatch=1); scipy cKDTree
        query), the joined result written to parquet once. No streaming, no masking before the gather, no shuffling (unless
        --smatch-shuffle, see run_smatch): if the tool runs out of memory or time, that is the tool's limit and its curve ends there.
    output_mode="write", tool_mode="stream": left catalog streamed per partition file against one right-hand index, each
        chunk's result written to parquet (the 2026-09-16 design; kept for reference)."""
    try:
        import pyarrow as pa
        import pyarrow.parquet  # noqa: F401  (imports happen here, outside every timed region)
        import astropy.coordinates  # noqa: F401
        import scipy.spatial  # noqa: F401
        import smatch  # noqa: F401

        pa.set_cpu_count(n_cpus)
        pa.set_io_thread_count(n_cpus)
        if tool == "stilts":
            out = run_stilts_leg(files, columns, cone, XMATCH_RADIUS_ARCSEC, mem_cap_gb, stilts_jar, workdir, output_mode, out_dir, n_threads=n_cpus,
                                 use_disk=stilts_disk, fits_cache=stilts_fits_cache)
            conn.send({"ok": True, "measurements": out, "peak_rss_child": rss_tree_bytes(os.getpid())})
            return
        out = {}
        if output_mode == "write" and tool_mode == "stream":
            out["gaia_pyarrow"] = to_report(m := load_parquet(files["gaia"], columns["gaia"]))
            gaia_df = m.result
            out["gaia_margin_pyarrow"] = to_report(m := load_parquet(files["gaia_margin"], columns["gaia"]))
            margin_df = m.result
            out["merge_right"] = to_report(m := merge_right(gaia_df, margin_df))
            right_df = m.result
            del gaia_df, margin_df
            out["right_rows"] = int(len(right_df))
            if tool not in ("astropy", "smatch", "scipy"):
                raise ValueError(f"{tool} has no write mode")
            written, left_rows, dt = run_chunked_write(tool, files["ztf"], columns["ztf"], cone, right_df, out_dir,
                                                       radius_arcsec=XMATCH_RADIUS_ARCSEC, workers=n_cpus)
            out[f"{tool}_xmatch"] = {"time": dt, "result_length": int(written)}
            out["left_rows"] = int(left_rows)
            out["n_chunks"] = len(files["ztf"])
            conn.send({"ok": True, "measurements": out, "peak_rss_child": rss_tree_bytes(os.getpid())})
            return
        out["ztf_pyarrow"] = to_report(m := load_parquet(files["ztf"], columns["ztf"]))
        ztf_df = m.result
        out["gaia_pyarrow"] = to_report(m := load_parquet(files["gaia"], columns["gaia"]))
        gaia_df = m.result
        out["gaia_margin_pyarrow"] = to_report(m := load_parquet(files["gaia_margin"], columns["gaia"]))
        margin_df = m.result
        out["merge_right"] = to_report(m := merge_right(gaia_df, margin_df))
        right_df = m.result
        del gaia_df, margin_df
        if cone is not None:
            out["cone_filter"] = to_report(m := cone_filter(ztf_df, *cone))
            ztf_df = m.result
        out["left_rows"] = int(len(ztf_df))
        out["right_rows"] = int(len(right_df))
        if tool == "astropy":
            out["skycoords"] = to_report(m := dfs_to_skycoords(ztf_df, right_df))
            xm = run_astropy(ztf_df, right_df, *m.result, radius_arcsec=XMATCH_RADIUS_ARCSEC, n_neighbors=XMATCH_N_NEIGHBORS)
        elif tool == "smatch":
            xm = run_smatch(ztf_df, right_df, radius_arcsec=XMATCH_RADIUS_ARCSEC, n_neighbors=XMATCH_N_NEIGHBORS, orientation=smatch_orientation,
                            shuffle=smatch_shuffle)
        elif tool == "scipy":
            xm = run_scipy(ztf_df, right_df, radius_arcsec=XMATCH_RADIUS_ARCSEC, n_neighbors=XMATCH_N_NEIGHBORS, workers=n_cpus)
        else:
            raise ValueError(tool)
        out[f"{tool}_xmatch"] = to_report(xm)
        if output_mode == "write":
            out_path = Path(out_dir)
            out_path.mkdir(parents=True, exist_ok=True)
            out[f"{tool}_write"] = to_report(write_parquet(xm.result, out_path / "xmatch.parquet"))
        conn.send({"ok": True, "measurements": out, "peak_rss_child": rss_tree_bytes(os.getpid())})
    except MemoryError:
        conn.send({"ok": False, "oom": True, "error": "MemoryError"})
    except Exception:  # pylint: disable=broad-except
        conn.send({"ok": False, "oom": False, "error": traceback.format_exc()})
    finally:
        conn.close()


def run_sequential_tool(tool, files, columns, cone, n_cpus, mem_cap_bytes, poll_s=0.5, smatch_orientation="right",
                        stilts_jar=STILTS_JAR_DEFAULT, workdir="/tmp", output_mode="count", out_dir=None, tool_mode="native",
                        stilts_disk=False, stilts_fits_cache=None, time_cap_s=0.0, smatch_shuffle=False):
    """Run `sequential_leg` in a spawned child; kill it if the RSS of its process tree exceeds the memory cap, or if it runs
    longer than time_cap_s (0 = no time cap). Either kill ends the tool's curve at this step."""
    ctx = mp.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    proc = ctx.Process(target=sequential_leg, args=(tool, files, columns, cone, n_cpus, child_conn, smatch_orientation,
                                                     stilts_jar, mem_cap_bytes / 2**30, workdir, output_mode, out_dir, tool_mode,
                                                     stilts_disk, stilts_fits_cache, smatch_shuffle))
    t0 = time.monotonic()
    proc.start()
    child_conn.close()
    peak = 0
    result = None
    while True:
        if parent_conn.poll(poll_s):
            try:
                result = parent_conn.recv()
            except EOFError:
                result = None
            break
        rss = rss_tree_bytes(proc.pid)
        peak = max(peak, rss)
        if rss > mem_cap_bytes:
            proc.kill()
            proc.join()
            return {"ok": False, "oom": True, "error": f"killed: RSS {rss/2**30:.1f} GiB > cap", "peak_rss": peak,
                    "wall": time.monotonic() - t0}
        if time_cap_s and time.monotonic() - t0 > time_cap_s:
            proc.kill()
            proc.join()
            return {"ok": False, "oom": False, "timeout": True, "error": f"killed: ran longer than the {time_cap_s:.0f} s time cap",
                    "peak_rss": peak, "wall": time.monotonic() - t0}
        if not proc.is_alive():
            break
    proc.join()
    if result is None:
        result = {"ok": False, "oom": proc.exitcode in (-9, 137), "error": f"child exited {proc.exitcode} without result"}
    result["peak_rss"] = max(peak, result.get("peak_rss_child", 0))
    result["wall"] = time.monotonic() - t0
    return result


# ----------------------------------------------------------------------------- settings block
def versions() -> dict:
    import astropy
    import dask
    import distributed
    import hats
    import lsdb
    import pyarrow

    v = {
        "python": platform.python_version(), "lsdb": lsdb.__version__, "hats": hats.__version__,
        "dask": dask.__version__, "distributed": distributed.__version__, "astropy": astropy.__version__,
        "numpy": np.__version__, "pandas": pd.__version__, "pyarrow": pyarrow.__version__,
    }
    try:
        import smatch

        v["smatch"] = getattr(smatch, "__version__", "unknown")
    except ImportError:
        v["smatch"] = None
    return v


def settings_block(args, seed, columns, ztf_full, gaia_full) -> dict:
    import psutil

    cpus = allowed_cpus()
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "hostname": socket.gethostname(),
        "cpu_set": cpus, "n_cpus": len(cpus), "node_cpus": os.cpu_count(),
        "node_ram_gb": round(psutil.virtual_memory().total / 2**30, 1),
        "mem_cap_gb": args.mem_cap_gb,
        "versions": versions(),
        "left": {"path": ZTF_PATH, "name": ztf_full.name, "total_rows": int(ztf_full.hc_structure.catalog_info.total_rows or 0),
                 "columns": columns["ztf"]},
        "right": {"path": GAIA_PATH, "name": gaia_full.name, "total_rows": int(gaia_full.hc_structure.catalog_info.total_rows or 0),
                  "columns": columns["gaia"], "margin": gaia_full.margin.name if gaia_full.margin is not None else None},
        "radius_arcsec": XMATCH_RADIUS_ARCSEC, "n_neighbors": XMATCH_N_NEIGHBORS,
        "seed_partition": seed, "columns_mode": args.columns, "warm_cache": not args.no_warmup,
        "rows_band": list(args.rows_band) if args.rows_band else None, "output_mode": args.output, "out_dir": args.out_dir if args.output == "write" else None, "smatch_orientation": args.smatch_orientation, "smatch_shuffle": args.smatch_shuffle, "threads_per_worker": args.threads_per_worker or "dask-default", "tool_threads": args.tool_threads,
        "tool_mode": args.tool_mode, "stilts_disk": args.stilts_disk, "time_cap_s": args.time_cap_s,
        "command": " ".join(sys.argv),
    }


# ----------------------------------------------------------------------------- main loop
def open_catalogs(columns):
    import lsdb

    ztf = lsdb.open_catalog(ZTF_PATH, columns=columns["ztf"])
    gaia = lsdb.open_catalog(GAIA_PATH, columns=columns["gaia"])
    return ztf, gaia


def make_client(n_workers: int, n_cpus: int, mem_cap_gb: float, local_dir: str, threads_per_worker: int | None = 1):
    """threads_per_worker=None keeps the Dask default (n_cpus // n_workers, the 2025 behaviour);
    the default here is 1, so "N workers" means N processes on N cores."""
    import dask.distributed

    threads = threads_per_worker if threads_per_worker else max(1, n_cpus // n_workers)
    client = dask.distributed.Client(
        n_workers=n_workers, threads_per_worker=threads, processes=True,
        memory_limit=f"{mem_cap_gb / n_workers:.2f}GB", local_directory=local_dir, dashboard_address=":0",
    )
    return client, threads


def warm_client(client, left_cat, right_cat, threads: int):
    """Untimed: import the stack on every worker, read the first partition of each side and run one
    single-partition crossmatch, so the first timed compute pays neither worker start-up nor first-call
    imports (the 2025 script absorbed this in an untimed load)."""

    def _imports(n_threads):
        import lsdb  # noqa: F401
        import nested_pandas  # noqa: F401
        import pyarrow as pa

        pa.set_cpu_count(n_threads)  # parquet decode threads per worker = Dask threads per worker
        return pa.cpu_count()

    t = time.monotonic()
    client.run(_imports, threads)
    lsdb_io(left_cat.partitions[0])
    lsdb_io(right_cat.margin.partitions[0])
    run_lsdb(left_cat.partitions[0], right_cat, radius_arcsec=XMATCH_RADIUS_ARCSEC, n_neighbors=XMATCH_N_NEIGHBORS)
    log.info("client warm-up %.1f s", time.monotonic() - t)


def warm_cache(files: dict, columns: dict):
    """Untimed pre-read of every file of the step (page cache), one file at a time so the parent never holds a big frame."""
    import pyarrow.parquet as pq

    t = time.monotonic()
    nbytes = 0
    for key, cols in (("ztf", columns["ztf"]), ("gaia", columns["gaia"]), ("gaia_margin", columns["gaia"])):
        for f in files[key]:
            nbytes += pq.read_table(f, columns=list(cols) + [SPATIAL_INDEX_COLUMN]).nbytes
    log.info("warm-up read done in %.1f s (%.1f GB decoded)", time.monotonic() - t, nbytes / 1e9)


def run(args):
    columns = {"ztf": ZTF_DEFAULT_COLS, "gaia": GAIA_DEFAULT_COLS} if args.columns == "default" else {"ztf": RADEC_COLS, "gaia": RADEC_COLS}
    ztf_full, gaia_full = open_catalogs(columns)
    seed, near = ladder_geometry(ztf_full, seed_override=args.seed, rows_band=args.rows_band, seed_radec=args.seed_radec)
    log.info("seed partition Norder=%d Npix=%d rows=%d centre=(%.4f, %.4f)", seed["order"], seed["pixel"], seed["rows"], seed["ra"], seed["dec"])
    if args.show_ladder:
        show = near[["Norder", "Npix", "num_rows", "ra_c", "dec_c", "b", "sep_deg", "cum_rows"]].head(args.show_ladder).round(3)
        print(show.to_string(index=True))
        return
    n_cpus = len(allowed_cpus())
    log.info("cpu set: %d cpus %s; memory cap %.0f GB", n_cpus, allowed_cpus()[:4] + ["..."], args.mem_cap_gb)
    pi_rows = ztf_partition_table(ztf_full).set_index(["Norder", "Npix"])["num_rows"]  # every partition, band or not
    settings = settings_block(args, seed, columns, ztf_full, gaia_full)
    results_root = Path(args.results_dir) / args.config_name
    results_root.mkdir(parents=True, exist_ok=True)
    (results_root / "settings.json").write_text(json.dumps(settings, indent=1))
    oom_tools: set[str] = set()      # tools that ran out of memory at an earlier (smaller) step
    slow_tools: set[str] = set()     # tools that hit the time cap at an earlier step
    mem_cap_bytes = int(args.mem_cap_gb * 2**30)

    def pending(step_dir, prefix):
        return [i for i in range(args.n_runs) if args.overwrite or not (step_dir / f"{prefix}_run{i}.json").exists()]

    for step in args.steps:
        kind, val = parse_step(step)
        step_dir = results_root / step.replace(":", "_")
        step_dir.mkdir(exist_ok=True)
        ztf_sel = select_left(ztf_full, step, seed, near)
        gaia_sel = select_right(gaia_full, ztf_sel)
        files = {"ztf": pixel_files(ztf_sel), "gaia": pixel_files(gaia_sel), "gaia_margin": pixel_files(gaia_sel.margin)}
        cone = (seed["ra"], seed["dec"], val) if kind == "cone" else None
        base = {"step": step, "kind": kind, "value": val, "n_ztf_partitions": ztf_sel.npartitions, "n_gaia_partitions": gaia_sel.npartitions}
        log.info("=== step %s: %d ztf partitions, %d gaia partitions, %d/%d/%d files",
                 step, ztf_sel.npartitions, gaia_sel.npartitions, len(files["ztf"]), len(files["gaia"]), len(files["gaia_margin"]))
        base["left_rows"] = count_left_rows(files["ztf"], cone, ztf_sel, near)
        base["left_rows_read"] = count_rows_read(ztf_sel, pi_rows)
        log.info("step %s: %d left (ZTF) rows in the selection, %d rows in the partitions read", step, base["left_rows"], base["left_rows_read"])
        if not args.no_warmup:
            warm_cache(files, columns)

        if "lsdb" in args.algos:
            for w in args.workers:
                todo = pending(step_dir, f"lsdb_w{w}")
                if not todo:
                    log.info("step %s lsdb %d workers: all runs exist, skipping", step, w)
                    continue
                client, threads = make_client(w, n_cpus, args.mem_cap_gb, os.path.join(args.dask_local_dir, f"w{w}"), args.threads_per_worker)
                try:
                    warm_client(client, ztf_sel, gaia_sel, threads)
                    for run_ind in todo:
                        rec = dict(base, run=run_ind, tool="lsdb", workers=w, threads_per_worker=threads)
                        try:
                            if args.lsdb_io:
                                rec["lsdb_io"] = {k: to_report(lsdb_io(c)) for k, c in (("ztf", ztf_sel), ("gaia", gaia_sel), ("gaia_margin", gaia_sel.margin))}
                            if args.output == "write":
                                out_path = Path(args.out_dir) / f"{args.config_name}_{step.replace(':', '_')}_lsdb_w{w}_run{run_ind}"
                                try:
                                    m = run_lsdb_write(ztf_sel, gaia_sel, str(out_path), radius_arcsec=XMATCH_RADIUS_ARCSEC, n_neighbors=XMATCH_N_NEIGHBORS)
                                finally:
                                    if not args.keep_output:
                                        shutil.rmtree(out_path, ignore_errors=True)
                            else:
                                m = run_lsdb(ztf_sel, gaia_sel, radius_arcsec=XMATCH_RADIUS_ARCSEC, n_neighbors=XMATCH_N_NEIGHBORS)
                            rec["lsdb_crossmatch"] = to_report(m)
                            log.info("step %s run %d lsdb %2d workers x %d threads: %.2f s, %d rows", step, run_ind, w, threads, m.time, rec["lsdb_crossmatch"]["result_length"])
                        except Exception:  # pylint: disable=broad-except
                            rec["error"] = traceback.format_exc()
                            log.error("lsdb failed:\n%s", rec["error"])
                        (step_dir / f"lsdb_w{w}_run{run_ind}.json").write_text(json.dumps(rec, indent=1))
                finally:
                    client.close(timeout=120)

        fits_cache = Path(args.dask_local_dir) / "stilts_fits" / f"{args.config_name}_{step.replace(':', '_')}"
        for tool in [t for t in args.algos if t != "lsdb"]:
            for run_ind in pending(step_dir, tool):
                rec = dict(base, run=run_ind, tool=tool, tool_mode=args.tool_mode, smatch_shuffle=(args.smatch_shuffle if tool == "smatch" else None))
                if tool in oom_tools:
                    rec.update(ok=False, skipped="an earlier step ran out of memory")
                elif tool in slow_tools:
                    rec.update(ok=False, skipped=f"an earlier step ran longer than the {args.time_cap_s:.0f} s time cap")
                else:
                    out_path = Path(args.out_dir) / f"{args.config_name}_{step.replace(':', '_')}_{tool}_run{run_ind}"
                    try:
                        res = run_sequential_tool(tool, files, columns, cone, args.tool_threads, mem_cap_bytes, smatch_orientation=args.smatch_orientation,
                                                  stilts_jar=args.stilts_jar, workdir=args.dask_local_dir, output_mode=args.output, out_dir=str(out_path),
                                                  tool_mode=args.tool_mode, stilts_disk=args.stilts_disk, stilts_fits_cache=str(fits_cache),
                                                  time_cap_s=args.time_cap_s, smatch_shuffle=args.smatch_shuffle)
                    finally:
                        if not args.keep_output:
                            shutil.rmtree(out_path, ignore_errors=True)
                    rec.update(res)
                    if res["ok"]:
                        ms = res["measurements"]
                        io = sum(v["time"] for k, v in ms.items() if isinstance(v, dict) and (k.endswith("_pyarrow") or k.endswith("_io")))
                        xm = ms[f"{tool}_xmatch"]
                        wr = ms.get(f"{tool}_write", {}).get("time", 0.0)
                        log.info("step %s run %d %s: io %.2f s + xmatch %.2f s + write %.2f s, %d rows, peak rss %.1f GiB", step, run_ind, tool, io, xm["time"], wr, xm["result_length"], res["peak_rss"] / 2**30)
                    else:
                        log.error("step %s run %d %s FAILED oom=%s timeout=%s: %s", step, run_ind, tool, res.get("oom"), res.get("timeout"), str(res.get("error"))[-400:])
                        if res.get("oom"):
                            oom_tools.add(tool)
                        if res.get("timeout"):
                            slow_tools.add(tool)
                (step_dir / f"{tool}_run{run_ind}.json").write_text(json.dumps(rec, indent=1))
        shutil.rmtree(fits_cache, ignore_errors=True)
    log.info("done")


def calibrate(args):
    """One lsdb crossmatch per step with the largest worker count; prints result rows per step."""
    columns = {"ztf": RADEC_COLS, "gaia": RADEC_COLS}
    ztf_full, gaia_full = open_catalogs(columns)
    seed, near = ladder_geometry(ztf_full, seed_override=args.seed, rows_band=args.rows_band, seed_radec=args.seed_radec)
    n_cpus = len(allowed_cpus())
    client, threads = make_client(max(args.workers), n_cpus, args.mem_cap_gb, os.path.join(args.dask_local_dir, "calib"), args.threads_per_worker)
    rows = []
    try:
        for step in args.steps:
            ztf_sel = select_left(ztf_full, step, seed, near)
            gaia_sel = select_right(gaia_full, ztf_sel)
            m = run_lsdb(ztf_sel, gaia_sel, radius_arcsec=XMATCH_RADIUS_ARCSEC, n_neighbors=XMATCH_N_NEIGHBORS)
            n = result_length(m.result)
            rows.append({"step": step, "ztf_partitions": ztf_sel.npartitions, "gaia_partitions": gaia_sel.npartitions, "result_rows": n, "time_s": round(m.time, 2)})
            log.info("calib %-14s ztf_parts=%4d gaia_parts=%4d rows=%12d  %.1f s", step, ztf_sel.npartitions, gaia_sel.npartitions, n, m.time)
    finally:
        client.close(timeout=120)
    df = pd.DataFrame(rows)
    print(df.to_string(index=False))
    out = Path(args.results_dir) / "calibration.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"seed": seed, "steps": rows, "workers": max(args.workers), "threads": threads}, indent=1))


def calibrate_cones(args):
    """Cone radii around the seed centre that hold the requested numbers of ZTF rows. Reads ra/dec of every ZTF file a
    wide cone touches, sorts the separations, puts each radius halfway between the N-th and (N+1)-th nearest row and
    re-counts with cone_filter at the printed precision. Prints the --steps string; writes results/cone_calibration_<config>.json."""
    import hats.pixel_math.healpix_shim as hs
    from lsdb import ConeSearch

    columns = {"ztf": RADEC_COLS, "gaia": RADEC_COLS}
    ztf_full, _ = open_catalogs(columns)
    seed, near = ladder_geometry(ztf_full, seed_override=args.seed, rows_band=args.rows_band, seed_radec=args.seed_radec)
    log.info("cone centre (%.4f, %.4f); nearest partition Norder=%d Npix=%d rows=%d", seed["ra"], seed["dec"], seed["order"], seed["pixel"], seed["rows"])
    targets = sorted(args.calibrate_cones)
    n_max = targets[-1]
    dens = seed["rows"] / hs.order2pixarea(seed["order"], degrees=True)  # rows / deg^2 at the seed
    r_deg = 1.3 * math.degrees(math.acos(max(-1.0, 1.0 - n_max / dens / (2.0 * math.pi) * (math.pi / 180.0) ** 2)))
    while True:
        files = pixel_files(ztf_full.search(ConeSearch(seed["ra"], seed["dec"], r_deg * 3600.0, fine=False)))
        t = time.monotonic()
        seps = []
        for f in files:
            df = load_parquet([f], RADEC_COLS).result
            sp = angular_sep_deg(df["ra"].to_numpy(dtype=float), df["dec"].to_numpy(dtype=float), seed["ra"], seed["dec"])
            seps.append(sp[sp <= r_deg])
        sep = np.sort(np.concatenate(seps))
        log.info("search cone %.2f deg: %d ZTF partitions, %d rows inside, %.1f s", r_deg, len(files), len(sep), time.monotonic() - t)
        if len(sep) > n_max:
            break
        r_deg *= 1.3
    rows = []
    for n in targets:
        r_arcsec = float(f"{0.5 * (sep[n - 1] + sep[n]) * 3600.0:.7g}")
        count = int(np.count_nonzero(sep <= r_arcsec / 3600.0))
        sel = ztf_full.search(ConeSearch(seed["ra"], seed["dec"], r_arcsec, fine=True))
        rows.append({"target": n, "radius_arcsec": r_arcsec, "radius_deg": r_arcsec / 3600.0, "left_rows": count, "ztf_partitions": sel.npartitions})
        log.info("target %10d -> %12.7g arcsec (%8.4f deg) %10d rows %3d partitions", n, r_arcsec, r_arcsec / 3600.0, count, sel.npartitions)
    df = pd.DataFrame(rows)
    print(df.to_string(index=False))
    steps = ",".join(f"cone:{r['radius_arcsec']:.7g}" for r in rows)
    print("STEPS=" + steps)
    out = Path(args.results_dir) / f"cone_calibration_{args.config_name}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({"seed": seed, "targets": rows, "steps": steps, "search_radius_deg": r_deg, "rows_within_search": int(len(sep)),
                               "ztf_path": ZTF_PATH, "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}, indent=1))
    log.info("wrote %s", out)


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--columns", choices=["default", "radec"], default="default")
    p.add_argument("--algos", default="lsdb,astropy,smatch", help="comma list of lsdb,astropy,smatch,scipy,stilts")
    p.add_argument("--stilts-jar", default=STILTS_JAR_DEFAULT)
    p.add_argument("--workers", default="1,4,16", help="comma list of Dask worker counts")
    p.add_argument("--steps", default="cone:115,cone:375,cone:1150,cone:3700,pixels:1,pixels:2,pixels:4,pixels:8,pixels:16,pixels:32,pixels:64,pixels:128",
                   help="comma list of ladder steps: cone:<arcsec> (inside the seed partition) or pixels:<k> (k nearest partitions)")
    p.add_argument("--n-runs", type=int, default=5)
    p.add_argument("--results-dir", default=str(Path(__file__).resolve().parent / "results"))
    p.add_argument("--config-name", default=None, help="results subdirectory; default = columns mode")
    p.add_argument("--mem-cap-gb", type=float, default=128.0)
    p.add_argument("--threads-per-worker", type=int, default=1, help="Dask threads per worker (0 = Dask default n_cpus//n_workers, the 2025 behaviour)")
    p.add_argument("--dask-local-dir", default=default_dask_local_dir(),
                   help="Dask worker spill/temp directory (default: /lscratch/$USER/dask-xmatch-bench when /lscratch exists)")
    p.add_argument("--no-warmup", action="store_true", help="skip the untimed warm-up read (cold page cache)")
    p.add_argument("--rows-band", default=None, metavar="LO,HI",
                   help="use only ZTF partitions with LO <= rows <= HI for the seed and the pixels:k ladder (e.g. 2.5e6,3.5e6), so all tasks are alike")
    p.add_argument("--output", choices=["write", "count"], default="write",
                   help="write: every tool writes its crossmatch to parquet under --out-dir (lsdb: to_hats; astropy/smatch: streamed per ZTF file), "
                        "row count read back from disk. count: lsdb map_partitions(len), astropy/smatch hold the result in memory (2025 design)")
    p.add_argument("--out-dir", default=default_out_dir(),
                   help="where --output write puts catalogs (deleted after each repeat unless --keep-output); "
                        "default: $XMATCH_OUT_DIR, else /sdf/data/rubin/user/$USER/xmatch-bench-out, else $SCRATCH/xmatch-bench-out")
    p.add_argument("--keep-output", action="store_true")
    p.add_argument("--lsdb-io", action="store_true", help="also time plain lsdb loads of each catalog (informational)")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--calibrate", action="store_true", help="one lsdb run per step, print result rows, no per-run JSON")
    p.add_argument("--show-ladder", type=int, default=0, metavar="N", help="print the N nearest partitions to the seed and exit")
    p.add_argument("--smatch-orientation", choices=["right", "left"], default=None,
                   help="left: smatch.match(ztf, r, gaia, maxmatch=1), the plain call (default in native mode; slow on sorted input); "
                        "right: Gaia indexed, all pairs + per-ZTF nearest with a lexsort (default in stream mode)")
    p.add_argument("--smatch-shuffle", action="store_true",
                   help="pass smatch's second argument in random order (its HEALPix tree is an unbalanced BST that HATS order degenerates into a chain); "
                        "indices are mapped back and the permutation is inside the timed region")
    p.add_argument("--tool-mode", choices=["native", "stream"], default="native",
                   help="native: every non-LSDB tool loads both tables and makes one call, result written once (default); "
                        "stream: ZTF streamed per partition file against one right-hand index (the 2026-09-16 design)")
    p.add_argument("--stilts-disk", action="store_true", help="run STILTS with -disk (tables backed by temp files); default: in the Java heap")
    p.add_argument("--time-cap-s", type=float, default=0.0,
                   help="kill a non-LSDB tool run after this many seconds and skip that tool at larger steps (0 = none)")
    p.add_argument("--seed", default=None, metavar="ORDER,PIXEL", help="force the seed partition (default: smallest with Dec>-10, |b|>30)")
    p.add_argument("--seed-radec", default=None, metavar="RA,DEC", help="cone centre in degrees (overrides --seed; the nearest partition centre becomes the 'seed partition')")
    p.add_argument("--tool-threads", type=int, default=None,
                   help="thread cap for the non-LSDB tools: scipy query workers, pyarrow decode in the child, STILTS runner=parallel<n> (default: max --workers)")
    p.add_argument("--calibrate-cones", default=None, metavar="N1,N2,...",
                   help="find the cone radii around the seed that hold N1, N2, ... ZTF rows, print them as a --steps string, write results/cone_calibration_<config>.json, exit")
    p.add_argument("--ztf-path", default=ZTF_PATH, help="HATS path of the left catalog (default: ZTF DR23 objects)")
    p.add_argument("--gaia-path", default=GAIA_PATH, help="HATS path of the right catalog or collection (default: Gaia DR3 collection)")
    a = p.parse_args(argv)
    a.algos = [s for s in a.algos.split(",") if s]
    a.workers = [int(w) for w in a.workers.split(",") if w]
    a.steps = [s for s in a.steps.split(",") if s]
    a.config_name = a.config_name or a.columns
    a.seed = tuple(int(x) for x in a.seed.split(",")) if a.seed else None
    a.rows_band = tuple(float(x) for x in a.rows_band.split(",")) if a.rows_band else None
    if a.rows_band and not (len(a.rows_band) == 2 and a.rows_band[0] < a.rows_band[1]):
        p.error("--rows-band needs LO,HI with LO < HI")
    a.threads_per_worker = a.threads_per_worker or None
    a.seed_radec = tuple(float(x) for x in a.seed_radec.split(",")) if a.seed_radec else None
    a.tool_threads = a.tool_threads or max(a.workers)
    a.calibrate_cones = [int(float(x)) for x in a.calibrate_cones.split(",") if x] if a.calibrate_cones else None
    a.smatch_orientation = a.smatch_orientation or ("left" if a.tool_mode == "native" else "right")
    return a


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout, force=True)
    import dask

    dask.config.set({"logging": {"distributed": "warning", "distributed.scheduler": "warning",
                                 "distributed.worker": "warning", "distributed.nanny": "warning",
                                 "distributed.core": "warning"}})
    for name in ("distributed", "distributed.scheduler", "distributed.worker", "distributed.nanny", "distributed.core", "bokeh", "tornado"):
        logging.getLogger(name).setLevel(logging.WARNING)
    args = parse_args(argv)
    global ZTF_PATH, GAIA_PATH
    ZTF_PATH = args.ztf_path
    GAIA_PATH = args.gaia_path
    if args.calibrate_cones:
        calibrate_cones(args)
    elif args.calibrate:
        calibrate(args)
    else:
        run(args)


if __name__ == "__main__":
    main()
