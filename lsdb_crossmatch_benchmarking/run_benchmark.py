import json
import traceback
import logging
import time
from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from sys import stdout
import astropy

import dask
import dask.distributed
import dask.dataframe as dd
import lsdb
import numpy as np
import pandas as pd
import pyarrow as pa
import smatch
from astropy.coordinates import (
    Angle,
    SkyCoord,
    match_coordinates_sky,
    search_around_sky,
)
from lsdb.core.search.moc_search import MOCSearch
from lsdb.core.search.cone_search import ConeSearch
import hats
from memory_profiler import profile
import os
from datetime import datetime
import json


dask.config.set({"dataframe.convert-string": False})


@dataclass
class Measurement:
    time: float
    result: object

def prepare_report(measurements):
    if isinstance(measurements, dict):
        return {k: prepare_report(v) for k, v in measurements.items()}
    elif isinstance(measurements, Measurement):
        try:
            result_length = len(measurements.result)
            if list(measurements.result.columns) == ["len"]:
                result_length = sum(measurements.result["len"])
        except AttributeError:
            result_length = None
        return {
            "time": measurements.time,
            "result_length": result_length,
        }
    else:
        return measurements


def measure(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        t = time.monotonic()
        result = func(*args, **kwargs)
        dt = time.monotonic() - t
        return Measurement(
            time=dt,
            result=result,
        )

    return wrapper

@measure
def run_lsdb(left, right, *, xmatch_radius_arcsec, xmatch_n_neighbors):

    return left.crossmatch(
        right,
        radius_arcsec=xmatch_radius_arcsec,
        n_neighbors=xmatch_n_neighbors,
    ).map_partitions(lambda df: pd.DataFrame({"len": [len(df)]})).compute()

@measure
def dfs_to_skycoords(
        left, right, **kwargs
):
    del kwargs

    return (
        SkyCoord(
            ra=left["ra"],
            dec=left["dec"],
            unit="deg",
        ),
        SkyCoord(
            ra=right["ra"],
            dec=right["dec"],
            unit="deg",
        ),
    )


@measure
def run_astropy(
        left_df,
        right_df,
        left_coord,
        right_coord,
        *,
        xmatch_radius_arcsec,
        xmatch_n_neighbors,
        **kwargs,
):
    del kwargs

    assert (
            xmatch_n_neighbors == 1
    ), f"run_astropy doesn't support --xmatch-n-neighbors != 1, {xmatch_n_neighbors} is given"

    idx_right, d2, _d3 = match_coordinates_sky(left_coord, right_coord, nthneighbor=1)
    result = pd.concat(
        [
            left_df.reset_index(drop=True),
            right_df.iloc[idx_right].reset_index(drop=True).add_suffix('_right'),
            pd.DataFrame({"_dist_arcsec": d2.to_value("arcsec")}),
        ],
        axis=1,
    )
    return result[d2 < Angle(xmatch_radius_arcsec, "arcsec")]


@measure
def run_smatch(
        left_df,
        right_df,
        *,
        xmatch_radius_arcsec,
        xmatch_n_neighbors,
        **kwargs
):
    del kwargs

    match = smatch.match(
        left_df["ra"],
        left_df["dec"],
        xmatch_radius_arcsec / 3600.0,
        right_df["ra"],
        right_df["dec"],
        maxmatch=1,
    )
    dist_arcsec = np.degrees(np.arccos(match['cosdist'])) * 3600
    return pd.concat(
        [
            left_df.iloc[match['i1']],
            right_df.iloc[match['i2']].reset_index(drop=True),
            pd.DataFrame({"_dist_arcsec": dist_arcsec})
        ]
    )

@measure
def load_parquet_paths(paths, **kwargs):
    return pd.read_parquet(paths, engine="pyarrow", **kwargs).set_index("_healpix_29")

def load_catalog_with_pyarrow(cat: lsdb.Catalog, base_path, **kwargs):
    healpix_pixels = cat.get_healpix_pixels()
    paths = [hats.io.paths.pixel_catalog_file(base_path, p) for p in healpix_pixels]
    schema = cat.hc_structure.schema
    return load_parquet_paths(paths, columns=list(cat.columns) + [cat._ddf.index.name], schema=schema, **kwargs)

@measure
def load_catalogs_with_dask(cats):
    new_cats = [cat.map_partitions(lambda df: pd.DataFrame({"len": [len(df)]}))._ddf for cat in cats]
    return dd.concat(new_cats, ignore_unknown_divisions=True).compute()


def run_benchmarks(n_runs: int, n_partitions_list: list[int], n_workers_list: list[int], algos, results_dir, additional_filters=None):
    out_dir = os.path.join(results_dir, f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}")
    os.mkdir(out_dir)
    for run_ind in range(n_runs):
        measurements = {}
        print(f"Starting Run {run_ind}", flush=True)
        for npartitions in n_partitions_list:
            print(f"Using {npartitions} partitions", flush=True)
            measurements[npartitions] = {}
            if additional_filters is None:
                additional_filters = [None]
            for filt in additional_filters:
                current_measurements = {}
                ztf = lsdb.read_hats("/ocean/projects/phy210048p/shared/hats/catalogs/ztf_dr14/ztf_object", columns=["ra", "dec"]).partitions[:npartitions]
                if filt is not None:
                    ztf = ztf.search(filt)
                gaia = lsdb.read_hats("/ocean/projects/phy210048p/shared/hats/catalogs/gaia_dr3/gaia", columns=["ra", "dec"], margin_cache="/ocean/projects/phy210048p/shared/hats/catalogs/gaia_dr3/gaia_10arcs").search(MOCSearch(ztf.hc_structure.moc, fine=False))
                measurements["settings"] = {"left_columns": ", ".join(ztf.columns), "right_columns": ", ".join(gaia.columns)}
                if "lsdb" in algos:
                    current_measurements["lsdb"] = {}
                    for worker_num in n_workers_list:
                        current_measurements["lsdb"][worker_num] = {}
                        print(f"Starting Dask client with {worker_num} workers", flush=True)
                        try:
                            _client = dask.distributed.Client(n_workers=worker_num)
                            print("Loading Catalogs with LSDB", flush=True)
                            current_measurements["lsdb"][worker_num]["total_dask"] = load_catalogs_with_dask([ztf, gaia, gaia.margin])
                            print("Crossmatching with LSDB", flush=True)
                            current_measurements["lsdb"][worker_num]["lsdb_crossmatch"] = run_lsdb(ztf, gaia, xmatch_radius_arcsec=10, xmatch_n_neighbors=1)
                            _client.close(timeout=600)
                        except Exception as e:
                            logging.error(traceback.format_exc())
                            pass
                
                if "astropy" in algos or "smatch" in algos:
                    print("Loading Catalogs with pandas", flush=True)
                    current_measurements["ztf_pyarrow"] = load_catalog_with_pyarrow(ztf, "/ocean/projects/phy210048p/shared/hats/catalogs/ztf_dr14/ztf_object")
                    current_measurements["gaia_pyarrow"] = load_catalog_with_pyarrow(gaia, "/ocean/projects/phy210048p/shared/hats/catalogs/gaia_dr3/gaia")
                    current_measurements["gaia_margin_pyarrow"] = load_catalog_with_pyarrow(gaia.margin, "/ocean/projects/phy210048p/shared/hats/catalogs/gaia_dr3/gaia_10arcs")
                    gaia_total_df = pd.concat([current_measurements["gaia_pyarrow"].result, current_measurements["gaia_margin_pyarrow"].result])
                    ztf_df = current_measurements["ztf_pyarrow"].result
                    gaia_df = gaia_total_df
                    if filt is not None:
                        ztf_df = ztf.compute()
                        gaia_df = gaia.compute()
                if "astropy" in algos:
                    print("Initializing skycoords", flush=True)
                    skycoords = dfs_to_skycoords(ztf_df, gaia_total_df)
                    print("Running astropy crossmatch", flush=True)
                    astropy_xmatch = run_astropy(ztf_df, gaia_df, *skycoords.result, xmatch_radius_arcsec=10, xmatch_n_neighbors=1)
                    current_measurements["skycoords"] = skycoords
                    current_measurements["astropy_xmatch"] = astropy_xmatch
                if "smatch" in algos:
                    print("Running smatch crossmatch", flush=True)
                    smatch_xmatch = run_smatch(ztf_df, gaia_df, xmatch_radius_arcsec=10, xmatch_n_neighbors=1)
                    current_measurements["smatch_xmatch"] = smatch_xmatch
                if filt is not None:
                    measurements[npartitions][filt.radius_arcsec] = current_measurements
                else:
                    measurements[npartitions] = current_measurements
        out_path = os.path.join(out_dir, f"run_{run_ind}.json")
        out_dict = prepare_report(measurements)
        with open(out_path, 'w') as fp:
            json.dump(out_dict, fp)


if __name__ == '__main__':
    # run_benchmarks(5, [1, 10, 100], [1, 4, 16], ["lsdb", "astropy"], "/jet/home/mcguires/lsdb/benchmark_results/")
    # run_benchmarks(2, [500], [1, 4, 16], ["lsdb", "astropy"], "/jet/home/mcguires/lsdb/benchmark_results/")
    # run_benchmarks(2, [1000, 2352], [16], ["lsdb"], "/jet/home/mcguires/lsdb/benchmark_results/")
    # run_benchmarks(2, [1000, 2352], [1, 4], ["lsdb"], "/jet/home/mcguires/lsdb/benchmark_results/")
    # run_benchmarks(2, [1, 10], [1], ["smatch"], "/jet/home/mcguires/lsdb/benchmark_results/")
    run_benchmarks(5, [1], [16], ["lsdb", "astropy"], "/jet/home/mcguires/lsdb/benchmark_results/", additional_filters = [ConeSearch(45, 4.78, 300), ConeSearch(45, 4.78, 1000), ConeSearch(45, 4.78, 3000)])
    
