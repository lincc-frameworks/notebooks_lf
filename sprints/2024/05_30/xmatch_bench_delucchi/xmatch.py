#!/usr/bin/env python3

import json
import logging
import time
from argparse import ArgumentParser
from dataclasses import dataclass
from datetime import datetime
from functools import wraps
from sys import stdout

import dask.distributed
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
from lsdb import ConeSearch


@dataclass
class Measurement:
    time: float
    result: object


def prepare_report(measurements, args):
    d = {
        "measurements": {},
        "args": args,
        "date": datetime.now().isoformat(),
    }
    for name, measurement in measurements.items():
        try:
            result_length = len(measurement.result)
        except TypeError:
            result_length = None
        d["measurements"][name] = {
            "time": measurement.time,
            "result_length": result_length,
        }
    return d


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


def get_lsdb_catalogs(
    *,
    left_catalog_path,
    left_catalog_columns,
    right_catalog_path,
    right_margin_path,
    right_catalog_columns,
    cone_radius_deg,
    cone_ra_deg,
    cone_dec_deg,
    **kwargs,
):
    del kwargs
    
    cone = ConeSearch(ra=cone_ra_deg, dec=cone_dec_deg, radius_arcsec=cone_radius_deg * 3600, fine=False)

    left = lsdb.read_hats(
        left_catalog_path,
        columns=left_catalog_columns,
        search_filter=cone,
    )
    
    right = lsdb.read_hats(
        right_catalog_path,
        columns=right_catalog_columns,
        search_filter=cone,
        margin_cache=right_margin_path,
    )

    return left, right


def catalog_persist_to_df(catalog):
    df = catalog._ddf.persist().compute()
    return df.reset_index(drop=True)


@measure
def run_lsdb(left, right, *, xmatch_radius_arcsec, xmatch_n_neighbors, **kwargs):
    del kwargs

    return left.crossmatch(
        right,
        radius_arcsec=xmatch_radius_arcsec,
        n_neighbors=xmatch_n_neighbors,
    ).compute()


@measure
def dfs_to_skycoords(
    left, right, *, left_catalog_columns, right_catalog_columns, **kwargs
):
    del kwargs

    return (
        SkyCoord(
            ra=left[left_catalog_columns[0]],
            dec=left[left_catalog_columns[1]],
            unit="deg",
        ),
        SkyCoord(
            ra=right[right_catalog_columns[0]],
            dec=right[right_catalog_columns[1]],
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
            left_df,
            right_df.iloc[idx_right].reset_index(drop=True),
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
    left_catalog_columns,
    right_catalog_columns,
    xmatch_radius_arcsec,
    xmatch_n_neighbors,
    **kwargs
):
    del kwargs
    
    match = smatch.match(
        left_df[left_catalog_columns[0]],
        left_df[left_catalog_columns[1]],
        xmatch_radius_arcsec / 3600.0,
        right_df[right_catalog_columns[0]],
        right_df[right_catalog_columns[1]],
    )
    dist_arcsec = np.degrees(np.arccos(match['cosdist'])) * 3600
    return pd.concat(
        [
            left_df.iloc[match['i1']],
            right_df.iloc[match['i2']].reset_index(drop=True),
            pd.DataFrame({"_dist_arcsec": dist_arcsec})
        ]
    )


def parse_args(cli_args):
    parser = ArgumentParser()
    parser.add_argument("algo", nargs="+", choices=["lsdb", "astropy", "smatch"])
    parser.add_argument("--xmatch-radius-arcsec", required=True, type=float)
    parser.add_argument("--xmatch-n-neighbors", required=True, type=int)
    parser.add_argument("--cone-radius-deg", required=True, type=float)
    parser.add_argument("--cone-ra-deg", default=283.44639, type=float)
    parser.add_argument("--cone-dec-deg", default=33.06623, type=float)
    parser.add_argument("--dask-n-workers", required=True, type=int)
    parser.add_argument(
        "--left-catalog-path",
        default="/epyc/data3/hats/catalogs/ztf_dr14/ztf_object",
    )
    parser.add_argument(
        "--left-catalog-columns",
        nargs="+",
        default=["ra", "dec"],
        help="List of columns to load from the left catalog, first two must be RA and Dec",
    )
    parser.add_argument(
        "--right-catalog-path",
        default="/epyc/data3/hats/catalogs/gaia_dr3/gaia",
    )
    parser.add_argument(
        "--right-margin-path",
        default="/epyc/data3/hats/catalogs/gaia_dr3/gaia_10arcs",
    )
    parser.add_argument(
        "--right-catalog-columns",
        nargs="+",
        default=["ra", "dec"],
        help="List of columns to load from the right catalog, first two must be RA and Dec",
    )
    parser.add_argument("--verbose", action='store_true')
    return parser.parse_args(cli_args)


def main(cli_args=None):
    args = vars(parse_args(cli_args))
    
    logging.basicConfig(level=logging.DEBUG if args["verbose"] else logging.INFO)

    logging.debug("Getting LSDB catalogs")
    left_catalog, right_catalog = get_lsdb_catalogs(**args)
    logging.info("Got LSDB catalogs")

    measurements = {}

    logging.debug("Starting Dask client")
    with dask.distributed.Client(n_workers=args["dask_n_workers"],
                                 local_directory="/epyc/data3/hats/tmp/",
                                 threads_per_worker=1) as _client:

        if 'lsdb' in args['algo']:
            logging.debug("Running LSDB cross-matching")
            measurements['lsdb'] = run_lsdb(left_catalog, right_catalog, **args)
            logging.info("LSDB cross=matcing is done")
        else:
            logging.debug("Persisting catalogs and converting to frames")
            dfs = tuple(map(catalog_persist_to_df, [left_catalog, right_catalog]))
            logging.info("Catalogs are persisted and converted to frames")

    if 'astropy' in args['algo']:
        logging.debug("Converting frames to SkyCoord")
        measurements['init_skycoord'] = dfs_to_skycoords(*dfs, **args)
        logging.info("Converted to SkyCoord")

        logging.debug("Running astropy cross-matching")
        measurements['astropy'] = run_astropy(*dfs, *measurements['init_skycoord'].result, **args)
        logging.info("astropy cross-matching is done")
        
    if 'smatch' in args['algo']:
        logging.debug("Runining smatch cross-matching")
        measurements['smatch'] = run_smatch(*dfs, **args)

    logging.debug("Preparing report")
    report = prepare_report(measurements, args)

    logging.debug("Printing report in JSON")
    json.dump(report, stdout, indent=4)
    
    logging.debug("Exiting main()")
    return report


if __name__ == "__main__":
    main()
