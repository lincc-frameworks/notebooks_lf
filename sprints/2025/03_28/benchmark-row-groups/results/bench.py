#!/usr/bin/env python

import argparse
import json
import time
from abc import ABC, abstractmethod
from copy import deepcopy
from functools import lru_cache
from typing import Callable

import numpy as np
from pyarrow import NA
import pyarrow.compute as pc
import pyarrow.parquet as pq
from astropy.coordinates import Angle, Latitude, Longitude, SkyCoord
from mocpy import MOC
from upath import UPath


def read_table(file, *args, **kwargs):
    with file.open("rb") as f:
        try:
            return pq.read_table(f, *args, **kwargs)
        except Exception as e:
            raise RuntimeError(f"Error reading table from {file}: {e}") from e


def timeit(*, n_iter: int, index_sorted: int):
    assert index_sorted < n_iter
    def decorator(func):
        def timeit_wrapper(*args, **kwargs):
            times = []
            for _ in range(n_iter):
                start = time.monotonic()
                _result = func(*args, **kwargs)
                end = time.monotonic()
                times.append(end - start)
            sorted_times = sorted(times)
            return sorted_times[index_sorted]
        return timeit_wrapper
    return decorator


def ra_dec_columns(file: UPath) -> tuple[str, str]:
    if file.name.startswith("gaia"):
        return "ra", "dec"
    if file.name.startswith("ztf"):
        return "objra", "objdec"
    raise ValueError(f"Unsupported file format: {file}")


def get_id_column(file: UPath) -> str:
    if file.name.startswith("gaia"):
        return "source_id"
    if file.name.startswith("ztf"):
        return "objectid"
    raise ValueError(f"Unsupported file format: {file}")


def get_filter_column(file: UPath) -> str:
    if file.name.startswith("gaia"):
        return "phot_g_mean_mag"
    if file.name.startswith("ztf"):
        return "nepochs"
    raise ValueError(f"Unsupported file format: {file}")


class Measurer(ABC):
    weight: float
    name: str

    @abstractmethod
    def measure(self, file: UPath, timeit_decorator, *, columns: list[str] | None) -> float:
        raise NotImplemented


class FullRead(Measurer):
    weight = 4.0
    name = "Full read"

    def measure(self, file: UPath, timeit_decorator, *, columns: list[str] | None) -> float:
        return timeit_decorator(read_table)(file, columns=columns)


class Cone(Measurer):
    def __init__(self, *, name: str, radius_arcsec: float, n_samples: int):
        self.name = name
        self.radius_arcsec = radius_arcsec
        self.n_samples = n_samples

    @lru_cache
    def get_healpix_29_range(self, ra: float, dec: float, radius_arcsec: float) -> tuple[int, int]:
        moc = MOC.from_cone(
            lon=Longitude(ra, "deg"),
            lat=Latitude(dec, "deg"),
            radius=Angle(radius_arcsec, "arcsec"),
            max_depth=29,
        )
        return moc.min_index, moc.max_index

    @abstractmethod
    def get_ra_dec(self, obj_ra: float, obj_dec: float) -> tuple[float, float]:
        raise NotImplemented

    def measure(self, file: UPath, timeit_decorator, *, columns: list[str] | None) -> float:
        ra_column, dec_column = ra_dec_columns(file)
        table = read_table(file, columns=[ra_column, dec_column])
        rng = np.random.default_rng(0)
        idx = rng.choice(table.num_rows, self.n_samples)
        result = 0
        for i in idx:
            ra, dec = self.get_ra_dec(table[ra_column][i].as_py(), table[dec_column][i].as_py())
            min_healpix_29, max_healpix_29 = self.get_healpix_29_range(ra, dec, self.radius_arcsec)
            expression = (pc.field("_healpix_29") >= min_healpix_29) & (pc.field("_healpix_29") <= max_healpix_29)
            result += timeit_decorator(read_table)(file, columns=columns, filters=expression)
        return result


class SmallCone(Cone):
    weight = 1.0

    def __init__(self, *, n_samples: int = 10):
        super().__init__(name="Small cone", radius_arcsec=3.0, n_samples=n_samples)

    def get_ra_dec(self, obj_ra: float, obj_dec: float) -> tuple[float, float]:
        return obj_ra, obj_dec


class LargeCone(Cone):
    weight = 1.0

    def __init__(self, *, n_samples: int = 3):
        super().__init__(name="Large cone", radius_arcsec=3600.0, n_samples=n_samples)
        self.offset_factor = 0.9
        self.offset_arcsec = self.radius_arcsec * self.offset_factor

    def get_ra_dec(self, obj_ra: float, obj_dec: float) -> tuple[float, float]:
        rng = np.random.default_rng(0)
        angle = Angle(rng.uniform(0, 360), "deg")
        offset = Angle(rng.uniform(0, self.offset_arcsec), "arcsec")
        obj_coord = SkyCoord(obj_ra, obj_dec, unit="deg")
        cone_center = obj_coord.directional_offset_by(angle, offset)

        return float(cone_center.ra.deg), float(cone_center.dec.deg)


class SelectById(Measurer):
    weight = 2.0
    name = "Select by ID"

    def __init__(self, *, num_samples: int = 10):
        self.num_samples = num_samples

    def measure(self, file: UPath, timeit_decorator, *, columns: list[str] | None) -> float:
        id_column = get_id_column(file)
        table = read_table(file, columns=[id_column])
        rng = np.random.default_rng(0)
        id_values = rng.choice(table[id_column], self.num_samples)
        result = 0
        for id_value in id_values:
            expression = pc.field(id_column) == id_value
            result += timeit_decorator(read_table)(file, columns=columns, filters=expression)

        return result


class FilterColumn(Measurer):
    def __init__(self, name: str, quantile_range: tuple[float, float]):
        self.name = name
        self.quantile_range = quantile_range

    def measure(self, file: UPath, timeit_decorator, *, columns: list[str] | None) -> float:
        filter_column = get_filter_column(file)
        table = read_table(file, columns=[filter_column])
        q1, q2 = pc.quantile(table[filter_column], q=self.quantile_range).tolist()
        expression = (pc.field(filter_column) >= q1) & (pc.field(filter_column) <= q2)

        result = timeit_decorator(read_table)(file, columns=columns, filters=expression)

        return result


class FilterColumnFewRows(FilterColumn):
    weight = 1.0

    def __init__(self):
        super().__init__("Filter column with few result rows", (0.98, 0.99))

class FilterColumnManyRows(FilterColumn):
    weight = 1.0

    def __init__(self):
        super().__init__("Filter column with many result rows", (0.4, 0.6))


class Runner:
    path_roots = {
        "local": UPath("../data"),
        "remote": UPath("s3://bucket", key="admin", secret="password", endpoint_url="http://localhost:9000"),
    }

    timeit_iters = {"local": 2, "remote": 1}
    timeit_index = {"local": 0, "remote": 0}

    prefixes = {"gaia": "gaia_dr3-2-0", "ztf": "ztf_dr22-6-21554"}

    columns = {
        "gaia": {
            "required": ["_healpix_29", "source_id", "ra", "dec", "phot_g_mean_mag"],
            "default": [
               "_healpix_29", "source_id", "ra", "dec", "parallax", "parallax_over_error", "pmra", "pmdec",
               "phot_g_mean_mag", "phot_bp_mean_mag", "phot_rp_mean_mag", "phot_g_mean_flux_over_error",
               "phot_bp_mean_flux_over_error", "phot_rp_mean_flux_over_error",
            ],
            "all": None,
        },
        "ztf": {
            "required": ["_healpix_29", "objectid", "objra", "objdec", "nepochs"],
            "default": ["_healpix_29", "objectid", "objra", "objdec", "filterid", "hmjd", "mag", "magerr"],
            "all": None,
        }
    }

    def __init__(
            self,
            m: list[Measurer],
            *,
            output: UPath = UPath("./results.json"),
            force: bool = False,
            storages: list[str] | None = None,
            catalogs: list[str] | None = None,
    ):
        self.measurers = m
        self.output = output
        self.force = force
        self.storages = storages or list(self.path_roots)
        self.catalogs = catalogs or list(self.prefixes)

        if not self.output.exists():
            previous_results = {}
        else:
            with self.output.open() as f:
                previous_results = json.load(f)
        self.previous_results = deepcopy(previous_results)
        
        if force:
            for suffix, suffix_results in previous_results.items():
                for measurer, measurer_results in suffix_results.items():
                    for storage, storage_results in measurer_results.items():
                        if storage in self.storages:
                            del self.previous_results[suffix][measurer][storage]
                            continue
                        for catalog, catalog_results in storage_results.items():
                            if catalog in self.catalogs:
                                del self.previous_results[suffix][measurer][storage][catalog]



    def get_suffixes(self):
        root = list(self.path_roots.values())[0]
        prefix = list(self.prefixes.values())[0]
        paths = sorted(root.glob(f"{prefix}*.parquet"))
        suffixes = [path.stem.removeprefix(prefix) for path in paths]
        return suffixes

    def bump(self, results):
        json_string = json.dumps(results, indent=2)
        with self.output.open("w") as f:
            f.write(json_string)

    def run(self):
        results = deepcopy(self.previous_results)

        suffixes = self.get_suffixes()

        for suffix in suffixes:
            print(f'Running for "{suffix}"')
            suffix_results = results.setdefault(suffix, {})

            for m in self.measurers:
                print(f'  Running "{m.name}"')
                m_results = suffix_results.setdefault(m.name, {})

                for storage in self.storages:
                    root = self.path_roots[storage]
                    print(f'    Running for "{storage} storage"')
                    storage_results = m_results.setdefault(storage, {})
                    n_iter = self.timeit_iters[storage]
                    timeit_index = self.timeit_index[storage]
                    timeit_decorator = timeit(n_iter=n_iter, index_sorted=timeit_index)

                    for catalog in self.catalogs:
                        prefix = self.prefixes[catalog]
                        file = root / f"{prefix}{suffix}.parquet"
                        print(f'      Running for "{file.name}"')
                        catalog_results = storage_results.setdefault(catalog, {})

                        for col_type, columns in self.columns[catalog].items():
                            if col_type in catalog_results:
                                print(f'        Skipping "{col_type}" columns')
                                continue
                            print(f'        Running for "{col_type}" columns')
                            result = m.measure(file, timeit_decorator, columns=columns)
                            catalog_results[col_type] = {"time": result, "weight": m.weight}

                            self.bump(results)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmarking tool")
    parser.add_argument("-f", "--force", action="store_true", help="Force benchmarks")
    parser.add_argument("-s", "--storages", choices=list(Runner.path_roots), nargs="+", help="Storages to benchmark")
    parser.add_argument("-c", "--catalogs", choices=list(Runner.prefixes), nargs="+", help="Catalogs to benchmark")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    m = [
        FullRead(),
        SmallCone(),
        LargeCone(),
        SelectById(),
        FilterColumnFewRows(),
        FilterColumnManyRows(),
    ]
    runner = Runner(m, force=args.force, storages=args.storages, catalogs=args.catalogs)
    runner.run()


if __name__ == "__main__":
    main()
