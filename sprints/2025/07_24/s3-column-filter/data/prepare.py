#!/usr/bin/env python

import argparse

import numpy as np
import pyarrow.parquet as pq
from upath import UPath


def download_files(force: bool = False) -> list[UPath]:
    files = []
    with UPath("list.txt").open() as url_list:
        for line in url_list:
            line = line.strip()
            if not line:
                continue
            url, filename = line.split()
            src = UPath(url)
            dst = UPath(filename)
            files.append(dst)
            if not force and dst.exists():
                continue
            print(f"Downloading {src} to {dst}")
            dst.write_bytes(src.read_bytes())
    return files


def add_suffix(src: UPath, suffix: str) -> UPath:
    return src.parent / f"{src.stem}-{suffix}.parquet"


def sort_and_split(
        inp: str | UPath,
        *,
        name: str | None,
        column: str,
        row_group_size: int,
        force: bool = False
) -> UPath:
    inp = UPath(inp)
    name = name or column
    output = add_suffix(inp, f"{name}-{row_group_size}")
    if not force and output.exists():
        return output
    print(f"Converting {inp} to {output}")
    with inp.open("rb") as f:
        table = pq.read_table(f)

    table = table.sort_by(column)

    with output.open("wb") as f:
        pq.write_table(table, f, row_group_size=row_group_size)

    return output


def deeper_healpix(file: str | UPath, *, order: int, diff_order: int, force: bool = False) -> UPath:
    inp = UPath(file)
    output = add_suffix(inp, f"healpix-{diff_order}")
    if not force and output.exists():
        return output
    print(f"Converting {inp} to {output}")
    with inp.open("rb") as f:
        table = pq.read_table(f)
    table = table.sort_by("_healpix_29")

    healpix29_table_range = 1 << (2 * (29 - order))
    healpix29_table_start = table["_healpix_29"][0].as_py() // healpix29_table_range * healpix29_table_range
    healpix29_table_end = healpix29_table_start + healpix29_table_range

    healpix29_rowgroup_range = healpix29_table_range >> (2 * diff_order)

    with output.open("wb") as f, pq.ParquetWriter(f, table.schema) as pw:
        idx_start = 0
        for healpix29_rowgroup_start in range(healpix29_table_start, healpix29_table_end + healpix29_rowgroup_range, healpix29_rowgroup_range):
            healpix29_rowgroup_end = healpix29_rowgroup_start + healpix29_rowgroup_range
            idx_end = np.searchsorted(table["_healpix_29"], healpix29_rowgroup_end)
            pw.write_table(table[idx_start:idx_end], row_group_size=1 << 30)
            idx_start = idx_end

    return output


def parse_filename(file: str | UPath) -> tuple[str, int, int]:
    stem = UPath(file).stem
    prefix, order, pix = stem.rsplit('-', maxsplit=2)
    return prefix, int(order), int(pix)


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare data for benchmarking")
    parser.add_argument("-f", "--force", action="store_true", help="Force download and prepare data")
    args = parser.parse_args(argv)
    return args


def main(argv: list[str] | None = None):
    args = parse_arguments(argv)

    original_files = download_files(force=args.force)

    for binary_exp in range(10, 22, 2):
        row_group_size = 1 << binary_exp
        for orig_file in original_files:
            _ = sort_and_split(orig_file, name=None, column="_healpix_29", row_group_size=row_group_size, force=args.force)

    for binary_exp in range(10, 22, 2):
        row_group_size = 1 << binary_exp
        for orig_file in original_files:
            prefix, _order, _pix = parse_filename(orig_file)
            if "ztf" in prefix:
                column = "nepochs"
            elif "gaia" in prefix:
                column = "phot_g_mean_mag"
            else:
                raise ValueError(f"Unsupported prefix: {prefix}")

            _ = sort_and_split(orig_file, name="filter", column=column, row_group_size=row_group_size, force=args.force)

    for binary_exp in range(10, 22, 2):
        row_group_size = 1 << binary_exp
        for orig_file in original_files:
            prefix, _order, _pix = parse_filename(orig_file)
            if "ztf" in prefix:
                column = "objectid"
            elif "gaia" in prefix:
                column = "source_id"
            else:
                raise ValueError(f"Unsupported prefix: {prefix}")

            _ = sort_and_split(orig_file, name="id", column=column, row_group_size=row_group_size, force=args.force)

    for diff_order in range(1, 7):
        for orig_file in original_files:
            _prefix, order, _pix = parse_filename(orig_file)
            _ = deeper_healpix(orig_file, order=order, diff_order=diff_order)


if __name__ == "__main__":
    main()
