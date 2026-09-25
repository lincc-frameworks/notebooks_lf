#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "numpy",
#     "pyarrow",
#     "scipy",
# ]
# ///
"""Benchmark parquet writing options on the median-sized file of every HATS catalog.

Every file is read and written back into memory twice: once the way hats-import
writes a catalog today, and once with the option under test added. The two copies
are then read back over many rounds, shuffled within each round, and compared with
a paired t-test on the per-round differences. Nothing is compared against the file
on disk -- that was written by whatever hats-import and pyarrow were current at the
time -- so `baseline` is the copy every measurement is made against.
"""

import argparse
import os
import random
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scipy import stats

REFERENCE = "baseline"
DEFAULT_WRITER = "page-index-64k"
SPATIAL_INDEX_COLUMN = "_healpix_29"

# Parquet spells a float column one of these ways: half floats become a two-byte
# fixed-length array carrying a Float16 logical type.
FLOAT_PHYSICAL_TYPES = frozenset({"FLOAT", "DOUBLE"})
FLOAT_LOGICAL_TYPE = "Float16"

# Rows written twice, once per encoding, to find out which one a column prefers.
SAMPLE_ROWS = 250_000

# Only the main catalogs. A margin, index, map or association partition is written
# by the same code but holds a different shape of data, and answers a different
# question than the one these measurements are about.
DATAPRODUCT_TYPE = "object"
PIXEL_PATTERN = re.compile(r"Norder=(\d+).*\bNpix=(\d+)")

# Warm-up budget per copy, whichever limit is reached first.
WARMUP_SECONDS = 0.1
WARMUP_READS = 200

# hats_import.runtime_arguments.RuntimeArguments.write_table_kwargs, which every
# import path hands to pq.ParquetWriter unless the caller overrides it.
HATS_IMPORT_KWARGS = {"compression": "ZSTD", "compression_level": 15}


def read_properties(path: "Path | str") -> "dict[str, str]":
    """Read a `hats.properties` file: `key=value` lines, `#` starting a comment."""
    values = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()
    return values


def find_median_parquet_files(root: "Path | str", dataproduct_type: str = DATAPRODUCT_TYPE) -> "list[Path]":
    """Return one parquet file per HATS catalog found under `root`.

    A catalog is any directory containing `hats.properties`, and only those whose
    `dataproduct_type` matches are kept. For each we take all of its parquet files
    and return the one with the median size (lower median for an even count).
    Catalogs with no parquet files are skipped.
    """
    root = Path(root)
    median_files = []
    for properties_path in sorted(root.rglob("hats.properties")):
        if read_properties(properties_path).get("dataproduct_type") != dataproduct_type:
            continue
        catalog_dir = properties_path.parent
        parquet_files = sorted(catalog_dir.rglob("*.parquet"))
        if not parquet_files:
            continue
        parquet_files.sort(key=lambda path: path.stat().st_size)
        median_files.append(parquet_files[(len(parquet_files) - 1) // 2])
    return median_files


def describe(path: Path) -> str:
    """Label a partition the way HATS addresses it: its catalog, then its pixel.

    Index and margin partitions carry no pixel in their path, so those fall back
    to the file name to stay distinguishable from their catalog's other files.
    """
    catalog = next(
        (parent.name for parent in path.parents if (parent / "hats.properties").exists()),
        path.parent.name,
    )
    pixel = PIXEL_PATTERN.search(str(path))
    return f"{catalog} ({pixel[1]}, {pixel[2]})" if pixel else f"{catalog} ({path.stem})"


def read_table(path: "Path | str") -> pa.Table:
    """Read a parquet file into one contiguous table.

    Reading splits the columns into chunks of its own, and the writer would turn
    those chunk boundaries into page boundaries, which hats-import -- writing a
    table it has just merged -- would not do.
    """
    return pq.read_table(path).combine_chunks()


def write(table: pa.Table, **kwargs) -> pa.Buffer:
    """Write the table into memory the way hats-import writes a catalog partition."""
    kwargs = HATS_IMPORT_KWARGS | kwargs
    if SPATIAL_INDEX_COLUMN in table.schema.names:
        ordering = [(SPATIAL_INDEX_COLUMN, "ascending")]
        kwargs.setdefault("sorting_columns", pq.SortingColumn.from_ordering(table.schema, ordering))
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, **kwargs)
    return sink.getvalue()


VARIANTS = {}


def variant(func):
    """Register a `pa.Table -> pa.Buffer` writer to benchmark."""
    VARIANTS[func.__name__.replace("_", "-")] = func
    return func


@variant
def baseline(table: pa.Table) -> pa.Buffer:
    """Today's hats-import output. Choosing it as the writer measures the noise floor."""
    return write(table)


@variant
def page_index(table: pa.Table) -> pa.Buffer:
    return write(table, write_page_index=True)


@variant
def page_index_64k(table: pa.Table) -> pa.Buffer:
    """Page index over smaller pages, to prune at a finer granularity.

    Pages are capped at 32768 values whatever `data_page_size` says, so the
    default 1 MiB budget never binds for a narrow column and every partition
    written today has 32768-row pages. 64 KiB is the first setting that bites:
    8192 rows for an 8-byte column, four times finer, and it applies to every
    column -- pyarrow takes one page size for the whole file.
    """
    return write(table, write_page_index=True, data_page_size=64 * 1024)


@variant
def page_index_128k(table: pa.Table) -> pa.Buffer:
    """Half the pruning granularity of `page-index-64k`, for half the page count.

    16384 rows for an 8-byte column. A 4-byte column is unchanged from the
    default: 32768 of its values already fit in 128 KiB, so the cap binds first.
    """
    return write(table, write_page_index=True, data_page_size=128 * 1024)


def split_leaves(table: pa.Table) -> "tuple[list[str], list[str]]":
    """Split the parquet leaf columns into the float-like ones and the rest.

    The paths have to come from the writer rather than the arrow schema: a nested
    leaf is addressed as `sources.list.element.mag`, and pyarrow renames list
    children on the way in, so writing an empty slice and reading its schema back
    is the only way to learn the names the writer will actually use.
    """
    sink = pa.BufferOutputStream()
    pq.write_table(table.slice(0, 0), sink)
    schema = pq.ParquetFile(pa.BufferReader(sink.getvalue())).schema

    floats, others = [], []
    for index in range(len(schema)):
        column = schema.column(index)
        is_float = column.physical_type in FLOAT_PHYSICAL_TYPES or str(column.logical_type).startswith(
            FLOAT_LOGICAL_TYPE
        )
        (floats if is_float else others).append(column.path)
    return floats, others


@variant
def byte_stream_split(table: pa.Table) -> pa.Buffer:
    """Byte stream split for every float column, dictionary for everything else.

    Splitting floats into byte planes groups the sign and exponent bytes together,
    which zstd compresses far better than interleaved mantissas, while a dictionary
    does nothing for floats that are nearly all distinct. The two encodings are
    mutually exclusive per column, so each leaf gets exactly one, nested leaves
    included. Passing True instead of a list is rejected outright: a string column
    cannot be byte stream split.
    """
    floats, others = split_leaves(table)
    return write(table, use_byte_stream_split=floats, use_dictionary=others)


def column_sizes(buffer: pa.Buffer) -> "dict[str, int]":
    """Compressed bytes per leaf column, summed over row groups."""
    metadata = pq.read_metadata(pa.BufferReader(buffer))
    sizes = {}
    for group in range(metadata.num_row_groups):
        row_group = metadata.row_group(group)
        for index in range(metadata.num_columns):
            column = row_group.column(index)
            sizes[column.path_in_schema] = sizes.get(column.path_in_schema, 0) + column.total_compressed_size
    return sizes


@variant
def byte_stream_split_auto(table: pa.Table) -> pa.Buffer:
    """Byte stream split only on the float columns where it beats a dictionary.

    Splitting bytes helps a float column whose values are all distinct and hurts
    one that repeats itself -- a magnitude rounded to millimags, a -99 fill, a
    mostly-null error column -- because it throws away exactly what a dictionary
    exploits. Type alone cannot tell those apart, so both encodings are written
    on a sample and each column keeps its winner.

    The sample can mislead where a dictionary only overflows its page limit at
    full size, so a column near that boundary may be judged on the wrong side.
    """
    floats, others = split_leaves(table)
    if not floats:
        return write(table, use_dictionary=others)

    sample = table.slice(0, SAMPLE_ROWS)
    split_sizes = column_sizes(write(sample, use_byte_stream_split=floats, use_dictionary=others))
    dictionary_sizes = column_sizes(write(sample, use_dictionary=True))

    split = [column for column in floats if split_sizes[column] < dictionary_sizes[column]]
    return write(table, use_byte_stream_split=split, use_dictionary=[c for c in floats + others if c not in split])


def read_seconds(buffer: pa.Buffer) -> float:
    """Time a full `pq.read_table` of an in-memory parquet file."""
    reader = pa.BufferReader(buffer)
    start = time.perf_counter()
    pq.read_table(reader)
    return time.perf_counter() - start


def warm_up(buffer: pa.Buffer) -> None:
    """Read until the timings settle, before any of them count.

    A small file needs tens of reads to reach its steady state -- the allocator
    grows its pools, the arrow thread pool starts, the clock ramps -- while a
    large one is there after the first. Spending a fixed budget of time rather
    than a fixed number of reads covers both without dragging on big files.
    """
    spent = 0.0
    for _ in range(WARMUP_READS):
        if spent >= WARMUP_SECONDS:
            return
        spent += read_seconds(buffer)


def measure(buffers: "dict[str, pa.Buffer]", repeat: int, rng: random.Random) -> "dict[str, np.ndarray]":
    """Time every buffer over `repeat` rounds, shuffling the order within a round.

    Shuffling, and comparing the copies round by round, keeps slow drift --
    thermal throttling, background load -- from being attributed to a writer.
    """
    names = list(buffers)
    for name in names:
        warm_up(buffers[name])

    times = {name: [] for name in names}
    for _ in range(repeat):
        rng.shuffle(names)
        for name in names:
            times[name].append(read_seconds(buffers[name]))
    return {name: np.array(times[name]) for name in buffers}


def paired_t_test(times: np.ndarray, reference_times: np.ndarray):
    """The t and p values of the per-round time differences against the reference."""
    result = stats.ttest_rel(times, reference_times)
    return result.statistic, result.pvalue


@dataclass
class Row:
    """One file's measurements for the chosen writer."""

    path: Path
    size: int
    size_percent: float
    read_ms: float
    difference_ms: float
    t: float
    p: float


def benchmark(path: Path, writer: str, repeat: int, seed: int) -> Row:
    """Write one file both ways and time reading the two copies back."""
    table = read_table(path)
    # Writing the baseline against itself needs two separately written copies,
    # or the two would be one buffer timed once and compared to itself.
    name = f"{writer} copy" if writer == REFERENCE else writer
    buffers = {REFERENCE: write(table), name: VARIANTS[writer](table)}
    times = measure(buffers, repeat=repeat, rng=random.Random(seed))
    t, p = paired_t_test(times[name], times[REFERENCE])

    return Row(
        path=path,
        size=buffers[REFERENCE].size,
        size_percent=100 * (buffers[name].size / buffers[REFERENCE].size - 1),
        read_ms=times[REFERENCE].mean() * 1e3,
        difference_ms=(times[name] - times[REFERENCE]).mean() * 1e3,
        t=t,
        p=p,
    )


def elide(label: str, width: int) -> str:
    """Shorten a path to `width`, cutting out the middle where the boilerplate sits."""
    if len(label) <= width:
        return label
    head = (width - 1) // 2
    return label[:head] + "…" + label[head + 1 - width :]


def print_header(labels: "list[str]", writer: str, repeat: int, root: Path) -> "tuple[int, int]":
    """Print the table's heading and return the column widths its rows must use.

    The widths come from the labels rather than the measurements, so the heading
    can go out before the first file is timed and every row can follow as soon as
    it is measured -- a sweep of a real catalog takes a while to finish.
    """
    width = min(max(len(label) for label in labels), 52)
    percent = max(len(writer) + 5, 11)

    print(f"{root}\n  {writer} against {REFERENCE}, {repeat} rounds per file, paired t-test")
    header = (
        f"  {'file':<{width}}{REFERENCE + ', bytes':>17}{writer + ', %':>{percent}}"
        f"{'read, ms':>11}{'diff, ms':>11}{'t':>8}{'p':>10}"
    )
    print(header)
    print("  " + "-" * (len(header) - 2), flush=True)
    return width, percent


def print_row(row: Row, label: str, width: int, percent: int) -> None:
    """Print one measured file. Flushed, so a pipe shows it before the run ends."""
    print(
        f"  {elide(label, width):<{width}}{row.size:>17,d}{row.size_percent:>+{percent}.3f}"
        f"{row.read_ms:>11.3f}{row.difference_ms:>+11.3f}{row.t:>8.1f}{row.p:>10.3g}"
        f"{'  *' if row.p < 0.05 else ''}",
        flush=True,
    )


def print_legend(rows: "list[Row]", writer: str, repeat: int) -> None:
    """Explain the columns, once every file has been measured."""
    faster = sum(1 for row in rows if row.p < 0.05 and row.difference_ms < 0)
    slower = sum(1 for row in rows if row.p < 0.05 and row.difference_ms > 0)
    compression = ", ".join(f"{key}={value}" for key, value in HATS_IMPORT_KWARGS.items())
    columns = [
        ("file", "catalog and (Norder, Npix), or the file name where the path carries no pixel"),
        (f"{REFERENCE}, bytes", f"size of the {REFERENCE} copy, written as hats-import writes today: {compression}"),
        (f"{writer}, %", f"how much bigger the {writer} copy is than the {REFERENCE} one"),
        ("read, ms", f"mean pq.read_table time of the {REFERENCE} copy over {repeat} rounds"),
        ("diff, ms", f"mean per-round time difference, {writer} minus {REFERENCE}; negative is faster"),
        ("t, p", f"paired t-test over those {repeat} per-round differences, * marks p < 0.05"),
    ]
    width = max(len(label) for label, _ in columns)

    print()
    for label, description in columns:
        print(f"  {label:<{width}}  {description}")
    print(
        f"\n  * {faster} faster, {slower} slower, of {len(rows)} files. Two identical copies still differ"
        f"\n    at 95% about once in twenty, so a handful of stars here is noise, not a result."
    )


def parse_args(argv: "list[str] | None" = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("path", type=Path, help=f"directory to search for {DATAPRODUCT_TYPE} catalogs")
    parser.add_argument(
        "-w",
        "--writer",
        choices=list(VARIANTS),
        default=DEFAULT_WRITER,
        help="the writing option to measure (default: %(default)s)",
    )
    parser.add_argument("-n", "--repeat", type=int, default=15, help="number of read rounds")
    parser.add_argument("--seed", type=int, default=0, help="seed for shuffling the read order")
    return parser.parse_args(argv)


def main():
    args = parse_args()

    files = find_median_parquet_files(args.path)
    if not files:
        sys.exit(f"no {DATAPRODUCT_TYPE} catalogs under {args.path}")

    labels = [describe(path) for path in files]
    width, percent = print_header(labels, args.writer, args.repeat, args.path)

    rows = []
    for path, label in zip(files, labels):
        row = benchmark(path, args.writer, repeat=args.repeat, seed=args.seed)
        rows.append(row)
        print_row(row, label, width, percent)

    print_legend(rows, writer=args.writer, repeat=args.repeat)


if __name__ == "__main__":
    try:
        main()
    except BrokenPipeError:
        # Piping into `head` closes the pipe early. Point what is left of stdout at
        # devnull, or the interpreter raises again while flushing it on the way out.
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(1)
