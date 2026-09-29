#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = ["hats", "pyarrow", "distributed", "tqdm", "bokeh>=3.1.0", "dask-jobqueue"]
# ///
"""Copy a HATS catalog/collection to a new path, re-encoding all parquet data."""

import argparse
import os
import shutil
import stat
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from distributed import Client, as_completed
from tqdm import tqdm

from hats.catalog.catalog_type import CatalogType
from hats.catalog.dataset.collection_properties import CollectionProperties
from hats.catalog.dataset.table_properties import TableProperties
from hats.io.parquet_metadata import write_parquet_metadata
from hats.io.size_estimates import estimate_dir_size
from hats.io.validation import is_valid_collection
from hats.pixel_math.spatial_index import SPATIAL_INDEX_COLUMN

COMPRESSION = "zstd"
COMPRESSION_LEVEL = 15
PAGE_SIZE_BYTES = 128 * 1024
INDEX_PAGE_SIZE_BYTES = 16 * 1024
ROW_GROUP_TARGET_BYTES = 100 * 1024 * 1024
ROW_GROUP_MAX_ROWS = 100_000
SIDE_FILE_NAMES = ("data_thumbnail.parquet", "per_partition_statistics.parquet")
THREADS_PER_WORKER = 1


def dask_local_directory() -> Path | None:
    """SLURM job-local scratch dir, if we're on one; otherwise Dask's own default."""
    slurm_local = Path(f"/local/slurm-{os.environ.get('SLURM_JOB_ID')}")
    return slurm_local / "dask-scratch" if slurm_local.is_dir() else None


DASK_LOCAL_DIRECTORY = dask_local_directory()

BRIDGES2_SLURM_KWARGS = dict(
    walltime="01:00:00",
    queue="RM-shared",
    cores=4,
    processes=1,
    memory="16GB",
    job_cpu=8,
)
BRIDGES2_MAX_JOBS = 128


def make_client(workers: str) -> Client:
    """Build a Dask Client: a local cluster with `workers` processes, or a
    distributed cluster at PSC (one worker per SLURM job) when workers == "bridges2"."""
    if workers == "bridges2":
        from dask_jobqueue import SLURMCluster

        cluster = SLURMCluster(local_directory=DASK_LOCAL_DIRECTORY, **BRIDGES2_SLURM_KWARGS)
        cluster.adapt(maximum_jobs=BRIDGES2_MAX_JOBS)
        return Client(cluster)

    return Client(
        n_workers=int(workers), threads_per_worker=THREADS_PER_WORKER, local_directory=DASK_LOCAL_DIRECTORY
    )


def leaf_columns(schema: pa.Schema) -> list[tuple[str, pa.DataType]]:
    """Enumerate (dotted parquet path, type) for every leaf column of a schema."""
    leaves = []

    def _walk(dtype: pa.DataType, path: str):
        if pa.types.is_struct(dtype):
            for field in dtype:
                _walk(field.type, f"{path}.{field.name}")
        elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype):
            _walk(dtype.value_type, f"{path}.list.element")
        elif pa.types.is_map(dtype):
            _walk(dtype.key_type, f"{path}.key_value.key")
            _walk(dtype.item_type, f"{path}.key_value.value")
        else:
            leaves.append((path, dtype))

    for field in schema:
        _walk(field.type, field.name)
    return leaves


def ceil_div(numerator: int, denominator: int) -> int:
    if numerator % denominator == 0:
        return numerator // denominator
    return numerator // denominator + 1


def rewrite_parquet_file(src: Path, dst: Path, page_size: int = PAGE_SIZE_BYTES, resume: bool = False) -> None:
    """Re-encode src to dst, streaming input row groups and regrouping to our target size.

    Reads one input row group at a time (bounded memory) and buffers them until there
    are enough rows to flush a ~100MB (capped at 100_000 rows) output row group, rather
    than loading the whole file into memory at once.
    """
    parquet_file = pq.ParquetFile(src)
    schema = parquet_file.schema_arrow
    leaves = leaf_columns(schema)
    dictionary_cols = [
        path for path, dtype in leaves if not pa.types.is_floating(dtype) and path != SPATIAL_INDEX_COLUMN
    ]
    column_encoding = {path: "BYTE_STREAM_SPLIT" for path, dtype in leaves if pa.types.is_floating(dtype)}
    if any(path == SPATIAL_INDEX_COLUMN for path, _ in leaves):
        column_encoding[SPATIAL_INDEX_COLUMN] = "DELTA_BINARY_PACKED"

    num_rows = parquet_file.metadata.num_rows
    if num_rows > 0:
        bytes_per_row = src.stat().st_size / num_rows
        row_group_rows = max(1, int(ROW_GROUP_TARGET_BYTES / bytes_per_row)) if bytes_per_row > 0 else num_rows
        row_group_rows = min(row_group_rows, ROW_GROUP_MAX_ROWS)
        # Redistribute evenly across that many groups, so we don't end up with one
        # full-sized group and a tiny leftover (e.g. 100_000 + 1 instead of ~50_000 + 50_001).
        num_output_groups = ceil_div(num_rows, row_group_rows)
        row_group_rows = ceil_div(num_rows, num_output_groups)
    else:
        row_group_rows = ROW_GROUP_MAX_ROWS

    dst.parent.mkdir(parents=True, exist_ok=True)
    write_target = dst.with_name(dst.name + ".tmp") if resume else dst
    with pq.ParquetWriter(
        write_target,
        schema,
        compression=COMPRESSION,
        compression_level=COMPRESSION_LEVEL,
        use_dictionary=dictionary_cols,
        column_encoding=column_encoding,
        data_page_size=page_size,
        dictionary_pagesize_limit=page_size,
        write_statistics=True,
        write_page_index=True,
    ) as writer:
        buffer = []
        buffered_rows = 0
        for row_group_index in range(parquet_file.num_row_groups):
            buffer.append(parquet_file.read_row_group(row_group_index))
            buffered_rows += buffer[-1].num_rows
            while buffered_rows >= row_group_rows:
                combined = pa.concat_tables(buffer)
                writer.write_table(combined.slice(0, row_group_rows))
                combined = combined.slice(row_group_rows)
                buffer = [combined] if combined.num_rows > 0 else []
                buffered_rows = combined.num_rows
        if buffer:
            combined = pa.concat_tables(buffer)
            if combined.num_rows > 0:
                writer.write_table(combined)

    if resume:
        write_target.replace(dst)


def plan_catalog_dir(src_dir: Path, dst_dir: Path) -> list[tuple[Path, Path, int]]:
    """Work out the (src_file, dst_file, page_size) mapping for every file in one catalog."""
    src_dataset = src_dir / "dataset"
    dst_dataset = dst_dir / "dataset"

    properties = TableProperties.read_from_dir(src_dir)
    page_size = INDEX_PAGE_SIZE_BYTES if properties.catalog_type == CatalogType.INDEX else PAGE_SIZE_BYTES

    dst_dataset.mkdir(parents=True, exist_ok=True)
    return [
        (src_file, dst_dataset / src_file.relative_to(src_dataset), page_size)
        for src_file in sorted(src_dataset.rglob("*.parquet"))
    ]


def finish_catalog_dir(
    src_dir: Path, dst_dir: Path, dst_files: list[Path], skip_metadata: bool = False
) -> None:
    """Rebuild metadata/properties for a catalog after its files have been rewritten."""
    src_dataset = src_dir / "dataset"
    dst_dataset = dst_dir / "dataset"
    properties = TableProperties.read_from_dir(src_dir)

    if dst_files:
        write_parquet_metadata(
            dst_dir,
            create_metadata=not skip_metadata and (src_dataset / "_metadata").exists(),
            create_thumbnail=(src_dir / "data_thumbnail.parquet").exists(),
            create_per_partition_stats=(src_dir / "per_partition_statistics.parquet").exists(),
        )
    else:
        names = ("_common_metadata",) if skip_metadata else ("_metadata", "_common_metadata")
        for name in names:
            if (src_dataset / name).exists():
                shutil.copy2(src_dataset / name, dst_dataset / name)

    # The two lines above already (re)write these from the new dataset files
    for item in sorted(src_dir.iterdir()):
        if item.name == "dataset":
            continue
        if item.name in ("hats.properties", "properties", *SIDE_FILE_NAMES):
            continue
        if item.is_dir():
            continue
        shutil.copy2(item, dst_dir / item.name)

    max_bytes = max((f.stat().st_size for f in dst_files), default=0)
    properties = properties.copy_and_update(
        hats_max_bytes=max_bytes,
        hats_estsize=estimate_dir_size(dst_dir, divisor=1024),
    )
    properties.to_properties_file(dst_dir)


def discover_catalog_dirs(src_root: Path) -> list[Path]:
    """Names of the catalogs in a collection come straight from collection.properties."""
    properties = CollectionProperties.read_from_dir(src_root)
    names = [properties.hats_primary_table_url, *(properties.all_margins or [])]
    names += (properties.all_indexes or {}).values()
    return [src_root / name for name in names]


def prepare_output_root(src_root: Path, dst_root: Path) -> None:
    """Create the output root matching src_root's group, setgid + group rwX.
    """
    dst_root.mkdir(parents=True, exist_ok=True)
    shutil.chown(dst_root, group=src_root.group())
    mode = dst_root.stat().st_mode | stat.S_ISGID | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP
    dst_root.chmod(mode)


def process_path(
    src_root: Path, dst_root: Path, workers: str, resume: bool = False, skip_primary_metadata: bool = False
) -> None:
    """Re-encode every catalog in the collection rooted at src_root."""
    catalog_dirs = discover_catalog_dirs(src_root)
    dst_dirs = [dst_root / src_dir.relative_to(src_root) for src_dir in catalog_dirs]
    catalog_tasks = [plan_catalog_dir(src_dir, dst_dir) for src_dir, dst_dir in zip(catalog_dirs, dst_dirs)]
    all_tasks = [task for tasks in catalog_tasks for task in tasks]
    tasks = [task for task in all_tasks if not (resume and task[1].exists())]
    src_files, dst_files, page_sizes = zip(*tasks) if tasks else ((), (), ())
    resumes = [resume] * len(tasks)

    skipped = len(all_tasks) - len(tasks)
    print(f"Found {len(catalog_dirs)} catalog(s), rewriting {len(tasks)} parquet file(s)", end="")
    print(f" ({skipped} already done, skipped)..." if skipped else "...")
    with make_client(workers) as client:
        try:
            print(f"Dask dashboard: {client.dashboard_link}")
        except KeyError:  # should be a bug in Dask
            pass
        futures = client.map(rewrite_parquet_file, src_files, dst_files, page_sizes, resumes)
        for future in tqdm(as_completed(futures), total=len(futures), desc="Rewriting", unit="file"):
            future.result()

    if resume:
        print("Cleaning up leftover .tmp files...")
        for tmp_file in dst_root.rglob("*.tmp"):
            tmp_file.unlink()

    print("Rebuilding metadata and properties...")
    # discover_catalog_dirs always puts the primary catalog first, ahead of any margins/indexes.
    for i, ((src_dir, dst_dir), catalog_task) in enumerate(zip(zip(catalog_dirs, dst_dirs), catalog_tasks)):
        finish_catalog_dir(
            src_dir,
            dst_dir,
            [dst_file for _, dst_file, _ in catalog_task],
            skip_metadata=(i == 0 and skip_primary_metadata),
        )

    shutil.copy2(src_root / "collection.properties", dst_root / "collection.properties")

    print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_path", type=Path, help="Path to a HATS catalog or collection directory")
    parser.add_argument("output_path", type=Path, help="Path to write the re-encoded copy")
    parser.add_argument(
        "-w",
        "--workers",
        required=True,
        help="Number of parallel Dask workers, or 'bridges2' for a distributed cluster at PSC",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume an interrupted run: skip files already written, write new ones atomically via .tmp",
    )
    parser.add_argument(
        "--skip_primary_metadata",
        action="store_true",
        help=(
            "Don't rebuild the primary catalog's (large) dataset/_metadata file; its "
            "_common_metadata is still rebuilt, and margins/indexes are unaffected"
        ),
    )
    args = parser.parse_args()

    src_root = args.input_path.resolve()
    dst_root = args.output_path.resolve()
    if not args.resume and dst_root.exists() and any(dst_root.iterdir()):
        raise SystemExit(f"Output path already exists and is non-empty: {dst_root}")

    prepare_output_root(src_root, dst_root)
    process_path(
        src_root,
        dst_root,
        args.workers,
        resume=args.resume,
        skip_primary_metadata=args.skip_primary_metadata,
    )

    print("Validating output collection...")
    if not is_valid_collection(dst_root, strict=True):
        raise SystemExit("Validation FAILED for the re-encoded collection.")
    print("Validation passed.")


if __name__ == "__main__":
    main()
