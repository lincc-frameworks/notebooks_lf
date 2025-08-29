#!/usr/bin/env python3

import logging
import shutil
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dask.distributed import Client
from hats_import.collection.arguments import CollectionArguments
from hats_import.pipeline import pipeline_with_client

## Directory where VSX catalogs are saved
vsx_dir = Path("/epyc/data3/hats/catalogs/vsx")
tmp_dir = vsx_dir / "tmp"

## Handling dates
today = datetime.now(timezone.utc).date()
month_id = today.strftime("%Y-%m")


def import_vsx():
    logging.basicConfig(level=logging.INFO)
    logging.info("Downloading VSX table...")
    input_path = download_latest_table()
    logging.info("Converting into HATS catalog...")
    make_hats(input_path)
    logging.info("Doing housekeeping...")
    delete_expired_catalogs()
    logging.info("Done.")


def download_latest_table():
    df = pd.read_csv(
        "https://www.aavso.org/vsx/external/vsx_csv.dat.gz",
        index_col=0,  # OID
        dtype_backend="pyarrow",
    )
    # Strip '#' from index name
    df.index.name = df.index.name.lstrip("#")
    filename = tmp_dir / "vsx.parquet"
    df.to_parquet(filename)
    return filename


def make_hats(input_path):
    args = (
        CollectionArguments(
            output_artifact_name=month_id,
            output_path=tmp_dir,
            progress_bar=False,
        )
        .catalog(
            sort_columns="OID",
            ra_column="RAdeg",
            dec_column="DEdeg",
            input_file_list=[input_path],
            file_reader="parquet",
            output_artifact_name=month_id,
        )
        .add_margin(margin_threshold=10.0)
    )

    with Client(n_workers=24, threads_per_worker=1, memory_limit="6GB") as client:
        pipeline_with_client(args, client)


def delete_expired_catalogs():
    """Delete catalogs older than 3 months."""
    expiry_date = today - timedelta(weeks=12)
    for folder in vsx_dir.glob("*"):
        if not folder.is_dir():
            continue
        try:
            date_str = folder.name.split("_", 1)[0]
            folder_date = date.fromisoformat(date_str)
        except Exception:
            continue
        if folder_date < expiry_date:
            logging.info(f"Deleting expired: {folder}")
            shutil.rmtree(folder)


if __name__ == "__main__":
    import_vsx()
