#!/usr/bin/env python3

import logging
import os
import shutil
from datetime import date, datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

import lsdb

## Directory where TNS catalogs are saved
tns_dir = Path("/epyc/data3/hats/catalogs/tns")


## Handling dates
today = datetime.now(timezone.utc).date()
today_id = today.strftime("%Y-%m-%d")


def import_tns():
    logging.basicConfig(level=logging.INFO)
    logging.info("Downloading TNS table...")
    tns_df = download_latest_table()
    logging.info("Converting into HATS catalog...")
    catalog = lsdb.from_dataframe(
        tns_df, ra_column="ra", dec_column="declination", margin_threshold=10
    )
    logging.info("Saving catalog to disk...")
    catalog.write_catalog(tns_dir / "tmp", catalog_name=today_id, overwrite=True)
    logging.info("Doing housekeeping...")
    delete_expired_catalogs()
    logging.info("Done.")


def download_latest_table():
    with requests.get(
        "https://www.wis-tns.org/sites/default/files/tns_public_objects/tns_public_objects.csv.zip",
        headers={
            "user-agent": 'tns_marker{{"tns_id":0, "type":"guest", "name":"LINCC Frameworks"}}'
        },
    ) as response:
        data = BytesIO(response.content)
        return pd.read_csv(data, skiprows=1, compression="zip")


def delete_expired_catalogs():
    """Delete catalogs older than a week"""
    expiry_date = today - timedelta(days=7)
    for folder in tns_dir.glob("*"):
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
    import_tns()
