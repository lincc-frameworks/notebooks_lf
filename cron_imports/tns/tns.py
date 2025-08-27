#!/usr/bin/env python3

import logging
import os
import shutil
from datetime import date, datetime, timezone, timedelta
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests

import lsdb

## Credentials for TNS BOT
TNS_API_KEY = os.environ["TNS_API_KEY"]
TNS_BOT_ID = os.environ["TNS_BOT_ID"]
TNS_BOT_NAME = os.environ["TNS_BOT_NAME"]


## Directory where TNS catalogs are saved
tns_dir = Path("/epyc/data3/hats/catalogs/tns")


## Handling dates
today = datetime.now(timezone.utc).date()
today_id = today.strftime("%Y-%m-%d")
today_folder = tns_dir / today_id


def import_tns():
    logging.basicConfig(level=logging.INFO)
    logging.info("Downloading TNS table...")
    tns_df = download_latest_table()
    logging.info("Converting into HATS catalog...")
    catalog = lsdb.from_dataframe(tns_df, ra_column="ra", dec_column="declination")
    logging.info("Saving catalog to disk...")
    catalog.write_catalog(today_folder, catalog_name=today_id)
    logging.info("Doing housekeeping...")
    update_symlink()
    delete_expired_catalogs()
    logging.info("Done.")


def download_latest_table():
    with requests.post(
        "https://www.wis-tns.org/system/files/tns_public_objects/tns_public_objects.csv.zip",
        headers={
            "user-agent": 'tns_marker{{"tns_id":"{id}","type":"bot","name":"{name}"}}'.format(
                id=TNS_BOT_ID,
                name=TNS_BOT_NAME,
            )
        },
        data={"api_key": (None, TNS_API_KEY)},
    ) as response:
        data = BytesIO(response.content)
        return pd.read_csv(data, skiprows=1, compression="zip")


def update_symlink():
    """Update "tns" symlink to point to today's folder"""
    symlink = tns_dir / "tns_latest"
    if symlink.is_symlink() or symlink.exists():
        symlink.unlink()
    symlink.symlink_to(today_folder, target_is_directory=True)
    logging.info(f"Updated: {symlink} -> {today_folder}")


def delete_expired_catalogs():
    """Delete catalogs older than a week"""
    expiry_date = today - timedelta(days=7)
    for folder in tns_dir.glob("*"):
        if folder.is_symlink():
            continue
        try:
            folder_date = date.fromisoformat(folder.name)
            if folder_date < expiry_date:
                logging.info(f"Deleting expired: {folder}")
                shutil.rmtree(folder)
        except Exception as e:
            logging.info(f"Skipping {folder}: {e}")


if __name__ == "__main__":
    import_tns()
