"""
Script run on vombat to take some pre-parquet'ed files and turn them into a HATS catalog.

This is fun, because the steps occur in a different order than those in the import pipeline, 
and kind of need to:

1) create the parquet _metadata and _common_metadata files, using all leaf parquet files
2) inspect the _metadata file to create the PartitionInfo object
3) write out partition_info.csv file
4) fake up some catalog properties and write to file
5) fake up a raw histogram for the point_map.fits file
"""


import hats.io.file_io as io
from hats.catalog import PartitionInfo
from hats.io import paths
from hats.io.parquet_metadata import write_parquet_metadata

from hats.catalog import TableProperties
from pathlib import Path

from datetime import datetime, timezone
import numpy as np

def do_stuff():

    # generate _metadata (and list of pixels) from parquet files
    destination_path = Path("/mnt/beegfs/scratch/data/priors/hats/s82_priors/")
    parquet_rows = write_parquet_metadata(destination_path)

    # Read partition info from _metadata and write to partition_info.csv
    partition_info = PartitionInfo.read_from_dir(destination_path)
    partition_info_file = paths.get_partition_info_pointer(destination_path)
    partition_info.write_to_file(partition_info_file)

    now = datetime.now(tz=timezone.utc)

    catalog_info = TableProperties(
        catalog_name="s82_priors",
        total_rows=parquet_rows,
        hats_order=5,
        hats_builder = "Melissa DeLucchi",
        hats_creation_date= now.strftime("%Y-%m-%dT%H:%M%Z"),

        ## These are fake/wrong for now:
        catalog_type="source",
        ra_column="ra",
        dec_column="dec",
    )
    catalog_info.to_properties_file(destination_path)

    ## Construct a raw histogram at order 5. Don't judge.
    raw_histogram = np.zeros(12_288, dtype=np.int64)
    for pix in partition_info.get_healpix_pixels():
        raw_histogram[pix.pixel] = 1
    io.write_fits_image(raw_histogram, paths.get_point_map_file_pointer(destination_path))


if __name__ == "__main__":
    do_stuff()

