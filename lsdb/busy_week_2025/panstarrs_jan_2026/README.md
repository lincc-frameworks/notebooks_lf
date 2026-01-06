There are some metadata mismatches occurring on the new PanStarrs catalogs hosted in `s3://stpubdata/panstarrs/ps1/public/hats/otmo/`.

The root-level `properties` file still shows that the data was generated with a much older version of hats-import, converting from a hipscat-import generated catalog. We were under the assumption that you would be re-importing from the original source and detection tables to refresh the datasets. This is important, as we have identified some issues with the import pipeline over the last two years, and want to make sure that the data for our users is the best we can provide.

The margin cache (otmo_10arcs) should also be re-generated, as we know that there were problems with the geometry used to determine the points inside the margins, and many points were dropped. This means that cross-matching around the edges of pixels may not find the best counterpart. We recommend re-generating this dataset, using the most recent hats-import.

There are other benefits to re-importing the data from the originals using the hats-import pipeline: using a larger pixel threshold of 2500000 (instead of 1000000) will create fewer, larger files (still under 1G each); we use the `ZSTD` compression by default, so the files will be slightly smaller, with no appreciable performance hit; setting the default columns property on the catalog will make it easier to limit the data downloaded (and is also used by firefly to limit fields in the queries).

# Summary of recommendations:

```
from hats_import import CollectionArguments, pipeline_with_client

args = (
    CollectionArguments(
        output_artifact_name="otmo",
        output_path=UPath("/hats/catalogs", <your storage options>),
        ## progress reporting arguments go here
    )
    .catalog(
        input_path=UPath("/path/to/original/panstarrs/files", <your storage options>),
        ## reader arguments go here.
        ra_column="ra",
        dec_column="dec",
        pixel_threshold=2_500_000,
        addl_hats_properties={"hats_cols_default": "decMean,decMeanErr,epochMean,gFlags,gMeanPSFMag,gMeanPSFMagErr,iFlags,iMeanPSFMag,iMeanPSFMagErr,nDetections,ng,ni,nr,ny,nz,objID,objInfoFlag,qualityFlag,raMean,raMeanErr,rFlags,rMeanPSFMag,rMeanPSFMagErr,surveyID,yFlags,yMeanPSFMag,yMeanPSFMagErr,zFlags,zMeanPSFMag,zMeanPSFMagErr", "obs_regime": "Optical"}
    )
    .add_margin(margin_threshold=10.0, is_default=True)
)

with Client(... all those dask arguments) as client:
    pipeline_with_client(args, client)
```

If you're using the same CSV files that you sent to us, we have some tips for using those files with the `hats-import` pipeline [here](https://hats-import.readthedocs.io/en/latest/catalogs/public/panstarrs.html)