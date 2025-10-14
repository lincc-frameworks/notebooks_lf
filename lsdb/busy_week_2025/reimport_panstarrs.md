# Re-import PanSTARRS

The aim is to end up with a structure like:

```
/hats/catalogs/panstarrs/
|-- collection.properties
|-- otmo /
| | -- hats.properties
| + -- . . .
|-- detection /
| | -- hats.properties
| + -- . . .
|-- otmo_10arcs /
| | -- hats.properties
| + -- . . .
+-- lightcurves /
    |-- hats.properties
```

## 1. Convert the primary catalog data

https://hats-import.readthedocs.io/en/latest/guide/reimporting.html

Use `UPath` for s3 access. You'll also want to `pip install s3fs`.

```
from dask.distributed import Client
from hats_import.pipeline import pipeline_with_client
from hats_import import ImportArguments
from upath import UPath

args = ImportArguments.reimport_from_hats(
    UPath(<current path>, <your storage options>),
    UPath("/hats/catalogs/panstarrs", <your storage options>),
    output_artifact_name="otmo",
)

with Client(n_workers=..., threads_per_worker=1, ...) as client:
    pipeline_with_client(args, client)
```

## 2. Use the primary catalog to create a collection

https://hats-import.readthedocs.io/en/latest/catalogs/collections.html

```
from hats_import import CollectionArguments

args = (
    CollectionArguments(
        output_artifact_name="panstarrs",
        output_path=UPath("/hats/catalogs", <your storage options>),
        ## dask arguments go here
        ## progress reporting arguments go here
    )
    .catalog(
        input_path=UPath("/hats/catalogs/panstarrs/otmo", <your storage options>),
    )
    .add_margin(margin_threshold=10.0, is_default=True)
)
```