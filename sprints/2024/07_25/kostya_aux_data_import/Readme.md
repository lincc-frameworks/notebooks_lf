# Gaia EDR3 distances

This is data from Bailer-Jones+20 paper: https://ui.adsabs.harvard.edu/abs/2021AJ....161..147B/abstract
Description and data links are avialable here:
https://www2.mpia-hd.mpg.de/homes/calj/gedr3_distances.html

I donwloaded it and uncompressed with
```sh
curl -C - -O http://vo.ari.uni-heidelberg.de/gedr3dist/gedr3dist.dump.gz
pigz -dk gedr3dist.dump.gz
```

The file is a CSV file, no coordinates are given, but it has Gaia  (E)DR3 ids as the first column.
I joined it with Gaia DR3 hipscat and converted it to a parquet dataset with (I used 512GB node):

```python
import polars as pl
from pyarrow.dataset import parquet_dataset

dr3_dataset = parquet_dataset('/ocean/projects/phy210048p/shared/hipscat/catalogs/gaia_dr3/gaia/_metadata')
dr3 = pl.scan_pyarrow_dataset(dr3_dataset)
dist = pl.scan_csv('gedr3dist.dump')
dist_columns = dist.collect_schema().names()
(dr3
    .join(dist, on='source_id')
    .select(*dist_columns, '_hipscat_index', 'ra', 'dec')
    .collect(streaming=True)
    .write_parquet('gedr3dist_joined.parquet')
)
```

Split it into pieces and store as a parquet dataset
```python
from pyarrow.dataset import dataset, write_dataset

d = dataset('gedr3dist_joined.parquet')
write_dataset(d, 'gedr3dist_joined_parquet_dataset', max_rows_per_group=1 << 20, max_rows_per_file=1 << 20)
```

Hipscat-import with `hipscat-import.ipynb`


Kostya Malanchev, 2024.07.23