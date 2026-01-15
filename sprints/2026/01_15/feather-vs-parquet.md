# Feather vs Parquet: random row access

### Apache Feather

Apache Feather is a serialization file format for Apache Arrow.
The storage is very close to the in-memory representation: no statistics and no encodings (though compression is still supported and enabled by default with `pyarrow.feather.write_feather`).
Feather v2 (the latest standard) is nearly 100% compatible with the Arrow IPC (inter-process communication) format.

`pyarrow` can memory-map a Feather file into a `Table` instance.
This makes it possible to access random rows and columns quickly, though the specific behavior depends on OS settings and the file system in use.


### Data and motivation

The motivation is to test data access patterns for the case where a spatially dense catalog with a massive data column is cross-matched against a much sparser catalog.
The idea of the alternative cross-matching approach is to load coordinates first, perform the cross-match, and then load all columns only for the matched objects.

We use the DESI MMU HATS catalog file as the input (`default.parquet` file): <https://huggingface.co/datasets/LSDB/mmu_desi_edr_sv3/blob/main/mmu_desi_edr_sv3/dataset/Norder%3D7/Dir%3D100000/Npix%3D109379.parquet>.

### Test code

Deps: `pip install pyarrow numpy`

```python
from time import monotonic

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc
import pyarrow.feather
import pyarrow.parquet
from pyarrow import MemoryMappedFile


BASELINE_PARQUET = 'baseline.parquet'
UNCOMPRESSED_PARQUET = 'uncompressed.parquet'
IPC = 'table.ipc'
UNCOMPRESSED_FEATHER = 'uncompressed.feather'


def make_data():
    table = pa.parquet.read_table(BASELINE_PARQUET)
    pa.parquet.write_table(table, UNCOMPRESSED_PARQUET, compression='none')
    pa.ipc.new_file(IPC, schema=table.schema).write(table)
    pa.feather.write_feather(table, UNCOMPRESSED_FEATHER, compression='uncompressed', chunksize=len(table))


def select_rows(fraction=0.01, rng=None):
    rng = np.random.default_rng(rng)
    healpix_29 = pa.parquet.read_table(BASELINE_PARQUET, columns=['_healpix_29'])['_healpix_29']
    num_rows = len(healpix_29)
    _, unique_index = np.unique(healpix_29, return_index=True)
    n = max(1, int(len(unique_index) * fraction))
    idx = rng.choice(unique_index, n, replace=False)
    values = healpix_29.take(idx)
    return idx, values


def read_full_parquet(path, rows):
    table = pa.parquet.read_table(path)
    return pc.take(table, rows)


def read_filtered_parquet(path, rows):
    return pa.parquet.read_table(path, filters=pc.field('_healpix_29').isin(rows))


def read_full_feather(path, rows):
    table = pa.feather.read_table(path)
    return pc.take(table, rows)


def read_mmap_feather(path, rows):
    table = pa.feather.read_table(path, memory_map=True)
    coords = table.select(['ra', 'dec'])
    mean_ra, mean_dec = np.mean(coords['ra']), np.mean(coords['dec'])
    return pc.take(table, rows)


def main():
    print("Creating the data")
    make_data()

    idx, values = select_rows()

    t = monotonic()
    first_result = read_full_parquet(BASELINE_PARQUET, idx)
    dt = monotonic() - t
    print(f'Read baseline parquet, filter table: {1000*dt:.1f} ms')

    t = monotonic()
    result = read_filtered_parquet(BASELINE_PARQUET, values)
    dt = monotonic() - t
    # assert result['_healpix_29'].equals(first_result['_healpix_29']), f'{result["_healpix_29"]}\n{first_result["_healpix_29"]}'
    print(f'Read baseline parquet with a read filter: {1000*dt:.1f} ms')
    
    t = monotonic()
    result = read_full_parquet(UNCOMPRESSED_PARQUET, idx)
    dt = monotonic() - t
    assert result['_healpix_29'].equals(first_result['_healpix_29'])
    print(f'Read uncompressed parquet, filter table: {dt*1000:.1f} ms')

    t = monotonic()
    result = read_filtered_parquet(UNCOMPRESSED_PARQUET, values)
    dt = monotonic() - t
    # assert result['_healpix_29'].equals(first_result['_healpix_29'])
    print(f'Read uncompressed parquet with a read filter: {dt*1000:.1f} ms')

    t = monotonic()
    result = read_full_feather(UNCOMPRESSED_FEATHER, idx)
    dt = monotonic() - t
    assert result['_healpix_29'].equals(first_result['_healpix_29'])
    print(f'Read uncompressed feather into memory: {dt*1000:.1f} ms')

    t = monotonic()
    result = read_mmap_feather(UNCOMPRESSED_FEATHER, idx)
    # sum_fl = pc.sum(result['spectrum'].combine_chunks().field('flux').values)
    dt = monotonic() - t
    assert result['_healpix_29'].equals(first_result['_healpix_29'])
    print(f'Read uncompressed feather with memmap: {dt*1000:.1f} ms')


if __name__ == "__main__":
    main()
```

### Outputs

I ran this on the CMU LSDB machine; the storage is HDDs.

```
Read baseline parquet, filter table: 1066.8 ms
Read baseline parquet with a read filter: 931.0 ms
Read uncompressed parquet, filter table: 719.6 ms
Read uncompressed parquet with a read filter: 740.2 ms
Read uncompressed feather into memory: 117.3 ms
Read uncompressed feather with memmap: 3.7 ms
```

### File sizes

```
> ls -lh *parquet *feather | awk '{print $5, $9}'
59M baseline.parquet
59M mmu_desi_edr_sv3_7_109379.parquet
123M uncompressed.feather
76M uncompressed.parquet
```

Even uncompressed Parquet is much smaller thanks to its encodings.
