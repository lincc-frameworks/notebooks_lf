# Benchmarking LSDB (Dask) vs `pyarrow`

It is ongoing work to resolve https://github.com/astronomy-commons/lsdb/issues/127 and based on Troy Raen's DC2 notebook.
He implemented the same analysis twice: loading the catalog with LSDB and filter it with `.query`, and loading the catalog with `pyarrow` and filtering while reading files on fly.
Here I explore the performance of both approaches and thinking about a hybrid approach we may apply in LSDB.

## Common stuff

```python
# PSC
DC2_PATH = Path('/ocean/projects/phy210048p/shared/hipscat/catalogs/cosmodc2-mockv1')

COLUMNS = [
    'stellar_mass',
    'halo_mass',
    'Mag_true_g_lsst_z0',
    'Mag_true_r_lsst_z0',
    'Mag_true_i_lsst_z0',
    'Mag_true_z_lsst_z0',
    'Mag_true_y_lsst_z0',
    'redshift',
]
```

## Naive approach

#### LSDB

```python
with Client():
    df_lsdb = lsdb.read_hipscat(
        DC2_PATH,
        columns=COLUMNS,
    ).query(
        "stellar_mass > 1.e7 and redshift > 0.5 and redshift < 0.54"
    ).compute()
```
`Wall time: 53.3 s`

#### Pyarrow

```python
schema = ds.parquet_dataset(DC2_PATH / "_common_metadata").schema
common_ds_args = dict(filesystem=None, partitioning="hive", schema=schema)
dataset = ds.parquet_dataset(DC2_PATH / "_metadata", **common_ds_args)
pyarrow_df = dataset.to_table(
    columns=COLUMNS,
    filter=(
        (ds.field('stellar_mass') >= 1.e7) &
        (ds.field('redshift') > 0.5) &
        (ds.field('redshift') < 0.54)
    )
).to_pandas()
```
`Wall time: 1min`

Looks good?

## Adjust number of workers

```python
N_CPUS = 20
```

#### LSDB

```python
with Client(n_workers=N_CPUS):
    df_lsdb = lsdb.read_hipscat(
        DC2_PATH,
        columns=COLUMNS,
    ).query(
        "stellar_mass > 1.e7 and redshift > 0.5 and redshift < 0.54"
    ).compute()
```

`Wall time: 28.8 s`

#### Pyarrow

```python
@contextmanager
def limit_pyarrow_threading(cpu_count=None):
    old_cpu_count = pyarrow.cpu_count()
    if cpu_count is not None:
        pyarrow.set_cpu_count(cpu_count)
    try:
        yield None
    finally:
        pyarrow.set_cpu_count(old_cpu_count)
    
with limit_pyarrow_threading(N_CPUS):
    schema = ds.parquet_dataset(DC2_PATH / "_common_metadata").schema
    common_ds_args = dict(filesystem=None, partitioning="hive", schema=schema)
    dataset = ds.parquet_dataset(DC2_PATH / "_metadata", **common_ds_args)
    pyarrow_df = dataset.to_table(
        columns=COLUMNS,
        filter=(
            (ds.field('stellar_mass') >= 1.e7) &
            (ds.field('redshift') > 0.5) &
            (ds.field('redshift') < 0.54)
        )
    ).to_pandas()
```

`Wall time: 36.5 s`

## Summary

I can find better `N_CPUS` for both approaches. LSDB — ~21s for `N_CPUS=40`, Pyarrow —21s for `N_CPUS=10`.

Does it mean that LSDB is optimal? No.

How do we set limit CPU count for LSDB (and prevent Dask to use more)? How to limit I/O?
