# Sprint demos for week of Feb 5, 2024

## Sandro

* [issue 90](https://github.com/astronomy-commons/lsdb/issues/90)

[Demo](./box_search.ipynb):

    lsdb.read_hipscat("ztf")
        .box(ra=[],dec=[])
        .compute()

## Sean

* [issue 7](https://github.com/astronomy-commons/lsdb/issues/7)
* [issue 51](https://github.com/astronomy-commons/lsdb/issues/51)

[Demo](./Margin%20generation.ipynb):

    gaia = lsdb.read_hipscat("gaia")
    ztf = lsdb.read_hipscat("ztf")
    vanilla_matches = ztf.crossmatch(gaia, r=1*arcsec).compute()

    ztf_margin = lsdb.read_hipscat("ztf_margin")
    ztf_with_margin = lsdb.read_hipscat("ztf", ztf_margin)
    margin_matches = ztf_with_margin.crossmatch(gaia, r=1*arcsec).compute()

    assert vanilla_matches != margin_matches

## Melissa

* [issue 107](https://github.com/astronomy-commons/lsdb/issues/107)
* [issue 108](https://github.com/astronomy-commons/lsdb/issues/108)

Demo: Show that the new stuff is faster than the old stuff.

- [`Association_table_stats.ipynb`](./Association_table_stats.ipynb)
- [`macauff_association_cardinality.ipynb`](./macauff_association_cardinality.ipynb)
- [`npy_stuff.ipynb`](./npy_stuff.ipynb)
- [`partition_info_load_speed.ipynb`](./partition_info_load_speed.ipynb)

## Mario

* [https://github.com/astronomy-commons/lsdb/issues/105]
* [https://github.com/astronomy-commons/hipscat/issues/188]
* [https://github.com/astronomy-commons/hipscat-import/issues/196]
* [https://github.com/astronomy-commons/hipscat/issues/198]
* [https://github.com/astronomy-commons/lsdb/issues/136]

[Demo](./conda-install.md)

## Mi

* [PR 142](https://github.com/astronomy-commons/lsdb/pull/142)

[Demo](./ztf_bts-ngc.ipynb): ZTF BTS vs NGC tutorial introduced by the PR

## Kostya

* https://github.com/lincc-frameworks/pandas-ts

[Demo](./nested-df.ipynb): work with `pandas-ts`.
