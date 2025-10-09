REMEMBER TO RECORD

## Derek

- [left-join: pixel choices](./left-join-plan.ipynb) Still working on getting `.compute()` to be correct,
  but this notebook demonstrates the expected pixel output for cases when the catalog resolutions are
  different.

- fsspec.parquet optimization for remote files

PR: https://github.com/lincc-frameworks/nested-pandas/pull/385

|Operation           |Before   |After | Ratio|
|--------------------|---------|------|-------
|`.head()`           |   10.4s | 6.16s| 0.59 |
|mean of one column  |   3h 3m |1h 52m| 0.61 |
|`.random_sample`    |   2m 36s|1m 37s| 0.62 |

- Rubin DP1 Weekly Release 39 completed!  One error w.r.t. embargo; fixed.

## Kostya

- Uncle Val: [new demo notebook](https://github.com/lincc-frameworks/uncle-val/blob/main/docs/pre_executed/demo.ipynb)

## Doug

- ~~nested pandas compatibility~~ Nothing to show so far just applying new names of nested-pandas functions internally in LSDB

## Melissa

- [alternative healpix index column](./healpix_column.ipynb)

## Sean

- [empty margin catalogs](./compressed_mocs.ipynb)
- [skymap optmizations](./compressed_mocs.ipynb)

## Sandro

- [Updates to PPDB import](https://github.com/lsst-sitcom/linccf/blob/main/ppdb/incremental/main.ipynb)

# Seeking feedback

_If your demo will be long, or you want to have a discussion, please put your name at the end_


## Olivia

- check for nan values in position columns
- let's talk about doctests (: (: (:
- [https://github.com/astronomy-commons/lsdb/pull/1072/files](https://github.com/astronomy-commons/lsdb/pull/1072/files)
