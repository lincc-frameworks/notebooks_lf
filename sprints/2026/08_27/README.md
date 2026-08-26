Remember to record!!

## Heather
- To finish `lsdb.Catalog.write_catalog(row_group_kwargs=[...])`, I moved a helper function `split_to_row_groups()` out of `hats-import` and into `hats`. FYI this is causing CI issues until `hats` PR is resolved.
- [`hats/pixel_math/test_spatial_index.py`](https://github.com/astronomy-commons/hats/pull/730/changes#diff-c1782fae8535bb0197e137d1eae8841c0ef063defb572523e730069fbd90fd76)
- Previously I reported that the parquet metadata `num_row_groups` seemed to be inaccurate. In reality, my test was reading the wrong file. I updated the test and now the `num_row_groups` is working as expected.

## Sean

## Doug
- [Core + Extension Catalogs Prototype](TBD)
## Kostya

## Sandro

- [Sliver of DP2](./rubin_dp2_sliver.ipynb)

# Seeking feedback

_If your demo will be long, or you want to have a discussion, please put your name at the end_
