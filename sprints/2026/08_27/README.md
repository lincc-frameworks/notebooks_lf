Remember to record!!

## Heather
- To finish `lsdb.Catalog.write_catalog(row_group_kwargs=[...])`, I moved a helper function `split_to_row_groups()` out of `hats-import` and into `hats`. FYI this is causing CI issues until `hats` PR is resolved.
- Previously I reported that the parquet metadata `num_row_groups` seemed to be inaccurate. In reality, my test was reading the wrong file. I updated the test and now the `num_row_groups` is working as expected.
- [`hats/pixel_math/test_spatial_index.py`](https://github.com/astronomy-commons/hats/blob/954795dbf0c73d055c3f22e530260c97a2766466/tests/hats/pixel_math/test_spatial_index.py)


## Doug
- [Core + Extension Catalogs Prototype](./core_ext_prototype.ipynb)
## Kostya

## Sandro

- [Sliver of DP2](./rubin_dp2_sliver.ipynb)
- [Plotting broadband SED](https://lsdb-rubin--89.org.readthedocs.build/en/89/notebooks/plot_seds.html)

# Seeking feedback

_If your demo will be long, or you want to have a discussion, please put your name at the end_
