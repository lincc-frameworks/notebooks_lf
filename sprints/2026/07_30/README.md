Remember to record!!

## Olivia
Documentation: 
- As a follow-up to the Rubin DP2 tutorial notebook standardization work done last week,
- Applying notebook standardization rules to our robot and human guides
- Plus, trying out a (semi) one-shot application of the guide to our existing notebooks

## Heather
Have been working on [adding args to `write_catalog()`](https://github.com/astronomy-commons/lsdb/issues/1361) to give it similar features to `hats_import.ImportArguments`.

See [`test_to_hats.py`](https://github.com/astronomy-commons/lsdb/pull/1519/changes#diff-e0a3ecde40da44acb3e366db29781b9a3baf9c90430170557caadb778ccc80ef).

Arg | Status
------|------
row_group_kwargs | implemented and tested
create_parquet_metadata | already implemented, added test
create_per_partition_statistics | already implemented, added test
should_write_skymap | already implemented, added test
write_table_kwargs | already implemented, added test
drop_empty_siblings | blocked, need help with design
columns | not started

## Sean

## Doug
Playing with pure nested-pandas representations of Image Cutouts
# Seeking feedback

_If your demo will be long, or you want to have a discussion, please put your name at the end_

[Heather] DRY implementation of row groups
[Olivia] Dask dashboard links
