Remember to record!!

## Melissa

- registry template rendering live demo
- [collector readme](https://github.com/lincc-frameworks/lsdb.phys.cmu.edu-services/blob/main/vo_registry/vo_registry/collector/README.md)

## Kostya
## Wilson

## Olivia
_Not demoing this but: [hats#599](https://github.com/astronomy-commons/hats/pull/599) and [hats-import#631](https://github.com/astronomy-commons/hats-import/pull/631) in final comments stage_

Related: Import Pipeline page/diagram [on this branch](https://github.com/astronomy-commons/hats-import/tree/u/olynn/docs-diagram-demo) (but I'll just show my local render)

## Doug
## Derek

# Seeking feedback

## Sandro
- [HATS always downloads the whole parquet file over HTTP](https://github.com/astronomy-commons/hats/issues/592)
- [Crossmatching kwargs](https://github.com/astronomy-commons/lsdb/issues/946)
    - HOMEWORK - please make some prototype example calls of crossmatch for discussion

## Sean
- [Map partitions running locally on single partition](./map_partitions_local.ipynb)
    - Decisions:
        - `map_partitions` should continue to return a catalog.
        - name the argument something more like `compute_single_partition`
        - this is an incremental feature toward alternate execution graph.
- [explode](./explode.ipynb)
    - Decisions:
        - let's not add this into the API
        - create a notebook [here](https://docs.lsdb.io/en/latest/tutorial_toc/toc_nested.html) to talk about exploding from nested into individual rows.

_If your demo will be long, or you want to have a discussion, please put your name at the end_
