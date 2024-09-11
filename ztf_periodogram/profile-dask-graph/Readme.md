# Profile `len(x.dask)` for ~0.6M task graph

Run performance profiler with `py-spy`:
```sh
py-spy record -o py-spy.svg -- python ./ztf-periodogram-data.lsdb.io.py
```

[Rendered SVG](https://raw.githubusercontent.com/lincc-frameworks/notebooks_lf/main/ztf_periodogram/profile-dask-graph/py-spy.svg)

Run memory profiler with `memray`:
```sh
memray run -o memray.bin ztf-periodogram-data.lsdb.io.py
memray flamegraph memray.bin
```

I think you should download and open the HTML locally, I cannot make it via [htmlpreview](https://htmlpreview.github.io): [link](https://htmlpreview.github.io/?https://github.com/lincc-frameworks/notebooks_lf/blob/main/ztf_periodogram/profile-dask-graph/memray-flamegraph-memray.html)
