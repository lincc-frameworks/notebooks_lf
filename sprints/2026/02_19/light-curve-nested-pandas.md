# `light-curve` 🩶 `nested-pandas`

The `light-curve` package has supported list-struct Arrow array input for bulk feature extraction since v0.11.1.

```python
import light_curve as licu
import pyarrow as pa
from nested_pandas.datasets import generate_data

# We need single band sorted by time
nf = generate_data(10_000, 333).query("nested.band == 'g'").sort_values("nested.t")
# Few cheap features
features = licu.Extractor(licu.Amplitude(), licu.LinearTrend(), licu.MedianAbsoluteDeviation())

%timeit nf.map_rows(features, ["nested.t", "nested.flux"], row_container="args")
# 75 ms ± 9.74 ms per loop (mean ± std. dev. of 7 runs, 10 loops each)
%timeit features.many(pa.array(nf["nested"].nest[["t", "flux"]]), n_jobs=1)
# 44.5 ms ± 765 μs per loop (mean ± std. dev. of 7 runs, 10 loops each)
%timeit features.many(pa.array(nf["nested"].nest[["t", "flux"]]), n_jobs=4)
# 14.5 ms ± 372 μs per loop (mean ± std. dev. of 7 runs, 100 loops each)
```

See also [an example in the `light-curve` Readme](https://github.com/light-curve/light-curve-python?tab=readme-ov-file#nested-pandas).
