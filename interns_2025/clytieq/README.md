Notebooks I used to import ZTF files and crossmatch with Gaia.

The ZTF files associated with a single query are very large, but relatively quick to download, i.e.:\
```curl -o query.tbl "https://irsa.ipac.caltech.edu/ibe/search/ztf/products/sci?WHERE=field=795"```\
is what I used to download the .tbl file named `query_field795.tbl`, which I then used in the `ztf query and curl.ipynb` notebook to download the specific ZTF lightcurve files as Feather files.
