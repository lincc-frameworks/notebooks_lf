Notebooks I used to import ZTF files and crossmatch with Gaia.

The ZTF files associated with a single query are very large and don't fit in Github, but they are relatively quick to download once locally:\
I used\
```curl -o query_field795.tbl "https://irsa.ipac.caltech.edu/ibe/search/ztf/products/sci?WHERE=field=795"``` \
to download the .tbl file representing the metadata for a query. \
I then used that metadata in the `ztf query and curl.ipynb` notebook to compile URLs and download the specific ZTF lightcurve files as Feather files.
