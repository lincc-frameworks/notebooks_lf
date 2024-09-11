#!/usr/bin/env python
# coding: utf-8

# # Run a Periodogram Across Full ZTF Sources
# 
# This notebook is an adaptation of the Nested Dask [tutorial for loading HiPSCat data](https://nested-dask.readthedocs.io/en/latest/tutorials/work_with_lsdb.html).

# ## Install dependencies for the notebook
# 
# The notebook requires few packages to be installed.
# - `lsdb` to load and join "object" (pointing) and "source" (detection) ZTF catalogs
# - `aiohttp` is `lsdb`'s optional dependency to download the data via web
# - `light-curve` to extract features from light curves
# - `matplotlib` to plot the results

# In[1]:


# Comment the following line to skip dependencies installation
# get_ipython().run_line_magic('pip', 'install --quiet tqdm aiohttp light-curve matplotlib lsdb')


# In[2]:


import os
from importlib.metadata import version
from pathlib import Path

import dask.array
import dask.distributed
import dask_jobqueue
import light_curve as licu
import matplotlib.pyplot as plt
import nested_pandas as npd
import numpy as np
import pandas as pd
from lsdb import read_hipscat
from matplotlib.colors import LogNorm
from nested_dask import NestedFrame


# In[3]:


print(f"{version('lsdb') = }")
print(f"{version('nested-dask') = }")
print(f"{version('dask') = }")
print(f"{version('dask-expr') = }")


# Some additional setup for using Dask on PSC Bridges2:

# ## Load ZTF DR14

# In[4]:


# Full catalog
search_area = None


# In[5]:


catalogs_dir = "https://data.lsdb.io/unstable/ztf/"


lsdb_object = read_hipscat(
    f"{catalogs_dir}/ztf_dr14",
    columns=["ra", "dec", "ps1_objid"],
    search_filter=search_area,
)
lsdb_source = read_hipscat(
    f"{catalogs_dir}/ztf_zource",
    columns=["mjd", "ra", "dec", "mag", "magerr", "band", "ps1_objid", "catflags"],
    search_filter=search_area,
)
lc_columns = ["mjd", "mag", "magerr", "band", "catflags"]


# In[6]:


lsdb_source


# We need to merge these two catalogs to get the light curve data.
# It is done with LSDB's `.join_nested()` method which would give us a new catalog with a nested frame of ZTF sources. For this tutorial we'll just use the underlying nested dataframe for the rest of the analysis rather than the LSDB catalog directly.

# In[7]:


# Nesting Sources into Object
nested_ddf = lsdb_object.join_nested(lsdb_source, left_on="ps1_objid", right_on="ps1_objid", nested_column_name="lc")

# TODO remove once have added LSDB wrappers for nested_dask (reduce, dropna, etc)
nested_ddf = nested_ddf._ddf


# ## Convert LSDB joined catalog to `nested_dask.NestedFrame`

# First, we plan the computation to convert the joined Dask DataFrame to a NestedFrame.

# Now we filter our dataframe by the `catflags` column (0 flags correspond to the perfect observational conditions) and the `band` column to be equal to `r`.
# After filtering the detections, we are going to count the number of detections per object and keep only those objects with more than 10 detections.

# In[8]:


r_band = nested_ddf.query("lc.catflags == 0 and lc.band == 'r'")
nobs = r_band.reduce(np.size, "lc.mjd", meta={0: int}).rename(columns={0: "nobs"})
r_band = r_band[nobs["nobs"] > 10]
r_band


# Later we are going to extract features, so we need to prepare light-curve data to be in the same float format.

# ### Extract features from ZTF light curves
# 
# Now we are going to extract some features:
# - Top periodogram peak
# - Mean magnitude
# - Von Neumann's eta statistics
# - Excess variance statistics
# - Number of observations
# 
# We are going to use [`light-curve`](https://github.com/light-curve/light-curve-python) package for this purposes

# In[9]:

extractor = licu.Extractor(
    licu.Periodogram(
        peaks=1,
        max_freq_factor=1.0, # Currently 1.0 for fast runs, will raise for more interesting graphs later
        fast=True,
    ),  # Would give two features: peak period and signa-to-noise ratio of the peak
)


# light-curve requires all arrays to be the same dtype.
# It also requires the time array to be ordered and to have no duplicates.
def extract_features(mjd, mag, **kwargs):
    # We offset date, so we still would have <1 second precision
    t = np.asarray(mjd - 60000, dtype=np.float32)
    _, sort_index = np.unique(t, return_index=True)
    features = extractor(
        t[sort_index],
        mag[sort_index],
        **kwargs,
    )
    # Return the features as a dictionary
    return dict(zip(extractor.names, features))


features = r_band.reduce(
    extract_features,
    "lc.mjd",
    "lc.mag",
    meta={name: np.float32 for name in extractor.names},
)


# Before we are going next and actually run the computation, let's create a Dask client which would allow us to run the computation in parallel.

# Now we can collect some statistics and plot it. 

# In[10]:


mean_period = features['period_0'].mean()


# In[11]:

print("Dask task graph length", len(mean_period.dask))
