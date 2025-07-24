# Benchmarking LSDB overheads

**Author**: Konstantin Malanchev

Here we measure time of cross-matching two catalogs (ZTF AXS vs Gaia DR4) versus copy-pasting these catalogs.
We do everything on LSDB

### LSDB pipeline

`xmatch-ztf-gaia.py`

```python
#!/usr/bin/env python3

import dask
import lsdb
from dask.distributed import Client


dask.config.set({
    'distributed.comm.timeouts.connect': '3600s',
    'distributed.comm.timeouts.tcp': '3600s',
})


ZTF_PATH = '/ocean/projects/phy210048p/shared/hipscat/catalogs/ztf_axs/ztf_dr14'
GAIA_PATH = '/ocean/projects/phy210048p/shared/hipscat/catalogs/gaia_dr3'

TMP_PATH = '/ocean/projects/phy210048p/malanche/tmp/dask'
OUTPUT_PATH = '/ocean/projects/phy210048p/malanche/tmp/x-ztf-gaia'


if __name__ == '__main__':
    ztf = lsdb.read_hipscat(ZTF_PATH)
    gaia = lsdb.read_hipscat(GAIA_PATH)
    # 180 GB instead of total 256 GB
    client = Client(n_workers=6, memory_limit='30GB', local_directory=TMP_PATH, threads_per_worker=1)
    gaia.crossmatch(ztf).to_hipscat(OUTPUT_PATH)
```

Sbatch script:

```bash
#!/bin/bash
#SBATCH --time=48:00:00                  # Job run time (hh:mm:ss)
#SBATCH --nodes=1                        # Number of nodes
#SBATCH --ntasks-per-node=1              # Number of task (cores/ppn) per node
#SBATCH --job-name=bench-lsdb            # Name of batch job
#SBATCH --partition=RM                   # Partition (queue)
#
###############################################################################
# Change to the directory from which the batch job was submitted
# Note: SLURM defaults to running jobs in the directory where
# they are submitted, no need for cd'ing to $SLURM_SUBMIT_DIR

module load python/3
source /jet/home/malanche/.virtualenvs/bench-lsdb/bin/activate

rm -rf /ocean/projects/phy210048p/malanche/tmp/dask
rm -rf /ocean/projects/phy210048p/malanche/tmp/x-ztf-gaia

time ./xmatch-ztf-gaia.py
```

The output size is 498GBi, and it took 46 minutes (stable by the level of few minutes).

### `cp -r` pipleine

Sbatch script:

```bash
#!/bin/bash
#SBATCH --time=48:00:00                  # Job run time (hh:mm:ss)
#SBATCH --nodes=1                        # Number of nodes
#SBATCH --ntasks-per-node=1              # Number of task (cores/ppn) per node
#SBATCH --job-name=bench-lsdb            # Name of batch job
#SBATCH --partition=RM-shared            # Partition (queue)
#
###############################################################################
# Change to the directory from which the batch job was submitted
# Note: SLURM defaults to running jobs in the directory where
# they are submitted, no need for cd'ing to $SLURM_SUBMIT_DIR

ZTF_PATH='/ocean/projects/phy210048p/shared/hipscat/catalogs/ztf_axs/ztf_dr14'
GAIA_PATH='/ocean/projects/phy210048p/shared/hipscat/catalogs/gaia_dr3'

TMP_PATH='/ocean/projects/phy210048p/malanche/tmp'
OUTPUT_PATH="${TMP_PATH}/copies"

rm -rf ${OUTPUT_PATH}
mkdir -p ${OUTPUT_PATH}

time cp -r ${ZTF_PATH} ${GAIA_PATH} ${OUTPUT_PATH}/
```

The output is 1030 GBi, and it took 86 minutes (stable by the level of few minutes).

### Summary

The overhead over I/O is on the level of 5-15%, which is great.
