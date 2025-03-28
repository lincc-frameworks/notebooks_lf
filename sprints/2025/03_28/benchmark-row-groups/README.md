# Benchmark row groups

## Preparation

### Getting data

```sh
cd ./data
python -m pip install -r requirements.txt
# This will download the data and copy with different row groups
# By default, it will skip existing files, use --force to overwrite
python ./prepare.py
```

It should take few minutes to download the files (one Gaia and one ZTF HATS partitions)
and create 26 different row-group splits per each of these catalogs.
You need 31GiB of storage for this.

### Start S3 server

Install [Docker](https://docs.docker.com/get-docker/) and [MinIO client](https://github.com/minio/mc).

Launch the server:

```sh
cd ./minio
docker compose up --build -d
```

Please note that MinIO data will live at `./minio/minio_data` and the download speed will be limited to 15 MiB/s (120 Mib/s).
You can change the speed by edditing `./minio/nginx.conf`, docker container restart is required.

Upload data:

```
mc alias set lsdb_bench http://localhost:9000 admin password
mc mb lsdb_bench/bucket
# From the project root
mc cp ./data/*.parquet lsdb_bench/bucket/
```

It will use additional 31 GiB of storage.

## Running benchmarks

```sh
cd ./results
python -m pip install -r requirements.txt
python ./bench.py --help
python ./bench.py # args if needed
```

You can run the "local" and "remote" storage benchmarks separately using `-s local` and `-s remote`.
Local benchmarks took roughly an hour, while remote ones took ~10 hours on my machine.

## Analysis

```sh
cd ./analysis
python -m pip install -r requirements.txt
# You also may need to activate widgets extension, but I think it is not required with the modern jupyter
python -m jupyter notebook
```

Run the notebook and select experiments you'd like to aggregate over.
For each experiment (1 measurer, 1 storage type, 1 catalog type, and 1 columns set) the score is calculated.
It is defined as the ratio of the execution time to the best execution time, so the best score is 1,
and all other scores are between 0 and 1.
For multiple experiments, the score is calculated as the average of the scores of the individual experiments,
with weights assigned according to the measurement type.

The notebook show you two leaderboards:
- **Score** - the averaged score over all selected experiments
- **Rank** - averaged score rank, binned into single-digit rank numbers

Is a single experiment selected, the notebook will also show execution times before the leaderboards.
