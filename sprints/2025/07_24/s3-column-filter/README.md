# Benchmark column filters

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

### Start HTTP server

Install [Docker](https://docs.docker.com/get-docker/).

Launch the server:

```sh
cd ./nginx
docker compose up --build -d
```

### Start S3 server

Install [Docker](https://docs.docker.com/get-docker/) and [MinIO client](https://github.com/minio/mc).

Launch the server:

```sh
cd ./minio
docker compose up --build -d
```

Please note that MinIO data will live at `./minio/minio_data` and the download speed will be limited to 40 MiB/s (320 Mib/s).
You can change the speed by editing `./minio/nginx.conf`, docker container restart is required.

Upload data:

```
mc alias set lsdb http://localhost:9000 admin password
mc mb lsdb/bucket
# From the project root
mc cp ./data/*.parquet lsdb/bucket/
```

It will use additional 31 GiB of storage.

### Run test

```shell
uv venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt
python ./run-test.py --help
```

When using `--location=minio`, minio client `mc` will be used to check how much data was sent.
When using `--location=nginx`, Nginx logs accessed via `docker compose` will be used to check how much data was sent.
That means that you shouldn't concurrently access minio and nginx servers when running these tests.
On macOS `--location=local` counting of read bytes is not supported, but you would still see the timing results.

Try to use different files with `--filename` and set different `--block-size-kb` (bot supported by `--location=local`).
