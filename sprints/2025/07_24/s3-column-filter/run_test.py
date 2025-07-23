#!/usr/bin/env python

import argparse
import json
import psutil
import os
import subprocess
import sys
from functools import partial
from time import sleep, monotonic

import nested_pandas
from upath import UPath


def usage_minio(*, minio_name, **_kwargs):
    output = subprocess.check_output([
        'mc',
        'admin',
        'prometheus',
        'metrics',
        minio_name,
        'cluster',
        '--json'
    ])
    parsed = json.loads(output)
    for part in parsed:
        if part['name'] == 'minio_s3_traffic_sent_bytes':
            return float(part['metrics'][0]['value'])
    raise ValueError('No minio_s3_traffic_sent_bytes found')


def usage_local(**_kwargs):
    if sys.platform == 'darwin':
        return float('nan')
    proc = psutil.Process(os.getpid())
    return proc.io_counters().read_bytes


def get_usage_fn(location, **kwargs):
    if location == 'minio':
        return partial(usage_minio, **kwargs)
    if location == 'local':
        return usage_local
    raise ValueError(f'Unknown location: {location}')


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--minio-name', default='lsdb')
    parser.add_argument('--filename', default='gaia_dr3-2-0-healpix-2.parquet')
    parser.add_argument('--block-size-kb', default=None, type=int)
    parser.add_argument('--location', choices=['local', 'minio'], default='minio')
    parser.add_argument('--columns', nargs='+', default=None)
    return parser.parse_args(argv)


def get_root_path(location, *, block_size_kb=None):
    if location == 'minio':
        upath_kwargs = {
            'key': 'admin',
            'secret': 'password',
            'endpoint_url': 'http://localhost:9000',
        }
        if block_size_kb is not None:
            upath_kwargs['default_block_size'] = block_size_kb * 1024
        return UPath('s3://bucket', **upath_kwargs)
    if location == 'local':
        return UPath('data/')
    raise ValueError(f'Unknown location: {location}')


def main(argv=None):
    args = parse_args(argv)
    if args.block_size_kb is not None and args.location == 'local':
        raise ValueError('--block-size-kb is not supported for --location=local')

    usage_fn = get_usage_fn(args.location, minio_name=args.minio_name)
    root = get_root_path(args.location, block_size_kb=args.block_size_kb)

    path = root / 'gaia_dr3-2-0-healpix-2.parquet'

    old_usage = usage_fn()
    t1 = monotonic()
    table = nested_pandas.read_parquet(path, columns=args.columns)
    wall_time = monotonic() - t1

    new_usage = usage_fn()
    while new_usage == old_usage:
        sleep(0.1)
        new_usage = usage_fn()
    used = new_usage - old_usage

    print(f'Read: {used / (1024 * 1024):.3f} MiB')
    print(f'Wall time: {wall_time:.3f} s')


if __name__ == '__main__':
    main()
