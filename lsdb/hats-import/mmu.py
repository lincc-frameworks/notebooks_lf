import argparse
import cmd
import math

import h5py
import numpy as np
import pyarrow as pa
from dask.distributed import Client
from hats_import import CollectionArguments
from hats_import.catalog.file_readers import InputReader
from hats_import.pipeline import pipeline_with_client
from upath import UPath


def parse_args(argv):
    parser = argparse.ArgumentParser("Convert MMU dataset to HATS")
    parser.add_argument(
        "-i", "--input", required=True, type=UPath, help="Input MMU dataset URI"
    )
    parser.add_argument(
        "-o", "--output", required=True, type=UPath, help="Output HATS URI"
    )
    parser.add_argument("-n", "--name", required=True, help="HATS catalog name")
    parser.add_argument("--ra", default="ra", help="Right ascension column name")
    parser.add_argument("--dec", default="dec", help="Declination column name")
    parser.add_argument("--first-n", default=None, type=int, help="First N files only, useful for debugging")
    return parser.parse_args(argv)


def np_to_pyarrow_array(array: np.ndarray) -> pa.Array:
    """Massively copy-pasted from hats_import.catalog.file_reader.fits._np_to_pyarrow_array
    https://github.com/astronomy-commons/hats-import/blob/e9c7b647dae309ced9f9ce2916692c2aecde2612/src/hats_import/catalog/file_readers/fits.py#L9
    """
    values = pa.array(array.reshape(-1))
    # "Base" type
    if array.ndim == 1:
        return values
    if array.ndim > 2:
        raise ValueError("Only 1D and 2D arrays are supported")
    n_lists, length = array.shape
    offsets = np.arange(0, (n_lists + 1) * length, length, dtype=np.int32)
    return pa.ListArray.from_arrays(values=values, offsets=offsets)


class MMUReader(InputReader):
    def __init__(self, chunk_mb: float):
        super().__init__()
        self.chunk_bytes = chunk_mb * 1024 * 1024

    def _num_chunks(
        self, file_obj, h5_file: h5py.File, columns: list[str] | None
    ) -> int:
        if columns is None:
            size = file_obj.size
        else:
            size = sum(h5_file[col].nbytes for col in columns)
        return max(1, int(math.ceil(size / self.chunk_bytes)))

    def read(self, input_file: str, read_columns: list[str] | None = None):
        with UPath(input_file).open("rb") as fh, h5py.File(fh) as h5_file:
            num_chunks = self._num_chunks(fh, h5_file, read_columns)
            if read_columns is None:
                read_columns = list(h5_file)
            n_rows = h5_file[read_columns[0]].shape[0]
            chunk_size = max(1, n_rows // num_chunks)
            for i in range(0, n_rows, chunk_size):
                data = {
                    col: np_to_pyarrow_array(h5_file[col][i : i + chunk_size])
                    for col in read_columns
                }
                table = pa.table(data)
                yield table


def input_file_list(path: UPath) -> list[str]:
    path_list = sorted(path.rglob("*.hdf5"))
    return [upath.path for upath in path_list]


def main(argv=None):
    cmd_args = parse_args(argv)

    input_files = input_file_list(cmd_args.input)
    if cmd_args.first_n is not None:
        input_files = input_files[:cmd_args.first_n]

    import_args = (
        CollectionArguments(
            output_artifact_name=cmd_args.name,
            output_path=cmd_args.output,
        )
        .catalog(
            input_file_list=input_files,
            file_reader=MMUReader(chunk_mb=16),
            ra_column=cmd_args.ra,
            dec_column=cmd_args.dec,
        )
        .add_margin(margin_threshold=10.0, is_default=True)
    )

    with Client(n_workers=8, threads_per_worker=1) as client:
        print(f"Dask dashboard: {client.dashboard_link}")
        pipeline_with_client(import_args, client)


if __name__ == "__main__":
    main()
