from hipscat_import.catalog.file_readers import InputReader

import asdf

import numpy as np

from astropy.table import Table

def find_table(tree, path = ['roman', 'source_catalog']):
    # This returns the Table that was stored by the example above
    return tree['roman']['source_catalog']

class ASDFReader(InputReader):
    def __init__(self, chunksize=500_000, **kwargs):
        self.chunksize = chunksize
        self.kwargs = kwargs

    def read(self, input_file, read_columns=None):
        self.regular_file_exists(input_file, **self.kwargs)
        table = Table.read(input_file, format='asdf', find_table=find_table, **self.kwargs)
        if read_columns:
            table.keep_columns(read_columns)

        total_rows = len(table)
        read_rows = 0

        while read_rows < total_rows:
            df_chunk = table[read_rows : read_rows + self.chunksize].to_pandas()
            for column in df_chunk.columns:
                if (
                    df_chunk[column].dtype == object
                    and df_chunk[column].apply(lambda x: isinstance(x, bytes)).any()
                ):
                    df_chunk[column] = df_chunk[column].apply(
                        lambda x: x.decode("utf-8") if isinstance(x, bytes) else x
                    )

            yield df_chunk

            read_rows += self.chunksize