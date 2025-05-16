import numpy as np
import pandas as pd
import pyarrow as pa
from astropy.table import Table
from hats_import.catalog.file_readers import FitsReader


class HSCFitsReader(FitsReader):
    """FITS reader that converts ra and dec from radians to degrees"""
    def __init__(self, *args, ra_column, dec_column, flags_column, **kwargs):
        super().__init__(*args, **kwargs)
        self.ra_column = ra_column
        self.dec_column = dec_column
        self.flags_column = flags_column

    def read(self, input_file, read_columns=None):
        # Mostly copy-pasted from the hipscat-import implementation
        self.regular_file_exists(input_file, **self.kwargs)
        table = Table.read(input_file, memmap=True, **self.kwargs)
        if read_columns:
            table.keep_columns(read_columns)
        elif self.column_names:
            table.keep_columns(self.column_names)
        elif self.skip_column_names:
            table.remove_columns(self.skip_column_names)

        total_rows = len(table)
        read_rows = 0

        while read_rows < total_rows:
            chunk = table[read_rows : read_rows + self.chunksize]
            df_chunk = self.astropy_table_to_df(chunk)
            yield df_chunk

            read_rows += self.chunksize

    def astropy_table_to_df(self, table):
        """Data convertion, spoils input table"""
        convert_flags = self.flags_column in table.columns
        
        # Flags wouldn't convert to pandas due to astropy's implementation limitations
        # So we convert them manually and delete from the table
        if convert_flags:
            flags = table[self.flags_column]
            flags_length = flags.shape[1]
            flags_pyarrow = pa.array(flags.tolist(), type=pa.list_(pa.bool_(), flags_length))

            table.remove_column(self.flags_column)

        df = table.to_pandas()

        # Convert coords from radians to degrees
        df[self.ra_column] = np.degrees(df[self.ra_column])
        df[self.dec_column] = np.degrees(df[self.dec_column])

        # Assign flags
        if convert_flags:
            df[self.flags_column] = pd.Series(flags_pyarrow, dtype=pd.ArrowDtype(flags_pyarrow.type))
            
        return df