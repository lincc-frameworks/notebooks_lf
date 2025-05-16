from hats_import.catalog.file_readers import FitsReader
import pyarrow as pa
from astropy.io import fits
from astropy.table import Table
import pandas as pd

class HeasarcReader(FitsReader):
    """FITS reader that can read heasarc photon data"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def read(self, input_file, read_columns=None):
        # Mostly copy-pasted from the hipscat-import implementation
        self.regular_file_exists(input_file, **self.kwargs)
        table = Table.read(input_file, memmap=True, hdu=1, **self.kwargs)
        #if read_columns:
        #    table.keep_columns(read_columns)
        #elif self.column_names:
        #    table.keep_columns(self.column_names)
        #elif self.skip_column_names:
        #    table.remove_columns(self.skip_column_names)

        total_rows = len(table)
        read_rows = 0

        while read_rows < total_rows:
            chunk = table[read_rows : read_rows + self.chunksize]
            df_chunk = self.astropy_table_to_df(chunk)
            yield df_chunk

            read_rows += self.chunksize

    def astropy_table_to_df(self, table):
        """Data convertion, spoils input table"""

        # Handle list columns
        list_columns = ["CALIB_VERSION", "EVENT_CLASS", "EVENT_TYPE"]
        
        calib_pyarrow = pa.array(table["CALIB_VERSION"].tolist(), type=pa.list_(pa.int32()))
        event_class_pyarrow = pa.array(table["EVENT_CLASS"].tolist(), type=pa.list_(pa.bool_()))
        event_type_pyarrow = pa.array(table["EVENT_TYPE"].tolist(), type=pa.list_(pa.bool_()))
        
        
        for col in list_columns:
            table.remove_column(col)

        df = table.to_pandas()

        df["CALIB_VERSION"] = pd.Series(calib_pyarrow, dtype=pd.ArrowDtype(calib_pyarrow.type))
        df["EVENT_CLASS"] = pd.Series(event_class_pyarrow, dtype=pd.ArrowDtype(event_class_pyarrow.type))
        df["EVENT_TYPE"] = pd.Series(event_type_pyarrow, dtype=pd.ArrowDtype(event_type_pyarrow.type))
        
        return df