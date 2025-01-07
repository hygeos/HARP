from pathlib import Path
import pandas as pd

from core import log
from core import table

from core.static import interface
from core.static import constraint

@interface
def _read_csv_as_df(path: Path):
    table = pd.read_csv(path, sep=",", skipinitialspace=True, keep_default_na=True)
    # table = table.dropna()
    # table = table.apply(lambda x: x.str.strip() if x.dtype == 'object' else x) # remove trailing whitespaces
    return table


class cds_table:

    @interface
    def __init__(self, files: list):        
        self.files: list = files
        c = constraint.path(exists=True, mode="file", context="HARP internal CDS tables")
        self.table = None
        
        other_files = self.files.copy()
        
        f = other_files.pop(0)
        c.check(f)          # check filepath is valid
        self.table = _read_csv_as_df(f)
        
        for f in other_files:    # ingest all provided csv files
            c.check(f)          # check filepath is valid
            t = _read_csv_as_df(f)
            
            # concatenate all the dataframes
            self.table = t if self.table is None else pd.concat([self.table, t], axis=0, ignore_index=False)
    
    def get_files(self):
        return self.files.copy()
    
    def shortname_to_cdsname(self, shortname) -> str:
        return table.select_cell(self.table, where=("short_name", "=", shortname), col="cds_name")
    
    def cdsname_to_shortname(self, cdsname) -> str:
        return table.select_cell(self.table, where=("cds_name", "=", cdsname), col="short_name")
        
    def has_harpname(self, harp_name) -> bool:
        df = table.select(self.table, where=("harp_name", "=", harp_name), cols=None)
        return df.shape[0] > 0
        
    def has_cdsname(self, cds_name) -> bool:
        df = table.select(self.table, where=("cds_name", "=", cds_name), cols=None)
        return df.shape[0] > 0
        
    def has_shortname(self, short_name) -> bool:
        df = table.select(self.table, where=("short_name", "=", short_name), cols=None)
        return df.shape[0] > 0
        
    # Pipeline:
    # harp_name -> short_name -> cds_name -> QUERY -> short_name -> harp_name
    def get_harpname(self, short_name) -> bool:
        df = table.select(self.table, where=("short_name", "=", short_name), cols="harp_name")
        return df.shape[0] > 0
        
    def get_cdsname(self, short_name) -> bool:
        return table.select_cell(self.table, where=("short_name", "=", short_name), col="cds_name")
        
    def get_shortname(self, harp_name) -> bool:
        df = table.select(self.table, where=("harp_name", "=", harp_name), col="short_name")
        return df.shape[0] > 0