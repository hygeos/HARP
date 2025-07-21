from pathlib import Path
import pandas as pd

from core import log
from core import table

from core.static import interface
from core.static import constraint

from harp._backend.cds import cds_tables_meta_infos

# @interface
def _read_csv_as_df(path: Path):
    table = pd.read_csv(path, sep=",", skipinitialspace=True, keep_default_na=True)
    # table = table.dropna()
    # table = table.apply(lambda x: x.str.strip() if x.dtype == 'object' else x) # remove trailing whitespaces
    return table


class cds_table:
    """
    Legacy, now kept to inject metadata before feeding the internal table to the Nomenclature object
    """

    # @interface
    def __init__(self, files: list):        
        self.files: list = files
        c = constraint.path(exists=True, mode="file", context="HARP internal CDS tables")
        self.table = None
        
        other_files = self.files.copy()
        
        f = other_files.pop(0)
        c.check(f)          # check filepath is valid
        self.table = _read_csv_as_df(f)
        cds_table._append_meta_infos(self.table, f)
                
        for f in other_files:    # ingest all provided csv files
            c.check(f)          # check filepath is valid
            t = _read_csv_as_df(f)
            cds_table._append_meta_infos(t, f)
            
            # concatenate all the dataframes
            self.table = t if self.table is None else pd.concat([self.table, t], axis=0, ignore_index=False)
        self.table = self.table.drop_duplicates(subset=['short_name', 'id'])
        
        doubles = list(self.table[self.table.duplicated('query_name')].dropna()["query_name"].values)
        
        log.debug(f"Nomenclature: droping variables {doubles} because of ambigous definition (duplicate)")
        self.table = self.table.drop_duplicates(subset=['query_name'])
        
        
        t = self.table.copy()
        
        # remove lines where query_name contains space (unqueryable)
        result = t[t["query_name"].str.contains('Not available', na=False)]
        if len(result) > 0:
            log.debug(log.rgb.orange, 
                "Unqueryable parameter (from the CDS):  ", 
                ", ".join(list(result["short_name"].values))
        )
        self.table = t[~t["query_name"].astype(str).str.contains('Not available', na=False)].copy()
        
        # remove lines where query_name contains space (unqueryable)
        result = t[t["query_name"].str.contains(' ', na=False)]
        if len(result) > 0:
            log.debug(log.rgb.orange, 
                "Invalid char ' ' in query name (ECMWF doc error): ", 
                ", ".join(list(result["query_name"].values))
        )
        self.table = t[~t["query_name"].astype(str).str.contains(' ', na=False)].copy()
        
        # warn where query_name contains '-' (should be '_')
        result = t[t["query_name"].str.contains('-', na=False)]
        if len(result) > 0:
            log.debug(log.rgb.orange, 
                "Invalid char '-' in query name (ECMWF doc error): ", 
                ", ".join(list(result["query_name"].values))
            )
            
        # warn where short_name starts with a number (shouldn't)
        result = t[t["short_name"].str.contains(r'^[0-9]', na=False, regex=True)]
        if len(result) > 0:
            log.debug(log.rgb.orange, 
                "Invalid char (starting with number) in query name (ECMWF doc error): ", 
                ", ".join(list(result["short_name"].values))
            )
        
        self.table["query_name"] = self.table["query_name"].str.replace("-", "_") # NOTE: ECMWF doc contains errors, with cds_name containing "-" instead of "_" 
        
    
    
    def _append_meta_infos(t: pd.DataFrame, f: Path):
        if "era5" in f.name:
            mtable = cds_tables_meta_infos.era5[f.name]
        else: # TODO: plug CAMS
            pass
            
        # t["src"] = f
        t["dims"] = mtable["dimensions"]
        t["spatial"] = mtable["spatial_degrees"]
    