from pathlib import Path

from core import log
from core import table

import pandas as pd


    
# @interface
def _load_csv_table(path: Path):
    
    if not path.is_file():
        log.error(f"Could not find file ", path, e=FileNotFoundError)
        
    table = pd.read_csv(path, sep=",", skipinitialspace=True, keep_default_na=True)
    # table = table.dropna()
    # table = table.apply(lambda x: x.str.strip() if x.dtype == 'object' else x) # remove trailing whitespaces
    return table

class Nomenclature:
    """
    Helper class initialized on a CSV table of correspondances between 
    provider specific names (called raw names) and standardized harp names
    
    contains helpful function for harp
    """
    
    # # @interface
    def __init__(self, 
        csv: Path|list[Path]|pd.DataFrame, 
        context: str,
        query_col: str,
        harp_col: str = None,
        res_col: str = None,
        *,
        stub = False # used for BaseProvider init without loading any file
    ):
        """

        Args:
            csv_list (Path | list[Path]): list of csv to concatenate and store as dataframe
            cols (list[str]): list of columns to check againt doubles etc..
            raw_col (str): column used for download query (provider specific names)
            context (str): context str used only for better error messages
        """        
        
        if stub: return
        
        if isinstance(csv, Path): csv = [csv]
        if isinstance(csv, pd.DataFrame):
            self.table = csv
        else:
            csv = csv.copy()
            # read all the files and concatenate them
            # assumes they have the same formalism
            self.table = _load_csv_table(csv.pop(0))
            for csv_file in csv:
                _table = _load_csv_table(csv_file)
                self.table = pd.concat([self.table, _table], axis=0, ignore_index=False)
                self.table.reset_index()
        
        self.context = context
        
        self.query_col  = query_col
        self.harp_col   = harp_col
        self.res_col    = res_col
        
        cols = [query_col] 
        if harp_col != None: cols += [harp_col]
        if res_col != None: cols += [res_col]
        
        # assert cols exist and verify them 
        for col in cols:
            self._warn_if_col_has_doubles(col)
            
            
    def untranslate_query_name(self, param: str):
        """
        If necessary translate param from self.harp_col to self.query_col
        """
        
        if self.harp_col is None: # no translation
            return param
            
        else:
            self.assert_has_query_param(param)
            lines = table.select(self.table, where=(self.query_col, "=", param), cols=self.harp_col)
            return lines.values[0]
        
    
    def translate_query_to_result_name(self, param: str):
        """
        If necessary translate param from self.query_col to self.res_col
        """
        
        if self.res_col is None: # no translation
            self.assert_has_query_param(param)
            return param
            
        else:
            self.assert_has_query_param(param)
            lines = table.select(self.table, where=(self.query_col, "=", param), cols=self.res_col)
            return lines.values[0]
    
    
    def translate_to_query_name(self, param: str):
        """
        If necessary translate param from self.harp_col to self.query_col
        """
        
        if self.harp_col is None: # no translation
            self.assert_has_query_param(param)
            return param
            
        else:
            # if self.has_query_param(param): # NOTE: could be removed to force harp_col as interface
                # return param
            self.assert_has_harp_param(param)
            lines = table.select(self.table, where=(self.harp_col, "=", param), cols=self.query_col)
            return lines.values[0]
    
    
    def has_harp_param(self, harp_param: str):
        """
        Asserts that harp_param exists in self.harp_col
        """
        lines = table.select(self.table, where=(self.harp_col, "=", harp_param))
        return lines.values.size > 0
    
    def assert_has_harp_param(self, harp_param: str):
        if not self.has_harp_param(harp_param):
            log.error(f"Could not find any match for parameter ",
                      harp_param,
                      " in harp internal layout files", 
                      e=KeyError)
    
    # @interface
    def has_query_param(self, query_param: str):
        """
        Asserts that query_param exists in self.query_col
        """
        lines = table.select(self.table, where=(self.query_col, "=", query_param))
        return lines.values.size > 0
    
    def assert_has_query_param(self, query_param: str):
        if not self.has_query_param(query_param):
            log.error(f"Could not find any match for parameter ",
                      query_param,
                      " in harp internal layout files", 
                      e=KeyError)
    
    # @interface
    def _warn_if_col_has_doubles(self, col: str):
        """Asserts that the provided column in table has no double values
        Args:
            table (pd.DataFrame): dataframe
            col (str): column
            dataset_context (str): context string for errors, ex: "ERA5"
        """    # harp name doubles (common to both std and raw descriptors)
        
        doubles = self.table[self.table.duplicated(col)].dropna()
        if doubles.shape[0] > 0:
            mess = f"Warning: two identitcal values in column \"{col}\" for {self.context} internal nomenclature"  
            mess += "\nConcerned values:\n  - "
            mess += "\n  - ".join(list(doubles[col]))
            log.debug(log.rgb.orange, mess)