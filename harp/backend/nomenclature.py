from pathlib import Path
from typing import Literal
from math import isnan

from core.static import interface
from core import log
from core import table

from harp.backend import harp_std
import pandas as pd


    
@interface
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
    
    harp_nomenclature_path = Path(Path(__file__).parent.parent / "harp_nomenclature.csv")
    harp_ref_table = _load_csv_table(harp_nomenclature_path)
    
    # @interface
    def __init__(self, csv_list: Path|list[Path], cols:list[str], raw_col: str, context: str):
        """

        Args:
            csv_list (Path | list[Path]): list of csv to concatenate and store as dataframe
            cols (list[str]): list of columns to check againt doubles etc..
            raw_col (str): column used for download query (provider specific names)
            context (str): context str used only for better error messages
        """        
        if isinstance(csv_list, Path): csv_list = [csv_list]
        
        csv_list = csv_list.copy()
        # read all the files and concatenate them
        # assumes they have the same formalism
        self.table = _load_csv_table(csv_list.pop(0))
        for csv_file in csv_list:
            _table = _load_csv_table(csv_file)
            pd.concat([self.table, _table], axis=0, ignore_index=False)
        
        self.context = context
        self.raw_col = raw_col
        self.cols = cols
        cols_list = self.table.columns.tolist()
        
        # assert table has standard harp name column
        if harp_std.harp_col not in cols_list: 
            log.error(f"Invalid table format, cannot find harp column ", 
                      harp_std.harp_col,
                      f"in tables for {context}", 
                      e=KeyError
            )
        
        # assert cols exist and verify them 
        for col in cols:
            if col not in cols_list: # check that the column is valid
                log.error(f"Invalid column \'{col}\', not found in nomenclature tables for {context}", e=KeyError)
            self._assert_col_has_no_doubles(col)
        
        # verify that harp_col values are all coherent and point to real harp values
        all_harp_names      = list(self.harp_ref_table[harp_std.harp_col].dropna())
        linked_harp_names   = list(self.table[harp_std.harp_col].dropna())
        
        missing = [n for n in linked_harp_names if n not in all_harp_names]
        if len(missing) > 0:
            mess  = f"Referencing Non-existant harp-names in file"
            mess +=  "\n    Concerned values: \n      - "
            mess +=  "\n      - ".join(missing)
            log.error(mess, e=KeyError)

    @interface
    def get_std_name(self, raw_name):
        lines = table.select(self.table, where=(self.raw_col, "=", raw_name))
        if not lines.values.size > 0:
            log.error(f"Could not find any match for raw name ",
                      raw_name,
                      " in harp internal layout files", 
                      e=KeyError
            )
            
        # we can assume the dataset contains at least one match
        if not lines.values.size > 1: # should not happend because it is guarded by the _check_table_for_doubles call
            log.error(f"Duplicate values ", raw_name, " for column raw_name in file ",
                      "in harp internal layout files. "
                      "\nThis error should not happen here, but earlier in the code.", 
                      e=KeyError
            )
        
        name = lines[harp_std.harp_col].values[0]
        
        if type(name) != str and isnan(name):
            log.error(f"Variable ", raw_name, 
                      f" is not interfaced (yet) with any harp name in {self.context} internal layout files",
                      e=KeyError
            )
        return name

    @interface
    def get_raw_name(self, harp_name: str):
        lines = table.select(self.table, where=(harp_std.harp_col, "=", harp_name))
        if not lines.values.size > 0:
            log.error(f"Could not find any match for harp name ",
                      harp_name,
                      " in ", self.context, " internal layout files", 
                      e=KeyError
            )
            
        # we can assume the dataset contains at least one match
        if not lines.values.size > 1: # should not happend because it is guarded by the _check_table_for_doubles call
            log.error(f"Duplicate values ", harp_name, f" for column {harp_std.harp_col} in file ",
                      " in ", self.context, " internal layout files",
                      "\nThis error should not happen here, but earlier in the code.", 
                      e=KeyError
            )
        
        name = lines[self.raw_col].values[0]
        
        if type(name) != str and isnan(name):
            log.error(f"Variable ", harp_name, 
                      f" is not interfaced (yet) with any harp name in {self.context} internal layout files",
                      e=KeyError
            )
        return name

    @interface
    def check_has_raw_name(self, raw_name: str):
        lines = table.select(self.table, where=(self.raw_col, "=", raw_name))
        if not lines.values.size > 0:
            log.error(f"Could not find any match for raw name ",
                      raw_name,
                      " in harp internal layout files", 
                      e=KeyError
            )
    
    @interface
    def _assert_col_has_no_doubles(self, col: str):
        """Asserts that the provided column in table has no double values
        Args:
            table (pd.DataFrame): dataframe
            col (str): column
            dataset_context (str): context string for errors, ex: "ERA5"
        """    # harp name doubles (common to both std and raw descriptors)
        
        doubles = self.table[self.table.duplicated(col)].dropna()
        if doubles.shape[0] > 0:
            mess = f"Error: two identitcal values in column \"{col}\" for {self.context} internal nomenclature"  
            mess += "\n    concerned values:\n        "
            mess += "\n        ".join(list(doubles[col]))
            log.error(mess, e=KeyError)