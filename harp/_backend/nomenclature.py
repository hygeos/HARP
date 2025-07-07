from pathlib import Path
from typing import Literal
from math import isnan

from core.static import interface
from core import log
from core import table

from harp._backend import harp_std
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
    
    # NOTE: legacy
    # harp_nomenclature_path = Path(Path(__file__).parent.parent / "harp_nomenclature.csv")
    # harp_ref_table = _load_csv_table(harp_nomenclature_path)
    
    # @interface
    def __init__(self, csv: Path|list[Path]|pd.DataFrame, cols:list[str], raw_col: str, context: str):
        """

        Args:
            csv_list (Path | list[Path]): list of csv to concatenate and store as dataframe
            cols (list[str]): list of columns to check againt doubles etc..
            raw_col (str): column used for download query (provider specific names)
            context (str): context str used only for better error messages
        """        
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
        self.raw_col = raw_col
        self.cols = cols
        cols_list = self.table.columns.tolist()
        
        # assert cols exist and verify them 
        for col in cols:
            if col not in cols_list: # check that the column is valid
                log.error(f"Invalid column \'{col}\', not found in nomenclature tables for {context}", e=KeyError)
            self._assert_col_has_no_doubles(col)

        
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