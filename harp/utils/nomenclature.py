from pathlib import Path
from typing import Literal

from core import log, rgb 
from core import table

import pandas as pd


def _check_table_for_doubles(table, path, mode: Literal["raw","std"]):
    
    # harp name doubles (common to both std and raw descriptors)
    col = "harp_name"
    doubles = table[table.duplicated(col)]
    if doubles.size > 0:
        mess = f"Error: two identitcal definition in column \"{col}\" of file {path}"
        mess += "\n    concerned values:\n        "
        mess += "\n        ".join(list(doubles[col]))
        log.error(mess, e=KeyError)
    
    if mode == "std": # Harp Harmonization Target
        col = "description"
        doubles = table[table.duplicated(col)]
        if doubles.size > 0:
            mess = f"Error: two identitcal definition in column \"{col}\" of file {path}"
            mess += "\n    concerned values:\n        "
            mess += "\n        ".join(list(doubles[col]))
            log.error(mess, e=KeyError)
            
    if mode == "raw":
        col = "raw_name"
        doubles = table[table.duplicated(col)]
        if doubles.size > 0:
            mess = f"Error: two identitcal definition in column \"{col}\" of file {path}"
            mess += "\n    concerned values:\n      - "
            mess += "\n      - ".join(list(doubles[col]))
            log.error(mess, e=KeyError)


def _load_dataset_csv(filepath) -> pd.DataFrame:
    
    if not filepath.is_file():
        log.error(f"Could not find file {filepath}", e=FileNotFoundError)
    
    table = pd.read_csv(filepath, sep=",", skipinitialspace=True, keep_default_na=True)
    table = table.dropna()
    table = table.apply(lambda x: x.str.strip() if x.dtype == 'object' else x) # remove trailing whitespaces
    
    mode = "raw" if "raw_name" in list(table.columns) else "std"
    
    _check_table_for_doubles(table, filepath, mode)
    
    return table
    
    
class Nomenclature:
    """
    Helper class initialized on a CSV table of correspondances between 
    provider specific names (called raw names) and standardized harp names
    
    contains helpful function for harp
    """
    
    std_file_path = Path(Path(__file__).parent.parent / "std_nomenclature.csv")
    std = _load_dataset_csv(std_file_path)
    
    def __init__(self, raw_nomenclature_path: Path):
        
        self.raw_file_path = raw_nomenclature_path
        self.raw = _load_dataset_csv(raw_nomenclature_path)
        # copy locally for ease of access
        self.std = Nomenclature.std                         # point to common harmonized nomenclature accross harp
        self.std_file_path = Nomenclature.std_file_path
        
        # check that the harp_name column from raw references existing names from std
        std_names = list(self.std["harp_name"])
        raw_names = list(self.raw["harp_name"])
        
        missing = [n for n in raw_names if n not in std_names]
        if len(missing) > 0:
            mess  = f"Referencing Non-existant harp-names in file {raw_nomenclature_path}"
            mess +=  "\n    Concerned values: \n      - "
            mess +=  "\n      - ".join(missing)
            log.error(mess, e=KeyError)


    def get_std_name(self, raw_name):
        lines = table.select(self.raw, where=("raw_name", "=", raw_name))
        if not lines.values.size > 0:
            log.error(f"Could not find any match for raw name {raw_name} in file {self.raw_file_path}", e=KeyError)
            
        # we can assume the dataset contains at least one match
        if not lines.values.size > 1: # should not happend because it is guarded by the _check_table_for_doubles call
            log.error(f"Duplicate values \"{raw_name}\" for column raw_name in file {self.raw_file_path}. This error should not happen here, but earlier.", e=KeyError)
        
        return lines["harp_name"].values[0]

        
    def get_raw_name(self, std_name):
        lines = table.select(self.raw, where=("harp_name", "=", std_name))
        if not lines.values.size > 0:
            log.error(f"Could not find any match for harp_name {std_name} in file {self.raw_file_path}", e=KeyError)
            
        # we can assume the dataset contains at least one match
        if not lines.values.size > 1: # should not happend because it is guarded by the _check_table_for_doubles call
            log.error(f"Duplicate values \"{std_name}\" for column harp_name in file {self.raw_file_path}. This error should not happen here, but earlier.", e=KeyError)
        
        return lines["raw_name"].values[0]

     