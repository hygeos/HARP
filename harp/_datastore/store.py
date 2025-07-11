from pathlib import Path
from harp._backend.baseprovider import BaseDatasetProvider
from harp.providers.NASA import MERRA2
from harp.providers.ECMWF.Copernicus import ERA5

import pandas as pd
from typing import Literal

_search_data_providers = [ # list of providers to participate in fuzzy searching
    # MERRA2.hourly.M2I1NXASM,
    # MERRA2.hourly.M2I1NXINT,
    # MERRA2.hourly.M2I1NXLFO,
    # MERRA2.hourly.M2I3NXGAS,
    # MERRA2.hourly.M2SDNXSLV,
    # MERRA2.hourly.M2T1NXADG,
    # MERRA2.hourly.M2T1NXAER,
    # MERRA2.hourly.M2T1NXCHM,
    # MERRA2.hourly.M2T1NXCSP,
    # MERRA2.hourly.M2T1NXFLX,
    # MERRA2.hourly.M2T1NXINT,
    # MERRA2.hourly.M2T1NXLFO,
    # MERRA2.hourly.M2T1NXLND,
    # MERRA2.hourly.M2T1NXOCN,
    # MERRA2.hourly.M2T1NXRAD,
    # MERRA2.hourly.M2T1NXSLV,
    # MERRA2.hourly.M2T3NXGLC,
    
    ERA5.hourly.GlobalReanalysis,
    ERA5.hourly.GlobalReanalysisVolumetric,
    # TODO: plug CAMS
]

def _compile_search_table(ip: BaseDatasetProvider):
    """
    Compile the search table for an instanced provider object
    """
    print(ip.name)
    # time_freq = ip.timespecs.
    t = ip.nomenclature.table.copy() # work on a copy
    t["search_column"] = t["long_name"].str \
        .replace('_', ' ', regex=False) \
        .replace('-', ' ', regex=False) \
        .replace(';', ' ', regex=False) \
        .replace(',', ' ', regex=False) \
        + "   " + t["query_name"] + "   " + ip.institution
        
    t.attrs["dataset"] = str(ip.__class__.__name__)
    t.attrs["import_path"] = str(ip.__class__).split("\'")[1]
        
    pass 

class store:
    """
    TODO: store for fuzzy searching
    """
    
    table = []

    for p in _search_data_providers:
        # instantiate the provider -> required to retrieve the informations
        ip = p(
            config=dict(dir_storage=Path("/tmp"), offline=True), 
            variables={}
        )
        table.append(_compile_search_table(ip))

