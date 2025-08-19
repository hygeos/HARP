"""
Meta data store for fuzzy searching
Indexes Variables per dataset
"""

from pathlib import Path
from harp._backend.baseprovider import BaseDatasetProvider
from harp.datasets import MERRA2
from harp.datasets import ERA5, CAMS


_search_data_providers = [ # list of providers to participate in fuzzy searching
    MERRA2.M2I1NXASM,
    MERRA2.M2I1NXINT,
    MERRA2.M2I1NXLFO,
    MERRA2.M2I3NXGAS,
    MERRA2.M2SDNXSLV,
    MERRA2.M2T1NXADG,
    MERRA2.M2T1NXAER,
    MERRA2.M2T1NXCHM,
    MERRA2.M2T1NXCSP,
    MERRA2.M2T1NXFLX,
    MERRA2.M2T1NXINT,
    MERRA2.M2T1NXLFO,
    MERRA2.M2T1NXLND,
    MERRA2.M2T1NXOCN,
    MERRA2.M2T1NXRAD,
    MERRA2.M2T1NXSLV,
    MERRA2.M2T3NXGLC,
    # TODO: plug MERRA2 DIURNAL
    # TODO: plug MERRA2 MONTHLY
    # TODO: plug MERRA2 CONSTANT
    ERA5.GlobalReanalysis,
    ERA5.GlobalReanalysisVolumetric,
    # TODO plug ERA5 monthly
    CAMS.GlobalReanalysis,
    CAMS.GlobalReanalysisVolumetric,
    CAMS.GlobalForecast,
    # TODO plug CAMS monthly (if exists)

]


def get_tables():
    
    tables = []
    for p in _search_data_providers:
        # instantiate the provider -> required to retrieve the informations
        ip = p(
            config=dict(dir_storage=Path("/tmp"), offline=True), 
            variables={}
        )
        tables.append(ip.format_search_table())

    return tables
