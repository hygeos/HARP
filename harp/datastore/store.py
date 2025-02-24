from typing import Literal
from harp.providers.NASA import MERRA2
from harp.providers.ECMWF.Copernicus import ERA5

class store:
    
    providers = [
        MERRA2.hourly.M2I1NXASM,
        MERRA2.hourly.M2I1NXINT,
        MERRA2.hourly.M2I1NXLFO,
        MERRA2.hourly.M2I3NXGAS,
        MERRA2.hourly.M2SDNXSLV,
        MERRA2.hourly.M2T1NXADG,
        MERRA2.hourly.M2T1NXAER,
        MERRA2.hourly.M2T1NXCHM,
        MERRA2.hourly.M2T1NXCSP,
        MERRA2.hourly.M2T1NXFLX,
        MERRA2.hourly.M2T1NXINT,
        MERRA2.hourly.M2T1NXLFO,
        MERRA2.hourly.M2T1NXLND,
        MERRA2.hourly.M2T1NXOCN,
        MERRA2.hourly.M2T1NXRAD,
        MERRA2.hourly.M2T1NXSLV,
        MERRA2.hourly.M2T3NXGLC,
        ERA5.hourly.GlobalReanalysis,
        ERA5.hourly.GlobalReanalysisVolumetric,
    ]
    
    # def prune_providers(
    #         institution:None|Literal["NASA","ECMWF"],
    #         keywords=
    # ):
    
    pass