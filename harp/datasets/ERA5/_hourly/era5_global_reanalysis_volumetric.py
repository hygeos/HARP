from datetime import date, datetime, timedelta
from pathlib import Path

from core import log
from core.static import interface

from harp._backend.harp_query import HarpQuery
from harp._backend.baseprovider import BaseDatasetProvider
from harp._backend.timerange import Timerange
from harp._backend.timespec import RegularTimespec
from harp._backend import cds

import xarray as xr

class GlobalReanalysisVolumetric(cds.CdsDatasetProvider): 
    
    # url = "https://cds.climate.copernicus.eu/api"
    keywords = ["ECMWF", "Copernicus", "climate"]
    institution = "ECMWF"
    collection = "ERA5"
    
    name = "reanalysis-era5-pressure-levels"
    product_type = "reanalysis"
    
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    
    pressure_levels = [ # all pressure levels 
            1,   2,   3,   5,   7,  10,  20,  30,  50,  70, 100, 125, 150, 
            175, 200, 225, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 
            750, 775, 800, 825, 850, 875, 900, 925, 950, 975, 1000
        ]
        
    
    def __init__(self, *, variables: dict[str: str], config: dict={}):
        folder = Path(__file__).parent / "tables"
        files = [
            folder / "era5_table9.csv",
        ]
        super().__init__(csv_files=files, variables=variables, config=config)
        
        self.timerange_str = "1940 ‥ T-5days"
        self.timerange = Timerange(start=datetime(1940, 1, 1), end=datetime.now()-timedelta(days=6))
    
    
    # overload baseprovider definition to add parameters
    def get(self,
            time: datetime, # type dictates if dt or range
            levels: list[int] = pressure_levels,
            **kwargs,  # catch-all for additional keyword arguments
            ) -> xr.Dataset:
        
        levels = [str(i) for i in levels]
            
        return BaseDatasetProvider.get(self, time=time, levels=levels, **kwargs)
        
    
    # @interface
    def _execute_cds_request(self, target_filepath: Path, hq: HarpQuery):
        
        # TODO area
        times = [t.strftime("%H:%M") for t in hq.times]
        
        dataset = self.name
        request = {
                "product_type": [self.product_type],
                
                "variable":     hq.variables,
                "year":         hq.extra["day"].year,
                "month":        hq.extra["day"].month,
                "day":          hq.extra["day"].day,
                "time":         times,
                "pressure_level": hq.levels,
                
                "data_format":"netcdf",
                "download_format":  "unarchived"
        }
        
        # if area is not None: 
            # request['area'] = area
        client = cds.auth.get_client(self.url)
        client.retrieve(dataset, request, target_filepath)
        
        return
   