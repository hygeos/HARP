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
    
    url = "https://ads.atmosphere.copernicus.eu/api"
    keywords = ["ECMWF", "Copernicus", "atmosphere"]
    institution = "ECMWF"
    collection = "CAMS"
    
    name = "cams-global-reanalysis-eac4"
    product_type = "reanalysis"
    
    timespecs = RegularTimespec(timedelta(seconds=0), 8) # Trihourly
    timerange_str = "2003 ‥ T-1years"
    timerange = Timerange(start=datetime(1940, 1, 1), end=datetime.now()-timedelta(days=430))
    

    # cf: https://ads.atmosphere.copernicus.eu/datasets/cams-global-reanalysis-eac4?tab=download
    pressure_levels = [1, 2, 3, 5, 7, 10, 20, 30, 50, 70, 100, 150, 200, 250, 
                      300, 400, 500, 600, 700, 800, 850, 900, 925, 950, 1000]
                      
    model_levels = [i for i in range(1, 61)] # 60 model levels starting at 1
    
    
    def __init__(self, variables: dict[str: str], config: dict={}, allow_slow_access=False):
        folder = Path(__file__).parent / "tables" / "GlobalReanalysis"
        files = [
            # folder / "cams_ra_table1.csv",            # single level
            folder / "cams_ra_table2.csv",
        ]
        slow_access_files = [
            # folder / "cams_ra_table3.csv",            # single level
            # folder / "cams_ra_table4.csv",            # single level
            # folder / "cams_ra_table5.csv",            # single level
            folder / "cams_ra_table6.csv",
            folder / "cams_ra_table7.csv",
        ]
        
        # TODO: Review specific config passing, maybe use kwargs instead ?
        # NOTE: should disable constructor override
        
        if allow_slow_access:
            log.warning(log.rgb.orange, self.name, " volumetric: ", "Enabled slow access variable query")
            files += slow_access_files
        
        super().__init__(csv_files=files, variables=variables, config=config)

        # overload baseprovider definition to add parameters
    def get(self,
            time: datetime, # type dictates if dt or range
            area: list = None, # [N, W, S, E]
            levels: list[int] = pressure_levels,
            **kwargs,  # catch-all for additional keyword arguments
            ) -> xr.Dataset:
        """
        Get a dataset from the provider, with the specified parameters
        Args:
            time (datetime): single datetime of query
            levels (list[int], optional): list of pressure levels to query. Defaults to all available levels.
            area (list, optional): [N, W, S, E] bounding box of query. Defaults to None (global).
            **kwargs: additional keyword arguments to pass to the provider (not used currently)
        """
        
        levels = [str(i) for i in levels]
            
        return BaseDatasetProvider.get(self, time=time, levels=levels, area=area, **kwargs)

    
    # @interface
    def _execute_cds_request(self, target_filepath: Path, hq: HarpQuery):
        
        times = [t.strftime("%H:%M") for t in hq.timesteps]
        d = hq.extra["day"]
        
        dataset = self.name
        request = {
                "variable":     hq.variables,
                'date':         [d.strftime("%Y-%m-%d")],       # "date": ["2023-12-01/2023-12-01"],
                "time":         times,
                
                "pressure_level": hq.levels,
                
                'data_format':'netcdf',
        }
        
        # insert area to the query if needed
        if hq.area is not None: 
            request['area'] = hq.area
            
        client = cds.auth.get_client(self.url)
        client.retrieve(dataset, request, target_filepath)
        
        return
   