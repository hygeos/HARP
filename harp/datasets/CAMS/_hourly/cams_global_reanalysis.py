from datetime import date, datetime, timedelta
from pathlib import Path

from core import log
from core.static import interface

from harp._backend.harp_query import HarpQuery
from harp._backend.timerange import Timerange
from harp._backend.timespec import RegularTimespec
from harp._backend import cds


class GlobalReanalysis(cds.CdsDatasetProvider): 
    
    url = "https://ads.atmosphere.copernicus.eu/api"
    keywords = ["ECMWF", "Copernicus", "atmosphere"]
    institution = "ECMWF"
    collection = "CAMS"
    
    name = "cams-global-reanalysis-eac4"
    product_type = "reanalysis"
    
    timespecs = RegularTimespec(timedelta(seconds=0), 8) # Trihourly
    
    def __init__(self, variables: dict[str: str], config: dict={}):
        folder = Path(__file__).parent / "tables" / "GlobalReanalysis"
        files = [
            folder / "cams_ra_table1.csv",
            # folder / "table2.csv",        # Volumetric
        ]
        slow_access_files = [
            folder / "cams_ra_table3.csv",
            folder / "cams_ra_table4.csv",
            folder / "cams_ra_table5.csv",
            # folder / "cams_ra_table6.csv",        # Volumetric
            # folder / "cams_ra_table7.csv",        # Volumetric
        ]
        
        # TODO: Review slow access capacities; currently disabled
        allow_slow_access = False
        
        if allow_slow_access:
            log.warning(log.rgb.orange, self.name, ": Enabled slow access variable query")
            files += slow_access_files
        
        super().__init__(csv_files=files, variables=variables, config=config)
        
        self.timerange_str = "2003 ‥ T-1years"
        self.timerange = Timerange(start=datetime(1940, 1, 1), end=datetime.now()-timedelta(days=430))
        
    
    # @interface
    def _execute_cds_request(self, target_filepath: Path, hq: HarpQuery):
        
        # TODO area
        times = [t.strftime("%H:%M") for t in hq.times]
        d = hq.extra["day"]
        
        dataset = self.name
        request = {
                "variable":     hq.variables,
                'date':         [d.strftime("%Y-%m-%d")],       # "date": ["2023-12-01/2023-12-01"],
                "time":         times,
                "data_format":      "netcdf",
                "download_format":  "unarchived"
        }
        # if area is not None: 
            # request['area'] = area
            
        client = cds.auth.get_client(self.url)
        client.retrieve(dataset, request, target_filepath)
        
        return
   