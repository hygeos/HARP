from datetime import date, datetime, timedelta
from pathlib import Path

from core import log
from core.static import interface

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
    
    def __init__(self, variables: dict[str: str], config: dict={}, allow_slow_access=False):
        folder = Path(__file__).parent / "tables" / "GlobalReanalysis"
        files = [
            folder / "table1.csv",
            # folder / "table2.csv",
        ]
        slow_access_files = [
            folder / "table3.csv",
            folder / "table4.csv",
            folder / "table5.csv",
            # folder / "table6.csv",
            # folder / "table7.csv",
        ]
        
        # TODO: Review specific config passing, maybe use kwargs instead ?
        # NOTE: should disable constructor override
        
        if allow_slow_access:
            log.warning(log.rgb.orange, self.name, ": Enabled slow access variable query")
            files += slow_access_files
        
        super().__init__(csv_files=files, variables=variables, config=config)
    
    @interface
    def _execute_cds_request(self, target_filepath: Path, query: dict, area: dict=None):
        
        if area is not None:
            log.error("Not implemented yet", e=RuntimeError)
        
        d = date(query["years"], query["months"], query["days"])
        
        dataset = self.name
        request = {
                'variable':     query["variables"],
                'date':         [d.strftime("%Y-%m-%d")],       # "date": ["2023-12-01/2023-12-01"],
                'time':         query["times"],
                'data_format':'netcdf',
        }
        # if area is not None: 
            # request['area'] = area
            
        client = cds.auth.get_client(self.url)
        client.retrieve(dataset, request, target_filepath)
        
        return
   