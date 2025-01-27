from datetime import date, datetime, timedelta
from pathlib import Path

from core import log
from core.static import interface

from harp.backend.timespecs import RegularTimesteps
from harp.backend import cds


class GlobalReanalysis(cds.CdsDatasetProvider): 
    
    # url = "https://cds.climate.copernicus.eu/api"
    keywords = ["ECMWF", "Copernicus", "C3S"]
    institution = "ECMWF"
    collection = "C3S"
    
    name = "reanalysis-era5-single-levels"
    product_type = "reanalysis"
    
    timespecs = RegularTimesteps(timedelta(seconds=0), 24)
    
    def __init__(self, **kwargs):
        folder = Path(__file__).parent / "tables"
        files = [
            folder / "table1.csv",
            folder / "table2.csv",
            folder / "table3.csv",
            folder / "table4.csv",
            folder / "table5.csv",
            folder / "table6.csv",
            folder / "table7.csv",
            folder / "table8.csv",
        ]
        super().__init__(config_subsection="ERA5_raster", csv_files=files, **kwargs)
    
    @interface
    def _execute_cds_request(self, target_filepath: Path, query, area: dict=None):
        
        # TODO area
        
        dataset = self.name
        request = {
                'product_type': [self.product_type],
                'variable':     query["variables"],
                'year':         query["years"],
                'month':        query["months"],
                'day':          query["days"],
                'time':         query["times"],
                'data_format':'netcdf',
        }
        
        # if area is not None: 
            # request['area'] = area
        client = cds.auth.get_client(self.url)
        client.retrieve(dataset, request, target_filepath)
        
        return
   