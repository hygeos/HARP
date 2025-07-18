from datetime import date, datetime, timedelta
from pathlib import Path

from core import log
from core.static import interface

from harp._backend.timespec import RegularTimespec
from harp._backend import cds


class GlobalReanalysisVolumetric(cds.CdsDatasetProvider): 
    
    # url = "https://cds.climate.copernicus.eu/api"
    keywords = ["ECMWF", "Copernicus", "climate"]
    institution = "ECMWF"
    collection = "ERA5"
    
    name = "reanalysis-era5-pressure-levels"
    product_type = "reanalysis"
    
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    
    def __init__(self, variables: dict[str: str], config: dict={}):
        folder = Path(__file__).parent / "tables"
        files = [
            folder / "era5_table9.csv",
        ]
        super().__init__(csv_files=files, variables=variables, config=config)
        
        self.timerange_str = "1940 … -5day"
        
    
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
                'pressure_level': [ # query all pressure levels 
                      '1',   '2',   '3',   '5',   '7',  '10',  '20',  '30',  '50',  '70', '100', '125','150', 
                    '175', '200', '225', '250', '300', '350', '400', '450', '500', '550', '600', '650', '700', 
                    '750', '775', '800', '825', '850', '875', '900', '925', '950', '975', '1000'
                ],
                'data_format':'netcdf',
        }
        
        # if area is not None: 
            # request['area'] = area
        client = cds.auth.get_client(self.url)
        client.retrieve(dataset, request, target_filepath)
        
        return
   