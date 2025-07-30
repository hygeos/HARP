from datetime import date, datetime, timedelta
from pathlib import Path

from core import log
from core.static import interface

from harp._backend.harp_query import HarpQuery
from harp._backend.timerange import Timerange
from harp._backend.timespec import RegularTimespec
from harp._backend import cds


class GlobalReanalysis(cds.CdsDatasetProvider): 
    
    # url = "https://cds.climate.copernicus.eu/api"
    keywords = ["ECMWF", "Copernicus", "climate"]
    institution = "ECMWF"
    collection = "ERA5"
    
    name = "reanalysis-era5-single-levels"
    product_type = "reanalysis"
    
    timespecs = RegularTimespec(timedelta(seconds=0), 24)
    
    def __init__(self, *, variables: dict[str: str], config: dict={}):
        folder = Path(__file__).parent / "tables"
        files = [
            folder / "era5_table1.csv",
            folder / "era5_table2.csv",
            folder / "era5_table3.csv",
            folder / "era5_table4.csv",
            folder / "era5_table5.csv",
            folder / "era5_table6.csv",
            folder / "era5_table7.csv",
            folder / "era5_table8.csv",
        ]
        super().__init__(csv_files=files, variables=variables, config=config)
        
        # latest = datetime.now() - timedelta(days=6)
        # self.timerange = Timerange(start=datetime(1940, 1 ,1), end=latest)
        self.timerange_str = "1940 … -5days"
        self.timerange = Timerange(start=datetime(1940, 1, 1), end=datetime.now()-timedelta(days=6))
    
    # @interface
    def _execute_cds_request(self, target_filepath: Path, hq: HarpQuery):
        
        # TODO area
        times = [t.strftime("%H:%M") for t in hq.times]
        
        dataset = self.name
        request = {
                "product_type":     [self.product_type],
                
                "variable":     hq.variables,
                "year":         hq.extra["day"].year,
                "month":        hq.extra["day"].month,
                "day":          hq.extra["day"].day,
                "time":         times,
                
                "data_format":      "netcdf",
                "download_format":  "unarchived"
        }
        
        # if area is not None: 
            # request['area'] = area
        client = cds.auth.get_client(self.url)
        client.retrieve(dataset, request, target_filepath)
        
        return
