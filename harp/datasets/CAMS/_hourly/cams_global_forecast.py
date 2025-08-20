from datetime import date, datetime, timedelta
from pathlib import Path

from core import log
from core.static import interface

from harp._backend.harp_query import HarpQuery
from harp._backend.timerange import Timerange
from harp._backend.timespec import RegularTimespec
from harp._backend import cds


class GlobalForecast(cds.CdsDatasetProvider): 
    
    url = "https://ads.atmosphere.copernicus.eu/api"
    keywords = ["ECMWF", "Copernicus", "atmosphere"]
    institution = "ECMWF"
    collection = "CAMS"
    
    name = "cams-global-forecast"
    product_type = "cams-global-atmospheric-composition-forecasts"
    
    timespecs           = RegularTimespec(timedelta(seconds=0), 24)
    timespecs_reference = RegularTimespec(timedelta(seconds=0), 2)
    
    
    def __init__(self, variables: dict[str: str], config: dict={}):
        folder = Path(__file__).parent / "tables" / "GlobalForecast"
        files = [
            folder / "cams_fo_table1.csv",
        ]
        
        slow_access_files = [
            folder / "cams_fo_table2.csv",
        ]
        
        # TODO: Review slow access capacities; currently disabled
        allow_slow_access = False
        
        if allow_slow_access:
            log.warning(log.rgb.orange, self.name, ": Enabled slow access variable query")
            files += slow_access_files
        
        super().__init__(csv_files=files, variables=variables, config=config)
        
        self.timerange_str = "2015 … +5days"
        self.timerange = Timerange(start=datetime(1940, 1, 1), end=datetime.now()-timedelta(days=430))
        
    
    # @interface
    def _execute_cds_request(self, target_filepath: Path, hq: HarpQuery):
        
        times = [t.strftime("%H:%M") for t in hq.times] # TODO plug
        d = hq.extra["day"]
        
        dataset = self.product_type
        
        request = {
            "date": [d.strftime("%Y-%m-%d")],
            "time": ["00:00", "12:00"],         # TODO decompose query and plug
            "leadtime_hour": [8, 9],            # TODO decompose query and plug
            "type": ["forecast"],
            "data_format":      "netcdf",
            "download_format":  "unarchived"
        }
        
        # if area is not None: 
            # request['area'] = area
            
        client = cds.auth.get_client(self.url)
        client.retrieve(dataset, request, target_filepath)
        
        return

    
    def _extract_forecast_times(self, hq: HarpQuery):
        """
        Split the times by their ref times (00:00 or 12:00 per days)
        Rationnale:
            We need to make one query per reference timestamp, instead of per day
        """
        ref_times = {}
        
        for t in hq.times:
            lower, upper = self.timespecs_reference.get_encompassing_timesteps([hq.times])
            if not lower in ref_times: ref_times[lower] = []
            ref_times[lower] += [t] 