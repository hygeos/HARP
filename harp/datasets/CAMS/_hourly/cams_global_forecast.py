from datetime import date, datetime, timedelta
from pathlib import Path

import xarray as xr

from core import log
from core.static import interface

from harp._backend._utils.HarpErrors import InvalidQueryError
from harp._backend.harp_query import HarpQuery
from harp._backend.baseprovider import BaseDatasetProvider
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
    
    timespecs     = RegularTimespec(timedelta(seconds=0), 24)
    timespecs_ref = RegularTimespec(timedelta(seconds=0), 2)
    latency_ref   = timedelta(hours=7) # latency for time_ref publishing (00:00 is published around 06:00)
    max_leadtime  = 120 # maximum leadtime 
    
    timerange_str = "2015 ‥ T+5days"
    timerange = Timerange(start=datetime(1940, 1, 1), end=datetime.now() + timedelta(days=5))

    
    def __init__(self, variables: dict[str: str], config: dict={}, *, allow_extended_forecast: bool = False):
        
        self.allow_extended_forecast = allow_extended_forecast
        
        
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
        
        
        
    # overload baseprovider definition to add parameters
    def get(self,
            time: datetime, # type dictates if dt or range
            **kwargs,  # catch-all for additional keyword arguments
            ) -> xr.Dataset:
        
        return BaseDatasetProvider.get(self, time=time, **kwargs)
    
    # @interface
    def _execute_cds_request(self, target_filepath: Path, hq: HarpQuery):
        
        d = hq.extra["day"]
        date = d.strftime("%Y-%m-%d")
        dataset = self.product_type
        
        time = hq.ref_time.strftime("%H:%M")
        leadtimes = [str(round((t - hq.ref_time).total_seconds() / 3600)) for t in hq.timesteps]
        
        request = {
            "variable":         hq.variables,
            "date":             [f"{date}/{date}"],
            "time":             [time],
            "leadtime_hour":    leadtimes, 
            "type":             ["forecast"],
            "data_format":      "netcdf",
            # "download_format":  "unarchived"
        }
        
        # if area is not None: 
            # request['area'] = area
            
        client = cds.auth.get_client(self.url)
        client.retrieve(dataset, request, target_filepath)
        
        return
        
        
    def _standardize_time(self, ds: xr.Dataset):
        """
        from time dims = (forecast_reference_time, forecast_perdiod) to (time)
        """
        ds = ds.isel(forecast_reference_time=0)  # Remove the singleton dimension
        ds = ds.assign_coords(forecast_period=ds.time.values)
        ds = ds.drop(["time"])
        ds = ds.rename({'forecast_period': 'time'})
        
        return ds
    
    
    def _decompose_into_subqueries(self, hq: HarpQuery) -> list[HarpQuery]:
        """
        Decompose the query as a (series of) CDS query for the missing data,
        One query per day,
        Split the times by their ref times (00:00 or 12:00 per days)
        Rationnale:
            We need to make one query per reference timestamp, instead of per day
        
        """
        
        timesteps = self.timespecs.get_encompassing_timesteps(hq.time)
        latest_pub_ref = self.timespecs_ref.get_encompassing_timesteps(datetime.now() - self.latency_ref)[0]
        ref_times = {}
        
        for timestep in timesteps:
            res = self.timespecs_ref.get_encompassing_timesteps(timestep)
            lower_ref = res[0]
            
            ref = lower_ref
            

            if lower_ref < latest_pub_ref: # No NRT forecast (in between actualizations)
                ref = lower_ref
                 
            elif lower_ref > latest_pub_ref and not self.allow_extended_forecast:
                raise InvalidQueryError("Querying outside of restricted forecast (>11h leadtime) without setting allow_extended_forecast param. \nTry setting allow_extended_forecast=True in the provider constructor")
                
            else:
                ref = latest_pub_ref
            
            req_lead = timestep - ref
            max_lead = timedelta(hours=self.max_leadtime)
            
            if req_lead > max_lead: # means ref == latest
                raise InvalidQueryError("Querying outside of Forecast range "
                    + f"\n\tavailable: {latest_pub_ref} + {self.max_leadtime}h max"
                    + f"\n\trequested: {latest_pub_ref} + {int(req_lead.total_seconds() / 3600)}h"
            )
            
            if not ref in ref_times: ref_times[ref] = []
            ref_times[ref] += [timestep]
        
        queries = []
        for rt, timesteps in ref_times.items(): # format one query per ref time
            
            hqs = HarpQuery(
                variables   = hq.variables, 
                timesteps   = timesteps, 
                area        = hq.area, 
                levels      = hq.levels, 
                offline     = hq.offline,
                ref_time    = rt,
            )
            hqs.extra["day"] = date(rt.year, rt.month, rt.day)
            
            queries.append(hqs)
            
        return queries